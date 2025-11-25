import asyncio
import aiohttp
import time 
import logging
from typing import Dict,List,Optional,Any,Set
from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path
import random

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class ScrapperConfig:
    """Configuration for the scrapper"""
    base_url:str= "https://issues.apache.org/jira/rest/api/2"
    max_concurrent_requests:int =5
    request_delay:float =0.2
    max_retries:int =5
    timeout:int =30
    page_size :int = 100
    checkpoint_interval:int =100
    batch_size:int =50


class AdaptiveRateLimiter:
    """Improved Rate limiter with better backoff strategy"""
    def __init__(self,initial_delay:float= 0.2,max_delay:float=60.0):
        self.delay = initial_delay
        self.min_delay = initial_delay
        self.max_delay = max_delay
        self.last_request_time = 0.0
        self.consecutive_errors = 0
        self._lock = asyncio.Lock()

    async def wait(self):
        """Wait before making the next request"""
        async with self._lock:
            now = time.time()
            elapsed = now - self.last_request_time
            if elapsed < self.delay:
                # Add jitter to avoid thundering herd
                jitter = random.uniform(0,0.1)
                await asyncio.sleep(self.delay - elapsed + jitter)
            self.last_request_time= time.time()

    def on_429(self,retry_after:Optional[int] = None):
        """Handle rate limiting with exponential backoff"""
        self.consecutive_errors +=1
        if retry_after:
            self.delay = retry_after + random.random()
        else:
            #exponential backoff with cap
            backoff = min(self.delay * (2 ** self.consecutive_errors), self.max_delay)
            jitter = random.random()
            self.delay = backoff + jitter
        logger.warning(f"Received 429. Increasing delay to {self.delay:.2f} seconds.")

    def on_success(self):
        """gradually reduce delay on success"""
        if self.consecutive_errors >0:
            self.consecutive_errors =0

        self.delay = max(self.delay * 0.95, self.min_delay)




class JiraScrapper:
    def __init__(self,config:ScrapperConfig,output_dir:Path):
        self.config = config
        self.output_dir = output_dir
        self.rate_limiter = AdaptiveRateLimiter(config.request_delay)
        self.session: Optional[aiohttp.ClientSession] = None

        self.stats = {
            'projects': {},
            'total_issues_found': 0,
            'total_issues_processed': 0,
            'successful': 0,
            'failed': 0,
            'start_time': None,
            'last_checkpoint': None
        }
    
    async def __aenter__(self):
        """Context manager entry with connection pooling"""
        timeout  = aiohttp.ClientTimeout(total=self.config.timeout)
        connector = aiohttp.TCPConnector(
            limit = self.config.max_concurrent_requests,
            limit_per_host = self.config.max_concurrent_requests,
            use_dns_cache = True
        )
        self.session = aiohttp.ClientSession(timeout=timeout,connector=connector,headers={'Accept': 'application/json'})
        return self
    
    async def __aexit__(self,exc_type,exc_val,exc_tb):
        """Context manager exit"""
        if self.session:
            await self.session.close()


    async def make_request(self,endpoint:str,params:Optional[Dict] = None,retry_count:int = 0)-> Optional[Dict]:
        if retry_count > self.config.max_retries:
            logger.error(f"Max retries exceeded for {endpoint}")
            return None
        await self.rate_limiter.wait()
        url= f"{self.config.base_url}{endpoint}"
        try:
            async with self.session.get(url,params=params) as response:
                if response.status == 200:
                    self.rate_limiter.on_success()
                    return await response.json()
                elif response.status ==429:
                    retry_after  = response.headers.get('Retry-After')
                    if retry_after:
                        retry_after = int(retry_after) 

                    self.rate_limiter.on_429(retry_after)

                    logger.warning(f"429 Too Many Requests for {url}. Retrying after {self.rate_limiter.delay:.2f} seconds.")
                    return await self.make_request(endpoint,params,retry_count+1)
                elif response.status >= 500:
                    self.rate_limiter.on_429()
                    wait_time = (2 ** retry_count) + random.random()
                    logger.warning(f"Server error {response.status} for {url}. Retrying in {wait_time:.2f} seconds. ")
                    await asyncio.sleep(wait_time)
                    return await self.make_request(endpoint,params,retry_count+1)
                
                else:
                    logger.error(f"Request to {url} failed with status {response.status}")
                    return None
        except asyncio.TimeoutError:
            logger.warning(f"Timeout for {url}. Retry {retry_count + 1}")
            if retry_count < self.config.max_retries:
                await asyncio.sleep(2 ** retry_count)
                return await self.make_request(endpoint, params, retry_count + 1)
            return None
        

        except aiohttp.ClientError as e:
            logger.warning(f"Connection error for {url}:{e},Retry {retry_count + 1} ")
            if retry_count < self.config.max_retries:
                await asyncio.sleep(2**retry_count)
                return await self.make_request(endpoint,params,retry_count+1)
            
        except Exception as e:
            logger.error(f"Unexpected error for {url}:{e}")
            return None
        
    async def get_issues_for_project(self,project_key:str)->List[str]:
        """Stream issue keys to avoid loading all in memory"""
        issue_keys = []
        start_at = 0
        while True:
            params = {
                'jql': f'project={project_key} ORDER BY created DESC',
                'fields': 'key',
                'maxResults': self.config.page_size,
                'startAt': start_at
            }
            logger.info(f"Fetching issues for project {project_key} starting at {start_at}")
            result = await self.make_request('/search',params)
            
            if not result:
                logger.error(f"Failed to fetch issues for project {project_key}")
                break
            batch_keys = [issue['key'] for issue in result.get('issues',[])]
            if not batch_keys:
                break
            issue_keys.extend(batch_keys)
            start_at += len(batch_keys)
            total = result.get('total',0)
            logger.info(f"Fetched {len(issue_keys)}/{total} issues for project {project_key}")
            
            # Stop if we have all issues or reach reasonable limit
            if len(issue_keys) >= total or len(issue_keys) >= 10000:
                break

        return issue_keys
    

    async def get_issue_details(self,issue_key:str)->Optional[Dict]:
        """Fetch only necessar fields"""
        params = {
            'fields': 'key,summary,description,status,priority,reporter,assignee,'
                     'created,updated,labels,comment',
            
        }
        result = await self.make_request(f'/issue/{issue_key}', params)

        if result:
            self.stats['successful'] +=1
        else:
            self.stats['failed'] += 1
            logger.error(f"Failed to fetch {issue_key}")

        return result
    
    def load_checkpoint(self,project_key:str)->Set[str]:
        """load checkpoint efficiently"""
        checkpoint_file = self.output_dir/project_key/'checkpoint.json'
        if checkpoint_file.exists():
            try:
                with open(checkpoint_file,'r') as f:
                    checkpoint = json.load(f)
                    return set(checkpoint.get('processed',[]))
            except Exception as e:
                logger.error(f"Failed to load checkpoints {e}")
        return set()
    
    def save_checkpoint(self,project_key:str,processed_keys:Set[str]):
        """Save checkpoints"""
        try:
            checkpoint_file = self.output_dir/project_key/'checkpoint.json'
            with open(checkpoint_file,'w') as f:
                json.dump({
                    'processed': list(processed_keys),
                    'timestamp': datetime.now().isoformat(),
                    'count': len(processed_keys)
                },f)

        except Exception as e:
            logger.error(f"Failed to save checkpoints")

    async def process_issue_batch(self,issue_keys:List[str],project_key:str,processed_key:Set[str],semaphore:asyncio.Semaphore):
        """process a batch of issues to control memory usage"""
        async def process_single_issue(key:str):
            if key in processed_key:
                return None
            
            async with semaphore:
                issue_data = await self.get_issue_details(key)
                if issue_data:
                    issue_file = self.output_dir/project_key/f'{key}.json'
                    try:
                        with open(issue_file,'w',encoding='utf-8') as f:
                            json.dump(issue_data,f,ensure_ascii=False,indent=2)
                        return key
                    except Exception as e:
                        logger.error(f"Failed to save {key}:{e}")
                        return None
                    
                return None
            
        #process batch with limited concurrency
        tasks = [process_single_issue(key) for key in issue_keys]
        results = await asyncio.gather(*tasks,return_exceptions=True)


        successful = 0
        for result in results:
            if isinstance(result,Exception):
                logger.error(f"Task Failed:{result}")
            elif result:
                processed_key.add(result)
                successful +=1
        self.stats['total_issues_processed']+= successful
        return successful
    
    async def scrape_project(self,project_key:str)->None:
        logger.info(f"Started scraping:{project_key}")

        project_dir = self.output_dir/project_key
        project_dir.mkdir(parents=True,exist_ok=True)
        #load checkpoints
        processed_keys= self.load_checkpoint(project_key)
        logger.info(f"{len(processed_keys)} issues processed")

        all_issue_keys = await self.get_issues_for_project(project_key)
        self.stats['total_issues_found'] += len(all_issue_keys)
        logger.info(f"Found {len(all_issue_keys)} total issues for {project_key}")

        #filter out already processed
        pending_keys= [k for k in all_issue_keys if k not in processed_keys]
        logger.info("Processing new issues")

        if not pending_keys:
            logger.info(f"no new issues to process for{project_key} ")
            return
        semaphore =asyncio.Semaphore(self.config.max_concurrent_requests) 
        total_processed = 0

        for batch_start in range(0,len(pending_keys),self.config.batch_size):
            batch_end = batch_start + self.config.batch_size
            batch = pending_keys[batch_start:batch_end]
            logger.info(f"Processing batch {batch_start//self.config.batch_size+1}")  
            batch_processed = await self.process_issue_batch(
                batch,project_key,processed_keys,semaphore
            ) 
            total_processed += batch_processed

            if len(processed_keys) % self.config.checkpoint_interval ==0:
                self.save_checkpoint(project_key,processed_keys)
                logger.info(f"Checkpoint:{len(processed_keys)}issues")

            await asyncio.sleep(0.1)

        #final checkpoint
        self.save_checkpoint(project_key,processed_keys) 
         # Update project stats
        self.stats['projects'][project_key] = {
            'total': len(all_issue_keys),
            'processed': len(processed_keys),
            'success_rate': len(processed_keys) / len(all_issue_keys) if all_issue_keys else 0
        }
        
        logger.info(f"Completed {project_key}: {len(processed_keys)}/{len(all_issue_keys)} "
                   f"({self.stats['projects'][project_key]['success_rate']:.1%})")   
        
    
    async def scrape_projects(self,project_keys:List[str])->None:
        self.stats['start_time'] = datetime.now()
        logger.info(f"Starting scrape for {len(project_keys)} projects:{project_keys}")
        for i,project_key in enumerate(project_keys,1):
            try:
                logger.info(f"🔹 Project {i}/{len(project_keys)}: {project_key}")
                await self.scrape_project(project_key)

                if i < len(project_keys):
                    await asyncio.sleep(1)

            except Exception as e:
                logger.error(f"💥 Critical error scraping {project_key}: {e}")

        await self._print_final_stats()

    
    async def _print_final_stats(self):
         
         
         """Print comprehensive final statistics"""
         end_time = datetime.now()
         duration = (end_time - self.stats['start_time']).total_seconds()
        
         logger.info("=" * 60)
         logger.info("🎊 SCRAPING COMPLETED!")
         logger.info("=" * 60)
        
         logger.info(" PROJECT SUMMARY:")
         for project, stats in self.stats['projects'].items():
             logger.info(f"   {project}: {stats['processed']}/{stats['total']} "
                f"({stats['success_rate']:.1%})")
        
         logger.info(" OVERALL STATISTICS:")
         logger.info(f"   Total issues found:    {self.stats['total_issues_found']}")
         logger.info(f"   Total issues processed: {self.stats['total_issues_processed']}")
         logger.info(f"   Successful requests:    {self.stats['successful']}")
         logger.info(f"   Failed requests:        {self.stats['failed']}")
         logger.info(f"   Success rate:          {self.stats['successful']/max(self.stats['successful'] + self.stats['failed'], 1):.1%}")
         logger.info(f"   Duration:              {duration:.2f} seconds")
         logger.info(f"   Rate:                  {self.stats['successful']/duration:.2f} issues/second")
         logger.info("=" * 60)

async def main():
    """Main entry point with error handling"""
    try:
        # Configuration
        config = ScrapperConfig(
            max_concurrent_requests=5,
            request_delay=0.2,
            page_size=100,
            batch_size=50  # Process 50 issues at a time
        )
        
        projects = ['KAFKA', 'SPARK', 'HADOOP']  # Start with small projects
        output_dir = Path('data/raw')
        
        # Create output directory
        output_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(" Starting Apache JIRA Scraper")
        logger.info(f"Output directory: {output_dir}")
        logger.info(f"Configuration: {config}")
        
        async with JiraScrapper(config, output_dir) as scraper:
            await scraper.scrape_projects(projects)
            
    except KeyboardInterrupt:
        logger.info("  Scraping interrupted by user")
    except Exception as e:
        logger.error(f" Fatal error: {e}")
        raise


if __name__ == '__main__':
    asyncio.run(main())