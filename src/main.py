import asyncio
import argparse
import logging
from pathlib import Path
from datetime import datetime
import sys

sys.path.insert(0,str(Path(__file__).parent/'src'))

from scrapper import JiraScrapper,ScrapperConfig
from transformer import DataTransformer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('pipeline.log'),logging.StreamHandler()]
)

logger = logging.getLogger(__name__)

class Pipeline:
    """Main Pipeline"""
    def __init__(self,projects:list,output_dir:str='data',max_concurrent:int=5,request_delay:float=0.2):
        self.projects = projects
        self.output_dir = Path(output_dir)
        self.raw_dir = self.output_dir/'raw'
        self.processed_dir =self.output_dir/'processed'

        #creating directories
        self.raw_dir.mkdir(parents=True,exist_ok=True)
        self.processed_dir.mkdir(parents=True,exist_ok=True)

        #configuration
        self.config = ScrapperConfig(
            max_concurrent_requests=max_concurrent,
            request_delay=request_delay
        )
        self.stats = {
            'start_time': None,
            'end_time': None,
            'scraping_duration': 0,
            'transformation_duration': 0,
            'total_issues_scraped': 0,
            'total_examples_created': 0
        }
    
    async def run_scraper(self):
        logger.info("PHASE 1: SCRAPING")
        start = datetime.now()

        async with JiraScrapper(self.config,self.raw_dir) as scraper:
            await scraper.scrape_projects(self.projects)
            self.stats['total_issues_scraped'] = scraper.stats['total_issues_processed']


        self.stats['scraping_duration'] = (datetime.now()-start).total_seconds()
        logger.info(f"Scraping completed in {self.stats['scraping_duration']:.2f} seconds")
        logger.info(f"Issues scraped: {self.stats['total_issues_scraped']}")


    def run_transformer(self):
        "Run the transformer"
        logger.info("PHASE 2: TRANSFORMATION")
        start = datetime.now()
        transformer = DataTransformer()
        transformer.transform_all(self.raw_dir,self.processed_dir)
        self.stats['transformation_duration'] = (datetime.now() - start).total_seconds()
        logger.info(f"Transformation completed in {self.stats['transformation_duration']:.2f} seconds")

        #count output example
        total_example = 0
        for jsonl_file in self.processed_dir.glob('*.jsonl'):
            try:
                with open(jsonl_file,'r',encoding='utf-8') as f:
                    total_example += sum(1 for _ in f)
                logger.info(f"Found {total_example} examples in {jsonl_file.name}")
            except Exception as e:
                logger.error(f"Error counting examples in {jsonl_file}")
        self.stats['total_examples_created'] = total_example
        logger.info(f"Total training examples created: {total_example}")


    async def run(self):
        """Run the complete pipeline"""
        self.stats['start_time'] = datetime.now()
        
        logger.info(" Starting Apache Jira Scraping Pipeline")
        logger.info(f" Target projects: {', '.join(self.projects)}")
        logger.info(f" Output directory: {self.output_dir}")
        logger.info(f" Max concurrent requests: {self.config.max_concurrent_requests}")
        logger.info(f"  Request delay: {self.config.request_delay}s")
        
        try:
            # Phase 1: Scraping
            await self.run_scraper()
            
            # Check if we have data before transformation
            raw_files = list(self.raw_dir.rglob('*.json'))
            if not raw_files:
                logger.error(" No data scraped! Cannot proceed with transformation.")
                return
            
            logger.info(f" Found {len(raw_files)} raw JSON files for transformation")
            
            # Phase 2: Transformation
            self.run_transformer()
            
            # Success
            self.stats['end_time'] = datetime.now()
            self.print_final_report()
            
        except KeyboardInterrupt:
            logger.warning(" Pipeline interrupted by user")
            logger.info(" Progress has been checkpointed. You can resume by running again.")
            sys.exit(1)
        
        except Exception as e:
            logger.error(f" Pipeline failed with error: {e}", exc_info=True)
            sys.exit(1)
    
    def print_final_report(self):
        """Print final statistics report"""
        total_duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        
        print("\n" + "=" * 60)
        print(" PIPELINE COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print(f"\n STATISTICS:")
        print(f"  • Projects processed: {len(self.projects)}")
        print(f"  • Issues scraped: {self.stats['total_issues_scraped']}")
        print(f"  • Training examples created: {self.stats['total_examples_created']}")
        
        if self.stats['total_issues_scraped'] > 0:
            examples_per_issue = self.stats['total_examples_created'] / self.stats['total_issues_scraped']
            print(f"  • Examples per issue: {examples_per_issue:.1f}")
        
        print(f"\n  PERFORMANCE:")
        print(f"  • Scraping time: {self.stats['scraping_duration']:.2f} seconds")
        print(f"  • Transformation time: {self.stats['transformation_duration']:.2f} seconds")
        print(f"  • Total time: {total_duration:.2f} seconds")
        
        if total_duration > 0:
            print(f"  • Average rate: {self.stats['total_issues_scraped']/total_duration:.2f} issues/sec")
        
        print(f"\n OUTPUT FILES:")
        
        output_files = list(self.processed_dir.glob('*.jsonl'))
        if output_files:
            for jsonl_file in output_files:
                size_mb = jsonl_file.stat().st_size / (1024 * 1024)
                print(f"  • {jsonl_file.name} ({size_mb:.2f} MB)")
        else:
            print("  • No output files found!")
        
        print("\n Ready for LLM training!")
        print("=" * 60 + "\n")


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Apache Jira Scraper - Extract and transform issue data for LLM training'
    )
    
    parser.add_argument(
        '--projects',
        type=str,
        required=True,
        help='Comma-separated list of Jira project keys (e.g., KAFKA,SPARK,HADOOP)'
    )
    
    parser.add_argument(
        '--output-dir',
        type=str,
        default='data',
        help='Output directory for all data (default: data)'
    )
    
    parser.add_argument(
        '--max-concurrent',
        type=int,
        default=5,
        help='Maximum concurrent API requests (default: 5)'
    )
    
    parser.add_argument(
        '--request-delay',
        type=float,
        default=0.2,
        help='Delay between requests in seconds (default: 0.2)'
    )
    
    parser.add_argument(
        '--scrape-only',
        action='store_true',
        help='Only run scraping phase'
    )
    
    parser.add_argument(
        '--transform-only',
        action='store_true',
        help='Only run transformation phase (requires existing scraped data)'
    )
    
    return parser.parse_args()


async def main():
    """Main entry point"""
    args = parse_args()
    
    # Parse projects
    projects = [p.strip().upper() for p in args.projects.split(',')]
    
    if not projects:
        logger.error(" No projects specified!")
        sys.exit(1)
    
    # Validate project format (basic check)
    for project in projects:
        if not project.isalpha():
            logger.warning(f" Project key '{project}' may be invalid")
    
    # Create pipeline
    pipeline = Pipeline(
        projects=projects,
        output_dir=args.output_dir,
        max_concurrent=args.max_concurrent,
        request_delay=args.request_delay
    )
    
    # Run pipeline
    if args.scrape_only:
        logger.info(" Running SCRAPE-ONLY mode")
        await pipeline.run_scraper()
    elif args.transform_only:
        logger.info(" Running TRANSFORM-ONLY mode")
        pipeline.run_transformer()
    else:
        logger.info("Running FULL PIPELINE mode")
        await pipeline.run()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n Interrupted by user. Exiting...")
        sys.exit(0)
    except Exception as e:
        print(f"\n\n Fatal error: {e}")
        sys.exit(1)
