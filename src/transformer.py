import json
import re
from pathlib import Path
from typing import Dict,List,Optional,Any,Iterator
from dataclasses import dataclass,asdict
from bs4 import BeautifulSoup
import logging
import asyncio
from concurrent.futures import ThreadPoolExecutor
import hashlib

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class TransformedIssue:
    """Structured Issue data from LLM Training"""
    issue_key: str
    project: str
    summary: str
    description: str
    status: str
    priority: str
    reporter: Optional[str]
    assignee: Optional[str]
    created: str
    updated: str
    resolution_date: Optional[str]
    labels: List[str]
    components: List[str]
    comments: List[Dict[str, str]]
    text_hash: str

    def to_dict(self)->Dict:
        """Convert to Dictionary"""
        return asdict(self)
    
class DataTransformer:
    """Optimized transformer for Jira data"""
    def __init__(self,max_workers:int=4):
        self.max_workers = max_workers
        self.stats = {
            'total_processed': 0,
            'total_examples': 0,
            'validation_failures': 0,
            'empty_descriptions': 0,
            'duplicates_skipped': 0,
            'file_errors': 0
        }
        self.seen_hashes = set()

    @staticmethod
    def clean_text(text:Optional[str],preserve_code:bool=True)->str:
        """Clean HTML while preserving important technical content"""
        if not text:
            return ""
        try:
            soup = BeautifulSoup(text,'lxml' if preserve_code else 'html.parser')
            #remove script and style
            for element in soup(['script','style']):
                element.decompose()

            if preserve_code:
                for code in soup.find_all('code'):
                    code_content = code.get_text()
                    if len(code_content.split()) > 2:
                        code.replace_with(f"CODE:{code_content}")
                    else:
                        code.replace_with(code_content)
                    
            text = soup.get_text()
            text = re.sub(r'[ \t]+', ' ', text)  # Normalize spaces
            text = re.sub(r'\n{3,}', '\n\n', text)  # Limit consecutive newlines
            text = re.sub(r'[ \t]+\n', '\n', text)  # Remove trailing spaces

            text = text.strip()
            return text
        except Exception as e:
            logger.warning(f"HTML parsing failed, using fallback: {e}")
            text = re.sub(r'<[^>]+>', '', text)  # Remove all tags
            text = re.sub(r'\s+', ' ', text)  # Normalize whitespace
            return text.strip()
        
    def calculate_text_hash(self, text: str) -> str:
        """Calculate hash for deduplication"""
        return hashlib.md5(text.encode('utf-8')).hexdigest()
    
    def extract_metadata(self, raw_issue: Dict) -> Dict[str, Any]:
        """
        Extract and clean metadata from raw issue with error handling
        """
        try:
            fields = raw_issue.get('fields', {})
            
            # Clean text fields
            summary = self.clean_text(fields.get('summary', ''), preserve_code=False)
            description = self.clean_text(fields.get('description'), preserve_code=True)
            
            # Extract comments with cleaning
            comments = []
            comment_data = fields.get('comment', {})
            raw_comments = comment_data.get('comments', [])
            
            for comment in raw_comments:
                comment_body = self.clean_text(comment.get('body'), preserve_code=True)
                if comment_body and len(comment_body.strip()) > 10:  # Minimum comment length
                    comments.append({
                        'author': comment.get('author', {}).get('displayName', 'Unknown'),
                        'created': comment.get('created', ''),
                        'body': comment_body
                    })
            
            # Calculate content hash for deduplication
            content_for_hash = f"{summary} {description} {' '.join(c['body'] for c in comments)}"
            text_hash = self.calculate_text_hash(content_for_hash)
            
            metadata = {
                'issue_key': raw_issue.get('key', ''),
                'project': raw_issue.get('key', '').split('-')[0] if raw_issue.get('key') else '',
                'summary': summary,
                'description': description,
                'status': fields.get('status', {}).get('name', 'Unknown'),
                'priority': fields.get('priority', {}).get('name', 'Unknown'),
                'reporter': fields.get('reporter', {}).get('displayName') if fields.get('reporter') else None,
                'assignee': fields.get('assignee', {}).get('displayName') if fields.get('assignee') else None,
                'created': fields.get('created', ''),
                'updated': fields.get('updated', ''),
                'resolution_date': fields.get('resolutiondate'),
                'labels': fields.get('labels', []),
                'components': [c.get('name', '') for c in fields.get('components', [])],
                'comments': comments,
                'text_hash': text_hash
            }
            
            return metadata
            
        except Exception as e:
            logger.error(f"Error extracting metadata: {e}")
            return {}
        
    def validate_issue(self, issue: TransformedIssue) -> bool:
        """
        Enhanced validation with quality scoring
        """
        if not issue.issue_key or not issue.summary:
            logger.debug(f"Missing required fields: {issue.issue_key}")
            self.stats['validation_failures'] += 1
            return False
        
        # Check for duplicates
        if issue.text_hash in self.seen_hashes:
            logger.debug(f"Duplicate content skipped: {issue.issue_key}")
            self.stats['duplicates_skipped'] += 1
            return False
        
        # Quality checks
        total_content = f"{issue.summary} {issue.description}"
        
        # Must have sufficient content
        if len(total_content.strip()) < 25:
            logger.debug(f"Insufficient content: {issue.issue_key}")
            self.stats['validation_failures'] += 1
            return False
        
        # Check for meaningful text (not just placeholders)
        placeholder_patterns = [
            r'^\s*$',  # Empty
            r'^N/A$', r'^na$', r'^none$',  # Common placeholders
            r'^\.+$',  # Just dots
        ]
        
        for pattern in placeholder_patterns:
            if re.match(pattern, issue.description, re.IGNORECASE):
                logger.debug(f"Placeholder content: {issue.issue_key}")
                self.stats['validation_failures'] += 1
                return False
        
        self.seen_hashes.add(issue.text_hash)
        return True
    

    def create_training_examples(self, issue: TransformedIssue) -> Iterator[Dict]:
        """
        Generate training examples as iterator to save memory
        """
        # Combine main content
        main_content = f"Title: {issue.summary}\n\nDescription: {issue.description}"
        
        # Add first few comments if they exist
        if issue.comments:
            comment_text = "\n\nComments:\n" + "\n".join(
                f"- {c['body']}" for c in issue.comments[:2]  # Limit comments
            )
            full_content = main_content + comment_text
        else:
            full_content = main_content
        
        # 1. Summarization task
        yield {
            'task_type': 'summarization',
            'instruction': 'Provide a concise summary of this software issue:',
            'input': self._truncate_text(full_content, 1500),
            'output': issue.summary,
            'metadata': {
                'issue_key': issue.issue_key,
                'project': issue.project,
                'status': issue.status,
                'priority': issue.priority,
                'example_type': 'summarization'
            }
        }
        
        # 2. Priority classification
        yield {
            'task_type': 'classification',
            'instruction': 'Classify the priority level of this issue:',
            'input': self._truncate_text(main_content, 1000),
            'output': issue.priority,
            'metadata': {
                'issue_key': issue.issue_key,
                'project': issue.project,
                'example_type': 'priority_classification'
            }
        }
        
        # 3. Status classification  
        yield {
            'task_type': 'classification',
            'instruction': 'Determine the appropriate status for this issue:',
            'input': self._truncate_text(main_content, 1000),
            'output': issue.status,
            'metadata': {
                'issue_key': issue.issue_key,
                'project': issue.project,
                'example_type': 'status_classification'
            }
        }
        
        # 4. Component prediction (if components exist)
        if issue.components:
            yield {
                'task_type': 'multi_label_classification',
                'instruction': 'Identify which software components this issue affects:',
                'input': self._truncate_text(main_content, 1000),
                'output': ', '.join(issue.components),
                'metadata': {
                    'issue_key': issue.issue_key,
                    'project': issue.project,
                    'example_type': 'component_prediction'
                }
            }
        
        # 5. Q&A from comments (if substantial comments exist)
        if len(issue.comments) >= 1 and len(issue.comments[0]['body']) > 50:
            yield {
                'task_type': 'question_answering',
                'instruction': f'Based on this {issue.project} issue, what was the response or solution?',
                'input': self._truncate_text(main_content, 1200),
                'output': issue.comments[0]['body'],
                'metadata': {
                    'issue_key': issue.issue_key,
                    'project': issue.project,
                    'example_type': 'qa_from_comments'
                }
            }
    
    def _truncate_text(self, text: str, max_length: int) -> str:
        """Smart text truncation that preserves sentences"""
        if len(text) <= max_length:
            return text
        
        # Try to truncate at sentence boundary
        truncated = text[:max_length]
        last_period = truncated.rfind('.')
        last_newline = truncated.rfind('\n')
        
        cutoff = max(last_period, last_newline)
        if cutoff > max_length * 0.7:  # Only use if we keep most content
            return truncated[:cutoff + 1] + " [truncated]"
        else:
            return truncated + " [truncated]"
        

    def process_single_file(self, json_file: Path) -> Iterator[Dict]:
        """
        Process a single JSON file and yield training examples
        """
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                raw_issue = json.load(f)
            
            # Extract metadata
            metadata = self.extract_metadata(raw_issue)
            if not metadata:
                return
            
            # Create transformed issue
            issue = TransformedIssue(**metadata)
            
            # Validate
            if not self.validate_issue(issue):
                return
            
            # Generate and yield examples
            for example in self.create_training_examples(issue):
                yield example
                
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in {json_file}: {e}")
            self.stats['file_errors'] += 1
        except Exception as e:
            logger.error(f"Error processing {json_file}: {e}")
            self.stats['file_errors'] += 1


    def process_project(self, project_dir: Path, output_file: Path) -> int:
        """
        Process project with checkpointing and parallel processing
        """
        logger.info(f"🔄 Processing project: {project_dir.name}")
        
        # Load checkpoint
        checkpoint_file = output_file.with_suffix('.checkpoint')
        processed_files = self._load_checkpoint(checkpoint_file)
        
        # Get files to process
        all_json_files = list(project_dir.glob('*.json'))
        all_json_files = [f for f in all_json_files if f.name != 'checkpoint.json']
        pending_files = [f for f in all_json_files if f.name not in processed_files]
        
        logger.info(f"📁 Found {len(pending_files)}/{len(all_json_files)} files to process")
        
        if not pending_files:
            logger.info(f"✅ No new files to process for {project_dir.name}")
            return 0
        
        examples_count = 0
        batch_size = 50
        
        with open(output_file, 'a', encoding='utf-8') as outf:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Process files in parallel batches
                for i in range(0, len(pending_files), batch_size):
                    batch = pending_files[i:i + batch_size]
                    
                    logger.info(f"📦 Processing batch {i//batch_size + 1}: "
                               f"files {i}-{i+len(batch)}")
                    
                    # Process batch
                    for json_file in batch:
                        try:
                            for example in self.process_single_file(json_file):
                                outf.write(json.dumps(example, ensure_ascii=False) + '\n')
                                outf.flush()  # Ensure data is written
                                examples_count += 1
                            
                            # Mark as processed
                            processed_files.add(json_file.name)
                            
                        except Exception as e:
                            logger.error(f"Failed to process {json_file}: {e}")
                    
                    # Save checkpoint after each batch
                    self._save_checkpoint(checkpoint_file, processed_files)
                    
                    # Update stats
                    self.stats['total_processed'] += len(batch)
                    self.stats['total_examples'] = examples_count
                    
                    logger.info(f"📊 Batch complete: {examples_count} examples generated")
        
        # Clean up checkpoint on success
        if checkpoint_file.exists():
            checkpoint_file.unlink()
        
        logger.info(f"✅ Completed {project_dir.name}: {examples_count} examples")
        return examples_count
    
    def _load_checkpoint(self, checkpoint_file: Path) -> set:
        """Load processed files from checkpoint"""
        if checkpoint_file.exists():
            try:
                with open(checkpoint_file, 'r') as f:
                    return set(json.load(f))
            except Exception as e:
                logger.warning(f"Failed to load checkpoint: {e}")
        return set()
    
    def _save_checkpoint(self, checkpoint_file: Path, processed_files: set):
        """Save checkpoint"""
        try:
            with open(checkpoint_file, 'w') as f:
                json.dump(list(processed_files), f)
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")


    def transform_all(self, raw_data_dir: Path, output_dir: Path) -> None:
        """
        Transform all projects with comprehensive reporting
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        
        total_examples = 0
        project_dirs = [d for d in raw_data_dir.iterdir() if d.is_dir()]
        
        logger.info(f"🎯 Starting transformation of {len(project_dirs)} projects")
        
        for project_dir in project_dirs:
            output_file = output_dir / f"{project_dir.name}_training.jsonl"
            
            # Clear previous output if no checkpoint exists
            if not output_file.with_suffix('.checkpoint').exists():
                output_file.write_text('')  # Clear file
            
            count = self.process_project(project_dir, output_file)
            total_examples += count
        
        # Final report
        self._print_final_report(total_examples)


    
    def _print_final_report(self, total_examples: int):
        """Print comprehensive transformation report"""
        logger.info("=" * 60)
        logger.info(" TRANSFORMATION COMPLETED!")
        logger.info("=" * 60)
        
        logger.info("📊 TRANSFORMATION STATISTICS:")
        logger.info(f"   Total issues processed:    {self.stats['total_processed']}")
        logger.info(f"   Total examples generated:  {self.stats['total_examples']}")
        logger.info(f"   Validation failures:       {self.stats['validation_failures']}")
        logger.info(f"   Empty descriptions:        {self.stats['empty_descriptions']}")
        logger.info(f"   Duplicates skipped:        {self.stats['duplicates_skipped']}")
        logger.info(f"   File errors:               {self.stats['file_errors']}")
        
        if self.stats['total_processed'] > 0:
            success_rate = (self.stats['total_processed'] - self.stats['validation_failures']) / self.stats['total_processed']
            examples_per_issue = self.stats['total_examples'] / self.stats['total_processed']
            
            logger.info(f"   Success rate:             {success_rate:.1%}")
            logger.info(f"   Examples per issue:       {examples_per_issue:.1f}")
        
        logger.info("=" * 60)



def main():
    """Main entry point with error handling"""
    try:
        # Use multiple workers for parallel processing
        transformer = DataTransformer(max_workers=4)
        
        raw_data_dir = Path('data/raw')
        output_dir = Path('data/processed')
        
        if not raw_data_dir.exists():
            logger.error(f"Raw data directory not found: {raw_data_dir}")
            return
        
        logger.info(" Starting Data Transformation")
        logger.info(f"Input:  {raw_data_dir}")
        logger.info(f" Output: {output_dir}")
        
        transformer.transform_all(raw_data_dir, output_dir)
        
    except KeyboardInterrupt:
        logger.info("⏹️  Transformation interrupted by user")
    except Exception as e:
        logger.error(f" Fatal error during transformation: {e}")
        raise


if __name__ == '__main__':
    main()
        