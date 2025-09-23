"""
Celery tasks for distributed image downloading and processing.
Provides task queue functionality with rate limiting and retry mechanisms.
"""

import time
import random
from typing import Dict, Any, Optional
from urllib.parse import urlparse
from celery import Celery
from celery.exceptions import Retry
import redis
from kombu import Queue

from .downloader import download_and_store
from .adapters.browser import fetch_images_sync
from .db import Database
from .config import load_config
from .utils.logger import get_logger

# Configure Celery
app = Celery('harvest')

# Celery configuration
app.conf.update(
    broker_url='redis://localhost:6379/0',
    result_backend='redis://localhost:6379/0',
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=300,  # 5 minutes
    task_soft_time_limit=240,  # 4 minutes
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    worker_disable_rate_limits=False,
    task_routes={
        'harvest.tasks.download_image_task': {'queue': 'download'},
        'harvest.tasks.browser_search_task': {'queue': 'browser'},
    },
    task_default_queue='default',
    task_queues=(
        Queue('default', routing_key='default'),
        Queue('download', routing_key='download'),
        Queue('browser', routing_key='browser'),
    ),
    task_default_exchange='default',
    task_default_exchange_type='direct',
    task_default_routing_key='default',
)

# Redis connection for rate limiting
redis_client = redis.Redis(host='localhost', port=6379, db=1, decode_responses=True)

logger = get_logger("harvest.tasks")


class RateLimiter:
    """Rate limiter for domain-based request limiting."""
    
    def __init__(self, redis_client, default_limit: int = 1, default_window: int = 5):
        self.redis = redis_client
        self.default_limit = default_limit
        self.default_window = default_window
    
    def is_allowed(self, domain: str, limit: int = None, window: int = None) -> bool:
        """
        Check if request is allowed for domain.
        
        Args:
            domain: Domain to check
            limit: Request limit per window (default: self.default_limit)
            window: Time window in seconds (default: self.default_window)
            
        Returns:
            True if request is allowed, False otherwise
        """
        limit = limit or self.default_limit
        window = window or self.default_window
        
        key = f"rate_limit:{domain}"
        current_time = int(time.time())
        window_start = current_time - window
        
        # Clean old entries
        self.redis.zremrangebyscore(key, 0, window_start)
        
        # Count requests in current window
        current_requests = self.redis.zcard(key)
        
        if current_requests < limit:
            # Add current request
            self.redis.zadd(key, {str(current_time): current_time})
            self.redis.expire(key, window)
            return True
        
        return False
    
    def get_wait_time(self, domain: str, limit: int = None, window: int = None) -> int:
        """Get wait time in seconds until next request is allowed."""
        limit = limit or self.default_limit
        window = window or self.default_window
        
        key = f"rate_limit:{domain}"
        current_time = int(time.time())
        window_start = current_time - window
        
        # Get oldest request in window
        oldest_requests = self.redis.zrangebyscore(key, window_start, current_time, start=0, num=1)
        
        if oldest_requests:
            oldest_time = int(oldest_requests[0])
            wait_time = (oldest_time + window) - current_time
            return max(0, wait_time)
        
        return 0


# Global rate limiter instance
rate_limiter = RateLimiter(redis_client)


@app.task(bind=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 3, 'countdown': 60})
def download_image_task(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Download image task with rate limiting and retry logic.
    
    Args:
        task_data: Dictionary containing:
            - url: Image URL to download
            - label: Label for the image
            - config: Configuration dictionary
            - domain: Domain for rate limiting (optional)
            - priority: Task priority (optional)
    
    Returns:
        Dictionary with download result
    """
    url = task_data.get('url')
    label = task_data.get('label')
    config = task_data.get('config', {})
    domain = task_data.get('domain')
    priority = task_data.get('priority', 0)
    
    if not url or not label:
        raise ValueError("URL and label are required")
    
    # Extract domain from URL if not provided
    if not domain:
        parsed_url = urlparse(url)
        domain = parsed_url.netloc
    
    # Apply rate limiting
    if not rate_limiter.is_allowed(domain):
        wait_time = rate_limiter.get_wait_time(domain)
        logger.info(f"Rate limited for domain {domain}, waiting {wait_time}s")
        
        # Retry with delay
        raise self.retry(countdown=wait_time, max_retries=5)
    
    # Add random jitter to avoid thundering herd
    jitter = random.uniform(0.5, 2.0)
    time.sleep(jitter)
    
    try:
        logger.info(f"Starting download task for {url} (attempt {self.request.retries + 1})")
        
        # Perform download
        result = download_and_store(url, label, config)
        
        # Log result
        if result['status'] == 'downloaded':
            logger.info(f"Successfully downloaded {url}")
        elif result['status'] == 'duplicate':
            logger.info(f"Duplicate image found: {url}")
        else:
            logger.warning(f"Download failed for {url}: {result['message']}")
        
        return result
        
    except Exception as e:
        logger.error(f"Download task failed for {url}: {e}")
        
        # Determine if we should retry
        if self.request.retries < self.max_retries:
            # Exponential backoff with jitter
            countdown = (2 ** self.request.retries) * 60 + random.uniform(0, 30)
            logger.info(f"Retrying download for {url} in {countdown:.1f}s")
            raise self.retry(countdown=countdown)
        else:
            logger.error(f"Max retries exceeded for {url}")
            return {
                'url': url,
                'label': label,
                'status': 'failed',
                'message': f"Max retries exceeded: {str(e)}",
                'image_id': None
            }


@app.task(bind=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 2, 'countdown': 30})
def browser_search_task(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Browser search task for finding images using browser adapter.
    
    Args:
        task_data: Dictionary containing:
            - query: Search query
            - max_results: Maximum number of results
            - label: Label for found images
            - config: Configuration dictionary
    
    Returns:
        Dictionary with search results
    """
    query = task_data.get('query')
    max_results = task_data.get('max_results', 50)
    label = task_data.get('label')
    config = task_data.get('config', {})
    
    if not query or not label:
        raise ValueError("Query and label are required")
    
    try:
        logger.info(f"Starting browser search for: {query}")
        
        # Perform browser search
        results = fetch_images_sync(query, max_results)
        
        if not results:
            logger.warning(f"No images found for query: {query}")
            return {
                'query': query,
                'status': 'no_results',
                'message': 'No images found',
                'results': []
            }
        
        logger.info(f"Found {len(results)} images for query: {query}")
        
        # Enqueue download tasks for found images
        download_tasks = []
        for result in results:
            download_task_data = {
                'url': result['url'],
                'label': label,
                'config': config,
                'domain': urlparse(result['url']).netloc,
                'source': result['source'],
                'priority': 1  # Higher priority for browser results
            }
            
            # Enqueue download task
            download_task = download_image_task.delay(download_task_data)
            download_tasks.append(download_task.id)
        
        return {
            'query': query,
            'status': 'success',
            'message': f'Found {len(results)} images, enqueued {len(download_tasks)} download tasks',
            'results': results,
            'download_tasks': download_tasks
        }
        
    except Exception as e:
        logger.error(f"Browser search task failed for {query}: {e}")
        
        if self.request.retries < self.max_retries:
            countdown = (2 ** self.request.retries) * 30 + random.uniform(0, 10)
            logger.info(f"Retrying browser search for {query} in {countdown:.1f}s")
            raise self.retry(countdown=countdown)
        else:
            logger.error(f"Max retries exceeded for browser search: {query}")
            return {
                'query': query,
                'status': 'failed',
                'message': f"Max retries exceeded: {str(e)}",
                'results': []
            }


@app.task
def cleanup_old_tasks():
    """Cleanup old completed tasks from Redis."""
    try:
        # Clean up old task results (older than 24 hours)
        cutoff_time = int(time.time()) - (24 * 60 * 60)
        
        # This would need to be implemented based on your specific cleanup needs
        logger.info("Task cleanup completed")
        return {'status': 'success', 'cleaned_tasks': 0}
        
    except Exception as e:
        logger.error(f"Task cleanup failed: {e}")
        return {'status': 'failed', 'error': str(e)}


@app.task
def health_check():
    """Health check task to monitor worker status."""
    try:
        # Check Redis connection
        redis_client.ping()
        
        # Check database connection
        config = load_config()
        db_path = config.get('database', {}).get('path', 'db/images.db')
        database = Database(db_path)
        
        # Get basic stats
        all_images = database.get_all_images()
        
        return {
            'status': 'healthy',
            'redis_connected': True,
            'database_connected': True,
            'total_images': len(all_images),
            'timestamp': time.time()
        }
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            'status': 'unhealthy',
            'error': str(e),
            'timestamp': time.time()
        }


# Task management functions
def enqueue_download(task_data: Dict[str, Any], priority: int = 0) -> str:
    """
    Enqueue a download task.
    
    Args:
        task_data: Task data dictionary
        priority: Task priority (0 = normal, 1 = high)
        
    Returns:
        Task ID
    """
    task = download_image_task.delay(task_data)
    return task.id


def enqueue_browser_search(task_data: Dict[str, Any]) -> str:
    """
    Enqueue a browser search task.
    
    Args:
        task_data: Task data dictionary
        
    Returns:
        Task ID
    """
    task = browser_search_task.delay(task_data)
    return task.id


def get_task_status(task_id: str) -> Dict[str, Any]:
    """
    Get task status and result.
    
    Args:
        task_id: Task ID
        
    Returns:
        Task status dictionary
    """
    try:
        task_result = app.AsyncResult(task_id)
        
        return {
            'task_id': task_id,
            'status': task_result.status,
            'result': task_result.result if task_result.ready() else None,
            'successful': task_result.successful() if task_result.ready() else None,
            'failed': task_result.failed() if task_result.ready() else None
        }
    except Exception as e:
        return {
            'task_id': task_id,
            'status': 'UNKNOWN',
            'error': str(e)
        }


def cancel_task(task_id: str) -> bool:
    """
    Cancel a task.
    
    Args:
        task_id: Task ID to cancel
        
    Returns:
        True if cancelled successfully
    """
    try:
        app.control.revoke(task_id, terminate=True)
        return True
    except Exception as e:
        logger.error(f"Failed to cancel task {task_id}: {e}")
        return False


def get_queue_stats() -> Dict[str, Any]:
    """
    Get queue statistics.
    
    Returns:
        Queue statistics dictionary
    """
    try:
        inspect = app.control.inspect()
        
        # Get active tasks
        active_tasks = inspect.active()
        
        # Get scheduled tasks
        scheduled_tasks = inspect.scheduled()
        
        # Get reserved tasks
        reserved_tasks = inspect.reserved()
        
        return {
            'active_tasks': active_tasks or {},
            'scheduled_tasks': scheduled_tasks or {},
            'reserved_tasks': reserved_tasks or {},
            'timestamp': time.time()
        }
    except Exception as e:
        logger.error(f"Failed to get queue stats: {e}")
        return {'error': str(e)}


# Rate limiting configuration per domain
DOMAIN_RATE_LIMITS = {
    'unsplash.com': {'limit': 1, 'window': 5},
    'pixabay.com': {'limit': 1, 'window': 10},
    'pexels.com': {'limit': 1, 'window': 8},
    'google.com': {'limit': 1, 'window': 15},
    'bing.com': {'limit': 1, 'window': 12},
    'default': {'limit': 1, 'window': 5}
}


def get_domain_rate_limit(domain: str) -> Dict[str, int]:
    """Get rate limit configuration for domain."""
    return DOMAIN_RATE_LIMITS.get(domain, DOMAIN_RATE_LIMITS['default'])
