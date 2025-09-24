"""
Monitored Celery tasks that integrate with the monitoring system.
Extends the base tasks with metrics collection.
"""

import time
from typing import Dict, Any
from celery import Celery

from .tasks import download_image_task as base_download_image_task, browser_search_task as base_browser_search_task
from .monitoring import record_metric, get_metrics_collector
from .utils.logger import get_logger

logger = get_logger("harvest.monitored_tasks")


def download_image_task(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Monitored download image task with metrics collection.
    
    Args:
        task_data: Task data dictionary
        
    Returns:
        Dictionary with download result
    """
    start_time = time.time()
    url = task_data.get('url')
    label = task_data.get('label')
    domain = task_data.get('domain', 'unknown')
    
    try:
        # Record task start
        record_metric("task_starts", 1, {"task_type": "download", "domain": domain})
        
        # Perform base task
        result = base_download_image_task(self, task_data)
        
        # Calculate processing time
        processing_time = time.time() - start_time
        
        # Record metrics based on result
        if result.get('status') == 'downloaded':
            record_metric("task_successes", 1, {"task_type": "download", "domain": domain})
            record_metric("task_processing_time", processing_time, {"task_type": "download"})
            
        elif result.get('status') == 'duplicate':
            record_metric("task_duplicates", 1, {"task_type": "download", "domain": domain})
            
        else:
            record_metric("task_failures", 1, {"task_type": "download", "domain": domain})
            
            # Check for specific error types
            if "429" in result.get('message', '') or "rate limit" in result.get('message', '').lower():
                record_metric("task_rate_limit_errors", 1, {"task_type": "download", "domain": domain})
        
        # Update daily stats
        get_metrics_collector().update_daily_stats()
        
        return result
        
    except Exception as e:
        # Record failure
        record_metric("task_failures", 1, {"task_type": "download", "domain": domain})
        record_metric("task_processing_time", time.time() - start_time, {"task_type": "download"})
        
        logger.error(f"Task exception: {url} - {e}")
        
        # Return error result
        return {
            'url': url,
            'label': label,
            'status': 'failed',
            'message': f"Task exception: {str(e)}",
            'image_id': None
        }


def browser_search_task(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Monitored browser search task with metrics collection.
    
    Args:
        task_data: Task data dictionary
        
    Returns:
        Dictionary with search results
    """
    start_time = time.time()
    query = task_data.get('query')
    max_results = task_data.get('max_results', 50)
    
    try:
        # Record task start
        record_metric("task_starts", 1, {"task_type": "browser_search", "query": query[:20]})
        
        # Perform base task
        result = base_browser_search_task(self, task_data)
        
        # Calculate processing time
        processing_time = time.time() - start_time
        
        # Record metrics based on result
        if result.get('status') == 'success':
            record_metric("task_successes", 1, {"task_type": "browser_search"})
            record_metric("task_processing_time", processing_time, {"task_type": "browser_search"})
            
            # Record search results
            results_count = len(result.get('results', []))
            record_metric("search_results", results_count, {"query": query[:20]})
            
            # Record download tasks created
            download_tasks = result.get('download_tasks', [])
            record_metric("download_tasks_created", len(download_tasks), {"query": query[:20]})
            
        else:
            record_metric("task_failures", 1, {"task_type": "browser_search"})
        
        # Update daily stats
        get_metrics_collector().update_daily_stats()
        
        return result
        
    except Exception as e:
        # Record failure
        record_metric("task_failures", 1, {"task_type": "browser_search"})
        record_metric("task_processing_time", time.time() - start_time, {"task_type": "browser_search"})
        
        logger.error(f"Browser search task exception: {query} - {e}")
        
        # Return error result
        return {
            'query': query,
            'status': 'failed',
            'message': f"Task exception: {str(e)}",
            'results': []
        }


def health_check_task():
    """Health check task with metrics collection."""
    try:
        start_time = time.time()
        
        # Record health check
        record_metric("health_checks", 1, {"status": "started"})
        
        # Perform health check (simplified)
        health_status = {
            'status': 'healthy',
            'timestamp': time.time(),
            'processing_time': time.time() - start_time
        }
        
        # Record success
        record_metric("health_checks", 1, {"status": "success"})
        record_metric("health_check_processing_time", time.time() - start_time)
        
        return health_status
        
    except Exception as e:
        # Record failure
        record_metric("health_checks", 1, {"status": "failed"})
        
        logger.error(f"Health check failed: {e}")
        return {
            'status': 'unhealthy',
            'error': str(e),
            'timestamp': time.time()
        }


def cleanup_old_tasks_task():
    """Cleanup old tasks with metrics collection."""
    try:
        start_time = time.time()
        
        # Record cleanup start
        record_metric("cleanup_tasks", 1, {"status": "started"})
        
        # Perform cleanup (simplified)
        cleaned_tasks = 0  # This would be implemented based on your cleanup logic
        
        # Record success
        record_metric("cleanup_tasks", 1, {"status": "success"})
        record_metric("cleanup_processing_time", time.time() - start_time)
        record_metric("cleaned_tasks", cleaned_tasks)
        
        return {
            'status': 'success',
            'cleaned_tasks': cleaned_tasks,
            'processing_time': time.time() - start_time
        }
        
    except Exception as e:
        # Record failure
        record_metric("cleanup_tasks", 1, {"status": "failed"})
        
        logger.error(f"Cleanup failed: {e}")
        return {
            'status': 'failed',
            'error': str(e)
        }
