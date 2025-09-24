"""
Monitored downloader that integrates with the monitoring system.
Extends the base downloader with metrics collection.
"""

import time
from typing import Dict, Any
from urllib.parse import urlparse

from .downloader import download_and_store as base_download_and_store
from .monitoring import record_metric, get_metrics_collector
from .utils.logger import get_logger

logger = get_logger("harvest.monitored_downloader")


def download_and_store(url: str, label: str, config: dict) -> Dict[str, Any]:
    """
    Download image with monitoring integration.
    
    Args:
        url: Image URL to download
        label: Label for the image
        config: Configuration dictionary
        
    Returns:
        Dictionary with download result information
    """
    start_time = time.time()
    domain = urlparse(url).netloc
    
    try:
        # Record download attempt
        record_metric("download_attempts", 1, {"domain": domain, "label": label})
        
        # Perform download
        result = base_download_and_store(url, label, config)
        
        # Calculate processing time
        processing_time = time.time() - start_time
        
        # Record metrics based on result
        if result['status'] == 'downloaded':
            record_metric("downloads", 1, {"domain": domain, "label": label})
            record_metric("processing_time", processing_time, {"domain": domain})
            logger.info(f"Download successful: {url} (took {processing_time:.2f}s)")
            
        elif result['status'] == 'duplicate':
            record_metric("duplicates", 1, {"domain": domain, "label": label})
            logger.info(f"Duplicate detected: {url}")
            
        else:
            record_metric("failures", 1, {"domain": domain, "label": label})
            
            # Check for specific error types
            if "429" in result.get('message', '') or "rate limit" in result.get('message', '').lower():
                record_metric("rate_limit_errors", 1, {"domain": domain})
                logger.warning(f"Rate limit error: {url}")
            else:
                logger.error(f"Download failed: {url} - {result.get('message', 'Unknown error')}")
        
        # Update daily stats
        get_metrics_collector().update_daily_stats()
        
        return result
        
    except Exception as e:
        # Record failure
        record_metric("failures", 1, {"domain": domain, "label": label})
        record_metric("processing_time", time.time() - start_time, {"domain": domain})
        
        logger.error(f"Download exception: {url} - {e}")
        
        # Return error result
        return {
            'url': url,
            'label': label,
            'status': 'failed',
            'message': f"Exception: {str(e)}",
            'image_id': None
        }
