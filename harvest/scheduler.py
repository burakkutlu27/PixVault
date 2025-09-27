#!/usr/bin/env python3
"""
Scheduler module for automated image harvesting using APScheduler.
"""

import time
from typing import Dict, Any
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from .config import load_config, validate_config
from .adapters.unsplash import search_unsplash
from .downloader import download_and_store
from .db import init_db
from .utils.logger import get_logger


class HarvestScheduler:
    """Scheduler for automated image harvesting."""
    
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize the scheduler with configuration."""
        self.config = load_config(config_path)
        self.logger = get_logger("harvest.scheduler")
        self.config_path = config_path
        
        # Validate configuration
        self.logger.info("Validating configuration for scheduler...")
        validate_config(self.config)
        
        self.scheduler = BackgroundScheduler()
        
    def _harvest_job(self, query: str = "nature", limit: int = 10, label: str = "scheduled"):
        """
        Job function to be executed by the scheduler.
        
        Args:
            query: Search query for images
            limit: Maximum number of images to download
            label: Label to assign to downloaded images
        """
        self.logger.info(f"Starting scheduled harvest: query='{query}', limit={limit}, label='{label}'")
        
        try:
            # Initialize database
            db_path = self.config.get('database', {}).get('path', 'db/images.db')
            init_db(db_path)
            
            # Get API key from config
            api_key = self.config.get('apis', {}).get('unsplash', {}).get('access_key')
            if not api_key:
                self.logger.error("Unsplash API key not found in configuration")
                return
            
            # Search Unsplash
            url_list = search_unsplash(query, limit, api_key)
            
            if not url_list:
                self.logger.warning("No images found for the given query")
                return
            
            self.logger.info(f"Found {len(url_list)} images from Unsplash")
            
            # Download and store images
            results = {
                'downloaded': 0,
                'duplicates': 0,
                'failed': 0,
                'errors': []
            }
            
            for image_data in url_list:
                try:
                    result = download_and_store(image_data['url'], label, self.config)
                    
                    if result['status'] == 'downloaded':
                        results['downloaded'] += 1
                    elif result['status'] == 'duplicate':
                        results['duplicates'] += 1
                    else:
                        results['failed'] += 1
                        results['errors'].append({
                            'url': image_data['url'],
                            'error': result['message']
                        })
                        
                except Exception as e:
                    results['failed'] += 1
                    results['errors'].append({
                        'url': image_data['url'],
                        'error': str(e)
                    })
                    self.logger.error(f"Error processing {image_data['url']}: {e}")
            
            # Log summary
            self.logger.info(f"Scheduled harvest completed: {results['downloaded']} downloaded, "
                           f"{results['duplicates']} duplicates, {results['failed']} failed")
            
        except Exception as e:
            self.logger.error(f"Error in scheduled harvest: {e}")
    
    def start_scheduler(self, query: str = "nature", limit: int = 10, label: str = "scheduled"):
        """
        Start the scheduler with the specified interval.
        
        Args:
            query: Search query for images
            limit: Maximum number of images to download
            label: Label to assign to downloaded images
        """
        # Get interval from config
        interval_hours = self.config.get('scheduler', {}).get('interval_hours', 24)
        
        # Add job to scheduler
        self.scheduler.add_job(
            func=self._harvest_job,
            trigger=IntervalTrigger(hours=interval_hours),
            args=[query, limit, label],
            id='harvest_job',
            name='Automated Image Harvest',
            replace_existing=True
        )
        
        # Start scheduler
        self.scheduler.start()
        self.logger.info(f"Scheduler started, interval={interval_hours} hours")
        
        return self.scheduler
    
    def stop_scheduler(self):
        """Stop the scheduler."""
        if self.scheduler.running:
            self.scheduler.shutdown()
            self.logger.info("Scheduler stopped")
    
    def run_once(self, query: str = "nature", limit: int = 10, label: str = "once"):
        """
        Run the harvest job once without scheduling.
        
        Args:
            query: Search query for images
            limit: Maximum number of images to download
            label: Label to assign to downloaded images
        """
        self.logger.info("Running harvest once (no scheduler)")
        self._harvest_job(query, limit, label)
    
    def is_running(self) -> bool:
        """Check if the scheduler is running."""
        return self.scheduler.running


def create_scheduler(config_path: str = "config.yaml") -> HarvestScheduler:
    """Create a new scheduler instance."""
    return HarvestScheduler(config_path)
