#!/usr/bin/env python3
"""
Test script for the scheduler functionality.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '.'))

from harvest.scheduler import create_scheduler
from harvest.utils.logger import get_logger

def test_scheduler():
    """Test the scheduler functionality."""
    logger = get_logger("test_scheduler")
    
    try:
        # Create scheduler instance
        logger.info("Creating scheduler instance...")
        scheduler = create_scheduler("config.yaml")
        
        # Test run_once method
        logger.info("Testing run_once method...")
        scheduler.run_once("nature", 5, "test")
        
        logger.info("Scheduler test completed successfully!")
        
    except Exception as e:
        logger.error(f"Scheduler test failed: {e}")
        return False
    
    return True

if __name__ == "__main__":
    success = test_scheduler()
    sys.exit(0 if success else 1)
