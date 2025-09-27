#!/usr/bin/env python3
"""
Example usage of the scheduler functionality.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '.'))

from harvest.scheduler import create_scheduler
from harvest.utils.logger import get_logger

def main():
    """Demonstrate scheduler usage."""
    logger = get_logger("example_scheduler")
    
    print("PixVault Scheduler Example")
    print("=" * 40)
    print()
    print("Usage examples:")
    print()
    print("1. Run harvest once (no scheduler):")
    print("   python -m harvest.cli --source unsplash --query 'nature' --limit 10 --label 'test' --once")
    print()
    print("2. Start scheduler for automated harvesting:")
    print("   python -m harvest.cli --source unsplash --query 'nature' --limit 10 --label 'scheduled' --scheduler")
    print()
    print("3. Run normally (original behavior):")
    print("   python -m harvest.cli --source unsplash --query 'nature' --limit 10 --label 'manual'")
    print()
    print("Configuration:")
    print("- Set 'scheduler.interval_hours' in config.yaml to control the interval")
    print("- Default interval is 24 hours")
    print("- Scheduler will log 'Scheduler started, interval=X hours' when started")
    print()

if __name__ == "__main__":
    main()
