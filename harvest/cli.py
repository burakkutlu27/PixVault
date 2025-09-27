#!/usr/bin/env python3
"""
Command-line interface for the harvest package.
"""

import argparse
import sys
import time
from pathlib import Path
from typing import Dict, Any, List

from tqdm import tqdm

from .config import load_config, validate_config
from .db import init_db
from .adapters.unsplash import search_unsplash
from .adapters.browser import fetch_images_sync
from .adapters.manager import get_all_adapters, get_adapter
from .downloader import download_and_store
from .utils.logger import get_logger
from .scheduler import create_scheduler


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Harvest images from various sources",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m harvest.cli --query "nature" --limit 50 --label landscape  # Use all configured sources
  python -m harvest.cli --sources "unsplash,bing" --query "mountains" --limit 20 --label mountains
  python -m harvest.cli --sources "pexels" --query "abstract art" --limit 30 --label abstract
  python -m harvest.cli --query "city skyline" --limit 15 --label city --once
  python -m harvest.cli --query "landscape" --limit 25 --label nature --scheduler
        """
    )
    
    parser.add_argument(
        "--sources",
        required=False,
        help="Comma-separated list of sources to use (e.g., 'unsplash,bing,pexels'). If not specified, uses sources from config.yaml"
    )
    
    parser.add_argument(
        "--query",
        required=True,
        help="Search query for images (e.g., 'dev iş makinesi')"
    )
    
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Maximum number of images to download (default: 50)"
    )
    
    parser.add_argument(
        "--label",
        required=True,
        help="Label to assign to downloaded images (e.g., 'excavator')"
    )
    
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to configuration file (default: config.yaml)"
    )
    
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run harvest once without scheduler (overrides --scheduler)"
    )
    
    parser.add_argument(
        "--scheduler",
        action="store_true",
        help="Start scheduler for automated harvesting"
    )
    
    args = parser.parse_args()
    
    # Initialize logger
    logger = get_logger("harvest.cli")
    
    try:
        # Step 1: Load and validate config.yaml
        logger.info(f"Loading configuration from {args.config}")
        config = load_config(args.config)
        
        # Validate configuration
        logger.info("Validating configuration...")
        validate_config(config)
        
        # Handle scheduler and once arguments
        if args.once:
            # Run once without scheduler
            logger.info("Running harvest once (--once flag detected)")
            scheduler = create_scheduler(args.config)
            scheduler.run_once(args.query, args.limit, args.label)
            sys.exit(0)
        
        elif args.scheduler:
            # Start scheduler
            logger.info("Starting scheduler (--scheduler flag detected)")
            scheduler = create_scheduler(args.config)
            scheduler.start_scheduler(args.query, args.limit, args.label)
            
            # Keep the program running
            try:
                logger.info("Scheduler is running. Press Ctrl+C to stop.")
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                logger.info("Received interrupt signal, stopping scheduler...")
                scheduler.stop_scheduler()
                sys.exit(0)
        
        else:
            # Original behavior - run once without scheduler
            # Step 2: Initialize database
            db_path = config.get('database', {}).get('path', 'db/images.db')
            logger.info(f"Initializing database at {db_path}")
            init_db(db_path)
            
            # Step 3: Determine sources to use
            if args.sources:
                # Use sources from command line
                sources = [s.strip() for s in args.sources.split(',')]
                logger.info(f"Using specified sources: {', '.join(sources)}")
            else:
                # Use sources from config.yaml
                sources = config.get('sources', ['unsplash'])
                logger.info(f"Using configured sources: {', '.join(sources)}")
            
            # Step 4: Process each source sequentially
            total_results = {
                'downloaded': 0,
                'duplicates': 0,
                'failed': 0,
                'errors': []
            }
            
            source_results = {}
            
            for source in sources:
                logger.info(f"\n{'='*60}")
                logger.info(f"Processing source: {source.upper()}")
                logger.info(f"{'='*60}")
                
                # Initialize results for this source
                source_results[source] = {
                    'downloaded': 0,
                    'duplicates': 0,
                    'failed': 0,
                    'errors': []
                }
                
                try:
                    if source == "browser":
                        # Use browser adapter for web scraping
                        logger.info("Using browser adapter for web scraping...")
                        logger.warning("Browser scraping may take longer due to human-like behavior simulation")
                        
                        url_list = fetch_images_sync(args.query, args.limit)
                        
                        if not url_list:
                            logger.warning(f"No images found from {source}")
                            print(f"[{source.upper()}] 0 görsel indirildi (0 duplicate atlandı)")
                            continue
                        
                        logger.info(f"Found {len(url_list)} images from {source}")
                        
                    else:
                        # Use API adapter
                        logger.info(f"Searching {source} for query: '{args.query}' (limit: {args.limit})")
                        
                        adapter = get_adapter(source, config)
                        if not adapter:
                            logger.error(f"Failed to initialize {source} adapter. Please check your API key in config.yaml")
                            print(f"[{source.upper()}] 0 görsel indirildi (0 duplicate atlandı) - Adapter initialization failed")
                            continue
                        
                        # Search using the adapter
                        url_list = adapter.search(args.query, args.limit)
                        
                        if not url_list:
                            logger.warning(f"No images found from {source}")
                            print(f"[{source.upper()}] 0 görsel indirildi (0 duplicate atlandı)")
                            continue
                        
                        logger.info(f"Found {len(url_list)} images from {source}")
                    
                    # Step 5: Download and store images for this source
                    logger.info(f"Starting download of {len(url_list)} images from {source} with label '{args.label}'")
                    
                    # Create progress bar for this source
                    with tqdm(total=len(url_list), desc=f"Downloading from {source}", unit="img") as pbar:
                        for image_data in url_list:
                            try:
                                # Handle both old format (url only) and new format (with metadata)
                                if isinstance(image_data, dict) and 'url' in image_data:
                                    url = image_data['url']
                                    source_name = image_data.get('source', source)
                                else:
                                    # Legacy format
                                    url = image_data
                                    source_name = source
                                
                                result = download_and_store(url, args.label, config)
                                
                                if result['status'] == 'downloaded':
                                    source_results[source]['downloaded'] += 1
                                    total_results['downloaded'] += 1
                                    pbar.set_postfix({
                                        'Downloaded': source_results[source]['downloaded'],
                                        'Duplicates': source_results[source]['duplicates'],
                                        'Failed': source_results[source]['failed']
                                    })
                                elif result['status'] == 'duplicate':
                                    source_results[source]['duplicates'] += 1
                                    total_results['duplicates'] += 1
                                    pbar.set_postfix({
                                        'Downloaded': source_results[source]['downloaded'],
                                        'Duplicates': source_results[source]['duplicates'],
                                        'Failed': source_results[source]['failed']
                                    })
                                else:
                                    source_results[source]['failed'] += 1
                                    total_results['failed'] += 1
                                    source_results[source]['errors'].append({
                                        'url': url,
                                        'source': source_name,
                                        'error': result['message']
                                    })
                                    total_results['errors'].append({
                                        'url': url,
                                        'source': source_name,
                                        'error': result['message']
                                    })
                                    pbar.set_postfix({
                                        'Downloaded': source_results[source]['downloaded'],
                                        'Duplicates': source_results[source]['duplicates'],
                                        'Failed': source_results[source]['failed']
                                    })
                                
                            except Exception as e:
                                source_results[source]['failed'] += 1
                                total_results['failed'] += 1
                                url = image_data.get('url', str(image_data)) if isinstance(image_data, dict) else str(image_data)
                                source_name = image_data.get('source', source) if isinstance(image_data, dict) else source
                                error_info = {
                                    'url': url,
                                    'source': source_name,
                                    'error': str(e)
                                }
                                source_results[source]['errors'].append(error_info)
                                total_results['errors'].append(error_info)
                                logger.error(f"Error processing {url}: {e}")
                            
                            pbar.update(1)
                    
                    # Print results for this source
                    downloaded = source_results[source]['downloaded']
                    duplicates = source_results[source]['duplicates']
                    print(f"[{source.upper()}] {downloaded} görsel indirildi ({duplicates} duplicate atlandı)")
                    
                except Exception as e:
                    logger.error(f"Error processing source {source}: {e}")
                    print(f"[{source.upper()}] 0 görsel indirildi (0 duplicate atlandı) - Error: {e}")
                    continue
            
            # Step 6: Print final summary
            print("\n" + "="*60)
            print("FINAL SUMMARY")
            print("="*60)
            print(f"Total downloaded: {total_results['downloaded']}")
            print(f"Total duplicates skipped: {total_results['duplicates']}")
            print(f"Total failed: {total_results['failed']}")
            
            if total_results['errors']:
                print(f"\nFailed downloads:")
                for error in total_results['errors'][:5]:  # Show first 5 errors
                    source_info = f" ({error.get('source', 'unknown')})" if error.get('source') != 'unknown' else ""
                    print(f"  - {error['url']}{source_info}: {error['error']}")
                if len(total_results['errors']) > 5:
                    print(f"  ... and {len(total_results['errors']) - 5} more errors")
            
            print("="*60)
            
            # Exit with appropriate code
            if total_results['failed'] > 0:
                sys.exit(1)
            else:
                sys.exit(0)
            
    except FileNotFoundError as e:
        logger.error(f"Configuration file not found: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()