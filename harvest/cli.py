#!/usr/bin/env python3
"""
Command-line interface for the harvest package.
"""

import argparse
import sys
from pathlib import Path
from typing import Dict, Any, List

from tqdm import tqdm

from .config import load_config
from .db import init_db
from .adapters.unsplash import search_unsplash
from .downloader import download_and_store
from .utils.logger import get_logger


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Harvest images from various sources",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m harvest.cli --source unsplash --query "dev iş makinesi" --limit 100 --label excavator
  python -m harvest.cli --source unsplash --query "nature" --limit 50 --label landscape
        """
    )
    
    parser.add_argument(
        "--source",
        required=True,
        choices=["unsplash"],
        help="Image source to harvest from (currently only 'unsplash' is supported)"
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
    
    args = parser.parse_args()
    
    # Initialize logger
    logger = get_logger("harvest.cli")
    
    try:
        # Step 1: Load config.yaml
        logger.info(f"Loading configuration from {args.config}")
        config = load_config(args.config)
        
        # Step 2: Initialize database
        db_path = config.get('database', {}).get('path', 'db/images.db')
        logger.info(f"Initializing database at {db_path}")
        init_db(db_path)
        
        # Step 3: Call selected source adapter
        logger.info(f"Searching {args.source} for query: '{args.query}' (limit: {args.limit})")
        
        if args.source == "unsplash":
            # Get API key from config
            api_key = config.get('apis', {}).get('unsplash', {}).get('access_key')
            if not api_key:
                logger.error("Unsplash API key not found in configuration. Please add it to config.yaml under apis.unsplash.access_key")
                sys.exit(1)
            
            # Search Unsplash
            url_list = search_unsplash(args.query, args.limit, api_key)
            
            if not url_list:
                logger.warning("No images found for the given query")
                sys.exit(0)
            
            logger.info(f"Found {len(url_list)} images from Unsplash")
        
        else:
            logger.error(f"Unsupported source: {args.source}")
            sys.exit(1)
        
        # Step 4: Download and store images with progress bar
        logger.info(f"Starting download of {len(url_list)} images with label '{args.label}'")
        
        results = {
            'downloaded': 0,
            'duplicates': 0,
            'failed': 0,
            'errors': []
        }
        
        # Create progress bar
        with tqdm(total=len(url_list), desc="Downloading images", unit="img") as pbar:
            for image_data in url_list:
                try:
                    result = download_and_store(image_data['url'], args.label, config)
                    
                    if result['status'] == 'downloaded':
                        results['downloaded'] += 1
                        pbar.set_postfix({
                            'Downloaded': results['downloaded'],
                            'Duplicates': results['duplicates'],
                            'Failed': results['failed']
                        })
                    elif result['status'] == 'duplicate':
                        results['duplicates'] += 1
                        pbar.set_postfix({
                            'Downloaded': results['downloaded'],
                            'Duplicates': results['duplicates'],
                            'Failed': results['failed']
                        })
                    else:
                        results['failed'] += 1
                        results['errors'].append({
                            'url': image_data['url'],
                            'error': result['message']
                        })
                        pbar.set_postfix({
                            'Downloaded': results['downloaded'],
                            'Duplicates': results['duplicates'],
                            'Failed': results['failed']
                        })
                    
                except Exception as e:
                    results['failed'] += 1
                    results['errors'].append({
                        'url': image_data['url'],
                        'error': str(e)
                    })
                    logger.error(f"Error processing {image_data['url']}: {e}")
                
                pbar.update(1)
        
        # Print summary
        print("\n" + "="*50)
        print("DOWNLOAD SUMMARY")
        print("="*50)
        print(f"Total images processed: {len(url_list)}")
        print(f"Successfully downloaded: {results['downloaded']}")
        print(f"Duplicates skipped: {results['duplicates']}")
        print(f"Failed downloads: {results['failed']}")
        
        if results['errors']:
            print(f"\nFailed downloads:")
            for error in results['errors'][:5]:  # Show first 5 errors
                print(f"  - {error['url']}: {error['error']}")
            if len(results['errors']) > 5:
                print(f"  ... and {len(results['errors']) - 5} more errors")
        
        print("="*50)
        
        # Exit with appropriate code
        if results['failed'] > 0:
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