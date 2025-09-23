#!/usr/bin/env python3
"""
CLI for semantic duplicate detection operations.
Provides commands for managing semantic index and finding duplicates.
"""

import argparse
import sys
from pathlib import Path
from typing import List, Dict, Any

from .semantic_dedup import SemanticDeduplicator, create_semantic_deduplicator
from .enhanced_dedupe import EnhancedDeduplicator, create_enhanced_deduplicator
from .db import init_db, Database
from .utils.logger import get_logger


def cmd_add_image(args):
    """Add image to semantic index."""
    logger = get_logger("semantic_cli.add_image")
    
    if not Path(args.image_path).exists():
        logger.error(f"Image file not found: {args.image_path}")
        sys.exit(1)
    
    try:
        deduplicator = create_semantic_deduplicator(args.index_path)
        success = deduplicator.add_to_index(args.image_path, args.image_id)
        
        if success:
            print(f"✓ Added {args.image_path} to semantic index")
        else:
            print(f"✗ Failed to add {args.image_path} to semantic index")
            sys.exit(1)
        
        deduplicator.close()
        
    except Exception as e:
        logger.error(f"Error adding image: {e}")
        sys.exit(1)


def cmd_find_similar(args):
    """Find similar images."""
    logger = get_logger("semantic_cli.find_similar")
    
    if not Path(args.image_path).exists():
        logger.error(f"Image file not found: {args.image_path}")
        sys.exit(1)
    
    try:
        deduplicator = create_semantic_deduplicator(args.index_path)
        similar_images = deduplicator.find_similar(
            args.image_path, 
            threshold=args.threshold,
            max_results=args.max_results
        )
        
        if similar_images:
            print(f"Found {len(similar_images)} similar images:")
            for i, result in enumerate(similar_images, 1):
                print(f"  {i}. ID: {result['id']}")
                print(f"     Similarity: {result['similarity']:.3f}")
                print(f"     Path: {result['metadata']['image_path']}")
                print()
        else:
            print("No similar images found")
        
        deduplicator.close()
        
    except Exception as e:
        logger.error(f"Error finding similar images: {e}")
        sys.exit(1)


def cmd_rebuild_index(args):
    """Rebuild semantic index from database."""
    logger = get_logger("semantic_cli.rebuild_index")
    
    try:
        # Initialize database
        init_db(args.db_path)
        database = Database(args.db_path)
        
        # Get all images from database
        all_images = database.get_all_images()
        image_paths = []
        image_ids = []
        
        for image in all_images:
            image_path = image.get('filename')
            if image_path and Path(image_path).exists():
                image_paths.append(image_path)
                image_ids.append(image['id'])
        
        if not image_paths:
            print("No valid images found in database")
            return
        
        print(f"Rebuilding semantic index with {len(image_paths)} images...")
        
        # Create enhanced deduplicator and rebuild
        deduplicator = create_enhanced_deduplicator(
            db_path=args.db_path,
            semantic_index_path=args.index_path
        )
        
        success = deduplicator.rebuild_semantic_index()
        
        if success:
            print("✓ Semantic index rebuilt successfully")
        else:
            print("✗ Failed to rebuild semantic index")
            sys.exit(1)
        
        deduplicator.close()
        
    except Exception as e:
        logger.error(f"Error rebuilding index: {e}")
        sys.exit(1)


def cmd_check_duplicate(args):
    """Check if image is a duplicate using enhanced methods."""
    logger = get_logger("semantic_cli.check_duplicate")
    
    if not Path(args.image_path).exists():
        logger.error(f"Image file not found: {args.image_path}")
        sys.exit(1)
    
    try:
        deduplicator = create_enhanced_deduplicator(
            db_path=args.db_path,
            semantic_index_path=args.index_path
        )
        
        is_duplicate, results = deduplicator.is_duplicate(
            args.image_path,
            use_phash=args.use_phash,
            use_semantic=args.use_semantic
        )
        
        print(f"Image: {args.image_path}")
        print(f"Is duplicate: {is_duplicate}")
        print(f"Methods used: {results['method_used']}")
        print(f"Phash duplicates: {len(results['phash_duplicates'])}")
        print(f"Semantic duplicates: {len(results['semantic_duplicates'])}")
        
        if results['phash_duplicates']:
            print("\nPhash duplicates:")
            for dup in results['phash_duplicates']:
                print(f"  - {dup['id']}: {dup.get('filename', 'Unknown')}")
        
        if results['semantic_duplicates']:
            print("\nSemantic duplicates:")
            for dup in results['semantic_duplicates']:
                print(f"  - {dup['id']}: {dup.get('filename', 'Unknown')}")
        
        deduplicator.close()
        
    except Exception as e:
        logger.error(f"Error checking duplicate: {e}")
        sys.exit(1)


def cmd_stats(args):
    """Show semantic index statistics."""
    logger = get_logger("semantic_cli.stats")
    
    try:
        deduplicator = create_semantic_deduplicator(args.index_path)
        stats = deduplicator.get_index_stats()
        
        print("Semantic Index Statistics:")
        print(f"  Total images: {stats['total_images']}")
        print(f"  Embedding dimension: {stats['embedding_dimension']}")
        print(f"  Model: {stats['model_name']}")
        print(f"  Device: {stats['device']}")
        print(f"  Index type: {stats['index_type']}")
        
        deduplicator.close()
        
    except Exception as e:
        logger.error(f"Error getting statistics: {e}")
        sys.exit(1)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Semantic duplicate detection CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m harvest.semantic_cli add-image --image-path storage/image.jpg --image-id img001
  python -m harvest.semantic_cli find-similar --image-path storage/image.jpg --threshold 0.8
  python -m harvest.semantic_cli rebuild-index --db-path db/images.db
  python -m harvest.semantic_cli check-duplicate --image-path storage/image.jpg
  python -m harvest.semantic_cli stats
        """
    )
    
    parser.add_argument(
        "--index-path",
        default="db/semantic_index",
        help="Path to semantic index (default: db/semantic_index)"
    )
    
    parser.add_argument(
        "--db-path",
        default="db/images.db",
        help="Path to SQLite database (default: db/images.db)"
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Add image command
    add_parser = subparsers.add_parser('add-image', help='Add image to semantic index')
    add_parser.add_argument('--image-path', required=True, help='Path to image file')
    add_parser.add_argument('--image-id', required=True, help='Unique image identifier')
    add_parser.set_defaults(func=cmd_add_image)
    
    # Find similar command
    find_parser = subparsers.add_parser('find-similar', help='Find similar images')
    find_parser.add_argument('--image-path', required=True, help='Path to query image')
    find_parser.add_argument('--threshold', type=float, default=0.8, help='Similarity threshold (0.0-1.0)')
    find_parser.add_argument('--max-results', type=int, default=10, help='Maximum number of results')
    find_parser.set_defaults(func=cmd_find_similar)
    
    # Rebuild index command
    rebuild_parser = subparsers.add_parser('rebuild-index', help='Rebuild semantic index from database')
    rebuild_parser.set_defaults(func=cmd_rebuild_index)
    
    # Check duplicate command
    check_parser = subparsers.add_parser('check-duplicate', help='Check if image is duplicate')
    check_parser.add_argument('--image-path', required=True, help='Path to image file')
    check_parser.add_argument('--use-phash', action='store_true', default=True, help='Use perceptual hashing')
    check_parser.add_argument('--use-semantic', action='store_true', default=True, help='Use semantic similarity')
    check_parser.set_defaults(func=cmd_check_duplicate)
    
    # Stats command
    stats_parser = subparsers.add_parser('stats', help='Show semantic index statistics')
    stats_parser.set_defaults(func=cmd_stats)
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Execute the command
    args.func(args)


if __name__ == "__main__":
    main()
