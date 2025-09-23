#!/usr/bin/env python3
"""
Test script for semantic duplicate detection system.
Demonstrates usage of both semantic and enhanced deduplication.
"""

import os
import sys
import time
from pathlib import Path

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from harvest.semantic_dedup import SemanticDeduplicator, create_semantic_deduplicator
from harvest.enhanced_dedupe import EnhancedDeduplicator, create_enhanced_deduplicator
from harvest.db import init_db, Database


def test_semantic_deduplicator():
    """Test the semantic deduplicator functionality."""
    print("Testing Semantic Deduplicator")
    print("=" * 50)
    
    try:
        # Create semantic deduplicator
        deduplicator = create_semantic_deduplicator("test_semantic_index")
        
        # Test with sample images from storage directory
        storage_dir = Path("storage")
        if not storage_dir.exists():
            print("⚠ No storage directory found. Creating dummy test...")
            return
        
        image_files = list(storage_dir.glob("*.jpg"))[:5]  # Test with first 5 images
        
        if not image_files:
            print("⚠ No images found in storage directory")
            return
        
        print(f"Found {len(image_files)} test images")
        
        # Add images to semantic index
        print("\nAdding images to semantic index...")
        for i, image_path in enumerate(image_files):
            image_id = f"test_image_{i}"
            success = deduplicator.add_to_index(str(image_path), image_id)
            print(f"  {image_path.name}: {'✓' if success else '✗'}")
        
        # Test similarity search
        if image_files:
            print(f"\nTesting similarity search with {image_files[0].name}...")
            similar_images = deduplicator.find_similar(
                str(image_files[0]), 
                threshold=0.7, 
                max_results=3
            )
            
            print(f"Found {len(similar_images)} similar images:")
            for result in similar_images:
                print(f"  - ID: {result['id']}, Similarity: {result['similarity']:.3f}")
        
        # Get index statistics
        stats = deduplicator.get_index_stats()
        print(f"\nIndex Statistics:")
        print(f"  Total images: {stats['total_images']}")
        print(f"  Embedding dimension: {stats['embedding_dimension']}")
        print(f"  Model: {stats['model_name']}")
        print(f"  Device: {stats['device']}")
        
        deduplicator.close()
        print("\n✓ Semantic deduplicator test completed")
        
    except Exception as e:
        print(f"✗ Semantic deduplicator test failed: {e}")


def test_enhanced_deduplicator():
    """Test the enhanced deduplicator functionality."""
    print("\nTesting Enhanced Deduplicator")
    print("=" * 50)
    
    try:
        # Initialize database
        init_db("test_images.db")
        
        # Create enhanced deduplicator
        deduplicator = create_enhanced_deduplicator(
            db_path="test_images.db",
            semantic_index_path="test_enhanced_index"
        )
        
        # Test with sample images
        storage_dir = Path("storage")
        if not storage_dir.exists():
            print("⚠ No storage directory found")
            return
        
        image_files = list(storage_dir.glob("*.jpg"))[:3]
        
        if not image_files:
            print("⚠ No images found in storage directory")
            return
        
        print(f"Testing with {len(image_files)} images")
        
        # Add images to database and semantic index
        print("\nAdding images to database and semantic index...")
        for i, image_path in enumerate(image_files):
            image_id = f"enhanced_test_{i}"
            
            # Add to database
            from harvest.db import insert_image
            insert_image({
                'id': image_id,
                'url': f'file://{image_path}',
                'filename': str(image_path),
                'domain': 'test',
                'label': 'test'
            }, "test_images.db")
            
            # Add to semantic index
            success = deduplicator.add_image(str(image_path), image_id)
            print(f"  {image_path.name}: {'✓' if success else '✗'}")
        
        # Test duplicate detection
        if image_files:
            print(f"\nTesting duplicate detection with {image_files[0].name}...")
            
            # Test with different strategies
            strategies = ['union', 'intersection', 'phash_first', 'semantic_first']
            
            for strategy in strategies:
                print(f"\nTesting strategy: {strategy}")
                deduplicator.combine_strategy = strategy
                
                is_duplicate, results = deduplicator.is_duplicate(
                    str(image_files[0]),
                    use_phash=True,
                    use_semantic=True
                )
                
                print(f"  Is duplicate: {is_duplicate}")
                print(f"  Methods used: {results['method_used']}")
                print(f"  Phash results: {len(results['phash_duplicates'])}")
                print(f"  Semantic results: {len(results['semantic_duplicates'])}")
        
        # Get statistics
        stats = deduplicator.get_statistics()
        print(f"\nEnhanced Deduplicator Statistics:")
        print(f"  Database images: {stats['database']['total_images']}")
        print(f"  Semantic index images: {stats['semantic_index']['total_images']}")
        print(f"  Phash threshold: {stats['database']['phash_threshold']}")
        print(f"  Semantic threshold: {stats['database']['semantic_threshold']}")
        
        deduplicator.close()
        print("\n✓ Enhanced deduplicator test completed")
        
    except Exception as e:
        print(f"✗ Enhanced deduplicator test failed: {e}")


def test_performance():
    """Test performance with multiple images."""
    print("\nTesting Performance")
    print("=" * 50)
    
    try:
        storage_dir = Path("storage")
        if not storage_dir.exists():
            print("⚠ No storage directory found for performance test")
            return
        
        image_files = list(storage_dir.glob("*.jpg"))
        
        if len(image_files) < 2:
            print("⚠ Need at least 2 images for performance test")
            return
        
        print(f"Testing with {len(image_files)} images")
        
        # Test semantic deduplicator performance
        deduplicator = create_semantic_deduplicator("test_performance_index")
        
        # Add images and measure time
        start_time = time.time()
        
        for i, image_path in enumerate(image_files):
            image_id = f"perf_test_{i}"
            deduplicator.add_to_index(str(image_path), image_id)
        
        add_time = time.time() - start_time
        print(f"Added {len(image_files)} images in {add_time:.2f} seconds")
        print(f"Average time per image: {add_time/len(image_files):.3f} seconds")
        
        # Test search performance
        search_times = []
        for image_path in image_files[:5]:  # Test with first 5 images
            start_time = time.time()
            similar = deduplicator.find_similar(str(image_path), threshold=0.8)
            search_time = time.time() - start_time
            search_times.append(search_time)
        
        avg_search_time = sum(search_times) / len(search_times)
        print(f"Average search time: {avg_search_time:.3f} seconds")
        
        deduplicator.close()
        print("✓ Performance test completed")
        
    except Exception as e:
        print(f"✗ Performance test failed: {e}")


def test_integration():
    """Test integration with existing system."""
    print("\nTesting Integration")
    print("=" * 50)
    
    try:
        # Test that the modules can be imported
        from harvest.semantic_dedup import add_image_to_semantic_index, find_semantic_similarities
        from harvest.enhanced_dedupe import check_duplicate_enhanced
        
        print("✓ All modules imported successfully")
        
        # Test convenience functions
        storage_dir = Path("storage")
        if storage_dir.exists() and list(storage_dir.glob("*.jpg")):
            test_image = str(list(storage_dir.glob("*.jpg"))[0])
            
            print(f"Testing with image: {Path(test_image).name}")
            
            # Test semantic functions
            success = add_image_to_semantic_index(test_image, "integration_test")
            print(f"Add to semantic index: {'✓' if success else '✗'}")
            
            similar = find_semantic_similarities(test_image, threshold=0.8)
            print(f"Find similarities: {len(similar)} results")
            
            # Test enhanced duplicate check
            is_dup, results = check_duplicate_enhanced(test_image)
            print(f"Enhanced duplicate check: {is_dup}")
            
        print("✓ Integration test completed")
        
    except Exception as e:
        print(f"✗ Integration test failed: {e}")


def cleanup_test_files():
    """Clean up test files."""
    print("\nCleaning up test files...")
    
    test_files = [
        "test_images.db",
        "test_semantic_index",
        "test_enhanced_index", 
        "test_performance_index"
    ]
    
    for file_path in test_files:
        if os.path.exists(file_path):
            if os.path.isdir(file_path):
                import shutil
                shutil.rmtree(file_path)
            else:
                os.remove(file_path)
            print(f"  Removed: {file_path}")


def main():
    """Main test function."""
    print("PixVault Semantic Duplicate Detection Test")
    print("=" * 60)
    
    # Check dependencies
    try:
        import torch
        import clip
        import faiss
        print("✓ All dependencies available")
    except ImportError as e:
        print(f"✗ Missing dependencies: {e}")
        print("Install with: pip install torch clip-by-openai faiss-cpu")
        return
    
    # Run tests
    test_semantic_deduplicator()
    test_enhanced_deduplicator()
    test_performance()
    test_integration()
    
    # Cleanup
    cleanup_test_files()
    
    print("\n" + "=" * 60)
    print("All tests completed!")
    print("\nTo use semantic duplicate detection in your code:")
    print("  from harvest.semantic_dedup import create_semantic_deduplicator")
    print("  from harvest.enhanced_dedupe import create_enhanced_deduplicator")


if __name__ == "__main__":
    main()
