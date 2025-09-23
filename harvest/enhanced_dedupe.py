"""
Enhanced deduplication system combining perceptual hashing and semantic similarity.
Provides comprehensive duplicate detection using both methods.
"""

import os
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple, Set
import logging

from .dedupe import ImageDeduplicator
from .semantic_dedup import SemanticDeduplicator
from .db import Database

logger = logging.getLogger(__name__)


class EnhancedDeduplicator:
    """
    Enhanced deduplication combining perceptual hashing and semantic similarity.
    Provides comprehensive duplicate detection with configurable strategies.
    """
    
    def __init__(self, 
                 db_path: str = "db/images.db",
                 semantic_index_path: str = "db/semantic_index",
                 phash_threshold: int = 5,
                 semantic_threshold: float = 0.85,
                 combine_strategy: str = "union"):
        """
        Initialize enhanced deduplicator.
        
        Args:
            db_path: Path to SQLite database
            semantic_index_path: Path to semantic index
            phash_threshold: Perceptual hash threshold (0-64)
            semantic_threshold: Semantic similarity threshold (0.0-1.0)
            combine_strategy: How to combine results ('union', 'intersection', 'phash_first', 'semantic_first')
        """
        self.db_path = db_path
        self.semantic_index_path = semantic_index_path
        
        # Thresholds
        self.phash_threshold = phash_threshold
        self.semantic_threshold = semantic_threshold
        self.combine_strategy = combine_strategy
        
        # Initialize components
        self.phash_deduplicator = ImageDeduplicator(db_path)
        self.semantic_deduplicator = SemanticDeduplicator(semantic_index_path)
        self.database = Database(db_path)
        
        logger.info(f"Enhanced deduplicator initialized with strategy: {combine_strategy}")
    
    def add_image(self, image_path: str, image_id: str, metadata: Optional[Dict[str, Any]] = None) -> bool:
        """
        Add image to both perceptual and semantic indices.
        
        Args:
            image_path: Path to image file
            image_id: Unique identifier
            metadata: Additional metadata
            
        Returns:
            True if successfully added to both indices
        """
        success = True
        
        # Add to semantic index
        if not self.semantic_deduplicator.add_to_index(image_path, image_id, metadata):
            logger.warning(f"Failed to add {image_id} to semantic index")
            success = False
        
        # Note: Perceptual hash is typically added during download process
        # This method focuses on semantic indexing
        
        return success
    
    def find_duplicates_phash(self, image_path: str) -> List[Dict[str, Any]]:
        """Find duplicates using only perceptual hashing."""
        try:
            from PIL import Image
            import imagehash
            
            # Calculate perceptual hash
            with Image.open(image_path) as img:
                phash = imagehash.phash(img)
            
            # Find similar images in database
            similar_images = self.database.find_similar_phash(str(phash), self.phash_threshold)
            
            return similar_images
            
        except Exception as e:
            logger.error(f"Error in phash duplicate detection: {e}")
            return []
    
    def find_duplicates_semantic(self, image_path: str) -> List[Dict[str, Any]]:
        """Find duplicates using only semantic similarity."""
        try:
            # Find semantically similar images
            similar_results = self.semantic_deduplicator.find_similar(
                image_path, 
                self.semantic_threshold
            )
            
            # Convert to database format
            similar_images = []
            for result in similar_results:
                # Get full image record from database
                image_record = self.database.find_by_md5(result['id'])  # Assuming ID is MD5
                if image_record:
                    similar_images.append(image_record)
            
            return similar_images
            
        except Exception as e:
            logger.error(f"Error in semantic duplicate detection: {e}")
            return []
    
    def find_duplicates_combined(self, image_path: str) -> Dict[str, List[Dict[str, Any]]]:
        """
        Find duplicates using both methods and combine results.
        
        Args:
            image_path: Path to query image
            
        Returns:
            Dictionary with 'phash', 'semantic', and 'combined' results
        """
        # Get results from both methods
        phash_results = self.find_duplicates_phash(image_path)
        semantic_results = self.find_duplicates_semantic(image_path)
        
        # Combine results based on strategy
        combined_results = self._combine_results(phash_results, semantic_results)
        
        return {
            'phash': phash_results,
            'semantic': semantic_results,
            'combined': combined_results
        }
    
    def _combine_results(self, phash_results: List[Dict[str, Any]], 
                        semantic_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Combine results from both methods based on strategy."""
        
        if self.combine_strategy == "union":
            # Return all unique results from both methods
            all_results = phash_results + semantic_results
            seen_ids = set()
            unique_results = []
            
            for result in all_results:
                if result['id'] not in seen_ids:
                    seen_ids.add(result['id'])
                    unique_results.append(result)
            
            return unique_results
        
        elif self.combine_strategy == "intersection":
            # Return only results found by both methods
            phash_ids = {result['id'] for result in phash_results}
            semantic_ids = {result['id'] for result in semantic_results}
            
            intersection_ids = phash_ids.intersection(semantic_ids)
            return [result for result in phash_results if result['id'] in intersection_ids]
        
        elif self.combine_strategy == "phash_first":
            # Return phash results, supplemented by semantic if few results
            if len(phash_results) >= 3:
                return phash_results
            else:
                # Add semantic results not already in phash
                phash_ids = {result['id'] for result in phash_results}
                additional = [result for result in semantic_results 
                             if result['id'] not in phash_ids]
                return phash_results + additional
        
        elif self.combine_strategy == "semantic_first":
            # Return semantic results, supplemented by phash if few results
            if len(semantic_results) >= 3:
                return semantic_results
            else:
                # Add phash results not already in semantic
                semantic_ids = {result['id'] for result in semantic_results}
                additional = [result for result in phash_results 
                             if result['id'] not in semantic_ids]
                return semantic_results + additional
        
        else:
            logger.warning(f"Unknown combine strategy: {self.combine_strategy}")
            return phash_results
    
    def is_duplicate(self, image_path: str, 
                    use_phash: bool = True, 
                    use_semantic: bool = True) -> Tuple[bool, Dict[str, Any]]:
        """
        Check if image is a duplicate using specified methods.
        
        Args:
            image_path: Path to query image
            use_phash: Whether to use perceptual hashing
            use_semantic: Whether to use semantic similarity
            
        Returns:
            Tuple of (is_duplicate, results_info)
        """
        results_info = {
            'phash_duplicates': [],
            'semantic_duplicates': [],
            'is_duplicate': False,
            'method_used': []
        }
        
        if use_phash:
            phash_results = self.find_duplicates_phash(image_path)
            results_info['phash_duplicates'] = phash_results
            if phash_results:
                results_info['is_duplicate'] = True
                results_info['method_used'].append('phash')
        
        if use_semantic:
            semantic_results = self.find_duplicates_semantic(image_path)
            results_info['semantic_duplicates'] = semantic_results
            if semantic_results:
                results_info['is_duplicate'] = True
                results_info['method_used'].append('semantic')
        
        return results_info['is_duplicate'], results_info
    
    def get_duplicate_groups(self, 
                            use_phash: bool = True, 
                            use_semantic: bool = True) -> List[List[Dict[str, Any]]]:
        """
        Get all duplicate groups in the database.
        
        Args:
            use_phash: Whether to use perceptual hashing
            use_semantic: Whether to use semantic similarity
            
        Returns:
            List of duplicate groups
        """
        all_groups = []
        
        if use_phash:
            phash_groups = self.phash_deduplicator.find_duplicates_in_db(self.phash_threshold)
            all_groups.extend(phash_groups)
        
        if use_semantic:
            # For semantic, we need to process all images
            all_images = self.database.get_all_images()
            processed = set()
            
            for image in all_images:
                if image['id'] in processed:
                    continue
                
                image_path = image.get('filename')
                if not image_path or not os.path.exists(image_path):
                    continue
                
                similar = self.find_duplicates_semantic(image_path)
                if len(similar) > 1:  # More than just the query image
                    group = [image] + similar
                    all_groups.append(group)
                    
                    # Mark all in group as processed
                    for img in group:
                        processed.add(img['id'])
        
        return all_groups
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get statistics about the deduplication system."""
        db_stats = {
            'total_images': len(self.database.get_all_images()),
            'phash_threshold': self.phash_threshold,
            'semantic_threshold': self.semantic_threshold,
            'combine_strategy': self.combine_strategy
        }
        
        semantic_stats = self.semantic_deduplicator.get_index_stats()
        
        return {
            'database': db_stats,
            'semantic_index': semantic_stats
        }
    
    def rebuild_semantic_index(self) -> bool:
        """Rebuild the semantic index from all images in database."""
        try:
            all_images = self.database.get_all_images()
            image_paths = []
            image_ids = []
            
            for image in all_images:
                image_path = image.get('filename')
                if image_path and os.path.exists(image_path):
                    image_paths.append(image_path)
                    image_ids.append(image['id'])
            
            if not image_paths:
                logger.warning("No valid image paths found for semantic index rebuild")
                return False
            
            logger.info(f"Rebuilding semantic index with {len(image_paths)} images")
            return self.semantic_deduplicator.rebuild_index(image_paths, image_ids)
            
        except Exception as e:
            logger.error(f"Failed to rebuild semantic index: {e}")
            return False
    
    def close(self):
        """Close and cleanup resources."""
        self.semantic_deduplicator.close()
        logger.info("Enhanced deduplicator closed")


# Convenience functions for easy integration
def create_enhanced_deduplicator(db_path: str = "db/images.db",
                               semantic_index_path: str = "db/semantic_index") -> EnhancedDeduplicator:
    """Create an enhanced deduplicator instance."""
    return EnhancedDeduplicator(db_path=db_path, semantic_index_path=semantic_index_path)


def check_duplicate_enhanced(image_path: str, 
                           db_path: str = "db/images.db",
                           semantic_index_path: str = "db/semantic_index",
                           use_phash: bool = True,
                           use_semantic: bool = True) -> Tuple[bool, Dict[str, Any]]:
    """Check if an image is a duplicate using enhanced methods."""
    deduplicator = EnhancedDeduplicator(db_path=db_path, semantic_index_path=semantic_index_path)
    try:
        return deduplicator.is_duplicate(image_path, use_phash, use_semantic)
    finally:
        deduplicator.close()
