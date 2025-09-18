"""
Image deduplication for the harvest package.
"""

import imagehash
from PIL import Image
from pathlib import Path
from typing import List, Dict, Any, Set
import sqlite3


class ImageDeduplicator:
    """Image deduplication using perceptual hashing."""
    
    def __init__(self, db_path: str = "db/images.db"):
        self.db_path = db_path
    
    def find_duplicates(self, image_paths: List[str], 
                       threshold: int = 5) -> List[List[str]]:
        """Find duplicate images using perceptual hashing."""
        hashes = {}
        duplicates = []
        
        for image_path in image_paths:
            try:
                with Image.open(image_path) as img:
                    phash = imagehash.phash(img)
                    
                    # Check against existing hashes
                    found_duplicate = False
                    for existing_hash, existing_paths in hashes.items():
                        if phash - existing_hash <= threshold:
                            existing_paths.append(image_path)
                            found_duplicate = True
                            break
                    
                    if not found_duplicate:
                        hashes[phash] = [image_path]
                        
            except Exception as e:
                print(f"Error processing {image_path}: {e}")
        
        # Return only groups with duplicates
        for hash_group in hashes.values():
            if len(hash_group) > 1:
                duplicates.append(hash_group)
        
        return duplicates
    
    def find_duplicates_in_db(self, threshold: int = 5) -> List[List[Dict[str, Any]]]:
        """Find duplicates in database using stored hashes."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM images WHERE hash IS NOT NULL")
            images = [dict(row) for row in cursor.fetchall()]
        
        hash_groups = {}
        duplicates = []
        
        for image in images:
            try:
                phash = imagehash.hex_to_hash(image['hash'])
                
                found_duplicate = False
                for existing_hash, existing_images in hash_groups.items():
                    if phash - existing_hash <= threshold:
                        existing_images.append(image)
                        found_duplicate = True
                        break
                
                if not found_duplicate:
                    hash_groups[phash] = [image]
                    
            except Exception as e:
                print(f"Error processing hash for {image['url']}: {e}")
        
        # Return only groups with duplicates
        for hash_group in hash_groups.values():
            if len(hash_group) > 1:
                duplicates.append(hash_group)
        
        return duplicates
    
    def remove_duplicates(self, duplicate_groups: List[List[str]], 
                         keep_strategy: str = "first") -> List[str]:
        """Remove duplicate files, keeping one from each group."""
        removed_files = []
        
        for group in duplicate_groups:
            if keep_strategy == "first":
                keep_file = group[0]
                remove_files = group[1:]
            elif keep_strategy == "smallest":
                # Keep the smallest file
                file_sizes = [(f, Path(f).stat().st_size) for f in group]
                file_sizes.sort(key=lambda x: x[1])
                keep_file = file_sizes[0][0]
                remove_files = [f for f, _ in file_sizes[1:]]
            else:
                continue
            
            for file_path in remove_files:
                try:
                    Path(file_path).unlink()
                    removed_files.append(file_path)
                    print(f"Removed duplicate: {file_path}")
                except Exception as e:
                    print(f"Error removing {file_path}: {e}")
        
        return removed_files
