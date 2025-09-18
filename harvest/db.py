"""
Database management for the harvest package.
"""

import sqlite3
import imagehash
from pathlib import Path
from typing import List, Dict, Any, Optional
from urllib.parse import urlparse


def init_db(db_path: str = "db/images.db") -> None:
    """
    Initialize database with images table if it doesn't exist.
    
    Args:
        db_path: Path to the SQLite database file
    """
    db_file = Path(db_path)
    db_file.parent.mkdir(parents=True, exist_ok=True)
    
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        
        # Create images table with specified schema
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS images (
                id TEXT PRIMARY KEY,
                url TEXT,
                domain TEXT,
                filename TEXT,
                md5 TEXT,
                phash TEXT,
                width INTEGER,
                height INTEGER,
                format TEXT,
                label TEXT,
                downloaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT,
                notes TEXT
            )
        """)
        
        # Create indexes for better performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_md5 ON images(md5)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_phash ON images(phash)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_domain ON images(domain)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_status ON images(status)")
        
        conn.commit()


def insert_image(record: Dict[str, Any], db_path: str = "db/images.db") -> None:
    """
    Insert image record into database.
    
    Args:
        record: Dictionary containing image data
        db_path: Path to the SQLite database file
    """
    # Extract domain from URL if not provided
    if 'url' in record and 'domain' not in record:
        parsed_url = urlparse(record['url'])
        record['domain'] = parsed_url.netloc
    
    # Prepare the SQL query
    columns = list(record.keys())
    placeholders = ', '.join(['?' for _ in columns])
    column_names = ', '.join(columns)
    
    values = [record.get(col) for col in columns]
    
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(f"""
            INSERT OR REPLACE INTO images ({column_names})
            VALUES ({placeholders})
        """, values)
        conn.commit()


def find_by_md5(md5: str, db_path: str = "db/images.db") -> Optional[Dict[str, Any]]:
    """
    Find image record by MD5 hash.
    
    Args:
        md5: MD5 hash to search for
        db_path: Path to the SQLite database file
        
    Returns:
        Dictionary containing image record or None if not found
    """
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM images WHERE md5 = ?", (md5,))
        row = cursor.fetchone()
        return dict(row) if row else None


def find_similar_phash(phash: str, threshold: int = 5, db_path: str = "db/images.db") -> List[Dict[str, Any]]:
    """
    Find images with similar perceptual hash.
    
    Args:
        phash: Perceptual hash to compare against
        threshold: Maximum hamming distance for similarity (default: 5)
        db_path: Path to the SQLite database file
        
    Returns:
        List of dictionaries containing similar image records
    """
    similar_images = []
    
    try:
        # Convert string hash to imagehash object for comparison
        target_hash = imagehash.hex_to_hash(phash)
        
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM images WHERE phash IS NOT NULL")
            rows = cursor.fetchall()
            
            for row in rows:
                try:
                    # Convert stored hash to imagehash object
                    stored_hash = imagehash.hex_to_hash(row['phash'])
                    
                    # Calculate hamming distance
                    distance = target_hash - stored_hash
                    
                    if distance <= threshold:
                        similar_images.append(dict(row))
                        
                except (ValueError, TypeError):
                    # Skip invalid hash values
                    continue
                    
    except (ValueError, TypeError):
        # Invalid input hash
        pass
    
    return similar_images


class Database:
    """Database manager for storing image metadata."""
    
    def __init__(self, db_path: str = "db/images.db"):
        self.db_path = db_path
        init_db(db_path)
    
    def insert_image(self, record: Dict[str, Any]) -> None:
        """Insert image record using the module function."""
        insert_image(record, self.db_path)
    
    def find_by_md5(self, md5: str) -> Optional[Dict[str, Any]]:
        """Find image by MD5 using the module function."""
        return find_by_md5(md5, self.db_path)
    
    def find_similar_phash(self, phash: str, threshold: int = 5) -> List[Dict[str, Any]]:
        """Find similar images by perceptual hash using the module function."""
        return find_similar_phash(phash, threshold, self.db_path)
    
    def get_all_images(self) -> List[Dict[str, Any]]:
        """Get all image records."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM images ORDER BY downloaded_at DESC")
            return [dict(row) for row in cursor.fetchall()]
    
    def get_images_by_status(self, status: str) -> List[Dict[str, Any]]:
        """Get images by status."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM images WHERE status = ? ORDER BY downloaded_at DESC", (status,))
            return [dict(row) for row in cursor.fetchall()]
    
    def update_image_status(self, image_id: str, status: str, notes: str = None) -> None:
        """Update image status and notes."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            if notes:
                cursor.execute("UPDATE images SET status = ?, notes = ? WHERE id = ?", (status, notes, image_id))
            else:
                cursor.execute("UPDATE images SET status = ? WHERE id = ?", (status, image_id))
            conn.commit()
