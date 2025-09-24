"""
Backup and archiving module for PixVault.
Provides secure backup, restore, and archiving functionality for images and metadata.
"""

import os
import shutil
import sqlite3
import json
import gzip
import tarfile
import hashlib
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional, Union
from dataclasses import dataclass, asdict
import logging

from .config import load_config
from .db import Database
from .utils.logger import get_logger

logger = get_logger("harvest.backup")


@dataclass
class BackupMetadata:
    """Metadata for a backup operation."""
    backup_id: str
    timestamp: float
    date: str
    backup_type: str  # 'full', 'incremental', 'metadata_only'
    source_path: str
    destination_path: str
    file_count: int
    total_size_bytes: int
    compression_ratio: float
    checksum: str
    status: str  # 'success', 'failed', 'partial'
    error_message: Optional[str] = None
    created_at: str = None
    expires_at: Optional[str] = None


@dataclass
class BackupConfig:
    """Configuration for backup operations."""
    storage_path: str
    db_path: str
    backup_root: str
    compression: bool = True
    encryption: bool = False
    encryption_key: Optional[str] = None
    retention_days: int = 30
    max_backups: int = 10
    exclude_patterns: List[str] = None
    
    def __post_init__(self):
        if self.exclude_patterns is None:
            self.exclude_patterns = ['*.tmp', '*.log', '__pycache__']


class BackupManager:
    """
    Manages backup and restore operations for PixVault.
    Provides secure backup, restore, and archiving functionality.
    """
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize backup manager.
        
        Args:
            config_path: Path to configuration file
        """
        self.config_path = config_path
        self.config = load_config(config_path)
        
        # Initialize backup configuration
        self.backup_config = self._load_backup_config()
        
        # Initialize backup metadata database
        self.backup_db_path = os.path.join(self.backup_config.backup_root, "backup_metadata.db")
        self._init_backup_db()
        
        logger.info(f"Backup manager initialized: {self.backup_config.backup_root}")
    
    def _load_backup_config(self) -> BackupConfig:
        """Load backup configuration from config file."""
        backup_config = self.config.get('backup', {})
        
        return BackupConfig(
            storage_path=backup_config.get('storage_path', 'storage'),
            db_path=backup_config.get('db_path', 'db/images.db'),
            backup_root=backup_config.get('backup_root', 'backups'),
            compression=backup_config.get('compression', True),
            encryption=backup_config.get('encryption', False),
            encryption_key=backup_config.get('encryption_key'),
            retention_days=backup_config.get('retention_days', 30),
            max_backups=backup_config.get('max_backups', 10),
            exclude_patterns=backup_config.get('exclude_patterns', [])
        )
    
    def _init_backup_db(self):
        """Initialize backup metadata database."""
        # Create backup root directory
        Path(self.backup_config.backup_root).mkdir(parents=True, exist_ok=True)
        
        # Create backup metadata database
        with sqlite3.connect(self.backup_db_path) as conn:
            cursor = conn.cursor()
            
            # Create backup_metadata table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS backup_metadata (
                    backup_id TEXT PRIMARY KEY,
                    timestamp REAL NOT NULL,
                    date TEXT NOT NULL,
                    backup_type TEXT NOT NULL,
                    source_path TEXT NOT NULL,
                    destination_path TEXT NOT NULL,
                    file_count INTEGER DEFAULT 0,
                    total_size_bytes INTEGER DEFAULT 0,
                    compression_ratio REAL DEFAULT 1.0,
                    checksum TEXT NOT NULL,
                    status TEXT NOT NULL,
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    expires_at TIMESTAMP
                )
            """)
            
            # Create indexes
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_backup_timestamp ON backup_metadata(timestamp)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_backup_date ON backup_metadata(date)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_backup_type ON backup_metadata(backup_type)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_backup_status ON backup_metadata(status)")
            
            conn.commit()
    
    def _generate_backup_id(self) -> str:
        """Generate unique backup ID."""
        timestamp = int(time.time())
        return f"backup_{timestamp}"
    
    def _calculate_checksum(self, file_path: str) -> str:
        """Calculate SHA256 checksum of a file."""
        sha256_hash = hashlib.sha256()
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    sha256_hash.update(chunk)
            return sha256_hash.hexdigest()
        except Exception as e:
            logger.error(f"Failed to calculate checksum for {file_path}: {e}")
            return ""
    
    def _get_file_count_and_size(self, directory: str) -> tuple[int, int]:
        """Get file count and total size of a directory."""
        file_count = 0
        total_size = 0
        
        try:
            for root, dirs, files in os.walk(directory):
                for file in files:
                    file_path = os.path.join(root, file)
                    if os.path.exists(file_path):
                        file_count += 1
                        total_size += os.path.getsize(file_path)
        except Exception as e:
            logger.error(f"Failed to calculate directory size: {e}")
        
        return file_count, total_size
    
    def _should_exclude_file(self, file_path: str) -> bool:
        """Check if file should be excluded from backup."""
        for pattern in self.backup_config.exclude_patterns:
            if file_path.endswith(pattern.replace('*', '')):
                return True
        return False
    
    def backup_storage(self, dest_path: str, backup_type: str = "full") -> BackupMetadata:
        """
        Backup storage directory to destination.
        
        Args:
            dest_path: Destination path for backup
            backup_type: Type of backup ('full', 'incremental', 'metadata_only')
            
        Returns:
            BackupMetadata object with backup information
        """
        backup_id = self._generate_backup_id()
        timestamp = time.time()
        date = datetime.now().strftime('%Y-%m-%d')
        
        logger.info(f"Starting {backup_type} backup: {backup_id}")
        
        try:
            # Create destination directory
            Path(dest_path).mkdir(parents=True, exist_ok=True)
            
            # Get source information
            source_path = self.backup_config.storage_path
            if not os.path.exists(source_path):
                raise FileNotFoundError(f"Source path does not exist: {source_path}")
            
            # Calculate source size and file count
            file_count, total_size = self._get_file_count_and_size(source_path)
            
            # Perform backup based on type
            if backup_type == "metadata_only":
                # Only backup metadata (database)
                self._backup_database(dest_path)
                actual_file_count = 1  # Only database file
                actual_size = os.path.getsize(os.path.join(dest_path, "images.db"))
            else:
                # Backup storage directory
                if self.backup_config.compression:
                    self._backup_with_compression(source_path, dest_path)
                else:
                    self._backup_with_shutil(source_path, dest_path)
                
                # Calculate actual backup size
                actual_file_count, actual_size = self._get_file_count_and_size(dest_path)
            
            # Calculate compression ratio
            compression_ratio = actual_size / total_size if total_size > 0 else 1.0
            
            # Calculate checksum
            checksum = self._calculate_checksum(dest_path)
            
            # Create backup metadata
            backup_metadata = BackupMetadata(
                backup_id=backup_id,
                timestamp=timestamp,
                date=date,
                backup_type=backup_type,
                source_path=source_path,
                destination_path=dest_path,
                file_count=actual_file_count,
                total_size_bytes=actual_size,
                compression_ratio=compression_ratio,
                checksum=checksum,
                status="success"
            )
            
            # Store backup metadata
            self._store_backup_metadata(backup_metadata)
            
            logger.info(f"Backup completed: {backup_id} ({actual_file_count} files, {actual_size:,} bytes)")
            return backup_metadata
            
        except Exception as e:
            logger.error(f"Backup failed: {e}")
            
            # Create failed backup metadata
            backup_metadata = BackupMetadata(
                backup_id=backup_id,
                timestamp=timestamp,
                date=date,
                backup_type=backup_type,
                source_path=source_path,
                destination_path=dest_path,
                file_count=0,
                total_size_bytes=0,
                compression_ratio=1.0,
                checksum="",
                status="failed",
                error_message=str(e)
            )
            
            self._store_backup_metadata(backup_metadata)
            return backup_metadata
    
    def _backup_with_compression(self, source_path: str, dest_path: str):
        """Backup with compression using tar.gz."""
        backup_file = os.path.join(dest_path, "storage_backup.tar.gz")
        
        with tarfile.open(backup_file, "w:gz") as tar:
            tar.add(source_path, arcname="storage", filter=self._tar_filter)
        
        logger.info(f"Compressed backup created: {backup_file}")
    
    def _backup_with_shutil(self, source_path: str, dest_path: str):
        """Backup using shutil.copytree."""
        dest_storage = os.path.join(dest_path, "storage")
        
        if os.path.exists(dest_storage):
            shutil.rmtree(dest_storage)
        
        shutil.copytree(source_path, dest_storage, ignore=self._shutil_ignore)
        logger.info(f"Directory backup created: {dest_storage}")
    
    def _tar_filter(self, tarinfo):
        """Filter function for tar backup."""
        if self._should_exclude_file(tarinfo.name):
            return None
        return tarinfo
    
    def _shutil_ignore(self, directory, files):
        """Ignore function for shutil backup."""
        ignored = []
        for file in files:
            file_path = os.path.join(directory, file)
            if self._should_exclude_file(file_path):
                ignored.append(file)
        return ignored
    
    def _backup_database(self, dest_path: str):
        """Backup database file."""
        db_source = self.backup_config.db_path
        db_dest = os.path.join(dest_path, "images.db")
        
        if os.path.exists(db_source):
            shutil.copy2(db_source, db_dest)
            logger.info(f"Database backed up: {db_dest}")
        else:
            logger.warning(f"Database file not found: {db_source}")
    
    def restore_backup(self, src_path: str, restore_path: str = None) -> bool:
        """
        Restore backup from source path.
        
        Args:
            src_path: Source path of backup
            restore_path: Path to restore to (default: original storage path)
            
        Returns:
            True if restore successful, False otherwise
        """
        if restore_path is None:
            restore_path = self.backup_config.storage_path
        
        logger.info(f"Starting restore from {src_path} to {restore_path}")
        
        try:
            # Check if source exists
            if not os.path.exists(src_path):
                raise FileNotFoundError(f"Backup source does not exist: {src_path}")
            
            # Create restore directory
            Path(restore_path).mkdir(parents=True, exist_ok=True)
            
            # Check if it's a compressed backup
            if src_path.endswith('.tar.gz'):
                self._restore_from_compressed(src_path, restore_path)
            else:
                self._restore_from_directory(src_path, restore_path)
            
            logger.info(f"Restore completed: {src_path} -> {restore_path}")
            return True
            
        except Exception as e:
            logger.error(f"Restore failed: {e}")
            return False
    
    def _restore_from_compressed(self, src_path: str, restore_path: str):
        """Restore from compressed backup."""
        with tarfile.open(src_path, "r:gz") as tar:
            tar.extractall(restore_path)
        logger.info(f"Restored from compressed backup: {src_path}")
    
    def _restore_from_directory(self, src_path: str, restore_path: str):
        """Restore from directory backup."""
        # Find storage directory in backup
        storage_backup = os.path.join(src_path, "storage")
        
        if os.path.exists(storage_backup):
            # Copy storage directory
            if os.path.exists(restore_path):
                shutil.rmtree(restore_path)
            shutil.copytree(storage_backup, restore_path)
            logger.info(f"Restored storage directory: {storage_backup} -> {restore_path}")
        else:
            # Copy entire backup directory
            if os.path.exists(restore_path):
                shutil.rmtree(restore_path)
            shutil.copytree(src_path, restore_path)
            logger.info(f"Restored entire backup: {src_path} -> {restore_path}")
    
    def _store_backup_metadata(self, backup_metadata: BackupMetadata):
        """Store backup metadata in database."""
        try:
            with sqlite3.connect(self.backup_db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT OR REPLACE INTO backup_metadata 
                    (backup_id, timestamp, date, backup_type, source_path, destination_path,
                     file_count, total_size_bytes, compression_ratio, checksum, status,
                     error_message, created_at, expires_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    backup_metadata.backup_id,
                    backup_metadata.timestamp,
                    backup_metadata.date,
                    backup_metadata.backup_type,
                    backup_metadata.source_path,
                    backup_metadata.destination_path,
                    backup_metadata.file_count,
                    backup_metadata.total_size_bytes,
                    backup_metadata.compression_ratio,
                    backup_metadata.checksum,
                    backup_metadata.status,
                    backup_metadata.error_message,
                    backup_metadata.created_at,
                    backup_metadata.expires_at
                ))
                conn.commit()
        except Exception as e:
            logger.error(f"Failed to store backup metadata: {e}")
    
    def get_backup_list(self, limit: int = 10) -> List[BackupMetadata]:
        """Get list of recent backups."""
        try:
            with sqlite3.connect(self.backup_db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT * FROM backup_metadata 
                    ORDER BY timestamp DESC 
                    LIMIT ?
                """, (limit,))
                rows = cursor.fetchall()
                
                backups = []
                for row in rows:
                    backup = BackupMetadata(
                        backup_id=row[0],
                        timestamp=row[1],
                        date=row[2],
                        backup_type=row[3],
                        source_path=row[4],
                        destination_path=row[5],
                        file_count=row[6],
                        total_size_bytes=row[7],
                        compression_ratio=row[8],
                        checksum=row[9],
                        status=row[10],
                        error_message=row[11],
                        created_at=row[12],
                        expires_at=row[13]
                    )
                    backups.append(backup)
                
                return backups
        except Exception as e:
            logger.error(f"Failed to get backup list: {e}")
            return []
    
    def cleanup_old_backups(self):
        """Clean up old backups based on retention policy."""
        try:
            cutoff_date = datetime.now() - timedelta(days=self.backup_config.retention_days)
            cutoff_timestamp = cutoff_date.timestamp()
            
            with sqlite3.connect(self.backup_db_path) as conn:
                cursor = conn.cursor()
                
                # Get old backups
                cursor.execute("""
                    SELECT backup_id, destination_path FROM backup_metadata 
                    WHERE timestamp < ? AND status = 'success'
                """, (cutoff_timestamp,))
                old_backups = cursor.fetchall()
                
                # Remove old backup files
                for backup_id, dest_path in old_backups:
                    try:
                        if os.path.exists(dest_path):
                            if os.path.isdir(dest_path):
                                shutil.rmtree(dest_path)
                            else:
                                os.remove(dest_path)
                            logger.info(f"Removed old backup: {dest_path}")
                    except Exception as e:
                        logger.error(f"Failed to remove backup {dest_path}: {e}")
                
                # Remove old metadata
                cursor.execute("DELETE FROM backup_metadata WHERE timestamp < ?", (cutoff_timestamp,))
                deleted_count = cursor.rowcount
                conn.commit()
                
                logger.info(f"Cleaned up {deleted_count} old backup records")
                
        except Exception as e:
            logger.error(f"Failed to cleanup old backups: {e}")
    
    def get_backup_stats(self) -> Dict[str, Any]:
        """Get backup statistics."""
        try:
            with sqlite3.connect(self.backup_db_path) as conn:
                cursor = conn.cursor()
                
                # Get total backups
                cursor.execute("SELECT COUNT(*) FROM backup_metadata")
                total_backups = cursor.fetchone()[0]
                
                # Get successful backups
                cursor.execute("SELECT COUNT(*) FROM backup_metadata WHERE status = 'success'")
                successful_backups = cursor.fetchone()[0]
                
                # Get total size
                cursor.execute("SELECT SUM(total_size_bytes) FROM backup_metadata WHERE status = 'success'")
                total_size = cursor.fetchone()[0] or 0
                
                # Get recent backups
                cursor.execute("""
                    SELECT COUNT(*) FROM backup_metadata 
                    WHERE timestamp > ?
                """, (time.time() - 86400,))  # Last 24 hours
                recent_backups = cursor.fetchone()[0]
                
                return {
                    "total_backups": total_backups,
                    "successful_backups": successful_backups,
                    "failed_backups": total_backups - successful_backups,
                    "total_size_bytes": total_size,
                    "recent_backups": recent_backups,
                    "retention_days": self.backup_config.retention_days,
                    "max_backups": self.backup_config.max_backups
                }
                
        except Exception as e:
            logger.error(f"Failed to get backup stats: {e}")
            return {}


# Global backup manager instance
_backup_manager: Optional[BackupManager] = None


def get_backup_manager() -> BackupManager:
    """Get global backup manager instance."""
    global _backup_manager
    if _backup_manager is None:
        _backup_manager = BackupManager()
    return _backup_manager


def backup_storage(dest_path: str, backup_type: str = "full") -> BackupMetadata:
    """Backup storage directory."""
    return get_backup_manager().backup_storage(dest_path, backup_type)


def restore_backup(src_path: str, restore_path: str = None) -> bool:
    """Restore backup from source."""
    return get_backup_manager().restore_backup(src_path, restore_path)


def get_backup_list(limit: int = 10) -> List[BackupMetadata]:
    """Get list of recent backups."""
    return get_backup_manager().get_backup_list(limit)


def cleanup_old_backups():
    """Clean up old backups."""
    get_backup_manager().cleanup_old_backups()


def get_backup_stats() -> Dict[str, Any]:
    """Get backup statistics."""
    return get_backup_manager().get_backup_stats()
