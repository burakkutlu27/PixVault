#!/usr/bin/env python3
"""
Test script for PixVault backup system.
Tests backup, restore, encryption, compression, and CLI functionality.
"""

import os
import sys
import time
import shutil
from pathlib import Path
from datetime import datetime

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from harvest.backup import (
    BackupManager, backup_storage, restore_backup,
    get_backup_list, cleanup_old_backups, get_backup_stats
)
from harvest.backup_encryption import (
    BackupEncryption, BackupCompression, SecureBackup,
    create_secure_backup, restore_secure_backup, verify_secure_backup
)
from harvest.utils.logger import get_logger


def test_backup_manager():
    """Test backup manager initialization."""
    print("Testing Backup Manager")
    print("=" * 40)
    
    try:
        # Initialize backup manager
        manager = BackupManager()
        print("✓ Backup manager initialized")
        
        # Test configuration
        print(f"  Storage path: {manager.backup_config.storage_path}")
        print(f"  Database path: {manager.backup_config.db_path}")
        print(f"  Backup root: {manager.backup_config.backup_root}")
        print(f"  Compression: {manager.backup_config.compression}")
        print(f"  Retention days: {manager.backup_config.retention_days}")
        
        return True
        
    except Exception as e:
        print(f"✗ Backup manager test failed: {e}")
        return False


def test_basic_backup():
    """Test basic backup functionality."""
    print("\nTesting Basic Backup")
    print("=" * 40)
    
    try:
        # Create test storage directory
        test_storage = "test_storage"
        test_backup = "test_backup"
        
        # Create test files
        Path(test_storage).mkdir(exist_ok=True)
        (Path(test_storage) / "test1.jpg").write_text("test image 1")
        (Path(test_storage) / "test2.jpg").write_text("test image 2")
        (Path(test_storage) / "subdir").mkdir(exist_ok=True)
        (Path(test_storage) / "subdir" / "test3.jpg").write_text("test image 3")
        
        print(f"✓ Test storage created: {test_storage}")
        
        # Create backup
        backup_metadata = backup_storage(test_backup, "full")
        
        if backup_metadata.status == "success":
            print(f"✓ Backup created successfully")
            print(f"  Backup ID: {backup_metadata.backup_id}")
            print(f"  Files: {backup_metadata.file_count}")
            print(f"  Size: {backup_metadata.total_size_bytes} bytes")
            print(f"  Compression: {backup_metadata.compression_ratio:.2f}")
        else:
            print(f"✗ Backup failed: {backup_metadata.error_message}")
            return False
        
        # Clean up
        if os.path.exists(test_storage):
            shutil.rmtree(test_storage)
        if os.path.exists(test_backup):
            shutil.rmtree(test_backup)
        
        return True
        
    except Exception as e:
        print(f"✗ Basic backup test failed: {e}")
        return False


def test_backup_restore():
    """Test backup and restore functionality."""
    print("\nTesting Backup and Restore")
    print("=" * 40)
    
    try:
        # Create test storage
        test_storage = "test_storage"
        test_backup = "test_backup"
        test_restore = "test_restore"
        
        # Create test files
        Path(test_storage).mkdir(exist_ok=True)
        (Path(test_storage) / "test1.jpg").write_text("test image 1")
        (Path(test_storage) / "test2.jpg").write_text("test image 2")
        
        print(f"✓ Test storage created: {test_storage}")
        
        # Create backup
        backup_metadata = backup_storage(test_backup, "full")
        if backup_metadata.status != "success":
            print(f"✗ Backup failed: {backup_metadata.error_message}")
            return False
        
        print(f"✓ Backup created: {backup_metadata.backup_id}")
        
        # Restore backup
        success = restore_backup(test_backup, test_restore)
        if not success:
            print(f"✗ Restore failed")
            return False
        
        print(f"✓ Restore completed")
        
        # Verify restore
        if os.path.exists(test_restore):
            files = list(Path(test_restore).rglob("*"))
            print(f"  Restored files: {len(files)}")
            
            # Check if files exist
            if (Path(test_restore) / "test1.jpg").exists():
                print(f"  ✓ test1.jpg restored")
            if (Path(test_restore) / "test2.jpg").exists():
                print(f"  ✓ test2.jpg restored")
        else:
            print(f"✗ Restore directory not found")
            return False
        
        # Clean up
        for path in [test_storage, test_backup, test_restore]:
            if os.path.exists(path):
                shutil.rmtree(path)
        
        return True
        
    except Exception as e:
        print(f"✗ Backup and restore test failed: {e}")
        return False


def test_compression():
    """Test compression functionality."""
    print("\nTesting Compression")
    print("=" * 40)
    
    try:
        # Create test directory
        test_dir = "test_compression"
        test_file = "test_compression.tar.gz"
        
        Path(test_dir).mkdir(exist_ok=True)
        (Path(test_dir) / "test1.txt").write_text("test content 1")
        (Path(test_dir) / "test2.txt").write_text("test content 2")
        
        print(f"✓ Test directory created: {test_dir}")
        
        # Test compression
        compression = BackupCompression()
        success = compression.compress_directory(test_dir, test_file)
        
        if success:
            print(f"✓ Directory compressed: {test_file}")
            print(f"  Original size: {sum(f.stat().st_size for f in Path(test_dir).rglob('*') if f.is_file())} bytes")
            print(f"  Compressed size: {os.path.getsize(test_file)} bytes")
        else:
            print(f"✗ Compression failed")
            return False
        
        # Test decompression
        decompress_dir = "test_decompress"
        success = compression.decompress_directory(test_file, decompress_dir)
        
        if success:
            print(f"✓ Directory decompressed: {decompress_dir}")
            files = list(Path(decompress_dir).rglob("*"))
            print(f"  Decompressed files: {len(files)}")
        else:
            print(f"✗ Decompression failed")
            return False
        
        # Clean up
        for path in [test_dir, test_file, decompress_dir]:
            if os.path.exists(path):
                if os.path.isdir(path):
                    shutil.rmtree(path)
                else:
                    os.remove(path)
        
        return True
        
    except Exception as e:
        print(f"✗ Compression test failed: {e}")
        return False


def test_encryption():
    """Test encryption functionality."""
    print("\nTesting Encryption")
    print("=" * 40)
    
    try:
        # Create test file
        test_file = "test_encryption.txt"
        encrypted_file = "test_encrypted.enc"
        decrypted_file = "test_decrypted.txt"
        
        with open(test_file, 'w') as f:
            f.write("This is a test file for encryption")
        
        print(f"✓ Test file created: {test_file}")
        
        # Test encryption
        encryption = BackupEncryption("test_password")
        success = encryption.encrypt_file(test_file, encrypted_file)
        
        if success:
            print(f"✓ File encrypted: {encrypted_file}")
            print(f"  Original size: {os.path.getsize(test_file)} bytes")
            print(f"  Encrypted size: {os.path.getsize(encrypted_file)} bytes")
        else:
            print(f"✗ Encryption failed")
            return False
        
        # Test decryption
        success = encryption.decrypt_file(encrypted_file, decrypted_file)
        
        if success:
            print(f"✓ File decrypted: {decrypted_file}")
            
            # Verify content
            with open(test_file, 'r') as f:
                original_content = f.read()
            with open(decrypted_file, 'r') as f:
                decrypted_content = f.read()
            
            if original_content == decrypted_content:
                print(f"  ✓ Content matches original")
            else:
                print(f"  ✗ Content mismatch")
                return False
        else:
            print(f"✗ Decryption failed")
            return False
        
        # Clean up
        for path in [test_file, encrypted_file, decrypted_file]:
            if os.path.exists(path):
                os.remove(path)
        
        return True
        
    except Exception as e:
        print(f"✗ Encryption test failed: {e}")
        return False


def test_secure_backup():
    """Test secure backup with encryption and compression."""
    print("\nTesting Secure Backup")
    print("=" * 40)
    
    try:
        # Create test directory
        test_dir = "test_secure"
        secure_backup = "test_secure_backup.enc"
        restore_dir = "test_secure_restore"
        
        Path(test_dir).mkdir(exist_ok=True)
        (Path(test_dir) / "test1.txt").write_text("secure test content 1")
        (Path(test_dir) / "test2.txt").write_text("secure test content 2")
        
        print(f"✓ Test directory created: {test_dir}")
        
        # Test secure backup
        success = create_secure_backup(test_dir, secure_backup, "test_password")
        
        if success:
            print(f"✓ Secure backup created: {secure_backup}")
            print(f"  Backup size: {os.path.getsize(secure_backup)} bytes")
        else:
            print(f"✗ Secure backup failed")
            return False
        
        # Test secure restore
        success = restore_secure_backup(secure_backup, restore_dir, "test_password")
        
        if success:
            print(f"✓ Secure restore completed: {restore_dir}")
            files = list(Path(restore_dir).rglob("*"))
            print(f"  Restored files: {len(files)}")
        else:
            print(f"✗ Secure restore failed")
            return False
        
        # Test backup verification
        success = verify_secure_backup(secure_backup, "test_password")
        
        if success:
            print(f"✓ Backup verification successful")
        else:
            print(f"✗ Backup verification failed")
            return False
        
        # Clean up
        for path in [test_dir, secure_backup, restore_dir]:
            if os.path.exists(path):
                if os.path.isdir(path):
                    shutil.rmtree(path)
                else:
                    os.remove(path)
        
        return True
        
    except Exception as e:
        print(f"✗ Secure backup test failed: {e}")
        return False


def test_backup_list():
    """Test backup listing functionality."""
    print("\nTesting Backup List")
    print("=" * 40)
    
    try:
        # Get backup list
        backups = get_backup_list(limit=5)
        
        if backups:
            print(f"✓ Found {len(backups)} backups")
            for backup in backups:
                print(f"  {backup.backup_id}: {backup.date} ({backup.status})")
        else:
            print("✓ No backups found (this is normal for new system)")
        
        return True
        
    except Exception as e:
        print(f"✗ Backup list test failed: {e}")
        return False


def test_backup_stats():
    """Test backup statistics functionality."""
    print("\nTesting Backup Stats")
    print("=" * 40)
    
    try:
        # Get backup stats
        stats = get_backup_stats()
        
        print(f"✓ Backup statistics retrieved")
        print(f"  Total backups: {stats.get('total_backups', 0)}")
        print(f"  Successful: {stats.get('successful_backups', 0)}")
        print(f"  Failed: {stats.get('failed_backups', 0)}")
        print(f"  Total size: {stats.get('total_size_bytes', 0):,} bytes")
        print(f"  Recent backups: {stats.get('recent_backups', 0)}")
        
        return True
        
    except Exception as e:
        print(f"✗ Backup stats test failed: {e}")
        return False


def test_backup_cli():
    """Test backup CLI functionality."""
    print("\nTesting Backup CLI")
    print("=" * 40)
    
    try:
        # Test CLI module import
        from harvest.backup_cli import main
        print("✓ Backup CLI module imported successfully")
        
        # Test CLI functions exist
        import harvest.backup_cli as cli_module
        if hasattr(cli_module, 'cmd_backup'):
            print("✓ CLI backup function exists")
        if hasattr(cli_module, 'cmd_restore'):
            print("✓ CLI restore function exists")
        if hasattr(cli_module, 'cmd_list'):
            print("✓ CLI list function exists")
        if hasattr(cli_module, 'cmd_stats'):
            print("✓ CLI stats function exists")
        
        return True
        
    except Exception as e:
        print(f"✗ Backup CLI test failed: {e}")
        return False


def test_cleanup():
    """Test cleanup functionality."""
    print("\nTesting Cleanup")
    print("=" * 40)
    
    try:
        # Test cleanup old backups
        cleanup_old_backups()
        print("✓ Cleanup completed successfully")
        
        return True
        
    except Exception as e:
        print(f"✗ Cleanup test failed: {e}")
        return False


def test_database_operations():
    """Test database operations."""
    print("\nTesting Database Operations")
    print("=" * 40)
    
    try:
        # Test backup manager database
        manager = BackupManager()
        
        # Check if backup database exists
        if os.path.exists(manager.backup_db_path):
            print(f"✓ Backup database exists: {manager.backup_db_path}")
        else:
            print(f"✗ Backup database not found: {manager.backup_db_path}")
            return False
        
        # Test database schema
        import sqlite3
        with sqlite3.connect(manager.backup_db_path) as conn:
            cursor = conn.cursor()
            
            # Check tables exist
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            
            if "backup_metadata" in tables:
                print("✓ Backup metadata table exists")
            else:
                print("✗ Backup metadata table not found")
                return False
            
            # Check data
            cursor.execute("SELECT COUNT(*) FROM backup_metadata")
            count = cursor.fetchone()[0]
            print(f"✓ Backup metadata table has {count} records")
        
        return True
        
    except Exception as e:
        print(f"✗ Database operations test failed: {e}")
        return False


def cleanup_test_files():
    """Clean up test files."""
    print("\nCleaning up test files...")
    
    test_dirs = [
        "test_storage",
        "test_backup", 
        "test_restore",
        "test_compression",
        "test_decompress",
        "test_secure",
        "test_secure_restore"
    ]
    
    test_files = [
        "test_encryption.txt",
        "test_encrypted.enc",
        "test_decrypted.txt",
        "test_compression.tar.gz",
        "test_secure_backup.enc"
    ]
    
    for path in test_dirs + test_files:
        if os.path.exists(path):
            if os.path.isdir(path):
                shutil.rmtree(path)
                print(f"  Removed directory: {path}")
            else:
                os.remove(path)
                print(f"  Removed file: {path}")


def main():
    """Main test function."""
    print("PixVault Backup System Test")
    print("=" * 60)
    
    # Check dependencies
    try:
        import sqlite3
        import tarfile
        import gzip
        from cryptography.fernet import Fernet
        print("✓ All dependencies available")
    except ImportError as e:
        print(f"✗ Missing dependencies: {e}")
        print("Install with: pip install cryptography")
        return
    
    # Run tests
    test_backup_manager()
    test_basic_backup()
    test_backup_restore()
    test_compression()
    test_encryption()
    test_secure_backup()
    test_backup_list()
    test_backup_stats()
    test_backup_cli()
    test_cleanup()
    test_database_operations()
    
    # Cleanup
    cleanup_test_files()
    
    print("\n" + "=" * 60)
    print("All backup tests completed!")
    print("\nTo use the backup system:")
    print("  1. Create backup: python -m harvest.backup_cli backup --dest-path 'backups/my_backup'")
    print("  2. Restore backup: python -m harvest.backup_cli restore --src-path 'backups/my_backup'")
    print("  3. List backups: python -m harvest.backup_cli list")
    print("  4. Show stats: python -m harvest.backup_cli stats")
    print("  5. Cleanup: python -m harvest.backup_cli cleanup --force")


if __name__ == "__main__":
    main()
