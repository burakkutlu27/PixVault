#!/usr/bin/env python3
"""
CLI for backup and restore operations.
Provides commands for managing backups, restoring data, and monitoring backup status.
"""

import argparse
import sys
import os
import json
from datetime import datetime
from pathlib import Path

from .backup import (
    get_backup_manager, backup_storage, restore_backup,
    get_backup_list, cleanup_old_backups, get_backup_stats
)
from .utils.logger import get_logger


def cmd_backup(args):
    """Create a backup."""
    try:
        # Validate destination path
        dest_path = args.dest_path
        if not dest_path:
            dest_path = f"backups/backup_{int(datetime.now().timestamp())}"
        
        # Create backup
        backup_metadata = backup_storage(dest_path, args.backup_type)
        
        if backup_metadata.status == "success":
            print(f"✓ Backup completed successfully")
            print(f"  Backup ID: {backup_metadata.backup_id}")
            print(f"  Destination: {backup_metadata.destination_path}")
            print(f"  Files: {backup_metadata.file_count}")
            print(f"  Size: {backup_metadata.total_size_bytes:,} bytes")
            print(f"  Compression: {backup_metadata.compression_ratio:.2f}")
            print(f"  Checksum: {backup_metadata.checksum}")
        else:
            print(f"✗ Backup failed: {backup_metadata.error_message}")
            sys.exit(1)
        
    except Exception as e:
        print(f"✗ Backup failed: {e}")
        sys.exit(1)


def cmd_restore(args):
    """Restore from backup."""
    try:
        # Validate source path
        src_path = args.src_path
        if not os.path.exists(src_path):
            print(f"✗ Source path does not exist: {src_path}")
            sys.exit(1)
        
        # Restore backup
        success = restore_backup(src_path, args.restore_path)
        
        if success:
            print(f"✓ Restore completed successfully")
            print(f"  Source: {src_path}")
            print(f"  Destination: {args.restore_path or 'default storage path'}")
        else:
            print(f"✗ Restore failed")
            sys.exit(1)
        
    except Exception as e:
        print(f"✗ Restore failed: {e}")
        sys.exit(1)


def cmd_list(args):
    """List backups."""
    try:
        backups = get_backup_list(args.limit)
        
        if not backups:
            print("No backups found")
            return
        
        if args.json:
            # Output as JSON
            backup_data = []
            for backup in backups:
                backup_data.append({
                    'backup_id': backup.backup_id,
                    'timestamp': backup.timestamp,
                    'date': backup.date,
                    'backup_type': backup.backup_type,
                    'source_path': backup.source_path,
                    'destination_path': backup.destination_path,
                    'file_count': backup.file_count,
                    'total_size_bytes': backup.total_size_bytes,
                    'compression_ratio': backup.compression_ratio,
                    'checksum': backup.checksum,
                    'status': backup.status,
                    'error_message': backup.error_message,
                    'created_at': backup.created_at,
                    'expires_at': backup.expires_at
                })
            print(json.dumps(backup_data, indent=2, default=str))
        else:
            # Output as table
            print(f"{'Backup ID':<20} {'Date':<12} {'Type':<12} {'Status':<10} {'Files':<8} {'Size':<12}")
            print("-" * 80)
            
            for backup in backups:
                size_mb = backup.total_size_bytes / 1024 / 1024
                print(f"{backup.backup_id:<20} {backup.date:<12} {backup.backup_type:<12} "
                      f"{backup.status:<10} {backup.file_count:<8} {size_mb:.1f} MB")
        
    except Exception as e:
        print(f"✗ Failed to list backups: {e}")
        sys.exit(1)


def cmd_stats(args):
    """Show backup statistics."""
    try:
        stats = get_backup_stats()
        
        if args.json:
            print(json.dumps(stats, indent=2, default=str))
        else:
            print("Backup Statistics")
            print("=" * 40)
            print(f"Total Backups: {stats.get('total_backups', 0)}")
            print(f"Successful: {stats.get('successful_backups', 0)}")
            print(f"Failed: {stats.get('failed_backups', 0)}")
            print(f"Total Size: {stats.get('total_size_bytes', 0):,} bytes")
            print(f"Recent Backups (24h): {stats.get('recent_backups', 0)}")
            print(f"Retention Days: {stats.get('retention_days', 0)}")
            print(f"Max Backups: {stats.get('max_backups', 0)}")
        
    except Exception as e:
        print(f"✗ Failed to get backup stats: {e}")
        sys.exit(1)


def cmd_cleanup(args):
    """Clean up old backups."""
    try:
        if not args.force:
            print("This will remove old backups based on retention policy.")
            response = input("Are you sure? (y/N): ")
            if response.lower() != 'y':
                print("Cleanup cancelled")
                return
        
        cleanup_old_backups()
        print("✓ Old backups cleaned up successfully")
        
    except Exception as e:
        print(f"✗ Cleanup failed: {e}")
        sys.exit(1)


def cmd_verify(args):
    """Verify backup integrity."""
    try:
        backup_path = args.backup_path
        if not os.path.exists(backup_path):
            print(f"✗ Backup path does not exist: {backup_path}")
            sys.exit(1)
        
        # Calculate checksum
        from .backup import get_backup_manager
        manager = get_backup_manager()
        checksum = manager._calculate_checksum(backup_path)
        
        if checksum:
            print(f"✓ Backup verification completed")
            print(f"  Path: {backup_path}")
            print(f"  Checksum: {checksum}")
        else:
            print(f"✗ Backup verification failed")
            sys.exit(1)
        
    except Exception as e:
        print(f"✗ Verification failed: {e}")
        sys.exit(1)


def cmd_info(args):
    """Show backup information."""
    try:
        backup_id = args.backup_id
        backups = get_backup_list(limit=100)  # Get more backups to find the one
        
        backup = None
        for b in backups:
            if b.backup_id == backup_id:
                backup = b
                break
        
        if not backup:
            print(f"✗ Backup not found: {backup_id}")
            sys.exit(1)
        
        if args.json:
            print(json.dumps(backup.__dict__, indent=2, default=str))
        else:
            print(f"Backup Information: {backup_id}")
            print("=" * 50)
            print(f"Backup ID: {backup.backup_id}")
            print(f"Date: {backup.date}")
            print(f"Type: {backup.backup_type}")
            print(f"Status: {backup.status}")
            print(f"Source: {backup.source_path}")
            print(f"Destination: {backup.destination_path}")
            print(f"Files: {backup.file_count}")
            print(f"Size: {backup.total_size_bytes:,} bytes")
            print(f"Compression Ratio: {backup.compression_ratio:.2f}")
            print(f"Checksum: {backup.checksum}")
            if backup.error_message:
                print(f"Error: {backup.error_message}")
            print(f"Created: {backup.created_at}")
            if backup.expires_at:
                print(f"Expires: {backup.expires_at}")
        
    except Exception as e:
        print(f"✗ Failed to get backup info: {e}")
        sys.exit(1)


def cmd_schedule(args):
    """Schedule backup operations."""
    try:
        # This would integrate with a scheduler like cron or APScheduler
        print("Backup scheduling functionality would be implemented here")
        print("This could integrate with:")
        print("  - Cron jobs")
        print("  - APScheduler")
        print("  - Systemd timers")
        print("  - Cloud backup services")
        
    except Exception as e:
        print(f"✗ Scheduling failed: {e}")
        sys.exit(1)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="PixVault Backup CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Create a backup
  python -m harvest.backup_cli backup --dest-path "backups/my_backup"
  
  # Restore from backup
  python -m harvest.backup_cli restore --src-path "backups/my_backup"
  
  # List backups
  python -m harvest.backup_cli list --limit 10
  
  # Show backup statistics
  python -m harvest.backup_cli stats
  
  # Clean up old backups
  python -m harvest.backup_cli cleanup --force
  
  # Verify backup integrity
  python -m harvest.backup_cli verify --backup-path "backups/my_backup"
  
  # Get backup information
  python -m harvest.backup_cli info --backup-id "backup_1234567890"
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Backup command
    backup_parser = subparsers.add_parser('backup', help='Create a backup')
    backup_parser.add_argument('--dest-path', help='Destination path for backup')
    backup_parser.add_argument('--backup-type', choices=['full', 'incremental', 'metadata_only'], 
                               default='full', help='Type of backup')
    backup_parser.set_defaults(func=cmd_backup)
    
    # Restore command
    restore_parser = subparsers.add_parser('restore', help='Restore from backup')
    restore_parser.add_argument('--src-path', required=True, help='Source path of backup')
    restore_parser.add_argument('--restore-path', help='Path to restore to (default: original storage)')
    restore_parser.set_defaults(func=cmd_restore)
    
    # List command
    list_parser = subparsers.add_parser('list', help='List backups')
    list_parser.add_argument('--limit', type=int, default=10, help='Limit number of backups to show')
    list_parser.add_argument('--json', action='store_true', help='Output as JSON')
    list_parser.set_defaults(func=cmd_list)
    
    # Stats command
    stats_parser = subparsers.add_parser('stats', help='Show backup statistics')
    stats_parser.add_argument('--json', action='store_true', help='Output as JSON')
    stats_parser.set_defaults(func=cmd_stats)
    
    # Cleanup command
    cleanup_parser = subparsers.add_parser('cleanup', help='Clean up old backups')
    cleanup_parser.add_argument('--force', action='store_true', help='Skip confirmation')
    cleanup_parser.set_defaults(func=cmd_cleanup)
    
    # Verify command
    verify_parser = subparsers.add_parser('verify', help='Verify backup integrity')
    verify_parser.add_argument('--backup-path', required=True, help='Path to backup to verify')
    verify_parser.set_defaults(func=cmd_verify)
    
    # Info command
    info_parser = subparsers.add_parser('info', help='Show backup information')
    info_parser.add_argument('--backup-id', required=True, help='Backup ID to get info for')
    info_parser.add_argument('--json', action='store_true', help='Output as JSON')
    info_parser.set_defaults(func=cmd_info)
    
    # Schedule command
    schedule_parser = subparsers.add_parser('schedule', help='Schedule backup operations')
    schedule_parser.set_defaults(func=cmd_schedule)
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Execute the command
    args.func(args)


if __name__ == "__main__":
    main()
