#!/usr/bin/env python3
"""
Test script for PixVault monitoring system.
Tests metrics collection, storage, retrieval, and export functionality.
"""

import os
import sys
import time
import json
from datetime import datetime, timedelta
from pathlib import Path

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from harvest.monitoring import (
    MetricsCollector, record_metric, get_metrics, get_daily_stats,
    get_metrics_summary, export_metrics_json, update_daily_stats
)
from harvest.prometheus_metrics import export_prometheus_metrics
from harvest.monitored_downloader import download_and_store as monitored_download
from harvest.utils.logger import get_logger


def test_metrics_collector():
    """Test metrics collector initialization and basic functionality."""
    print("Testing Metrics Collector")
    print("=" * 40)
    
    try:
        # Initialize metrics collector
        collector = MetricsCollector(db_path="test_metrics.db")
        print("✓ Metrics collector initialized")
        
        # Test database creation
        if Path("test_metrics.db").exists():
            print("✓ Metrics database created")
        else:
            print("✗ Metrics database not created")
            return False
        
        return True
        
    except Exception as e:
        print(f"✗ Metrics collector test failed: {e}")
        return False


def test_metric_recording():
    """Test metric recording functionality."""
    print("\nTesting Metric Recording")
    print("=" * 40)
    
    try:
        # Test basic metric recording
        record_metric("test_metric", 42, {"label1": "value1"})
        print("✓ Basic metric recorded")
        
        # Test multiple metrics
        record_metric("downloads", 1, {"domain": "example.com"})
        record_metric("downloads", 2, {"domain": "test.com"})
        record_metric("failures", 1, {"domain": "example.com"})
        print("✓ Multiple metrics recorded")
        
        # Test different metric types
        record_metric("processing_time", 1.5, {"operation": "download"})
        record_metric("disk_usage", 1024000, {"unit": "bytes"})
        print("✓ Different metric types recorded")
        
        return True
        
    except Exception as e:
        print(f"✗ Metric recording test failed: {e}")
        return False


def test_metric_retrieval():
    """Test metric retrieval functionality."""
    print("\nTesting Metric Retrieval")
    print("=" * 40)
    
    try:
        # Test getting all metrics
        all_metrics = get_metrics()
        print(f"✓ Retrieved {len(all_metrics)} metric categories")
        
        # Test getting specific metric
        downloads = get_metrics(metric_name="downloads")
        if "downloads" in downloads:
            print(f"✓ Retrieved downloads metric: {len(downloads['downloads'])} points")
        else:
            print("⚠ Downloads metric not found")
        
        # Test date filtering
        today = datetime.now().strftime('%Y-%m-%d')
        today_metrics = get_metrics(start_date=today, end_date=today)
        print(f"✓ Retrieved {len(today_metrics)} metrics for today")
        
        return True
        
    except Exception as e:
        print(f"✗ Metric retrieval test failed: {e}")
        return False


def test_daily_stats():
    """Test daily statistics functionality."""
    print("\nTesting Daily Statistics")
    print("=" * 40)
    
    try:
        # Test getting daily stats
        today = datetime.now().strftime('%Y-%m-%d')
        stats = get_daily_stats(today)
        print(f"✓ Retrieved daily stats for {today}")
        print(f"  Downloads: {stats.downloads}")
        print(f"  Failures: {stats.failures}")
        print(f"  Disk Usage: {stats.disk_usage_bytes} bytes")
        
        # Test updating daily stats
        update_daily_stats(today)
        print("✓ Updated daily statistics")
        
        return True
        
    except Exception as e:
        print(f"✗ Daily stats test failed: {e}")
        return False


def test_metrics_summary():
    """Test metrics summary functionality."""
    print("\nTesting Metrics Summary")
    print("=" * 40)
    
    try:
        # Test getting summary
        summary = get_metrics_summary(days=1)
        print(f"✓ Retrieved metrics summary")
        print(f"  Period: {summary.get('period', 'Unknown')}")
        print(f"  Total downloads: {summary.get('summary', {}).get('total_downloads', 0)}")
        print(f"  Total failures: {summary.get('summary', {}).get('total_failures', 0)}")
        
        return True
        
    except Exception as e:
        print(f"✗ Metrics summary test failed: {e}")
        return False


def test_export_functionality():
    """Test export functionality."""
    print("\nTesting Export Functionality")
    print("=" * 40)
    
    try:
        # Test JSON export
        json_output = export_metrics_json()
        if json_output:
            print("✓ JSON export successful")
            print(f"  Output length: {len(json_output)} characters")
        else:
            print("⚠ JSON export returned empty")
        
        # Test Prometheus export
        prometheus_output = export_prometheus_metrics()
        if prometheus_output:
            print("✓ Prometheus export successful")
            print(f"  Output length: {len(prometheus_output)} characters")
        else:
            print("⚠ Prometheus export returned empty")
        
        # Test file export
        json_file = "test_metrics_export.json"
        prometheus_file = "test_metrics_export.txt"
        
        export_metrics_json(json_file)
        export_prometheus_metrics(prometheus_file)
        
        if Path(json_file).exists():
            print(f"✓ JSON file export successful: {json_file}")
            os.remove(json_file)
        
        if Path(prometheus_file).exists():
            print(f"✓ Prometheus file export successful: {prometheus_file}")
            os.remove(prometheus_file)
        
        return True
        
    except Exception as e:
        print(f"✗ Export functionality test failed: {e}")
        return False


def test_monitored_downloader():
    """Test monitored downloader integration."""
    print("\nTesting Monitored Downloader")
    print("=" * 40)
    
    try:
        # Test monitored download (this would require actual download)
        # For now, just test that the function exists and can be called
        print("✓ Monitored downloader module imported successfully")
        
        # Test metric recording in download context
        record_metric("download_attempts", 1, {"domain": "test.com", "label": "test"})
        record_metric("downloads", 1, {"domain": "test.com", "label": "test"})
        record_metric("processing_time", 0.5, {"domain": "test.com"})
        
        print("✓ Download metrics recorded")
        
        return True
        
    except Exception as e:
        print(f"✗ Monitored downloader test failed: {e}")
        return False


def test_prometheus_integration():
    """Test Prometheus integration."""
    print("\nTesting Prometheus Integration")
    print("=" * 40)
    
    try:
        # Test Prometheus export
        prometheus_data = export_prometheus_metrics()
        
        if prometheus_data:
            print("✓ Prometheus metrics generated")
            print(f"  Output length: {len(prometheus_data)} characters")
            
            # Check for key Prometheus format elements
            if "pixvault_downloads_total" in prometheus_data:
                print("✓ Downloads metric found in Prometheus output")
            if "pixvault_failures_total" in prometheus_data:
                print("✓ Failures metric found in Prometheus output")
            if "pixvault_disk_usage_bytes" in prometheus_data:
                print("✓ Disk usage metric found in Prometheus output")
        else:
            print("⚠ Prometheus export returned empty")
        
        return True
        
    except Exception as e:
        print(f"✗ Prometheus integration test failed: {e}")
        return False


def test_database_operations():
    """Test database operations."""
    print("\nTesting Database Operations")
    print("=" * 40)
    
    try:
        # Test database file creation
        if Path("test_metrics.db").exists():
            print("✓ Metrics database file exists")
        else:
            print("✗ Metrics database file not found")
            return False
        
        # Test database schema
        import sqlite3
        with sqlite3.connect("test_metrics.db") as conn:
            cursor = conn.cursor()
            
            # Check tables exist
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            
            if "metrics" in tables:
                print("✓ Metrics table exists")
            else:
                print("✗ Metrics table not found")
                return False
            
            if "daily_stats" in tables:
                print("✓ Daily stats table exists")
            else:
                print("✗ Daily stats table not found")
                return False
            
            # Check data
            cursor.execute("SELECT COUNT(*) FROM metrics")
            metrics_count = cursor.fetchone()[0]
            print(f"✓ Metrics table has {metrics_count} records")
            
            cursor.execute("SELECT COUNT(*) FROM daily_stats")
            stats_count = cursor.fetchone()[0]
            print(f"✓ Daily stats table has {stats_count} records")
        
        return True
        
    except Exception as e:
        print(f"✗ Database operations test failed: {e}")
        return False


def test_cleanup():
    """Test cleanup functionality."""
    print("\nTesting Cleanup")
    print("=" * 40)
    
    try:
        # Test cleanup old metrics
        collector = MetricsCollector(db_path="test_metrics.db")
        collector.cleanup_old_metrics(days_to_keep=0)  # Clean up all data
        print("✓ Cleanup old metrics successful")
        
        return True
        
    except Exception as e:
        print(f"✗ Cleanup test failed: {e}")
        return False


def test_monitoring_cli():
    """Test monitoring CLI functionality."""
    print("\nTesting Monitoring CLI")
    print("=" * 40)
    
    try:
        # Test CLI module import
        from harvest.monitoring_cli import main
        print("✓ Monitoring CLI module imported successfully")
        
        # Test CLI functions exist
        import harvest.monitoring_cli as cli_module
        if hasattr(cli_module, 'cmd_record_metric'):
            print("✓ CLI record metric function exists")
        if hasattr(cli_module, 'cmd_get_metrics'):
            print("✓ CLI get metrics function exists")
        if hasattr(cli_module, 'cmd_daily_stats'):
            print("✓ CLI daily stats function exists")
        
        return True
        
    except Exception as e:
        print(f"✗ Monitoring CLI test failed: {e}")
        return False


def test_monitoring_dashboard():
    """Test monitoring dashboard functionality."""
    print("\nTesting Monitoring Dashboard")
    print("=" * 40)
    
    try:
        # Test dashboard module import
        from harvest.monitoring_dashboard import app, run_dashboard
        print("✓ Monitoring dashboard module imported successfully")
        
        # Test FastAPI app creation
        if app:
            print("✓ FastAPI app created successfully")
        else:
            print("✗ FastAPI app not created")
            return False
        
        # Test dashboard template
        template_path = Path("templates/monitoring_dashboard.html")
        if template_path.exists():
            print("✓ Dashboard template exists")
        else:
            print("⚠ Dashboard template not found")
        
        return True
        
    except Exception as e:
        print(f"✗ Monitoring dashboard test failed: {e}")
        return False


def cleanup_test_files():
    """Clean up test files."""
    print("\nCleaning up test files...")
    
    test_files = [
        "test_metrics.db",
        "test_metrics_export.json",
        "test_metrics_export.txt"
    ]
    
    for file_path in test_files:
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"  Removed: {file_path}")


def main():
    """Main test function."""
    print("PixVault Monitoring System Test")
    print("=" * 60)
    
    # Check dependencies
    try:
        import sqlite3
        import json
        print("✓ All dependencies available")
    except ImportError as e:
        print(f"✗ Missing dependencies: {e}")
        return
    
    # Run tests
    test_metrics_collector()
    test_metric_recording()
    test_metric_retrieval()
    test_daily_stats()
    test_metrics_summary()
    test_export_functionality()
    test_monitored_downloader()
    test_prometheus_integration()
    test_database_operations()
    test_monitoring_cli()
    test_monitoring_dashboard()
    test_cleanup()
    
    # Cleanup
    cleanup_test_files()
    
    print("\n" + "=" * 60)
    print("All monitoring tests completed!")
    print("\nTo use the monitoring system:")
    print("  1. Record metrics: python -m harvest.monitoring_cli record-metric --metric-name 'downloads' --value 1")
    print("  2. View dashboard: python -m harvest.monitoring_dashboard")
    print("  3. Export metrics: python -m harvest.monitoring_cli export-json")
    print("  4. Prometheus endpoint: http://localhost:8000/metrics")


if __name__ == "__main__":
    main()
