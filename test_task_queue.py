#!/usr/bin/env python3
"""
Test script for task queue system.
Demonstrates usage of Celery tasks, worker management, and monitoring.
"""

import os
import sys
import time
import json
from pathlib import Path

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from harvest.tasks import (
    enqueue_download, enqueue_browser_search, get_task_status,
    cancel_task, get_queue_stats, app
)
from harvest.worker_manager import worker_manager, get_system_status, scale_workers
from harvest.config import load_config


def test_redis_connection():
    """Test Redis connection."""
    print("Testing Redis Connection")
    print("=" * 40)
    
    try:
        import redis
        redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        redis_client.ping()
        print("✓ Redis connection successful")
        return True
    except Exception as e:
        print(f"✗ Redis connection failed: {e}")
        print("  Make sure Redis is running: redis-server")
        return False


def test_celery_connection():
    """Test Celery connection."""
    print("\nTesting Celery Connection")
    print("=" * 40)
    
    try:
        # Test Celery app
        inspect = app.control.inspect()
        stats = inspect.stats()
        
        if stats:
            print(f"✓ Celery connection successful")
            print(f"  Active workers: {len(stats)}")
            for worker_name in stats.keys():
                print(f"    - {worker_name}")
        else:
            print("⚠ Celery connected but no workers found")
            print("  Start workers with: celery -A harvest.tasks worker --loglevel=info")
        
        return True
        
    except Exception as e:
        print(f"✗ Celery connection failed: {e}")
        return False


def test_enqueue_download():
    """Test enqueueing download tasks."""
    print("\nTesting Download Task Enqueueing")
    print("=" * 40)
    
    try:
        # Load configuration
        config = load_config()
        
        # Test URLs (using publicly available test images)
        test_urls = [
            "https://picsum.photos/800/600?random=1",
            "https://picsum.photos/800/600?random=2",
            "https://picsum.photos/800/600?random=3"
        ]
        
        task_ids = []
        
        for i, url in enumerate(test_urls):
            task_data = {
                'url': url,
                'label': f'test_download_{i+1}',
                'config': config,
                'domain': 'picsum.photos',
                'priority': 0
            }
            
            task_id = enqueue_download(task_data)
            task_ids.append(task_id)
            print(f"  Enqueued download {i+1}: {task_id}")
        
        print(f"✓ Enqueued {len(task_ids)} download tasks")
        return task_ids
        
    except Exception as e:
        print(f"✗ Failed to enqueue download tasks: {e}")
        return []


def test_enqueue_browser_search():
    """Test enqueueing browser search tasks."""
    print("\nTesting Browser Search Task Enqueueing")
    print("=" * 40)
    
    try:
        # Load configuration
        config = load_config()
        
        # Test search queries
        test_queries = [
            "nature landscape",
            "abstract art"
        ]
        
        task_ids = []
        
        for i, query in enumerate(test_queries):
            task_data = {
                'query': query,
                'max_results': 5,
                'label': f'test_browser_{i+1}',
                'config': config
            }
            
            task_id = enqueue_browser_search(task_data)
            task_ids.append(task_id)
            print(f"  Enqueued browser search {i+1}: {task_id}")
        
        print(f"✓ Enqueued {len(task_ids)} browser search tasks")
        return task_ids
        
    except Exception as e:
        print(f"✗ Failed to enqueue browser search tasks: {e}")
        return []


def test_task_status(task_ids):
    """Test getting task status."""
    print("\nTesting Task Status")
    print("=" * 40)
    
    if not task_ids:
        print("⚠ No task IDs to test")
        return
    
    for i, task_id in enumerate(task_ids[:3]):  # Test first 3 tasks
        try:
            status = get_task_status(task_id)
            print(f"  Task {i+1} ({task_id[:8]}...): {status['status']}")
            
            if status.get('result'):
                result = status['result']
                if isinstance(result, dict):
                    print(f"    Status: {result.get('status', 'unknown')}")
                    print(f"    Message: {result.get('message', 'no message')[:50]}...")
        except Exception as e:
            print(f"  Task {i+1}: Error getting status - {e}")


def test_queue_stats():
    """Test getting queue statistics."""
    print("\nTesting Queue Statistics")
    print("=" * 40)
    
    try:
        stats = get_queue_stats()
        
        print("Queue Statistics:")
        print(f"  Active Tasks: {len(stats.get('active_tasks', {}))}")
        print(f"  Scheduled Tasks: {len(stats.get('scheduled_tasks', {}))}")
        print(f"  Reserved Tasks: {len(stats.get('reserved_tasks', {}))}")
        
        # Show active tasks by worker
        active_tasks = stats.get('active_tasks', {})
        if active_tasks:
            print("  Active Tasks by Worker:")
            for worker, tasks in active_tasks.items():
                print(f"    {worker}: {len(tasks)} tasks")
        
        print("✓ Queue statistics retrieved successfully")
        
    except Exception as e:
        print(f"✗ Failed to get queue statistics: {e}")


def test_system_status():
    """Test getting system status."""
    print("\nTesting System Status")
    print("=" * 40)
    
    try:
        status = get_system_status()
        
        print("System Status:")
        print(f"  System Status: {status.get('system_status', 'unknown')}")
        print(f"  Worker Count: {status.get('workers', {}).get('worker_count', 0)}")
        print(f"  Total Active Tasks: {status.get('workers', {}).get('total_active_tasks', 0)}")
        
        health = status.get('health', {})
        print(f"  Responsive Workers: {health.get('responsive_workers', 0)}")
        print(f"  Total Workers: {health.get('total_workers', 0)}")
        
        queue_lengths = status.get('queue_lengths', {})
        if queue_lengths:
            print("  Queue Lengths:")
            for queue, length in queue_lengths.items():
                print(f"    {queue}: {length}")
        
        print("✓ System status retrieved successfully")
        
    except Exception as e:
        print(f"✗ Failed to get system status: {e}")


def test_worker_management():
    """Test worker management functions."""
    print("\nTesting Worker Management")
    print("=" * 40)
    
    try:
        # Test getting worker stats
        worker_stats = worker_manager.monitor.get_worker_stats()
        print(f"  Worker Count: {worker_stats.get('worker_count', 0)}")
        print(f"  Total Active Tasks: {worker_stats.get('total_active_tasks', 0)}")
        
        # Test getting worker health
        health_status = worker_manager.monitor.get_worker_health()
        print(f"  Responsive Workers: {health_status.get('responsive_workers', 0)}")
        print(f"  Total Workers: {health_status.get('total_workers', 0)}")
        
        # Test getting queue lengths
        queue_lengths = worker_manager.get_queue_lengths()
        if queue_lengths:
            print("  Queue Lengths:")
            for queue, length in queue_lengths.items():
                print(f"    {queue}: {length}")
        
        print("✓ Worker management functions working")
        
    except Exception as e:
        print(f"✗ Worker management test failed: {e}")


def test_rate_limiting():
    """Test rate limiting functionality."""
    print("\nTesting Rate Limiting")
    print("=" * 40)
    
    try:
        from harvest.tasks import rate_limiter
        
        # Test rate limiting for a domain
        test_domain = "test.example.com"
        
        # Check if first request is allowed
        allowed1 = rate_limiter.is_allowed(test_domain)
        print(f"  First request to {test_domain}: {'✓' if allowed1 else '✗'}")
        
        # Check if second request is blocked
        allowed2 = rate_limiter.is_allowed(test_domain)
        print(f"  Second request to {test_domain}: {'✓' if allowed2 else '✗'}")
        
        # Get wait time
        wait_time = rate_limiter.get_wait_time(test_domain)
        print(f"  Wait time: {wait_time} seconds")
        
        print("✓ Rate limiting test completed")
        
    except Exception as e:
        print(f"✗ Rate limiting test failed: {e}")


def test_task_monitoring():
    """Test task monitoring."""
    print("\nTesting Task Monitoring")
    print("=" * 40)
    
    try:
        # Start monitoring
        from harvest.worker_manager import start_monitoring
        start_monitoring()
        print("✓ Monitoring started")
        
        # Wait a bit
        time.sleep(2)
        
        # Get task history
        task_history = worker_manager.monitor.get_task_history(limit=10)
        print(f"  Task history entries: {len(task_history)}")
        
        # Stop monitoring
        from harvest.worker_manager import stop_monitoring
        stop_monitoring()
        print("✓ Monitoring stopped")
        
    except Exception as e:
        print(f"✗ Task monitoring test failed: {e}")


def test_integration():
    """Test integration with existing system."""
    print("\nTesting Integration")
    print("=" * 40)
    
    try:
        # Test that modules can be imported
        from harvest.tasks import download_image_task, browser_search_task
        from harvest.worker_manager import WorkerManager, WorkerMonitor
        
        print("✓ All modules imported successfully")
        
        # Test configuration loading
        config = load_config()
        print(f"✓ Configuration loaded: {len(config)} sections")
        
        # Test database connection
        from harvest.db import init_db
        init_db("test_task_queue.db")
        print("✓ Database initialized")
        
        print("✓ Integration test completed")
        
    except Exception as e:
        print(f"✗ Integration test failed: {e}")


def cleanup_test_files():
    """Clean up test files."""
    print("\nCleaning up test files...")
    
    test_files = [
        "test_task_queue.db",
        "storage/test_*"
    ]
    
    for file_pattern in test_files:
        if "*" in file_pattern:
            # Handle glob patterns
            from pathlib import Path
            for file_path in Path(".").glob(file_pattern):
                if file_path.is_file():
                    file_path.unlink()
                    print(f"  Removed: {file_path}")
        else:
            if os.path.exists(file_pattern):
                if os.path.isdir(file_pattern):
                    import shutil
                    shutil.rmtree(file_pattern)
                else:
                    os.remove(file_pattern)
                print(f"  Removed: {file_pattern}")


def main():
    """Main test function."""
    print("PixVault Task Queue System Test")
    print("=" * 60)
    
    # Check dependencies
    try:
        import celery
        import redis
        print("✓ All dependencies available")
    except ImportError as e:
        print(f"✗ Missing dependencies: {e}")
        print("Install with: pip install celery redis kombu")
        return
    
    # Test Redis connection
    if not test_redis_connection():
        print("\n⚠ Redis is not running. Please start Redis server:")
        print("  redis-server")
        return
    
    # Test Celery connection
    if not test_celery_connection():
        print("\n⚠ Celery workers not running. Start workers with:")
        print("  celery -A harvest.tasks worker --loglevel=info")
        print("  (This test will continue but some features may not work)")
    
    # Run tests
    test_integration()
    test_enqueue_download()
    test_enqueue_browser_search()
    test_queue_stats()
    test_system_status()
    test_worker_management()
    test_rate_limiting()
    test_task_monitoring()
    
    # Test task status (if we have task IDs)
    download_task_ids = test_enqueue_download()
    browser_task_ids = test_enqueue_browser_search()
    all_task_ids = download_task_ids + browser_task_ids
    
    if all_task_ids:
        test_task_status(all_task_ids)
    
    # Cleanup
    cleanup_test_files()
    
    print("\n" + "=" * 60)
    print("All tests completed!")
    print("\nTo use the task queue system:")
    print("  1. Start Redis: redis-server")
    print("  2. Start Celery workers: celery -A harvest.tasks worker --loglevel=info")
    print("  3. Use CLI: python -m harvest.task_cli --help")


if __name__ == "__main__":
    main()
