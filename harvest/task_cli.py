#!/usr/bin/env python3
"""
CLI for task queue management and monitoring.
Provides commands for managing Celery tasks, workers, and monitoring.
"""

import argparse
import sys
import json
import time
from typing import Dict, Any, List

from .tasks import (
    enqueue_download, enqueue_browser_search, get_task_status, 
    cancel_task, get_queue_stats, app
)
from .worker_manager import worker_manager, get_system_status, scale_workers
from .config import load_config
from .utils.logger import get_logger


def cmd_enqueue_download(args):
    """Enqueue a download task."""
    logger = get_logger("task_cli.enqueue_download")
    
    try:
        # Load configuration
        config = load_config(args.config)
        
        # Prepare task data
        task_data = {
            'url': args.url,
            'label': args.label,
            'config': config,
            'domain': args.domain,
            'priority': args.priority
        }
        
        # Enqueue task
        task_id = enqueue_download(task_data, priority=args.priority)
        
        print(f"✓ Download task enqueued")
        print(f"  Task ID: {task_id}")
        print(f"  URL: {args.url}")
        print(f"  Label: {args.label}")
        print(f"  Priority: {args.priority}")
        
        if args.wait:
            print(f"Waiting for task completion...")
            while True:
                status = get_task_status(task_id)
                if status['status'] in ['SUCCESS', 'FAILURE']:
                    break
                time.sleep(1)
            
            print(f"Task completed with status: {status['status']}")
            if status['result']:
                result = status['result']
                print(f"  Status: {result.get('status', 'unknown')}")
                print(f"  Message: {result.get('message', 'no message')}")
        
    except Exception as e:
        logger.error(f"Failed to enqueue download task: {e}")
        print(f"✗ Failed to enqueue download task: {e}")
        sys.exit(1)


def cmd_enqueue_browser_search(args):
    """Enqueue a browser search task."""
    logger = get_logger("task_cli.enqueue_browser_search")
    
    try:
        # Load configuration
        config = load_config(args.config)
        
        # Prepare task data
        task_data = {
            'query': args.query,
            'max_results': args.max_results,
            'label': args.label,
            'config': config
        }
        
        # Enqueue task
        task_id = enqueue_browser_search(task_data)
        
        print(f"✓ Browser search task enqueued")
        print(f"  Task ID: {task_id}")
        print(f"  Query: {args.query}")
        print(f"  Max Results: {args.max_results}")
        print(f"  Label: {args.label}")
        
        if args.wait:
            print(f"Waiting for task completion...")
            while True:
                status = get_task_status(task_id)
                if status['status'] in ['SUCCESS', 'FAILURE']:
                    break
                time.sleep(1)
            
            print(f"Task completed with status: {status['status']}")
            if status['result']:
                result = status['result']
                print(f"  Status: {result.get('status', 'unknown')}")
                print(f"  Results: {len(result.get('results', []))}")
                print(f"  Download Tasks: {len(result.get('download_tasks', []))}")
        
    except Exception as e:
        logger.error(f"Failed to enqueue browser search task: {e}")
        print(f"✗ Failed to enqueue browser search task: {e}")
        sys.exit(1)


def cmd_task_status(args):
    """Get task status."""
    try:
        status = get_task_status(args.task_id)
        
        print(f"Task Status: {args.task_id}")
        print(f"  Status: {status['status']}")
        
        if status.get('result'):
            result = status['result']
            print(f"  Result: {json.dumps(result, indent=2)}")
        
        if status.get('error'):
            print(f"  Error: {status['error']}")
        
    except Exception as e:
        print(f"✗ Failed to get task status: {e}")
        sys.exit(1)


def cmd_cancel_task(args):
    """Cancel a task."""
    try:
        success = cancel_task(args.task_id)
        
        if success:
            print(f"✓ Task {args.task_id} cancelled")
        else:
            print(f"✗ Failed to cancel task {args.task_id}")
            sys.exit(1)
        
    except Exception as e:
        print(f"✗ Failed to cancel task: {e}")
        sys.exit(1)


def cmd_queue_stats(args):
    """Get queue statistics."""
    try:
        stats = get_queue_stats()
        
        if args.json:
            print(json.dumps(stats, indent=2))
        else:
            print("Queue Statistics:")
            print(f"  Active Tasks: {stats.get('active_tasks', {})}")
            print(f"  Scheduled Tasks: {stats.get('scheduled_tasks', {})}")
            print(f"  Reserved Tasks: {stats.get('reserved_tasks', {})}")
        
    except Exception as e:
        print(f"✗ Failed to get queue stats: {e}")
        sys.exit(1)


def cmd_system_status(args):
    """Get system status."""
    try:
        status = get_system_status()
        
        if args.json:
            print(json.dumps(status, indent=2))
        else:
            print("System Status:")
            print(f"  System Status: {status.get('system_status', 'unknown')}")
            print(f"  Worker Count: {status.get('workers', {}).get('worker_count', 0)}")
            print(f"  Total Active Tasks: {status.get('workers', {}).get('total_active_tasks', 0)}")
            print(f"  Queue Lengths: {status.get('queue_lengths', {})}")
            
            health = status.get('health', {})
            print(f"  Responsive Workers: {health.get('responsive_workers', 0)}")
            print(f"  Total Workers: {health.get('total_workers', 0)}")
        
    except Exception as e:
        print(f"✗ Failed to get system status: {e}")
        sys.exit(1)


def cmd_scale_workers(args):
    """Scale workers."""
    try:
        success = scale_workers(args.target_workers)
        
        if success:
            print(f"✓ Scaled workers to {args.target_workers}")
        else:
            print(f"✗ Failed to scale workers to {args.target_workers}")
            sys.exit(1)
        
    except Exception as e:
        print(f"✗ Failed to scale workers: {e}")
        sys.exit(1)


def cmd_purge_queue(args):
    """Purge queue."""
    try:
        success = worker_manager.purge_queue(args.queue_name)
        
        if success:
            queue_name = args.queue_name or "all queues"
            print(f"✓ Purged {queue_name}")
        else:
            print(f"✗ Failed to purge {args.queue_name or 'all queues'}")
            sys.exit(1)
        
    except Exception as e:
        print(f"✗ Failed to purge queue: {e}")
        sys.exit(1)


def cmd_start_worker(args):
    """Start a worker."""
    try:
        success = worker_manager.start_worker(
            concurrency=args.concurrency,
            queues=args.queues.split(',') if args.queues else None,
            hostname=args.hostname,
            loglevel=args.loglevel
        )
        
        if success:
            print(f"✓ Worker start command prepared")
            print(f"  Concurrency: {args.concurrency}")
            print(f"  Queues: {args.queues or 'default,download,browser'}")
            print(f"  Hostname: {args.hostname or 'auto'}")
            print(f"  Log Level: {args.loglevel}")
        else:
            print(f"✗ Failed to start worker")
            sys.exit(1)
        
    except Exception as e:
        print(f"✗ Failed to start worker: {e}")
        sys.exit(1)


def cmd_stop_worker(args):
    """Stop workers."""
    try:
        success = worker_manager.stop_worker(args.hostname)
        
        if success:
            target = args.hostname or "all workers"
            print(f"✓ Stopped {target}")
        else:
            print(f"✗ Failed to stop {args.hostname or 'all workers'}")
            sys.exit(1)
        
    except Exception as e:
        print(f"✗ Failed to stop worker: {e}")
        sys.exit(1)


def cmd_monitor(args):
    """Start monitoring."""
    try:
        from .worker_manager import start_monitoring, stop_monitoring
        
        if args.action == 'start':
            start_monitoring()
            print("✓ Monitoring started")
        elif args.action == 'stop':
            stop_monitoring()
            print("✓ Monitoring stopped")
        else:
            print("✗ Invalid action. Use 'start' or 'stop'")
            sys.exit(1)
        
    except Exception as e:
        print(f"✗ Failed to {args.action} monitoring: {e}")
        sys.exit(1)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Task queue management CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Enqueue download task
  python -m harvest.task_cli enqueue-download --url "https://example.com/image.jpg" --label "test"
  
  # Enqueue browser search
  python -m harvest.task_cli enqueue-browser-search --query "nature" --label "nature" --max-results 10
  
  # Get task status
  python -m harvest.task_cli task-status --task-id "abc123"
  
  # Get system status
  python -m harvest.task_cli system-status
  
  # Scale workers
  python -m harvest.task_cli scale-workers --target-workers 4
  
  # Start monitoring
  python -m harvest.task_cli monitor --action start
        """
    )
    
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to configuration file (default: config.yaml)"
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Enqueue download command
    download_parser = subparsers.add_parser('enqueue-download', help='Enqueue download task')
    download_parser.add_argument('--url', required=True, help='Image URL to download')
    download_parser.add_argument('--label', required=True, help='Label for the image')
    download_parser.add_argument('--domain', help='Domain for rate limiting')
    download_parser.add_argument('--priority', type=int, default=0, help='Task priority (0=normal, 1=high)')
    download_parser.add_argument('--wait', action='store_true', help='Wait for task completion')
    download_parser.set_defaults(func=cmd_enqueue_download)
    
    # Enqueue browser search command
    browser_parser = subparsers.add_parser('enqueue-browser-search', help='Enqueue browser search task')
    browser_parser.add_argument('--query', required=True, help='Search query')
    browser_parser.add_argument('--label', required=True, help='Label for found images')
    browser_parser.add_argument('--max-results', type=int, default=50, help='Maximum number of results')
    browser_parser.add_argument('--wait', action='store_true', help='Wait for task completion')
    browser_parser.set_defaults(func=cmd_enqueue_browser_search)
    
    # Task status command
    status_parser = subparsers.add_parser('task-status', help='Get task status')
    status_parser.add_argument('--task-id', required=True, help='Task ID')
    status_parser.set_defaults(func=cmd_task_status)
    
    # Cancel task command
    cancel_parser = subparsers.add_parser('cancel-task', help='Cancel task')
    cancel_parser.add_argument('--task-id', required=True, help='Task ID to cancel')
    cancel_parser.set_defaults(func=cmd_cancel_task)
    
    # Queue stats command
    stats_parser = subparsers.add_parser('queue-stats', help='Get queue statistics')
    stats_parser.add_argument('--json', action='store_true', help='Output as JSON')
    stats_parser.set_defaults(func=cmd_queue_stats)
    
    # System status command
    system_parser = subparsers.add_parser('system-status', help='Get system status')
    system_parser.add_argument('--json', action='store_true', help='Output as JSON')
    system_parser.set_defaults(func=cmd_system_status)
    
    # Scale workers command
    scale_parser = subparsers.add_parser('scale-workers', help='Scale workers')
    scale_parser.add_argument('--target-workers', type=int, required=True, help='Target number of workers')
    scale_parser.set_defaults(func=cmd_scale_workers)
    
    # Purge queue command
    purge_parser = subparsers.add_parser('purge-queue', help='Purge queue')
    purge_parser.add_argument('--queue-name', help='Queue name to purge (default: all queues)')
    purge_parser.set_defaults(func=cmd_purge_queue)
    
    # Start worker command
    start_parser = subparsers.add_parser('start-worker', help='Start worker')
    start_parser.add_argument('--concurrency', type=int, default=4, help='Number of concurrent tasks')
    start_parser.add_argument('--queues', help='Comma-separated list of queues')
    start_parser.add_argument('--hostname', help='Worker hostname')
    start_parser.add_argument('--loglevel', default='info', help='Log level')
    start_parser.set_defaults(func=cmd_start_worker)
    
    # Stop worker command
    stop_parser = subparsers.add_parser('stop-worker', help='Stop workers')
    stop_parser.add_argument('--hostname', help='Worker hostname to stop (default: all workers)')
    stop_parser.set_defaults(func=cmd_stop_worker)
    
    # Monitor command
    monitor_parser = subparsers.add_parser('monitor', help='Start/stop monitoring')
    monitor_parser.add_argument('--action', choices=['start', 'stop'], required=True, help='Action to perform')
    monitor_parser.set_defaults(func=cmd_monitor)
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Execute the command
    args.func(args)


if __name__ == "__main__":
    main()
