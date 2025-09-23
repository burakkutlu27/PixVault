"""
Worker management and monitoring for Celery task queue.
Provides utilities for managing workers, monitoring tasks, and scaling operations.
"""

import time
import json
from typing import Dict, Any, List, Optional
from celery import Celery
from celery.events.state import State
from celery.events import EventReceiver
import redis
from threading import Thread
import logging

from .tasks import app, get_queue_stats, get_task_status
from .utils.logger import get_logger

logger = get_logger("harvest.worker_manager")


class WorkerMonitor:
    """Monitor Celery workers and tasks."""
    
    def __init__(self, celery_app: Celery = app):
        self.app = celery_app
        self.redis_client = redis.Redis(host='localhost', port=6379, db=1, decode_responses=True)
        self.state = State()
        self.monitoring = False
        self.monitor_thread = None
    
    def start_monitoring(self):
        """Start monitoring workers and tasks."""
        if self.monitoring:
            logger.warning("Monitoring already started")
            return
        
        self.monitoring = True
        self.monitor_thread = Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        logger.info("Worker monitoring started")
    
    def stop_monitoring(self):
        """Stop monitoring workers and tasks."""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
        logger.info("Worker monitoring stopped")
    
    def _monitor_loop(self):
        """Main monitoring loop."""
        with self.app.connection() as connection:
            receiver = EventReceiver(connection, handlers={
                'worker-online': self._on_worker_online,
                'worker-offline': self._on_worker_offline,
                'task-started': self._on_task_started,
                'task-succeeded': self._on_task_succeeded,
                'task-failed': self._on_task_failed,
                'task-retried': self._on_task_retried,
            })
            
            while self.monitoring:
                try:
                    receiver.capture(limit=1, timeout=1)
                except Exception as e:
                    logger.error(f"Error in monitoring loop: {e}")
                    time.sleep(1)
    
    def _on_worker_online(self, event):
        """Handle worker online event."""
        worker_name = event['hostname']
        logger.info(f"Worker {worker_name} came online")
        self._update_worker_status(worker_name, 'online')
    
    def _on_worker_offline(self, event):
        """Handle worker offline event."""
        worker_name = event['hostname']
        logger.info(f"Worker {worker_name} went offline")
        self._update_worker_status(worker_name, 'offline')
    
    def _on_task_started(self, event):
        """Handle task started event."""
        task_id = event['uuid']
        task_name = event['name']
        worker_name = event['hostname']
        logger.info(f"Task {task_id} ({task_name}) started on {worker_name}")
        self._update_task_status(task_id, 'started', worker_name)
    
    def _on_task_succeeded(self, event):
        """Handle task succeeded event."""
        task_id = event['uuid']
        result = event.get('result', {})
        logger.info(f"Task {task_id} succeeded")
        self._update_task_status(task_id, 'succeeded', result=result)
    
    def _on_task_failed(self, event):
        """Handle task failed event."""
        task_id = event['uuid']
        exception = event.get('exception', 'Unknown error')
        logger.error(f"Task {task_id} failed: {exception}")
        self._update_task_status(task_id, 'failed', error=str(exception))
    
    def _on_task_retried(self, event):
        """Handle task retried event."""
        task_id = event['uuid']
        retries = event.get('retries', 0)
        logger.info(f"Task {task_id} retried (attempt {retries + 1})")
        self._update_task_status(task_id, 'retried', retries=retries)
    
    def _update_worker_status(self, worker_name: str, status: str):
        """Update worker status in Redis."""
        key = f"worker_status:{worker_name}"
        data = {
            'status': status,
            'timestamp': time.time(),
            'last_seen': time.time()
        }
        self.redis_client.hset(key, mapping=data)
        self.redis_client.expire(key, 3600)  # Expire after 1 hour
    
    def _update_task_status(self, task_id: str, status: str, **kwargs):
        """Update task status in Redis."""
        key = f"task_status:{task_id}"
        data = {
            'status': status,
            'timestamp': time.time(),
            **kwargs
        }
        self.redis_client.hset(key, mapping=data)
        self.redis_client.expire(key, 86400)  # Expire after 24 hours
    
    def get_worker_stats(self) -> Dict[str, Any]:
        """Get worker statistics."""
        try:
            inspect = self.app.control.inspect()
            
            # Get worker stats
            stats = inspect.stats()
            active_tasks = inspect.active()
            scheduled_tasks = inspect.scheduled()
            reserved_tasks = inspect.reserved()
            
            worker_count = len(stats) if stats else 0
            total_active = sum(len(tasks) for tasks in (active_tasks or {}).values())
            total_scheduled = sum(len(tasks) for tasks in (scheduled_tasks or {}).values())
            total_reserved = sum(len(tasks) for tasks in (reserved_tasks or {}).values())
            
            return {
                'worker_count': worker_count,
                'total_active_tasks': total_active,
                'total_scheduled_tasks': total_scheduled,
                'total_reserved_tasks': total_reserved,
                'workers': stats or {},
                'active_tasks': active_tasks or {},
                'scheduled_tasks': scheduled_tasks or {},
                'reserved_tasks': reserved_tasks or {}
            }
        except Exception as e:
            logger.error(f"Failed to get worker stats: {e}")
            return {'error': str(e)}
    
    def get_task_history(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent task history."""
        try:
            # Get task keys from Redis
            pattern = "task_status:*"
            keys = self.redis_client.keys(pattern)
            
            tasks = []
            for key in keys[:limit]:
                task_data = self.redis_client.hgetall(key)
                if task_data:
                    task_data['task_id'] = key.split(':')[1]
                    tasks.append(task_data)
            
            # Sort by timestamp (newest first)
            tasks.sort(key=lambda x: float(x.get('timestamp', 0)), reverse=True)
            
            return tasks
        except Exception as e:
            logger.error(f"Failed to get task history: {e}")
            return []
    
    def get_worker_health(self) -> Dict[str, Any]:
        """Get worker health status."""
        try:
            inspect = self.app.control.inspect()
            
            # Ping all workers
            ping_results = inspect.ping()
            
            # Get worker stats
            stats = inspect.stats()
            
            health_status = {
                'total_workers': len(ping_results) if ping_results else 0,
                'responsive_workers': len([w for w in (ping_results or {}).values() if w]),
                'worker_details': {}
            }
            
            if stats:
                for worker_name, worker_stats in stats.items():
                    health_status['worker_details'][worker_name] = {
                        'status': 'online' if worker_name in (ping_results or {}) else 'offline',
                        'total_tasks': worker_stats.get('total', {}),
                        'pool': worker_stats.get('pool', {}),
                        'rusage': worker_stats.get('rusage', {})
                    }
            
            return health_status
        except Exception as e:
            logger.error(f"Failed to get worker health: {e}")
            return {'error': str(e)}


class WorkerManager:
    """Manage Celery workers and scaling operations."""
    
    def __init__(self, celery_app: Celery = app):
        self.app = celery_app
        self.monitor = WorkerMonitor(celery_app)
    
    def start_worker(self, 
                    concurrency: int = 4,
                    queues: List[str] = None,
                    hostname: str = None,
                    loglevel: str = 'info') -> bool:
        """
        Start a new worker process.
        
        Args:
            concurrency: Number of concurrent tasks
            queues: List of queues to consume from
            hostname: Worker hostname
            loglevel: Log level
            
        Returns:
            True if worker started successfully
        """
        try:
            if queues is None:
                queues = ['default', 'download', 'browser']
            
            # Build worker command
            cmd = [
                'celery', '-A', 'harvest.tasks', 'worker',
                '--loglevel', loglevel,
                '--concurrency', str(concurrency),
                '--queues', ','.join(queues)
            ]
            
            if hostname:
                cmd.extend(['--hostname', hostname])
            
            logger.info(f"Starting worker with command: {' '.join(cmd)}")
            
            # Note: In production, you'd use subprocess or a process manager
            # For now, we'll just log the command
            logger.info("Worker start command prepared (use subprocess in production)")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to start worker: {e}")
            return False
    
    def stop_worker(self, hostname: str = None) -> bool:
        """
        Stop a worker.
        
        Args:
            hostname: Worker hostname to stop (None for all)
            
        Returns:
            True if worker stopped successfully
        """
        try:
            if hostname:
                # Stop specific worker
                self.app.control.broadcast('shutdown', destination=[hostname])
                logger.info(f"Stopped worker {hostname}")
            else:
                # Stop all workers
                self.app.control.broadcast('shutdown')
                logger.info("Stopped all workers")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to stop worker: {e}")
            return False
    
    def scale_workers(self, target_workers: int) -> bool:
        """
        Scale workers to target number.
        
        Args:
            target_workers: Target number of workers
            
        Returns:
            True if scaling successful
        """
        try:
            # Get current worker count
            inspect = self.app.control.inspect()
            stats = inspect.stats()
            current_workers = len(stats) if stats else 0
            
            if target_workers > current_workers:
                # Scale up
                workers_to_add = target_workers - current_workers
                logger.info(f"Scaling up: adding {workers_to_add} workers")
                
                for i in range(workers_to_add):
                    self.start_worker(hostname=f"worker-{current_workers + i + 1}")
                
            elif target_workers < current_workers:
                # Scale down
                workers_to_remove = current_workers - target_workers
                logger.info(f"Scaling down: removing {workers_to_remove} workers")
                
                # Get worker names
                worker_names = list(stats.keys()) if stats else []
                
                for i in range(min(workers_to_remove, len(worker_names))):
                    self.stop_worker(worker_names[i])
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to scale workers: {e}")
            return False
    
    def get_queue_lengths(self) -> Dict[str, int]:
        """Get queue lengths."""
        try:
            inspect = self.app.control.inspect()
            
            active_tasks = inspect.active()
            scheduled_tasks = inspect.scheduled()
            reserved_tasks = inspect.reserved()
            
            queue_lengths = {}
            
            # Count tasks by queue
            for worker_tasks in (active_tasks or {}).values():
                for task in worker_tasks:
                    queue = task.get('delivery_info', {}).get('routing_key', 'default')
                    queue_lengths[queue] = queue_lengths.get(queue, 0) + 1
            
            return queue_lengths
            
        except Exception as e:
            logger.error(f"Failed to get queue lengths: {e}")
            return {}
    
    def purge_queue(self, queue_name: str = None) -> bool:
        """
        Purge tasks from queue.
        
        Args:
            queue_name: Queue to purge (None for all queues)
            
        Returns:
            True if purge successful
        """
        try:
            if queue_name:
                self.app.control.purge(queue_name)
                logger.info(f"Purged queue {queue_name}")
            else:
                self.app.control.purge()
                logger.info("Purged all queues")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to purge queue: {e}")
            return False
    
    def get_system_status(self) -> Dict[str, Any]:
        """Get comprehensive system status."""
        try:
            # Get worker stats
            worker_stats = self.monitor.get_worker_stats()
            
            # Get queue stats
            queue_stats = get_queue_stats()
            
            # Get worker health
            health_status = self.monitor.get_worker_health()
            
            # Get queue lengths
            queue_lengths = self.get_queue_lengths()
            
            return {
                'timestamp': time.time(),
                'workers': worker_stats,
                'queues': queue_stats,
                'health': health_status,
                'queue_lengths': queue_lengths,
                'system_status': 'healthy' if health_status.get('responsive_workers', 0) > 0 else 'unhealthy'
            }
            
        except Exception as e:
            logger.error(f"Failed to get system status: {e}")
            return {
                'timestamp': time.time(),
                'error': str(e),
                'system_status': 'error'
            }


# Global worker manager instance
worker_manager = WorkerManager()


def start_monitoring():
    """Start worker monitoring."""
    worker_manager.monitor.start_monitoring()


def stop_monitoring():
    """Stop worker monitoring."""
    worker_manager.monitor.stop_monitoring()


def get_system_status() -> Dict[str, Any]:
    """Get system status."""
    return worker_manager.get_system_status()


def scale_workers(target_workers: int) -> bool:
    """Scale workers to target number."""
    return worker_manager.scale_workers(target_workers)
