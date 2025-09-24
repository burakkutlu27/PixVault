"""
Monitoring and metrics module for PixVault.
Tracks daily downloads, disk usage, errors, and provides metrics output.
"""

import os
import json
import time
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional, Union
from dataclasses import dataclass, asdict
import logging
from collections import defaultdict

from .utils.logger import get_logger

logger = get_logger("harvest.monitoring")


@dataclass
class MetricPoint:
    """A single metric data point."""
    timestamp: float
    metric_name: str
    value: Union[int, float]
    labels: Dict[str, str] = None
    
    def __post_init__(self):
        if self.labels is None:
            self.labels = {}


@dataclass
class DailyStats:
    """Daily statistics for monitoring."""
    date: str
    downloads: int = 0
    duplicates: int = 0
    failures: int = 0
    rate_limit_errors: int = 0
    disk_usage_bytes: int = 0
    storage_files: int = 0
    processing_time_seconds: float = 0.0
    unique_domains: int = 0
    proxy_rotations: int = 0


class MetricsCollector:
    """
    Collects and stores metrics for PixVault operations.
    Provides both logging and structured output for monitoring.
    """
    
    def __init__(self, db_path: str = "db/metrics.db", storage_path: str = "storage"):
        """
        Initialize metrics collector.
        
        Args:
            db_path: Path to metrics database
            storage_path: Path to storage directory for disk usage calculation
        """
        self.db_path = db_path
        self.storage_path = Path(storage_path)
        self.metrics_db_path = Path(db_path)
        self.metrics_db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Initialize metrics database
        self._init_metrics_db()
        
        # In-memory metrics for current session
        self.session_metrics: Dict[str, List[MetricPoint]] = defaultdict(list)
        self.daily_stats: Dict[str, DailyStats] = {}
        
        logger.info(f"Metrics collector initialized: {db_path}")
    
    def _init_metrics_db(self):
        """Initialize metrics database schema."""
        with sqlite3.connect(self.metrics_db_path) as conn:
            cursor = conn.cursor()
            
            # Create metrics table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp REAL NOT NULL,
                    metric_name TEXT NOT NULL,
                    value REAL NOT NULL,
                    labels TEXT,
                    date TEXT NOT NULL
                )
            """)
            
            # Create daily_stats table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS daily_stats (
                    date TEXT PRIMARY KEY,
                    downloads INTEGER DEFAULT 0,
                    duplicates INTEGER DEFAULT 0,
                    failures INTEGER DEFAULT 0,
                    rate_limit_errors INTEGER DEFAULT 0,
                    disk_usage_bytes INTEGER DEFAULT 0,
                    storage_files INTEGER DEFAULT 0,
                    processing_time_seconds REAL DEFAULT 0.0,
                    unique_domains INTEGER DEFAULT 0,
                    proxy_rotations INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create indexes
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_metrics_timestamp ON metrics(timestamp)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_metrics_name ON metrics(metric_name)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_metrics_date ON metrics(date)")
            
            conn.commit()
    
    def record_metric(self, metric_name: str, value: Union[int, float], labels: Dict[str, str] = None) -> None:
        """
        Record a metric value.
        
        Args:
            metric_name: Name of the metric
            value: Metric value
            labels: Optional labels for the metric
        """
        timestamp = time.time()
        date = datetime.now().strftime('%Y-%m-%d')
        
        # Create metric point
        metric_point = MetricPoint(
            timestamp=timestamp,
            metric_name=metric_name,
            value=value,
            labels=labels or {}
        )
        
        # Store in session
        self.session_metrics[metric_name].append(metric_point)
        
        # Store in database
        with sqlite3.connect(self.metrics_db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO metrics (timestamp, metric_name, value, labels, date)
                VALUES (?, ?, ?, ?, ?)
            """, (
                timestamp,
                metric_name,
                value,
                json.dumps(labels or {}),
                date
            ))
            conn.commit()
        
        logger.debug(f"Recorded metric: {metric_name}={value} (labels: {labels})")
    
    def get_metrics(self, metric_name: str = None, start_date: str = None, end_date: str = None) -> Dict[str, Any]:
        """
        Get metrics data.
        
        Args:
            metric_name: Specific metric name to retrieve
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            
        Returns:
            Dictionary containing metrics data
        """
        try:
            with sqlite3.connect(self.metrics_db_path) as conn:
                cursor = conn.cursor()
                
                # Build query
                query = "SELECT timestamp, metric_name, value, labels FROM metrics WHERE 1=1"
                params = []
                
                if metric_name:
                    query += " AND metric_name = ?"
                    params.append(metric_name)
                
                if start_date:
                    query += " AND date >= ?"
                    params.append(start_date)
                
                if end_date:
                    query += " AND date <= ?"
                    params.append(end_date)
                
                query += " ORDER BY timestamp DESC"
                
                cursor.execute(query, params)
                rows = cursor.fetchall()
                
                # Process results
                metrics = {}
                for timestamp, name, value, labels_json in rows:
                    if name not in metrics:
                        metrics[name] = []
                    
                    labels = json.loads(labels_json) if labels_json else {}
                    metrics[name].append({
                        'timestamp': timestamp,
                        'value': value,
                        'labels': labels,
                        'date': datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
                    })
                
                return metrics
                
        except Exception as e:
            logger.error(f"Failed to get metrics: {e}")
            return {}
    
    def get_daily_stats(self, date: str = None) -> DailyStats:
        """
        Get daily statistics for a specific date.
        
        Args:
            date: Date in YYYY-MM-DD format (default: today)
            
        Returns:
            DailyStats object
        """
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')
        
        try:
            with sqlite3.connect(self.metrics_db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM daily_stats WHERE date = ?", (date,))
                row = cursor.fetchone()
                
                if row:
                    return DailyStats(
                        date=row[0],
                        downloads=row[1],
                        duplicates=row[2],
                        failures=row[3],
                        rate_limit_errors=row[4],
                        disk_usage_bytes=row[5],
                        storage_files=row[6],
                        processing_time_seconds=row[7],
                        unique_domains=row[8],
                        proxy_rotations=row[9]
                    )
                else:
                    # Return empty stats for new date
                    return DailyStats(date=date)
                    
        except Exception as e:
            logger.error(f"Failed to get daily stats: {e}")
            return DailyStats(date=date)
    
    def update_daily_stats(self, date: str = None) -> None:
        """Update daily statistics from metrics data."""
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')
        
        try:
            # Get metrics for the date
            metrics = self.get_metrics(start_date=date, end_date=date)
            
            # Calculate daily stats
            stats = DailyStats(date=date)
            
            # Process metrics
            for metric_name, points in metrics.items():
                if metric_name == 'downloads':
                    stats.downloads = sum(point['value'] for point in points)
                elif metric_name == 'duplicates':
                    stats.duplicates = sum(point['value'] for point in points)
                elif metric_name == 'failures':
                    stats.failures = sum(point['value'] for point in points)
                elif metric_name == 'rate_limit_errors':
                    stats.rate_limit_errors = sum(point['value'] for point in points)
                elif metric_name == 'processing_time':
                    stats.processing_time_seconds = sum(point['value'] for point in points)
                elif metric_name == 'proxy_rotations':
                    stats.proxy_rotations = sum(point['value'] for point in points)
            
            # Calculate disk usage
            stats.disk_usage_bytes = self._calculate_disk_usage()
            stats.storage_files = self._count_storage_files()
            
            # Calculate unique domains
            stats.unique_domains = self._count_unique_domains(date)
            
            # Store daily stats
            with sqlite3.connect(self.metrics_db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT OR REPLACE INTO daily_stats 
                    (date, downloads, duplicates, failures, rate_limit_errors, 
                     disk_usage_bytes, storage_files, processing_time_seconds, 
                     unique_domains, proxy_rotations, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """, (
                    stats.date, stats.downloads, stats.duplicates, stats.failures,
                    stats.rate_limit_errors, stats.disk_usage_bytes, stats.storage_files,
                    stats.processing_time_seconds, stats.unique_domains, stats.proxy_rotations
                ))
                conn.commit()
            
            logger.info(f"Updated daily stats for {date}: {stats.downloads} downloads, {stats.failures} failures")
            
        except Exception as e:
            logger.error(f"Failed to update daily stats: {e}")
    
    def _calculate_disk_usage(self) -> int:
        """Calculate total disk usage of storage directory."""
        try:
            total_size = 0
            if self.storage_path.exists():
                for file_path in self.storage_path.rglob('*'):
                    if file_path.is_file():
                        total_size += file_path.stat().st_size
            return total_size
        except Exception as e:
            logger.error(f"Failed to calculate disk usage: {e}")
            return 0
    
    def _count_storage_files(self) -> int:
        """Count files in storage directory."""
        try:
            if self.storage_path.exists():
                return len([f for f in self.storage_path.iterdir() if f.is_file()])
            return 0
        except Exception as e:
            logger.error(f"Failed to count storage files: {e}")
            return 0
    
    def _count_unique_domains(self, date: str) -> int:
        """Count unique domains for a specific date."""
        try:
            # This would need integration with the main database
            # For now, return 0
            return 0
        except Exception as e:
            logger.error(f"Failed to count unique domains: {e}")
            return 0
    
    def get_metrics_summary(self, days: int = 7) -> Dict[str, Any]:
        """
        Get metrics summary for the last N days.
        
        Args:
            days: Number of days to include
            
        Returns:
            Summary dictionary
        """
        try:
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
            
            # Get daily stats
            with sqlite3.connect(self.metrics_db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT * FROM daily_stats 
                    WHERE date >= ? AND date <= ?
                    ORDER BY date DESC
                """, (start_date, end_date))
                rows = cursor.fetchall()
            
            # Calculate summary
            summary = {
                'period': f"{start_date} to {end_date}",
                'total_downloads': 0,
                'total_duplicates': 0,
                'total_failures': 0,
                'total_rate_limit_errors': 0,
                'total_disk_usage_bytes': 0,
                'total_storage_files': 0,
                'total_processing_time': 0.0,
                'total_unique_domains': 0,
                'total_proxy_rotations': 0,
                'daily_stats': []
            }
            
            for row in rows:
                daily_stat = {
                    'date': row[0],
                    'downloads': row[1],
                    'duplicates': row[2],
                    'failures': row[3],
                    'rate_limit_errors': row[4],
                    'disk_usage_bytes': row[5],
                    'storage_files': row[6],
                    'processing_time_seconds': row[7],
                    'unique_domains': row[8],
                    'proxy_rotations': row[9]
                }
                
                summary['daily_stats'].append(daily_stat)
                summary['total_downloads'] += row[1]
                summary['total_duplicates'] += row[2]
                summary['total_failures'] += row[3]
                summary['total_rate_limit_errors'] += row[4]
                summary['total_disk_usage_bytes'] += row[5]
                summary['total_storage_files'] += row[6]
                summary['total_processing_time'] += row[7]
                summary['total_unique_domains'] += row[8]
                summary['total_proxy_rotations'] += row[9]
            
            return summary
            
        except Exception as e:
            logger.error(f"Failed to get metrics summary: {e}")
            return {}
    
    def export_metrics_json(self, output_file: str = None) -> str:
        """
        Export metrics to JSON format.
        
        Args:
            output_file: Output file path (optional)
            
        Returns:
            JSON string or file path
        """
        try:
            # Get metrics summary
            summary = self.get_metrics_summary(days=30)
            
            # Add metadata
            export_data = {
                'export_timestamp': time.time(),
                'export_date': datetime.now().isoformat(),
                'metrics_db_path': str(self.metrics_db_path),
                'storage_path': str(self.storage_path),
                'summary': summary
            }
            
            json_str = json.dumps(export_data, indent=2, default=str)
            
            if output_file:
                with open(output_file, 'w') as f:
                    f.write(json_str)
                logger.info(f"Exported metrics to {output_file}")
                return output_file
            else:
                return json_str
                
        except Exception as e:
            logger.error(f"Failed to export metrics: {e}")
            return "{}"
    
    def cleanup_old_metrics(self, days_to_keep: int = 90) -> None:
        """Clean up old metrics data."""
        try:
            cutoff_date = (datetime.now() - timedelta(days=days_to_keep)).strftime('%Y-%m-%d')
            
            with sqlite3.connect(self.metrics_db_path) as conn:
                cursor = conn.cursor()
                
                # Delete old metrics
                cursor.execute("DELETE FROM metrics WHERE date < ?", (cutoff_date,))
                deleted_metrics = cursor.rowcount
                
                # Delete old daily stats
                cursor.execute("DELETE FROM daily_stats WHERE date < ?", (cutoff_date,))
                deleted_stats = cursor.rowcount
                
                conn.commit()
            
            logger.info(f"Cleaned up old metrics: {deleted_metrics} metrics, {deleted_stats} daily stats")
            
        except Exception as e:
            logger.error(f"Failed to cleanup old metrics: {e}")


# Global metrics collector instance
_metrics_collector: Optional[MetricsCollector] = None


def get_metrics_collector() -> MetricsCollector:
    """Get global metrics collector instance."""
    global _metrics_collector
    if _metrics_collector is None:
        _metrics_collector = MetricsCollector()
    return _metrics_collector


def record_metric(metric_name: str, value: Union[int, float], labels: Dict[str, str] = None) -> None:
    """Record a metric value."""
    get_metrics_collector().record_metric(metric_name, value, labels)


def get_metrics(metric_name: str = None, start_date: str = None, end_date: str = None) -> Dict[str, Any]:
    """Get metrics data."""
    return get_metrics_collector().get_metrics(metric_name, start_date, end_date)


def get_daily_stats(date: str = None) -> DailyStats:
    """Get daily statistics."""
    return get_metrics_collector().get_daily_stats(date)


def update_daily_stats(date: str = None) -> None:
    """Update daily statistics."""
    get_metrics_collector().update_daily_stats(date)


def get_metrics_summary(days: int = 7) -> Dict[str, Any]:
    """Get metrics summary."""
    return get_metrics_collector().get_metrics_summary(days)


def export_metrics_json(output_file: str = None) -> str:
    """Export metrics to JSON."""
    return get_metrics_collector().export_metrics_json(output_file)
