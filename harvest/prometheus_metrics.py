"""
Prometheus metrics endpoint for PixVault.
Provides Prometheus-compatible metrics output for monitoring systems.
"""

import time
from typing import Dict, Any, List
from fastapi import FastAPI, Response
from fastapi.responses import PlainTextResponse

from .monitoring import get_metrics_collector, get_metrics_summary, get_daily_stats
from .utils.logger import get_logger

logger = get_logger("harvest.prometheus_metrics")


class PrometheusExporter:
    """
    Exports PixVault metrics in Prometheus format.
    """
    
    def __init__(self):
        self.metrics_collector = get_metrics_collector()
    
    def format_metric(self, name: str, value: float, labels: Dict[str, str] = None, 
                     metric_type: str = "gauge", help_text: str = "") -> str:
        """
        Format a single metric in Prometheus format.
        
        Args:
            name: Metric name
            value: Metric value
            labels: Optional labels
            metric_type: Metric type (gauge, counter, histogram)
            help_text: Help text for the metric
            
        Returns:
            Formatted metric string
        """
        lines = []
        
        # Add help text
        if help_text:
            lines.append(f"# HELP {name} {help_text}")
        
        # Add type
        lines.append(f"# TYPE {name} {metric_type}")
        
        # Format labels
        label_str = ""
        if labels:
            label_pairs = [f'{k}="{v}"' for k, v in labels.items()]
            label_str = "{" + ",".join(label_pairs) + "}"
        
        # Add metric value
        lines.append(f"{name}{label_str} {value}")
        
        return "\n".join(lines)
    
    def export_metrics(self) -> str:
        """
        Export all metrics in Prometheus format.
        
        Returns:
            Prometheus-formatted metrics string
        """
        try:
            # Get current daily stats
            today = time.strftime('%Y-%m-%d')
            daily_stats = get_daily_stats(today)
            
            # Get metrics summary
            summary = get_metrics_summary(days=1)
            
            metrics_lines = []
            
            # Add header
            metrics_lines.append("# PixVault Metrics")
            metrics_lines.append("# Generated at: " + time.strftime('%Y-%m-%d %H:%M:%S'))
            metrics_lines.append("")
            
            # Daily downloads
            metrics_lines.append(self.format_metric(
                "pixvault_downloads_total",
                daily_stats.downloads,
                {"date": today},
                "counter",
                "Total number of images downloaded"
            ))
            
            # Daily duplicates
            metrics_lines.append(self.format_metric(
                "pixvault_duplicates_total",
                daily_stats.duplicates,
                {"date": today},
                "counter",
                "Total number of duplicate images detected"
            ))
            
            # Daily failures
            metrics_lines.append(self.format_metric(
                "pixvault_failures_total",
                daily_stats.failures,
                {"date": today},
                "counter",
                "Total number of download failures"
            ))
            
            # Rate limit errors
            metrics_lines.append(self.format_metric(
                "pixvault_rate_limit_errors_total",
                daily_stats.rate_limit_errors,
                {"date": today},
                "counter",
                "Total number of rate limit errors (429)"
            ))
            
            # Disk usage
            metrics_lines.append(self.format_metric(
                "pixvault_disk_usage_bytes",
                daily_stats.disk_usage_bytes,
                {"date": today},
                "gauge",
                "Disk usage in bytes"
            ))
            
            # Storage files count
            metrics_lines.append(self.format_metric(
                "pixvault_storage_files_total",
                daily_stats.storage_files,
                {"date": today},
                "gauge",
                "Total number of files in storage"
            ))
            
            # Processing time
            metrics_lines.append(self.format_metric(
                "pixvault_processing_time_seconds",
                daily_stats.processing_time_seconds,
                {"date": today},
                "counter",
                "Total processing time in seconds"
            ))
            
            # Unique domains
            metrics_lines.append(self.format_metric(
                "pixvault_unique_domains_total",
                daily_stats.unique_domains,
                {"date": today},
                "gauge",
                "Number of unique domains accessed"
            ))
            
            # Proxy rotations
            metrics_lines.append(self.format_metric(
                "pixvault_proxy_rotations_total",
                daily_stats.proxy_rotations,
                {"date": today},
                "counter",
                "Total number of proxy rotations"
            ))
            
            # System metrics
            metrics_lines.append(self.format_metric(
                "pixvault_system_uptime_seconds",
                time.time(),
                {},
                "gauge",
                "System uptime in seconds"
            ))
            
            # Add summary metrics
            if summary and 'summary' in summary:
                summary_data = summary['summary']
                
                # Total downloads over period
                metrics_lines.append(self.format_metric(
                    "pixvault_downloads_period_total",
                    summary_data.get('total_downloads', 0),
                    {"period": summary_data.get('period', 'unknown')},
                    "counter",
                    "Total downloads over period"
                ))
                
                # Total failures over period
                metrics_lines.append(self.format_metric(
                    "pixvault_failures_period_total",
                    summary_data.get('total_failures', 0),
                    {"period": summary_data.get('period', 'unknown')},
                    "counter",
                    "Total failures over period"
                ))
                
                # Total disk usage over period
                metrics_lines.append(self.format_metric(
                    "pixvault_disk_usage_period_bytes",
                    summary_data.get('total_disk_usage_bytes', 0),
                    {"period": summary_data.get('period', 'unknown')},
                    "gauge",
                    "Total disk usage over period"
                ))
            
            return "\n".join(metrics_lines)
            
        except Exception as e:
            logger.error(f"Failed to export Prometheus metrics: {e}")
            return f"# Error generating metrics: {e}\n"
    
    def get_metric_help(self) -> str:
        """Get help text for all metrics."""
        help_text = """
# PixVault Prometheus Metrics

## Counter Metrics
- pixvault_downloads_total: Total number of images downloaded
- pixvault_duplicates_total: Total number of duplicate images detected
- pixvault_failures_total: Total number of download failures
- pixvault_rate_limit_errors_total: Total number of rate limit errors (429)
- pixvault_processing_time_seconds: Total processing time in seconds
- pixvault_proxy_rotations_total: Total number of proxy rotations

## Gauge Metrics
- pixvault_disk_usage_bytes: Disk usage in bytes
- pixvault_storage_files_total: Total number of files in storage
- pixvault_unique_domains_total: Number of unique domains accessed
- pixvault_system_uptime_seconds: System uptime in seconds

## Labels
- date: Date in YYYY-MM-DD format
- period: Time period for aggregated metrics

## Usage
Add this endpoint to your Prometheus configuration:
```yaml
scrape_configs:
  - job_name: 'pixvault'
    static_configs:
      - targets: ['localhost:8000']
    metrics_path: '/metrics'
    scrape_interval: 30s
```
"""
        return help_text


# Global exporter instance
_prometheus_exporter: PrometheusExporter = None


def get_prometheus_exporter() -> PrometheusExporter:
    """Get global Prometheus exporter instance."""
    global _prometheus_exporter
    if _prometheus_exporter is None:
        _prometheus_exporter = PrometheusExporter()
    return _prometheus_exporter


def create_prometheus_endpoint(app: FastAPI) -> None:
    """
    Add Prometheus metrics endpoint to FastAPI app.
    
    Args:
        app: FastAPI application instance
    """
    
    @app.get("/metrics", response_class=PlainTextResponse)
    async def prometheus_metrics():
        """Prometheus metrics endpoint."""
        try:
            exporter = get_prometheus_exporter()
            metrics = exporter.export_metrics()
            return Response(content=metrics, media_type="text/plain")
        except Exception as e:
            logger.error(f"Failed to generate Prometheus metrics: {e}")
            return Response(content=f"# Error: {e}\n", media_type="text/plain")
    
    @app.get("/metrics/help", response_class=PlainTextResponse)
    async def prometheus_help():
        """Prometheus metrics help endpoint."""
        try:
            exporter = get_prometheus_exporter()
            help_text = exporter.get_metric_help()
            return Response(content=help_text, media_type="text/plain")
        except Exception as e:
            logger.error(f"Failed to generate Prometheus help: {e}")
            return Response(content=f"# Error: {e}\n", media_type="text/plain")


def export_prometheus_metrics(output_file: str = None) -> str:
    """
    Export metrics in Prometheus format to file or return as string.
    
    Args:
        output_file: Output file path (optional)
        
    Returns:
        Prometheus-formatted metrics string or file path
    """
    try:
        exporter = get_prometheus_exporter()
        metrics = exporter.export_metrics()
        
        if output_file:
            with open(output_file, 'w') as f:
                f.write(metrics)
            logger.info(f"Exported Prometheus metrics to {output_file}")
            return output_file
        else:
            return metrics
            
    except Exception as e:
        logger.error(f"Failed to export Prometheus metrics: {e}")
        return f"# Error: {e}\n"
