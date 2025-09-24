#!/usr/bin/env python3
"""
CLI for monitoring and metrics management.
Provides commands for viewing metrics, generating reports, and managing monitoring data.
"""

import argparse
import sys
import json
from datetime import datetime, timedelta
from typing import Dict, Any

from .monitoring import (
    get_metrics_collector, record_metric, get_metrics, get_daily_stats,
    get_metrics_summary, export_metrics_json, update_daily_stats
)
from .prometheus_metrics import export_prometheus_metrics
from .utils.logger import get_logger


def cmd_record_metric(args):
    """Record a metric value."""
    try:
        labels = {}
        if args.labels:
            for label in args.labels:
                if '=' in label:
                    key, value = label.split('=', 1)
                    labels[key] = value
        
        record_metric(args.metric_name, args.value, labels)
        print(f"✓ Recorded metric: {args.metric_name}={args.value}")
        if labels:
            print(f"  Labels: {labels}")
        
    except Exception as e:
        print(f"✗ Failed to record metric: {e}")
        sys.exit(1)


def cmd_get_metrics(args):
    """Get metrics data."""
    try:
        metrics = get_metrics(
            metric_name=args.metric_name,
            start_date=args.start_date,
            end_date=args.end_date
        )
        
        if args.json:
            print(json.dumps(metrics, indent=2, default=str))
        else:
            if not metrics:
                print("No metrics found")
                return
            
            for metric_name, points in metrics.items():
                print(f"\nMetric: {metric_name}")
                print("-" * 40)
                for point in points[:args.limit]:
                    print(f"  {point['date']} {point['timestamp']}: {point['value']}")
                    if point['labels']:
                        print(f"    Labels: {point['labels']}")
        
    except Exception as e:
        print(f"✗ Failed to get metrics: {e}")
        sys.exit(1)


def cmd_daily_stats(args):
    """Get daily statistics."""
    try:
        stats = get_daily_stats(args.date)
        
        if args.json:
            print(json.dumps(stats.__dict__, indent=2, default=str))
        else:
            print(f"Daily Statistics for {stats.date}")
            print("=" * 40)
            print(f"Downloads: {stats.downloads}")
            print(f"Duplicates: {stats.duplicates}")
            print(f"Failures: {stats.failures}")
            print(f"Rate Limit Errors: {stats.rate_limit_errors}")
            print(f"Disk Usage: {stats.disk_usage_bytes:,} bytes")
            print(f"Storage Files: {stats.storage_files}")
            print(f"Processing Time: {stats.processing_time_seconds:.2f} seconds")
            print(f"Unique Domains: {stats.unique_domains}")
            print(f"Proxy Rotations: {stats.proxy_rotations}")
        
    except Exception as e:
        print(f"✗ Failed to get daily stats: {e}")
        sys.exit(1)


def cmd_summary(args):
    """Get metrics summary."""
    try:
        summary = get_metrics_summary(days=args.days)
        
        if args.json:
            print(json.dumps(summary, indent=2, default=str))
        else:
            print(f"Metrics Summary ({summary.get('period', 'Unknown period')})")
            print("=" * 50)
            
            summary_data = summary.get('summary', {})
            print(f"Total Downloads: {summary_data.get('total_downloads', 0)}")
            print(f"Total Duplicates: {summary_data.get('total_duplicates', 0)}")
            print(f"Total Failures: {summary_data.get('total_failures', 0)}")
            print(f"Total Rate Limit Errors: {summary_data.get('total_rate_limit_errors', 0)}")
            print(f"Total Disk Usage: {summary_data.get('total_disk_usage_bytes', 0):,} bytes")
            print(f"Total Storage Files: {summary_data.get('total_storage_files', 0)}")
            print(f"Total Processing Time: {summary_data.get('total_processing_time', 0):.2f} seconds")
            print(f"Total Unique Domains: {summary_data.get('total_unique_domains', 0)}")
            print(f"Total Proxy Rotations: {summary_data.get('total_proxy_rotations', 0)}")
            
            # Show daily breakdown
            daily_stats = summary.get('daily_stats', [])
            if daily_stats:
                print(f"\nDaily Breakdown ({len(daily_stats)} days):")
                print("-" * 50)
                for day in daily_stats[-7:]:  # Show last 7 days
                    print(f"{day['date']}: {day['downloads']} downloads, {day['failures']} failures")
        
    except Exception as e:
        print(f"✗ Failed to get summary: {e}")
        sys.exit(1)


def cmd_export_json(args):
    """Export metrics to JSON."""
    try:
        output_file = export_metrics_json(args.output_file)
        print(f"✓ Exported metrics to: {output_file}")
        
    except Exception as e:
        print(f"✗ Failed to export metrics: {e}")
        sys.exit(1)


def cmd_export_prometheus(args):
    """Export metrics to Prometheus format."""
    try:
        output_file = export_prometheus_metrics(args.output_file)
        print(f"✓ Exported Prometheus metrics to: {output_file}")
        
    except Exception as e:
        print(f"✗ Failed to export Prometheus metrics: {e}")
        sys.exit(1)


def cmd_update_stats(args):
    """Update daily statistics."""
    try:
        update_daily_stats(args.date)
        date = args.date or datetime.now().strftime('%Y-%m-%d')
        print(f"✓ Updated daily statistics for {date}")
        
    except Exception as e:
        print(f"✗ Failed to update daily stats: {e}")
        sys.exit(1)


def cmd_cleanup(args):
    """Clean up old metrics data."""
    try:
        collector = get_metrics_collector()
        collector.cleanup_old_metrics(days_to_keep=args.days_to_keep)
        print(f"✓ Cleaned up metrics older than {args.days_to_keep} days")
        
    except Exception as e:
        print(f"✗ Failed to cleanup metrics: {e}")
        sys.exit(1)


def cmd_dashboard(args):
    """Show monitoring dashboard."""
    try:
        # Get current stats
        today = datetime.now().strftime('%Y-%m-%d')
        stats = get_daily_stats(today)
        summary = get_metrics_summary(days=7)
        
        print("PixVault Monitoring Dashboard")
        print("=" * 50)
        print(f"Date: {today}")
        print(f"Time: {datetime.now().strftime('%H:%M:%S')}")
        print()
        
        # Today's stats
        print("Today's Statistics:")
        print("-" * 30)
        print(f"Downloads: {stats.downloads}")
        print(f"Duplicates: {stats.duplicates}")
        print(f"Failures: {stats.failures}")
        print(f"Rate Limit Errors: {stats.rate_limit_errors}")
        print(f"Disk Usage: {stats.disk_usage_bytes:,} bytes ({stats.disk_usage_bytes / 1024 / 1024:.2f} MB)")
        print(f"Storage Files: {stats.storage_files}")
        print(f"Processing Time: {stats.processing_time_seconds:.2f} seconds")
        print()
        
        # 7-day summary
        summary_data = summary.get('summary', {})
        print("7-Day Summary:")
        print("-" * 30)
        print(f"Total Downloads: {summary_data.get('total_downloads', 0)}")
        print(f"Total Failures: {summary_data.get('total_failures', 0)}")
        print(f"Total Disk Usage: {summary_data.get('total_disk_usage_bytes', 0):,} bytes")
        print(f"Average Daily Downloads: {summary_data.get('total_downloads', 0) / 7:.1f}")
        print()
        
        # Success rate
        total_attempts = summary_data.get('total_downloads', 0) + summary_data.get('total_failures', 0)
        if total_attempts > 0:
            success_rate = (summary_data.get('total_downloads', 0) / total_attempts) * 100
            print(f"Success Rate: {success_rate:.1f}%")
        
    except Exception as e:
        print(f"✗ Failed to show dashboard: {e}")
        sys.exit(1)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="PixVault Monitoring CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Record a metric
  python -m harvest.monitoring_cli record-metric --metric-name "downloads" --value 1 --labels "domain=example.com"
  
  # Get metrics
  python -m harvest.monitoring_cli get-metrics --metric-name "downloads" --start-date "2024-01-01"
  
  # Get daily stats
  python -m harvest.monitoring_cli daily-stats --date "2024-01-01"
  
  # Show dashboard
  python -m harvest.monitoring_cli dashboard
  
  # Export metrics
  python -m harvest.monitoring_cli export-json --output-file "metrics.json"
  python -m harvest.monitoring_cli export-prometheus --output-file "metrics.txt"
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Record metric command
    record_parser = subparsers.add_parser('record-metric', help='Record a metric value')
    record_parser.add_argument('--metric-name', required=True, help='Metric name')
    record_parser.add_argument('--value', type=float, required=True, help='Metric value')
    record_parser.add_argument('--labels', nargs='*', help='Labels in key=value format')
    record_parser.set_defaults(func=cmd_record_metric)
    
    # Get metrics command
    get_parser = subparsers.add_parser('get-metrics', help='Get metrics data')
    get_parser.add_argument('--metric-name', help='Specific metric name')
    get_parser.add_argument('--start-date', help='Start date (YYYY-MM-DD)')
    get_parser.add_argument('--end-date', help='End date (YYYY-MM-DD)')
    get_parser.add_argument('--limit', type=int, default=10, help='Limit results')
    get_parser.add_argument('--json', action='store_true', help='Output as JSON')
    get_parser.set_defaults(func=cmd_get_metrics)
    
    # Daily stats command
    stats_parser = subparsers.add_parser('daily-stats', help='Get daily statistics')
    stats_parser.add_argument('--date', help='Date (YYYY-MM-DD, default: today)')
    stats_parser.add_argument('--json', action='store_true', help='Output as JSON')
    stats_parser.set_defaults(func=cmd_daily_stats)
    
    # Summary command
    summary_parser = subparsers.add_parser('summary', help='Get metrics summary')
    summary_parser.add_argument('--days', type=int, default=7, help='Number of days')
    summary_parser.add_argument('--json', action='store_true', help='Output as JSON')
    summary_parser.set_defaults(func=cmd_summary)
    
    # Export JSON command
    export_json_parser = subparsers.add_parser('export-json', help='Export metrics to JSON')
    export_json_parser.add_argument('--output-file', help='Output file path')
    export_json_parser.set_defaults(func=cmd_export_json)
    
    # Export Prometheus command
    export_prom_parser = subparsers.add_parser('export-prometheus', help='Export metrics to Prometheus format')
    export_prom_parser.add_argument('--output-file', help='Output file path')
    export_prom_parser.set_defaults(func=cmd_export_prometheus)
    
    # Update stats command
    update_parser = subparsers.add_parser('update-stats', help='Update daily statistics')
    update_parser.add_argument('--date', help='Date (YYYY-MM-DD, default: today)')
    update_parser.set_defaults(func=cmd_update_stats)
    
    # Cleanup command
    cleanup_parser = subparsers.add_parser('cleanup', help='Clean up old metrics data')
    cleanup_parser.add_argument('--days-to-keep', type=int, default=90, help='Days to keep')
    cleanup_parser.set_defaults(func=cmd_cleanup)
    
    # Dashboard command
    dashboard_parser = subparsers.add_parser('dashboard', help='Show monitoring dashboard')
    dashboard_parser.set_defaults(func=cmd_dashboard)
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Execute the command
    args.func(args)


if __name__ == "__main__":
    main()
