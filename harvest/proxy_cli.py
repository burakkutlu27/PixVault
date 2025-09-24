#!/usr/bin/env python3
"""
CLI for proxy pool management.
Provides commands for managing proxies, health checking, and monitoring.
"""

import argparse
import sys
import json
import asyncio
from typing import Dict, Any, List

from .proxy_manager import (
    ProxyManager, get_proxy_manager, get_proxy, mark_bad, mark_success,
    health_check_all, get_proxy_stats
)
from .config import load_config
from .utils.logger import get_logger


def cmd_get_proxy(args):
    """Get next available proxy."""
    try:
        config = load_config(args.config)
        proxy_manager = get_proxy_manager(config.get('proxy', {}))
        
        proxy = proxy_manager.get_proxy()
        
        if proxy:
            print("✓ Proxy obtained:")
            print(f"  HTTP: {proxy.get('http', 'N/A')}")
            print(f"  HTTPS: {proxy.get('https', 'N/A')}")
        else:
            print("✗ No proxies available")
            sys.exit(1)
        
    except Exception as e:
        print(f"✗ Failed to get proxy: {e}")
        sys.exit(1)


def cmd_mark_bad(args):
    """Mark a proxy as bad."""
    try:
        config = load_config(args.config)
        proxy_manager = get_proxy_manager(config.get('proxy', {}))
        
        # Create proxy dict from arguments
        proxy_dict = {
            'http': f"http://{args.host}:{args.port}",
            'https': f"http://{args.host}:{args.port}"
        }
        
        proxy_manager.mark_bad(proxy_dict)
        print(f"✓ Marked proxy {args.host}:{args.port} as bad")
        
    except Exception as e:
        print(f"✗ Failed to mark proxy as bad: {e}")
        sys.exit(1)


def cmd_mark_success(args):
    """Mark a proxy as successful."""
    try:
        config = load_config(args.config)
        proxy_manager = get_proxy_manager(config.get('proxy', {}))
        
        # Create proxy dict from arguments
        proxy_dict = {
            'http': f"http://{args.host}:{args.port}",
            'https': f"http://{args.host}:{args.port}"
        }
        
        proxy_manager.mark_success(proxy_dict)
        print(f"✓ Marked proxy {args.host}:{args.port} as successful")
        
    except Exception as e:
        print(f"✗ Failed to mark proxy as successful: {e}")
        sys.exit(1)


def cmd_health_check(args):
    """Check health of all proxies."""
    try:
        config = load_config(args.config)
        proxy_manager = get_proxy_manager(config.get('proxy', {}))
        
        print("Starting health check for all proxies...")
        
        # Run health check
        results = asyncio.run(proxy_manager.health_check_all())
        
        print(f"Health Check Results:")
        print(f"  Total Proxies: {results['total']}")
        print(f"  Healthy: {results['healthy']}")
        print(f"  Unhealthy: {results['unhealthy']}")
        print(f"  Checked: {results['checked']}")
        
        if args.json:
            print(json.dumps(results, indent=2))
        
    except Exception as e:
        print(f"✗ Health check failed: {e}")
        sys.exit(1)


def cmd_stats(args):
    """Get proxy statistics."""
    try:
        config = load_config(args.config)
        proxy_manager = get_proxy_manager(config.get('proxy', {}))
        
        stats = proxy_manager.get_proxy_stats()
        
        if args.json:
            print(json.dumps(stats, indent=2))
        else:
            print("Proxy Statistics:")
            print(f"  Total Proxies: {stats['total_proxies']}")
            print(f"  Active Proxies: {stats['active_proxies']}")
            print(f"  Bad Proxies: {stats['bad_proxies']}")
            print(f"  Success Rate: {stats['success_rate']}%")
            print(f"  Total Requests: {stats['total_requests']}")
            print(f"  Total Success: {stats['total_success']}")
            print(f"  Total Failures: {stats['total_failures']}")
        
    except Exception as e:
        print(f"✗ Failed to get proxy stats: {e}")
        sys.exit(1)


def cmd_list_proxies(args):
    """List all proxies."""
    try:
        config = load_config(args.config)
        proxy_manager = get_proxy_manager(config.get('proxy', {}))
        
        proxies = proxy_manager.proxies
        
        if not proxies:
            print("No proxies configured")
            return
        
        print(f"Proxy Pool ({len(proxies)} proxies):")
        print("-" * 80)
        
        for i, proxy in enumerate(proxies, 1):
            status = "✓ Active" if proxy.is_active else "✗ Inactive"
            if proxy in proxy_manager.bad_proxies:
                status = "✗ Bad"
            
            print(f"{i:2d}. {proxy.host}:{proxy.port} ({proxy.protocol}) - {status}")
            if proxy.country:
                print(f"     Country: {proxy.country}")
            if proxy.provider:
                print(f"     Provider: {proxy.provider}")
            print(f"     Success: {proxy.success_count}, Failures: {proxy.failure_count}")
            print()
        
    except Exception as e:
        print(f"✗ Failed to list proxies: {e}")
        sys.exit(1)


def cmd_add_proxy(args):
    """Add a new proxy to the pool."""
    try:
        config = load_config(args.config)
        proxy_manager = get_proxy_manager(config.get('proxy', {}))
        
        proxy_manager.add_proxy(
            host=args.host,
            port=args.port,
            username=args.username,
            password=args.password,
            protocol=args.protocol,
            country=args.country,
            provider=args.provider
        )
        
        print(f"✓ Added proxy {args.host}:{args.port}")
        
    except Exception as e:
        print(f"✗ Failed to add proxy: {e}")
        sys.exit(1)


def cmd_remove_proxy(args):
    """Remove a proxy from the pool."""
    try:
        config = load_config(args.config)
        proxy_manager = get_proxy_manager(config.get('proxy', {}))
        
        success = proxy_manager.remove_proxy(args.host, args.port)
        
        if success:
            print(f"✓ Removed proxy {args.host}:{args.port}")
        else:
            print(f"✗ Proxy {args.host}:{args.port} not found")
            sys.exit(1)
        
    except Exception as e:
        print(f"✗ Failed to remove proxy: {e}")
        sys.exit(1)


def cmd_reset_bad(args):
    """Reset all bad proxies to active state."""
    try:
        config = load_config(args.config)
        proxy_manager = get_proxy_manager(config.get('proxy', {}))
        
        proxy_manager.reset_bad_proxies()
        print("✓ Reset all bad proxies to active state")
        
    except Exception as e:
        print(f"✗ Failed to reset bad proxies: {e}")
        sys.exit(1)


def cmd_save_pool(args):
    """Save proxy pool to file."""
    try:
        config = load_config(args.config)
        proxy_manager = get_proxy_manager(config.get('proxy', {}))
        
        proxy_manager.save_proxy_pool(args.file)
        print(f"✓ Saved proxy pool to {args.file}")
        
    except Exception as e:
        print(f"✗ Failed to save proxy pool: {e}")
        sys.exit(1)


def cmd_load_pool(args):
    """Load proxy pool from file."""
    try:
        config = load_config(args.config)
        proxy_manager = get_proxy_manager(config.get('proxy', {}))
        
        proxy_manager.load_proxy_pool(args.file)
        print(f"✓ Loaded proxy pool from {args.file}")
        
    except Exception as e:
        print(f"✗ Failed to load proxy pool: {e}")
        sys.exit(1)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Proxy pool management CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Get next available proxy
  python -m harvest.proxy_cli get-proxy
  
  # Check health of all proxies
  python -m harvest.proxy_cli health-check
  
  # Get proxy statistics
  python -m harvest.proxy_cli stats
  
  # List all proxies
  python -m harvest.proxy_cli list-proxies
  
  # Add a new proxy
  python -m harvest.proxy_cli add-proxy --host "proxy.example.com" --port 8080
  
  # Mark proxy as bad
  python -m harvest.proxy_cli mark-bad --host "proxy.example.com" --port 8080
        """
    )
    
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to configuration file (default: config.yaml)"
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Get proxy command
    get_parser = subparsers.add_parser('get-proxy', help='Get next available proxy')
    get_parser.set_defaults(func=cmd_get_proxy)
    
    # Mark bad command
    mark_bad_parser = subparsers.add_parser('mark-bad', help='Mark proxy as bad')
    mark_bad_parser.add_argument('--host', required=True, help='Proxy host')
    mark_bad_parser.add_argument('--port', type=int, required=True, help='Proxy port')
    mark_bad_parser.set_defaults(func=cmd_mark_bad)
    
    # Mark success command
    mark_success_parser = subparsers.add_parser('mark-success', help='Mark proxy as successful')
    mark_success_parser.add_argument('--host', required=True, help='Proxy host')
    mark_success_parser.add_argument('--port', type=int, required=True, help='Proxy port')
    mark_success_parser.set_defaults(func=cmd_mark_success)
    
    # Health check command
    health_parser = subparsers.add_parser('health-check', help='Check health of all proxies')
    health_parser.add_argument('--json', action='store_true', help='Output as JSON')
    health_parser.set_defaults(func=cmd_health_check)
    
    # Stats command
    stats_parser = subparsers.add_parser('stats', help='Get proxy statistics')
    stats_parser.add_argument('--json', action='store_true', help='Output as JSON')
    stats_parser.set_defaults(func=cmd_stats)
    
    # List proxies command
    list_parser = subparsers.add_parser('list-proxies', help='List all proxies')
    list_parser.set_defaults(func=cmd_list_proxies)
    
    # Add proxy command
    add_parser = subparsers.add_parser('add-proxy', help='Add new proxy to pool')
    add_parser.add_argument('--host', required=True, help='Proxy host')
    add_parser.add_argument('--port', type=int, required=True, help='Proxy port')
    add_parser.add_argument('--username', help='Proxy username')
    add_parser.add_argument('--password', help='Proxy password')
    add_parser.add_argument('--protocol', default='http', help='Proxy protocol (http, https, socks4, socks5)')
    add_parser.add_argument('--country', help='Proxy country')
    add_parser.add_argument('--provider', help='Proxy provider')
    add_parser.set_defaults(func=cmd_add_proxy)
    
    # Remove proxy command
    remove_parser = subparsers.add_parser('remove-proxy', help='Remove proxy from pool')
    remove_parser.add_argument('--host', required=True, help='Proxy host')
    remove_parser.add_argument('--port', type=int, required=True, help='Proxy port')
    remove_parser.set_defaults(func=cmd_remove_proxy)
    
    # Reset bad proxies command
    reset_parser = subparsers.add_parser('reset-bad', help='Reset all bad proxies to active')
    reset_parser.set_defaults(func=cmd_reset_bad)
    
    # Save pool command
    save_parser = subparsers.add_parser('save-pool', help='Save proxy pool to file')
    save_parser.add_argument('--file', required=True, help='Output file path')
    save_parser.set_defaults(func=cmd_save_pool)
    
    # Load pool command
    load_parser = subparsers.add_parser('load-pool', help='Load proxy pool from file')
    load_parser.add_argument('--file', required=True, help='Input file path')
    load_parser.set_defaults(func=cmd_load_pool)
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Execute the command
    args.func(args)


if __name__ == "__main__":
    main()
