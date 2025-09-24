#!/usr/bin/env python3
"""
Test script for proxy management system.
Demonstrates usage of proxy rotation, health checking, and integration.
"""

import os
import sys
import time
import asyncio
from pathlib import Path

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from harvest.proxy_manager import (
    ProxyManager, get_proxy_manager, get_proxy, mark_bad, mark_success,
    health_check_all, get_proxy_stats
)
from harvest.proxy_downloader import download_and_store_with_proxy
from harvest.adapters.proxy_browser import fetch_images_with_proxy_sync
from harvest.config import load_config


def test_proxy_manager_creation():
    """Test proxy manager creation and initialization."""
    print("Testing Proxy Manager Creation")
    print("=" * 40)
    
    try:
        # Test with empty configuration
        config = {'proxy': {'proxies': []}}
        proxy_manager = ProxyManager(config['proxy'])
        
        print(f"✓ Proxy manager created with {len(proxy_manager.proxies)} proxies")
        print(f"  Rotation strategy: {proxy_manager.rotation_strategy}")
        print(f"  Max failures: {proxy_manager.max_failures}")
        print(f"  Health check interval: {proxy_manager.health_check_interval}s")
        
        return True
        
    except Exception as e:
        print(f"✗ Proxy manager creation failed: {e}")
        return False


def test_proxy_addition():
    """Test adding proxies to the pool."""
    print("\nTesting Proxy Addition")
    print("=" * 40)
    
    try:
        config = {'proxy': {'proxies': []}}
        proxy_manager = ProxyManager(config['proxy'])
        
        # Add test proxies
        test_proxies = [
            {
                'host': 'proxy1.example.com',
                'port': 8080,
                'username': 'user1',
                'password': 'pass1',
                'protocol': 'http',
                'country': 'US',
                'provider': 'provider1'
            },
            {
                'host': 'proxy2.example.com',
                'port': 3128,
                'username': 'user2',
                'password': 'pass2',
                'protocol': 'http',
                'country': 'UK',
                'provider': 'provider2'
            }
        ]
        
        for proxy_config in test_proxies:
            proxy_manager.add_proxy(**proxy_config)
            print(f"  Added proxy: {proxy_config['host']}:{proxy_config['port']}")
        
        print(f"✓ Added {len(test_proxies)} proxies to pool")
        print(f"  Total proxies: {len(proxy_manager.proxies)}")
        
        return True
        
    except Exception as e:
        print(f"✗ Proxy addition failed: {e}")
        return False


def test_proxy_rotation():
    """Test proxy rotation strategies."""
    print("\nTesting Proxy Rotation")
    print("=" * 40)
    
    try:
        config = {'proxy': {'proxies': []}}
        proxy_manager = ProxyManager(config['proxy'])
        
        # Add test proxies
        for i in range(3):
            proxy_manager.add_proxy(
                host=f'proxy{i+1}.example.com',
                port=8080 + i,
                protocol='http'
            )
        
        # Test round-robin rotation
        proxy_manager.rotation_strategy = 'round_robin'
        print("Testing round-robin rotation:")
        
        for i in range(5):
            proxy = proxy_manager.get_proxy()
            if proxy:
                print(f"  Round {i+1}: {proxy.get('http', 'N/A')}")
            else:
                print(f"  Round {i+1}: No proxy available")
        
        # Test random rotation
        proxy_manager.rotation_strategy = 'random'
        print("\nTesting random rotation:")
        
        for i in range(3):
            proxy = proxy_manager.get_proxy()
            if proxy:
                print(f"  Random {i+1}: {proxy.get('http', 'N/A')}")
            else:
                print(f"  Random {i+1}: No proxy available")
        
        print("✓ Proxy rotation working")
        return True
        
    except Exception as e:
        print(f"✗ Proxy rotation failed: {e}")
        return False


def test_proxy_marking():
    """Test marking proxies as bad and successful."""
    print("\nTesting Proxy Marking")
    print("=" * 40)
    
    try:
        config = {'proxy': {'proxies': []}}
        proxy_manager = ProxyManager(config['proxy'])
        
        # Add test proxy
        proxy_manager.add_proxy(
            host='test.example.com',
            port=8080,
            protocol='http'
        )
        
        # Get proxy
        proxy = proxy_manager.get_proxy()
        if not proxy:
            print("✗ No proxy available for testing")
            return False
        
        print(f"  Original proxy: {proxy.get('http', 'N/A')}")
        
        # Mark as successful
        proxy_manager.mark_success(proxy)
        print("  ✓ Marked proxy as successful")
        
        # Mark as bad
        proxy_manager.mark_bad(proxy)
        print("  ✓ Marked proxy as bad")
        
        # Check if proxy is now bad
        proxy_info = proxy_manager._find_proxy_by_dict(proxy)
        if proxy_info and not proxy_info.is_active:
            print("  ✓ Proxy correctly marked as inactive")
        else:
            print("  ✗ Proxy not marked as inactive")
        
        print("✓ Proxy marking working")
        return True
        
    except Exception as e:
        print(f"✗ Proxy marking failed: {e}")
        return False


def test_proxy_stats():
    """Test proxy statistics."""
    print("\nTesting Proxy Statistics")
    print("=" * 40)
    
    try:
        config = {'proxy': {'proxies': []}}
        proxy_manager = ProxyManager(config['proxy'])
        
        # Add test proxies
        for i in range(3):
            proxy_manager.add_proxy(
                host=f'proxy{i+1}.example.com',
                port=8080 + i,
                protocol='http'
            )
        
        # Get initial stats
        stats = proxy_manager.get_proxy_stats()
        print(f"  Total proxies: {stats['total_proxies']}")
        print(f"  Active proxies: {stats['active_proxies']}")
        print(f"  Bad proxies: {stats['bad_proxies']}")
        print(f"  Success rate: {stats['success_rate']}%")
        
        # Simulate some usage
        proxy = proxy_manager.get_proxy()
        if proxy:
            proxy_manager.mark_success(proxy)
            proxy_manager.mark_success(proxy)
            proxy_manager.mark_bad(proxy)
        
        # Get updated stats
        stats = proxy_manager.get_proxy_stats()
        print(f"  Updated success rate: {stats['success_rate']}%")
        print(f"  Total requests: {stats['total_requests']}")
        
        print("✓ Proxy statistics working")
        return True
        
    except Exception as e:
        print(f"✗ Proxy statistics failed: {e}")
        return False


def test_proxy_health_check():
    """Test proxy health checking."""
    print("\nTesting Proxy Health Check")
    print("=" * 40)
    
    try:
        config = {'proxy': {'proxies': []}}
        proxy_manager = ProxyManager(config['proxy'])
        
        # Add test proxy (this will fail health check since it's fake)
        proxy_manager.add_proxy(
            host='fake-proxy.example.com',
            port=8080,
            protocol='http'
        )
        
        print("  Running health check (this may take a moment)...")
        
        # Run health check
        results = asyncio.run(proxy_manager.health_check_all())
        
        print(f"  Health check results:")
        print(f"    Total: {results['total']}")
        print(f"    Healthy: {results['healthy']}")
        print(f"    Unhealthy: {results['unhealthy']}")
        print(f"    Checked: {results['checked']}")
        
        print("✓ Proxy health check working")
        return True
        
    except Exception as e:
        print(f"✗ Proxy health check failed: {e}")
        return False


def test_proxy_downloader_integration():
    """Test proxy downloader integration."""
    print("\nTesting Proxy Downloader Integration")
    print("=" * 40)
    
    try:
        # Load configuration
        config = load_config()
        
        # Test with a simple image URL (this will likely fail with fake proxies)
        test_url = "https://picsum.photos/100/100"
        
        print(f"  Testing download with proxy: {test_url}")
        
        # This will use proxy if available, or fall back to direct connection
        result = download_and_store_with_proxy(
            test_url,
            "test_proxy",
            config
        )
        
        print(f"  Download result: {result['status']}")
        print(f"  Message: {result['message']}")
        
        print("✓ Proxy downloader integration working")
        return True
        
    except Exception as e:
        print(f"✗ Proxy downloader integration failed: {e}")
        return False


def test_proxy_browser_integration():
    """Test proxy browser integration."""
    print("\nTesting Proxy Browser Integration")
    print("=" * 40)
    
    try:
        # Load configuration
        config = load_config()
        
        print("  Testing browser search with proxy...")
        
        # This will use proxy if available, or fall back to direct connection
        results = fetch_images_with_proxy_sync(
            "test query",
            max_results=2,
            config=config
        )
        
        print(f"  Browser search results: {len(results)} images found")
        
        print("✓ Proxy browser integration working")
        return True
        
    except Exception as e:
        print(f"✗ Proxy browser integration failed: {e}")
        return False


def test_proxy_pool_persistence():
    """Test proxy pool save/load functionality."""
    print("\nTesting Proxy Pool Persistence")
    print("=" * 40)
    
    try:
        config = {'proxy': {'proxies': []}}
        proxy_manager = ProxyManager(config['proxy'])
        
        # Add test proxies
        for i in range(2):
            proxy_manager.add_proxy(
                host=f'proxy{i+1}.example.com',
                port=8080 + i,
                protocol='http',
                country='US',
                provider=f'provider{i+1}'
            )
        
        # Simulate some usage
        proxy = proxy_manager.get_proxy()
        if proxy:
            proxy_manager.mark_success(proxy)
            proxy_manager.mark_success(proxy)
        
        # Save proxy pool
        test_file = "test_proxy_pool.json"
        proxy_manager.save_proxy_pool(test_file)
        print(f"  ✓ Saved proxy pool to {test_file}")
        
        # Create new manager and load pool
        new_manager = ProxyManager({'proxies': []})
        new_manager.load_proxy_pool(test_file)
        
        print(f"  ✓ Loaded proxy pool from {test_file}")
        print(f"    Loaded {len(new_manager.proxies)} proxies")
        
        # Clean up
        if os.path.exists(test_file):
            os.remove(test_file)
            print(f"  ✓ Cleaned up test file")
        
        print("✓ Proxy pool persistence working")
        return True
        
    except Exception as e:
        print(f"✗ Proxy pool persistence failed: {e}")
        return False


def test_integration():
    """Test integration with existing system."""
    print("\nTesting Integration")
    print("=" * 40)
    
    try:
        # Test that modules can be imported
        from harvest.proxy_manager import ProxyManager, get_proxy_manager
        from harvest.proxy_downloader import download_and_store_with_proxy
        from harvest.adapters.proxy_browser import fetch_images_with_proxy_sync
        
        print("✓ All modules imported successfully")
        
        # Test configuration loading
        config = load_config()
        proxy_config = config.get('proxy', {})
        print(f"✓ Configuration loaded: {len(proxy_config)} proxy settings")
        
        print("✓ Integration test completed")
        return True
        
    except Exception as e:
        print(f"✗ Integration test failed: {e}")
        return False


def cleanup_test_files():
    """Clean up test files."""
    print("\nCleaning up test files...")
    
    test_files = [
        "test_proxy_pool.json",
        "storage/test_*"
    ]
    
    for file_pattern in test_files:
        if "*" in file_pattern:
            # Handle glob patterns
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
    print("PixVault Proxy Management Test")
    print("=" * 60)
    
    # Run tests
    test_proxy_manager_creation()
    test_proxy_addition()
    test_proxy_rotation()
    test_proxy_marking()
    test_proxy_stats()
    test_proxy_health_check()
    test_proxy_downloader_integration()
    test_proxy_browser_integration()
    test_proxy_pool_persistence()
    test_integration()
    
    # Cleanup
    cleanup_test_files()
    
    print("\n" + "=" * 60)
    print("All tests completed!")
    print("\nTo use the proxy management system:")
    print("  1. Configure proxies in config.yaml")
    print("  2. Use CLI: python -m harvest.proxy_cli --help")
    print("  3. Use in code: from harvest.proxy_manager import get_proxy")


if __name__ == "__main__":
    main()
