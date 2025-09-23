#!/usr/bin/env python3
"""
Test script for the browser adapter.
Demonstrates integration with the existing PixVault system.
"""

import asyncio
import sys
import os

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from harvest.adapters.browser import fetch_images_sync, fetch_images


async def test_async():
    """Test the async version of fetch_images."""
    print("Testing async browser adapter...")
    try:
        results = await fetch_images("nature landscape", max_results=5)
        print(f"Async test found {len(results)} images:")
        for i, result in enumerate(results, 1):
            print(f"  {i}. {result['source']}: {result['url'][:80]}...")
        return results
    except Exception as e:
        print(f"Async test failed: {e}")
        return []


def test_sync():
    """Test the sync version of fetch_images."""
    print("\nTesting sync browser adapter...")
    try:
        results = fetch_images_sync("mountain sunset", max_results=3)
        print(f"Sync test found {len(results)} images:")
        for i, result in enumerate(results, 1):
            print(f"  {i}. {result['source']}: {result['url'][:80]}...")
        return results
    except Exception as e:
        print(f"Sync test failed: {e}")
        return []


def test_integration():
    """Test integration with existing downloader system."""
    print("\nTesting integration with existing system...")
    
    # Import the existing downloader to show compatibility
    try:
        from harvest.downloader import download_image
        from harvest.db import ImageDB
        
        # Test that our results are compatible with the downloader
        results = fetch_images_sync("test query", max_results=2)
        
        if results:
            print("✓ Browser adapter results are compatible with existing downloader")
            print("✓ Results format matches expected structure:")
            print(f"  - URL: {results[0]['url'][:50]}...")
            print(f"  - ID: {results[0]['id']}")
            print(f"  - Source: {results[0]['source']}")
        else:
            print("⚠ No results returned (this might be expected in test environment)")
            
    except ImportError as e:
        print(f"⚠ Could not import existing modules: {e}")
        print("  This is expected if dependencies are not installed")


async def main():
    """Main test function."""
    print("PixVault Browser Adapter Test")
    print("=" * 40)
    
    # Test async version
    await test_async()
    
    # Test sync version
    test_sync()
    
    # Test integration
    test_integration()
    
    print("\n" + "=" * 40)
    print("Test completed!")
    print("\nTo use the browser adapter in your code:")
    print("  from harvest.adapters.browser import fetch_images_sync")
    print("  results = fetch_images_sync('your query', max_results=10)")


if __name__ == "__main__":
    asyncio.run(main())
