#!/usr/bin/env python3
"""
Test script for Adapter Manager.
This demonstrates how to use the AdapterManager class.
"""

import os
import sys
from harvest.adapters.manager import get_all_adapters, get_adapter
from harvest.config import load_config

def test_adapter_manager():
    """Test the adapter manager functionality."""
    
    try:
        # Load configuration
        config = load_config("config.yaml")
        
        # Test 1: Get all adapters
        print("🔧 Testing Adapter Manager")
        print("=" * 50)
        
        manager = get_all_adapters(config)
        available_sources = manager.get_available_adapters()
        
        print(f"✅ Available sources: {', '.join(available_sources) if available_sources else 'None'}")
        
        if not available_sources:
            print("⚠️  No adapters available. Please check your API keys in config.yaml")
            return
        
        # Test 2: Search all sources
        print(f"\n🔍 Testing search across all sources for 'nature'...")
        all_results = manager.search_all('nature', limit=3)
        
        total_results = 0
        for source, results in all_results.items():
            print(f"📸 {source}: {len(results)} images")
            total_results += len(results)
            
            # Show first result details
            if results:
                first_result = results[0]
                print(f"   First image: {first_result.get('title', 'No title')}")
                print(f"   URL: {first_result.get('url', 'No URL')[:50]}...")
                print(f"   Size: {first_result.get('width', '?')}x{first_result.get('height', '?')}")
        
        print(f"\n📊 Total images found: {total_results}")
        
        # Test 3: Get specific adapter
        print(f"\n🎯 Testing specific adapter access...")
        for source in available_sources:
            adapter = get_adapter(source, config)
            if adapter:
                print(f"✅ {source} adapter: {type(adapter).__name__}")
            else:
                print(f"❌ {source} adapter: Failed to get")
        
        # Test 4: Get source statistics
        print(f"\n📈 Testing source statistics...")
        stats = manager.get_source_stats('landscape', limit=2)
        
        for source, stat in stats.items():
            print(f"📊 {source}:")
            print(f"   Count: {stat['count']}")
            print(f"   Has results: {stat['has_results']}")
            if stat['dimensions']:
                print(f"   Dimensions: {stat['dimensions'][:2]}...")  # Show first 2
        
        # Test 5: Download test (if we have results)
        if total_results > 0:
            print(f"\n💾 Testing download functionality...")
            
            # Create output directory
            output_dir = "test_downloads"
            os.makedirs(output_dir, exist_ok=True)
            
            # Try to download from first available source
            for source, results in all_results.items():
                if results:
                    print(f"📥 Testing download from {source}...")
                    try:
                        adapter = manager.get_adapter(source)
                        if adapter:
                            # Download first image
                            file_path = adapter.download(results[0], output_dir)
                            print(f"✅ Downloaded: {file_path}")
                            
                            # Check file
                            if os.path.exists(file_path):
                                file_size = os.path.getsize(file_path)
                                print(f"📁 File size: {file_size:,} bytes")
                            break
                    except Exception as e:
                        print(f"❌ Download failed: {e}")
                        continue
        
        print(f"\n🎉 Adapter Manager test completed!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_adapter_manager()
