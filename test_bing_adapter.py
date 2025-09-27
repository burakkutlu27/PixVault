#!/usr/bin/env python3
"""
Test script for Bing adapter.
This demonstrates how to use the BingAdapter class.
"""

import os
import sys
from harvest.adapters.bing import BingAdapter

def test_bing_adapter():
    """Test the Bing adapter functionality."""
    
    # Configuration (you need to add your Bing API key to config.yaml)
    config = {
        'api_key': 'your_bing_api_key_here'  # Replace with actual API key
    }
    
    try:
        # Initialize the adapter
        adapter = BingAdapter(config)
        
        # Test search functionality
        print("🔍 Searching for 'nature' images...")
        results = adapter.search('nature', limit=5)
        
        print(f"✅ Found {len(results)} images")
        
        # Display results
        for i, result in enumerate(results, 1):
            print(f"\n📸 Image {i}:")
            print(f"   URL: {result['url']}")
            print(f"   ID: {result['id']}")
            print(f"   Title: {result['title']}")
            print(f"   Size: {result['width']}x{result['height']}")
            print(f"   Source: {result['source']}")
        
        # Test download functionality (only if we have results)
        if results:
            print(f"\n💾 Testing download of first image...")
            
            # Create output directory
            output_dir = "test_downloads"
            os.makedirs(output_dir, exist_ok=True)
            
            # Download the first image
            try:
                file_path = adapter.download(results[0], output_dir)
                print(f"✅ Downloaded to: {file_path}")
                
                # Check if file exists and get size
                if os.path.exists(file_path):
                    file_size = os.path.getsize(file_path)
                    print(f"📁 File size: {file_size:,} bytes")
                else:
                    print("❌ File not found after download")
                    
            except Exception as e:
                print(f"❌ Download failed: {e}")
        else:
            print("⚠️  No images found to download")
            
    except ValueError as e:
        print(f"❌ Configuration error: {e}")
        print("💡 Please add your Bing API key to the config")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    test_bing_adapter()
