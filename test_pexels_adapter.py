#!/usr/bin/env python3
"""
Test script for Pexels adapter.
This demonstrates how to use the PexelsAdapter class.
"""

import os
import sys
from harvest.adapters.pexels import PexelsAdapter

def test_pexels_adapter():
    """Test the Pexels adapter functionality."""
    
    # Configuration (you need to add your Pexels API key to config.yaml)
    config = {
        'api_key': 'your_pexels_api_key_here'  # Replace with actual API key
    }
    
    try:
        # Initialize the adapter
        adapter = PexelsAdapter(config)
        
        # Test search functionality
        print("🔍 Searching for 'landscape' images...")
        results = adapter.search('landscape', limit=5)
        
        print(f"✅ Found {len(results)} images")
        
        # Display results
        for i, result in enumerate(results, 1):
            print(f"\n📸 Image {i}:")
            print(f"   URL: {result['url']}")
            print(f"   ID: {result['id']}")
            print(f"   Title: {result['title']}")
            print(f"   Size: {result['width']}x{result['height']}")
            print(f"   Photographer: {result['photographer']}")
            print(f"   Source: {result['source']}")
            print(f"   Average Color: {result['avg_color']}")
        
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
                    
                    # Show photographer info
                    photographer_info = adapter.get_photographer_info(results[0])
                    print(f"👤 Photographer: {photographer_info['name']}")
                    print(f"🔗 Photographer URL: {photographer_info['url']}")
                    
                    # Show image variants
                    variants = adapter.get_image_variants(results[0])
                    print(f"🖼️  Available sizes: {list(variants.keys())}")
                    
                else:
                    print("❌ File not found after download")
                    
            except Exception as e:
                print(f"❌ Download failed: {e}")
        else:
            print("⚠️  No images found to download")
            
    except ValueError as e:
        print(f"❌ Configuration error: {e}")
        print("💡 Please add your Pexels API key to the config")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    test_pexels_adapter()
