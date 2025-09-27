#!/usr/bin/env python3
"""
Test script for HTTP Client.
This demonstrates User-Agent rotation and proxy functionality.
"""

import os
import sys
from harvest.utils.http_client import get_http_client, reset_http_client
from harvest.config import load_config

def test_http_client():
    """Test the HTTP client functionality."""
    
    try:
        # Load configuration
        config = load_config("config.yaml")
        http_config = config.get('http_client', {})
        
        print("🌐 Testing HTTP Client")
        print("=" * 50)
        
        # Test 1: Basic HTTP client initialization
        print("🔧 Testing HTTP client initialization...")
        http_client = get_http_client(http_config)
        print(f"✅ HTTP client initialized")
        print(f"   User-Agents: {len(http_client.user_agents)}")
        print(f"   Proxies: {len(http_client.proxies)}")
        print(f"   Timeout: {http_client.timeout}s")
        print(f"   Max retries: {http_client.max_retries}")
        
        # Test 2: User-Agent rotation
        print(f"\n🎭 Testing User-Agent rotation...")
        user_agents = []
        for i in range(5):
            ua = http_client.get_random_user_agent()
            user_agents.append(ua)
            print(f"   Request {i+1}: {ua[:50]}...")
        
        # Check if we got different User-Agents
        unique_uas = set(user_agents)
        print(f"   Unique User-Agents: {len(unique_uas)}/{len(user_agents)}")
        
        # Test 3: Proxy rotation (if configured)
        print(f"\n🔄 Testing proxy rotation...")
        if http_client.proxies:
            for i in range(3):
                proxy = http_client.get_random_proxy()
                if proxy:
                    print(f"   Proxy {i+1}: {proxy.get('host', 'unknown')}:{proxy.get('port', 'unknown')}")
                else:
                    print(f"   Proxy {i+1}: None")
        else:
            print("   No proxies configured")
        
        # Test 4: Make actual HTTP requests
        print(f"\n🌍 Testing HTTP requests...")
        
        # Test with a simple API endpoint
        test_urls = [
            "https://httpbin.org/user-agent",
            "https://httpbin.org/ip",
            "https://httpbin.org/headers"
        ]
        
        for i, url in enumerate(test_urls, 1):
            try:
                print(f"   Request {i}: {url}")
                response = http_client.get(url)
                
                if response.status_code == 200:
                    data = response.json()
                    if 'user-agent' in data:
                        print(f"      User-Agent: {data['user-agent'][:50]}...")
                    if 'origin' in data:
                        print(f"      Origin IP: {data['origin']}")
                    print(f"      Status: ✅ {response.status_code}")
                else:
                    print(f"      Status: ❌ {response.status_code}")
                    
            except Exception as e:
                print(f"      Error: ❌ {e}")
        
        # Test 5: Test with different adapters
        print(f"\n🔌 Testing with adapters...")
        
        # Test Unsplash adapter
        try:
            from harvest.adapters.unsplash import UnsplashAdapter
            
            # Create adapter with HTTP client config
            adapter_config = {
                'api_key': 'test_key',  # This will fail but we can test the HTTP client integration
                'http_client': http_config
            }
            
            adapter = UnsplashAdapter(adapter_config)
            print(f"   ✅ Unsplash adapter created with HTTP client")
            
        except Exception as e:
            print(f"   ⚠️  Unsplash adapter test: {e}")
        
        # Test 6: Reset and reinitialize
        print(f"\n🔄 Testing client reset...")
        reset_http_client()
        new_client = get_http_client(http_config)
        print(f"   ✅ HTTP client reset and reinitialized")
        
        print(f"\n🎉 HTTP Client test completed!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_http_client()
