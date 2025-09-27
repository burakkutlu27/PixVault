#!/usr/bin/env python3
"""
Example usage of the retry mechanism with exponential backoff.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '.'))

from harvest.utils.retry import create_retryable_client, retry_with_backoff
from harvest.utils.logger import get_logger

logger = get_logger("example_retry")


def example_basic_retry():
    """Example of basic retry usage."""
    logger.info("Example: Basic retry usage")
    
    try:
        # This will automatically retry on failures
        with create_retryable_client(timeout=10) as client:
            response = client.get("https://httpbin.org/status/200")
            logger.info(f"Success: {response.status_code}")
            
    except Exception as e:
        logger.error(f"Request failed: {e}")


@retry_with_backoff(max_retries=3, base_delay=1.0)
def example_decorator_retry():
    """Example of using the retry decorator."""
    logger.info("Example: Using retry decorator")
    
    # This function will be automatically retried if it fails
    import httpx
    
    with httpx.Client() as client:
        response = client.get("https://httpbin.org/status/200")
        response.raise_for_status()
        return response


def example_retry_configuration():
    """Example of custom retry configuration."""
    logger.info("Example: Custom retry configuration")
    
    from harvest.utils.retry import RetryableHTTPClient, RetryConfig
    
    # Custom retry configuration
    config = RetryConfig(
        max_retries=5,           # Try up to 5 times
        base_delay=0.5,          # Start with 0.5 second delay
        max_delay=30.0,          # Cap at 30 seconds
        exponential_base=1.5,    # Slower exponential growth
        jitter=True              # Add random jitter
    )
    
    # Use custom configuration
    with RetryableHTTPClient(config=config, timeout=10) as client:
        try:
            response = client.get("https://httpbin.org/status/200")
            logger.info(f"Success with custom config: {response.status_code}")
        except Exception as e:
            logger.error(f"Request failed with custom config: {e}")


def main():
    """Run retry examples."""
    print("PixVault Retry Mechanism Examples")
    print("=" * 40)
    print()
    
    print("The retry mechanism provides:")
    print("✅ Automatic retry on network errors (timeout, connection error)")
    print("✅ Automatic retry on 5xx server errors")
    print("✅ Exponential backoff with jitter (1s, 2s, 4s delays)")
    print("✅ Configurable retry attempts (default: 3)")
    print("✅ Works with both sync and async code")
    print("✅ Integrated into all httpx requests in the codebase")
    print()
    
    print("Usage examples:")
    print()
    print("1. Basic usage (automatic retry):")
    print("   with create_retryable_client() as client:")
    print("       response = client.get(url)")
    print()
    print("2. Decorator usage:")
    print("   @retry_with_backoff(max_retries=3)")
    print("   def my_function():")
    print("       # This function will be retried automatically")
    print()
    print("3. Custom configuration:")
    print("   config = RetryConfig(max_retries=5, base_delay=0.5)")
    print("   with RetryableHTTPClient(config=config) as client:")
    print("       response = client.get(url)")
    print()
    
    print("The retry mechanism is now integrated into:")
    print("- harvest.adapters.unsplash.search_unsplash()")
    print("- harvest.downloader.download_and_store()")
    print("- harvest.proxy_downloader.download_and_store_with_proxy()")
    print("- harvest.proxy_manager.health_check_proxy()")
    print()
    
    print("All httpx requests will now automatically retry on failures!")
    print("No code changes needed - it's transparent to existing code.")


if __name__ == "__main__":
    main()
