#!/usr/bin/env python3
"""
Test script for the retry mechanism with exponential backoff.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '.'))

import asyncio
import httpx
from harvest.utils.retry import create_retryable_client, async_retry_with_backoff
from harvest.utils.logger import get_logger

logger = get_logger("test_retry")


def test_sync_retry():
    """Test synchronous retry mechanism."""
    logger.info("Testing synchronous retry mechanism...")
    
    try:
        # Test with a working URL
        with create_retryable_client(timeout=10) as client:
            response = client.get("https://httpbin.org/status/200")
            logger.info(f"Success: Status {response.status_code}")
        
        # Test with a URL that returns 5xx (should trigger retries)
        try:
            with create_retryable_client(timeout=10) as client:
                response = client.get("https://httpbin.org/status/500")
                logger.info(f"Unexpected success: Status {response.status_code}")
        except httpx.HTTPStatusError as e:
            logger.info(f"Expected error caught: {e}")
        
        logger.info("Synchronous retry test completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Synchronous retry test failed: {e}")
        return False


async def test_async_retry():
    """Test asynchronous retry mechanism."""
    logger.info("Testing asynchronous retry mechanism...")
    
    async def failing_function():
        """Function that fails on first attempts."""
        async with httpx.AsyncClient() as client:
            response = await client.get("https://httpbin.org/status/500")
            response.raise_for_status()
            return response
    
    try:
        # This should fail and retry
        try:
            await async_retry_with_backoff(failing_function)
            logger.info("Unexpected success in async retry test")
        except httpx.HTTPStatusError as e:
            logger.info(f"Expected error caught in async retry: {e}")
        
        logger.info("Asynchronous retry test completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Asynchronous retry test failed: {e}")
        return False


def test_retry_configuration():
    """Test retry configuration and timing."""
    logger.info("Testing retry configuration...")
    
    from harvest.utils.retry import RetryConfig, calculate_delay
    
    config = RetryConfig(
        max_retries=3,
        base_delay=1.0,
        max_delay=10.0,
        exponential_base=2.0,
        jitter=True
    )
    
    # Test delay calculation
    delays = [calculate_delay(i, config) for i in range(3)]
    logger.info(f"Calculated delays: {delays}")
    
    # Verify exponential backoff
    assert delays[1] > delays[0], "Second delay should be greater than first"
    assert delays[2] > delays[1], "Third delay should be greater than second"
    
    logger.info("Retry configuration test passed!")
    return True


async def main():
    """Run all retry tests."""
    logger.info("Starting retry mechanism tests...")
    
    tests = [
        ("Retry Configuration", test_retry_configuration),
        ("Synchronous Retry", test_sync_retry),
        ("Asynchronous Retry", test_async_retry),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        logger.info(f"Running {test_name}...")
        try:
            if asyncio.iscoroutinefunction(test_func):
                result = await test_func()
            else:
                result = test_func()
            results.append((test_name, result))
        except Exception as e:
            logger.error(f"Test {test_name} failed with exception: {e}")
            results.append((test_name, False))
    
    # Print results
    logger.info("\n" + "="*50)
    logger.info("RETRY MECHANISM TEST RESULTS")
    logger.info("="*50)
    
    all_passed = True
    for test_name, passed in results:
        status = "PASSED" if passed else "FAILED"
        logger.info(f"{test_name}: {status}")
        if not passed:
            all_passed = False
    
    logger.info("="*50)
    
    if all_passed:
        logger.info("All retry mechanism tests passed! ✅")
    else:
        logger.error("Some retry mechanism tests failed! ❌")
    
    return all_passed


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
