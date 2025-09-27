#!/usr/bin/env python3
"""
Retry utility with exponential backoff for httpx requests.
"""

import time
import random
import asyncio
from typing import Callable, Any, Optional, Type, Tuple
from functools import wraps
import httpx
from .logger import get_logger

logger = get_logger("harvest.retry")


class RetryConfig:
    """Configuration for retry behavior."""
    
    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        exponential_base: float = 2.0,
        jitter: bool = True,
        retryable_exceptions: Tuple[Type[Exception], ...] = (
            httpx.TimeoutException,
            httpx.ConnectError,
            httpx.NetworkError,
            httpx.HTTPStatusError
        )
    ):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter
        self.retryable_exceptions = retryable_exceptions


def is_retryable_error(exception: Exception, config: RetryConfig) -> bool:
    """
    Check if an exception is retryable.
    
    Args:
        exception: The exception to check
        config: Retry configuration
        
    Returns:
        True if the exception is retryable, False otherwise
    """
    # Check if it's a retryable exception type
    if isinstance(exception, config.retryable_exceptions):
        return True
    
    # Check for specific HTTP status codes (5xx server errors)
    if isinstance(exception, httpx.HTTPStatusError):
        status_code = exception.response.status_code
        if 500 <= status_code < 600:
            return True
    
    return False


def calculate_delay(attempt: int, config: RetryConfig) -> float:
    """
    Calculate delay for exponential backoff with jitter.
    
    Args:
        attempt: Current attempt number (0-based)
        config: Retry configuration
        
    Returns:
        Delay in seconds
    """
    # Exponential backoff: base_delay * (exponential_base ^ attempt)
    delay = config.base_delay * (config.exponential_base ** attempt)
    
    # Cap at max_delay
    delay = min(delay, config.max_delay)
    
    # Add jitter to prevent thundering herd
    if config.jitter:
        jitter_range = delay * 0.1  # 10% jitter
        delay += random.uniform(-jitter_range, jitter_range)
    
    return max(0, delay)


def retry_with_backoff(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
    retryable_exceptions: Tuple[Type[Exception], ...] = (
        httpx.TimeoutException,
        httpx.ConnectError,
        httpx.NetworkError,
        httpx.HTTPStatusError
    )
):
    """
    Decorator for retrying functions with exponential backoff.
    
    Args:
        max_retries: Maximum number of retry attempts
        base_delay: Base delay in seconds for first retry
        max_delay: Maximum delay in seconds
        exponential_base: Base for exponential backoff calculation
        jitter: Whether to add random jitter to delays
        retryable_exceptions: Tuple of exception types that should trigger retries
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            config = RetryConfig(
                max_retries=max_retries,
                base_delay=base_delay,
                max_delay=max_delay,
                exponential_base=exponential_base,
                jitter=jitter,
                retryable_exceptions=retryable_exceptions
            )
            
            last_exception = None
            
            for attempt in range(config.max_retries + 1):
                try:
                    return func(*args, **kwargs)
                    
                except Exception as e:
                    last_exception = e
                    
                    # Check if this is the last attempt
                    if attempt >= config.max_retries:
                        logger.error(f"All {config.max_retries} retry attempts failed for {func.__name__}: {e}")
                        break
                    
                    # Check if the exception is retryable
                    if not is_retryable_error(e, config):
                        logger.warning(f"Non-retryable error in {func.__name__}: {e}")
                        break
                    
                    # Calculate delay and wait
                    delay = calculate_delay(attempt, config)
                    logger.warning(f"Attempt {attempt + 1} failed for {func.__name__}: {e}. Retrying in {delay:.2f}s...")
                    time.sleep(delay)
            
            # If we get here, all retries failed
            logger.error(f"Function {func.__name__} failed after {config.max_retries} retries. Last error: {last_exception}")
            raise last_exception
            
        return wrapper
    return decorator


class RetryableHTTPClient:
    """
    HTTP client wrapper with built-in retry logic.
    """
    
    def __init__(self, config: Optional[RetryConfig] = None, **httpx_kwargs):
        """
        Initialize retryable HTTP client.
        
        Args:
            config: Retry configuration
            **httpx_kwargs: Arguments passed to httpx.Client
        """
        self.config = config or RetryConfig()
        self.httpx_kwargs = httpx_kwargs
        self._client = None
    
    def __enter__(self):
        """Context manager entry."""
        self._client = httpx.Client(**self.httpx_kwargs)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        if self._client:
            self._client.close()
    
    def get(self, url: str, **kwargs) -> httpx.Response:
        """
        Make GET request with retry logic.
        
        Args:
            url: URL to request
            **kwargs: Additional arguments for httpx
            
        Returns:
            HTTP response
            
        Raises:
            Exception: If all retry attempts fail
        """
        last_exception = None
        
        for attempt in range(self.config.max_retries + 1):
            try:
                response = self._client.get(url, **kwargs)
                
                # Check for 5xx status codes
                if 500 <= response.status_code < 600:
                    raise httpx.HTTPStatusError(
                        f"Server error {response.status_code}",
                        request=response.request,
                        response=response
                    )
                
                return response
                
            except Exception as e:
                last_exception = e
                
                # Check if this is the last attempt
                if attempt >= self.config.max_retries:
                    logger.error(f"All {self.config.max_retries} retry attempts failed for GET {url}: {e}")
                    break
                
                # Check if the exception is retryable
                if not is_retryable_error(e, self.config):
                    logger.warning(f"Non-retryable error for GET {url}: {e}")
                    break
                
                # Calculate delay and wait
                delay = calculate_delay(attempt, self.config)
                logger.warning(f"Attempt {attempt + 1} failed for GET {url}: {e}. Retrying in {delay:.2f}s...")
                time.sleep(delay)
        
        # If we get here, all retries failed
        logger.error(f"GET request to {url} failed after {self.config.max_retries} retries. Last error: {last_exception}")
        raise last_exception
    
    def post(self, url: str, **kwargs) -> httpx.Response:
        """
        Make POST request with retry logic.
        
        Args:
            url: URL to request
            **kwargs: Additional arguments for httpx
            
        Returns:
            HTTP response
            
        Raises:
            Exception: If all retry attempts fail
        """
        last_exception = None
        
        for attempt in range(self.config.max_retries + 1):
            try:
                response = self._client.post(url, **kwargs)
                
                # Check for 5xx status codes
                if 500 <= response.status_code < 600:
                    raise httpx.HTTPStatusError(
                        f"Server error {response.status_code}",
                        request=response.request,
                        response=response
                    )
                
                return response
                
            except Exception as e:
                last_exception = e
                
                # Check if this is the last attempt
                if attempt >= self.config.max_retries:
                    logger.error(f"All {self.config.max_retries} retry attempts failed for POST {url}: {e}")
                    break
                
                # Check if the exception is retryable
                if not is_retryable_error(e, self.config):
                    logger.warning(f"Non-retryable error for POST {url}: {e}")
                    break
                
                # Calculate delay and wait
                delay = calculate_delay(attempt, self.config)
                logger.warning(f"Attempt {attempt + 1} failed for POST {url}: {e}. Retrying in {delay:.2f}s...")
                time.sleep(delay)
        
        # If we get here, all retries failed
        logger.error(f"POST request to {url} failed after {self.config.max_retries} retries. Last error: {last_exception}")
        raise last_exception


def create_retryable_client(**httpx_kwargs) -> RetryableHTTPClient:
    """
    Create a retryable HTTP client with default configuration.
    
    Args:
        **httpx_kwargs: Arguments passed to httpx.Client
        
    Returns:
        RetryableHTTPClient instance
    """
    return RetryableHTTPClient(**httpx_kwargs)


async def async_retry_with_backoff(
    func: Callable,
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
    retryable_exceptions: Tuple[Type[Exception], ...] = (
        httpx.TimeoutException,
        httpx.ConnectError,
        httpx.NetworkError,
        httpx.HTTPStatusError
    )
):
    """
    Async retry function with exponential backoff.
    
    Args:
        func: Async function to retry
        max_retries: Maximum number of retry attempts
        base_delay: Base delay in seconds for first retry
        max_delay: Maximum delay in seconds
        exponential_base: Base for exponential backoff calculation
        jitter: Whether to add random jitter to delays
        retryable_exceptions: Tuple of exception types that should trigger retries
        
    Returns:
        Result of the function call
        
    Raises:
        Exception: If all retry attempts fail
    """
    config = RetryConfig(
        max_retries=max_retries,
        base_delay=base_delay,
        max_delay=max_delay,
        exponential_base=exponential_base,
        jitter=jitter,
        retryable_exceptions=retryable_exceptions
    )
    
    last_exception = None
    
    for attempt in range(config.max_retries + 1):
        try:
            return await func()
            
        except Exception as e:
            last_exception = e
            
            # Check if this is the last attempt
            if attempt >= config.max_retries:
                logger.error(f"All {config.max_retries} retry attempts failed for async function: {e}")
                break
            
            # Check if the exception is retryable
            if not is_retryable_error(e, config):
                logger.warning(f"Non-retryable error in async function: {e}")
                break
            
            # Calculate delay and wait
            delay = calculate_delay(attempt, config)
            logger.warning(f"Attempt {attempt + 1} failed for async function: {e}. Retrying in {delay:.2f}s...")
            await asyncio.sleep(delay)
    
    # If we get here, all retries failed
    logger.error(f"Async function failed after {config.max_retries} retries. Last error: {last_exception}")
    raise last_exception
