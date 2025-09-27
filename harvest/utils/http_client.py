"""
Centralized HTTP client for PixVault.
Provides User-Agent rotation and proxy support for all adapters.
"""

import random
import time
from typing import Dict, List, Optional, Any
import httpx
from urllib.parse import urlparse
from .logger import get_logger

logger = get_logger("harvest.utils.http_client")


class HTTPClient:
    """
    Centralized HTTP client with User-Agent rotation and proxy support.
    """
    
    def __init__(self, config: Dict = None):
        """
        Initialize the HTTP client.
        
        Args:
            config: Configuration dictionary containing http_client settings
        """
        self.config = config or {}
        self.user_agents = self.config.get('user_agents', [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ])
        self.proxies = self.config.get('proxies', [])
        self.timeout = self.config.get('timeout', 30)
        self.max_retries = self.config.get('max_retries', 3)
        self.retry_delay = self.config.get('retry_delay', 1.0)
        
        # Initialize proxy rotation
        self._proxy_index = 0
        self._last_proxy_use = {}
        
        logger.info(f"HTTP Client initialized with {len(self.user_agents)} User-Agents and {len(self.proxies)} proxies")
    
    def get_random_user_agent(self) -> str:
        """
        Get a random User-Agent string.
        
        Returns:
            Random User-Agent string
        """
        return random.choice(self.user_agents)
    
    def get_random_proxy(self) -> Optional[Dict[str, str]]:
        """
        Get a random proxy configuration.
        
        Returns:
            Proxy configuration dictionary or None if no proxies available
        """
        if not self.proxies:
            return None
        
        # Simple round-robin proxy selection
        proxy = self.proxies[self._proxy_index % len(self.proxies)]
        self._proxy_index += 1
        
        return proxy
    
    def _build_proxy_url(self, proxy_config: Dict[str, str]) -> str:
        """
        Build proxy URL from configuration.
        
        Args:
            proxy_config: Proxy configuration dictionary
            
        Returns:
            Proxy URL string
        """
        protocol = proxy_config.get('protocol', 'http')
        host = proxy_config.get('host')
        port = proxy_config.get('port')
        username = proxy_config.get('username')
        password = proxy_config.get('password')
        
        if not host or not port:
            return None
        
        # Build proxy URL
        if username and password:
            return f"{protocol}://{username}:{password}@{host}:{port}"
        else:
            return f"{protocol}://{host}:{port}"
    
    def _get_proxy_for_request(self) -> Optional[str]:
        """
        Get proxy URL for the current request.
        
        Returns:
            Proxy URL string or None
        """
        proxy_config = self.get_random_proxy()
        if not proxy_config:
            return None
        
        return self._build_proxy_url(proxy_config)
    
    def _get_headers_for_request(self, additional_headers: Dict[str, str] = None) -> Dict[str, str]:
        """
        Get headers for the current request with random User-Agent.
        
        Args:
            additional_headers: Additional headers to include
            
        Returns:
            Headers dictionary
        """
        headers = {
            'User-Agent': self.get_random_user_agent(),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        
        if additional_headers:
            headers.update(additional_headers)
        
        return headers
    
    def get(self, url: str, headers: Dict[str, str] = None, **kwargs) -> httpx.Response:
        """
        Make a GET request with User-Agent rotation and proxy support.
        
        Args:
            url: Request URL
            headers: Additional headers
            **kwargs: Additional httpx parameters
            
        Returns:
            httpx Response object
        """
        return self._make_request('GET', url, headers=headers, **kwargs)
    
    def post(self, url: str, headers: Dict[str, str] = None, **kwargs) -> httpx.Response:
        """
        Make a POST request with User-Agent rotation and proxy support.
        
        Args:
            url: Request URL
            headers: Additional headers
            **kwargs: Additional httpx parameters
            
        Returns:
            httpx Response object
        """
        return self._make_request('POST', url, headers=headers, **kwargs)
    
    def _make_request(self, method: str, url: str, headers: Dict[str, str] = None, **kwargs) -> httpx.Response:
        """
        Make an HTTP request with retry logic, User-Agent rotation, and proxy support.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            url: Request URL
            headers: Additional headers
            **kwargs: Additional httpx parameters
            
        Returns:
            httpx Response object
        """
        # Get request headers with random User-Agent
        request_headers = self._get_headers_for_request(headers)
        
        # Get proxy for this request
        proxy_url = self._get_proxy_for_request()
        
        # Prepare httpx parameters
        httpx_kwargs = {
            'timeout': self.timeout,
            'headers': request_headers,
            **kwargs
        }
        
        if proxy_url:
            httpx_kwargs['proxies'] = proxy_url
            logger.debug(f"Using proxy: {proxy_url}")
        
        # Retry logic
        last_exception = None
        for attempt in range(self.max_retries):
            try:
                with httpx.Client() as client:
                    response = client.request(method, url, **httpx_kwargs)
                    response.raise_for_status()
                    
                    logger.debug(f"Request successful: {method} {url} (attempt {attempt + 1})")
                    return response
                    
            except httpx.HTTPError as e:
                last_exception = e
                logger.warning(f"HTTP error on attempt {attempt + 1}: {e}")
                
                if attempt < self.max_retries - 1:
                    delay = self.retry_delay * (2 ** attempt)  # Exponential backoff
                    logger.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    logger.error(f"All retry attempts failed for {method} {url}")
                    
            except Exception as e:
                last_exception = e
                logger.error(f"Unexpected error on attempt {attempt + 1}: {e}")
                
                if attempt < self.max_retries - 1:
                    delay = self.retry_delay * (2 ** attempt)
                    logger.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    logger.error(f"All retry attempts failed for {method} {url}")
        
        # If we get here, all retries failed
        raise last_exception or Exception(f"Failed to make request after {self.max_retries} attempts")
    
    def stream(self, method: str, url: str, headers: Dict[str, str] = None, **kwargs) -> httpx.Response:
        """
        Make a streaming request with User-Agent rotation and proxy support.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            url: Request URL
            headers: Additional headers
            **kwargs: Additional httpx parameters
            
        Returns:
            httpx Response object for streaming
        """
        # Get request headers with random User-Agent
        request_headers = self._get_headers_for_request(headers)
        
        # Get proxy for this request
        proxy_url = self._get_proxy_for_request()
        
        # Prepare httpx parameters
        httpx_kwargs = {
            'timeout': self.timeout,
            'headers': request_headers,
            **kwargs
        }
        
        if proxy_url:
            httpx_kwargs['proxies'] = proxy_url
            logger.debug(f"Using proxy for stream: {proxy_url}")
        
        with httpx.stream(method, url, **httpx_kwargs) as response:
            response.raise_for_status()
            return response
    
    def get_client(self) -> httpx.Client:
        """
        Get a configured httpx client instance.
        
        Returns:
            httpx Client instance
        """
        # Get proxy for this client
        proxy_url = self._get_proxy_for_request()
        
        client_kwargs = {
            'timeout': self.timeout,
            'headers': self._get_headers_for_request()
        }
        
        if proxy_url:
            client_kwargs['proxies'] = proxy_url
            logger.debug(f"Client using proxy: {proxy_url}")
        
        return httpx.Client(**client_kwargs)


# Global HTTP client instance
_http_client: Optional[HTTPClient] = None


def get_http_client(config: Dict = None) -> HTTPClient:
    """
    Get the global HTTP client instance.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        HTTPClient instance
    """
    global _http_client
    
    if _http_client is None or config is not None:
        _http_client = HTTPClient(config)
    
    return _http_client


def reset_http_client():
    """Reset the global HTTP client instance."""
    global _http_client
    _http_client = None
