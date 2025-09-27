"""
Proxy-enabled browser adapter for PixVault.
Extends the base browser adapter with proxy support and rotation.
"""

import asyncio
import random
import time
from typing import List, Dict, Optional
from urllib.parse import urlparse
from playwright.async_api import async_playwright, Browser, Page, TimeoutError as PlaywrightTimeoutError

from .browser import BrowserAdapter
from ..proxy_manager import get_proxy_manager, get_proxy, mark_bad, mark_success


class ProxyBrowserAdapter(BrowserAdapter):
    """
    Proxy-enabled browser adapter for extracting image URLs from JavaScript-rendered sites.
    Uses Playwright with proxy rotation and human-like behavior simulation.
    """
    
    def __init__(self, config: Dict = None):
        super().__init__(config)
        self.proxy_manager = get_proxy_manager(self.config.get('proxy', {}))
        self.current_proxy = None
    
    async def __aenter__(self):
        """Async context manager entry with proxy support."""
        self.playwright = await async_playwright().start()
        
        # Get proxy for browser
        proxy_config = self._get_proxy_config()
        
        browser_args = [
            '--no-sandbox',
            '--disable-dev-shm-usage',
            '--disable-blink-features=AutomationControlled',
            '--disable-web-security',
            '--disable-features=VizDisplayCompositor'
        ]
        
        # Add proxy configuration if available
        if proxy_config:
            proxy_server = proxy_config.get('server')
            if proxy_server:
                browser_args.append(f'--proxy-server={proxy_server}')
                self.logger.info(f"Using proxy for browser: {proxy_server}")
        
        self.browser = await self.playwright.chromium.launch(
            headless=True,
            args=browser_args
        )
        
        return self
    
    def _get_proxy_config(self) -> Optional[Dict]:
        """Get proxy configuration for browser."""
        try:
            proxy_dict = self.proxy_manager.get_proxy()
            if not proxy_dict:
                return None
            
            # Convert proxy dictionary to browser format
            proxy_url = proxy_dict.get('http') or proxy_dict.get('https')
            if not proxy_url:
                return None
            
            # Extract proxy info
            parsed = urlparse(proxy_url)
            server = f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"
            
            proxy_config = {
                'server': server,
                'username': parsed.username,
                'password': parsed.password
            }
            
            self.current_proxy = proxy_dict
            return proxy_config
            
        except Exception as e:
            self.logger.error(f"Error getting proxy config: {e}")
            return None
    
    async def _handle_proxy_error(self, error: Exception) -> None:
        """Handle proxy-related errors."""
        if self.current_proxy:
            self.logger.warning(f"Proxy error, marking proxy as bad: {error}")
            mark_bad(self.current_proxy)
            self.current_proxy = None
    
    async def _handle_proxy_success(self) -> None:
        """Handle successful proxy usage."""
        if self.current_proxy:
            mark_success(self.current_proxy)
    
    async def fetch_images(self, query: str, max_results: int = 50) -> List[Dict[str, str]]:
        """
        Fetch images from web search results using proxy rotation.
        
        Args:
            query: Search query string
            max_results: Maximum number of results to return
            
        Returns:
            List of dictionaries with keys: url, id, source
        """
        if not self.browser:
            raise RuntimeError("Browser not initialized. Use async context manager.")
        
        all_results = []
        
        try:
            # Create a new page with realistic user agent
            page = await self.browser.new_page()
            
            # Set realistic user agent and viewport
            await page.set_extra_http_headers({
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            })
            
            await page.set_viewport_size({'width': 1920, 'height': 1080})
            
            # Try multiple search engines
            search_engines = [
                {
                    'name': 'google',
                    'url': f'https://www.google.com/search?q={query}&tbm=isch&safe=off',
                    'domain': 'google.com'
                },
                {
                    'name': 'bing',
                    'url': f'https://www.bing.com/images/search?q={query}&form=HDRSC2',
                    'domain': 'bing.com'
                }
            ]
            
            for search_engine in search_engines:
                if len(all_results) >= max_results:
                    break
                
                try:
                    # Enforce rate limiting
                    await self._enforce_rate_limit(search_engine['domain'])
                    
                    self.logger.info(f"Searching {search_engine['name']} for: {query} (using proxy: {bool(self.current_proxy)})")
                    
                    # Navigate to search results
                    await page.goto(search_engine['url'], wait_until='networkidle', timeout=30000)
                    
                    # Check for captcha or login requirements
                    if await self._check_for_captcha_or_login(page):
                        self.logger.warning(f"Captcha or login required on {search_engine['name']}, skipping")
                        continue
                    
                    # Simulate human behavior
                    await self._simulate_human_behavior(page)
                    
                    # Extract images from the page
                    page_results = await self._extract_images_from_page(page, search_engine['name'])
                    
                    # Add results up to max_results
                    remaining = max_results - len(all_results)
                    all_results.extend(page_results[:remaining])
                    
                    self.logger.info(f"Found {len(page_results)} images from {search_engine['name']}")
                    
                    # Mark proxy as successful
                    await self._handle_proxy_success()
                    
                    # Random delay between search engines
                    await asyncio.sleep(random.uniform(2, 5))
                    
                except PlaywrightTimeoutError:
                    self.logger.warning(f"Timeout loading {search_engine['name']}")
                    await self._handle_proxy_error(PlaywrightTimeoutError("Timeout"))
                    continue
                except Exception as e:
                    self.logger.error(f"Error searching {search_engine['name']}: {e}")
                    await self._handle_proxy_error(e)
                    continue
            
            await page.close()
            
            # Remove duplicates based on URL
            seen_urls = set()
            unique_results = []
            for result in all_results:
                if result['url'] not in seen_urls:
                    seen_urls.add(result['url'])
                    unique_results.append(result)
            
            self.logger.info(f"Total unique images found: {len(unique_results)}")
            return unique_results[:max_results]
            
        except Exception as e:
            self.logger.error(f"Error in fetch_images: {e}")
            await self._handle_proxy_error(e)
            return []


# Convenience functions for compatibility
async def fetch_images_with_proxy(query: str, max_results: int = 50, config: Dict = None) -> List[Dict[str, str]]:
    """
    Convenience function to fetch images using the proxy browser adapter.
    
    Args:
        query: Search query string
        max_results: Maximum number of results to return
        config: Configuration dictionary
        
    Returns:
        List of dictionaries with keys: url, id, source
    """
    async with ProxyBrowserAdapter(config) as adapter:
        return await adapter.fetch_images(query, max_results)


def fetch_images_with_proxy_sync(query: str, max_results: int = 50, config: Dict = None) -> List[Dict[str, str]]:
    """
    Synchronous wrapper for fetch_images_with_proxy function.
    
    Args:
        query: Search query string
        max_results: Maximum number of results to return
        config: Configuration dictionary
        
    Returns:
        List of dictionaries with keys: url, id, source
    """
    return asyncio.run(fetch_images_with_proxy(query, max_results, config))
