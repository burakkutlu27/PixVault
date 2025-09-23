import asyncio
import random
import time
import re
from typing import List, Dict, Optional
from urllib.parse import urlparse
from playwright.async_api import async_playwright, Browser, Page, TimeoutError as PlaywrightTimeoutError
import logging

logger = logging.getLogger(__name__)


class BrowserAdapter:
    """
    Browser adapter for extracting image URLs from JavaScript-rendered sites.
    Uses Playwright with human-like behavior simulation.
    """
    
    def __init__(self):
        self.browser: Optional[Browser] = None
        self.playwright = None
        self.domain_delays = {}  # Track last request time per domain
        self.rate_limit_min = 5  # Minimum delay in seconds
        self.rate_limit_max = 15  # Maximum delay in seconds
        
    async def __aenter__(self):
        """Async context manager entry."""
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--disable-blink-features=AutomationControlled',
                '--disable-web-security',
                '--disable-features=VizDisplayCompositor'
            ]
        )
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
    
    async def _simulate_human_behavior(self, page: Page) -> None:
        """Simulate human-like mouse movements and scrolling."""
        try:
            # Random scroll behavior
            scroll_count = random.randint(2, 5)
            for _ in range(scroll_count):
                scroll_distance = random.randint(200, 800)
                await page.mouse.wheel(0, scroll_distance)
                await asyncio.sleep(random.uniform(0.5, 1.5))
            
            # Random mouse movements
            viewport = page.viewport_size
            if viewport:
                for _ in range(random.randint(3, 8)):
                    x = random.randint(0, viewport['width'])
                    y = random.randint(0, viewport['height'])
                    await page.mouse.move(x, y)
                    await asyncio.sleep(random.uniform(0.1, 0.3))
                    
        except Exception as e:
            logger.warning(f"Human behavior simulation failed: {e}")
    
    async def _check_for_captcha_or_login(self, page: Page) -> bool:
        """Check if page requires captcha or login."""
        try:
            # Common captcha indicators
            captcha_selectors = [
                '[class*="captcha"]',
                '[id*="captcha"]',
                '[class*="recaptcha"]',
                '[id*="recaptcha"]',
                'iframe[src*="recaptcha"]',
                '[class*="hcaptcha"]',
                '[id*="hcaptcha"]'
            ]
            
            # Common login indicators
            login_selectors = [
                'input[type="password"]',
                '[class*="login"]',
                '[id*="login"]',
                '[class*="signin"]',
                '[id*="signin"]',
                'button:has-text("Login")',
                'button:has-text("Sign In")',
                'a:has-text("Login")',
                'a:has-text("Sign In")'
            ]
            
            # Check for captcha
            for selector in captcha_selectors:
                if await page.locator(selector).count() > 0:
                    logger.warning("Captcha detected on page")
                    return True
            
            # Check for login requirements
            for selector in login_selectors:
                if await page.locator(selector).count() > 0:
                    logger.warning("Login required on page")
                    return True
                    
            return False
            
        except Exception as e:
            logger.warning(f"Error checking for captcha/login: {e}")
            return False
    
    async def _enforce_rate_limit(self, domain: str) -> None:
        """Enforce rate limiting per domain."""
        current_time = time.time()
        
        if domain in self.domain_delays:
            time_since_last = current_time - self.domain_delays[domain]
            required_delay = random.uniform(self.rate_limit_min, self.rate_limit_max)
            
            if time_since_last < required_delay:
                sleep_time = required_delay - time_since_last
                logger.info(f"Rate limiting: waiting {sleep_time:.2f}s for domain {domain}")
                await asyncio.sleep(sleep_time)
        
        self.domain_delays[domain] = time.time()
    
    async def _extract_images_from_page(self, page: Page, source: str) -> List[Dict[str, str]]:
        """Extract image URLs from the current page."""
        try:
            # Wait for images to load
            await page.wait_for_load_state('networkidle', timeout=10000)
            
            # Extract image URLs using various selectors
            image_data = await page.evaluate("""
                () => {
                    const images = [];
                    const imgElements = document.querySelectorAll('img');
                    
                    imgElements.forEach((img, index) => {
                        const src = img.src || img.getAttribute('data-src') || img.getAttribute('data-lazy-src');
                        if (src && src.startsWith('http')) {
                            images.push({
                                url: src,
                                id: `img_${index}_${Date.now()}`,
                                alt: img.alt || '',
                                width: img.naturalWidth || img.width,
                                height: img.naturalHeight || img.height
                            });
                        }
                    });
                    
                    return images;
                }
            """)
            
            results = []
            for img in image_data:
                # Filter out very small images (likely icons/buttons)
                if img.get('width', 0) > 50 and img.get('height', 0) > 50:
                    results.append({
                        'url': img['url'],
                        'id': img['id'],
                        'source': source
                    })
            
            return results
            
        except Exception as e:
            logger.error(f"Error extracting images: {e}")
            return []
    
    async def fetch_images(self, query: str, max_results: int = 50) -> List[Dict[str, str]]:
        """
        Fetch images from web search results.
        
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
                    
                    logger.info(f"Searching {search_engine['name']} for: {query}")
                    
                    # Navigate to search results
                    await page.goto(search_engine['url'], wait_until='networkidle', timeout=30000)
                    
                    # Check for captcha or login requirements
                    if await self._check_for_captcha_or_login(page):
                        logger.warning(f"Captcha or login required on {search_engine['name']}, skipping")
                        continue
                    
                    # Simulate human behavior
                    await self._simulate_human_behavior(page)
                    
                    # Extract images from the page
                    page_results = await self._extract_images_from_page(page, search_engine['name'])
                    
                    # Add results up to max_results
                    remaining = max_results - len(all_results)
                    all_results.extend(page_results[:remaining])
                    
                    logger.info(f"Found {len(page_results)} images from {search_engine['name']}")
                    
                    # Random delay between search engines
                    await asyncio.sleep(random.uniform(2, 5))
                    
                except PlaywrightTimeoutError:
                    logger.warning(f"Timeout loading {search_engine['name']}")
                    continue
                except Exception as e:
                    logger.error(f"Error searching {search_engine['name']}: {e}")
                    continue
            
            await page.close()
            
            # Remove duplicates based on URL
            seen_urls = set()
            unique_results = []
            for result in all_results:
                if result['url'] not in seen_urls:
                    seen_urls.add(result['url'])
                    unique_results.append(result)
            
            logger.info(f"Total unique images found: {len(unique_results)}")
            return unique_results[:max_results]
            
        except Exception as e:
            logger.error(f"Error in fetch_images: {e}")
            return []


async def fetch_images(query: str, max_results: int = 50) -> List[Dict[str, str]]:
    """
    Convenience function to fetch images using the browser adapter.
    
    Args:
        query: Search query string
        max_results: Maximum number of results to return
        
    Returns:
        List of dictionaries with keys: url, id, source
    """
    async with BrowserAdapter() as adapter:
        return await adapter.fetch_images(query, max_results)


# Synchronous wrapper for compatibility with existing code
def fetch_images_sync(query: str, max_results: int = 50) -> List[Dict[str, str]]:
    """
    Synchronous wrapper for fetch_images function.
    
    Args:
        query: Search query string
        max_results: Maximum number of results to return
        
    Returns:
        List of dictionaries with keys: url, id, source
    """
    return asyncio.run(fetch_images(query, max_results))
