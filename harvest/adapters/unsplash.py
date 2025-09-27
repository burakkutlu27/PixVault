import os
from typing import List, Dict
from ..utils.http_client import get_http_client
from .base import BaseAdapter


class UnsplashAdapter(BaseAdapter):
    """
    Unsplash adapter for searching and downloading images from Unsplash API.
    """
    
    def __init__(self, config: Dict = None):
        """
        Initialize the Unsplash adapter.
        
        Args:
            config: Configuration dictionary containing 'api_key'
        """
        super().__init__(config)
        self.api_key = self.config.get('api_key')
        if not self.api_key:
            raise ValueError("Unsplash API key is required in config")
        
        # Initialize HTTP client
        self.http_client = get_http_client(self.config.get('http_client', {}))
    
    def search(self, query: str, limit: int) -> List[Dict]:
        """
        Search for images on Unsplash using the API with retry mechanism.
        
        Args:
            query: Search query string
            limit: Maximum number of results to return
            
        Returns:
            List of dictionaries containing image data with keys: url, id, source, title, width, height
        """
        url = f"https://api.unsplash.com/search/photos?query={query}&per_page={limit}"
        headers = {
            "Authorization": f"Client-ID {self.api_key}"
        }
        
        try:
            # Use centralized HTTP client with User-Agent rotation and proxy support
            response = self.http_client.get(url, headers=headers)
            
            data = response.json()
            results = []
            
            for photo in data.get("results", []):
                result = {
                    "url": photo.get("urls", {}).get("regular", ""),
                    "id": photo.get("id", ""),
                    "source": "unsplash",
                    "title": photo.get("alt_description", ""),
                    "width": photo.get("width"),
                    "height": photo.get("height")
                }
                results.append(result)
            
            self.logger.info(f"Successfully retrieved {len(results)} images from Unsplash for query: {query}")
            return results
            
        except Exception as e:
            self.logger.error(f"Error occurred while searching Unsplash: {e}")
            return []
    
    def download(self, item: Dict, output_dir: str) -> str:
        """
        Download an image from Unsplash.
        
        Args:
            item: Dictionary containing image metadata (from search results)
            output_dir: Directory to save the downloaded image
            
        Returns:
            Path to the downloaded file
        """
        url = item.get('url')
        if not url:
            raise ValueError("No URL found in item")
        
        # Generate output path
        output_path = self._generate_filename(item, output_dir)
        
        # Download the file
        if self._download_file(url, output_path):
            return output_path
        else:
            raise RuntimeError(f"Failed to download image from {url}")


# Legacy function for backward compatibility
def search_unsplash(query: str, per_page: int, api_key: str) -> List[Dict[str, str]]:
    """
    Legacy function for backward compatibility.
    Search for images on Unsplash using the API with retry mechanism.
    
    Args:
        query: Search query string
        per_page: Number of results per page
        api_key: Unsplash API key
        
    Returns:
        List of dictionaries containing image data with keys: url, id, source
    """
    adapter = UnsplashAdapter({'api_key': api_key})
    results = adapter.search(query, per_page)
    
    # Convert to legacy format
    legacy_results = []
    for result in results:
        legacy_results.append({
            "url": result.get("url", ""),
            "id": result.get("id", ""),
            "source": result.get("source", "unsplash")
        })
    
    return legacy_results