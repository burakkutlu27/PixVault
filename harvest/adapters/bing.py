"""
Bing Image Search adapter for PixVault.
Uses Bing Image Search API to find and download images.
"""

import os
import uuid
import hashlib
from typing import List, Dict
from urllib.parse import urlparse
from ..utils.http_client import get_http_client
from .base import BaseAdapter


class BingAdapter(BaseAdapter):
    """
    Bing adapter for searching and downloading images from Bing Image Search API.
    """
    
    def __init__(self, config: Dict = None):
        """
        Initialize the Bing adapter.
        
        Args:
            config: Configuration dictionary containing 'api_key'
        """
        super().__init__(config)
        self.api_key = self.config.get('api_key')
        if not self.api_key:
            raise ValueError("Bing API key is required in config")
        
        self.base_url = "https://api.bing.microsoft.com/v7.0/images/search"
        
        # Initialize HTTP client
        self.http_client = get_http_client(self.config.get('http_client', {}))
    
    def search(self, query: str, limit: int) -> List[Dict]:
        """
        Search for images using Bing Image Search API.
        
        Args:
            query: Search query string
            limit: Maximum number of results to return
            
        Returns:
            List of dictionaries containing image metadata with keys: url, id, source, title, width, height
        """
        params = {
            "q": query,
            "count": min(limit, 150),  # Bing API max is 150 per request
            "offset": 0,
            "mkt": "en-US",
            "safeSearch": "Moderate",
            "imageType": "Photo",
            "size": "All"
        }
        
        try:
            # Prepare headers with API key
            headers = {
                "Ocp-Apim-Subscription-Key": self.api_key
            }
            
            # Use centralized HTTP client with User-Agent rotation and proxy support
            response = self.http_client.get(self.base_url, headers=headers, params=params)
            
            data = response.json()
            results = []
            
            for image in data.get("value", []):
                # Extract image metadata
                result = {
                    "url": image.get("contentUrl", ""),
                    "id": self._generate_image_id(image),
                    "source": "bing",
                    "title": image.get("name", ""),
                    "width": image.get("width"),
                    "height": image.get("height"),
                    "thumbnail": image.get("thumbnailUrl", ""),
                    "host_page": image.get("hostPageUrl", ""),
                    "content_size": image.get("contentSize", ""),
                    "encoding_format": image.get("encodingFormat", "")
                }
                
                # Only include images with valid URLs
                if result["url"]:
                    results.append(result)
            
            self.logger.info(f"Successfully retrieved {len(results)} images from Bing for query: {query}")
            return results[:limit]
            
        except Exception as e:
            self.logger.error(f"Error occurred while searching Bing: {e}")
            return []
    
    def download(self, item: Dict, output_dir: str) -> str:
        """
        Download an image from Bing search results.
        
        Args:
            item: Dictionary containing image metadata (from search results)
            output_dir: Directory to save the downloaded image
            
        Returns:
            Path to the downloaded file
        """
        url = item.get('url')
        if not url:
            raise ValueError("No URL found in item")
        
        # Generate unique filename using UUID and hash
        filename = self._generate_unique_filename(item, url)
        output_path = os.path.join(output_dir, filename)
        
        # Download the file
        if self._download_file(url, output_path):
            return output_path
        else:
            raise RuntimeError(f"Failed to download image from {url}")
    
    def _generate_image_id(self, image_data: Dict) -> str:
        """
        Generate a unique ID for the image based on its properties.
        
        Args:
            image_data: Image data from Bing API
            
        Returns:
            Unique identifier string
        """
        # Use image ID from Bing if available, otherwise generate one
        bing_id = image_data.get("imageId")
        if bing_id:
            return f"bing_{bing_id}"
        
        # Generate ID based on URL hash
        url = image_data.get("contentUrl", "")
        if url:
            url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
            return f"bing_{url_hash}"
        
        # Fallback to UUID
        return f"bing_{uuid.uuid4().hex[:12]}"
    
    def _generate_unique_filename(self, item: Dict, url: str) -> str:
        """
        Generate a unique filename for the downloaded image.
        
        Args:
            item: Image metadata dictionary
            url: Image URL
            
        Returns:
            Unique filename with proper extension
        """
        # Get file extension
        extension = self._get_file_extension(url)
        
        # Generate unique identifier
        image_id = item.get('id', '')
        if image_id:
            # Use the image ID as base
            base_name = image_id
        else:
            # Generate UUID-based name
            base_name = f"bing_{uuid.uuid4().hex[:12]}"
        
        # Add content hash for additional uniqueness
        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        filename = f"{base_name}_{url_hash}{extension}"
        
        # Sanitize filename
        return self._sanitize_filename(filename)
    
    def _get_file_extension(self, url: str, content_type: str = None) -> str:
        """
        Determine file extension from URL or content type.
        Override base method to handle Bing-specific cases.
        
        Args:
            url: Image URL
            content_type: HTTP content type header
            
        Returns:
            File extension (including the dot)
        """
        # Try to get extension from URL
        parsed_url = urlparse(url)
        path = parsed_url.path.lower()
        
        # Common image extensions
        image_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.tiff', '.svg']
        
        for ext in image_extensions:
            if path.endswith(ext):
                return ext
        
        # Try to get extension from content type
        if content_type:
            content_type = content_type.lower()
            if 'jpeg' in content_type or 'jpg' in content_type:
                return '.jpg'
            elif 'png' in content_type:
                return '.png'
            elif 'gif' in content_type:
                return '.gif'
            elif 'webp' in content_type:
                return '.webp'
            elif 'svg' in content_type:
                return '.svg'
            elif 'bmp' in content_type:
                return '.bmp'
            elif 'tiff' in content_type:
                return '.tiff'
        
        # Default to .jpg if we can't determine
        return '.jpg'
