"""
Pexels adapter for PixVault.
Uses Pexels API to find and download high-quality images.
"""

import os
import uuid
import hashlib
from typing import List, Dict
from urllib.parse import urlparse
from ..utils.http_client import get_http_client
from .base import BaseAdapter


class PexelsAdapter(BaseAdapter):
    """
    Pexels adapter for searching and downloading images from Pexels API.
    """
    
    def __init__(self, config: Dict = None):
        """
        Initialize the Pexels adapter.
        
        Args:
            config: Configuration dictionary containing 'api_key'
        """
        super().__init__(config)
        self.api_key = self.config.get('api_key')
        if not self.api_key:
            raise ValueError("Pexels API key is required in config")
        
        self.base_url = "https://api.pexels.com/v1/search"
        
        # Initialize HTTP client
        self.http_client = get_http_client(self.config.get('http_client', {}))
    
    def search(self, query: str, limit: int) -> List[Dict]:
        """
        Search for images using Pexels API.
        
        Args:
            query: Search query string
            limit: Maximum number of results to return
            
        Returns:
            List of dictionaries containing image metadata with keys: url, id, source, title, width, height
        """
        params = {
            "query": query,
            "per_page": min(limit, 80),  # Pexels API max is 80 per request
            "page": 1,
            "orientation": "all",
            "size": "all",
            "color": "all"
        }
        
        try:
            # Prepare headers with API key
            headers = {
                "Authorization": self.api_key
            }
            
            # Use centralized HTTP client with User-Agent rotation and proxy support
            response = self.http_client.get(self.base_url, headers=headers, params=params)
            
            data = response.json()
            results = []
            
            for photo in data.get("photos", []):
                # Extract image metadata
                result = {
                    "url": photo.get("src", {}).get("large2x", ""),  # High quality image
                    "id": self._generate_image_id(photo),
                    "source": "pexels",
                    "title": photo.get("alt", ""),
                    "width": photo.get("width"),
                    "height": photo.get("height"),
                    "photographer": photo.get("photographer", ""),
                    "photographer_url": photo.get("photographer_url", ""),
                    "photographer_id": photo.get("photographer_id"),
                    "avg_color": photo.get("avg_color", ""),
                    "liked": photo.get("liked", False),
                    "thumbnail": photo.get("src", {}).get("medium", ""),
                    "small": photo.get("src", {}).get("small", ""),
                    "original": photo.get("src", {}).get("original", ""),
                    "large": photo.get("src", {}).get("large", ""),
                    "portrait": photo.get("src", {}).get("portrait", ""),
                    "landscape": photo.get("src", {}).get("landscape", "")
                }
                
                # Only include images with valid URLs
                if result["url"]:
                    results.append(result)
            
            self.logger.info(f"Successfully retrieved {len(results)} images from Pexels for query: {query}")
            return results[:limit]
            
        except Exception as e:
            self.logger.error(f"Error occurred while searching Pexels: {e}")
            return []
    
    def download(self, item: Dict, output_dir: str) -> str:
        """
        Download an image from Pexels search results.
        
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
    
    def _generate_image_id(self, photo_data: Dict) -> str:
        """
        Generate a unique ID for the image based on its properties.
        
        Args:
            photo_data: Photo data from Pexels API
            
        Returns:
            Unique identifier string
        """
        # Use Pexels photo ID if available
        pexels_id = photo_data.get("id")
        if pexels_id:
            return f"pexels_{pexels_id}"
        
        # Generate ID based on URL hash
        url = photo_data.get("src", {}).get("large2x", "")
        if url:
            url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
            return f"pexels_{url_hash}"
        
        # Fallback to UUID
        return f"pexels_{uuid.uuid4().hex[:12]}"
    
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
            base_name = f"pexels_{uuid.uuid4().hex[:12]}"
        
        # Add content hash for additional uniqueness
        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        filename = f"{base_name}_{url_hash}{extension}"
        
        # Sanitize filename
        return self._sanitize_filename(filename)
    
    def _get_file_extension(self, url: str, content_type: str = None) -> str:
        """
        Determine file extension from URL or content type.
        Override base method to handle Pexels-specific cases.
        
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
        
        # Default to .jpg if we can't determine (Pexels typically serves JPEG)
        return '.jpg'
    
    def get_photographer_info(self, item: Dict) -> Dict:
        """
        Get photographer information from image metadata.
        
        Args:
            item: Image metadata dictionary
            
        Returns:
            Dictionary with photographer information
        """
        return {
            "name": item.get("photographer", ""),
            "url": item.get("photographer_url", ""),
            "id": item.get("photographer_id", "")
        }
    
    def get_image_variants(self, item: Dict) -> Dict:
        """
        Get all available image size variants.
        
        Args:
            item: Image metadata dictionary
            
        Returns:
            Dictionary with different image size URLs
        """
        return {
            "original": item.get("original", ""),
            "large2x": item.get("url", ""),  # This is what we use for download
            "large": item.get("large", ""),
            "medium": item.get("thumbnail", ""),
            "small": item.get("small", ""),
            "portrait": item.get("portrait", ""),
            "landscape": item.get("landscape", "")
        }
