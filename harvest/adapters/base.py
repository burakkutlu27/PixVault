"""
Base adapter class for PixVault image harvesting.
All adapters should extend this class to ensure consistent interface.
"""

from abc import ABC, abstractmethod
from typing import List, Dict
import os
from urllib.parse import urlparse
from ..utils.logger import get_logger
from ..utils.http_client import get_http_client

logger = get_logger("harvest.adapters.base")


class BaseAdapter(ABC):
    """
    Abstract base class for all image harvesting adapters.
    
    All adapters must implement the search and download methods
    to provide a consistent interface for image harvesting.
    """
    
    def __init__(self, config: Dict = None):
        """
        Initialize the adapter with optional configuration.
        
        Args:
            config: Configuration dictionary for the adapter
        """
        self.config = config or {}
        self.logger = get_logger(f"harvest.adapters.{self.__class__.__name__.lower()}")
        
        # Initialize HTTP client for downloads
        self.http_client = get_http_client(self.config.get('http_client', {}))
    
    @abstractmethod
    def search(self, query: str, limit: int) -> List[Dict]:
        """
        Search for images based on a query string.
        
        Args:
            query: Search query string
            limit: Maximum number of results to return
            
        Returns:
            List of dictionaries containing image metadata with keys:
            - url: Image URL
            - id: Unique identifier for the image
            - source: Source of the image (e.g., 'unsplash', 'google', 'bing')
            - title: Optional title/description
            - width: Optional image width
            - height: Optional image height
        """
        pass
    
    @abstractmethod
    def download(self, item: Dict, output_dir: str) -> str:
        """
        Download an image from the provided item metadata.
        
        Args:
            item: Dictionary containing image metadata (from search results)
            output_dir: Directory to save the downloaded image
            
        Returns:
            Path to the downloaded file
        """
        pass
    
    def _get_file_extension(self, url: str, content_type: str = None) -> str:
        """
        Determine file extension from URL or content type.
        
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
        
        # Default to .jpg if we can't determine
        return '.jpg'
    
    def _sanitize_filename(self, filename: str) -> str:
        """
        Sanitize filename for safe filesystem usage.
        
        Args:
            filename: Original filename
            
        Returns:
            Sanitized filename safe for filesystem
        """
        # Remove or replace invalid characters
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '_')
        
        # Remove multiple consecutive underscores
        while '__' in filename:
            filename = filename.replace('__', '_')
        
        # Limit length
        if len(filename) > 200:
            filename = filename[:200]
        
        return filename.strip('_')
    
    def _download_file(self, url: str, output_path: str, headers: Dict = None) -> bool:
        """
        Download a file from URL to the specified path.
        
        Args:
            url: URL to download from
            output_path: Local path to save the file
            headers: Optional HTTP headers
            
        Returns:
            True if download successful, False otherwise
        """
        try:
            # Use centralized HTTP client with User-Agent rotation and proxy support
            with self.http_client.stream('GET', url, headers=headers) as response:
                # Ensure directory exists
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                
                with open(output_path, 'wb') as f:
                    for chunk in response.iter_bytes(chunk_size=8192):
                        f.write(chunk)
                
                self.logger.info(f"Successfully downloaded: {output_path}")
                return True
                
        except Exception as e:
            self.logger.error(f"Error downloading {url}: {e}")
            return False
    
    def _generate_filename(self, item: Dict, output_dir: str) -> str:
        """
        Generate a filename for the downloaded image.
        
        Args:
            item: Image metadata dictionary
            output_dir: Output directory
            
        Returns:
            Full path for the downloaded file
        """
        # Get image ID or generate one
        image_id = item.get('id', 'unknown')
        
        # Get file extension
        url = item.get('url', '')
        content_type = item.get('content_type', '')
        extension = self._get_file_extension(url, content_type)
        
        # Create filename
        filename = f"{image_id}{extension}"
        filename = self._sanitize_filename(filename)
        
        return os.path.join(output_dir, filename)
