"""
Adapter manager for PixVault.
Manages multiple image source adapters and provides a unified interface.
"""

from typing import Dict, List, Optional, Any
from ..utils.logger import get_logger
from .base import BaseAdapter

# Import all adapters
from .unsplash import UnsplashAdapter
from .bing import BingAdapter
from .pexels import PexelsAdapter

logger = get_logger("harvest.adapters.manager")


class AdapterManager:
    """
    Manages multiple image source adapters.
    Provides a unified interface for searching and downloading from multiple sources.
    """
    
    def __init__(self, config: Dict = None):
        """
        Initialize the adapter manager.
        
        Args:
            config: Configuration dictionary containing API keys and settings
        """
        self.config = config or {}
        self.adapters: Dict[str, BaseAdapter] = {}
        self.sources = self.config.get('sources', ['unsplash'])
        
        # Initialize adapters
        self._initialize_adapters()
    
    def _initialize_adapters(self):
        """Initialize all configured adapters."""
        for source in self.sources:
            try:
                adapter = self._create_adapter(source)
                if adapter:
                    self.adapters[source] = adapter
                    logger.info(f"Initialized {source} adapter")
                else:
                    logger.warning(f"Failed to initialize {source} adapter")
            except Exception as e:
                logger.error(f"Error initializing {source} adapter: {e}")
    
    def _create_adapter(self, source: str) -> Optional[BaseAdapter]:
        """
        Create an adapter instance for the specified source.
        
        Args:
            source: Source name (unsplash, bing, pexels)
            
        Returns:
            Adapter instance or None if creation fails
        """
        try:
            # Prepare adapter config with HTTP client settings
            adapter_config = {
                'http_client': self.config.get('http_client', {})
            }
            
            if source == 'unsplash':
                api_config = self.config.get('apis', {}).get('unsplash', {})
                if not api_config.get('access_key'):
                    logger.warning("Unsplash access key not configured")
                    return None
                adapter_config['api_key'] = api_config['access_key']
                return UnsplashAdapter(adapter_config)
            
            elif source == 'bing':
                api_config = self.config.get('apis', {}).get('bing', {})
                if not api_config.get('api_key'):
                    logger.warning("Bing API key not configured")
                    return None
                adapter_config['api_key'] = api_config['api_key']
                return BingAdapter(adapter_config)
            
            elif source == 'pexels':
                api_config = self.config.get('apis', {}).get('pexels', {})
                if not api_config.get('api_key'):
                    logger.warning("Pexels API key not configured")
                    return None
                adapter_config['api_key'] = api_config['api_key']
                return PexelsAdapter(adapter_config)
            
            else:
                logger.warning(f"Unknown source: {source}")
                return None
                
        except Exception as e:
            logger.error(f"Error creating {source} adapter: {e}")
            return None
    
    def get_adapter(self, name: str) -> Optional[BaseAdapter]:
        """
        Get an adapter by name.
        
        Args:
            name: Adapter name (unsplash, bing, pexels)
            
        Returns:
            Adapter instance or None if not found
        """
        return self.adapters.get(name)
    
    def get_available_adapters(self) -> List[str]:
        """
        Get list of available (initialized) adapters.
        
        Returns:
            List of adapter names
        """
        return list(self.adapters.keys())
    
    def search_all(self, query: str, limit_per_source: int = 10) -> Dict[str, List[Dict]]:
        """
        Search all available adapters for images.
        
        Args:
            query: Search query string
            limit_per_source: Maximum results per source
            
        Returns:
            Dictionary mapping source names to their search results
        """
        results = {}
        
        for source, adapter in self.adapters.items():
            try:
                logger.info(f"Searching {source} for: {query}")
                source_results = adapter.search(query, limit_per_source)
                results[source] = source_results
                logger.info(f"Found {len(source_results)} images from {source}")
                
            except Exception as e:
                logger.error(f"Error searching {source}: {e}")
                results[source] = []
        
        return results
    
    def download_from_all(self, query: str, output_dir: str, limit_per_source: int = 5) -> Dict[str, List[str]]:
        """
        Search and download from all available sources.
        
        Args:
            query: Search query string
            output_dir: Directory to save downloaded images
            limit_per_source: Maximum downloads per source
            
        Returns:
            Dictionary mapping source names to lists of downloaded file paths
        """
        downloaded_files = {}
        
        # Search all sources
        search_results = self.search_all(query, limit_per_source)
        
        # Download from each source
        for source, results in search_results.items():
            downloaded_files[source] = []
            
            if not results:
                continue
            
            adapter = self.adapters.get(source)
            if not adapter:
                continue
            
            logger.info(f"Downloading {len(results)} images from {source}")
            
            for item in results:
                try:
                    file_path = adapter.download(item, output_dir)
                    downloaded_files[source].append(file_path)
                    logger.info(f"Downloaded: {file_path}")
                    
                except Exception as e:
                    logger.error(f"Error downloading from {source}: {e}")
        
        return downloaded_files
    
    def get_total_results(self, query: str, limit_per_source: int = 10) -> int:
        """
        Get total number of results available across all sources.
        
        Args:
            query: Search query string
            limit_per_source: Maximum results per source
            
        Returns:
            Total number of results
        """
        results = self.search_all(query, limit_per_source)
        return sum(len(source_results) for source_results in results.values())
    
    def get_source_stats(self, query: str, limit_per_source: int = 10) -> Dict[str, Any]:
        """
        Get statistics about search results from each source.
        
        Args:
            query: Search query string
            limit_per_source: Maximum results per source
            
        Returns:
            Dictionary with statistics for each source
        """
        results = self.search_all(query, limit_per_source)
        stats = {}
        
        for source, source_results in results.items():
            stats[source] = {
                'count': len(source_results),
                'has_results': len(source_results) > 0,
                'sources': [item.get('source', source) for item in source_results],
                'dimensions': [(item.get('width'), item.get('height')) for item in source_results if item.get('width') and item.get('height')]
            }
        
        return stats


def get_adapter(name: str, config: Dict = None) -> Optional[BaseAdapter]:
    """
    Convenience function to get an adapter by name.
    
    Args:
        name: Adapter name (unsplash, bing, pexels)
        config: Configuration dictionary
        
    Returns:
        Adapter instance or None if not found
    """
    manager = AdapterManager(config)
    return manager.get_adapter(name)


def get_all_adapters(config: Dict = None) -> AdapterManager:
    """
    Convenience function to get an adapter manager with all configured adapters.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        AdapterManager instance
    """
    return AdapterManager(config)
