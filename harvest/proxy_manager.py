"""
Proxy management module for PixVault.
Provides proxy rotation, health checking, and integration with downloader and browser adapter.
"""

import random
import time
import asyncio
import httpx
from typing import Dict, List, Optional, Any, Tuple
from urllib.parse import urlparse
import logging
from .utils.retry import create_retryable_client, async_retry_with_backoff
from dataclasses import dataclass
from pathlib import Path
import json

from .utils.logger import get_logger

logger = get_logger("harvest.proxy_manager")


@dataclass
class ProxyInfo:
    """Proxy information container."""
    host: str
    port: int
    username: Optional[str] = None
    password: Optional[str] = None
    protocol: str = 'http'  # http, https, socks4, socks5
    country: Optional[str] = None
    provider: Optional[str] = None
    last_used: float = 0.0
    success_count: int = 0
    failure_count: int = 0
    is_active: bool = True
    last_checked: float = 0.0
    response_time: float = 0.0


class ProxyManager:
    """
    Proxy management with rotation, health checking, and integration.
    Manages a pool of proxies with automatic rotation and health monitoring.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize proxy manager.
        
        Args:
            config: Configuration dictionary with proxy settings
        """
        self.config = config or {}
        self.proxies: List[ProxyInfo] = []
        self.bad_proxies: set = set()
        self.current_index = 0
        self.health_check_interval = self.config.get('health_check_interval', 300)  # 5 minutes
        self.max_failures = self.config.get('max_failures', 3)
        self.health_check_timeout = self.config.get('health_check_timeout', 10)
        self.rotation_strategy = self.config.get('rotation_strategy', 'round_robin')  # round_robin, random, weighted
        
        # Load proxies from configuration
        self._load_proxies()
        
        logger.info(f"Proxy manager initialized with {len(self.proxies)} proxies")
    
    def _load_proxies(self):
        """Load proxies from configuration."""
        proxy_list = self.config.get('proxies', [])
        
        for proxy_config in proxy_list:
            try:
                proxy = ProxyInfo(
                    host=proxy_config['host'],
                    port=proxy_config['port'],
                    username=proxy_config.get('username'),
                    password=proxy_config.get('password'),
                    protocol=proxy_config.get('protocol', 'http'),
                    country=proxy_config.get('country'),
                    provider=proxy_config.get('provider')
                )
                self.proxies.append(proxy)
            except KeyError as e:
                logger.error(f"Invalid proxy configuration: missing {e}")
                continue
        
        logger.info(f"Loaded {len(self.proxies)} proxies from configuration")
    
    def get_proxy(self) -> Optional[Dict[str, Any]]:
        """
        Get next available proxy for use.
        
        Returns:
            Proxy dictionary for use with httpx/requests or None if no proxies available
        """
        if not self.proxies:
            logger.warning("No proxies available")
            return None
        
        # Filter out bad proxies
        available_proxies = [p for p in self.proxies if p.is_active and p not in self.bad_proxies]
        
        if not available_proxies:
            logger.warning("No active proxies available")
            return None
        
        # Select proxy based on rotation strategy
        if self.rotation_strategy == 'round_robin':
            proxy = self._get_round_robin_proxy(available_proxies)
        elif self.rotation_strategy == 'random':
            proxy = self._get_random_proxy(available_proxies)
        elif self.rotation_strategy == 'weighted':
            proxy = self._get_weighted_proxy(available_proxies)
        else:
            proxy = available_proxies[0]
        
        # Update last used time
        proxy.last_used = time.time()
        
        # Convert to dictionary format
        proxy_dict = self._proxy_to_dict(proxy)
        
        logger.debug(f"Selected proxy: {proxy.host}:{proxy.port}")
        return proxy_dict
    
    def _get_round_robin_proxy(self, available_proxies: List[ProxyInfo]) -> ProxyInfo:
        """Get proxy using round-robin strategy."""
        proxy = available_proxies[self.current_index % len(available_proxies)]
        self.current_index = (self.current_index + 1) % len(available_proxies)
        return proxy
    
    def _get_random_proxy(self, available_proxies: List[ProxyInfo]) -> ProxyInfo:
        """Get proxy using random selection."""
        return random.choice(available_proxies)
    
    def _get_weighted_proxy(self, available_proxies: List[ProxyInfo]) -> ProxyInfo:
        """Get proxy using weighted selection based on success rate."""
        if not available_proxies:
            return None
        
        # Calculate weights based on success rate
        weights = []
        for proxy in available_proxies:
            total_requests = proxy.success_count + proxy.failure_count
            if total_requests == 0:
                weight = 1.0  # Default weight for unused proxies
            else:
                weight = proxy.success_count / total_requests
            weights.append(weight)
        
        # Select proxy based on weights
        return random.choices(available_proxies, weights=weights)[0]
    
    def _proxy_to_dict(self, proxy: ProxyInfo) -> Dict[str, Any]:
        """Convert ProxyInfo to dictionary format for httpx/requests."""
        proxy_dict = {
            'http': f"{proxy.protocol}://{proxy.host}:{proxy.port}",
            'https': f"{proxy.protocol}://{proxy.host}:{proxy.port}"
        }
        
        # Add authentication if provided
        if proxy.username and proxy.password:
            auth = f"{proxy.username}:{proxy.password}@"
            proxy_dict['http'] = f"{proxy.protocol}://{auth}{proxy.host}:{proxy.port}"
            proxy_dict['https'] = f"{proxy.protocol}://{auth}{proxy.host}:{proxy.port}"
        
        return proxy_dict
    
    def mark_bad(self, proxy: Dict[str, Any]) -> None:
        """
        Mark a proxy as bad and remove from rotation.
        
        Args:
            proxy: Proxy dictionary that failed
        """
        # Find the proxy in our list
        proxy_info = self._find_proxy_by_dict(proxy)
        
        if proxy_info:
            proxy_info.failure_count += 1
            
            # Mark as bad if failure count exceeds threshold
            if proxy_info.failure_count >= self.max_failures:
                proxy_info.is_active = False
                self.bad_proxies.add(proxy_info)
                logger.warning(f"Marked proxy {proxy_info.host}:{proxy_info.port} as bad")
            else:
                logger.info(f"Proxy {proxy_info.host}:{proxy_info.port} failure count: {proxy_info.failure_count}")
        else:
            logger.warning("Could not find proxy to mark as bad")
    
    def mark_success(self, proxy: Dict[str, Any]) -> None:
        """
        Mark a proxy as successful.
        
        Args:
            proxy: Proxy dictionary that succeeded
        """
        proxy_info = self._find_proxy_by_dict(proxy)
        
        if proxy_info:
            proxy_info.success_count += 1
            logger.debug(f"Proxy {proxy_info.host}:{proxy_info.port} success count: {proxy_info.success_count}")
    
    def _find_proxy_by_dict(self, proxy_dict: Dict[str, Any]) -> Optional[ProxyInfo]:
        """Find ProxyInfo object by proxy dictionary."""
        if not proxy_dict:
            return None
        
        # Extract host and port from proxy dictionary
        proxy_url = proxy_dict.get('http') or proxy_dict.get('https')
        if not proxy_url:
            return None
        
        try:
            parsed = urlparse(proxy_url)
            host = parsed.hostname
            port = parsed.port
            
            # Find matching proxy
            for proxy_info in self.proxies:
                if proxy_info.host == host and proxy_info.port == port:
                    return proxy_info
        except Exception as e:
            logger.error(f"Error parsing proxy URL: {e}")
        
        return None
    
    async def health_check_proxy(self, proxy: ProxyInfo) -> bool:
        """
        Check if a proxy is healthy with retry mechanism.
        
        Args:
            proxy: Proxy to check
            
        Returns:
            True if proxy is healthy, False otherwise
        """
        async def _check_proxy():
            proxy_dict = self._proxy_to_dict(proxy)
            
            async with httpx.AsyncClient(
                proxies=proxy_dict,
                timeout=self.health_check_timeout
            ) as client:
                # Test with a simple request
                response = await client.get('http://httpbin.org/ip')
                response.raise_for_status()
                
                # Update proxy info
                proxy.last_checked = time.time()
                proxy.response_time = response.elapsed.total_seconds()
                proxy.is_active = True
                
                logger.debug(f"Proxy {proxy.host}:{proxy.port} health check passed")
                return True
        
        try:
            return await async_retry_with_backoff(_check_proxy)
        except Exception as e:
            logger.warning(f"Proxy {proxy.host}:{proxy.port} health check failed after retries: {e}")
            proxy.is_active = False
            return False
    
    async def health_check_all(self) -> Dict[str, Any]:
        """
        Check health of all proxies.
        
        Returns:
            Health check results
        """
        logger.info("Starting health check for all proxies")
        
        results = {
            'total': len(self.proxies),
            'healthy': 0,
            'unhealthy': 0,
            'checked': 0
        }
        
        for proxy in self.proxies:
            if await self.health_check_proxy(proxy):
                results['healthy'] += 1
            else:
                results['unhealthy'] += 1
            results['checked'] += 1
        
        logger.info(f"Health check completed: {results['healthy']}/{results['total']} healthy")
        return results
    
    def get_proxy_stats(self) -> Dict[str, Any]:
        """Get proxy statistics."""
        total_proxies = len(self.proxies)
        active_proxies = len([p for p in self.proxies if p.is_active])
        bad_proxies = len(self.bad_proxies)
        
        # Calculate average success rate
        total_success = sum(p.success_count for p in self.proxies)
        total_failures = sum(p.failure_count for p in self.proxies)
        total_requests = total_success + total_failures
        
        success_rate = (total_success / total_requests * 100) if total_requests > 0 else 0
        
        return {
            'total_proxies': total_proxies,
            'active_proxies': active_proxies,
            'bad_proxies': bad_proxies,
            'success_rate': round(success_rate, 2),
            'total_requests': total_requests,
            'total_success': total_success,
            'total_failures': total_failures
        }
    
    def reset_bad_proxies(self) -> None:
        """Reset all bad proxies to active state."""
        for proxy in self.bad_proxies:
            proxy.is_active = True
            proxy.failure_count = 0
        
        self.bad_proxies.clear()
        logger.info("Reset all bad proxies to active state")
    
    def add_proxy(self, host: str, port: int, username: str = None, 
                  password: str = None, protocol: str = 'http',
                  country: str = None, provider: str = None) -> None:
        """
        Add a new proxy to the pool.
        
        Args:
            host: Proxy host
            port: Proxy port
            username: Proxy username
            password: Proxy password
            protocol: Proxy protocol
            country: Proxy country
            provider: Proxy provider
        """
        proxy = ProxyInfo(
            host=host,
            port=port,
            username=username,
            password=password,
            protocol=protocol,
            country=country,
            provider=provider
        )
        
        self.proxies.append(proxy)
        logger.info(f"Added proxy {host}:{port}")
    
    def remove_proxy(self, host: str, port: int) -> bool:
        """
        Remove a proxy from the pool.
        
        Args:
            host: Proxy host
            port: Proxy port
            
        Returns:
            True if proxy was removed, False if not found
        """
        for i, proxy in enumerate(self.proxies):
            if proxy.host == host and proxy.port == port:
                removed_proxy = self.proxies.pop(i)
                self.bad_proxies.discard(removed_proxy)
                logger.info(f"Removed proxy {host}:{port}")
                return True
        
        logger.warning(f"Proxy {host}:{port} not found")
        return False
    
    def save_proxy_pool(self, file_path: str) -> None:
        """Save proxy pool to file."""
        try:
            proxy_data = []
            for proxy in self.proxies:
                proxy_data.append({
                    'host': proxy.host,
                    'port': proxy.port,
                    'username': proxy.username,
                    'password': proxy.password,
                    'protocol': proxy.protocol,
                    'country': proxy.country,
                    'provider': proxy.provider,
                    'success_count': proxy.success_count,
                    'failure_count': proxy.failure_count,
                    'is_active': proxy.is_active
                })
            
            with open(file_path, 'w') as f:
                json.dump(proxy_data, f, indent=2)
            
            logger.info(f"Saved proxy pool to {file_path}")
            
        except Exception as e:
            logger.error(f"Failed to save proxy pool: {e}")
    
    def load_proxy_pool(self, file_path: str) -> None:
        """Load proxy pool from file."""
        try:
            with open(file_path, 'r') as f:
                proxy_data = json.load(f)
            
            self.proxies.clear()
            self.bad_proxies.clear()
            
            for data in proxy_data:
                proxy = ProxyInfo(
                    host=data['host'],
                    port=data['port'],
                    username=data.get('username'),
                    password=data.get('password'),
                    protocol=data.get('protocol', 'http'),
                    country=data.get('country'),
                    provider=data.get('provider'),
                    success_count=data.get('success_count', 0),
                    failure_count=data.get('failure_count', 0),
                    is_active=data.get('is_active', True)
                )
                self.proxies.append(proxy)
                
                if not proxy.is_active:
                    self.bad_proxies.add(proxy)
            
            logger.info(f"Loaded proxy pool from {file_path}")
            
        except Exception as e:
            logger.error(f"Failed to load proxy pool: {e}")


# Global proxy manager instance
_proxy_manager: Optional[ProxyManager] = None


def get_proxy_manager(config: Dict[str, Any] = None) -> ProxyManager:
    """Get global proxy manager instance."""
    global _proxy_manager
    if _proxy_manager is None:
        _proxy_manager = ProxyManager(config)
    return _proxy_manager


def get_proxy() -> Optional[Dict[str, Any]]:
    """Get next available proxy."""
    return get_proxy_manager().get_proxy()


def mark_bad(proxy: Dict[str, Any]) -> None:
    """Mark a proxy as bad."""
    get_proxy_manager().mark_bad(proxy)


def mark_success(proxy: Dict[str, Any]) -> None:
    """Mark a proxy as successful."""
    get_proxy_manager().mark_success(proxy)


def health_check_all() -> Dict[str, Any]:
    """Check health of all proxies."""
    return asyncio.run(get_proxy_manager().health_check_all())


def get_proxy_stats() -> Dict[str, Any]:
    """Get proxy statistics."""
    return get_proxy_manager().get_proxy_stats()
