"""
Configuration management for the harvest package.
"""

import yaml
import sys
import os
from pathlib import Path
from typing import Dict, Any, List
from .utils.logger import get_logger


def load_config(config_path: str = "config.yaml") -> Dict[str, Any]:
    """
    Load configuration from YAML file and return as dictionary.
    
    Args:
        config_path: Path to the configuration file (default: "config.yaml")
        
    Returns:
        Dictionary containing configuration data
        
    Raises:
        FileNotFoundError: If config.yaml file doesn't exist
        yaml.YAMLError: If YAML file is malformed
    """
    config_file = Path(config_path)
    
    if not config_file.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config_data = yaml.safe_load(f)
            return config_data or {}
    except yaml.YAMLError as e:
        raise yaml.YAMLError(f"Error parsing YAML file {config_path}: {e}")


class Config:
    """Configuration manager."""
    
    def __init__(self, config_path: str = "config.yaml"):
        self.config_path = Path(config_path)
        self._config: Dict[str, Any] = {}
        self.load()
    
    def load(self) -> None:
        """Load configuration from YAML file."""
        if self.config_path.exists():
            with open(self.config_path, 'r', encoding='utf-8') as f:
                self._config = yaml.safe_load(f) or {}
        else:
            self._config = {}
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value."""
        return self._config.get(key, default)
    
    def set(self, key: str, value: Any) -> None:
        """Set configuration value."""
        self._config[key] = value
    
    def save(self) -> None:
        """Save configuration to YAML file."""
        with open(self.config_path, 'w', encoding='utf-8') as f:
            yaml.dump(self._config, f, default_flow_style=False)


def validate_config(config: Dict[str, Any]) -> None:
    """
    Validate configuration and exit with error if validation fails.
    
    Args:
        config: Configuration dictionary to validate
        
    Raises:
        SystemExit: If validation fails
    """
    logger = get_logger("harvest.config")
    errors: List[str] = []
    
    # 1. Check API key
    api_key = config.get('apis', {}).get('unsplash', {}).get('access_key')
    if not api_key or api_key.strip() == "":
        errors.append("Unsplash API key is required (apis.unsplash.access_key)")
    
    # 2. Check download directory (storage path)
    storage_path = config.get('storage', {}).get('path', 'storage')
    try:
        storage_dir = Path(storage_path)
        if not storage_dir.exists():
            logger.info(f"Creating storage directory: {storage_dir}")
            storage_dir.mkdir(parents=True, exist_ok=True)
        
        # Check if directory is writable
        if not os.access(storage_dir, os.W_OK):
            errors.append(f"Storage directory is not writable: {storage_dir}")
            
    except Exception as e:
        errors.append(f"Invalid storage directory '{storage_path}': {e}")
    
    # 3. Check scheduler interval
    scheduler_config = config.get('scheduler', {})
    interval_hours = scheduler_config.get('interval_hours')
    
    if interval_hours is not None:
        try:
            interval_value = int(interval_hours)
            if interval_value <= 0:
                errors.append("scheduler.interval_hours must be a positive integer")
        except (ValueError, TypeError):
            errors.append("scheduler.interval_hours must be a valid integer")
    
    # 4. Check database path
    db_path = config.get('database', {}).get('path', 'db/images.db')
    try:
        db_dir = Path(db_path).parent
        if not db_dir.exists():
            logger.info(f"Creating database directory: {db_dir}")
            db_dir.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        errors.append(f"Invalid database path '{db_path}': {e}")
    
    # 5. Check download timeout
    download_config = config.get('download', {})
    timeout = download_config.get('timeout', 30)
    try:
        timeout_value = int(timeout)
        if timeout_value <= 0:
            errors.append("download.timeout must be a positive integer")
    except (ValueError, TypeError):
        errors.append("download.timeout must be a valid integer")
    
    # 6. Check max concurrent downloads
    max_concurrent = download_config.get('max_concurrent', 5)
    try:
        concurrent_value = int(max_concurrent)
        if concurrent_value <= 0:
            errors.append("download.max_concurrent must be a positive integer")
    except (ValueError, TypeError):
        errors.append("download.max_concurrent must be a valid integer")
    
    # 7. Check retry attempts
    retry_attempts = download_config.get('retry_attempts', 3)
    try:
        retry_value = int(retry_attempts)
        if retry_value < 0:
            errors.append("download.retry_attempts must be a non-negative integer")
    except (ValueError, TypeError):
        errors.append("download.retry_attempts must be a valid integer")
    
    # If there are validation errors, log them and exit
    if errors:
        logger.error("Config validation failed:")
        for error in errors:
            logger.error(f"  - {error}")
        logger.error("Please fix the configuration and try again.")
        sys.exit(1)
    
    logger.info("Configuration validation passed successfully")
