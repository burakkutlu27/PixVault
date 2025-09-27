#!/usr/bin/env python3
"""
Example usage of config validation functionality.
"""

import sys
import os
import yaml
from pathlib import Path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '.'))

from harvest.config import load_config, validate_config
from harvest.utils.logger import get_logger

logger = get_logger("example.config")


def create_example_configs():
    """Create example config files to demonstrate validation."""
    
    # Valid configuration
    valid_config = {
        'apis': {
            'unsplash': {
                'access_key': 'your_unsplash_api_key_here'
            }
        },
        'storage': {
            'path': 'storage'
        },
        'scheduler': {
            'interval_hours': 24
        },
        'download': {
            'timeout': 30,
            'max_concurrent': 5,
            'retry_attempts': 3
        },
        'database': {
            'path': 'db/images.db'
        }
    }
    
    # Invalid configuration (missing API key)
    invalid_config = {
        'apis': {
            'unsplash': {
                'access_key': ''  # Empty API key
            }
        },
        'storage': {
            'path': 'storage'
        },
        'scheduler': {
            'interval_hours': -1  # Invalid interval
        }
    }
    
    # Save example configs
    with open('example_valid_config.yaml', 'w', encoding='utf-8') as f:
        yaml.dump(valid_config, f, default_flow_style=False)
    
    with open('example_invalid_config.yaml', 'w', encoding='utf-8') as f:
        yaml.dump(invalid_config, f, default_flow_style=False)
    
    logger.info("Created example config files:")
    logger.info("  - example_valid_config.yaml")
    logger.info("  - example_invalid_config.yaml")


def demonstrate_validation():
    """Demonstrate config validation."""
    print("PixVault Config Validation Examples")
    print("=" * 50)
    print()
    
    print("🔍 Config Validation Features:")
    print("✅ API key validation (required)")
    print("✅ Storage directory validation (creates if missing)")
    print("✅ Scheduler interval validation (positive integer)")
    print("✅ Download timeout validation (positive integer)")
    print("✅ Database path validation (creates directory if missing)")
    print("✅ Max concurrent downloads validation")
    print("✅ Retry attempts validation")
    print()
    
    print("📋 Validation Checks:")
    print("1. apis.unsplash.access_key - Must not be empty")
    print("2. storage.path - Must be valid directory (created if missing)")
    print("3. scheduler.interval_hours - Must be positive integer")
    print("4. download.timeout - Must be positive integer")
    print("5. download.max_concurrent - Must be positive integer")
    print("6. download.retry_attempts - Must be non-negative integer")
    print("7. database.path - Must be valid path (directory created if missing)")
    print()
    
    print("🚨 Error Handling:")
    print("- If validation fails, program exits with red error logs")
    print("- All validation errors are logged before exit")
    print("- Program won't start with invalid configuration")
    print()
    
    print("📁 Automatic Directory Creation:")
    print("- Storage directory created if missing")
    print("- Database directory created if missing")
    print("- Directories are created with proper permissions")
    print()
    
    print("💡 Usage in Code:")
    print("```python")
    print("from harvest.config import load_config, validate_config")
    print("")
    print("# Load configuration")
    print("config = load_config('config.yaml')")
    print("")
    print("# Validate configuration (exits if invalid)")
    print("validate_config(config)")
    print("```")
    print()
    
    print("🧪 Testing:")
    print("- Run 'python test_config_validation.py' to test validation")
    print("- Check example config files for valid/invalid examples")
    print()
    
    print("✨ Config validation is now integrated into:")
    print("- harvest.cli (command line interface)")
    print("- harvest.scheduler (scheduler module)")
    print("- All modules that load configuration")


def main():
    """Main function."""
    # Create example configs
    create_example_configs()
    
    # Demonstrate validation
    demonstrate_validation()
    
    print("\n" + "=" * 50)
    print("Config validation is now active!")
    print("Invalid configurations will be caught before program starts.")


if __name__ == "__main__":
    main()
