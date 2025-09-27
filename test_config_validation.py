#!/usr/bin/env python3
"""
Test script for config validation functionality.
"""

import sys
import os
import tempfile
import yaml
from pathlib import Path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '.'))

from harvest.config import load_config, validate_config
from harvest.utils.logger import get_logger

logger = get_logger("test.config")


def create_test_config(config_data: dict, filename: str = "test_config.yaml") -> str:
    """Create a temporary config file for testing."""
    with open(filename, 'w', encoding='utf-8') as f:
        yaml.dump(config_data, f, default_flow_style=False)
    return filename


def test_valid_config():
    """Test with a valid configuration."""
    logger.info("Testing valid configuration...")
    
    valid_config = {
        'apis': {
            'unsplash': {
                'access_key': 'test_api_key_12345'
            }
        },
        'storage': {
            'path': 'test_storage'
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
            'path': 'test_db/images.db'
        }
    }
    
    config_file = create_test_config(valid_config)
    
    try:
        config = load_config(config_file)
        validate_config(config)
        logger.info("✅ Valid config test passed")
        return True
    except SystemExit:
        logger.error("❌ Valid config test failed - should not exit")
        return False
    except Exception as e:
        logger.error(f"❌ Valid config test failed with exception: {e}")
        return False
    finally:
        # Cleanup
        Path(config_file).unlink(missing_ok=True)
        Path("test_storage").rmdir() if Path("test_storage").exists() else None
        Path("test_db").rmdir() if Path("test_db").exists() else None


def test_missing_api_key():
    """Test with missing API key."""
    logger.info("Testing missing API key...")
    
    invalid_config = {
        'apis': {
            'unsplash': {
                'access_key': ''  # Empty API key
            }
        },
        'storage': {
            'path': 'test_storage'
        }
    }
    
    config_file = create_test_config(invalid_config)
    
    try:
        config = load_config(config_file)
        validate_config(config)
        logger.error("❌ Missing API key test failed - should have exited")
        return False
    except SystemExit:
        logger.info("✅ Missing API key test passed - correctly exited")
        return True
    except Exception as e:
        logger.error(f"❌ Missing API key test failed with exception: {e}")
        return False
    finally:
        Path(config_file).unlink(missing_ok=True)


def test_invalid_scheduler_interval():
    """Test with invalid scheduler interval."""
    logger.info("Testing invalid scheduler interval...")
    
    invalid_config = {
        'apis': {
            'unsplash': {
                'access_key': 'test_api_key_12345'
            }
        },
        'storage': {
            'path': 'test_storage'
        },
        'scheduler': {
            'interval_hours': -5  # Negative interval
        }
    }
    
    config_file = create_test_config(invalid_config)
    
    try:
        config = load_config(config_file)
        validate_config(config)
        logger.error("❌ Invalid scheduler interval test failed - should have exited")
        return False
    except SystemExit:
        logger.info("✅ Invalid scheduler interval test passed - correctly exited")
        return True
    except Exception as e:
        logger.error(f"❌ Invalid scheduler interval test failed with exception: {e}")
        return False
    finally:
        Path(config_file).unlink(missing_ok=True)


def test_invalid_download_timeout():
    """Test with invalid download timeout."""
    logger.info("Testing invalid download timeout...")
    
    invalid_config = {
        'apis': {
            'unsplash': {
                'access_key': 'test_api_key_12345'
            }
        },
        'storage': {
            'path': 'test_storage'
        },
        'download': {
            'timeout': 0  # Invalid timeout
        }
    }
    
    config_file = create_test_config(invalid_config)
    
    try:
        config = load_config(config_file)
        validate_config(config)
        logger.error("❌ Invalid download timeout test failed - should have exited")
        return False
    except SystemExit:
        logger.info("✅ Invalid download timeout test passed - correctly exited")
        return True
    except Exception as e:
        logger.error(f"❌ Invalid download timeout test failed with exception: {e}")
        return False
    finally:
        Path(config_file).unlink(missing_ok=True)


def test_directory_creation():
    """Test automatic directory creation."""
    logger.info("Testing automatic directory creation...")
    
    config_data = {
        'apis': {
            'unsplash': {
                'access_key': 'test_api_key_12345'
            }
        },
        'storage': {
            'path': 'auto_created_storage'
        },
        'database': {
            'path': 'auto_created_db/images.db'
        }
    }
    
    config_file = create_test_config(config_data)
    
    try:
        config = load_config(config_file)
        validate_config(config)
        
        # Check if directories were created
        if Path("auto_created_storage").exists() and Path("auto_created_db").exists():
            logger.info("✅ Directory creation test passed")
            return True
        else:
            logger.error("❌ Directory creation test failed - directories not created")
            return False
    except SystemExit:
        logger.error("❌ Directory creation test failed - should not exit")
        return False
    except Exception as e:
        logger.error(f"❌ Directory creation test failed with exception: {e}")
        return False
    finally:
        # Cleanup
        Path(config_file).unlink(missing_ok=True)
        Path("auto_created_storage").rmdir() if Path("auto_created_storage").exists() else None
        Path("auto_created_db").rmdir() if Path("auto_created_db").exists() else None


def main():
    """Run all config validation tests."""
    logger.info("Config Validation Test Suite")
    logger.info("=" * 50)
    
    tests = [
        ("Valid Configuration", test_valid_config),
        ("Missing API Key", test_missing_api_key),
        ("Invalid Scheduler Interval", test_invalid_scheduler_interval),
        ("Invalid Download Timeout", test_invalid_download_timeout),
        ("Directory Creation", test_directory_creation),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        logger.info(f"\n{'='*20} {test_name} {'='*20}")
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            logger.error(f"Test {test_name} failed with exception: {e}")
            results.append((test_name, False))
    
    # Print results
    logger.info("\n" + "=" * 50)
    logger.info("CONFIG VALIDATION TEST RESULTS")
    logger.info("=" * 50)
    
    all_passed = True
    for test_name, passed in results:
        status = "PASSED" if passed else "FAILED"
        logger.info(f"{test_name}: {status}")
        if not passed:
            all_passed = False
    
    logger.info("=" * 50)
    
    if all_passed:
        logger.info("All config validation tests passed! ✅")
    else:
        logger.error("Some config validation tests failed! ❌")
    
    return all_passed


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
