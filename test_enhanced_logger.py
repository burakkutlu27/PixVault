#!/usr/bin/env python3
"""
Test script for the enhanced logger with colored console output and file logging.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '.'))

import time
from harvest.utils.logger import get_logger, setup_logger, log_with_extra
from pathlib import Path

def test_basic_logging():
    """Test basic logging functionality."""
    print("Testing basic logging functionality...")
    
    logger = get_logger("test.basic")
    
    logger.debug("This is a DEBUG message")
    logger.info("This is an INFO message")
    logger.warning("This is a WARNING message")
    logger.error("This is an ERROR message")
    logger.critical("This is a CRITICAL message")
    
    print("Basic logging test completed!")


def test_colored_output():
    """Test colored console output."""
    print("\nTesting colored console output...")
    
    logger = get_logger("test.colors")
    
    print("You should see colored output below:")
    logger.info("✅ INFO messages should be GREEN")
    logger.warning("⚠️  WARNING messages should be YELLOW")
    logger.error("❌ ERROR messages should be RED")
    logger.critical("🚨 CRITICAL messages should be MAGENTA")
    
    print("Colored output test completed!")


def test_extra_fields():
    """Test logging with extra fields."""
    print("\nTesting extra fields logging...")
    
    logger = get_logger("test.extra")
    
    # Test with extra fields
    log_with_extra(
        logger, "INFO", "Download completed successfully",
        url="https://example.com/image.jpg",
        filename="image.jpg",
        size="1024x768",
        duration=2.5
    )
    
    log_with_extra(
        logger, "ERROR", "Download failed",
        url="https://example.com/broken.jpg",
        error_code=404,
        retry_count=3
    )
    
    print("Extra fields test completed!")


def test_file_logging():
    """Test file logging functionality."""
    print("\nTesting file logging...")
    
    # Create a test logger with file logging
    logger = setup_logger(
        name="test.file",
        level="INFO",
        enable_console=True,
        enable_file_logging=True,
        log_dir="test_logs"
    )
    
    logger.info("This should appear in both console and logs/info.log")
    logger.error("This should appear in console, logs/info.log, and logs/error.log")
    logger.warning("This should appear in console and logs/info.log")
    
    # Check if log files were created
    info_log = Path("test_logs/info.log")
    error_log = Path("test_logs/error.log")
    
    if info_log.exists():
        print(f"✅ Info log file created: {info_log}")
        print(f"   Size: {info_log.stat().st_size} bytes")
    else:
        print("❌ Info log file not found!")
    
    if error_log.exists():
        print(f"✅ Error log file created: {error_log}")
        print(f"   Size: {error_log.stat().st_size} bytes")
    else:
        print("❌ Error log file not found!")
    
    print("File logging test completed!")


def test_different_loggers():
    """Test different logger instances."""
    print("\nTesting different logger instances...")
    
    # Create different loggers for different modules
    cli_logger = get_logger("harvest.cli")
    downloader_logger = get_logger("harvest.downloader")
    scheduler_logger = get_logger("harvest.scheduler")
    
    cli_logger.info("CLI module started")
    downloader_logger.info("Downloader module initialized")
    scheduler_logger.info("Scheduler module started")
    
    # Test with extra fields
    downloader_logger.info("Image downloaded", 
                          url="https://example.com/test.jpg",
                          filename="test.jpg",
                          size="800x600")
    
    scheduler_logger.warning("Scheduled task delayed", 
                            task="daily_harvest",
                            delay_minutes=5)
    
    print("Different loggers test completed!")


def test_exception_logging():
    """Test exception logging."""
    print("\nTesting exception logging...")
    
    logger = get_logger("test.exception")
    
    try:
        # Simulate an error
        result = 1 / 0
    except ZeroDivisionError as e:
        logger.error("Division by zero error occurred", exc_info=True)
    
    try:
        # Simulate another error
        data = {"key": "value"}
        print(data["missing_key"])
    except KeyError as e:
        logger.error("Key not found in dictionary", exc_info=True)
    
    print("Exception logging test completed!")


def main():
    """Run all logger tests."""
    print("Enhanced Logger Test Suite")
    print("=" * 50)
    
    tests = [
        ("Basic Logging", test_basic_logging),
        ("Colored Output", test_colored_output),
        ("Extra Fields", test_extra_fields),
        ("File Logging", test_file_logging),
        ("Different Loggers", test_different_loggers),
        ("Exception Logging", test_exception_logging),
    ]
    
    for test_name, test_func in tests:
        print(f"\n{'='*20} {test_name} {'='*20}")
        try:
            test_func()
            print(f"✅ {test_name} PASSED")
        except Exception as e:
            print(f"❌ {test_name} FAILED: {e}")
    
    print("\n" + "=" * 50)
    print("Enhanced Logger Test Suite Completed!")
    print("\nCheck the following:")
    print("- Console output should be colored")
    print("- logs/info.log should contain INFO and above messages")
    print("- logs/error.log should contain ERROR and above messages")
    print("- test_logs/ directory should contain test log files")


if __name__ == "__main__":
    main()
