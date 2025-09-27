#!/usr/bin/env python3
"""
Example usage of the enhanced logger with colored console output and file logging.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '.'))

from harvest.utils.logger import get_logger, setup_logger, log_with_extra
from pathlib import Path

def main():
    """Demonstrate enhanced logger features."""
    print("PixVault Enhanced Logger Examples")
    print("=" * 50)
    print()
    
    print("🎨 Enhanced Logger Features:")
    print("✅ Colored console output (INFO=green, WARNING=yellow, ERROR=red)")
    print("✅ Separate log files (logs/info.log, logs/error.log)")
    print("✅ JSON formatted file logs")
    print("✅ Extra fields support")
    print("✅ Exception logging")
    print("✅ Multiple logger instances")
    print()
    
    # Example 1: Basic usage
    print("Example 1: Basic Logger Usage")
    print("-" * 30)
    logger = get_logger("harvest.example")
    
    logger.info("Application started")
    logger.warning("This is a warning message")
    logger.error("This is an error message")
    print()
    
    # Example 2: Extra fields
    print("Example 2: Logging with Extra Fields")
    print("-" * 30)
    
    # Download example
    log_with_extra(
        logger, "INFO", "Image downloaded successfully",
        url="https://example.com/image.jpg",
        filename="image_001.jpg",
        size="1920x1080",
        file_size="2.5MB",
        duration=1.2
    )
    
    # Error example
    log_with_extra(
        logger, "ERROR", "Download failed",
        url="https://example.com/broken.jpg",
        error_code=404,
        retry_count=3,
        error_message="File not found"
    )
    print()
    
    # Example 3: Different modules
    print("Example 3: Different Module Loggers")
    print("-" * 30)
    
    cli_logger = get_logger("harvest.cli")
    downloader_logger = get_logger("harvest.downloader")
    scheduler_logger = get_logger("harvest.scheduler")
    
    cli_logger.info("CLI command executed", command="harvest --source unsplash --query nature")
    downloader_logger.info("Image processing completed", images_processed=15, duplicates_found=3)
    scheduler_logger.info("Scheduled task executed", task="daily_harvest", images_downloaded=25)
    print()
    
    # Example 4: Exception logging
    print("Example 4: Exception Logging")
    print("-" * 30)
    
    try:
        # Simulate an error
        data = {"images": ["img1.jpg", "img2.jpg"]}
        print(data["missing_key"])
    except KeyError as e:
        logger.error("KeyError occurred while processing data", exc_info=True)
    print()
    
    # Example 5: Custom logger configuration
    print("Example 5: Custom Logger Configuration")
    print("-" * 30)
    
    # Create a custom logger for testing
    test_logger = setup_logger(
        name="harvest.test",
        level="DEBUG",
        enable_console=True,
        enable_file_logging=True,
        log_dir="test_logs"
    )
    
    test_logger.debug("Debug message (only in console)")
    test_logger.info("Info message (console + logs/info.log)")
    test_logger.warning("Warning message (console + logs/info.log)")
    test_logger.error("Error message (console + logs/info.log + logs/error.log)")
    print()
    
    print("📁 Log Files Created:")
    print("   - logs/info.log (INFO and above)")
    print("   - logs/error.log (ERROR and above)")
    print("   - test_logs/info.log (test logger)")
    print("   - test_logs/error.log (test logger)")
    print()
    
    print("🎨 Console Colors:")
    print("   - INFO messages: GREEN")
    print("   - WARNING messages: YELLOW") 
    print("   - ERROR messages: RED")
    print("   - CRITICAL messages: MAGENTA")
    print("   - DEBUG messages: CYAN")
    print()
    
    print("📋 Usage in Your Code:")
    print("   from harvest.utils.logger import get_logger")
    print("   logger = get_logger('your.module.name')")
    print("   logger.info('Your message here')")
    print()
    
    print("✨ The enhanced logger is now active across all PixVault modules!")


if __name__ == "__main__":
    main()
