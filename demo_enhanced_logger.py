#!/usr/bin/env python3
"""
Quick demonstration of the enhanced logger features.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '.'))

from harvest.utils.logger import get_logger, log_with_extra

def main():
    """Quick demo of enhanced logger."""
    print("🚀 PixVault Enhanced Logger Demo")
    print("=" * 40)
    print()
    
    # Get logger
    logger = get_logger("demo")
    
    print("1. Colored Console Output:")
    print("   (You should see colored text below)")
    logger.info("✅ INFO message in GREEN")
    logger.warning("⚠️  WARNING message in YELLOW")
    logger.error("❌ ERROR message in RED")
    print()
    
    print("2. Extra Fields Logging:")
    log_with_extra(
        logger, "INFO", "Image download completed",
        url="https://example.com/image.jpg",
        filename="image_001.jpg",
        size="1920x1080",
        duration=1.5
    )
    print()
    
    print("3. File Logging:")
    print("   - Check 'logs/info.log' for INFO and above messages")
    print("   - Check 'logs/error.log' for ERROR and above messages")
    print()
    
    print("✨ Enhanced logger is now active across all PixVault modules!")
    print("   All existing code will automatically use the enhanced features.")


if __name__ == "__main__":
    main()
