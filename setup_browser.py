#!/usr/bin/env python3
"""
Setup script for browser adapter dependencies.
Installs Playwright and downloads browser binaries.
"""

import subprocess
import sys
import os


def install_playwright():
    """Install Playwright and download browser binaries."""
    print("Setting up browser adapter for PixVault...")
    print("=" * 50)
    
    try:
        # Install Playwright
        print("Installing Playwright...")
        subprocess.run([sys.executable, "-m", "pip", "install", "playwright>=1.40.0"], check=True)
        
        # Install browser binaries
        print("Installing browser binaries...")
        subprocess.run([sys.executable, "-m", "playwright", "install", "chromium"], check=True)
        
        print("✓ Browser adapter setup completed successfully!")
        print("\nYou can now use the browser adapter with:")
        print("  python -m harvest.cli --source browser --query 'your query' --limit 10 --label test")
        
    except subprocess.CalledProcessError as e:
        print(f"✗ Setup failed: {e}")
        print("\nPlease try running manually:")
        print("  pip install playwright>=1.40.0")
        print("  playwright install chromium")
        sys.exit(1)
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        sys.exit(1)


def check_installation():
    """Check if Playwright is properly installed."""
    try:
        import playwright
        print(f"✓ Playwright version {playwright.__version__} is installed")
        return True
    except ImportError:
        print("✗ Playwright is not installed")
        return False


def main():
    """Main setup function."""
    print("PixVault Browser Adapter Setup")
    print("=" * 50)
    
    # Check if already installed
    if check_installation():
        print("Playwright is already installed. Skipping installation.")
        return
    
    # Install Playwright
    install_playwright()
    
    # Verify installation
    if check_installation():
        print("\n✓ Setup completed successfully!")
    else:
        print("\n✗ Setup failed. Please check the error messages above.")


if __name__ == "__main__":
    main()
