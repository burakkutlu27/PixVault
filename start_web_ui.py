#!/usr/bin/env python3
"""
Startup script for PixVault Web UI.
Launches the FastAPI web interface with proper configuration.
"""

import os
import sys
import argparse
from pathlib import Path

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from harvest.web_ui import run_web_ui
from harvest.config import load_config
from harvest.db import init_db


def setup_directories():
    """Create necessary directories."""
    directories = [
        "static",
        "templates", 
        "storage",
        "db",
        "logs"
    ]
    
    for directory in directories:
        Path(directory).mkdir(exist_ok=True)
        print(f"✓ Created directory: {directory}")


def setup_database():
    """Initialize database."""
    try:
        config = load_config()
        db_path = config.get('database', {}).get('path', 'db/images.db')
        init_db(db_path)
        print(f"✓ Database initialized: {db_path}")
        return True
    except Exception as e:
        print(f"✗ Database initialization failed: {e}")
        return False


def check_dependencies():
    """Check if all required dependencies are available."""
    required_modules = [
        'fastapi',
        'uvicorn', 
        'jinja2',
        'PIL',
        'sqlite3'
    ]
    
    missing_modules = []
    
    for module in required_modules:
        try:
            __import__(module)
        except ImportError:
            missing_modules.append(module)
    
    if missing_modules:
        print(f"✗ Missing dependencies: {', '.join(missing_modules)}")
        print("Install with: pip install -r requirements.txt")
        return False
    
    print("✓ All dependencies available")
    return True


def main():
    """Main startup function."""
    parser = argparse.ArgumentParser(description="Start PixVault Web UI")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind to (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8000, help="Port to bind to (default: 8000)")
    parser.add_argument("--reload", action="store_true", help="Enable auto-reload for development")
    parser.add_argument("--setup", action="store_true", help="Setup directories and database")
    
    args = parser.parse_args()
    
    print("PixVault Web UI Startup")
    print("=" * 40)
    
    # Check dependencies
    if not check_dependencies():
        sys.exit(1)
    
    # Setup if requested
    if args.setup:
        print("\nSetting up PixVault Web UI...")
        setup_directories()
        if not setup_database():
            sys.exit(1)
        print("✓ Setup completed")
    
    # Start web UI
    print(f"\nStarting web UI on {args.host}:{args.port}")
    print(f"Open browser: http://{args.host}:{args.port}")
    print("Press Ctrl+C to stop")
    
    try:
        run_web_ui(host=args.host, port=args.port, reload=args.reload)
    except KeyboardInterrupt:
        print("\n✓ Web UI stopped")
    except Exception as e:
        print(f"\n✗ Web UI failed to start: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
