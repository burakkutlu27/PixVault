#!/usr/bin/env python3
"""
Setup script for task queue system dependencies.
Installs Celery, Redis, and other required packages.
"""

import subprocess
import sys
import os
import time


def install_dependencies():
    """Install all required dependencies for task queue system."""
    print("Setting up task queue system for PixVault...")
    print("=" * 60)
    
    dependencies = [
        "celery>=5.3.0",
        "redis>=4.5.0",
        "kombu>=5.3.0"
    ]
    
    try:
        for dep in dependencies:
            print(f"Installing {dep}...")
            subprocess.run([sys.executable, "-m", "pip", "install", dep], check=True)
        
        print("✓ All dependencies installed successfully!")
        
    except subprocess.CalledProcessError as e:
        print(f"✗ Installation failed: {e}")
        print("\nPlease try installing manually:")
        for dep in dependencies:
            print(f"  pip install {dep}")
        sys.exit(1)
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        sys.exit(1)


def check_installation():
    """Check if all dependencies are properly installed."""
    print("\nChecking installation...")
    
    dependencies = {
        'celery': 'Celery',
        'redis': 'Redis',
        'kombu': 'Kombu'
    }
    
    all_good = True
    
    for module, name in dependencies.items():
        try:
            __import__(module)
            print(f"✓ {name} is installed")
        except ImportError:
            print(f"✗ {name} is not installed")
            all_good = False
    
    return all_good


def test_redis_connection():
    """Test Redis connection."""
    print("\nTesting Redis connection...")
    
    try:
        import redis
        
        # Test connection
        redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        redis_client.ping()
        
        print("✓ Redis connection successful")
        
        # Test basic operations
        redis_client.set('test_key', 'test_value')
        value = redis_client.get('test_key')
        redis_client.delete('test_key')
        
        if value == 'test_value':
            print("✓ Redis operations working correctly")
            return True
        else:
            print("✗ Redis operations failed")
            return False
        
    except redis.ConnectionError:
        print("✗ Redis connection failed - Redis server not running")
        print("  Start Redis with: redis-server")
        return False
    except Exception as e:
        print(f"✗ Redis test failed: {e}")
        return False


def test_celery_setup():
    """Test Celery setup."""
    print("\nTesting Celery setup...")
    
    try:
        from harvest.tasks import app
        
        # Test Celery app
        inspect = app.control.inspect()
        
        # Try to get stats (this will fail if no workers, but app should be configured)
        try:
            stats = inspect.stats()
            if stats:
                print(f"✓ Celery app configured, {len(stats)} workers found")
            else:
                print("✓ Celery app configured, no workers running")
        except Exception:
            print("✓ Celery app configured")
        
        return True
        
    except Exception as e:
        print(f"✗ Celery setup test failed: {e}")
        return False


def create_redis_config():
    """Create Redis configuration file."""
    print("\nCreating Redis configuration...")
    
    redis_config = """
# Redis configuration for PixVault task queue
# Save this as redis.conf and start Redis with: redis-server redis.conf

# Basic settings
port 6379
bind 127.0.0.1
timeout 300
tcp-keepalive 60

# Memory settings
maxmemory 256mb
maxmemory-policy allkeys-lru

# Persistence settings
save 900 1
save 300 10
save 60 10000

# Logging
loglevel notice
logfile ""

# Security (uncomment for production)
# requirepass your_password_here
"""
    
    try:
        with open('redis.conf', 'w') as f:
            f.write(redis_config.strip())
        print("✓ Redis configuration file created: redis.conf")
        return True
    except Exception as e:
        print(f"✗ Failed to create Redis config: {e}")
        return False


def create_celery_config():
    """Create Celery configuration file."""
    print("\nCreating Celery configuration...")
    
    celery_config = """
# Celery configuration for PixVault
# Save this as celery_config.py

from celery import Celery

# Celery app configuration
app = Celery('harvest')

# Broker and result backend
app.conf.update(
    broker_url='redis://localhost:6379/0',
    result_backend='redis://localhost:6379/0',
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=300,
    task_soft_time_limit=240,
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    worker_disable_rate_limits=False,
)

# Task routes
app.conf.task_routes = {
    'harvest.tasks.download_image_task': {'queue': 'download'},
    'harvest.tasks.browser_search_task': {'queue': 'browser'},
}

# Queues
from kombu import Queue
app.conf.task_queues = (
    Queue('default', routing_key='default'),
    Queue('download', routing_key='download'),
    Queue('browser', routing_key='browser'),
)
"""
    
    try:
        with open('celery_config.py', 'w') as f:
            f.write(celery_config.strip())
        print("✓ Celery configuration file created: celery_config.py")
        return True
    except Exception as e:
        print(f"✗ Failed to create Celery config: {e}")
        return False


def create_startup_scripts():
    """Create startup scripts for Redis and Celery."""
    print("\nCreating startup scripts...")
    
    # Redis startup script
    redis_script = """#!/bin/bash
# Start Redis server for PixVault

echo "Starting Redis server..."
redis-server redis.conf &

echo "Redis server started"
echo "PID: $!"
echo "To stop: kill $!"
"""
    
    # Celery worker startup script
    celery_script = """#!/bin/bash
# Start Celery worker for PixVault

echo "Starting Celery worker..."
celery -A harvest.tasks worker --loglevel=info --concurrency=4 &

echo "Celery worker started"
echo "PID: $!"
echo "To stop: kill $!"
"""
    
    try:
        with open('start_redis.sh', 'w') as f:
            f.write(redis_script)
        os.chmod('start_redis.sh', 0o755)
        print("✓ Redis startup script created: start_redis.sh")
        
        with open('start_celery.sh', 'w') as f:
            f.write(celery_script)
        os.chmod('start_celery.sh', 0o755)
        print("✓ Celery startup script created: start_celery.sh")
        
        return True
    except Exception as e:
        print(f"✗ Failed to create startup scripts: {e}")
        return False


def main():
    """Main setup function."""
    print("PixVault Task Queue System Setup")
    print("=" * 60)
    
    # Check if already installed
    if check_installation():
        print("\nAll dependencies are already installed!")
        
        # Test functionality
        if test_redis_connection() and test_celery_setup():
            print("\n✓ Setup verification completed successfully!")
            print("\nYou can now use the task queue system:")
            print("  python test_task_queue.py")
            return
        else:
            print("\n⚠ Dependencies installed but tests failed.")
            print("You may need to start Redis server or check your environment.")
            return
    
    # Install dependencies
    install_dependencies()
    
    # Verify installation
    if check_installation():
        print("\n✓ Installation completed!")
        
        # Create configuration files
        create_redis_config()
        create_celery_config()
        create_startup_scripts()
        
        # Test functionality
        if test_redis_connection() and test_celery_setup():
            print("\n✓ All tests passed!")
            print("\nYou can now use the task queue system:")
            print("  python test_task_queue.py")
            print("\nTo start the system:")
            print("  1. Start Redis: ./start_redis.sh")
            print("  2. Start Celery: ./start_celery.sh")
            print("  3. Use CLI: python -m harvest.task_cli --help")
        else:
            print("\n⚠ Installation completed but tests failed.")
            print("You may need to start Redis server or check your environment.")
    else:
        print("\n✗ Installation failed. Please check the error messages above.")


if __name__ == "__main__":
    main()
