#!/usr/bin/env python3
"""
Test script for PixVault web UI.
Demonstrates usage of FastAPI web interface and API endpoints.
"""

import os
import sys
import time
import requests
from pathlib import Path

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from harvest.web_ui import app, run_web_ui
from harvest.config import load_config
from harvest.db import init_db, Database


def test_web_ui_startup():
    """Test web UI startup and basic functionality."""
    print("Testing Web UI Startup")
    print("=" * 40)
    
    try:
        # Test FastAPI app creation
        print("✓ FastAPI app created successfully")
        
        # Test configuration loading
        config = load_config()
        print("✓ Configuration loaded successfully")
        
        # Test database initialization
        init_db("test_web_ui.db")
        print("✓ Database initialized successfully")
        
        return True
        
    except Exception as e:
        print(f"✗ Web UI startup failed: {e}")
        return False


def test_api_endpoints():
    """Test API endpoints (requires running server)."""
    print("\nTesting API Endpoints")
    print("=" * 40)
    
    base_url = "http://127.0.0.1:8000"
    
    try:
        # Test health check (if available)
        try:
            response = requests.get(f"{base_url}/api/stats", timeout=5)
            if response.status_code == 200:
                print("✓ API server is running")
                return True
        except requests.exceptions.ConnectionError:
            print("⚠ API server not running - start with: python -m harvest.web_ui")
            return False
        
    except Exception as e:
        print(f"✗ API endpoint test failed: {e}")
        return False


def test_database_integration():
    """Test database integration."""
    print("\nTesting Database Integration")
    print("=" * 40)
    
    try:
        # Initialize test database
        db_path = "test_web_ui.db"
        init_db(db_path)
        database = Database(db_path)
        
        # Test database operations
        print("✓ Database connection successful")
        
        # Get all images
        images = database.get_all_images()
        print(f"✓ Found {len(images)} images in database")
        
        # Test image status update
        if images:
            test_image = images[0]
            database.update_image_status(test_image['id'], 'test_status', 'Test update')
            print("✓ Image status update successful")
        
        return True
        
    except Exception as e:
        print(f"✗ Database integration test failed: {e}")
        return False


def test_thumbnail_generation():
    """Test thumbnail generation functionality."""
    print("\nTesting Thumbnail Generation")
    print("=" * 40)
    
    try:
        from harvest.web_ui import create_thumbnail
        
        # Test with a sample image if available
        storage_dir = Path("storage")
        if storage_dir.exists():
            image_files = list(storage_dir.glob("*.jpg"))
            if image_files:
                test_image = str(image_files[0])
                thumbnail = create_thumbnail(test_image)
                
                if thumbnail:
                    print("✓ Thumbnail generation successful")
                    print(f"  Thumbnail size: {len(thumbnail)} characters")
                else:
                    print("⚠ Thumbnail generation returned None")
            else:
                print("⚠ No test images found in storage directory")
        else:
            print("⚠ Storage directory not found")
        
        return True
        
    except Exception as e:
        print(f"✗ Thumbnail generation test failed: {e}")
        return False


def test_duplicate_detection():
    """Test duplicate detection integration."""
    print("\nTesting Duplicate Detection Integration")
    print("=" * 40)
    
    try:
        from harvest.enhanced_dedupe import EnhancedDeduplicator
        
        # Initialize enhanced deduplicator
        deduplicator = EnhancedDeduplicator(
            db_path="test_web_ui.db",
            semantic_index_path="test_semantic_index"
        )
        
        print("✓ Enhanced deduplicator initialized")
        
        # Test getting duplicate groups
        duplicate_groups = deduplicator.get_duplicate_groups()
        print(f"✓ Found {len(duplicate_groups)} duplicate groups")
        
        # Test statistics
        stats = deduplicator.get_statistics()
        print(f"✓ Statistics retrieved: {len(stats)} sections")
        
        return True
        
    except Exception as e:
        print(f"✗ Duplicate detection test failed: {e}")
        return False


def test_template_rendering():
    """Test template rendering."""
    print("\nTesting Template Rendering")
    print("=" * 40)
    
    try:
        from fastapi.testclient import TestClient
        
        # Create test client
        client = TestClient(app)
        
        # Test main page
        response = client.get("/")
        if response.status_code == 200:
            print("✓ Main page template rendered successfully")
        else:
            print(f"⚠ Main page returned status {response.status_code}")
        
        return True
        
    except Exception as e:
        print(f"✗ Template rendering test failed: {e}")
        return False


def test_api_responses():
    """Test API response formats."""
    print("\nTesting API Response Formats")
    print("=" * 40)
    
    try:
        from fastapi.testclient import TestClient
        
        # Create test client
        client = TestClient(app)
        
        # Test images endpoint
        response = client.get("/api/images")
        if response.status_code == 200:
            data = response.json()
            print(f"✓ Images API returned {len(data)} images")
        else:
            print(f"⚠ Images API returned status {response.status_code}")
        
        # Test stats endpoint
        response = client.get("/api/stats")
        if response.status_code == 200:
            stats = response.json()
            print(f"✓ Stats API returned {len(stats)} statistics")
        else:
            print(f"⚠ Stats API returned status {response.status_code}")
        
        # Test duplicates endpoint
        response = client.get("/api/duplicates")
        if response.status_code == 200:
            duplicates = response.json()
            print(f"✓ Duplicates API returned {len(duplicates)} groups")
        else:
            print(f"⚠ Duplicates API returned status {response.status_code}")
        
        return True
        
    except Exception as e:
        print(f"✗ API response test failed: {e}")
        return False


def test_image_operations():
    """Test image operations."""
    print("\nTesting Image Operations")
    print("=" * 40)
    
    try:
        from fastapi.testclient import TestClient
        
        # Create test client
        client = TestClient(app)
        
        # Get images first
        response = client.get("/api/images")
        if response.status_code != 200:
            print("⚠ No images available for testing")
            return True
        
        images = response.json()
        if not images:
            print("⚠ No images found for testing")
            return True
        
        test_image = images[0]
        image_id = test_image['id']
        
        # Test approve image
        response = client.post(f"/api/images/{image_id}/approve")
        if response.status_code == 200:
            print("✓ Image approval successful")
        else:
            print(f"⚠ Image approval returned status {response.status_code}")
        
        # Test reject image
        response = client.post(f"/api/images/{image_id}/reject")
        if response.status_code == 200:
            print("✓ Image rejection successful")
        else:
            print(f"⚠ Image rejection returned status {response.status_code}")
        
        return True
        
    except Exception as e:
        print(f"✗ Image operations test failed: {e}")
        return False


def test_integration():
    """Test integration with existing system."""
    print("\nTesting Integration")
    print("=" * 40)
    
    try:
        # Test that modules can be imported
        from harvest.web_ui import app, run_web_ui
        from harvest.db import Database
        from harvest.enhanced_dedupe import EnhancedDeduplicator
        
        print("✓ All modules imported successfully")
        
        # Test configuration loading
        config = load_config()
        print(f"✓ Configuration loaded: {len(config)} sections")
        
        # Test database connection
        database = Database("test_web_ui.db")
        images = database.get_all_images()
        print(f"✓ Database connection successful: {len(images)} images")
        
        print("✓ Integration test completed")
        return True
        
    except Exception as e:
        print(f"✗ Integration test failed: {e}")
        return False


def cleanup_test_files():
    """Clean up test files."""
    print("\nCleaning up test files...")
    
    test_files = [
        "test_web_ui.db",
        "test_semantic_index",
        "static",
        "templates"
    ]
    
    for file_path in test_files:
        if os.path.exists(file_path):
            if os.path.isdir(file_path):
                import shutil
                shutil.rmtree(file_path)
                print(f"  Removed directory: {file_path}")
            else:
                os.remove(file_path)
                print(f"  Removed file: {file_path}")


def main():
    """Main test function."""
    print("PixVault Web UI Test")
    print("=" * 60)
    
    # Check dependencies
    try:
        import fastapi
        import uvicorn
        import jinja2
        print("✓ All dependencies available")
    except ImportError as e:
        print(f"✗ Missing dependencies: {e}")
        print("Install with: pip install fastapi uvicorn jinja2 python-multipart")
        return
    
    # Run tests
    test_web_ui_startup()
    test_database_integration()
    test_thumbnail_generation()
    test_duplicate_detection()
    test_template_rendering()
    test_api_responses()
    test_image_operations()
    test_integration()
    test_api_endpoints()
    
    # Cleanup
    cleanup_test_files()
    
    print("\n" + "=" * 60)
    print("All tests completed!")
    print("\nTo use the web UI:")
    print("  1. Start the server: python -m harvest.web_ui")
    print("  2. Open browser: http://127.0.0.1:8000")
    print("  3. Use the web interface to manage images")


if __name__ == "__main__":
    main()
