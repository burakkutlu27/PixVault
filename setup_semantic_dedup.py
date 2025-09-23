#!/usr/bin/env python3
"""
Setup script for semantic duplicate detection dependencies.
Installs CLIP, FAISS, and other required packages.
"""

import subprocess
import sys
import os


def install_dependencies():
    """Install all required dependencies for semantic duplicate detection."""
    print("Setting up semantic duplicate detection for PixVault...")
    print("=" * 60)
    
    dependencies = [
        "torch>=2.0.0",
        "clip-by-openai>=1.0", 
        "faiss-cpu>=1.7.4",
        "numpy>=1.21.0"
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
        'torch': 'PyTorch',
        'clip': 'CLIP',
        'faiss': 'FAISS',
        'numpy': 'NumPy'
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


def test_clip_model():
    """Test if CLIP model can be loaded."""
    print("\nTesting CLIP model loading...")
    
    try:
        import torch
        import clip
        
        # Test loading a small model
        device = "cuda" if torch.cuda.is_available() else "cpu"
        model, preprocess = clip.load("ViT-B/32", device=device)
        
        print(f"✓ CLIP model loaded successfully on {device}")
        print(f"  Model: ViT-B/32")
        print(f"  Device: {device}")
        
        return True
        
    except Exception as e:
        print(f"✗ CLIP model test failed: {e}")
        return False


def test_faiss():
    """Test if FAISS is working."""
    print("\nTesting FAISS...")
    
    try:
        import faiss
        import numpy as np
        
        # Create a simple test index
        dimension = 512
        index = faiss.IndexFlatIP(dimension)
        
        # Add some random vectors
        vectors = np.random.random((10, dimension)).astype('float32')
        index.add(vectors)
        
        # Test search
        query = np.random.random((1, dimension)).astype('float32')
        distances, indices = index.search(query, 5)
        
        print(f"✓ FAISS test successful")
        print(f"  Index size: {index.ntotal}")
        print(f"  Dimension: {dimension}")
        
        return True
        
    except Exception as e:
        print(f"✗ FAISS test failed: {e}")
        return False


def main():
    """Main setup function."""
    print("PixVault Semantic Duplicate Detection Setup")
    print("=" * 60)
    
    # Check if already installed
    if check_installation():
        print("\nAll dependencies are already installed!")
        
        # Test functionality
        if test_clip_model() and test_faiss():
            print("\n✓ Setup verification completed successfully!")
            print("\nYou can now use semantic duplicate detection:")
            print("  python test_semantic_dedup.py")
            return
        else:
            print("\n⚠ Dependencies installed but tests failed.")
            print("You may need to reinstall or check your environment.")
            return
    
    # Install dependencies
    install_dependencies()
    
    # Verify installation
    if check_installation():
        print("\n✓ Installation completed!")
        
        # Test functionality
        if test_clip_model() and test_faiss():
            print("\n✓ All tests passed!")
            print("\nYou can now use semantic duplicate detection:")
            print("  python test_semantic_dedup.py")
        else:
            print("\n⚠ Installation completed but tests failed.")
            print("You may need to restart your Python environment.")
    else:
        print("\n✗ Installation failed. Please check the error messages above.")


if __name__ == "__main__":
    main()
