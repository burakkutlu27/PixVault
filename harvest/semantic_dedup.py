"""
Semantic duplicate detection using CLIP embeddings and FAISS.
Works alongside perceptual hashing for improved duplicate detection.
"""

import os
import pickle
import numpy as np
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
import logging

try:
    import torch
    import clip
    from PIL import Image
    import faiss
    CLIP_AVAILABLE = True
except ImportError:
    CLIP_AVAILABLE = False
    print("Warning: CLIP dependencies not available. Install with: pip install torch clip-by-openai faiss-cpu")

logger = logging.getLogger(__name__)


class SemanticDeduplicator:
    """
    Semantic duplicate detection using CLIP embeddings and FAISS.
    Provides approximate nearest neighbor search for image similarity.
    """
    
    def __init__(self, 
                 index_path: str = "db/semantic_index",
                 model_name: str = "ViT-B/32",
                 device: str = "auto"):
        """
        Initialize semantic deduplicator.
        
        Args:
            index_path: Path to store FAISS index and metadata
            model_name: CLIP model name (ViT-B/32, ViT-L/14, etc.)
            device: Device to run CLIP on ('auto', 'cpu', 'cuda')
        """
        if not CLIP_AVAILABLE:
            raise ImportError("CLIP dependencies not available. Install with: pip install torch clip-by-openai faiss-cpu")
        
        self.index_path = Path(index_path)
        self.index_path.mkdir(parents=True, exist_ok=True)
        
        self.model_name = model_name
        self.device = self._get_device(device)
        
        # Initialize CLIP model
        self.model, self.preprocess = self._load_clip_model()
        
        # FAISS index and metadata
        self.index = None
        self.id_to_metadata = {}
        self.embedding_dim = None
        
        # Load existing index if available
        self._load_index()
    
    def _get_device(self, device: str) -> str:
        """Determine the best device to use."""
        if device == "auto":
            if torch.cuda.is_available():
                return "cuda"
            elif hasattr(torch.backends, 'mps') and torch.backends.mps.is_available():
                return "mps"
            else:
                return "cpu"
        return device
    
    def _load_clip_model(self):
        """Load CLIP model and preprocessing."""
        try:
            model, preprocess = clip.load(self.model_name, device=self.device)
            logger.info(f"Loaded CLIP model {self.model_name} on {self.device}")
            return model, preprocess
        except Exception as e:
            logger.error(f"Failed to load CLIP model: {e}")
            raise
    
    def _get_image_embedding(self, image_path: str) -> Optional[np.ndarray]:
        """
        Extract CLIP embedding from image.
        
        Args:
            image_path: Path to image file
            
        Returns:
            CLIP embedding as numpy array or None if failed
        """
        try:
            # Load and preprocess image
            image = Image.open(image_path).convert('RGB')
            image_input = self.preprocess(image).unsqueeze(0).to(self.device)
            
            # Extract features
            with torch.no_grad():
                image_features = self.model.encode_image(image_input)
                image_features = image_features / image_features.norm(dim=-1, keepdim=True)
                
            return image_features.cpu().numpy().flatten()
            
        except Exception as e:
            logger.error(f"Failed to extract embedding from {image_path}: {e}")
            return None
    
    def _create_faiss_index(self, embedding_dim: int):
        """Create new FAISS index."""
        # Use IndexFlatIP for cosine similarity (since embeddings are normalized)
        self.index = faiss.IndexFlatIP(embedding_dim)
        self.embedding_dim = embedding_dim
        logger.info(f"Created FAISS index with dimension {embedding_dim}")
    
    def _load_index(self):
        """Load existing FAISS index and metadata."""
        index_file = self.index_path / "index.faiss"
        metadata_file = self.index_path / "metadata.pkl"
        
        if index_file.exists() and metadata_file.exists():
            try:
                # Load FAISS index
                self.index = faiss.read_index(str(index_file))
                self.embedding_dim = self.index.d
                
                # Load metadata
                with open(metadata_file, 'rb') as f:
                    self.id_to_metadata = pickle.load(f)
                
                logger.info(f"Loaded existing index with {self.index.ntotal} embeddings")
                
            except Exception as e:
                logger.error(f"Failed to load existing index: {e}")
                self.index = None
                self.id_to_metadata = {}
        else:
            logger.info("No existing index found, will create new one")
    
    def _save_index(self):
        """Save FAISS index and metadata to disk."""
        if self.index is None:
            return
        
        try:
            # Save FAISS index
            index_file = self.index_path / "index.faiss"
            faiss.write_index(self.index, str(index_file))
            
            # Save metadata
            metadata_file = self.index_path / "metadata.pkl"
            with open(metadata_file, 'wb') as f:
                pickle.dump(self.id_to_metadata, f)
            
            logger.info(f"Saved index with {self.index.ntotal} embeddings")
            
        except Exception as e:
            logger.error(f"Failed to save index: {e}")
    
    def add_to_index(self, image_path: str, image_id: str, metadata: Optional[Dict[str, Any]] = None) -> bool:
        """
        Add image to semantic index.
        
        Args:
            image_path: Path to image file
            image_id: Unique identifier for the image
            metadata: Additional metadata to store
            
        Returns:
            True if successfully added, False otherwise
        """
        # Extract embedding
        embedding = self._get_image_embedding(image_path)
        if embedding is None:
            return False
        
        # Create index if it doesn't exist
        if self.index is None:
            self._create_faiss_index(len(embedding))
        
        # Check if embedding dimension matches
        if len(embedding) != self.embedding_dim:
            logger.error(f"Embedding dimension mismatch: {len(embedding)} vs {self.embedding_dim}")
            return False
        
        try:
            # Add to FAISS index
            self.index.add(embedding.reshape(1, -1))
            
            # Store metadata
            self.id_to_metadata[image_id] = {
                'image_path': image_path,
                'metadata': metadata or {}
            }
            
            logger.debug(f"Added {image_id} to semantic index")
            return True
            
        except Exception as e:
            logger.error(f"Failed to add {image_id} to index: {e}")
            return False
    
    def find_similar(self, image_path: str, threshold: float = 0.8, max_results: int = 10) -> List[Dict[str, Any]]:
        """
        Find semantically similar images.
        
        Args:
            image_path: Path to query image
            threshold: Similarity threshold (0.0 to 1.0)
            max_results: Maximum number of results to return
            
        Returns:
            List of similar images with metadata
        """
        if self.index is None or self.index.ntotal == 0:
            return []
        
        # Extract query embedding
        query_embedding = self._get_image_embedding(image_path)
        if query_embedding is None:
            return []
        
        try:
            # Search for similar images
            similarities, indices = self.index.search(
                query_embedding.reshape(1, -1), 
                min(max_results, self.index.ntotal)
            )
            
            results = []
            for similarity, idx in zip(similarities[0], indices[0]):
                if similarity >= threshold and idx != -1:  # -1 means no result
                    # Get image ID from index position
                    image_ids = list(self.id_to_metadata.keys())
                    if idx < len(image_ids):
                        image_id = image_ids[idx]
                        result = {
                            'id': image_id,
                            'similarity': float(similarity),
                            'metadata': self.id_to_metadata[image_id]
                        }
                        results.append(result)
            
            return results
            
        except Exception as e:
            logger.error(f"Failed to find similar images: {e}")
            return []
    
    def find_similar_by_id(self, image_id: str, threshold: float = 0.8, max_results: int = 10) -> List[Dict[str, Any]]:
        """
        Find similar images by image ID.
        
        Args:
            image_id: ID of the query image
            threshold: Similarity threshold (0.0 to 1.0)
            max_results: Maximum number of results to return
            
        Returns:
            List of similar images with metadata
        """
        if image_id not in self.id_to_metadata:
            logger.warning(f"Image ID {image_id} not found in index")
            return []
        
        image_path = self.id_to_metadata[image_id]['image_path']
        return self.find_similar(image_path, threshold, max_results)
    
    def remove_from_index(self, image_id: str) -> bool:
        """
        Remove image from index.
        
        Args:
            image_id: ID of image to remove
            
        Returns:
            True if successfully removed, False otherwise
        """
        if image_id not in self.id_to_metadata:
            return False
        
        try:
            # FAISS doesn't support direct removal, so we need to rebuild
            # This is a limitation of FAISS IndexFlatIP
            logger.warning("FAISS IndexFlatIP doesn't support removal. Consider using IndexIVFFlat for large datasets.")
            
            # For now, just remove from metadata
            del self.id_to_metadata[image_id]
            return True
            
        except Exception as e:
            logger.error(f"Failed to remove {image_id} from index: {e}")
            return False
    
    def get_index_stats(self) -> Dict[str, Any]:
        """Get statistics about the index."""
        if self.index is None:
            return {
                'total_images': 0,
                'embedding_dimension': 0,
                'index_type': 'None'
            }
        
        return {
            'total_images': self.index.ntotal,
            'embedding_dimension': self.embedding_dim,
            'index_type': type(self.index).__name__,
            'model_name': self.model_name,
            'device': self.device
        }
    
    def rebuild_index(self, image_paths: List[str], image_ids: List[str]) -> bool:
        """
        Rebuild the entire index from scratch.
        
        Args:
            image_paths: List of image file paths
            image_ids: List of corresponding image IDs
            
        Returns:
            True if successful, False otherwise
        """
        if len(image_paths) != len(image_ids):
            logger.error("Number of image paths and IDs must match")
            return False
        
        try:
            # Clear existing index
            self.index = None
            self.id_to_metadata = {}
            
            # Process all images
            embeddings = []
            valid_paths = []
            valid_ids = []
            
            for image_path, image_id in zip(image_paths, image_ids):
                embedding = self._get_image_embedding(image_path)
                if embedding is not None:
                    embeddings.append(embedding)
                    valid_paths.append(image_path)
                    valid_ids.append(image_id)
            
            if not embeddings:
                logger.error("No valid embeddings found")
                return False
            
            # Create new index
            embeddings_array = np.vstack(embeddings)
            self._create_faiss_index(embeddings_array.shape[1])
            
            # Add all embeddings
            self.index.add(embeddings_array)
            
            # Store metadata
            for image_path, image_id in zip(valid_paths, valid_ids):
                self.id_to_metadata[image_id] = {
                    'image_path': image_path,
                    'metadata': {}
                }
            
            # Save index
            self._save_index()
            
            logger.info(f"Rebuilt index with {len(valid_ids)} images")
            return True
            
        except Exception as e:
            logger.error(f"Failed to rebuild index: {e}")
            return False
    
    def close(self):
        """Save index and cleanup resources."""
        self._save_index()
        logger.info("Semantic deduplicator closed")


# Convenience functions for easy integration
def create_semantic_deduplicator(index_path: str = "db/semantic_index") -> SemanticDeduplicator:
    """Create a semantic deduplicator instance."""
    return SemanticDeduplicator(index_path=index_path)


def add_image_to_semantic_index(image_path: str, image_id: str, 
                               index_path: str = "db/semantic_index",
                               metadata: Optional[Dict[str, Any]] = None) -> bool:
    """Add a single image to the semantic index."""
    deduplicator = SemanticDeduplicator(index_path=index_path)
    try:
        return deduplicator.add_to_index(image_path, image_id, metadata)
    finally:
        deduplicator.close()


def find_semantic_similarities(image_path: str, 
                              threshold: float = 0.8,
                              max_results: int = 10,
                              index_path: str = "db/semantic_index") -> List[Dict[str, Any]]:
    """Find semantically similar images."""
    deduplicator = SemanticDeduplicator(index_path=index_path)
    try:
        return deduplicator.find_similar(image_path, threshold, max_results)
    finally:
        deduplicator.close()
