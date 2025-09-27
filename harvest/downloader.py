"""
Image downloader for the harvest package.
"""

import httpx
import hashlib
import tempfile
import uuid
from pathlib import Path
from typing import Optional, Dict, Any
from PIL import Image
import imagehash
from urllib.parse import urlparse

from .db import insert_image, find_by_md5
from .utils.logger import get_logger
from .utils.retry import create_retryable_client


def download_and_store(url: str, label: str, config: dict) -> Dict[str, Any]:
    """
    Download image from URL and store in database with metadata.
    
    Args:
        url: Image URL to download
        label: Label for the image
        config: Configuration dictionary
        
    Returns:
        Dictionary with download result information
    """
    logger = get_logger("harvest.downloader")
    storage_path = Path(config.get('storage', {}).get('path', 'storage'))
    storage_path.mkdir(parents=True, exist_ok=True)
    
    timeout = config.get('download', {}).get('timeout', 30)
    db_path = config.get('database', {}).get('path', 'db/images.db')
    
    result = {
        'url': url,
        'label': label,
        'status': 'failed',
        'message': '',
        'image_id': None
    }
    
    try:
        # Step 1: Make GET request with retry mechanism
        logger.info(f"Starting download: {url}")
        
        with create_retryable_client(timeout=timeout) as client:
            response = client.get(url)
            
            # Step 2: Check content type
            content_type = response.headers.get('content-type', '').lower()
            if not content_type.startswith('image/'):
                result['message'] = f"Invalid content type: {content_type}"
                logger.warning(f"Invalid content type for {url}: {content_type}")
                return result
            
            # Step 3: Download to temporary file and calculate MD5
            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                temp_file.write(response.content)
                temp_path = Path(temp_file.name)
            
            # Calculate MD5 hash
            with open(temp_path, 'rb') as f:
                file_content = f.read()
                md5_hash = hashlib.md5(file_content).hexdigest()
            
            # Check if MD5 already exists in database
            existing_record = find_by_md5(md5_hash, db_path)
            if existing_record:
                result['status'] = 'duplicate'
                result['message'] = f"Image already exists with MD5: {md5_hash}"
                result['image_id'] = existing_record['id']
                logger.info(f"Duplicate image found: {url} (MD5: {md5_hash})")
                
                # Clean up temp file
                temp_path.unlink()
                return result
            
            # Step 4: Open with Pillow and extract metadata
            try:
                with Image.open(temp_path) as img:
                    # Calculate perceptual hash
                    phash = str(imagehash.phash(img))
                    
                    # Get image dimensions and format
                    width = img.width
                    height = img.height
                    format_name = img.format
                    
            except Exception as e:
                result['message'] = f"Invalid image file: {str(e)}"
                logger.error(f"Invalid image file {url}: {e}")
                temp_path.unlink()
                return result
            
            # Generate final filename
            parsed_url = urlparse(url)
            domain = parsed_url.netloc
            file_extension = _get_extension_from_content_type(content_type)
            filename = f"{md5_hash}{file_extension}"
            final_path = storage_path / filename
            
            # Move temp file to final location
            temp_path.rename(final_path)
            
            # Step 5: Save to database
            image_id = str(uuid.uuid4())
            record = {
                'id': image_id,
                'url': url,
                'domain': domain,
                'filename': filename,
                'md5': md5_hash,
                'phash': phash,
                'width': width,
                'height': height,
                'format': format_name,
                'label': label,
                'status': 'downloaded'
            }
            
            insert_image(record, db_path)
            
            # Step 6 & 7: Set status based on success
            result['status'] = 'downloaded'
            result['message'] = 'Image downloaded and stored successfully'
            result['image_id'] = image_id
            
            logger.info(f"Successfully downloaded and stored: {url}", 
                       extra_fields={
                           'image_id': image_id,
                           'filename': filename,
                           'md5': md5_hash,
                           'size': f"{width}x{height}",
                           'format': format_name
                       })
            
    except httpx.HTTPError as e:
        result['message'] = f"HTTP error: {str(e)}"
        logger.error(f"HTTP error downloading {url}: {e}")
        
    except Exception as e:
        result['message'] = f"Unexpected error: {str(e)}"
        logger.error(f"Unexpected error downloading {url}: {e}")
    
    return result


def _get_extension_from_content_type(content_type: str) -> str:
    """Get file extension from content type."""
    content_type = content_type.lower()
    
    if 'jpeg' in content_type or 'jpg' in content_type:
        return '.jpg'
    elif 'png' in content_type:
        return '.png'
    elif 'gif' in content_type:
        return '.gif'
    elif 'webp' in content_type:
        return '.webp'
    elif 'bmp' in content_type:
        return '.bmp'
    elif 'tiff' in content_type:
        return '.tiff'
    else:
        return '.jpg'  # default


class ImageDownloader:
    """Image downloader with deduplication support."""
    
    def __init__(self, storage_path: str = "storage", timeout: int = 30):
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        self.timeout = timeout
        self.client = httpx.Client(timeout=timeout)
    
    def download_image(self, url: str, filename: str = None) -> Optional[Dict[str, Any]]:
        """Download image from URL with retry mechanism."""
        try:
            # Use retryable client for this download
            with create_retryable_client(timeout=self.timeout) as client:
                response = client.get(url)
                
                if not filename:
                    filename = self._generate_filename(url, response.headers)
                
                file_path = self.storage_path / filename
                
                # Save image
                with open(file_path, 'wb') as f:
                    f.write(response.content)
                
                # Get image metadata
                metadata = self._get_image_metadata(file_path)
                
                return {
                    'url': url,
                    'filename': filename,
                    'file_path': str(file_path),
                    'file_size': file_path.stat().st_size,
                    'content_type': response.headers.get('content-type'),
                    **metadata
                }
            
        except Exception as e:
            print(f"Error downloading {url}: {e}")
            return None
    
    def _generate_filename(self, url: str, headers: Dict[str, str]) -> str:
        """Generate filename from URL and headers."""
        # Try to get filename from Content-Disposition header
        content_disposition = headers.get('content-disposition', '')
        if 'filename=' in content_disposition:
            filename = content_disposition.split('filename=')[1].strip('"')
            return filename
        
        # Generate from URL
        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        content_type = headers.get('content-type', '')
        
        if 'jpeg' in content_type or 'jpg' in content_type:
            ext = '.jpg'
        elif 'png' in content_type:
            ext = '.png'
        elif 'gif' in content_type:
            ext = '.gif'
        elif 'webp' in content_type:
            ext = '.webp'
        else:
            ext = '.jpg'  # default
        
        return f"{url_hash}{ext}"
    
    def _get_image_metadata(self, file_path: Path) -> Dict[str, Any]:
        """Get image metadata using PIL."""
        try:
            with Image.open(file_path) as img:
                # Calculate perceptual hash
                phash = str(imagehash.phash(img))
                
                return {
                    'width': img.width,
                    'height': img.height,
                    'format': img.format,
                    'mode': img.mode,
                    'hash': phash
                }
        except Exception as e:
            print(f"Error getting metadata for {file_path}: {e}")
            return {}
    
    def close(self):
        """Close HTTP client."""
        self.client.close()
