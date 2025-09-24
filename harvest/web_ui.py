"""
FastAPI web UI for PixVault image management.
Provides image listing, duplicate detection, and approval workflows.
"""

import os
import base64
from pathlib import Path
from typing import List, Dict, Any, Optional
from fastapi import FastAPI, HTTPException, Request, Form, File, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
import uvicorn
from PIL import Image as PILImage
import io

from .db import Database, find_similar_phash
from .dedupe import ImageDeduplicator
from .enhanced_dedupe import EnhancedDeduplicator
from .config import load_config
from .utils.logger import get_logger

logger = get_logger("harvest.web_ui")

# Initialize FastAPI app
app = FastAPI(
    title="PixVault Web UI",
    description="Image management and duplicate detection interface",
    version="1.0.0"
)

# Mount static files and templates
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# Global database instance
database = None
deduplicator = None
enhanced_deduplicator = None


class ImageResponse(BaseModel):
    """Response model for image data."""
    id: str
    url: str
    filename: str
    label: str
    status: str
    width: int
    height: int
    format: str
    downloaded_at: str
    thumbnail: Optional[str] = None
    is_duplicate: bool = False
    duplicate_group: Optional[List[str]] = None


class ApprovalRequest(BaseModel):
    """Request model for image approval."""
    image_id: str
    action: str  # "approve" or "reject"


class DuplicateGroup(BaseModel):
    """Model for duplicate groups."""
    group_id: str
    images: List[ImageResponse]
    primary_image: ImageResponse
    duplicate_count: int


@app.on_event("startup")
async def startup_event():
    """Initialize database and deduplicator on startup."""
    global database, deduplicator, enhanced_deduplicator
    
    try:
        # Load configuration
        config = load_config()
        
        # Initialize database
        db_path = config.get('database', {}).get('path', 'db/images.db')
        database = Database(db_path)
        
        # Initialize deduplicators
        deduplicator = ImageDeduplicator(db_path)
        enhanced_deduplicator = EnhancedDeduplicator(
            db_path=db_path,
            semantic_index_path="db/semantic_index"
        )
        
        logger.info("Web UI initialized successfully")
        
    except Exception as e:
        logger.error(f"Failed to initialize web UI: {e}")
        raise


def create_thumbnail(image_path: str, size: tuple = (150, 150)) -> Optional[str]:
    """
    Create thumbnail for image and return as base64 string.
    
    Args:
        image_path: Path to image file
        size: Thumbnail size (width, height)
        
    Returns:
        Base64 encoded thumbnail or None if failed
    """
    try:
        if not os.path.exists(image_path):
            return None
        
        with PILImage.open(image_path) as img:
            # Convert to RGB if necessary
            if img.mode in ('RGBA', 'LA', 'P'):
                img = img.convert('RGB')
            
            # Create thumbnail
            img.thumbnail(size, PILImage.Resampling.LANCZOS)
            
            # Convert to base64
            buffer = io.BytesIO()
            img.save(buffer, format='JPEG', quality=85)
            buffer.seek(0)
            
            # Encode as base64
            thumbnail_data = base64.b64encode(buffer.getvalue()).decode('utf-8')
            return f"data:image/jpeg;base64,{thumbnail_data}"
            
    except Exception as e:
        logger.error(f"Failed to create thumbnail for {image_path}: {e}")
        return None


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Main page with image listing."""
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/images", response_model=List[ImageResponse])
async def get_images(
    status: Optional[str] = None,
    label: Optional[str] = None,
    limit: int = 50,
    offset: int = 0
):
    """Get list of images with optional filtering."""
    try:
        # Get images from database
        if status:
            images = database.get_images_by_status(status)
        else:
            images = database.get_all_images()
        
        # Filter by label if specified
        if label:
            images = [img for img in images if img.get('label') == label]
        
        # Apply pagination
        total_images = len(images)
        images = images[offset:offset + limit]
        
        # Convert to response format
        result = []
        for img in images:
            # Create thumbnail
            thumbnail = None
            if img.get('filename'):
                image_path = os.path.join("storage", img['filename'])
                thumbnail = create_thumbnail(image_path)
            
            # Check for duplicates
            is_duplicate = False
            duplicate_group = None
            
            if img.get('phash'):
                similar_images = find_similar_phash(img['phash'], threshold=5, db_path=database.db_path)
                if len(similar_images) > 1:  # More than just the current image
                    is_duplicate = True
                    duplicate_group = [sim['id'] for sim in similar_images if sim['id'] != img['id']]
            
            result.append(ImageResponse(
                id=img['id'],
                url=img['url'],
                filename=img['filename'],
                label=img.get('label', ''),
                status=img.get('status', 'unknown'),
                width=img.get('width', 0),
                height=img.get('height', 0),
                format=img.get('format', ''),
                downloaded_at=img.get('downloaded_at', ''),
                thumbnail=thumbnail,
                is_duplicate=is_duplicate,
                duplicate_group=duplicate_group
            ))
        
        return result
        
    except Exception as e:
        logger.error(f"Failed to get images: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/images/{image_id}", response_model=ImageResponse)
async def get_image(image_id: str):
    """Get specific image details."""
    try:
        # Get image from database
        with database.conn.cursor() as cursor:
            cursor.execute("SELECT * FROM images WHERE id = ?", (image_id,))
            img = cursor.fetchone()
            
        if not img:
            raise HTTPException(status_code=404, detail="Image not found")
        
        # Create thumbnail
        thumbnail = None
        if img.get('filename'):
            image_path = os.path.join("storage", img['filename'])
            thumbnail = create_thumbnail(image_path)
        
        # Check for duplicates
        is_duplicate = False
        duplicate_group = None
        
        if img.get('phash'):
            similar_images = find_similar_phash(img['phash'], threshold=5, db_path=database.db_path)
            if len(similar_images) > 1:
                is_duplicate = True
                duplicate_group = [sim['id'] for sim in similar_images if sim['id'] != img['id']]
        
        return ImageResponse(
            id=img['id'],
            url=img['url'],
            filename=img['filename'],
            label=img.get('label', ''),
            status=img.get('status', 'unknown'),
            width=img.get('width', 0),
            height=img.get('height', 0),
            format=img.get('format', ''),
            downloaded_at=img.get('downloaded_at', ''),
            thumbnail=thumbnail,
            is_duplicate=is_duplicate,
            duplicate_group=duplicate_group
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get image {image_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/duplicates", response_model=List[DuplicateGroup])
async def get_duplicates():
    """Get all duplicate groups."""
    try:
        # Get duplicate groups using enhanced deduplicator
        duplicate_groups = enhanced_deduplicator.get_duplicate_groups(
            use_phash=True,
            use_semantic=True
        )
        
        result = []
        for i, group in enumerate(duplicate_groups):
            # Convert to response format
            images = []
            for img in group:
                # Create thumbnail
                thumbnail = None
                if img.get('filename'):
                    image_path = os.path.join("storage", img['filename'])
                    thumbnail = create_thumbnail(image_path)
                
                images.append(ImageResponse(
                    id=img['id'],
                    url=img['url'],
                    filename=img['filename'],
                    label=img.get('label', ''),
                    status=img.get('status', 'unknown'),
                    width=img.get('width', 0),
                    height=img.get('height', 0),
                    format=img.get('format', ''),
                    downloaded_at=img.get('downloaded_at', ''),
                    thumbnail=thumbnail,
                    is_duplicate=True
                ))
            
            # Primary image is the first one (oldest)
            primary_image = images[0] if images else None
            
            result.append(DuplicateGroup(
                group_id=f"group_{i}",
                images=images,
                primary_image=primary_image,
                duplicate_count=len(images) - 1
            ))
        
        return result
        
    except Exception as e:
        logger.error(f"Failed to get duplicates: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/images/{image_id}/approve")
async def approve_image(image_id: str):
    """Approve an image (mark as approved)."""
    try:
        # Update image status
        database.update_image_status(image_id, "approved", "Approved by user")
        
        logger.info(f"Image {image_id} approved")
        return {"message": "Image approved successfully", "image_id": image_id}
        
    except Exception as e:
        logger.error(f"Failed to approve image {image_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/images/{image_id}/reject")
async def reject_image(image_id: str):
    """Reject an image (mark as rejected)."""
    try:
        # Update image status
        database.update_image_status(image_id, "rejected", "Rejected by user")
        
        logger.info(f"Image {image_id} rejected")
        return {"message": "Image rejected successfully", "image_id": image_id}
        
    except Exception as e:
        logger.error(f"Failed to reject image {image_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/images/{image_id}")
async def delete_image(image_id: str):
    """Delete an image and its file."""
    try:
        # Get image details
        with database.conn.cursor() as cursor:
            cursor.execute("SELECT * FROM images WHERE id = ?", (image_id,))
            img = cursor.fetchone()
            
        if not img:
            raise HTTPException(status_code=404, detail="Image not found")
        
        # Delete file if exists
        if img.get('filename'):
            file_path = os.path.join("storage", img['filename'])
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Deleted file: {file_path}")
        
        # Delete from database
        with database.conn.cursor() as cursor:
            cursor.execute("DELETE FROM images WHERE id = ?", (image_id,))
            database.conn.commit()
        
        logger.info(f"Image {image_id} deleted")
        return {"message": "Image deleted successfully", "image_id": image_id}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete image {image_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/stats")
async def get_stats():
    """Get database statistics."""
    try:
        all_images = database.get_all_images()
        
        stats = {
            "total_images": len(all_images),
            "by_status": {},
            "by_label": {},
            "duplicates": 0
        }
        
        # Count by status
        for img in all_images:
            status = img.get('status', 'unknown')
            stats["by_status"][status] = stats["by_status"].get(status, 0) + 1
            
            label = img.get('label', 'unlabeled')
            stats["by_label"][label] = stats["by_label"].get(label, 0) + 1
        
        # Count duplicates
        duplicate_groups = enhanced_deduplicator.get_duplicate_groups()
        stats["duplicates"] = len(duplicate_groups)
        
        return stats
        
    except Exception as e:
        logger.error(f"Failed to get stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/images/{image_id}/file")
async def get_image_file(image_id: str):
    """Get image file for download."""
    try:
        # Get image details
        with database.conn.cursor() as cursor:
            cursor.execute("SELECT * FROM images WHERE id = ?", (image_id,))
            img = cursor.fetchone()
            
        if not img:
            raise HTTPException(status_code=404, detail="Image not found")
        
        if not img.get('filename'):
            raise HTTPException(status_code=404, detail="Image file not found")
        
        file_path = os.path.join("storage", img['filename'])
        if not os.path.exists(file_path):
            raise HTTPException(status_code=404, detail="Image file not found on disk")
        
        return FileResponse(
            file_path,
            media_type='image/jpeg',
            filename=img['filename']
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get image file {image_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def run_web_ui(host: str = "127.0.0.1", port: int = 8000, reload: bool = False):
    """Run the web UI server."""
    uvicorn.run(
        "harvest.web_ui:app",
        host=host,
        port=port,
        reload=reload,
        log_level="info"
    )


if __name__ == "__main__":
    run_web_ui()
