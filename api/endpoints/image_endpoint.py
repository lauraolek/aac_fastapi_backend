import os
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse
from config import settings

from services.image_storage_service import ImageStorageService, get_storage_service

UPLOAD_DIR = settings.upload_dir

router = APIRouter(prefix="", tags=["images"])

@router.get("/{filename}")
async def get_image_url(
    filename: str,
    expires_in: int = 3600,
    storage: ImageStorageService = Depends(get_storage_service)
):
    """
    Smart resolver: 
    1. Tries to get a Cloud/Remote URL from the storage service.
    2. If remote storage fails or is disabled, checks local disk.
    3. Returns a local '/serve/' path if found locally.
    """
    # 1. Try Cloud Storage first
    try:
        url = await storage.get_url(filename, expires_in=expires_in)
        if url and url.startswith("http"):
            return {"url": url, "source": "cloud"}
    except Exception:
        # Fallback to local check if cloud fails or isn't configured
        pass

    # 2. Check local filesystem fallback
    file_path = os.path.join(UPLOAD_DIR, filename)
    if os.path.exists(file_path):
        # Return the relative path to our serve endpoint
        return {
            "url": f"/api/images/serve/{filename}",
            "source": "local"
        }

    raise HTTPException(status_code=404, detail="Image not found in any storage provider")

@router.get("/serve/{filename}")
async def serve_local_image(filename: str):
    """
    Serves images from the local filesystem. 
    This is used as the fallback target for get_image_url.
    """
    print("serve")
    file_path = os.path.join(UPLOAD_DIR, filename)
    
    # Security: Prevent directory traversal (e.g., filename="../../etc/passwd")
    abs_upload_dir = os.path.abspath(UPLOAD_DIR)
    abs_file_path = os.path.abspath(file_path)
    
    if not abs_file_path.startswith(abs_upload_dir):
        raise HTTPException(status_code=403, detail="Invalid file path")

    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")
        
    return FileResponse(file_path)