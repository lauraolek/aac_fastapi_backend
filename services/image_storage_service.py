import asyncio
import io
import logging
import os
import time
from typing import Any, Dict, List, Tuple
import uuid
from PIL import Image, ImageOps
import pillow_heif
import aiofiles
from abc import ABC, abstractmethod
from fastapi import UploadFile
import aiobotocore
import aiobotocore.session
from config import settings

# Configuration via environment variables
STORAGE_TYPE = settings.storage_type  # Options: LOCAL, CLOUDFLARE
R2_BUCKET_NAME = settings.r2_bucket_name
R2_ACCOUNT_ID = settings.r2_account_id
R2_ACCESS_KEY = settings.r2_access_key
R2_SECRET_KEY = settings.r2_secret_key
R2_PUBLIC_URL = settings.r2_public_url # e.g., https://pub-xyz.r2.dev

logger = logging.getLogger(__name__)

pillow_heif.register_heif_opener()

class ImageStorageService(ABC):
    @abstractmethod
    async def upload(self, file: UploadFile, rotation_turns: int = 0, original_filename: str = "") -> str:
        pass

    @abstractmethod
    async def delete(self, filename: str) -> bool:
        return False
    
    @abstractmethod
    async def get_url(self, filename: str, expires_in: int = 3600) -> str:
        """Returns a URL for the frontend. Presigned if cloud, local path if not."""
        pass

    @abstractmethod
    async def upload_batch(self, items: List[Tuple[Any, Any, UploadFile, int]]) -> List[Tuple[Any, Any, str]]:
        pass

    @abstractmethod
    async def delete_batch(self, filenames: List[str]) -> Dict[str, bool]:
        pass

    async def _process_image_to_jpeg(self, file: UploadFile, rotation_turns: int = 0) -> Tuple[bytes, str]:
        """
        Reads UploadFile, converts to JPEG, bakes in rotation adjustments, 
        cleans up conflict EXIF metadata tags, and returns (bytes, ".jpg")
        """
        await file.seek(0)
        original_content = await file.read()
        
        # Open the image using Pillow
        # (register_heif_opener allows Image.open to handle HEIC)
        img = Image.open(io.BytesIO(original_content))

        # TRANSPARENCY HANDLING
        if img.mode != "RGBA":
            img = img.convert("RGBA")

        # Create a background canvas 
        # (Using pure white (255, 255, 255) to match the card background)
        background = Image.new("RGBA", img.size, settings.image_background_color)
        
        # Composite the image over the white background
        # Alpha_composite is cleaner than 'paste' for transparent images
        img = Image.alpha_composite(background, img)
        
        # Drop the Alpha channel now that we have a solid background
        img = img.convert("RGB")

        # Strip layout conflicts out of original mobile camera orientation tags
        try:
            img = ImageOps.exif_transpose(img)
        except Exception:
            pass 

        # Bake custom user turn rotation into structural pixel canvas layers
        if rotation_turns > 0:
            # 1 turn = 90 clockwise (-90 in Pillow), 2 = 180, 3 = 270 (-270 in Pillow)
            angle = rotation_turns * -90
            img = img.rotate(angle, expand=True) # expand=True prevents corners from being cut off

        max_size = 1600 
        if max(img.size) > max_size:
            img.thumbnail((max_size, max_size), Image.Resampling.LANCZOS)
        
        output = io.BytesIO()
        # Saving without passing original 'exif' parameter cleans/overwrites EXIF completely
        img.save(output, format="JPEG", quality=60, optimize=True)
        jpeg_bytes = output.getvalue()
        
        return jpeg_bytes, ".jpg"

class LocalStorageService(ImageStorageService):
    """Saves to a local directory - Best for development"""
    def __init__(self, upload_dir: str = "uploads"):
        self.upload_dir = upload_dir
        if not os.path.exists(upload_dir):
            os.makedirs(upload_dir)

    async def upload_batch(self, files: List[Tuple]) -> List[Tuple[Any, Any, str]]:
        """Batch upload for local storage with safe element fallback unpacking."""
        async def _upload_one(tag, meta, file: UploadFile, rotation_turns: int = 0):            
            content, extension = await self._process_image_to_jpeg(file, rotation_turns)
            unique_name = f"{uuid.uuid4()}{extension}"
            filepath = os.path.join(self.upload_dir, unique_name)
            async with aiofiles.open(filepath, 'wb') as out_file:
                await out_file.write(content)
            return tag, meta, unique_name

        tasks = []
        for item in files:
            # Fallback handling: checks if seed data sent 3 items or 4
            t, m, f = item[0], item[1], item[2]
            r = item[3] if len(item) > 3 else 0
            tasks.append(_upload_one(t, m, f, r))

        return await asyncio.gather(*tasks)

    async def upload(self, file: UploadFile, rotation_turns: int = 0, original_filename: str = "") -> str:
        source_name = original_filename or file.filename
        if not source_name:
            raise ValueError("Not a valid filename.")

        content, extension = await self._process_image_to_jpeg(file, rotation_turns)
        filename = f"{uuid.uuid4()}{extension}"
        filepath = os.path.join(self.upload_dir, filename)
        async with aiofiles.open(filepath, 'wb') as out_file:
            await out_file.write(content)
        return filename
    
    async def delete_batch(self, filenames: List[str]) -> Dict[str, bool]:
        """Batch delete for local storage."""
        results = {}
        for fname in filenames:
            safe_name = os.path.basename(fname)
            path = os.path.join(self.upload_dir, safe_name)
            try:
                if os.path.exists(path):
                    os.remove(path)
                    results[fname] = True
                else:
                    results[fname] = False
            except Exception:
                results[fname] = False
        return results
    
    async def delete(self, filename: str) -> bool:
        """
        Deletes a file from the local storage directory.
        Returns True if deleted, False if file not found.
        """
        # Security check: prevent directory traversal by taking only the basename
        safe_filename = os.path.basename(filename)
        filepath = os.path.join(self.upload_dir, safe_filename)

        try:
            if os.path.exists(filepath):
                os.remove(filepath)
                return True
            return False
        except Exception as e:
            logger.error(f"Error deleting file {filepath}: {e}")
            return False
        
    async def get_url(self, filename: str, expires_in: int = 3600) -> str:
        # In local mode, we point back to our own API endpoint
        return f"/shared/{filename}"

class CloudflareR2Service(ImageStorageService):
    """
    Saves to Cloudflare R2 using aiobotocore.
    """
    
    def __init__(self):
        self.endpoint_url = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
        # Create a single session instance for the service
        self.session = aiobotocore.session.get_session()
        self._url_cache = {} # In-memory cache: { "path": (url, expiry_timestamp) }

    async def upload_batch(self, items: List[Tuple]) -> List[Tuple[Any, Any, str]]:
        """
        Uploads multiple files using a SINGLE client session to avoid handshake overhead.
        Safely falls back if seed data missing rotation index.
        """
        uploaded_results = []
        
        async with self.session.create_client(
            's3',
            endpoint_url=self.endpoint_url,
            aws_access_key_id=R2_ACCESS_KEY,
            aws_secret_access_key=R2_SECRET_KEY
        ) as client:
            s3_client: Any = client

            async def _single_upload(type_tag, metadata, file: UploadFile, rotation_turns: int = 0):
                content, extension = await self._process_image_to_jpeg(file, rotation_turns)
                unique_name = f"{uuid.uuid4()}{extension}"
                
                await s3_client.put_object(
                    Bucket=R2_BUCKET_NAME,
                    Key=unique_name,
                    Body=content,
                    ContentType="image/jpeg"
                )
                return type_tag, metadata, unique_name

            # Safe mapping array list comprehension to unpack either 3 or 4 item variations
            tasks = []
            for item in items:
                t, m, f = item[0], item[1], item[2]
                r = item[3] if len(item) > 3 else 0
                tasks.append(_single_upload(t, m, f, r))

            uploaded_results = await asyncio.gather(*tasks)
            
        return uploaded_results

    async def upload(self, file: UploadFile, rotation_turns: int = 0, original_filename: str = "") -> str:
        """
        Uploads a file to Cloudflare R2.
        """
        async with self.session.create_client(
            's3',
            endpoint_url=self.endpoint_url,
            aws_access_key_id=R2_ACCESS_KEY,
            aws_secret_access_key=R2_SECRET_KEY
        ) as client:
            s3_client: Any = client

            content, extension = await self._process_image_to_jpeg(file, rotation_turns)
            filename = f"{uuid.uuid4()}{extension}"
            
            await s3_client.put_object(
                Bucket=R2_BUCKET_NAME,
                Key=filename,
                Body=content,
                ContentType="image/jpeg"
            )
        return filename
    
    async def delete_batch(self, filenames: List[str]) -> Dict[str, bool]:
        """Bulk delete from R2."""
        results = {}
        async with self.session.create_client(
            's3', endpoint_url=self.endpoint_url,
            aws_access_key_id=R2_ACCESS_KEY, aws_secret_access_key=R2_SECRET_KEY
        ) as client:
            s3_client: Any = client
            delete_list = [{'Key': os.path.basename(f)} for f in filenames]
            try:
                await s3_client.delete_objects(
                    Bucket=R2_BUCKET_NAME,
                    Delete={'Objects': delete_list}
                )
                return {f: True for f in filenames}
            except Exception as e:
                logger.error(f"Bulk delete failed: {e}")
                return {f: False for f in filenames}

    async def delete(self, filename: str) -> bool:
        """
        Deletes an object from R2.
        Returns True if the operation was successful, False otherwise.
        """
        # Sanitize the filename to prevent path traversal/directory escaping
        # even in object storage, this ensures the key is just the filename.
        safe_key = os.path.basename(filename)

        try:
            async with self.session.create_client(
                's3',
                endpoint_url=self.endpoint_url,
                aws_access_key_id=R2_ACCESS_KEY,
                aws_secret_access_key=R2_SECRET_KEY
            ) as client:
                # Type hinting the client as Any prevents Pylance from 
                # incorrectly assuming methods like delete_object are NoReturn.
                s3_client: Any = client

                await s3_client.delete_object(
                    Bucket=R2_BUCKET_NAME, 
                    Key=safe_key
                )
                return True
        except Exception as e:
            logger.error(f"Error deleting {safe_key} from R2: {e}")
            return False
        
    async def get_url(self, filename: str, expires_in: int = 3600) -> str:
        """Generates a Presigned URL for direct download from R2"""
        if filename in self._url_cache:
            url, expiry = self._url_cache[filename]
            if expiry > time.time() + 3000:
                return url


        async with self.session.create_client(
            's3',
            endpoint_url=self.endpoint_url,
            aws_access_key_id=R2_ACCESS_KEY,
            aws_secret_access_key=R2_SECRET_KEY,
            region_name="auto" # R2 requires a region, 'auto' is standard for R2
        ) as client:
            s3_client: Any = client

            url = await s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': R2_BUCKET_NAME, 'Key': filename},
                ExpiresIn=expires_in
            )

            self._url_cache[filename] = (url, time.time() + expires_in)
            return url


def get_storage_service() -> ImageStorageService:
    """Factory to switch between local and prod"""
    logger.debug(f"Storage Type detected as {STORAGE_TYPE}")
    if STORAGE_TYPE == "CLOUDFLARE":
        return CloudflareR2Service()
    return LocalStorageService()