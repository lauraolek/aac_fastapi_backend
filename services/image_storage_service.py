import asyncio
import logging
import os
import pathlib
import time
from typing import Any, Dict, List, Tuple
import uuid
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

class ImageStorageService(ABC):
    @abstractmethod
    async def upload(self, file: UploadFile, original_filename: str = "") -> str:
        pass

    @abstractmethod
    async def delete(self, filename: str) -> bool:
        return False
    
    @abstractmethod
    async def get_url(self, filename: str, expires_in: int = 3600) -> str:
        """Returns a URL for the frontend. Presigned if cloud, local path if not."""
        pass

    @abstractmethod
    async def upload_batch(self, items: List[Tuple[Any, Any, UploadFile]]) -> List[Tuple[Any, Any, str]]:
        pass

    @abstractmethod
    async def delete_batch(self, filenames: List[str]) -> Dict[str, bool]:
        pass

class LocalStorageService(ImageStorageService):
    """Saves to a local directory - Best for development"""
    def __init__(self, upload_dir: str = "uploads"):
        self.upload_dir = upload_dir
        if not os.path.exists(upload_dir):
            os.makedirs(upload_dir)

    async def upload_batch(self, files: List[Tuple[Any, Any, UploadFile]]) -> List[Tuple[Any, Any, str]]:
        """Batch upload for local storage."""
        async def _upload_one(tag, meta, file: UploadFile):
            source_name = file.filename or "unknown.bin"
            extension = pathlib.Path(source_name).suffix.lower()
            unique_name = f"{uuid.uuid4()}{extension}"
            filepath = os.path.join(self.upload_dir, unique_name)
            
            await file.seek(0)
            content = await file.read()
            async with aiofiles.open(filepath, 'wb') as out_file:
                await out_file.write(content)
            return tag, meta, unique_name

        tasks = [_upload_one(t, m, f) for t, m, f in files]
        return await asyncio.gather(*tasks)

    async def upload(self, file: UploadFile, original_filename: str = "") -> str:
        source_name = original_filename or file.filename
        if not source_name:
            raise ValueError("Not a valid filename: No filename provided or found on object.")

        extension = pathlib.Path(source_name).suffix.lower()
        filename = f"{uuid.uuid4()}{extension}"
        filepath = os.path.join(self.upload_dir, filename)
        async with aiofiles.open(filepath, 'wb') as out_file:
            await file.seek(0)
            content = await file.read()
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
            print(f"Error deleting file {filepath}: {e}")
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

    async def upload_batch(self, items: List[Tuple[Any, Any, UploadFile]]) -> List[Tuple[Any, Any, str]]:
        """
        Uploads multiple files using a SINGLE client session to avoid handshake overhead.
        'items' is a list of (type_tag, metadata, upload_file)
        """
        uploaded_results = []
        
        async with self.session.create_client(
            's3',
            endpoint_url=self.endpoint_url,
            aws_access_key_id=R2_ACCESS_KEY,
            aws_secret_access_key=R2_SECRET_KEY
        ) as client:
            s3_client: Any = client

            async def _single_upload(type_tag, metadata, file: UploadFile):
                await file.seek(0)
                content = await file.read()
                
                source_name = file.filename or "unknown.bin"
                extension = pathlib.Path(source_name).suffix.lower()
                unique_name = f"{uuid.uuid4()}{extension}"
                
                await s3_client.put_object(
                    Bucket=R2_BUCKET_NAME,
                    Key=unique_name,
                    Body=content,
                    ContentType=file.content_type
                )
                return type_tag, metadata, unique_name

            # Execute all uploads within the SAME client context
            tasks = [_single_upload(t, m, f) for t, m, f in items]
            uploaded_results = await asyncio.gather(*tasks)
            
        return uploaded_results

    async def upload(self, file: UploadFile, original_filename: str = "") -> str:
        """
        Uploads a file to Cloudflare R2.
        """
        async with self.session.create_client(
            's3',
            endpoint_url=self.endpoint_url,
            aws_access_key_id=R2_ACCESS_KEY,
            aws_secret_access_key=R2_SECRET_KEY
        ) as client:
            # Type hinting the client as Any prevents Pylance from 
            # incorrectly assuming methods like put_object are NoReturn.
            s3_client: Any = client

            await file.seek(0)
            content = await file.read()

            extension = pathlib.Path(original_filename).suffix.lower()
            filename = f"{uuid.uuid4()}{extension}"
            
            # This will now pass type checking while remaining awaitable at runtime
            await s3_client.put_object(
                Bucket=R2_BUCKET_NAME,
                Key=filename,
                Body=content,
                ContentType=file.content_type
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
            print(f"Error deleting {safe_key} from R2: {e}")
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