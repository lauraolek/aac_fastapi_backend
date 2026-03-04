import asyncio
import logging
from typing import List, Optional, Tuple
from fastapi import Depends, UploadFile, HTTPException, status
from sqlalchemy.exc import NoResultFound
from uuid import UUID as PyUUID

from db.image_word_repository import ImageWordRepository
from models.schemas import ImageWord, ImageWordCreate
from services.image_storage_service import ImageStorageService, get_storage_service

logger = logging.getLogger(__name__)

class ImageWordService:
    """
    Service class for managing ImageWord entities.
    Handles business logic, asset lifecycle, and repository interaction.
    """

    def __init__(
        self, 
        repository: ImageWordRepository, 
        storage_service: ImageStorageService = Depends(get_storage_service)
    ):
        self.repo = repository
        self.storage_service = storage_service

    async def find_by_category_id(self, user_id: PyUUID, category_id: int) -> List[ImageWord]:
        """
        Retrieves all image words for a given category.
        """
        try:
            word_dicts = await self.repo.get_image_words_by_category(user_id, category_id)
            return [ImageWord.model_validate(wd) for wd in word_dicts]
        except Exception as e:
            logger.error(f"Error fetching words for category {category_id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                detail="Failed to retrieve words."
            )

    async def find_by_id(self, user_id: PyUUID, id: int) -> ImageWord:
        """
        Retrieves a single image word by ID, ensuring user authorization.
        """
        word_data = await self.repo.find_image_word_by_id(user_id, id)
        if not word_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, 
                detail=f"ImageWord not found with ID: {id}"
            )
        
        return ImageWord.model_validate(word_data)
    
    async def save_batch(
        self, 
        user_id: PyUUID, 
        items: List[Tuple[int, str, UploadFile]]
    ) -> List[ImageWord]:
        """
        Processes multiple images in parallel and commits them in a single DB transaction.
        'items' is a list of (category_id, word_text, image_file)
        """
        uploaded_urls = []
        
        try:
            # 1. Parallel Uploads to Cloudflare
            upload_tasks = [self.storage_service.upload(item[2]) for item in items]
            uploaded_urls = await asyncio.gather(*upload_tasks)

            # 2. Prepare Database Objects
            word_data_list = []
            for i, (category_id, word_text, _) in enumerate(items):
                word_data_list.append(
                    ImageWordCreate(
                        category_id=category_id,
                        word=word_text,
                        image_url=uploaded_urls[i]
                    )
                )
            
            # 3. Batch DB Save
            saved_records = await self.repo.save_many(user_id, word_data_list)

            await self.repo.session.commit()
            
            return [ImageWord.model_validate(r) for r in saved_records]

        except Exception as e:
            # Cleanup: Delete ALL successfully uploaded images from Cloudflare on any failure
            if uploaded_urls:
                cleanup_tasks = [self.storage_service.delete(url) for url in uploaded_urls if url]
                await asyncio.gather(*cleanup_tasks, return_exceptions=True)
            
            await self.repo.session.rollback()
            logger.error(f"Batch seeding failed: {e}")
            
            if isinstance(e, NoResultFound):
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
            
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                detail="Failed to seed user images."
            )
        
    async def save(
        self, 
        user_id: PyUUID, 
        category_id: int, 
        word_text: str, 
        image_file: UploadFile
    ) -> ImageWord:
        results = await self.save_batch(user_id, [(category_id, word_text, image_file)])
        return results[0]

    async def update(
        self, 
        user_id: PyUUID, 
        image_word_id: int, 
        word_text: str, 
        category_id: int, 
        image_file: Optional[UploadFile]
    ) -> ImageWord:
        """
        Updates an ImageWord, replacing the image in storage only after successful DB update.
        """
        # 1. Verify existence and ownership
        current_word = await self.find_by_id(user_id, image_word_id)
        old_image_url = current_word.image_url
        new_image_url = None
        
        # Checking for file content, not just presence of object
        has_new_image = image_file is not None and image_file.filename != ""

        try:
            # 2. Upload new asset if provided
            if has_new_image:
                new_image_url = await self.storage_service.upload(image_file) # type: ignore

            # 3. Save to DB
            update_data = ImageWordCreate(            
                category_id=category_id,
                word=word_text,
                image_url=new_image_url if has_new_image else old_image_url
            )

            updated_data = await self.repo.save(user_id, update_data, image_word_id)
            await self.repo.session.commit()
            
            # 4. Cleanup old image only after successful commit
            if has_new_image and old_image_url:
                await self.storage_service.delete(str(old_image_url))
                
            return ImageWord.model_validate(updated_data)

        except Exception as e:
            # Cleanup orphaned new upload
            if new_image_url:
                 await self.storage_service.delete(new_image_url)
            
            await self.repo.session.rollback()
            logger.error(f"Error updating image word {image_word_id}: {e}")
            
            if isinstance(e, NoResultFound):
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                detail="Failed to update image word."
            )

    async def delete_by_id(self, user_id: PyUUID, id: int):
        """
        Deletes the DB record first, then cleans up storage.
        """
        # 1. Authorize (find_by_id handles the logic)
        image_word = await self.find_by_id(user_id, id)

        try:
            # 2. Delete the record
            await self.repo.delete_image_word_by_id(user_id, id)
            await self.repo.session.commit()
        except Exception as e:
            await self.repo.session.rollback()
            logger.error(f"Delete failed for word {id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Database deletion failed."
            )
        
        # 3. Storage Cleanup (Post-Commit)
        if image_word.image_url:
            try:
                await self.storage_service.delete(str(image_word.image_url))
            except Exception as e:
                # Log but don't fail the request since the DB is already updated
                # TODO In a pro system, you'd log this for a background cleanup task.
                logger.warning(f"Failed to cleanup image {image_word.image_url} for word {id}: {e}")