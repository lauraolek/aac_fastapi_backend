import logging
from typing import List, Optional
from fastapi import HTTPException, status
from sqlalchemy import delete, select, update
from uuid import UUID as PyUUID
from sqlalchemy.exc import NoResultFound, SQLAlchemyError

from db.base_repository import BaseRepository
from db.models import CategoryModel, ImageWordModel, ProfileModel
from models.schemas import ImageWordCreate

logger = logging.getLogger(__name__)

class ImageWordRepository(BaseRepository[ImageWordModel]):
    async def find_image_word_by_id(self, user_id: PyUUID, word_id: int) -> Optional[ImageWordModel]:
        stmt = (
            select(ImageWordModel)
            .join(CategoryModel).join(ProfileModel)
            .where(ImageWordModel.id == word_id, ProfileModel.user_id == user_id)
        )
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()
    

    async def get_image_words_by_category(self, user_id: PyUUID, category_id: int) -> List[ImageWordModel]:
        stmt = (
            select(ImageWordModel)
            .join(CategoryModel)
            .join(ProfileModel)
            .where(
                ImageWordModel.category_id == category_id,
                ProfileModel.user_id == user_id
            )
        )
        result = await self.session.execute(stmt)
        return list(result.scalars().all())
    

    async def save_many(
        self, 
        user_id: PyUUID, 
        word_dtos: List[ImageWordCreate]
    ) -> List[ImageWordModel]:
        """
        The core engine for bulk saving. 
        Verified category ownership for the entire batch.
        """
        if not word_dtos:
            return []

        try:
            # 1. Verify Category Ownership for all unique categories in the batch
            category_ids = list(set(dto.category_id for dto in word_dtos))
            cat_check = (
                select(CategoryModel.id)
                .join(ProfileModel)
                .where(
                    CategoryModel.id.in_(category_ids),
                    ProfileModel.user_id == user_id
                )
            )
            result = await self.session.execute(cat_check)
            owned_categories = result.scalars().all()

            if len(owned_categories) != len(category_ids):
                raise NoResultFound("Unauthorized or missing category access for batch operation.")

            # 2. Batch instantiate
            words = [
                ImageWordModel(
                    category_id=dto.category_id,
                    word=dto.word,
                    word_osastav=dto.word_osastav,
                    image_url=dto.image_url
                )
                for dto in word_dtos
            ]
            
            self.session.add_all(words)
            
            # 3. Synchronize
            await self.session.flush()
            
            # Note: We return the list. Objects are attached to session with IDs.
            return words

        except NoResultFound:
            raise
        except SQLAlchemyError as e:
            await self.session.rollback()
            logger.error(f"SQLAlchemy Batch Error: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Database error during batch image_word operation."
            )
    

    async def save(
        self, 
        user_id: PyUUID, 
        word_dto: ImageWordCreate, 
        word_id: Optional[int] = None
    ) -> ImageWordModel:
        """
        Unified save method. 
        If word_id exists: Performs an Update.
        If word_id is None: Delegates to save_many for Creation.
        """
        # --- CASE 1: UPDATE ---
        if word_id:
            try:
                stmt = (
                    update(ImageWordModel)
                    .where(
                        ImageWordModel.id == word_id,
                        ImageWordModel.category_id.in_(
                            select(CategoryModel.id)
                            .join(ProfileModel)
                            .where(ProfileModel.user_id == user_id)
                        )
                    )
                    .values(word=word_dto.word, word_osastav=word_dto.word_osastav, image_url=word_dto.image_url)
                    .returning(ImageWordModel)
                )
                result = await self.session.execute(stmt)
                word = result.scalars().first()
                
                if not word:
                    raise NoResultFound(f"Word {word_id} not found or unauthorized.")
                
                await self.session.flush()
                await self.session.refresh(word)
                return word
            except NoResultFound:
                raise
            except Exception as e:
                await self.session.rollback()
                logger.error(f"Update error: {e}")
                raise HTTPException(status_code=500, detail="Update failed.")

        # --- CASE 2: CREATE (Delegates to save_many) ---
        results = await self.save_many(user_id, [word_dto])
        word = results[0]
        
        # Single items usually require a refresh to load DB defaults for the UI
        await self.session.refresh(word)
        return word
    
    async def delete_image_word_by_id(self, user_id: PyUUID, word_id: int) -> bool:
        user_allowed_categories = (
            select(CategoryModel.id)
            .join(ProfileModel, CategoryModel.profile_id == ProfileModel.id)
            .where(ProfileModel.user_id == user_id)
        )
        stmt = (
            delete(ImageWordModel)
            .where(ImageWordModel.id == word_id, ImageWordModel.category_id.in_(user_allowed_categories))
            .returning(ImageWordModel.id)
        )
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none() is not None