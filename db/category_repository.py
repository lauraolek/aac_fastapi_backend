import logging
from typing import Any, Dict, List, Optional
from fastapi import HTTPException, status
from sqlalchemy import and_, delete, select
from uuid import UUID as PyUUID
from sqlalchemy.exc import NoResultFound, SQLAlchemyError
from sqlalchemy.orm import selectinload

from db.base_repository import BaseRepository
from db.models import CategoryModel, ProfileModel
from models.schemas import CategoryCreate

logger = logging.getLogger(__name__)

class CategoryRepository(BaseRepository[CategoryModel]):
    async def find_category_by_id(self, user_id: PyUUID, category_id: int) -> Optional[CategoryModel]:
        """Fetch category and verify it belongs to one of the user's profiles."""
        query = (
            select(CategoryModel)
            .join(ProfileModel, CategoryModel.profile_id == ProfileModel.id)
            .filter(and_(CategoryModel.id == category_id, ProfileModel.user_id == user_id))
        )
        result = await self.session.execute(query)
        return result.scalar_one_or_none()
    
    
    async def find_by_profile(self, user_id: PyUUID, profile_id: int) -> List[CategoryModel]:
        stmt = (
            select(CategoryModel)
            .join(ProfileModel)
            .where(CategoryModel.profile_id == profile_id, ProfileModel.user_id == user_id)
        )
        result = await self.session.execute(stmt)
        return list(result.scalars().all())
    
    async def save_many(
        self, 
        user_id: PyUUID, 
        category_dtos: List[CategoryCreate]
    ) -> List[CategoryModel]:
        """
        Bulk saves categories after verifying that all profile IDs belong to the user.
        This is the primary engine for category creation.
        """
        if not category_dtos:
            return []

        try:
            # 1. Extract unique profile IDs and verify ownership in one query
            profile_ids = list(set(dto.profile_id for dto in category_dtos))
            ownership_check = (
                select(ProfileModel.id)
                .where(
                    ProfileModel.id.in_(profile_ids),
                    ProfileModel.user_id == user_id
                )
            )
            result = await self.session.execute(ownership_check)
            owned_profiles = result.scalars().all()

            if len(owned_profiles) != len(profile_ids):
                raise NoResultFound("Unauthorized or missing profile access for one or more categories.")

            # 2. Batch instantiate
            categories = [
                CategoryModel(
                    profile_id=dto.profile_id,
                    name=dto.name,
                    image_url=dto.image_url,
                    items=[]
                )
                for dto in category_dtos
            ]
            
            self.session.add_all(categories)
            await self.session.flush()
            
            return categories

        except NoResultFound:
            raise
        except SQLAlchemyError as e:
            await self.session.rollback()
            logger.error(f"SQLAlchemy Category Batch Error: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Database error during batch category creation."
            )

    async def save(self, user_id: PyUUID, category_dto: CategoryCreate) -> CategoryModel:
        """
        Saves a single category by delegating to the bulk save logic.
        """
        results = await self.save_many(user_id, [category_dto])
        category = results[0]
        
        # Refresh for single save to ensure any DB-generated defaults are loaded
        await self.session.refresh(category)
        return category
    
    async def get_category_with_words(self, user_id: PyUUID, category_id: int) -> Optional[CategoryModel]:
        """Fetch category with all its image-words loaded, verified by user_id."""
        query = (
            select(CategoryModel)
            .join(ProfileModel, CategoryModel.profile_id == ProfileModel.id)
            .where(
                CategoryModel.id == category_id, 
                ProfileModel.user_id == user_id
            )
            .options(selectinload(CategoryModel.items))
        )
        result = await self.session.execute(query)
        return result.scalar_one_or_none()

    async def update_fields(
        self, 
        user_id: PyUUID, 
        category_id: int, 
        update_data: Dict[str, Any]
    ) -> CategoryModel:
        """
        Updates specific fields of a category after verifying ownership.
        """
        try:
            # Re-using logic: Find the category while joining Profile to ensure ownership
            query = (
                select(CategoryModel)
                .join(ProfileModel)
                .where(
                    CategoryModel.id == category_id, 
                    ProfileModel.user_id == user_id
                )
            )
            result = await self.session.execute(query)
            category = result.scalar_one_or_none()

            if not category:
                raise NoResultFound(f"Category {category_id} not found or unauthorized.")

            # Set attributes dynamically
            for key, value in update_data.items():
                if hasattr(category, key) and value is not None:
                    setattr(category, key, value)
            
            await self.session.flush()
            await self.session.refresh(category)
            return category

        except NoResultFound as e:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
        except SQLAlchemyError as e:
            await self.session.rollback()
            logger.error(f"Update Category Error: {e}")
            raise HTTPException(status_code=500, detail="Failed to update category.")

    async def delete_category_by_id(self, user_id: PyUUID, category_id: int) -> bool:
        user_profiles = select(ProfileModel.id).where(ProfileModel.user_id == user_id)
        stmt = (
            delete(CategoryModel)
            .where(CategoryModel.id == category_id, CategoryModel.profile_id.in_(user_profiles))
            .returning(CategoryModel.id)
        )
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none() is not None