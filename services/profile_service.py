import asyncio
import logging
from typing import List, Optional, Set, Tuple, cast
from uuid import UUID as PyUUID
from fastapi import Depends, HTTPException, UploadFile, status
from sqlalchemy.exc import NoResultFound

from db.category_repository import CategoryRepository
from db.image_word_repository import ImageWordRepository
from db.profile_repository import ProfileRepository
from services.seeding_service import SeedingService
from services.image_storage_service import ImageStorageService, get_storage_service
from models.schemas import CategoryCreate, ImageWordCreate, Profile, ProfileCreate 

logger = logging.getLogger(__name__)

class ProfileService:
    """
    Service class for managing Profile entities.
    Handles profile lifecycle including initial seeding and recursive asset cleanup.
    """

    def __init__(
        self, 
        repo: ProfileRepository,
        cat_repo: CategoryRepository,
        i_w_repo: ImageWordRepository,
        seeding_service: SeedingService,
        image_storage_service: ImageStorageService = Depends(get_storage_service), 
    ):
        self.repo = repo
        self.cat_repo = cat_repo
        self.i_w_repo = i_w_repo
        self.image_storage_service = image_storage_service
        self.seeding_service = seeding_service

    async def find_by_user_id(self, user_id: PyUUID) -> List[Profile]:
        """Retrieves all profiles for a specific user and 
        recursively signs all nested image URLs."""
        profiles_raw = await self.repo.find_all_by_user(user_id)
        
        pydantic_profiles = [Profile.model_validate(item) for item in profiles_raw]

        # Process the detached Pydantic objects
        await asyncio.gather(
            *[self._process_profile_urls(item) for item in pydantic_profiles]
        )

        return pydantic_profiles

    async def find_by_id(self, id: int, user_id: PyUUID) -> Optional[Profile]:
        """Retrieves a single profile by ID if it belongs to the user."""
        profile = await self.repo.find_by_id(user_id, id)
        return Profile.model_validate(profile) if profile else None

    async def save(self, user_id: PyUUID, profile_dto: ProfileCreate) -> Profile:
        """
        Creates a profile and seeds it with default categories and words.
        Uses a single transaction for both profile creation and seeding.
        """
        try:
            profile_dto.user_id = user_id
            saved_profile = await self.repo.save(user_id, profile_dto)
            
            # Seed initial content while still in transaction
            profile_id = cast(int, saved_profile.id)
            await self.seed_categories_and_image_words(user_id, profile_id)
            
            await self.repo.session.commit()

            # Fetch fresh data to include seeded relationships
            fetched_profile = await self.find_by_id(profile_id, user_id) 
            if not fetched_profile:
                raise NoResultFound("Profile not found after save.")
                
            return fetched_profile

        except Exception as e:
            await self.repo.session.rollback()
            logger.error(f"Failed to create and seed profile: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to initialize profile data."
            )

    async def delete_by_id(self, id: int, user_id: PyUUID):
        """
        Deletes a profile and all associated images in storage.
        """
        # Fetch full graph for cleanup (ensure categories and items are loaded)
        profile = await self.find_by_id(id, user_id)
        if not profile:
            raise HTTPException(status_code=404, detail="Profile not found")
            
        urls_to_delete: Set[str] = set()
        
        # Collect all asset URLs from categories and their words
        if profile.categories:
            for category in profile.categories:
                if category.image_url:
                    urls_to_delete.add(category.image_url)
                
                items = getattr(category, 'items', [])
                for item in items:
                    if item.image_url:
                        urls_to_delete.add(item.image_url)

        try:
            # Database deletion (cascading delete should handle child rows in DB)
            await self.repo.delete(user_id, id)
            await self.repo.session.commit()
        except Exception as e:
            await self.repo.session.rollback()
            logger.error(f"Profile deletion failed: {e}")
            raise HTTPException(status_code=500, detail="Database deletion failed.")

        # Storage cleanup (Post-Commit)
        if urls_to_delete:
            try:
                await self.image_storage_service.delete_batch(list(urls_to_delete))
            except Exception as e:
                # TODO In a pro system, you'd log this for a background cleanup task.
                logger.warning(f"Orphaned image left: {e}")

    async def seed_categories_and_image_words(self, user_id: PyUUID, profile_id: int):
        """
        Internal helper to populate a new profile with defaults.
        Imports services locally to avoid circular dependency issues.
        """
        from service_dependencies import get_category_service, get_image_word_service
        
        category_service = get_category_service(self.cat_repo, self.image_storage_service)
        image_word_service = get_image_word_service(self.i_w_repo, self.image_storage_service)

        # Structure: (Category Name, Asset Filename)
        categories_to_seed = [
            ("Algused", "beginning.png"),
            ("Numbrid", "numbers.png"),
            ("Värvid", "colors.png"),
            ("Omadused", "adjectives.png"),
            ("Köök", "kitchen.png"),
            ("Mänguasjad", "toys.png"),
            ("Tegevused", "activity.png"),
        ]

        # Structure: (Target Category, Word Text, Osastav text, Asset Filename)
        words_to_seed = [
            ("Algused", "Ma tahan", None, "I want.png"),
            ("Algused", "Jah", None, "yes.png"),
            ("Algused", "Ei", None, "no.png"),
            ("Algused", "Aita mind", None, "help.png"),
            ("Algused", "Oota", "Oodata", "wait.png"),
            ("Numbrid", "üks", "ühte", "one.png"),
            ("Numbrid", "kaks", "kahte", "2.png"),
            ("Numbrid", "kolm", "kolme", "3.png"),
            ("Numbrid", "neli", "nelja", "4.png"),
            ("Numbrid", "viis", "viite", "5.png"),
            ("Numbrid", "kuus", "kuute", "6.png"),
            ("Numbrid", "seitse", "seitset", "7.png"),
            ("Numbrid", "kaheksa", "kaheksat", "8.png"),
            ("Numbrid", "üheksa", "üheksat", "9.png"),
            ("Numbrid", "kümme", "kümmet", "10.png"),
            ("Värvid", "punane", "punast", "red.png"),
            ("Värvid", "oranž", "oranži", "orange.png"),
            ("Värvid", "kollane", "kollast", "yellow.png"),
            ("Värvid", "roheline", "rohelist", "green.png"),
            ("Värvid", "sinine", "sinist", "blue.png"),
            ("Värvid", "lilla", "lillat", "purple.png"),
            ("Värvid", "valge", "valget", "white.png"),
            ("Värvid", "must", "musta", "black.png"),
            ("Värvid", "pruun", "pruuni", "brown.png"),
            ("Värvid", "roosa", "roosat", "pink.png"),
            ("Omadused", "suur", "suurt", "big.png"),
            ("Omadused", "keskmine", "keskmist", "medium.png"),
            ("Omadused", "väike", "väikest", "small.png"),
            ("Omadused", "ümmargune", "ümmargust", "circle.png"),
            ("Omadused", "triibuline", "triibulist", "striped.png"),
            ("Köök", "vesi", "vett", "water.png"),
            ("Köök", "piim", "piima", "milk.png"),
            ("Köök", "mahl", "mahla", "juice.png"),
            ("Köök", "klaas", "klaasi", "glass.png"),
            ("Köök", "lusikas", "lusikat", "spoon.png"),
            ("Köök", "kahvel", "kahvlit", "fork.png"),
            ("Köök", "nuga", "nuga", "knife.png"),
            ("Mänguasjad", "kaisukaru", "kaisukaru", "teddy-bear.png"),
            ("Mänguasjad", "veoauto", "veoautot", "toy truck.png"),
            ("Mänguasjad", "nukk", "nukku", "Barbie.png"),
            ("Mänguasjad", "ratas", "ratast", "tricycle.png"),
            ("Mänguasjad", "pusle", "puslet", "puzzle.png"),
            ("Tegevused", "mängima", "mängida", "play.png"),
            ("Tegevused", "sööma", "süüa", "eat.png"),
            ("Tegevused", "jooma", "juua", "drink.png"),
            ("Tegevused", "magama", "magada", "sleep.png"),
        ]

        all_items_to_upload = []
        for name, img in categories_to_seed:
            all_items_to_upload.append(("cat", name, self.seeding_service.get_upload_file(img)))
        for cat_name, word_text, osastav_text, img in words_to_seed:
            all_items_to_upload.append(("word", (cat_name, word_text, osastav_text), self.seeding_service.get_upload_file(img)))

        logger.info(f"Uploading {len(all_items_to_upload)} files in one session...")
        uploaded_data = await self.image_storage_service.upload_batch(all_items_to_upload)

        cat_dtos = []
        word_map = {}
        
        for tag, meta, url in uploaded_data:
            if tag == "cat":
                cat_dtos.append(CategoryCreate(name=meta, image_url=url, profile_id=profile_id))
            else:
                c_name, w_text, osastav_text = meta
                word_map.setdefault(c_name, []).append((w_text, osastav_text, url))

        try:
            # Batch 1: Categories
            created_cats = await self.cat_repo.save_many(user_id, cat_dtos)
            await self.cat_repo.session.flush() # Get the IDs without committing
            
            cat_id_lookup = {c.name: c.id for c in created_cats}
            
            # Batch 2: Words
            final_word_dtos = []
            for c_name, items in word_map.items():
                cid = cat_id_lookup.get(c_name)
                if cid is None:
                    logger.error(f"Category {c_name} ID not found after save.")
                    continue

                for w_text, osastav_text, url in items:
                    final_word_dtos.append(ImageWordCreate(category_id=cid, word=w_text, word_osastav=osastav_text, image_url=url))
            
            await self.i_w_repo.save_many(user_id, final_word_dtos)
            
            await self.cat_repo.session.commit()
            return created_cats
            
        except Exception as e:
            await self.cat_repo.session.rollback()
            # Optional: trigger cleanup of uploaded_data urls
            raise e

    
    async def _process_profile_urls(self, profile: Profile) -> Profile:
        """
        Helper to traverse the nested structure: 
        Profile -> Categories -> ImageWords
        """
        if not profile.categories:
            return profile

        for category in profile.categories:
            if category.image_url:
                category.image_url = await self.image_storage_service.get_url(category.image_url)

            if category.items:
                for word in category.items:
                    if word.image_url:
                        word.image_url = await self.image_storage_service.get_url(word.image_url)
        
        return profile