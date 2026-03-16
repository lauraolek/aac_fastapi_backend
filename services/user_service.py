from datetime import datetime, timedelta, timezone
import logging
import secrets
from uuid import UUID as PyUUID
from typing import Optional
from fastapi import BackgroundTasks, HTTPException, status
from passlib.context import CryptContext

from db.models import PasswordResetToken
from db.profile_repository import ProfileRepository
from db.user_repository import UserRepository
from models.schemas import ResetPasswordUpdate, UserCreate, UserLogin, UserOut, ProfileCreate
from auth.password_handler import hash_password, verify_password
from services.email_service import EmailService
from config import settings

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
logger = logging.getLogger(__name__)

DEFAULT_PIN = settings.default_pin

class UserService:
    def __init__(self, repository: UserRepository, prof_repo: ProfileRepository, email_service: EmailService, profile_service=None):
        self.repo = repository
        self.prof_repo = prof_repo
        # profile_service is injected to handle complex seeding
        self.profile_service = profile_service
        self.email_service = email_service

    async def register_user(self, user_data: UserCreate) -> UserOut:
        """Registers a new user and seeds their default profile."""
        existing_user = await self.repo.get_by_email(user_data.email)
        if existing_user:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Email already registered"
            )

        try:
            hashed = hash_password(user_data.password)
            user_model = await self.repo.create(user_data, hashed)
            
            # Flush to get user_model.id without committing yet
            await self.repo.session.flush()
            
            # Seed initial profile immediately
            await self.seed_initial_profile(user_model.id)
            
            # Commit the whole unit (User + Profile + Seeded Content)
            await self.repo.session.commit()
            return user_model.to_user_out()
            
        except Exception as e:
            await self.repo.session.rollback()
            logger.error(f"Registration failed for {user_data.email}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="User registration failed during initialization."
            )

    async def authenticate_user(self, login_data: UserLogin) -> Optional[UserOut]:
        """Authenticates user and ensures a profile exists (lazy-migration)."""
        user = await self.repo.get_by_email(login_data.email)
        if not user or not verify_password(login_data.password, str(user.hashed_password)):
            return None
        
        # we check this here to ensure every active user has at least one profile
        await self.seed_initial_profile(user.id)
        # We commit here because seed_initial_profile might have made changes
        await self.repo.session.commit()
        
        return user.to_user_out()

    async def seed_initial_profile(self, user_id: PyUUID) -> None:
        """
        Checks if a user has profiles; if not, creates a default one.
        If profile_service is available, it triggers deep seeding of categories/words.
        """
        # Optimized check: we only care if 'any' profile exists
        existing_profiles = await self.prof_repo.find_all_by_user(user_id)
        
        if not existing_profiles:
            logger.info(f"User {user_id} has no profiles. Seeding 'Vaikimisi' (Default).")
            
            try:
                # 1. Create the base profile record
                new_profile = await self.prof_repo.save(
                    user_id, 
                    ProfileCreate(name="Vaikimisi")
                )
                
                # Ensure the profile ID is available for foreign keys
                await self.prof_repo.session.flush()
                
                # 2. Trigger deep seeding (Categories & Words)
                if self.profile_service:
                    await self.profile_service.seed_categories_and_image_words(
                        user_id, 
                        new_profile.id
                    )
                else:
                    logger.warning(f"ProfileService not available for deep seeding user {user_id}")
                    
            except Exception as e:
                # We don't commit/rollback here; the caller (register/auth) manages the transaction
                logger.error(f"Error during initial profile seeding for user {user_id}: {e}")
                raise

    async def get_user_pin(self, user_id: PyUUID) -> Optional[str]:
        """Retrieves the PIN for a specific user."""
        user = await self.repo.get_by_id(user_id)
        if not user:
            return None
        return user.pin

    async def update_user_pin(self, user_id: PyUUID, new_pin: str) -> bool:
        """Updates the user's PIN."""
        try:
            # You might want to add validation here (e.g., 4 digits)
            await self.repo.update_user(user_id, {"pin": new_pin})
            await self.repo.session.commit()
            return True
        except Exception as e:
            await self.repo.session.rollback()
            logger.error(f"Failed to update PIN for user {user_id}: {e}")
            return False

    async def initiate_pin_reset(self, user_id: PyUUID, background_tasks: BackgroundTasks) -> bool:
        """Logic to send a reset PIN email."""
        user = await self.repo.get_by_id(user_id)
        if not user:
            return False
        
        await self.repo.update_user(user_id, {"pin": DEFAULT_PIN})
        await self.repo.session.commit()
            
        logger.info(f"Initiating PIN reset for {user.email}")
        background_tasks.add_task(self.email_service.send_pin_reset_email, user.email, DEFAULT_PIN)
        
        return True
    
    async def initiate_password_reset(self, email: str, background_tasks) -> bool:
        """
        Generates a secure token, saves it to the DB, and queues the email.
        Returns True regardless of user existence to prevent enumeration.
        """
        user = await self.repo.get_by_email(email)
        
        if not user:
            logger.warning(f"Password reset attempted for non-existent email: {email}")
            return True 
            
        reset_token = secrets.token_urlsafe(32)
        
        expires_at = datetime.now(timezone.utc) + timedelta(hours=1)
        
        try:
            # 3. Save token to database
            new_token_entry = PasswordResetToken(
                token=reset_token,
                user_id=user.id,
                expires_at=expires_at
            )
            self.repo.session.add(new_token_entry)
            await self.repo.session.commit()
            
            logger.info(f"Stored reset token in DB for user_id: {user.id}")

            # 4. Add email sending to background tasks
            background_tasks.add_task(
                self.email_service.send_password_reset_email, 
                email, 
                reset_token
            )
        except Exception as e:
            await self.repo.session.rollback()
            logger.error(f"Failed to initiate password reset for {email}: {str(e)}")
            # We still return True to avoid leaking info, 
            # though internally it failed.
        
        return True

    async def complete_password_reset(self, data: ResetPasswordUpdate) -> bool:
        """Coordinates the reset process and handles database commits."""
        # 1. Fetch token record
        record = await self.repo.get_reset_token_record(data.token)
        
        if not record:
            return False
            
        # 2. Check expiration using timezone-aware UTC objects
        if datetime.now(timezone.utc) > record.expires_at:
            await self.repo.delete_reset_token(data.token)
            await self.repo.session.commit() # Commit deletion of expired token
            return False

        # 3. Hash and Update
        hashed_pw = hash_password(data.new_password)
        await self.repo.update_user_password(record.user_id, hashed_pw)
        
        # 4. Cleanup token
        await self.repo.delete_reset_token(data.token)
        
        # 5. Commit all changes at the Service level
        try:
            await self.repo.session.commit()
            return True
        except Exception as e:
            await self.repo.session.rollback()
            logger.error(f"Transaction failed, rolling back: {e}")
            return False