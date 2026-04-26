from datetime import datetime
import uuid
from uuid import UUID as PyUUID
from sqlalchemy import Column, DateTime, Integer, String, ForeignKey, Text, Boolean, true
from sqlalchemy.orm import relationship, DeclarativeBase, Mapped, mapped_column
from sqlalchemy.dialects.postgresql import UUID 
from typing import TYPE_CHECKING

class Base(DeclarativeBase):
    """
    The base class for all models. 
    It contains the 'metadata' object required to create tables in the database.
    """
    pass

if TYPE_CHECKING:
    from models.schemas import UserOut

class PasswordResetToken(Base):
    __tablename__ = "password_reset_tokens"
    
    id: Mapped[int] = mapped_column(primary_key=True)
    token: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
    user_id: Mapped[PyUUID] = mapped_column(UUID(as_uuid=True), index=True, nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

class UserModel(Base):
    __tablename__ = "users"
    id: Mapped[PyUUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    hashed_password = Column(String, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, server_default=true(), nullable=False)
    pin: Mapped[str] = mapped_column(String, nullable=True)
    
    profiles = relationship("ProfileModel", back_populates="user", cascade="all, delete-orphan")

    def to_user_out(self) -> "UserOut":
        from models.schemas import UserOut
        return UserOut(id=self.id, email=self.email, is_active=self.is_active)

class ProfileModel(Base):
    __tablename__ = "profiles"
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), index=True, nullable=False) 
    name = Column(String, nullable=False)
    
    user = relationship("UserModel", back_populates="profiles")
    categories = relationship("CategoryModel", back_populates="profile", cascade="all, delete-orphan", order_by="CategoryModel.id")

class CategoryModel(Base):
    __tablename__ = "categories"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    profile_id = Column(Integer, ForeignKey("profiles.id", ondelete="CASCADE"), nullable=False)
    name = Column(String, nullable=False)
    image_url: Mapped[str] = mapped_column(Text, nullable=True)
    
    profile = relationship("ProfileModel", back_populates="categories")
    items = relationship("ImageWordModel", back_populates="category", cascade="all, delete-orphan", order_by="ImageWordModel.id")

class ImageWordModel(Base):
    __tablename__ = "image_words"
    id = Column(Integer, primary_key=True, autoincrement=True)
    category_id: Mapped[int] = mapped_column(Integer, ForeignKey("categories.id", ondelete="CASCADE"), nullable=False)
    word = Column(String, nullable=False)
    word_osastav = Column(String, nullable=True)
    image_url = Column(Text, nullable=True)
    
    category = relationship("CategoryModel", back_populates="items")