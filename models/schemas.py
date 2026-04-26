from __future__ import annotations
from typing import List, Optional
from pydantic import BaseModel, ConfigDict, EmailStr, Field
from uuid import UUID as PyUUID

# Utility function for Pydantic's alias_generator
def to_camel(string: str) -> str:
    """Converts a string from snake_case to camelCase."""
    if "_" not in string:
        return string
    parts = string.split('_')
    return parts[0] + "".join(part.capitalize() for part in parts[1:])

# Base configuration class
class CamelModel(BaseModel):
    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
        from_attributes=True
    )

# --- IMAGE WORD SCHEMAS ---

class ImageWordBase(CamelModel):
    word: str = Field(..., description="The base word (e.g., 'table', 'run').")
    word_osastav: Optional[str] = None
    image_url: Optional[str] = Field(None, description="The URL for the image.")

class ImageWordCreate(ImageWordBase):
    category_id: int = Field(..., description="Database ID of its category.")

class ImageWordUpdate(CamelModel):
    id: int
    word: Optional[str] = None
    word_osastav: Optional[str] = None
    image_url: Optional[str] = None

class ImageWord(ImageWordBase):
    id: int
    conjugated_word: Optional[str] = None

# --- CATEGORY SCHEMAS ---

class CategoryBase(CamelModel):
    name: str = Field(..., description="Name of the category.")
    image_url: Optional[str] = Field(None, description="URL for the category's icon/image.")
    profile_id: Optional[int] = Field(None, description="Database ID of its profile.")

class CategoryCreate(CategoryBase):
    pass

class CategorySimple(CategoryBase):
    id: int

class Category(CategorySimple):
    items: List[ImageWord] = Field(default_factory=list)

# --- PROFILE SCHEMAS ---

class ProfileBase(CamelModel):
    name: str = Field(..., description="The name of the profile.")
    user_id: Optional[PyUUID] = None

class ProfileCreate(ProfileBase):
    pass

class Profile(ProfileBase):
    id: int
    categories: list[Category] = Field(default_factory=list)

# --- USER & AUTH SCHEMAS ---

class UserBase(CamelModel):
    email: EmailStr

class UserCreate(UserBase):
    password: str

class UserLogin(CamelModel):
    email: EmailStr
    password: str

class UserOut(UserBase):
    id: PyUUID
    is_active: bool = True

class PinUpdatePayload(CamelModel):
    pin: str = Field(..., description="The new 4 digit security PIN")

class Token(CamelModel):
    token: str
    token_type: str = "bearer"

class ResetPasswordUpdate(CamelModel):
    token: str
    new_password: str

# IMPORTANT: Rebuild models to resolve type hints for FastAPI
ImageWord.model_rebuild()
Category.model_rebuild()
Profile.model_rebuild()