from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status, Path, Form, File, UploadFile
import logging
from uuid import UUID as PyUUID

from models.schemas import Category
from service_dependencies import get_category_service
from auth.dependencies import get_current_user_id

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="",
    tags=["Categories"]
)

@router.get(
    "/profile/{profile_id}",
    response_model=List[Category],
    status_code=status.HTTP_200_OK,
    summary="Retrieve all categories for a specific profile"
)
async def get_categories_by_profile_id(
    profile_id: int = Path(..., description="The ID of the profile"),
    user_id: PyUUID = Depends(get_current_user_id),
    category_service = Depends(get_category_service)
):
    return await category_service.find_by_profile_id(user_id, profile_id)

@router.get(
    "/{id}",
    response_model=Category,
    status_code=status.HTTP_200_OK,
    summary="Retrieve a specific category by ID"
)
async def get_category_by_id(
    id: int = Path(..., description="The ID of the category"),
    user_id: PyUUID = Depends(get_current_user_id),
    category_service = Depends(get_category_service)
):
    category = await category_service.get_category_by_id(user_id, id)
    if not category:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Category not found or access denied.")
    return category

@router.post(
    "/profile/{profile_id}",
    response_model=Category,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new category for a profile with an image upload"
)
async def create_category(
    profile_id: int = Path(..., description="The ID of the profile"),
    name: str = Form(..., description="The name of the new category"),
    image_file: UploadFile = File(..., alias="imageFile", description="The image file for the category"),
    rotation_turns: int = Form(0, alias="rotationTurns"),
    user_id: PyUUID = Depends(get_current_user_id),
    category_service = Depends(get_category_service)
):
    image_bytes = await image_file.read()
    if not image_bytes:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, 
            detail="Image file is required and cannot be empty."
        )
    
    await image_file.seek(0)
    # 3. Call the service to save the category and image
    try:
        return await category_service.create_category(
            user_id=user_id, 
            profile_id=profile_id, 
            name=name, 
            image_file=image_file,
            rotation_turns=rotation_turns
        )
    except HTTPException:
        # Re-raise exceptions raised by the service (like 404 for not found/unauthorized)
        raise
    except Exception as e:
        logger.error(f"Error creating category: {e}")
        # Catch unexpected errors and return 500
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail="An unexpected error occurred while creating the category."
        )


@router.put(
    "/{id}",
    response_model=Category,
    status_code=status.HTTP_200_OK,
    summary="Update an existing category, optionally replacing its image"
)
async def update_category(
    id: int = Path(..., description="The ID of the category to update"),
    name: str = Form(..., description="The new name of the category"),
    image_file: UploadFile | None = File(None, alias="imageFile", description="Optional new image file for the category"),
    rotation_turns: Optional[int] = Form(None, alias="rotationTurns"),
    user_id: PyUUID = Depends(get_current_user_id),
    category_service = Depends(get_category_service)
):
    """
    Updates the category name and optionally replaces the image if a file is provided.
    Handles multipart/form-data.
    """
    if image_file and image_file.filename:
        image_bytes = await image_file.read()
        if not image_bytes:
             raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, 
                detail="Provided image file is empty."
            )
        await image_file.seek(0)

    try:
        updated_category = await category_service.update_category(
            user_id=user_id, 
            category_id=id, 
            name=name, 
            image_file=image_file if (image_file and image_file.filename) else None,
            rotation_turns=rotation_turns
        )
        return updated_category
    except HTTPException:
        # Re-raise exceptions raised by the service (like 404 for not found/unauthorized)
        raise
    except Exception as e:
        logger.error(f"Error updating category {id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail="An unexpected error occurred while updating the category."
        )


@router.delete(
    "/{id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete a category by ID"
)
async def delete_category(
    id: int = Path(..., description="The ID of the category to delete"),
    user_id: PyUUID = Depends(get_current_user_id),
    category_service = Depends(get_category_service)
):
    """
    Deletes the category, its image, and cascades deletion of associated image words and their images.
    Returns 204 No Content on success.
    """
    try:
        await category_service.delete_category(user_id, id)
    except HTTPException:
        # Re-raise exceptions raised by the service (though deleteById often handles 404 internally)
        raise
    except Exception as e:
        logger.error(f"Error deleting category {id}: {e}")
        # The Java code returns 404 on general exception, but a successful delete should return 204.
        # We rely on the service to handle authorization and missing entities gracefully.
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail="An unexpected error occurred during deletion."
        )