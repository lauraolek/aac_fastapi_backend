from typing import List, Optional
from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from uuid import UUID as PyUUID
from auth.dependencies import get_current_user_id
from models.schemas import ImageWord
from service_dependencies import get_image_word_service
from services.image_word_service import ImageWordService

router = APIRouter(prefix="", tags=["ImageWords"])

@router.get("/category/{category_id}", response_model=List[ImageWord])
async def get_image_words_by_category_id(
    category_id: int,
    user_id: PyUUID = Depends(get_current_user_id),
    svc: ImageWordService = Depends(get_image_word_service)
):
    return await svc.find_by_category_id(user_id, category_id)

@router.get("/{id}", response_model=ImageWord)
async def get_image_word_by_id(
    id: int,
    user_id: PyUUID = Depends(get_current_user_id),
    svc: ImageWordService = Depends(get_image_word_service),
):
    """Retrieves a specific image+word by its ID."""
    iw = await svc.find_by_id(user_id, id)
    if not iw:
        raise HTTPException(status_code=404, detail="ImageWord not found")
    return iw

@router.post("/category/{category_id}", response_model=ImageWord, status_code=201)
async def create_image_word(
    category_id: int,
    word: str = Form(...),
    word_osastav: Optional[str] = Form(None, alias="wordOsastav"),
    imageFile: UploadFile = File(...),
    user_id: PyUUID = Depends(get_current_user_id),
    svc: ImageWordService = Depends(get_image_word_service),
):
    """Creates a new image+word for a given category with a file upload."""
    try:
        return await svc.save(user_id, category_id, word, word_osastav, imageFile)
    except Exception:
        raise HTTPException(status_code=400, detail="Error creating image word")

@router.put("/{id}", response_model=ImageWord)
async def update_image_word(
    id: int,
    wordText: str = Form(...),
    word_osastav: Optional[str] =  Form(None, alias="wordOsastav"),
    imageFile: Optional[UploadFile] = File(None),
    category_id: int = Form(..., alias="categoryId"),
    user_id: PyUUID = Depends(get_current_user_id),
    svc: ImageWordService = Depends(get_image_word_service)
):
    """Updates an existing image+word, optionally with a new image."""
    try:
        return await svc.update(user_id, id, wordText, word_osastav, category_id, imageFile)
    except ValueError:
        raise HTTPException(status_code=404, detail="ImageWord not found")
    except Exception:
        raise HTTPException(status_code=400, detail="Update failed")

@router.delete("/{id}", status_code=204)
async def delete_image_word(
    id: int,
    user_id: PyUUID = Depends(get_current_user_id),
    svc: ImageWordService = Depends(get_image_word_service)
):
    """Deletes an image+word by its ID."""
    try:
        await svc.delete_by_id(user_id, id)
        return None
    except ValueError:
        raise HTTPException(status_code=404, detail="ImageWord not found")