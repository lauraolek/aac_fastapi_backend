from fastapi import APIRouter, HTTPException, Query
from models.estnltk_schemas import SentenceRequest, SentenceResponse
from models.schemas import ImageWord
from services.estnltk_service import teisenda_ma_tahan_lauseosa, get_suggestions
from typing import List

router = APIRouter()

@router.post("/convert", response_model=SentenceResponse, summary="Convert 'Ma tahan' sentence structure")
def convert_sentence(request: SentenceRequest):
    """
    Converts a list of words (provided as ImageWordBase objects) into the correct
    grammatical forms for the Estonian 'Ma tahan' construction.
    The result is returned as a list of fully populated ImageWord objects.
    """
    try:
        # 1. Extract the list of base words (strings) for the morphology service
        # This extracts the 'word' attribute from each ImageWordBase object in the input list.
        base_words = [item.word for item in request.sentence]
        
        # 2. Call the service function to perform the core morphology logic
        conjugated_words = teisenda_ma_tahan_lauseosa(base_words)
        
        # 3. Combine original DTOs and conjugated results into the final response list
        converted_sentence_data: List[ImageWord] = []
        
        # Iterate over the original input DTOs and the resulting conjugated words simultaneously
        for original_dto, conjugated_word in zip(request.sentence, conjugated_words):            
            # Create the final ImageWord response object
            image_word_dto = ImageWord(
                id=original_dto.id,
                # Use data from the original request DTO
                word=original_dto.word,
                image_url=original_dto.image_url,
                
                # Add the computed conjugated word
                conjugated_word=conjugated_word,
                # id remains None as this is purely a conjugation endpoint
            )
            converted_sentence_data.append(image_word_dto)
        
        return SentenceResponse(
            sentence=converted_sentence_data
        )
    except Exception as e:
        # Always log the full error, and return a generic 500 status to the client
        print(f"Error during sentence conversion: {e}")
        raise HTTPException(
            status_code=500, 
            detail="An internal server error occurred during morphology processing."
        )
    
@router.get("/partitive", response_model=List[str])
async def get_word_suggestions(
    word: str = Query(..., min_length=1, description="The word in nominative/root form")
):
    try:
        suggestions = get_suggestions(word)
        return suggestions
    except Exception as e:
        # Log the error but return the original word so the UI doesn't break
        print(f"EstNLTK Error: {e}")
        return [word]