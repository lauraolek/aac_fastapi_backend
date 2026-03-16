from typing import List
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.endpoints.category_endpoint import router as category_router
from api.endpoints.estnltk_endpoint import router as estnltk_router
from api.endpoints.image_endpoint import router as image_router
from api.endpoints.image_word_endpoint import router as image_word_router
from api.endpoints.profile_endpoint import router as profile_router
from api.endpoints.tts_endpoint import router as tts_router, tts_lifespan_manager
from api.endpoints.auth_endpoint import router as auth_router
from contextlib import asynccontextmanager, AsyncExitStack
from db.database import create_all_tables

# --- Configuration ---
origins: List[str] = [
    "https://aac.itcollege.ee",
    "http://localhost:8080",
    "http://localhost:61900",
]

# --- Lifecycle Management ---
# Use AsyncExitStack to manage multiple context managers gracefully
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Handles application startup and shutdown events.
    """
    async with AsyncExitStack() as stack:
        # 1. Run the TTS service lifespan manager first
        await stack.enter_async_context(tts_lifespan_manager(app))
        
        # 2. Ensure all database tables exist
        await create_all_tables()
        
        print("Application Startup: Database tables checked, TTS manager initialized.")
        yield
    print("Application Shutdown: All resources released.")

# --- FastAPI Initialization ---
app = FastAPI(
    title="AAC API",
    description="API for AAC app.",
    version="1.0.0",
    lifespan=lifespan,
)

# --- ADD CORS MIDDLEWARE ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,             # Allow cookies and authentication headers
    allow_methods=["*"],                # Allow all HTTP methods (POST, GET, etc.)
    allow_headers=["*"],
)

# --- Include Routers ---
app.include_router(estnltk_router, prefix="/api/v1/estnltk", tags=["estnltk"])
app.include_router(tts_router, prefix="/api/v1/tts", tags=["tts"])
app.include_router(auth_router, prefix="/api/v1/users", tags=["Authentication"])
app.include_router(category_router, prefix="/api/v1/categories", tags=["Categories"])
app.include_router(profile_router, prefix="/api/v1/profiles", tags=["Profiles"]) 
app.include_router(image_word_router, prefix="/api/v1/imagewords", tags=["ImageWords"])
app.include_router(image_router, prefix="/api/v1/images", tags=["images"])


# --- Root Endpoint (Health Check / Documentation Index) ---
@app.get("/", tags=["root"])
def read_root():
    """
    A simple root endpoint directing users to the API documentation.
    """
    return {
        "message": "Welcome to the Estonian Morphology API. Authentication services are now available.", 
        "docs": "See /docs for the OpenAPI documentation (Swagger UI)."
    }