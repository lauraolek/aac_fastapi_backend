import logging
import os
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

from .models import Base
from config import settings

logger = logging.getLogger(__name__)

# --- 1. Database Configuration ---

DATABASE_URL = settings.database_url

async_engine = create_async_engine(
    DATABASE_URL,
    pool_size=10,                # Maintain up to 10 keep-alive connections
    max_overflow=20,             # Allow up to 20 extra connections during spikes
    pool_recycle=3600,           # Refresh connections older than 1 hour
    pool_pre_ping=True,          # Check if connection is alive before every request
    echo=False                   # Set to True only for local debugging
)
AsyncSessionFactory = async_sessionmaker(
    bind=async_engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

async def create_all_tables():
    """Creates all defined tables in the database."""
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database tables created successfully.")

# --- 2. Database Session Dependency ---

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionFactory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()