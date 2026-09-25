import sys
from pathlib import Path

import asyncpg
import pytest_asyncio

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import get_settings


@pytest_asyncio.fixture
async def db_pool():
    """Реальный пул к БД — только для интеграционных тестов (Блок 4).
    Требует поднятого docker compose up -d postgres."""
    settings = get_settings()
    pool = await asyncpg.create_pool(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        database=settings.DB_NAME,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
    )
    yield pool
    await pool.close()
