from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI

from src.api import state
from src.config import get_settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    state.pool = await asyncpg.create_pool(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        database=settings.DB_NAME,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
        min_size=2,
        max_size=10,
    )
    yield
    await state.pool.close()


app = FastAPI(title="MarketPulse API", lifespan=lifespan)

from src.api.routers import stream, ticks, volatility  # noqa: E402

app.include_router(ticks.router, tags=["ticks"])
app.include_router(volatility.router, tags=["volatility"])
app.include_router(stream.router, tags=["stream"])


@app.get("/")
async def root():
    return {"service": "MarketPulse", "status": "ok"}
