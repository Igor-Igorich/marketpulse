import logging
from contextlib import asynccontextmanager

import asyncpg
import joblib
from fastapi import FastAPI

from src.api import state
from src.config import get_settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

logger = logging.getLogger(__name__)


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

    for ticker in settings.tickers_list:
        model_path = f"models/direction_{ticker}.pkl"
        try:
            state.models[ticker] = joblib.load(model_path)
            pr_auc = state.models[ticker]["metrics"].get("pr_auc")
            logger.info("Модель %s загружена (PR-AUC=%s)", ticker, pr_auc)
        except FileNotFoundError:
            logger.warning(
                "Модель %s не найдена (%s) — /predict/%s будет отдавать 404",
                ticker,
                model_path,
                ticker,
            )

    yield
    await state.pool.close()


app = FastAPI(title="MarketPulse API", lifespan=lifespan)

from src.api.routers import predict, stream, ticks, volatility  # noqa: E402

app.include_router(ticks.router, tags=["ticks"])
app.include_router(volatility.router, tags=["volatility"])
app.include_router(stream.router, tags=["stream"])
app.include_router(predict.router, tags=["predict"])


@app.get("/")
async def root():
    return {"service": "MarketPulse", "status": "ok"}
