"""Тесты роутеров без реального Postgres — подменяем src.api.state
напрямую. ASGITransport НЕ запускает lifespan автоматически (в отличие
от uvicorn), поэтому state.pool останется тем, что мы сами туда положим,
а не тем, что создал бы реальный lifespan."""

import sys
from pathlib import Path
from unittest.mock import patch

import pytest
from httpx import ASGITransport, AsyncClient

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.api import state
from src.api.main import app


class FakeConnection:
    def __init__(self, rows):
        self._rows = rows

    async def fetch(self, query, *args):
        return self._rows


class FakePool:
    def __init__(self, rows):
        self._conn = FakeConnection(rows)

    def acquire(self):
        return self

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


@pytest.mark.asyncio
async def test_get_ticks_returns_rows():
    fake_rows = [
        {
            "trade_id": 1,
            "ticker": "SBER",
            "trade_time": "2026-09-15T10:00:00",
            "price": 287.5,
            "quantity": 10,
            "side": "buy",
            "source": "live",
        }
    ]
    with patch.object(state, "pool", FakePool(fake_rows)):
        transport = ASGITransport(app=app)
        async with AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            response = await client.get("/ticks/SBER?limit=5")

    assert response.status_code == 200
    assert response.json()[0]["ticker"] == "SBER"


@pytest.mark.asyncio
async def test_get_ticks_rejects_limit_over_1000():
    transport = ASGITransport(app=app)
    async with AsyncClient(
        transport=transport, base_url="http://test"
    ) as client:
        response = await client.get("/ticks/SBER?limit=5000")

    # Декларативная граница Query(le=1000)

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_predict_returns_404_when_model_missing():
    with patch.object(state, "models", {}):
        transport = ASGITransport(app=app)
        async with AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            response = await client.get("/predict/UNKNOWN")
    assert response.status_code == 404
