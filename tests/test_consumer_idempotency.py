"""Требует реальный Postgres: docker compose up -d postgres."""

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.consumer.trade_consumer import handle_message

TEST_TRADE_ID = -999999999  # заведомо не пересечётся с реальными live/replay id
FIXED_TRADE_TIME = "2026-09-25T12:00:00"
FIXED_SESSION_DATE = "2026-09-25"


def make_message(price: float) -> bytes:
    payload = {
        "trade_id": TEST_TRADE_ID,
        "ticker": "TEST",
        "board": "TQBR",
        "trade_time": FIXED_TRADE_TIME,
        "session_date": FIXED_SESSION_DATE,
        "price": price,
        "quantity": 1,
        "value": price,
        "side": "buy",
        "source": "replay",
    }
    return json.dumps(payload).encode("utf-8")


@pytest.mark.asyncio
async def test_duplicate_message_not_inserted_twice(db_pool):
    async with db_pool.acquire() as conn:
        await conn.execute("SELECT create_trades_partition(CURRENT_DATE)")
        await conn.execute(
            "DELETE FROM trades WHERE trade_id = $1", TEST_TRADE_ID
        )

    await handle_message(db_pool, make_message(100.0))
    await handle_message(db_pool, make_message(100.0))  # тот же trade_id

    async with db_pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM trades WHERE trade_id = $1", TEST_TRADE_ID
        )
    assert count == 1  # ON CONFLICT DO NOTHING

    async with db_pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM trades WHERE trade_id = $1", TEST_TRADE_ID
        )
