from typing import List

from fastapi import APIRouter, Query

from src.api import state
from src.api.schemas import TradeOut

router = APIRouter()

TICKS_QUERY = """
    SELECT trade_id, ticker, trade_time, price, quantity, side, source
    FROM trades
    WHERE ticker = $1
    ORDER BY trade_time DESC, trade_id DESC
    LIMIT $2
"""


@router.get("/ticks/{ticker}", response_model=List[TradeOut])
async def get_ticks(
    ticker: str,
    limit: int = Query(
        default=100,
        ge=1,
        le=1000,
        description="Сколько последних сделок вернуть",
    ),
):
    async with state.pool.acquire() as conn:
        rows = await conn.fetch(TICKS_QUERY, ticker.upper(), limit)
    return [dict(r) for r in rows]
