from typing import List

from fastapi import APIRouter, Query

from src.api import state
from src.api.schemas import VolatilityPoint

router = APIRouter()

VOLATILITY_QUERY = """
    SELECT trade_time, price, rolling_mean, rolling_volatility
    FROM trade_volatility
    WHERE ticker = $1
    ORDER BY trade_time DESC, trade_id DESC
    LIMIT $2
"""


@router.get("/volatility/{ticker}", response_model=List[VolatilityPoint])
async def get_volatility(
    ticker: str,
    limit: int = Query(default=100, ge=1, le=1000),
):
    async with state.pool.acquire() as conn:
        rows = await conn.fetch(VOLATILITY_QUERY, ticker.upper(), limit)
    return [dict(r) for r in rows]
