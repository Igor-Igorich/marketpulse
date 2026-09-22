import asyncio
import json
import logging

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from src.api import state

logger = logging.getLogger(__name__)
router = APIRouter()

STREAM_POLL_INTERVAL_SECONDS = 1.0
HEARTBEAT_INTERVAL_SECONDS = 15.0

LATEST_TRADE_QUERY = """
    SELECT trade_id, ticker, trade_time, price, quantity, side, source
    FROM trades
    WHERE ticker = $1
    ORDER BY trade_time DESC, trade_id DESC
    LIMIT 1
"""


async def trade_event_generator(request: Request, ticker: str):
    """Раз в секунду проверяем последнюю сделку по тикеру; если она новее
    последней отправленной — шлём SSE-событие. Если долго нечего слать —
    шлём heartbeat, чтобы промежуточные прокси не решили, что соединение
    мертво и не оборвали его сами."""
    ticker = ticker.upper()
    last_sent_trade_id: int | None = None
    seconds_since_last_send = 0.0

    while True:
        # Без этой проверки генератор продолжит опрашивать БД вечно, даже
        # после того как клиент давно закрыл вкладку
        # (короткоживущие эндпоинты такой проблемы в принципе не имеют).
        if await request.is_disconnected():
            logger.info("Клиент отключился от /stream/%s", ticker)
            break

        async with state.pool.acquire() as conn:
            row = await conn.fetchrow(LATEST_TRADE_QUERY, ticker)

        if row is not None and row["trade_id"] != last_sent_trade_id:
            last_sent_trade_id = row["trade_id"]
            payload = {
                "trade_id": row["trade_id"],
                "ticker": row["ticker"],
                "trade_time": row["trade_time"].isoformat(),
                "price": float(row["price"]),
                "quantity": row["quantity"],
                "side": row["side"],
                "source": row["source"],
            }
            yield f"data: {json.dumps(payload)}\n\n"
            seconds_since_last_send = 0.0
        elif seconds_since_last_send >= HEARTBEAT_INTERVAL_SECONDS:
            # Строка, начинающаяся с ':', — комментарий по спецификации SSE:
            # клиент его не воспримет как событие, но соединение останется
            # активным для прокси/балансировщиков по пути.
            yield ": heartbeat\n\n"
            seconds_since_last_send = 0.0

        await asyncio.sleep(STREAM_POLL_INTERVAL_SECONDS)
        seconds_since_last_send += STREAM_POLL_INTERVAL_SECONDS


@router.get("/stream/{ticker}")
async def stream_ticker(request: Request, ticker: str):
    return StreamingResponse(
        trade_event_generator(request, ticker),
        media_type="text/event-stream",
    )
