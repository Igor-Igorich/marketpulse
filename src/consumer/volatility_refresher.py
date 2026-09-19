import asyncio
import logging

import asyncpg

logger = logging.getLogger(__name__)

REFRESH_INTERVAL_SECONDS = 15


async def refresh_volatility_loop(pool: asyncpg.Pool) -> None:
    while True:
        await asyncio.sleep(REFRESH_INTERVAL_SECONDS)
        try:
            async with pool.acquire() as conn:
                await conn.execute(
                    "REFRESH MATERIALIZED VIEW CONCURRENTLY trade_volatility"
                )
            logger.info("trade_volatility обновлена.")
        except Exception as e:
            logger.error("Не удалось обновить trade_volatility: %s.", e)
