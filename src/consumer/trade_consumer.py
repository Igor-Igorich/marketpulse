import asyncio
import logging
import sys

import asyncpg
from aiokafka import AIOKafkaConsumer
from pydantic import ValidationError

from src.config import get_settings
from src.consumer.schemas import TradeMessage

logger = logging.getLogger(__name__)

INSERT_SQL = """
    INSERT INTO trades (trade_id, ticker, board, trade_time, session_date,
                         price, quantity, value, side, source)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
    ON CONFLICT (trade_id, trade_time) DO NOTHING
"""


async def ensure_partitions(pool: asyncpg.Pool) -> None:
    """Партиции на сегодня и завтра должны существовать ДО того, как начнём
    писать — иначе первый же INSERT за пределами существующих партиций
    упадёт.

    Осознанное упрощение: если consumer проработает без перезапуска
    больше суток, партиция на послезавтра создастся с опозданием."""

    async with pool.acquire() as conn:
        await conn.execute("SELECT create_trades_partition(CURRENT_DATE)")
        await conn.execute("SELECT create_trades_partition(CURRENT_DATE + 1)")


async def handle_message(pool: asyncpg.Pool, raw_value: bytes) -> None:
    try:
        trade = TradeMessage.model_validate_json(raw_value)
    except ValidationError as e:
        logger.error("Невалидное сообщение, пропускаю: %s", e)
        return

    try:
        async with pool.acquire() as conn:
            await conn.execute(
                INSERT_SQL,
                trade.trade_id,
                trade.ticker,
                trade.board,
                trade.trade_time,
                trade.session_date,
                trade.price,
                trade.quantity,
                trade.value,
                trade.side,
                trade.source,
            )
    except Exception as e:
        logger.error("Ошибка записи сделки %s в БД: %s", trade.trade_id, e)


async def run_consumer() -> None:
    settings = get_settings()

    pool = await asyncpg.create_pool(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        database=settings.DB_NAME,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
        min_size=2,
        max_size=10,
    )
    await ensure_partitions(pool)

    consumer = AIOKafkaConsumer(
        settings.KAFKA_TOPIC_RAW_TRADES,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id="trade-consumer",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )
    await consumer.start()
    logger.info("Consumer запущен, group_id=trade-consumer")

    try:
        async for msg in consumer:
            await handle_message(pool, msg.value)
    finally:
        await consumer.stop()
        await pool.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(run_consumer())
