import asyncio
import json
import logging

import aiohttp
from aiokafka import AIOKafkaProducer

from src.config import get_settings

logger = logging.getLogger(__name__)

ISS_TRADES_URL_TEMPLATE = (
    "https://iss.moex.com/iss/engines/stock/markets/shares/boards/{board}"
    "/securities/{ticker}/trades.json?reversed=1"
)
COLUMNS = [
    "TRADENO",
    "TRADETIME",
    "SECID",
    "PRICE",
    "QUANTITY",
    "PERIOD",
    "VALUE",
    "BOARDID",
    "SYSTIME",
    "BUYSELL",
    "DECIMALS",
    "TRADINGSESSION",
    "TRADEDATE",
    "TRADE_SESSION_DATE",
]
IDX = {name: i for i, name in enumerate(COLUMNS)}


def row_to_trade(row: list, source: str = "live") -> dict:
    """Переводит сырую строку MOEX в принятый каноническим формат сделки."""
    side_raw = row[IDX["BUYSELL"]]
    return {
        "trade_id": row[IDX["TRADENO"]],
        "ticker": row[IDX["SECID"]],
        "board": row[IDX["BOARDID"]],
        "trade_time": f'{row[IDX["TRADEDATE"]]}T{row[IDX["TRADETIME"]]}',
        "session_date": row[IDX["TRADE_SESSION_DATE"]],
        "price": float(row[IDX["PRICE"]]),
        "quantity": int(row[IDX["QUANTITY"]]),
        "value": float(row[IDX["VALUE"]]),
        "side": "buy" if side_raw == "B" else "sell",
        "source": source,
    }


async def poll_ticker(
    session: aiohttp.ClientSession,
    producer: AIOKafkaProducer,
    ticker: str,
    board: str,
    topic: str,
    last_seen: dict[str, int],
) -> None:
    url = ISS_TRADES_URL_TEMPLATE.format(board=board, ticker=ticker)
    async with session.get(url) as resp:
        resp.raise_for_status()
        data = await resp.json()

    rows = data["trades"]["data"]
    if not rows:
        return

    known_max = last_seen.get(ticker, 0)
    new_rows = [r for r in rows if r[IDX["TRADENO"]] > known_max]

    if new_rows and len(new_rows) == len(rows) and known_max != 0:
        logger.warning(
            "Возможен пропуск сделок по %s: все %d строк новее последней "
            "виденной (%d)",
            ticker,
            len(rows),
            known_max,
        )

    for row in new_rows:
        trade = row_to_trade(row, source="live")
        await producer.send_and_wait(
            topic,
            key=trade["ticker"].encode("utf-8"),
            value=json.dumps(trade).encode("utf-8"),
        )

    if new_rows:
        last_seen[ticker] = max(r[IDX["TRADENO"]] for r in new_rows)
        logger.info("Отправлено %d новых сделок по %s", len(new_rows), ticker)


async def run_live_producer() -> None:
    settings = get_settings()
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS
    )
    await producer.start()
    last_seen: dict[str, int] = {}

    try:
        async with aiohttp.ClientSession() as session:
            while True:
                await asyncio.gather(
                    *[
                        poll_ticker(
                            session,
                            producer,
                            ticker,
                            settings.MOEX_BOARD,
                            settings.KAFKA_TOPIC_RAW_TRADES,
                            last_seen,
                        )
                        for ticker in settings.tickers_list
                    ]
                )
                await asyncio.sleep(settings.MOEX_POLL_INTERVAL_SECONDS)
    finally:
        await producer.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run_live_producer())
