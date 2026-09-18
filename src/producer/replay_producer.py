import asyncio
import json
import logging
import sys
import time
from datetime import datetime

import aiohttp
from aiokafka import AIOKafkaProducer

from src.config import get_settings

logger = logging.getLogger(__name__)

CANDLES_URL_TEMPLATE = (
    "https://iss.moex.com/iss/engines/stock/markets/shares/boards/{board}"
    "/securities/{ticker}/candles.json?interval=1&from={date_from}&till={date_till}"
)
MOEX_CANDLES_PAGE_SIZE = 500
CANDLE_COLUMNS = [
    "open",
    "close",
    "high",
    "low",
    "value",
    "volume",
    "begin",
    "end",
]
CIDX = {name: i for i, name in enumerate(CANDLE_COLUMNS)}


def synthetic_trade_id(ticker: str, candle_begin: str) -> int:
    """Детерминированный отрицательный trade_id: кодируем минуту свечи и
    тикер в число. Отрицательный знак гарантирует отсутствие пересечения
    с реальными TRADENO."""
    dt = datetime.strptime(candle_begin, "%Y-%m-%d %H:%M:%S")
    minute_key = int(dt.strftime("%Y%m%d%H%M"))
    ticker_hash = sum(ord(c) for c in ticker) % 1000
    return -(minute_key * 1000 + ticker_hash)


def candle_to_trade(ticker: str, board: str, row: list) -> dict:
    """Второй 'антикоррупционный слой' проекта: свеча MOEX -> тот же
    канонический формат сделки, что и у live_producer. Consumer
    и всё, что ниже по потоку, не будет знать и не должно знать, что эта
    запись на самом деле пришла не из отдельной сделки, а из агрегата."""
    begin = row[CIDX["begin"]]
    dt = datetime.strptime(begin, "%Y-%m-%d %H:%M:%S")
    return {
        "trade_id": synthetic_trade_id(ticker, begin),
        "ticker": ticker,
        "board": board,
        "trade_time": dt.isoformat(),
        "session_date": dt.strftime("%Y-%m-%d"),
        "price": float(row[CIDX["close"]]),
        "quantity": int(row[CIDX["volume"]]),
        "value": float(row[CIDX["value"]]),
        # У свечи нет стороны сделки (это агрегат многих сделок сразу) —
        # берём нейтральное значение по умолчанию. Честная оговорка про
        # огрубление данных реплея — в README.
        "side": "buy",
        "source": "replay",
    }


async def replay_ticker(
    session: aiohttp.ClientSession,
    producer: AIOKafkaProducer,
    ticker: str,
    board: str,
    topic: str,
    date_from: str,
    date_till: str,
    speed_seconds_per_candle: float,
) -> None:

    start = 0
    total_cnt = 0

    while True:
        url = (
            CANDLES_URL_TEMPLATE.format(
                board=board,
                ticker=ticker,
                date_from=date_from,
                date_till=date_till,
            )
            + f"&start={start}"
        )

        async with session.get(url) as resp:
            resp.raise_for_status()
            data = await resp.json()

        rows = data["candles"]["data"]
        if not rows:
            break

        total_cnt += len(rows)

        for row in rows:
            trade = candle_to_trade(ticker, board, row)
            await producer.send(
                topic,
                key=trade["ticker"].encode("utf-8"),
                value=json.dumps(trade).encode("utf-8"),
            )
            if speed_seconds_per_candle > 0:
                await asyncio.sleep(speed_seconds_per_candle)

        if len(rows) < MOEX_CANDLES_PAGE_SIZE:
            break
        start += MOEX_CANDLES_PAGE_SIZE

    logger.info(
        "Реплей %s: %d свечей за %s..%s",
        ticker,
        total_cnt,
        date_from,
        date_till,
    )


async def run_replay(
    date_from: str, date_till: str, speed_seconds_per_candle: float = 0.5
) -> None:
    settings = get_settings()
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS
    )
    await producer.start()
    try:
        async with aiohttp.ClientSession() as session:
            await asyncio.gather(
                *[
                    replay_ticker(
                        session,
                        producer,
                        ticker,
                        settings.MOEX_BOARD,
                        settings.KAFKA_TOPIC_RAW_TRADES,
                        date_from,
                        date_till,
                        speed_seconds_per_candle,
                    )
                    for ticker in settings.tickers_list
                ]
            )
            await producer.flush()
    finally:
        await producer.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    date_from = sys.argv[1] if len(sys.argv) > 1 else "2026-09-15"
    date_till = sys.argv[2] if len(sys.argv) > 2 else date_from
    # speed=0 -> залить всё максимально быстро (для тестов/наполнения БД)
    # speed>0 -> "проигрывать" с задержкой для наглядного демо
    start_time = time.perf_counter()
    asyncio.run(run_replay(date_from, date_till, speed_seconds_per_candle=0.3))
    end_time = time.perf_counter()
    print(f"Время выполнения: {end_time - start_time:.2f} сек")
