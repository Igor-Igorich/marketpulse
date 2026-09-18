import argparse
import asyncio
import json
import logging
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
BULK_SEND_BATCH_SIZE = 50
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


async def fetch_all_candles(
    session: aiohttp.ClientSession,
    ticker: str,
    board: str,
    date_from: str,
    date_till: str,
) -> list[list]:
    all_rows: list[list] = []
    start = 0
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
        all_rows.extend(rows)
        if len(rows) < MOEX_CANDLES_PAGE_SIZE:
            break
        start += MOEX_CANDLES_PAGE_SIZE
    return all_rows


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
    rows = await fetch_all_candles(session, ticker, board, date_from, date_till)
    trades = [candle_to_trade(ticker, board, row) for row in rows]
    logger.info(
        "Реплей %s: %d свечей за %s..%s",
        ticker,
        len(trades),
        date_from,
        date_till,
    )

    if speed_seconds_per_candle > 0:
        # Демо-темп: одно сообщение за раз, с реальной паузой — для наглядного
        # "живого" вида при показе, не для массовой заливки.
        for trade in trades:
            await producer.send_and_wait(
                topic,
                key=trade["ticker"].encode("utf-8"),
                value=json.dumps(trade).encode("utf-8"),
            )
            await asyncio.sleep(speed_seconds_per_candle)
    else:
        # Быстрая: пачки по BULK_SEND_BATCH_SIZE отправляются параллельно.
        for i in range(0, len(trades), BULK_SEND_BATCH_SIZE):
            batch = trades[i : i + BULK_SEND_BATCH_SIZE]
            await asyncio.gather(
                *[
                    producer.send_and_wait(
                        topic,
                        key=trade["ticker"].encode("utf-8"),
                        value=json.dumps(trade).encode("utf-8"),
                    )
                    for trade in batch
                ]
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

    parser = argparse.ArgumentParser(
        description="Реплей исторических свечей MOEX в Kafka"
    )
    parser.add_argument("date_from", nargs="?", default="2026-09-15")
    parser.add_argument("date_till", nargs="?", default=None)
    parser.add_argument(
        "--speed",
        type=float,
        default=0.0,
        help="Пауза между свечами в секундах (0 = быстрая заливка; >0 = демо-темп)",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    asyncio.run(
        run_replay(
            args.date_from,
            args.date_till or args.date_from,
            speed_seconds_per_candle=args.speed,
        )
    )
