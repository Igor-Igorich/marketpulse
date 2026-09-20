import asyncpg
import pandas as pd

FEATURE_QUERY = """
    SELECT
        ticker,
        trade_time,
        trade_id,
        price,
        LAG(price, 1) OVER w AS lag_1,
        LAG(price, 2) OVER w AS lag_2,
        AVG(price) OVER (
            PARTITION BY ticker ORDER BY trade_time, trade_id
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) AS rolling_mean_30,
        STDDEV(price) OVER (
            PARTITION BY ticker ORDER BY trade_time, trade_id
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) AS rolling_std_30,
        EXTRACT(HOUR FROM trade_time)::int AS hour,
        LEAD(price, 1) OVER w AS next_price
    FROM trades
    WHERE ticker = $1 AND source = 'live'
    WINDOW w AS (PARTITION BY ticker ORDER BY trade_time, trade_id)
    ORDER BY trade_time, trade_id
"""

FEATURE_COLUMNS = [
    "lag_1",
    "lag_2",
    "rolling_mean_30",
    "rolling_std_30",
    "hour",
]


async def load_features(pool: asyncpg.Pool, ticker: str) -> pd.DataFrame:

    async with pool.acquire() as conn:
        rows = await conn.fetch(FEATURE_QUERY, ticker)

    df = pd.DataFrame(
        rows,
        columns=[
            "ticker",
            "trade_time",
            "trade_id",
            "price",
            "lag_1",
            "lag_2",
            "rolling_mean_30",
            "rolling_std_30",
            "hour",
            "next_price",
        ],
    )
    # target = "вырастет ли цена на СЛЕДУЮЩЕЙ сделке относительно текущей".
    # next_price получен через LEAD — единственное место, где мы сознательно
    # смотрим "в будущее", и только для target, никогда для X.
    df["target"] = (df["next_price"] > df["price"]).astype("float")
    return df
