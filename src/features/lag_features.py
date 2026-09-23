import asyncpg
import pandas as pd

FEATURE_COLUMNS = [
    "lag_1",
    "lag_2",
    "rolling_mean_30",
    "rolling_std_30",
    "hour",
    "price_vs_mean",
]

_BASE_QUERY = """
    SELECT
        ticker,
        trade_time,
        trade_id,
        price::float8                                    AS price,
        CAST(LAG(price, 1) OVER w AS float8)             AS lag_1,
        CAST(LAG(price, 2) OVER w AS float8)             AS lag_2,
        CAST(AVG(price) OVER w30 AS float8)              AS rolling_mean_30,
        CAST(STDDEV(price) OVER w30 AS float8)           AS rolling_std_30,
        EXTRACT(HOUR FROM trade_time)::int               AS hour,
        CAST(LEAD(price, 1) OVER w AS float8)            AS next_price
    FROM trades
    WHERE ticker = $1 AND source = 'live'
    WINDOW
        w AS (PARTITION BY ticker ORDER BY trade_time, trade_id),
        w30 AS (PARTITION BY ticker ORDER BY trade_time, trade_id
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
"""

TRAINING_QUERY = _BASE_QUERY + " ORDER BY trade_time, trade_id"
LATEST_ROW_QUERY = (
    _BASE_QUERY + " ORDER BY trade_time DESC, trade_id DESC LIMIT 1"
)


def _add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Единая точка вычисления производных фичей — используется и для
    батча обучения, и для одной строки на инференсе. Не дублируем формулу
    в двух местах, которые могли бы со временем разъехаться."""

    std_safe = df["rolling_std_30"].replace(0, pd.NA)

    # Насколько текущая цена отклонилась от своего скользящего среднего,
    # в единицах скользящего стандартного отклонения

    df["price_vs_mean"] = (df["price"] - df["rolling_mean_30"]) / std_safe
    return df


async def load_features(pool: asyncpg.Pool, ticker: str) -> pd.DataFrame:
    async with pool.acquire() as conn:
        rows = await conn.fetch(TRAINING_QUERY, ticker)
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
    df = _add_derived_columns(df)
    df["target"] = (df["next_price"] > df["price"]).astype("float")
    return df


async def load_latest_feature_row(
    pool: asyncpg.Pool, ticker: str
) -> "pd.Series | None":
    """Та же логика окон, что и для обучения (общий _BASE_QUERY), но
    только самая свежая строка — для инференса в /predict."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(LATEST_ROW_QUERY, ticker)
    if row is None:
        return None
    df = pd.DataFrame([dict(row)])
    df = _add_derived_columns(df)
    return df.iloc[0]
