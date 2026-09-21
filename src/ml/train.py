import asyncio
import logging

import asyncpg
import joblib
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from src.config import get_settings
from src.features.lag_features import FEATURE_COLUMNS, load_features

logger = logging.getLogger(__name__)

ZERO_VARIANCE_STD_THRESHOLD = 1e-8


def check_feature_variance(X_train: pd.DataFrame, ticker: str) -> None:
    """Признак с нулевой дисперсией в обучающей выборке физически не может
    нести сигнал для линейной модели — StandardScaler не упадёт (просто
    оставит его на 0), но и толку от него не будет. Явно предупреждаем
    об этом."""
    stds = X_train.std()
    dead_features = stds[stds < ZERO_VARIANCE_STD_THRESHOLD].index.tolist()
    if dead_features:
        logger.warning(
            "%s: признаки без вариации в текущей обучающей выборке "
            "(не несут сигнала прямо сейчас): %s — скорее всего, "
            "недостаточно данных по разнообразию (например, все сделки "
            "пришлись на один час), а не ошибка в вычислении.",
            ticker,
            dead_features,
        )


def temporal_split(df: pd.DataFrame, test_fraction: float = 0.2):
    """Не train_test_split со случайным перемешиванием, т.к. данные уже
    отсортированы по (trade_time, trade_id) в SQL-запросе. Модель
    обучается на прошлом, проверяется на будущем, а не наоборот."""
    split_idx = int(len(df) * (1 - test_fraction))
    return df.iloc[:split_idx], df.iloc[split_idx:]


async def train_for_ticker(pool: asyncpg.Pool, ticker: str) -> dict:
    df = await load_features(pool, ticker)
    df = df.dropna(subset=FEATURE_COLUMNS + ["target"])

    if len(df) < 100:
        logger.warning(
            "Слишком мало данных по %s (%d строк) — пропускаю. "
            "Дай live_producer поработать дольше и вернись позже.",
            ticker,
            len(df),
        )
        return {}

    df["target"] = df["target"].astype(int)
    train_df, test_df = temporal_split(df)
    X_train, y_train = train_df[FEATURE_COLUMNS], train_df["target"]
    X_test, y_test = test_df[FEATURE_COLUMNS], test_df["target"]

    check_feature_variance(X_train, ticker)

    pipeline = Pipeline(
        [
            ("scaler", StandardScaler()),
            ("clf", LogisticRegression(class_weight="balanced", max_iter=1000)),
        ]
    )
    pipeline.fit(X_train, y_train)

    y_proba = pipeline.predict_proba(X_test)[:, 1]
    y_pred = pipeline.predict(X_test)

    metrics = {
        "ticker": ticker,
        "n_train": len(train_df),
        "n_test": len(test_df),
        "target_rate_train": round(y_train.mean(), 3),
        "target_rate_test": round(y_test.mean(), 3),
        "majority_baseline_accuracy": round(1 - y_test.mean(), 3),
        "accuracy": round(accuracy_score(y_test, y_pred), 4),
        "roc_auc": (
            round(roc_auc_score(y_test, y_proba), 4)
            if y_test.nunique() > 1
            else None
        ),
        "pr_auc": (
            round(average_precision_score(y_test, y_proba), 4)
            if y_test.nunique() > 1
            else None
        ),
        "precision": round(precision_score(y_test, y_pred, zero_division=0), 4),
        "recall": round(recall_score(y_test, y_pred, zero_division=0), 4),
    }
    logger.info("Метрики %s: %s", ticker, metrics)

    joblib.dump(
        {
            "pipeline": pipeline,
            "features": FEATURE_COLUMNS,
            "ticker": ticker,
            "metrics": metrics,
        },
        f"models/direction_{ticker}.pkl",
    )
    return metrics


async def main() -> None:
    settings = get_settings()
    pool = await asyncpg.create_pool(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        database=settings.DB_NAME,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
    )
    for ticker in settings.tickers_list:
        await train_for_ticker(pool, ticker)
    await pool.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
