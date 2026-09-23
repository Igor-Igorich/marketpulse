import pandas as pd
from fastapi import APIRouter, HTTPException

from src.api import state
from src.api.schemas import PredictionOut
from src.features.lag_features import load_latest_feature_row

router = APIRouter()


@router.get("/predict/{ticker}", response_model=PredictionOut)
async def predict_direction(ticker: str):
    ticker = ticker.upper()

    if ticker not in state.models:
        raise HTTPException(
            404,
            f"Модель для {ticker} недоступна (недостаточно накопленных "
            f"live-сделок на момент обучения)",
        )

    row = await load_latest_feature_row(state.pool, ticker)
    if row is None:
        raise HTTPException(404, f"Нет сделок по {ticker} в базе")

    model_bundle = state.models[ticker]
    feature_values = row[model_bundle["features"]]

    if feature_values.isna().any():
        raise HTTPException(
            503,
            f"Недостаточно истории по {ticker} для расчёта всех признаков "
            f"(нужно минимум 30 предыдущих сделок подряд)",
        )

    X = pd.DataFrame([feature_values.to_dict()])
    proba_up = model_bundle["pipeline"].predict_proba(X)[0, 1]

    return PredictionOut(
        ticker=ticker,
        as_of_trade_id=int(row["trade_id"]),
        as_of_trade_time=row["trade_time"],
        probability_up=round(float(proba_up), 4),
        pr_auc_of_model=model_bundle["metrics"].get("pr_auc"),
    )
