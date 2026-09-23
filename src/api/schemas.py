from datetime import datetime
from typing import Literal

from pydantic import BaseModel


class TradeOut(BaseModel):
    trade_id: int
    ticker: str
    trade_time: datetime
    price: float
    quantity: int
    side: Literal["buy", "sell"]
    source: Literal["live", "replay"]


class VolatilityPoint(BaseModel):
    trade_time: datetime
    price: float
    rolling_mean: float | None
    rolling_volatility: float | None


class PredictionOut(BaseModel):
    ticker: str
    as_of_trade_id: int
    as_of_trade_time: datetime
    probability_up: float
    pr_auc_of_model: float | None
