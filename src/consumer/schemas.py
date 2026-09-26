from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, field_validator


class TradeMessage(BaseModel):
    trade_id: int
    ticker: str
    board: str
    trade_time: datetime
    session_date: date
    price: float
    quantity: int
    value: float
    side: Literal["buy", "sell"]
    source: Literal["live", "replay"]

    @field_validator("trade_time")
    @classmethod
    def ensure_naive(cls, v: datetime) -> datetime:
        return v.replace(tzinfo=None) if v.tzinfo is not None else v
