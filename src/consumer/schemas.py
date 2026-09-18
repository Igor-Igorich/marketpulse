from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel


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
