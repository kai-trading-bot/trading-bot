import pandas as pd

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from src.utils.logger import logger


@dataclass
class Trade:
    date: str
    symbol: str
    side: str
    qty: float
    price: float
    timestamp: datetime
    fee: float = 0.0
    signal: Optional[str] = None
    broker: Optional[str] = None
    account: Optional[str] = None
    total_notional: Optional[float] = None

    def __init__(self, symbol: str, side: str, qty: float, price: float, timestamp: Any) -> None:
        if side.lower() not in ('buy', 'sell'):
            raise Exception('Invalid trade side')

        if qty < 0 or price < 0:
            raise Exception(f'Trade quantity and price cannot be negative')

        t = pd.Timestamp(timestamp)
        if t.tz is None:
            logger.warning(f'Trade timestamp does not have a timezone: {timestamp}')

        self.symbol = symbol
        self.side = side
        self.qty = qty
        self.price = price
        self.date = t.date().isoformat()  # No timezone info.
        self.timestamp = t.to_pydatetime()

    @property
    def notional(self):
        return self.price * (1 if self.side == 'buy' else -1)
