import ib_insync as ib

from dataclasses import dataclass

from src.constant import *
from src.utils.time import now

SUBMITTED = 'submitted'
FILLED = 'filled'
CANCELLED = 'cancelled'
REJECTED = 'rejected'  # submitted -> trying -> rejected
FAILED = 'failed'  # submitted -> failed due to wrong parameter


@dataclass
class Leg:
    """ Option leg """
    ticker: str
    delta: float
    dte: int
    cp: str = CALL
    multiplier: int = 100


class Trade:
    trades: List[ib.Trade]
    status: str = SUBMITTED
    created_at: datetime = now()
    updated_at: datetime = now()
