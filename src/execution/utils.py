import asyncio
import numpy as np
import pandas as pd
import random

from functools import reduce, update_wrapper
from operator import add
from typing import *

from src.constant import OrderSide
from src.model import Quote


def drop_duplicated_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.loc[:, ~df.columns.duplicated()]


def drop_zero_columns(df: pd.DataFrame) -> pd.DataFrame:
    """ Drop all columns with 0 value """
    return df.loc[:, (df != 0).any(axis=0)]


def flatten(array: List[List[Any]]) -> List[Any]:
    return list(set(reduce(add, array)))


def to_float(value: Union[str, float, int], precision: int = 2) -> float:
    return round(float(value), precision)


def parse(text: str, separator: str = ',') -> List[str]:
    return [word.strip() for word in text.split(separator)]


def chunk(seq: List, size: int) -> Iterable:
    """ Return sequence in 'chunks' of size defined by size """
    return (seq[pos: pos + size] for pos in range(0, len(seq), size))


def dedup(data: List) -> List:
    return list(set(data))


def random_id(digits: int = 3) -> int:
    lower, upper = max(digits - 1, 0), digits
    return random.randint(10 ** lower, 10 ** upper)


def id_from_str(s: str, digit: int = 3) -> int:
    """ Generate a unique id for IB connection. """
    return abs(hash(s)) % (10 ** digit)


def run(future: Awaitable) -> Any:
    loop = asyncio.get_event_loop()
    task = asyncio.ensure_future(future)
    return loop.run_until_complete(task)


def coro(f: Callable[..., Awaitable]):
    def wrapper(*args, **kwargs):
        loop = asyncio.get_event_loop()
        task = asyncio.ensure_future(f(*args, **kwargs))
        return loop.run_until_complete(task)
    return update_wrapper(wrapper, f)


def get_side(quantity: float):
    """ Get order side given quantity. """
    return OrderSide.BUY if quantity > 0 else OrderSide.SELL


def get_mid_price(quote: Quote):
    """ Get mid price given quote """
    return quote.mid


def get_diff(current: Dict[str, float], target: Dict[str, float], threshold: float = 1e-06) -> Dict[str, float]:
    """ Get the difference between the current and target position. """
    trades = dict()
    for ticker in target.keys():
        delta = target[ticker] - current.get(ticker, 0.0)
        if abs(delta) > threshold:
            trades[ticker] = round(delta, 6)
    return trades


def round_2(num: float) -> float:
    return round(num, 2)


def txn_cost(positions: pd.DataFrame, prices: pd.DataFrame, threshold: int = 5) -> pd.Series:
    """ Calculate transaction cost based on IB's pricing model. """
    qty = positions.diff().abs().dropna()
    notional = qty.mul(prices).dropna()
    min_cost = pd.DataFrame(1.0, index=qty.index, columns=qty.columns)
    share_cost = qty.mul(0.005)
    notional_cost = notional.mul(0.01)
    cost = min_cost.combine(share_cost.combine(notional_cost, np.minimum), np.maximum)
    return pd.DataFrame(np.where(qty > threshold, cost, 0.0), index=qty.index, columns=qty.columns).sum(axis=1)
