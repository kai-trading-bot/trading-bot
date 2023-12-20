import pandas as pd

from src.analytics.signal import Signal, TimeSeries
from src.constant import *


def reversal(prices: pd.DataFrame, notional: int = 10000, window: int = 20) -> pd.DataFrame:
    universe = list(prices.keys())
    signals = []
    for symbol in universe:
        signal = -(prices[symbol] - TimeSeries.ewma(prices[symbol], window))
        signals.append(signal)
    signal = pd.concat(signals, axis=1, keys=universe)
    # TODO: handle missing bar values (ffill)
    holdings = Signal.holdings(signal, xs=True, pad=False)
    positions = holdings.mul(notional).div(prices).round().dropna()
    return positions


def book_pressure(quotes: pd.DataFrame, window: int = 100) -> pd.Series:
    ask_volume = quotes[ASK_PRICE] * quotes[ASK_SIZE]
    bid_volume = quotes[BID_PRICE] * quotes[BID_SIZE]
    spread = (ask_volume - bid_volume) / (ask_volume + bid_volume)
    signal = TimeSeries.ewma(spread, window)
    return signal
