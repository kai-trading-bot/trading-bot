import numpy as np
import pandas as pd

from overrides import overrides
from typing import *

from src.data import Yahoo
from src.execution.signal import DailySignal
from src.utils.fe import ANNUAL

BOUND = 0
BUCKETS = 5
NOTIONAL = 10000
QUANTILE = 'q'
SIGNAL = 'signal'
SPREAD = 'spread'
BOND, EQUITY = ['TMF'], ['TQQQ', 'UDOW', 'SPXL']
TICKERS = BOND + EQUITY
WINDOW = 20
VOLATILITY = 'VOLATILITY'


class Cubby(DailySignal):

    @property
    def tickers(self) -> List[str]:
        return BOND + EQUITY

    @overrides
    async def _update(self, notional: float) -> None:
        prices = self.prices
        returns = prices.pct_change()
        weights = dict(zip(BOND + EQUITY, [0.5] + [(1 - 0.5) / len(EQUITY)] * len(EQUITY)))
        spread = pd.Series(np.dot(returns, pd.Series(weights).T), index=returns.index)
        weights = pd.DataFrame([list(weights.values())] * len(prices), index=prices.index, columns=prices.columns)
        positions = weights.mul(notional, axis=0).div(prices, axis=1).dropna().round().astype(int)
        vol = np.sqrt(ANNUAL) * returns[TICKERS[0]].rolling(WINDOW).std().dropna()
        sig = pd.concat([vol.shift(), pd.qcut(vol, BUCKETS, [str(j) for j in range(BUCKETS)]).shift(), spread], axis=1,
                        keys=[VOLATILITY, QUANTILE, SPREAD]).dropna()
        signal = pd.concat([sig, positions], axis=1, ).dropna()
        signal[SIGNAL] = np.where(signal[QUANTILE] >= str(BOUND), 1, 0)
        signal[TICKERS] = signal[TICKERS].mul(signal[SIGNAL], axis=0)
        self.positions = signal[TICKERS]
        self.weights = weights
