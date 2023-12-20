import numpy as np
import pandas as pd

from typing import *

from src.execution.signal import DailySignal

NOTIONAL = 10000
THRESHOLD, WINDOW = 0, 10
SIGNAL = 'signal'


class Butterfly(DailySignal):

    @property
    def tickers(self) -> List[str]:
        return ['QQQ', 'MDY', 'DIA', 'SPY']

    async def _update(self, notional: float) -> None:
        head = self.tickers[:-1]
        tail = self.tickers[-1:]
        prices = self.prices
        returns = prices.pct_change()
        weights = dict(zip(head + tail, [0.5 / len(head)] * len(head) + [-0.5]))
        self.weights = pd.DataFrame([pd.Series(weights) for _ in prices.index], index=prices.index)
        fly = returns[head].mean(axis=1).sub(returns[tail[0]])
        score = (fly - fly.rolling(WINDOW).mean()) / (fly.rolling(WINDOW).std())
        df = self.weights.mul(notional, axis=0).div(prices, axis=1).dropna().round().astype(int)
        df[SIGNAL] = pd.Series(np.where(score > THRESHOLD, -1, np.where(score < -THRESHOLD, 1, np.nan)),
                               index=score.index).fillna(0)
        df[head + tail] = df[head + tail].mul(df[SIGNAL], axis=0)
        self.positions = df[head + tail]


class Butterfly2X(Butterfly):

    @property
    def tickers(self) -> List[str]:
        return ['QLD', 'MVV', 'DDM', 'SSO']


class Butterfly3X(Butterfly):

    @property
    def tickers(self) -> List[str]:
        return ['TQQQ', 'MIDU', 'UDOW', 'SPXL']
