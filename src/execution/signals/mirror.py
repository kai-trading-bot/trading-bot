import pandas as pd
from typing import List

from src.analytics.signal import Combination
from src.execution.signal import DailySignal

PAIRS = {
    'SOXL': 'SOXS',
    'UDOW': 'SDOW',
    'SPXL': 'SPXS',
    'TQQQ': 'SQQQ',
}

MIN_WEIGHT = 1 / len(PAIRS.keys())


class Mirror(DailySignal):

    @property
    def tickers(self) -> List[str]:
        return list(PAIRS.keys()) + list(PAIRS.values())

    async def _update(self, notional: float) -> None:
        pairs = [list(pair) for pair in list(zip(PAIRS.keys(), PAIRS.values()))]
        returns = self.prices.pct_change()
        signal = pd.concat([returns[pair].mean(axis=1) for pair in pairs], axis=1).dropna()
        weights = -Combination().spo(signals=signal.shift().dropna(), min_weight=MIN_WEIGHT)
        weights = pd.Series(dict(dict(zip(PAIRS.keys(), weights)), **dict(zip(PAIRS.values(), weights))))
        self.weights = pd.DataFrame([pd.Series(weights) for _ in self.prices.index], index=self.prices.index)
        self.positions = self.weights.mul(notional, axis=0)\
            .div(self.weights.abs().sum(axis=1), axis=0)\
            .div(self.prices).dropna().round().astype(int)
