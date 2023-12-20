import itertools
import pandas as pd
from typing import List

from src.analytics.signal import Signal
from src.execution.signal import DailySignal

DATE_BUFFER = 0.2
THRESHOLD = 0.5
HEDGE_WINDOW = 252
TICKERS = ['UBT', 'TYD', 'TMF', 'UST']


class Slope(DailySignal):

    @property
    def tickers(self) -> List[str]:
        return TICKERS

    async def _update(self, notional: float) -> None:
        target = pd.DataFrame(0, index=self.prices.index, columns=self.tickers)
        pairs = [list(pair) for pair in list(itertools.combinations(list(self.prices.columns), 2))]
        returns = self.prices.pct_change()
        corr = 1 / returns.rolling(HEDGE_WINDOW).corr()
        for pair in pairs:
            sub = returns[pair].dropna()
            leg_1, leg_2 = pair
            hr = corr.xs(leg_1, level=1)[leg_2]
            signal = sub[leg_2] - hr * sub[leg_1]
            holdings = Signal.holdings(signal=signal, xs=False, threshold=THRESHOLD)
            target[leg_2] += holdings
            target[leg_1] -= holdings
        self.weights = target.div(target.abs().sum(axis=1), axis=0)
        self.positions = self.weights.mul(notional).div(self.prices).round().fillna(method='ffill').dropna().astype(int)
