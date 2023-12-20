import numpy as np
import pandas as pd
from typing import List

from src.data import Yahoo
from src.execution.signal import DailySignal

HEAD = ['USMV', 'DGRO', 'QUAL', 'MTUM']
TAIL = ['VLUE', 'HDV']
ANNUAL, MAX_VOL, WINDOW = 252, 0.2, 30


class Factor(DailySignal):

    @property
    def tickers(self) -> List[str]:
        return HEAD + TAIL

    async def _update(self, notional: float) -> None:
        tickers = self.tickers
        returns = self.prices.pct_change().dropna()
        weights = dict(zip(HEAD + TAIL, [1 / len(HEAD)] * len(HEAD) + [-1 / len(TAIL)] * len(TAIL)))
        spread = returns.mul(weights).sum(axis=1)
        spy_price = await Yahoo().daily(tickers=['SPY'])
        spy_vol = (np.sqrt(ANNUAL) * spy_price.pct_change()['SPY'].rolling(WINDOW).std()).shift().loc[spread.index]
        self.weights = pd.DataFrame([pd.Series(weights) for _ in spread.index], index=spread.index)
        positions = self.weights.mul(notional, axis=0).div(self.prices).dropna().round().astype(int)
        df = pd.concat([spy_vol, positions], axis=1)
        for ticker in tickers:
            df[ticker] = pd.Series(np.where(df.SPY < MAX_VOL, df[ticker], 0), index=df.index)
        self.positions = df[tickers]
