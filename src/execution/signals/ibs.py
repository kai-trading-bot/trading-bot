import numpy as np
import pandas as pd

from typing import *

from src.data import Yahoo
from src.execution.signal import DailySignal

KEYS = ['Close', 'High', 'Low']
TICKERS = ['SPXL', 'TMF', 'TQQQ', 'UDOW']
WEIGHTS = {'TMF': 0.5, 'SPXL': 0.5 / 3, 'TQQQ': 0.5 / 3, 'UDOW': 0.5 / 3}


def ibs(data: pd.DataFrame) -> pd.Series:
    return ((data.Close - data.Low) / (data.High - data.Low)) - 0.5


class IBS(DailySignal):

    @property
    def tickers(self) -> List[str]:
        return TICKERS

    async def _update(self, notional: float) -> None:
        tickers = self.tickers
        HEAD = tickers[0]
        [close, high, low] = [await Yahoo().daily(tickers=[HEAD], field=key) for key in KEYS]
        prices = await Yahoo().daily(tickers=tickers)
        data = pd.concat([close[HEAD], high[HEAD], low[HEAD]], keys=KEYS, axis=1)
        signal = ibs(data).shift().to_frame().rename(columns={0: 'signal'})
        self.weights = pd.DataFrame([pd.Series(WEIGHTS) for _ in signal.index], index=signal.index)
        positions = self.weights.mul(notional, axis=0).div(prices).dropna().round().astype(int)
        df = pd.concat([signal, positions], axis=1)
        for ticker in tickers:
            df[ticker] = pd.Series(np.where(df.signal > 0, df[ticker], 0), index=df.index)
        self.positions = df[tickers]
