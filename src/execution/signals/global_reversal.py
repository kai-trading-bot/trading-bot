import itertools
import numpy as np
import pandas as pd
from overrides import overrides
from typing import List

from src.analytics.signal import Signal
from src.execution.signal import DailySignal
from src.utils.logger import logger

MSCI = ['EWJ', 'EWZ', 'EWT', 'EWG', 'EWH', 'EWI', 'EWW', 'EWU', 'EWY',
        'EWA', 'EWM', 'EWS', 'EWC', 'EWP', 'EWL']
ASIA = ['EWJ', 'EWT', 'EWH']
WINDOW = 256


def reversal(prices: pd.DataFrame, window: int = 5):
    return -np.log(prices / prices.rolling(window).max())


class GlobalReversalBase(DailySignal):

    def __init__(self, tickers: List[str], window: int = 5):
        super().__init__()
        self._tickers = tickers
        self.window = window

    @property
    def tickers(self) -> List[str]:
        return self._tickers

    @overrides
    async def _update(self, notional: float) -> None:
        signal = reversal(self.prices, self.window)
        self.weights = Signal.holdings(signal, pad=False, xs=True)
        self.positions = self.weights.mul(notional, axis=0).div(self.prices).dropna().round().astype(int)


class GlobalReversal(GlobalReversalBase):

    def __init__(self):
        super().__init__(ASIA, WINDOW)


async def search(notional: float):
    """ Find the optimal symbol and window combination. """
    self = GlobalReversalBase(MSCI)
    await self.fetch()
    prices = self.prices
    dfs = []
    for n in range(2, len(MSCI)):
        data = []
        pairs = list(itertools.combinations(MSCI, n))
        logger.info(f'Generating {n} with {len(pairs)} pairs.')
        for i in range(1, 9):
            window = 2 ** i
            logger.info(f'Generating with window size: {window}')
            for pair in pairs:
                tickers = list(pair)
                signal = GlobalReversalBase(tickers, window)
                signal.prices = prices[tickers]
                await signal.update(notional)
                stats = signal.yearly_stats()
                avg_sharpe = stats['Sharpe'].drop(stats['Sharpe'].idxmax()).mean()
                if avg_sharpe >= 1:
                    info = dict(
                        Pair=tickers,
                        Window=window,
                        Sharpe=avg_sharpe,
                        Return=stats['Return'].mean(),
                        Min=stats['Min'].mean(),
                        Max=stats['Max'].mean(),
                        Win=stats['Win'].mean(),
                    )
                    data.append(info)
            logger.info(f'Finished window size {i} with cumulative size {len(data)}')
        df = pd.DataFrame(data)
        print(df.head())
        dfs.append(df)
    return pd.concat(dfs)
