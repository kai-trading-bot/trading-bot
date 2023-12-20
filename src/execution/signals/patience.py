import pandas as pd
import matplotlib.pyplot as plt

from typing import *

from src.execution.signal import DailySignal


class Patience(DailySignal):
    """
    Are you patient enough to hold your position until it recovers?

    Enter: (P - P_max) / P_max <= THRESHOLD
    Exit: P = P_max
    """

    def __init__(self, ticker: str, threshold: float = -0.2):
        super().__init__()
        self._ticker = ticker
        self.threshold = threshold

    @property
    def tickers(self) -> List[str]:
        return [self._ticker]

    @property
    def name(self) -> str:
        return f'Patience({self._ticker})'

    async def _update(self, notional: float) -> None:
        price = self.prices[self._ticker]
        # Assume rebalance near market close. No shift needed.
        signal = (price - price.cummax()) / price.cummax()
        signal.name = 'signal'
        df = pd.DataFrame(signal)
        df.loc[df.signal <= self.threshold, 'weight'] = 1
        df.loc[df.signal == 0, 'weight'] = 0
        df.weight = df.weight.fillna(method='ffill')
        self.weights = pd.Series(df.weight, name=self._ticker).to_frame()
        self.positions = self.weights.mul(notional).div(self.prices).dropna().round().astype(int)

    def plot_signal(self):
        """ Note: only run this after await self.fetch(). """
        ((self.prices - self.prices.cummax()) / self.prices.cummax()).plot()

    async def show_performance(self, notional: float = 10000, yearly: bool = False):
        """ Display in Jupyter notebook the effect of different thresholds. """
        from src.utils.jupyter import display_dfs
        await self.fetch()
        thresholds = [-0.05, -0.1, -0.15, -0.2, -0.25, -0.3]
        sharpes = []
        returns = []
        for threshold in thresholds:
            self.threshold = threshold
            await self.update(notional)
            self.cumulative_returns(yearly=yearly)
            stats = self.yearly_stats()
            sharpes.append(pd.Series(stats.Sharpe, name=str(threshold)))
            returns.append(pd.Series(stats.Return, name=str(threshold)))
        plt.legend([str(t) for t in thresholds])
        display_dfs([pd.DataFrame(sharpes).T, pd.DataFrame(returns).T], ['Sharpe', 'Return'])
