import warnings
from overrides import overrides

from src.analytics.signal import Signal
from src.data import Yahoo
from src.execution.analysis import Analysis
from src.execution.plotting import *
from src.execution.utils import *
from src.utils.logger import logger

warnings.simplefilter(action='ignore')


class BaseSignal:

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @property
    def backtestable(self) -> bool:
        return True

    def __str__(self) -> str:
        return f'Signal(name={self.name})'

    def __repr__(self) -> str:
        return self.__str__()


class DailySignal(BaseSignal):

    def __init__(self) -> None:
        self.prices = None
        self.positions = None
        self.weights = None
        self.notional = None
        self.analyzable = False

    @property
    def tickers(self) -> List[str]:
        raise NotImplementedError()

    async def fetch(self, force_update: bool = False) -> None:
        """ Fetch daily close prices for tickers. """
        if self.prices is None or force_update:
            logger.info(f'Fetching {self.name}')
            self.prices = await self._fetch()

    async def _fetch(self) -> pd.DataFrame:
        return await Yahoo().daily(tickers=self.tickers)

    def set_prices(self, prices: float) -> None:
        self.prices = prices[self.tickers]

    async def update(self, notional: float) -> None:
        self.notional = notional
        await self._update(notional)
        self.analyzable = True

    async def _update(self, notional: float) -> None:
        raise NotImplementedError()

    def get_trade_list(self, date: str) -> Dict[str, int]:
        try:
            logger.info(f'Getting trade list for {date}')
            return self.positions.loc[date].to_dict()
        except Exception as e:
            logger.exception(f'Unable to get trade list for {date}: {e}\nPositions:\n{self.positions}')
            return dict()

    async def backtest(self, notional: float) -> None:
        if self.backtestable:
            await self.fetch()
            await self.update(notional)
            self.analyzable = True
        else:
            logger.warning(f'Signal {self.name} is not backtestable.')

    @property
    def signal(self) -> pd.Series:
        return pd.Series(self.prices.mul(self.weights).sum(axis=1), name=self.name)

    @property
    def pnl(self) -> pd.Series:
        # PnL is calculated by the price difference between day t and day t-1 multiply day t-1's position.
        # Because the rebalance happens near market close.
        return pd.Series(
            self.prices.diff().mul(self.positions.shift())
                .dropna(axis=1, how='all')  # Drop column whose values are all NaN.
                .dropna()
                .sum(axis=1), name=self.name)

    @property
    def returns(self) -> pd.Series:
        return self.pct_return()

    @property
    def tcost(self) -> pd.Series:
        return pd.Series(txn_cost(self.positions, self.prices), name=self.name + '_TCost')

    @property
    def net_pnl(self) -> pd.Series:
        return pd.Series(self.pnl.subtract(self.tcost, axis='index'), name=self.name + '_Net')

    @property
    def net_returns(self) -> pd.Series:
        return pd.Series(self.net_pnl.div(self.notional), name=self.name + '_Net').dropna()

    def pct_return(self) -> pd.Series:
        return pd.Series(self.pnl.div(self.notional), name=self.name).dropna()

    def yearly_stats(self, notional: bool = False, tcost: bool = False) -> pd.DataFrame:
        if tcost:
            df = self.net_pnl if notional else self.net_returns
        else:
            df = self.pnl if notional else self.returns
        return Analysis.yearly_stats(df, notional=notional)

    def monthly_stats(self, ytd: bool = True, notional: bool = False) -> pd.DataFrame:
        df = self.pnl if notional else self.pct_return()
        if ytd:
            df = df.loc[str(datetime.today().year)]
        return Analysis.monthly_stats(df, notional=notional)

    def cumulative_returns(self,
                           notional: bool = False,
                           ytd: bool = True,
                           yearly: bool = False,
                           rolling: int = 0,
                           tcost: bool = False,
                           title: Optional[str] = None,
                           save: bool = False) -> Optional[str]:
        returns = self.pnl if notional else self.returns
        if tcost:
            net_returns = self.net_pnl if notional else self.net_returns
            returns = pd.concat([returns, net_returns], axis=1)
        if rolling:
            returns = returns.iloc[-rolling:]
        if ytd and not yearly and not rolling:
            year = str(datetime.today().year)
            returns = returns.loc[year]
        if yearly:
            plot_cumulative_returns_by_year(returns, notional=notional, title=title)
        else:
            plot_cumulative_returns(returns, notional=notional, title=title)
        if save:
            return save_plot(f"{self.name}_cum_ret_{notional}_{ytd}_{yearly}_{rolling}")

    def turnover(self, ax: plt.Axes = None) -> None:
        ax = ax or plt.gca()
        turnover = Signal.turnover(self.prices, self.positions)
        turnover.plot(figsize=(12, 6), ax=ax, label=self.name)


class Long(DailySignal):

    def __init__(self, ticker: str):
        super().__init__()
        self.ticker = ticker

    @property
    def name(self) -> str:
        return self.ticker

    @property
    def tickers(self) -> List[str]:
        return [self.ticker]

    @overrides
    async def _update(self, notional) -> None:
        self.weights = pd.DataFrame([1] * len(self.prices), index=self.prices.index, columns=self.tickers)
        self.positions = self.weights.mul(notional, axis=0).div(self.prices).dropna().round().astype(int)
