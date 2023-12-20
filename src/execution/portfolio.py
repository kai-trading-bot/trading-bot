from src.data import Yahoo
from src.execution.analysis import Analysis
from src.execution.plotting import *
from src.execution.signal import DailySignal, Long
from src.execution.utils import *
from src.utils import catch_async
from src.utils.logger import logger

SPY = 'SPY'
PORTFOLIO = 'Portfolio'
PORTFOLIO_NET = 'Portfolio_Net'
TCOST = 'TCost'


class Portfolio:

    def __init__(self, signals: List[DailySignal], benchmark: str = SPY) -> None:
        if len(signals) == 0:
            raise Exception('Portfolio must have at least one signal.')
        self.signals = signals
        self.benchmark = Long(benchmark)
        self.tickers = flatten([signal.tickers for signal in self.signals])
        self.backtestable = all([signal.backtestable for signal in signals])
        self.notional = None
        self.prices = None
        self.positions = None
        self.weights = None

    @property
    def name(self) -> str:
        return self.__str__()

    @property
    def signal_names(self) -> str:
        return '_'.join([signal.name for signal in self.signals])

    def __str__(self):
        return f'Portfolio({self.signal_names})'

    async def fetch(self) -> None:
        if self.prices is None:
            prices = await Yahoo().daily(tickers=self.tickers)
            self.set_prices(prices)

    def set_prices(self, prices: pd.DataFrame) -> None:
        if not isinstance(prices.index, pd.core.indexes.datetimes.DatetimeIndex):
            prices.index = pd.to_datetime(prices.index)
        self.prices = prices[self.tickers]
        for signal in self.signals:
            signal.prices = self.prices[signal.tickers]

    async def update(self, notional: float) -> None:
        positions = pd.DataFrame()
        weights = pd.DataFrame()
        allocation = 1.0 / len(self.signals)
        for signal in self.signals:
            logger.info(f'Updating {signal.name}')
            await signal.update(notional * allocation)
            positions = positions.add(signal.positions, fill_value=0)
            if signal.weights is not None:
                weights = weights.add(signal.weights.mul(allocation, axis='index'), fill_value=0)
        self.positions = positions
        self.weights = weights

    async def run(self, notional: float) -> None:
        """ Main entry point for running the portfolio. """
        self.notional = notional
        await self.fetch()
        await self.update(notional)
        await self.run_benchmark(notional)

    async def backtest(self, notional: float) -> None:
        logger.warning('This method is deprecated. Please use `Portfolio.run` instead.')
        await self.run(notional)
        await self.run_benchmark()

    @catch_async('Failed to run benchmark')
    async def run_benchmark(self, notional: float) -> None:
        logger.info(f'Running portfolio benchmark: {self.benchmark}')
        await self.benchmark.backtest(notional)

    # -*- Trading -*-

    def get_trade_list(self, date: str) -> Dict[str, float]:
        """ Get the target positions for a given date. """
        if date not in self.positions.index:
            raise Exception(f'{date} not in trade positions:\n{self.positions}')
        return self.positions.loc[date].round(6).to_dict()

    def get_notional_pos(self, date: str) -> Dict[str, float]:
        """ Get the target notional values for a given date.
            This can be used for fractional trading.
        """
        if date not in self.weights.index:
            raise Exception(f'{date} is not present in the weight dataframe:\n{self.weights}')
        return self.weights.mul(self.notional).round(2).loc[date].to_dict()

    # -*- Analysis -*-

    def signal(self, name: str) -> DailySignal:
        mapping = {signal.name.lower(): signal for signal in self.signals}
        if name not in mapping:
            raise Exception(f'Signal {name} cannot be found in {list(mapping.keys())}')
        return mapping[name.lower()]

    @property
    def analyzable(self) -> bool:
        return all([signal.analyzable for signal in self.signals]) and self.benchmark.analyzable and self.backtestable

    @property
    def signal_pnl(self) -> pd.DataFrame:
        return pd.concat([signal.pnl for signal in self.signals], axis=1).dropna()

    @property
    def signal_returns(self) -> pd.DataFrame:
        # Signal return is calculated differently from returns. It represents the percent return
        # by each signal given the allocated notional, while the returns is calculated by using
        # the notional return of each signal divided by the total portfolio notional amount.
        return pd.concat([signal.pct_return() for signal in self.signals], axis=1).dropna()

    @property
    def pnl(self) -> pd.Series:
        return pd.Series(self.signal_pnl.sum(axis=1), name=PORTFOLIO)

    @property
    def tcost(self) -> pd.Series:
        return pd.Series(txn_cost(self.positions, self.prices), name=PORTFOLIO + '_TCOST')

    @property
    def net_pnl(self) -> pd.Series:
        return pd.Series(self.pnl.subtract(self.tcost, axis='index'), name=PORTFOLIO_NET)

    @property
    def pnls(self) -> pd.DataFrame:
        return pd.concat([self.pnl, self.net_pnl, self.benchmark.pnl, self.signal_pnl], axis=1).dropna()

    @property
    def returns(self) -> pd.DataFrame:
        return self.pnls.div(self.notional).dropna()

    def yearly_stats(self, notional: bool = False) -> pd.DataFrame:
        returns = self.pnls if notional else self.returns
        return Analysis.yearly_stats(returns[PORTFOLIO], notional=notional)

    def monthly_stats(self, notional: bool = False) -> pd.DataFrame:
        returns = self.pnls if notional else self.returns
        return Analysis.monthly_stats(returns[PORTFOLIO], notional=notional)

    def cumulative_returns(self,
                           notional: bool = False,
                           ytd: bool = True,
                           decompose: bool = False,
                           yearly: bool = False,
                           rolling: int = 0,
                           title: Optional[str] = None,
                           start: Optional[str] = None,
                           end: Optional[str] = None,
                           tcost: bool = False,
                           save: bool = False) -> Optional[str]:
        df = self.pnls if notional else self.returns
        if rolling:
            df = df.iloc[-rolling:]
        if start:
            df = df.loc[start:]
        if end:
            df = df.loc[:end]
        if decompose:
            returns = df.drop([self.benchmark.name, PORTFOLIO_NET], axis=1)
        else:
            columns = [PORTFOLIO, PORTFOLIO_NET, self.benchmark.name] if tcost else [PORTFOLIO, self.benchmark.name]
            returns = df[columns]
        if ytd and not yearly and not rolling and not start:
            year = str(datetime.today().year)
            returns = returns.loc[year]
        if yearly:
            plot_cumulative_returns_by_year(returns, notional=notional, title=title)
        else:
            plot_cumulative_returns(returns, notional=notional, title=title)
        if save:
            return save_plot(f'{self.name}_cumulative_returns_{notional}_{ytd}_{decompose}_{yearly}')

    def cumulative_signal_returns(self, ytd: bool = True, rolling: int = 0, save: bool = False) -> Optional[str]:
        """ Compare the portfolio's percent return with each signal's percent return. """
        signal_returns = self.signal_returns
        portfolio_return = self.returns[PORTFOLIO]
        if rolling:
            signal_returns = signal_returns.iloc[-rolling:]
            portfolio_return = portfolio_return.iloc[-rolling:]
        if ytd and not rolling:
            year = str(datetime.today().year)
            signal_returns = signal_returns.loc[year]
            portfolio_return = portfolio_return.loc[year]
        plot_cumulative_returns_by_signal(signal_returns, portfolio_return, notional=False)
        if save:
            return save_plot(f'{self.name}_cumulative_signal_return')

    def corr(self, other: pd.Series, overall: bool = True, yearly: bool = False) -> Union[float, pd.DataFrame]:
        if overall:
            return round(self.returns[PORTFOLIO].corr(other), 4)
        returns = self.returns.drop([PORTFOLIO_NET], axis=1)
        return Analysis.correlation(returns, other, yearly)

    def summary_stats(self, plot: bool = True, notional: bool = False) -> pd.DataFrame:
        """ Plot (if true) and return a dataframe with summary stats in different windows. """
        df = self.pnls if notional else self.returns
        summary = Analysis.summarize(df[PORTFOLIO], notional=notional)
        reference = Analysis.summarize(df[self.benchmark.name], notional=notional)
        if plot:
            plot_summary_stats(summary, reference, notional=notional)
        return pd.concat([summary, reference], keys=[PORTFOLIO, self.benchmark.name], axis=1)

    def underwater(self, ytd: bool = False, start: Optional[Union[int, str]] = None) -> None:
        returns = self.returns[PORTFOLIO]
        start = str(start) if start is not None else str(datetime.today().year) if ytd else None
        if start is not None:
            returns = returns.loc[start:]
        plot_underwater(Analysis.underwater(returns))

    def drawdowns(self, top: int = 5, ytd: bool = False, start: Optional[Union[int, str]] = None) -> None:
        returns = self.returns[PORTFOLIO]
        start = str(start) if start is not None else str(datetime.today().year) if ytd else None
        if start is not None:
            returns = returns.loc[str(start):]
        plot_drawdowns(returns, Analysis.drawdowns(returns, top))

    def correlations(self):
        returns = self.returns[[PORTFOLIO, self.benchmark.name]]
        plot_correlations(returns, self.benchmark.name)

    def holdings(self, date: Optional[str], save: bool = False) -> Optional[str]:
        """ Plot the notional holdings for the given date. """
        date = date if date is not None else self.positions.iloc[-1].index
        positions = self.positions.loc[date]
        prices = self.prices.loc[date]
        plot_holdings(positions, prices, title=f'Holdings by Notional Value ({date})')
        if save:
            return save_plot(f'{self.name}_holdings')
