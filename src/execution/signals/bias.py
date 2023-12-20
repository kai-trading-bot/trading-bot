import cvxpy as cp
import numpy as np
import pandas as pd

from overrides import overrides
from tqdm import tqdm
from typing import *

from src.signals.optimizer import optimize_min_vol
from src.execution.signal import DailySignal
from src.utils.logger import logger
from src.utils.time import MarketCalendar, today

NOTIONAL = 100_000
LB, UB = 0.01, 0.15
GAMMA = 0.075
WINDOW = 252
ETF = ['USMV', 'QQQ']
BIG_TECH = ['AAPL', 'MSFT', 'AMZN', 'GOOG']
TECH = ['CRM']
FINANCIAL_SERVICES = ['SPGI', 'MSCI', 'ICE', 'CME', 'NDAQ']
TICKERS = ETF + BIG_TECH + TECH + FINANCIAL_SERVICES
FINANCIAL_SERVICES_WEIGHT = 0.35
MIN_WEIGHT = 0.0001
START_DATE = '2012-12-01'


def get_quarter_ends(start: str, end: str = None) -> List[str]:
    end = end if end is not None else today()
    rng = pd.date_range(start=start, end=end, freq='BQ')
    return [d.date().isoformat() for d in rng]


# Note this is not a daily signal.
class Bias(DailySignal):
    """ A quarterly rebalanced long-term portfolio with tech bias. """

    def __init__(self,
                 tickers: List[str] = TICKERS,
                 date: str = today(),
                 gamma: float = GAMMA,
                 lb: float = LB,
                 ub: float = UB,
                 window: float = WINDOW,
                 fractional: bool = False) -> None:
        """
        :param tickers: a list of tickers included in the portfolio
        :param gamma: risk factor (the higher the more risk averse)
        :param lb: lower bound for equity weight
        :param ub: upper bound for equity weight
        :param window: look back window
        :param fractional: whether to enable fractional trading.
                           Fractional trading is not available with IB API.
        """
        super().__init__()
        self.date = date
        self._tickers = tickers
        self._gamma = gamma
        self._lb = lb
        self._ub = ub
        self._window = window
        self._fractional = fractional

    @property
    @overrides
    def tickers(self) -> List[str]:
        return self._tickers

    @overrides
    async def _update(self, notional: float) -> None:
        dates = get_quarter_ends(start=START_DATE, end=self.date)
        weights = []
        for date in tqdm(dates):
            # Market is not necessarily open on each business quarter end date.
            bq_end = MarketCalendar.most_recent_open_date(date)
            w = self._compute_weight(bq_end)
            weights.append(pd.DataFrame([w]))
        # Remove rows that have all NaN values, and fill the rest NaN with 0.
        w = pd.concat(weights).dropna(how='all').fillna(0)
        self.weights = w.reindex(self.prices.index, method='ffill')
        self.positions = w.mul(notional).div(self.prices).fillna(method='ffill')
        if not self._fractional:
            self.positions = self.positions.dropna(axis=1, how='all').dropna().round().astype(int)

    @property
    def quarter_weights(self) -> pd.DataFrame:
        """ Return weights for each quarter. """
        return self.weights.drop_duplicates().tail().sort_index(axis=1)

    def _compute_weight(self, date: str) -> pd.Series:
        returns = self.prices.pct_change()
        # Drop any ticker that has NaN in the past WINDOW days.
        r = returns[returns.index <= date].tail(WINDOW).dropna(axis=1)
        w = self._optimize(r)
        weights = pd.Series(w, index=r.columns, name=pd.Timestamp(date)).sort_values(ascending=False)
        # Remove tickers that have too little weight.
        weights = weights[weights > MIN_WEIGHT]
        # Normalize the weights so that they sum to 1.
        weights = weights.div(weights.sum())
        return weights

    def _optimize(self, returns: pd.DataFrame) -> np.ndarray:
        """ Return an optimized weight matrix minimizing vol. """
        mu = returns.mean().T.values
        sigma = returns.cov()
        w = cp.Variable(mu.shape[0])
        tickers = list(returns.columns)
        # List of constraints for BIAS.
        extra_constraints = self.constraints(w, tickers, returns)
        return optimize_min_vol(mu, sigma, w=w, risk_factor=self._gamma,
                                w_lower=self._lb, w_upper=self._ub, constraints=extra_constraints)

    def constraints(self, w: cp.Variable, tickers: List[str], returns: pd.DataFrame) -> List:
        """ Return a list of constraints for BIAS optimization. To be override by subclasses.

        :param w: a cvxpy variable with dimension equals to size of tickers.
        :param tickers: a list of tickers
        :param returns: a dataframe that has daily returns of the given list of tickers.
        """
        return []


class BiasV1(Bias):
    """ Bias (V1) with a maximum sum of weights for tickers in financial services sector. """

    @overrides
    def constraints(self, w: cp.Variable, tickers: List[str], returns: pd.DataFrame) -> List:
        return Constraint(w, tickers, returns).max_weight(FINANCIAL_SERVICES, FINANCIAL_SERVICES_WEIGHT)


class BiasV2(Bias):
    """ Bias (V2) with a weight upper bound for each ticker based on previous quarter's vol. """

    HALFLIFE: int = 30

    @overrides
    def constraints(self, w: cp.Variable, tickers: List[str], returns: pd.DataFrame) -> List:
        return Constraint(w, tickers, returns).vol_bound(halflife=self.HALFLIFE)


class Constraint:
    """ Generate different constraints given weights and returns. """

    def __init__(self, w: cp.Variable, tickers: List[str], returns: pd.DataFrame):
        self._w = w
        self._tickers = tickers
        self.returns = returns

    def max_weight(self, tickers: List[str], max_weight: float) -> List:
        """ Bound the sum of weights for a subset of tickers by a constant. """
        assert 0 <= max_weight <= 1, f'Weight must be in [0, 1] range'
        subset = [self.w(ticker) for ticker in tickers]
        return [cp.sum(subset) <= max_weight]

    def vol_bound(self, halflife: int = 60) -> List:
        """
        Bound each individual ticker's weight by its previous quarter's volatility.
        Tickers with larger volatility will have smaller weight upper bounds and vice versa.
        """
        n = len(self.returns.columns)
        vols = (1 / self.returns.ewm(halflife=halflife).var()) ** (1 / n)
        weights = vols.div(vols.sum(axis=1), axis=0).iloc[-1].to_dict()
        return [self.w(ticker) <= ub for ticker, ub in weights.items()]

    def w(self, ticker: str):
        """ Return the weight variable for the given ticker. """
        if ticker not in self._tickers:
            logger.warning(f'Ticker {ticker} is not in the universe. Skip.')
        pos = self._tickers.index(ticker)
        return self._w[pos]
