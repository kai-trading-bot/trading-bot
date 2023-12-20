import arrow
import ib_insync as ib

from dataclasses import dataclass
from datetime import datetime
from typing import *

from src.exceptions import QuoteException
from src.utils.logger import logger
from src.utils.time import to_pst

MAX_PERCENT_SPREAD = 0.05
ROUND: int = 3


@dataclass
class Quote:
    _bid: float
    _ask: float
    _timestamp: datetime = None  # TODO: make timestamp required
    symbol: str = ''
    exp: str = ''
    strike: float = 0.0
    cp: str = ''
    _delta: float = 0.0
    _gamma: float = 0.0
    _theta: float = 0.0
    _vega: float = 0.0
    _iv: float = 0.0
    _und_price: float = 0.0

    # Whether to check spread and throw exception if over MAX_PERCENT_SPREAD.
    # This is usually disabled in option quotes since the spread can be very wide.
    _check: bool = False

    def __init__(self, bid: float, ask: float, timestamp: Any = None, check: bool = False) -> None:
        self._bid = bid
        self._ask = ask
        self._timestamp = timestamp
        self._check = check

    @classmethod
    def from_ib(cls, ticker: ib.Ticker, check: bool = False) -> 'Quote':
        quote = cls(ticker.bid, ticker.ask, ticker.time, check)
        quote.symbol = ticker.contract.symbol
        quote.exp = ticker.contract.lastTradeDateOrContractMonth
        quote.strike = ticker.contract.strike
        quote.cp = ticker.contract.right
        if ticker.modelGreeks is not None:
            quote._delta = ticker.modelGreeks.delta
            quote._gamma = ticker.modelGreeks.gamma
            quote._theta = ticker.modelGreeks.theta
            quote._vega = ticker.modelGreeks.vega
            quote._iv = ticker.modelGreeks.impliedVol
            quote._und_price = ticker.modelGreeks.undPrice
        return quote

    @property
    def bid(self) -> float:
        if self._bid <= 0:
            raise QuoteException(f'Bid is invalid: {self._bid}')
        return round(self._bid, 2)

    @property
    def ask(self) -> float:
        if self._ask <= 0:
            raise QuoteException(f'Ask is invalid: {self._ask}')
        return round(self._ask, 2)

    @property
    def timestamp(self) -> datetime:
        if self._timestamp is None:
            raise QuoteException(f"Quote timestamp is None.")
        return arrow.get(self._timestamp).datetime

    @property
    def mid(self) -> float:
        price = self.get_price(0.5)
        self._check_spread(price)
        return price

    @property
    def erroneous(self) -> bool:
        return not (
            self._bid is not None and
            self._ask is not None and
            self._ask >= self._bid > 0
        )

    @property
    def spread(self) -> float:
        spread = self.ask - self.bid
        if spread < 0:
            raise QuoteException(f'Spread is less than zero: bid={self.bid}, ask={self.ask}')
        return spread

    @property
    def delta(self) -> float:
        # Note IB will return negative delta for put.
        assert -1 <= self._delta <= 1, f'Invalid delta: {self._delta}'
        return round(self._delta, ROUND)

    @property
    def gamma(self) -> float:
        # It's possible that gamma value is super small (even negative)
        # E.g.: -5.745827703180817e-14, so we can't assert gamma >= 0.
        return round(self._gamma, ROUND)

    @property
    def theta(self) -> float:
        return round(self._theta, ROUND)

    @property
    def vega(self) -> float:
        return round(self._vega, ROUND)

    @property
    def iv(self) -> float:
        return round(self._iv, ROUND)

    @property
    def und_price(self) -> float:
        assert self._und_price >= 0, f'Invalid underlying price: {self._und_price}'
        return round(self._und_price, ROUND)

    def to_dict(self) -> Dict:
        data = dict(symbol=self.symbol,
                    bid=self.bid,
                    ask=self.ask,
                    mid=self.mid)
        if self.exp and self.strike:
            data += dict(exp=self.exp,
                         strike=self.strike,
                         cp=self.cp,
                         delta=self.delta,
                         gamma=self.gamma,
                         theta=self.theta,
                         vega=self.vega,
                         iv=self.iv,
                         und_price=self.und_price)
        data['timestamp'] = to_pst(self.timestamp)
        return data

    def get_price(self, percentage: float) -> float:
        """ Get price based on the percentage of spread to bid. Formula: bid + spread * percentage

        :param percentage: percentage of spread within range [0, 1]
        :return: instrument price
        """
        if self.erroneous:
            raise QuoteException(f"Quote is erroneous: {self}")
        if percentage < 0:
            raise QuoteException(f"Percentage must be non negative: {percentage}")
        if percentage > 1:
            logger.warning(f"Percentage {percentage} is greater than 1, using ask price.")
            return self.ask
        price = self.bid + self.spread * percentage
        if price <= 0:
            raise QuoteException(f"Invalid price: {price}. quote={self}. percentage={percentage}")
        return round(price, 2)

    def _check_spread(self, price: float) -> None:
        if self._check:
            percent_spread = round(self.spread / price, 3)
            if percent_spread > MAX_PERCENT_SPREAD:
                raise QuoteException(f'{self.symbol} spread too large: Bid={self.bid}, Ask={self.ask}'
                                     'You might need to fetch it during regular market hours.')
