import asyncio
import ib_insync as ib
import pandas as pd
import src.execution.utils as util

from datetime import timedelta
from dataclasses import asdict

from src.config import *
from src.constant import *
from src.execution.marketdata import MarketData
from src.utils.logger import get_logger


class InteractiveBroker:

    def __init__(self, host: str = IB_HOST, port: int = IB_LIVE_PORT, account: str = '') -> None:
        self.host = host
        self.port = port
        self.account = account
        self.ib = ib.IB()
        self.client_id = util.random_id()
        self.logger = get_logger(__class__.__name__)
        self.marketdata = MarketData(self.ib)

    def connect(self) -> None:
        self.ib.connect(host=self.host, port=self.port, clientId=self.client_id, timeout=60,
                        readonly=True)
        self.logger.info(f'IB Connected: {self.ib}')

    def disconnect(self) -> None:
        self.ib.disconnect()

    def get_accounts(self) -> List[str]:
        return self.ib.managedAccounts()

    def account_summary(self) -> Dict:
        """
        - AvailableFunds: This value tells what you have available for trading.
        - BuyingPower: with margin
        - EquityWithLoanValue: Total portfolio values.
        """
        summary = self.ib.accountSummary(self.account)
        data = {s.tag: s.value for s in summary}
        return data

    def portfolio(self, greeks: bool = False) -> pd.DataFrame:
        """
        Return the current portfolio.
        :param greeks: whether also fetch the quotes and greeks for the portfolio.
        """
        return util.run(self._fetch_portfolio(greeks))

    def positions(self) -> List[ib.Position]:
        return self.ib.positions(self.account)

    def futures(self, symbol: str) -> pd.DataFrame:
        """ Get a list of future contracts with the given symbol. E.g. ES, NQ """
        return util.run(self._fetch_futures(symbol))

    def options(self, symbol: str, percent_strike: float = 0.05, dte: int = 180) -> pd.DataFrame:
        """
        Get a list of stock options with quotes for the given symbol.
        :param symbol: stock symbol
        :param percent_strike: (1 - percent_strike, 1 + percent_strike) * market_price
        :param dte: maximum days to expiration from now (non-business days)

        E.g. options('SPY', percent_strike=0.05, dte=180)

        WARNING: this method can be slow.
        """
        return util.run(self._fetch_options(symbol, percent_strike, dte))

    def fop(self, symbol: str, expiration: str, percent_strike: float = 0.05) -> pd.DataFrame:
        """
        Get a list of future options with quotes for the given symbol and expiration date (YYYYMMDD).
        Note returned option contracts' strike prices are within
        (1 - percent_strike, 1 + percent_strike) * market_price of the symbol.
        E.g. fop('ES', '20210319', 0.05)

        **Warning** this method can be slow.
        """
        return util.run(self._fetch_fop(symbol, expiration, percent_strike))

    def quote(self,
              symbol: str,
              expiry: Optional[str] = None,
              strike: Optional[float] = None,
              cp: Optional[str] = None,
              fop: bool = False):
        """
        Get quote for a given stock / option / future.
        :param symbol: contract symbol
        :param expiry: contract expiration in YYYYMMDD format
        :param strike: option strike price
        :param cp: option right (C or P)
        :param fop: whether it's futures option
        :return:
        """
        return util.run(self._fetch_quote(symbol, expiry, strike, cp, fop))

    # -*- Private Methods -*-

    async def _fetch_portfolio(self, greeks: bool = False) -> pd.DataFrame:
        """
        Return the greeks for the current portfolio.
        - Stock: we have delta = 1 for long positions and -1 for short positions.
        - Options: we use the modelGreeks field from IB.
        - Futures: currently not supported.
        """
        items = self.ib.portfolio()
        data = await asyncio.gather(*[self._fetch_portfolio_item(item, greeks) for item in items])
        return pd.DataFrame(data)

    async def _fetch_portfolio_item(self, item: ib.PortfolioItem, greeks: bool = False) -> Dict:
        data = dict()
        contract = await self.marketdata.qualify_contract(item.contract)
        data[SYMBOL] = contract.symbol
        data[SEC_TYPE] = contract.secType
        data[POSITION] = item.position
        data[MARKET_PRICE] = round(item.marketPrice, 2)
        data[MARKET_VALUE] = item.marketValue  # position * market_price
        data[EXP] = contract.lastTradeDateOrContractMonth
        data[STRIKE] = contract.strike
        data[CP] = contract.right
        if greeks:
            quote = await self.marketdata.get_quote(contract, check_spread=False)
            data.update(quote.to_dict())
            if contract.secType == STK:
                data[DELTA] = -1.0 if data[POSITION] < 0 else 1.0
        return data

    async def _fetch_futures(self, symbol: str) -> pd.DataFrame:
        contracts = await self.marketdata.get_future_contracts(symbol)
        return pd.DataFrame([asdict(contract) for contract in contracts])

    async def _fetch_options(self, symbol: str, percent_strike: float = 0.1, dte: int = 180) -> pd.DataFrame:
        return await self.marketdata.get_stock_options(symbol, percent_strike, timedelta(dte))

    async def _fetch_fop(self, symbol: str, expiration: str, percent_strike: float = 0.05) -> pd.DataFrame:
        return await self.marketdata.get_future_options(symbol, expiration, percent_strike)

    async def _fetch_quote(self,
                           symbol: str,
                           expiry: Optional[str] = None,
                           strike: Optional[float] = None,
                           cp: Optional[str] = None,
                           fop: bool = False) -> Dict:
        """
        Get quote for a given stock / option / future.
        :param symbol:
        :param expiry:
        :param strike:
        :param cp:
        :return:
        """
        if all([expiry, strike, cp]):
            if fop:
                contract = ib.FuturesOption(symbol, expiry, strike, cp, GLOBEX)
            else:
                contract = ib.Option(symbol, expiry, strike, cp, SMART)
        elif expiry is not None and not any([strike, cp]):
            contract = ib.Future(symbol, expiry, GLOBEX)
        elif not any([expiry, strike, cp]):
            contract = ib.Stock(symbol, SMART, USD)
        else:
            raise Exception(f'Unrecognized combination: {symbol}, {expiry}, {strike}, {cp}')
        contract = await self.marketdata.qualify_contract(contract)
        quote = await self.marketdata.get_quote(contract, check_spread=False)
        return quote.to_dict()
