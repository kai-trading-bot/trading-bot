import ib_insync as ib
import pandas as pd
import src.execution.utils as util

from datetime import timedelta

from src.config import IB_HOST, IB_LIVE_PORT
from src.constant import *
from src.model.quote import Quote
from src.utils.logger import logger
from src.utils.time import iso_to_compact

CLIENT_ID = 666


class MarketData:

    def __init__(self,
                 client: Optional[ib.IB] = None,
                 host: str = IB_HOST,
                 port: int = IB_LIVE_PORT,
                 client_id: int = CLIENT_ID) -> None:
        if client is not None:
            self.ib = client
        else:
            self.ib = ib.IB()
            self.host = host
            self.port = port
            self.client_id = client_id

    def __str__(self) -> str:
        return f'MarketData({self.ib})'

    async def connect(self):
        if not self.ib.isConnected():
            await self.ib.connectAsync(self.host, self.port, clientId=self.client_id)
            logger.info(f'Marketdata connected: host={self.host} port={self.port} client_id={self.client_id}')

    def disconnect(self):
        if self.ib.isConnected():
            self.ib.disconnect()
            logger.info(f'Marketdata disconnected.')

    async def get_quote(self, contract: ib.Contract, check_spread: Optional[bool] = None) -> Quote:
        """
        Get quote for the given contract. Throw exception if the quote is invalid.
        Assume the given contract is already qualified.
        :param contract: a qualified IB contract
        :param check_spread: an optional boolean indicating whether to throw exception when spread
                             is too large. This is useful during execution.
        """
        tickers = await self.ib.reqTickersAsync(contract)
        if not len(tickers):
            # This is possible if the given contract is not qualified so IB cannot find ticker for it.
            raise Exception(f"Invalid contract. Please qualify it first.")
        ticker = tickers[0]
        if not ticker.hasBidAsk():
            raise Exception(f'No quote for {contract.localSymbol}. '
                            f'Please query it during market open.')
        # Do not check spread for options since the bid and ask can be very wide.
        check_spread = check_spread if check_spread is not None else contract.secType != OPTION
        return Quote.from_ib(ticker, check_spread)

    async def get_quotes(self, contracts: List[ib.Contract]) -> List[Quote]:
        """ Get quotes for a list of contracts. Assume given contracts are already qualified. """
        tickers = await self.ib.reqTickersAsync(*contracts)
        return [Quote.from_ib(ticker) for ticker in tickers if ticker.hasBidAsk()]

    async def get_option_chain(self, contract: ib.Contract) -> ib.OptionChain:
        chains = await self.ib.reqSecDefOptParamsAsync(contract.symbol, '', contract.secType, contract.conId)
        if not chains:
            raise Exception(f'Option chain is empty for {contract}')
        # IB returns a list of chains from different exchanges. For simplicity we take the first one.
        return chains[0]

    async def get_stock_options(self,
                                symbol: str,
                                percent_strike: float = 0.1,
                                within: timedelta = timedelta(days=180)) -> pd.DataFrame:
        """ Return a list of options with quotes for the given stock. """
        contract = ib.Stock(symbol, SMART, USD)
        contract = await self.qualify_contract(contract)
        quote = await self.get_quote(contract)
        chain = await self.get_option_chain(contract)
        strike_lower, strike_upper = (1 - percent_strike) * quote.mid, (1 + percent_strike) * quote.mid
        max_exp = iso_to_compact(datetime.utcnow() + within)
        selected = [
            ib.Option(symbol, exp, strike, cp, SMART)
            for cp in (CALL, PUT)
            for strike in chain.strikes
            for exp in chain.expirations
            if strike_lower <= strike <= strike_upper and exp <= max_exp
        ]
        logger.info(f'Get {len(selected)} potential option contracts.')
        contracts = await self.qualify_contracts(selected)
        quotes = await self.get_quotes(contracts)
        logger.info(f'Get {len(quotes)} option quotes.')
        df = pd.DataFrame.from_records([quote.to_dict() for quote in quotes])
        if df.empty:
            raise Exception(f'No option contract fetched')
        return df.sort_values([CP, EXP, STRIKE]).reset_index(drop=True)

    async def get_future_contracts(self, symbol: str) -> List[ib.Contract]:
        """
        Given the symbol, return a list of futures with various expiration dates.
        """
        contract = ib.Contract(symbol=symbol, secType='FUT+CONTFUT', exchange=GLOBEX, currency=USD)
        details = await self.ib.reqContractDetailsAsync(contract)
        return [detail.contract for detail in details]

    async def get_future_options(self,
                                 symbol: str,
                                 expiration: str,
                                 percent_strike: float = 0.05) -> pd.DataFrame:
        """
        Return a list of options with quotes for the given future.

        :param symbol: the future contract symbol, e.g. ES, NQ
        :param expiration: The future contract's last trading day or contract month.
            * YYYYMM format: To specify last month
            * YYYYMMDD format: To specify last trading day
        :param percent_strike: only fetch options with strike price within this percent of
                               underlying price. Note if this value is too big, the function
                               can run for a long time.
        """
        fut = ib.Future(symbol, expiration, GLOBEX)
        contract = await self.qualify_contract(fut)
        quote = await self.get_quote(contract)
        strike_lower, strike_upper = (1 - percent_strike) * quote.mid, (1 + percent_strike) * quote.mid
        chains = await self.ib.reqSecDefOptParamsAsync(
            underlyingSymbol=contract.symbol, futFopExchange=contract.exchange,
            underlyingSecType=contract.secType, underlyingConId=contract.conId)
        data = [[chain.tradingClass, expiration, chain.strikes] for chain in chains for expiration in chain.expirations]
        df = pd.DataFrame(data, columns=[TICKER, EXP, STRIKE]).sort_values(EXP)
        expiries = util.dedup(df[EXP].to_list())
        strikes = util.dedup(util.flatten(df[STRIKE].to_list()))
        contracts = [
            ib.FuturesOption(symbol, expiry, strike, cp, GLOBEX)
            for expiry in expiries for strike in strikes for cp in (CALL, PUT)
            if strike_lower <= strike <= strike_upper
        ]
        logger.info(f'{symbol}: get {len(contracts)} potential options.')
        contracts = await self.qualify_contracts(contracts)
        quotes = await self.get_quotes(contracts)
        logger.info(f'{symbol}: get {len(quotes)} option quotes.')
        df = pd.DataFrame.from_records([quote.to_dict() for quote in quotes])
        if df.empty:
            raise Exception(f'No option contract fetched for {symbol}')
        return df.sort_values([CP, EXP, STRIKE]).reset_index(drop=True)

    async def qualify_contract(self, contract: ib.Contract) -> ib.Contract:
        contracts = await self.ib.qualifyContractsAsync(contract)
        if not contracts:
            raise Exception(f'Failed to qualify contract: {contract}')
        return contracts[0]

    async def qualify_contracts(self, contracts: List[ib.Contract], size: int = 30) -> List[ib.Contract]:
        ret = []
        for unqualified in util.chunk(contracts, size):
            qualified = await self.ib.qualifyContractsAsync(*unqualified)
            ret.extend(qualified)
        return ret
