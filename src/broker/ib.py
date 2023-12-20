import ib_insync as ib
import tenacity
from datetime import timedelta
from src.config import *
from src.constant import *
from src.execution.order import OrderStrategy
from src.execution.utils import *
from src.model.quote import Quote
from src.utils.logger import logger
from src.utils.time import iso_to_compact


def get_port(account: str, mode: str) -> int:
    mapping = {
        (IB_KALLY_ACCT, LIVE): IB_LIVE_PORT,
        (IB_KALLY_ACCT, PAPER): IB_PAPER_PORT,
        (IB_MARS_ACCT, LIVE): IB_MARS_PORT
    }
    if (account, mode) not in mapping:
        raise ValueError(f'Cannot connection to IB with account {account} and mode {mode}')
    return mapping[(account, mode)]


def stock(symbol: str) -> ib.Stock:
    return ib.Stock(symbol, SMART, USD)


class InteractiveBroker:

    def __init__(self, client_id: int, mode: str, account: str = IB_KALLY_ACCT) -> None:
        self.host = IB_HOST
        self.mode = mode
        self.port = get_port(account, mode)
        self.client_id = client_id
        self.ib = ib.IB()
        self._ib_live = None
        if mode == PAPER:
            self._ib_live = InteractiveBroker(client_id=client_id, mode=LIVE)
        self.marketdata = self._ib_live if self._ib_live is not None else self

    def __str__(self):
        return f'InteractiveBroker({self.ib})'

    @tenacity.retry(stop=tenacity.stop_after_attempt(2))
    async def connect(self):
        if not self.ib.isConnected():
            logger.info(f'Connecting to IB: host={self.host} port={self.port} client_id={self.client_id}')
            await self.ib.connectAsync(self.host, self.port, clientId=self.client_id)
            logger.info(f'IB connected: host={self.host} port={self.port} client_id={self.client_id}')
        if self._ib_live is not None:
            logger.info(f'Connecting to IB (Live) for market data')
            await self._ib_live.connect()

    def disconnect(self):
        if self.ib.isConnected():
            self.ib.disconnect()
            logger.info(f'IB disconnected.')
        if self._ib_live is not None:
            self._ib_live.disconnect()

    # -*- Account -*-

    def get_accounts(self) -> List[str]:
        return self.ib.managedAccounts()

    # -*- Positions -*-

    async def get_positions(self,
                            sec_type: str = STOCK,
                            currency: str = USD,
                            quantity_only: bool = True) -> Dict[str, Union[int, ib.Position]]:
        """ Get positions that also filter by currency

        :param sec_type: type of the security
        :param currency: position currency
        :param quantity_only: if true, return only the number of shares, otherwise return the entire ib position
        :return: a dictionary of position's contract's localSymbol to number of shares or position
        """
        try:
            positions: List[ib.Position] = await self.ib.reqPositionsAsync()
            res = {position.contract.localSymbol: position for position in positions
                   if position.contract.currency == currency and position.contract.secType == sec_type}
            if quantity_only:
                res = {symbol: position.position for symbol, position in res.items()}
            return res
        except Exception as e:
            raise Exception(f'Failed to get IB positions: {e}')

    async def get_stock_positions(self) -> Dict[str, int]:
        return await self.get_positions(sec_type=STOCK)

    # -*- Orders -*-

    async def submit_order(self,
                           contract: ib.Contract,
                           quantity: float,
                           quote: Optional[Quote] = None) -> ib.Trade:
        """ Submit order for contract with desired quantity.
        :param contract: an IB contract
        :param quantity: quantity to place
        :param quote: optional quote object to use
        """
        symbol = contract.localSymbol
        if quantity == 0:
            raise Exception(f'Cannot submit order with 0 quantity: {symbol}')
        quote = quote or await self.get_quote(contract)
        side = get_side(quantity)
        price = get_mid_price(quote)
        order = ib.LimitOrder(side, abs(quantity), price)
        order = OrderStrategy.adaptive(order)
        trade = self.ib.placeOrder(contract, order)
        return trade

    async def submit_market_order(self, symbol: str, quantity: float) -> ib.Trade:
        """ Submit a stock market order. This can be used in interactive notebook to place orders.

            Usage:
                >>> self.submit_market_order('AAPL', 1)
                >>> self.submit_market_order('MSFT', -1)
        """
        if not (isinstance(quantity, int) or float(quantity).is_integer()):
            raise Exception(f'IB does not support fractional order: {quantity}')
        side = get_side(quantity)
        contract = await self.qualify_contract(stock(symbol))
        order = ib.MarketOrder(side.upper(), abs(quantity))
        trade = self.ib.placeOrder(contract, order)
        return trade

    async def close_positions(self, *symbols: str) -> List[ib.Trade]:
        """ Close the position for a given list of stock symbols. Noop if a symbol does not exist. """
        positions = await self.get_stock_positions()
        trades = []
        for symbol in symbols:
            if symbol not in positions:
                logger.warning(f'Symbol {symbol} does not exist in positions. Skip')
            quantity = -1 * positions[symbol]
            logger.info(f'Closing position for {symbol}: {quantity}')
            trade = await self.submit_market_order(symbol, quantity)
            trades.append(trade)
        return trades

    async def get_trade(self, symbol: str) -> List[ib.Trade]:
        return [trade for trade in self.ib.trades() if trade.contract.localSymbol == symbol]

    async def submit_what_if_order(self, contract: ib.Contract, quantity: float) -> ib.OrderState:
        side = get_side(quantity)
        order = ib.MarketOrder(side, abs(quantity))
        return await self.ib.whatIfOrderAsync(contract, order)

    async def modify_order(self, trade: ib.Trade) -> ib.Trade:
        """ Modify order to aid execution. """
        order = trade.order
        symbol = trade.contract.localSymbol
        if trade.isDone():
            logger.warning(f'[{symbol}] Cannot modify trade: trade is not active')
            return trade
        logger.info(f'[{symbol}] Modifying order')
        quote = await self.get_quote(trade.contract)
        price = get_mid_price(quote)
        sign = 1 if order.action == OrderSide.BUY else -1
        # Only modify if the new limit price is "higher" than the old limit price.
        if price * sign > order.lmtPrice * sign:
            logger.info(f'[{symbol}] Modify order with a new limit price: {order.lmtPrice} -> {price}')
            order.lmtPrice = price
            trade = self.ib.placeOrder(trade.contract, order)
            logger.info(f'[{symbol}] Order modified: {trade.order}')
        return trade

    async def cancel_order(self, trade: ib.Trade) -> ib.Trade:
        symbol = trade.contract.localSymbol
        logger.info(f'[{symbol}] Cancelling order')
        if trade.isActive():
            trade = self.ib.cancelOrder(trade.order)
            logger.info(f'[{symbol}] Order cancelled')
            return trade
        raise Exception(f'[{symbol}] Cannot cancel order: trade is not active.')

    # -*- Marketdata -*-

    async def get_quote(self, contract: ib.Contract, check_spread: Optional[bool] = None) -> Quote:
        """ Get quote for the contract, assuming it's qualified.
        :param contract: a qualified IB contract
        :param check_spread: whether to throw exception when spread is too large
        """
        tickers = await self.marketdata.ib.reqTickersAsync(contract)
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
        """ Get quotes for a list of qualified contracts. Skip the ones without bid and ask. """
        tickers = await self.marketdata.ib.reqTickersAsync(*contracts)
        return [Quote.from_ib(ticker) for ticker in tickers if ticker.hasBidAsk()]

    # -*- Research only: Option and Option on Futures -*-

    async def get_option_chain(self, contract: ib.Contract) -> ib.OptionChain:
        chains = await self.marketdata.ib.reqSecDefOptParamsAsync(
            contract.symbol, '', contract.secType, contract.conId)
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
        """ Return a list of options with quotes for the given future.

        get_future_options('ES', '20210331')

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
        expiries = dedup(df[EXP].to_list())
        strikes = dedup(flatten(df[STRIKE].to_list()))
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

    # -*- Account -*-

    async def account_summary(self, full: bool = False) -> Dict:
        accounts = await self.ib.accountSummaryAsync()
        data = {account.tag: account.value for account in accounts}
        account = accounts[0].account
        if full:
            return data
        return dict(
            Account=account,
            EquityWithLoanValue=data['EquityWithLoanValue'],
            AvailableFunds=data['AvailableFunds'],
            BuyingPower=data['BuyingPower'],
            MarginCushion=data['Cushion'],
            StockMarketValue=data['StockMarketValue'],
            UnrealizedPnL=data['UnrealizedPnL'],
            RealizedPnL=data['RealizedPnL'],
        )

    # -*- Utility -*-

    async def qualify_contract(self, contract: ib.Contract) -> ib.Contract:
        contracts = await self.ib.qualifyContractsAsync(contract)
        if not contracts:
            raise Exception(f'Failed to qualify contract: {contract}')
        return contracts[0]

    async def qualify_contracts(self, contracts: List[ib.Contract], size: int = 30) -> List[ib.Contract]:
        ret = []
        for unqualified in chunk(contracts, size):
            qualified = await self.ib.qualifyContractsAsync(*unqualified)
            ret.extend(qualified)
        return ret
