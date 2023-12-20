import asyncio
import ib_insync as ib
import pandas as pd
import sys

from collections import defaultdict
from datetime import timedelta
from mode import Service

from src.config import *
from src.constant import *
from src.execution import ExecutionQuality
from src.execution.helper import Helper
from src.execution.marketdata import MarketData
from src.execution.order import OrderStrategy
from src.model.quote import Quote
from src.execution.utils import chunk
from src.storage import GCS
from src.utils import catch, catch_async, fmt
from src.utils.logger import get_logger
from src.utils.opsgenie import OpsGenie
from src.utils.slack import Slack
from src.utils.time import iso_to_compact, today, now, is_market_open, MarketCalendar

# When there are only three minutes left until market close, we consider it near market close
NEAR_MARKET_CLOSE = timedelta(minutes=3)


class Executor(Service):
    """ A base IB executor """

    def __init__(self,
                 name: str,
                 mode: str = PAPER,
                 channel: str = SLACK_REPORT_CHANNEL,  # TODO: deprecate this
                 debug: bool = False,
                 dry_run: bool = False,
                 adaptive: bool = True) -> None:
        super().__init__()
        self.name = name
        self.mode = mode
        self.dry_run = dry_run
        self.debug = debug or dry_run
        self.channel = SLACK_TEST_CHANNEL if self.debug else SLACK_REPORT_CHANNEL
        self.recipients = TEST_RECIPIENTS if self.debug else RECIPIENTS
        self.adaptive = adaptive
        self.date = today()

        self.ib = ib.IB()
        self.ib.errorEvent += self.on_error
        self.client_id = Helper.get_ib_client_id(self.name)

        self.logger = get_logger(self.name)
        self.slack = Slack(self.channel)

        self.target: Dict[str, float] = dict()  # Desired positions for ib.contract.localSymbol
        self.trades: Dict[str, float] = dict()  # Order quantity
        self.idmap: Dict[int, str] = dict()  # Mapping between IB reqId -> ticker
        self.filled: Dict[str, Union[ib.Trade, ib.OrderState]] = dict()  # Filled orders or what-if order state
        self.cancelled: Dict[str, ib.Trade] = dict()  # Cancelled orders
        self.rejected: Dict[str, Any] = dict()  # Rejected orders by IB
        self.failed: Dict[str, Any] = dict()  # Failed to submit to IB

        self.quality = ExecutionQuality()
        self.storage = GCS()
        if self.mode == LIVE:
            self.marketdata = MarketData(self.ib)
        else:
            self.marketdata = MarketData(client_id=self.client_id)

    async def on_started(self) -> None:
        await super().on_started()
        await self.execute()

    async def execute(self) -> None:
        if self.should_run():
            self.logger.info(f'Starting to run {self.name}')
            try:
                await self.connect()
                await self.marketdata.connect()
                await self.run()
            except asyncio.CancelledError as e:
                pass
            except Exception as e:
                await self.on_execution_error(e)
            finally:
                self.disconnect()
                self.marketdata.disconnect()
                self.logger.info('Execution finished. Bye!')
                sys.exit(0)
        else:
            self.logger.warning(f'Skipping {self.name}.')

    async def connect(self) -> None:
        """ Establish IB connection. Raise error if failed. """
        await self.ib.connectAsync(HOST, PORTS[self.mode], clientId=self.client_id)
        self.logger.info(f"IB Connected {self.client_id} mode={self.mode}")

    def disconnect(self) -> None:
        """ Disconnect IB. """
        if self.ib.isConnected():
            self.ib.disconnect()
            self.logger.info(f"IB Disconnected {self.client_id}")

    def on_error(self, req_id: int, error_code: int, error_string: str, contract: ib.Contract = None) -> None:
        """ IB error handler. """
        self.logger.error(f'Req {req_id} IB Error {error_code}: {error_string}. {contract if contract else ""}')
        ticker = self.idmap.get(req_id)
        if error_code == 201 and ticker is not None:
            self.rejected[ticker] = (error_code, error_string)

    def should_run(self) -> bool:
        """ Whether the executor should be run. Can be override by subclass. """
        return is_market_open()

    async def on_execution_error(self, e: Exception) -> None:
        """ Handle execution error """
        self.logger.exception(f"Execution error: {e}")
        await self.slack.error('Execution Error', text=str(e))
        if self.mode == LIVE:
            OpsGenie().send(message=f'{self.name} Execution Error', description=str(e))

    async def run(self) -> None:
        """ Main method to run the execution. To be implemented by subclass. """
        ...

    async def current_positions(self) -> Dict[str, float]:
        """ Get the current positions. To be implemented by subclass. """
        return dict()

    async def get_quote(self, contract: ib.Contract) -> Quote:
        """ Get the quote of the given contract. Can be override by subclass. """
        return await self.marketdata.get_quote(contract)

    def get_price(self, quote: Quote) -> float:
        """ Get the price given the quote. Can be override by subclass. """
        return quote.mid

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
            self.logger.error(f'Failed to get positions: {e}')
            return {}

    async def submit(self,
                     contract: ib.Contract,
                     quantity: float,
                     quote: Optional[Quote] = None) -> Optional[Union[ib.Trade, ib.OrderState]]:
        """ Submit order for contract with desired quantity.
            Under debug mode, this method returns the what if order state.

            :param contract: IB contract
            :param quantity: quantity to place
            :param quote: optional quote object to pass in
        """
        if quantity == 0:
            self.logger.warning(f'Skip order with zero quantity. Contract: {contract}.')
            return
        symbol = contract.localSymbol
        side = BUY if quantity > 0 else SELL
        try:
            quote = quote or await self.get_quote(contract)
            price = self.get_price(quote)
            order = ib.LimitOrder(side, abs(quantity), price)
            if self.dry_run:
                state = await self.ib.whatIfOrderAsync(contract, order)
                self.filled[symbol] = state
                return state
            # Try adaptive order
            if self.adaptive:
                order = OrderStrategy.adaptive(order)
            trade = self.ib.placeOrder(contract, order)
            self.idmap[trade.order.orderId] = symbol
            self.record(trade, quote)
            return trade
        except Exception as e:
            self.logger.error(f'Error submitting order for {contract.localSymbol}: {e}')
            self.failed[symbol] = str(e)

    async def watch(self) -> None:
        """ Watch trade status and replace the order if needed.
            Currently the logic is simple. It watches the trade and replace the ones that are waiting
            with new quotes.
        """
        while True:
            for trade in self.ib.trades():
                symbol = trade.contract.localSymbol
                if symbol in self.trades:
                    await self.maybe_modify(trade)
                    # TODO: refactor me here
                    if trade.orderStatus.status == ib.OrderStatus.Filled:
                        self.filled[symbol] = trade
                    elif trade.orderStatus.status == ib.OrderStatus.Cancelled:
                        self.cancelled[symbol] = trade
                    else:
                        # This should show order with status `Inactive`, which means the order is rejected.
                        # Or the order is just created or modified and is currently waiting to be executed.
                        self.logger.warning(f'Order status for {symbol}: {trade.orderStatus.status}')
            if not is_market_open() or self.done:
                return
            self.logger.info(self.status())
            await asyncio.sleep(10)

    @catch_async('Maybe modify order failed', log_exception=True)
    async def maybe_modify(self, trade: ib.Trade):
        if self._should_modify(trade):
            await self.modify(trade)

    def _should_modify(self, trade: ib.Trade, threshold: int = 60) -> bool:
        """ Whether the trade should be modified. Conditions:
            1) order type is limit order
            2) order is active
            3) order has been created or modified more than 60 seconds ago
        """
        record = self.quality.get(trade)
        if record is None:
            self.logger.error(f'[_should_modify] Record not found for trade {trade}')
            return False
        return (
            trade.order.orderType == OrderType.LMT and
            trade.isActive() and
            (now() - record.updated_at) > timedelta(seconds=threshold) and is_market_open()
        )

    @catch_async('Modify order failed')
    async def modify(self, trade: ib.Trade) -> None:
        """ Modify the trade order by using an updated limit price.
            Limit price will be the bid / ask price near market close.
        """
        order = trade.order
        quote = await self.get_quote(trade.contract)
        if self.near_market_close:
            self.logger.warning(f'Modifying order near market close: {order}')
            if self.adaptive:
                order = OrderStrategy.adaptive(order, Priority.URGENT)
            price = quote.bid if order.action == OrderSide.BUY else quote.ask
        else:
            price = quote.mid
        sign = 1 if order.action == OrderSide.BUY else -1
        # Only modify if the new limit price is "higher" than the old limit price.
        if price * sign > order.lmtPrice * sign:
            self.logger.info(f'Modify with a new limit price: {order.lmtPrice} -> {price}. {order}')
            order.lmtPrice = price
            trade = self.ib.placeOrder(trade.contract, order)
            self.record(trade, quote)
            self.logger.info(f'Order modified: {trade.order}')

    @property
    def near_market_close(self) -> bool:
        ts = now()
        return is_market_open() and (MarketCalendar.next_close(ts) - ts) < NEAR_MARKET_CLOSE

    @catch_async('Failed to cancel orders', log_exception=True)
    async def cancel(self) -> None:
        """ Cancel all active orders and wait for the cancellation to complete.
            Orders are not necessarily need to be cancelled since the TIF is DAY.
            But to be safe and correctly record states we can still explicitly cancel the active order.
        """
        for trade in self.ib.trades():
            if trade.isActive():
                self.logger.info(f'Cancelling trade {trade}')
                self.cancelled[trade.contract.localSymbol] = self.ib.cancelOrder(trade.order)

    @property
    def done(self) -> bool:
        return len(self.filled) + len(self.cancelled) + len(self.rejected) + len(self.failed) == len(self.trades)

    @catch_async('Integrity check failed')
    async def check(self) -> None:
        if self.debug:
            return
        status = defaultdict(list)
        terminal_status = [ib.OrderStatus.Filled, ib.OrderStatus.Cancelled, ib.OrderStatus.Inactive]
        current_status = [self.filled, self.cancelled, self.rejected]
        for trade in self.ib.trades():
            status[trade.orderStatus.status].append(trade.contract.localSymbol)
        # Check: all orders should be in three terminal states: Filled, Cancelled or Inactive (Rejected)
        assert set(status).issubset(set(terminal_status)), f'Inconsistent status: {set(status)}'
        # Check: System order states should match IB side order states.
        for (ib_status, sys_status) in zip(terminal_status, current_status):
            assert len(status[ib_status]) == len(sys_status), \
                f'Incorrect number of {ib_status} orders: IB={len(status[ib_status])} Sys={len(sys_status)}'
        # Check: Total number for all four states should match the intended trade list.
        assert len(self.filled) + len(self.cancelled) + len(self.rejected) + len(self.failed) == len(self.trades), \
            f'Total number of order status and number of trades does not match! {self.status()}'
        # If filled, check positions
        if len(self.filled) == len(self.trades):
            await self.verify_positions(self.target)

    async def verify_positions(self, target: Dict[str, float]):
        """ This method verifies after rebalance finished whether the position matches the target position
            computed from the signal. Prerequisite: signal shouldn't have duplicated symbols. Please use
            combined signals to combine them together before rebalancing.

            :param target:
            :param buffer:
        """
        positions = await self.current_positions()
        mismatch = []
        msg = 'Symbol: {}, Current Qty: {}, Target Qty: {}'
        for symbol, target_qty in target.items():
            current_qty = positions.get(symbol, 0.0)
            if abs(current_qty - target_qty) > self.turnover_threshold:
                error_msg = msg.format(symbol, current_qty, target_qty)
                self.logger.error(f'Position mismatch greater than {self.turnover_threshold}: {error_msg}')
                mismatch.append(error_msg)
        if mismatch:
            await self.slack.error(
                'Position Mismatch!',
                text='One or more symbols\' current positions do not match signal target rebalance positions.',
                Mismatch='\n'.join(mismatch),
            )

    @catch(f'Upload failed', log_exception=True)
    def upload(self, dry_run: bool = False) -> Optional[pd.DataFrame]:
        """ Upload execution information to cloud. """
        if self.debug:
            return
        path = 'data/execution/{date}_{name}.csv.gz'
        trades = list(self.filled.values()) + list(self.cancelled.values())
        data = [self.quality.evaluate(trade) for trade in trades]
        rejected = [dict(ticker=k, status='Rejected', note=v) for k, v in self.rejected.items()]
        failed = [dict(ticker=k, status='Failed', note=v) for k, v in self.failed.items()]
        df = pd.DataFrame(data + rejected + failed)
        if df.empty:
            self.logger.info(f'No data to upload')
            return
        df = df.set_index('ticker')
        df['target'] = pd.Series(self.target)
        df['trade'] = pd.Series(self.trades)
        if dry_run:
            return df
        self.storage.write_csv(df, path.format(date=iso_to_compact(self.date), name=self.name))

    def status(self) -> Dict:
        """ Return system status. """
        total = len(self.trades)
        unfilled = list(self.trades.keys() - self.filled.keys())
        return dict(
            Total=total,
            Filled=len(self.filled),
            Unfilled=fmt(unfilled),
            Cancelled=fmt(list(self.cancelled.keys())),
            Failed=fmt(self.failed),
            Rejected=fmt(self.rejected),
            Commissions=self.commissions,
        )

    @property
    @catch('Failed to get commissions', -1)
    def commissions(self) -> float:
        res = sum([fill.commissionReport.commission for trade in self.filled.values() for fill in trade.fills])
        return round(res, 4)

    @property
    def success(self) -> bool:
        return len(self.filled) == len(self.trades)

    def record(self, trade: ib.Trade, quote: Quote) -> None:
        self.quality.record(trade, quote)

    @property
    def turnover_threshold(self) -> int:
        """ A threshold to prevent unnecessary turnover.
            TODO: this should be calculated using notional in the future.
        """
        return 5

    async def qualify_one(self, contract: ib.Contract) -> ib.Contract:
        contracts = await self.ib.qualifyContractsAsync(contract)
        if not contracts:
            raise Exception(f'No contract fetch for the given {contract}')
        return contracts[0]

    async def qualify_multi(self, contracts: List[ib.Contract], size: int = 30) -> List[ib.Contract]:
        ret = []
        for unqualified in chunk(contracts, size):
            qualified = await self.ib.qualifyContractsAsync(*unqualified)
            ret.extend(qualified)
        return ret
