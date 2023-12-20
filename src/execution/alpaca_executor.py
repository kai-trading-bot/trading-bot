from overrides import overrides

from src.broker.alpaca import Alpaca, Order, AlpacaOrderStatus
from src.config import *
from src.enum import TradingMode
from src.execution.base_executor import BaseExecutor
from src.execution.portfolio import Portfolio
from src.execution.utils import get_diff
from src.model.quote import Quote
from src.utils import catch_async, fmt


class AlpacaExecutor(BaseExecutor):
    """ Execute orders on Alpaca.

    Example:
        >>> self = AlpacaExecutor()
        >>> await self.run()

    Notes:
        - Alpaca can only be used to submit equity market orders.
        - Alpaca supports fractional trading.
        - Alpaca does not support short selling well. Avoid short positions.
    """

    def __init__(self,
                 portfolio: Portfolio,
                 notional: float,
                 mode: TradingMode,
                 dry_run: bool,
                 date: str) -> None:
        super().__init__(
            portfolio=portfolio,
            notional=notional,
            mode=mode,
            dry_run=dry_run,
            dt=date,
            slack_channel=SLACK_REPORT_CHANNEL if mode == TradingMode.LIVE else SLACK_TEST_CHANNEL,
            slack_username=f'Alpaca ({mode.name})',
            slack_emoji=':alpaca:',
            email_subject=f'[{date}][{portfolio.name}] Alpaca Execution Report',
            email_recipients=TEST_RECIPIENTS)
        if mode == TradingMode.PAPER:
            self.alpaca = Alpaca(ALPACA_PAPER_KEY_ID, ALPACA_PAPER_SECRET_KEY, ALPACA_PAPER_URL)
        else:
            self.alpaca = Alpaca(ALPACA_LIVE_KEY_ID, ALPACA_LIVE_SECRET_KEY, ALPACA_LIVE_URL)

    # -*- Positions -*-

    @overrides
    async def get_current_positions(self):
        positions = await self.alpaca.list_positions()
        # TODO: confirm with Alpaca for short positions.
        return {pos.symbol: float(pos.qty) for pos in positions}

    async def get_quote(self, symbol: str) -> Quote:
        """ Get the quote of the symbol. """
        return await self.alpaca.get_quote(symbol)

    # -*- Trading -*-

    async def trade(self) -> None:
        """ Alpaca can only take market orders. """
        for symbol, qty in self.trades.items():
            if qty == 0:
                self.logger.warning(f'Symbol {symbol} has 0 trade quantity. Skip.')
                continue
            side = 'buy' if qty > 0 else 'sell'
            try:
                order = await self.alpaca.submit_order(symbol=symbol, qty=abs(qty), side=side, order_type='market')
                self.orders[symbol] = order
            except Exception as e:
                self.logger.error(f'Trade failed: {e}')
                self.failed[symbol] = str(e)

    @overrides
    async def _watch(self) -> None:
        for symbol, order in self.orders.items():
            order = await self.alpaca.get_order(order.id)
            self.orders[symbol] = order
            self._update_order_status(symbol, order)

    def _update_order_status(self, symbol: str, order: Order) -> None:
        if order.status == AlpacaOrderStatus.FILLED:
            self.filled[symbol] = order
        elif order.status == AlpacaOrderStatus.CANCELED:
            self.cancelled[symbol] = order
        elif order.status == AlpacaOrderStatus.REJECTED:
            self.rejected[symbol] = order
        elif AlpacaOrderStatus.is_active(order.status):
            self.logger.info(f'Order is active: {order}')
        else:
            self.logger.warning(f'Unknown order status: {order}')

    # -*- Reporting -*-

    @overrides
    async def report(self):
        await self._send_slack_report()

    @catch_async('slack_report failed')
    async def _send_slack_report(self) -> None:
        current = await self.get_current_positions()
        unbalanced = get_diff(current, self.target)
        status = self.status
        msg = dict(
            Date=self.dt,
            Notional=self.notional,
            Trades=fmt(self.trades),
            Positions=fmt(self.target),
            Filled=status['Filled'],
            Cancelled=status['Cancelled'],
            Failed='\n'.join(status['Failed']),
            Rejected=status['Rejected'],
            Unbalanced=fmt(unbalanced),
        )
        await self.slack.send('Execution Report', success=len(unbalanced) == 0, **msg)
