import asyncio

from typing import *

from src.config import *
from src.enum import TradingMode
from src.execution.utils import get_diff
from src.execution import Portfolio
from src.utils import Email, OpsGenie, Slack, get_logger, fmt
from src.utils.time import today, is_market_open


class BaseExecutor:

    # Wait time (in seconds) between each order status check.
    WATCH_WAIT_TIME: int = 10

    def __init__(
        self,
        portfolio: Portfolio,
        notional: float,
        dt: str,
        mode: TradingMode = TradingMode.LIVE,
        dry_run: bool = False,
        slack_channel: str = SLACK_REPORT_CHANNEL,
        slack_username: str = SLACK_USERNAME,
        slack_emoji: str = SLACK_EMOJI,
        email_subject: str = '',
        email_recipients: List[str] = TEST_RECIPIENTS,
    ) -> None:
        self.portfolio = portfolio
        self.notional = notional
        self.mode = mode
        self.dry_run = dry_run
        self.dt = dt
        self.slack = Slack(slack_channel, username=slack_username, icon_emoji=slack_emoji)
        self.email = Email(subject=email_subject, recipients=email_recipients)
        self.logger = get_logger(self.name)
        self.target = dict()
        self.trades = dict()
        self.orders = dict()
        self.filled = dict()
        self.cancelled = dict()
        self.failed = dict()
        self.rejected = dict()

    @property
    def name(self):
        return self.__class__.__name__

    async def run(self) -> None:
        """ Main execution logic. """
        if not self.should_run():
            self.logger.warning(f'Should not start the execution. Skipping.')
            return
        try:
            await self.init()
            if self.dry_run:
                self.logger.info('Dry run succeeded. Exiting.')
                return
            await self.trade()
            await self.watch()
            await self.post_exec()
            await self.record()
            await self.report()
        except Exception as e:
            await self.on_execution_error(e)
        finally:
            self.cleanup()

    def should_run(self) -> bool:
        """ Check if the execution should be run. """
        return self.is_market_open

    # -*- Initialization -*-

    async def init(self) -> None:
        """ Initialize the execution. """
        await self.portfolio.run(self.notional)
        self.target = self.portfolio.get_trade_list(self.dt)
        current_pos = await self.get_current_positions()
        # TODO: pass in turnover threshold.
        self.trades = get_diff(current=current_pos, target=self.target)
        self.logger.info(f'Target: {self.target}. Trade: {self.trades}')

        # Report to Slack
        if not self.dry_run:
            await self.slack.info('Starting execution', **self.status)

    # -*- Trading -*-

    async def trade(self) -> None:
        """ Execute the trades.

        Fill in the instance variable `self.orders`.
        """
        raise NotImplementedError()

    async def watch(self) -> None:
        while self.is_market_open and not self.is_done:
            await self._watch()
            self.logger.info(f'Status: {self.status}')
            await asyncio.sleep(self.WATCH_WAIT_TIME)

    async def _watch(self) -> None:
        """ Watch the trade status. To be overridden by subclass. """
        raise NotImplementedError()

    async def post_exec(self) -> None:
        """ Do any post execution calculations. """
        ...

    async def report(self) -> None:
        """ Send execution reports. """
        ...

    async def record(self) -> None:
        """ Store the executions in storage. """
        ...

    # -*- Error handling and clean up -*-

    async def on_execution_error(self, e: Exception) -> None:
        """ Invoked when any exception is thrown during the execution. """
        self.logger.exception(f'Execution error: {e}')
        await self.slack.error('Execution Error', text=str(e))
        if self.mode == TradingMode.LIVE:
            OpsGenie().send(message='Execution Error', description=str(e))

    def cleanup(self) -> None:
        """ Clean up the execution on exit. """
        ...

    # -*- Positions -*-

    async def get_current_positions(self):
        """ Return the current positions of the symbols. """
        raise NotImplementedError()

    # -*- Helper functions -*-

    @property
    def is_market_open(self) -> bool:
        return is_market_open()

    @property
    def is_done(self) -> bool:
        return len(self.filled) + len(self.cancelled) + len(self.rejected) + len(self.failed) == len(self.trades)

    @property
    def status(self) -> Dict:
        return dict(
            Portfolio=self.portfolio.name,
            Notional=self.notional,
            Date=self.dt,
            Mode=self.mode.name,
            Target=fmt(self.target),
            Trades=fmt(self.trades),
            Filled=fmt(self.filled.values()),
            Cancelled=fmt(self.cancelled.values()),
            Rejected=fmt(self.rejected.values()),
            Failed=fmt(self.failed),
        )
