from src.config import *
from src.execution.signal import DailySignal
from src.utils.logger import get_logger
from src.utils.slack import Slack

DAYS = 365


class SignalExecutor:

    def __init__(self, signal: DailySignal, notional: float = 10000, debug: bool = False) -> None:
        self.signal = signal
        self.notional = notional
        self.name = signal.name
        channel = SLACK_TEST_CHANNEL if debug else SLACK_REPORT_CHANNEL
        self.slack = Slack(channel)
        self.logger = get_logger(self.name)

    async def run(self) -> None:
        try:
            await self.signal.fetch()
            await self.signal.update(self.notional)
            await self.report()
        except Exception as e:
            self.logger.exception(f"Execution failed: {e}")
            await self.slack.error('Execution Failed', text=str(e))

    async def report(self) -> None:
        response = await self.slack.info(f'{self.name} update completed')
        thread_ts = response.get('ts')
        if self.signal.backtestable and thread_ts is not None:
            ret_plot = self.signal.cumulative_returns(ytd=True)
            rolling_plot = self.signal.cumulative_returns(ytd=False, rolling=DAYS)
            await self.slack.send_image(ret_plot, thread_ts=thread_ts)
            await self.slack.send_image(rolling_plot, thread_ts=thread_ts)
