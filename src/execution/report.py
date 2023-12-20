from src.config import *
from src.execution.signal import DailySignal
from src.execution.signals import *
from src.utils import parse
from src.utils.email import Email
from src.utils.logger import logger
from src.utils.time import today

DAYS = 365
SIGNAL_NOTIONAL = 10000


async def generate(signal: DailySignal, email: Email):
    await signal.backtest(SIGNAL_NOTIONAL)
    ret = signal.cumulative_returns(tcost=True, title='Cumulative YTD Returns', save=True)
    rolling = signal.cumulative_returns(rolling=DAYS, title=f'Cumulative Returns (Last {DAYS} Days)', save=True)
    yearly = signal.cumulative_returns(yearly=True, save=True)
    stats = signal.yearly_stats()
    email.add_title(f'{signal.name.capitalize()}')
    email.add_dataframe(stats, 'Yearly Stats')
    email.add_image(ret, 'Cumulative YTD Returns')
    email.add_image(rolling, f'Cumulative Returns (Last {DAYS} Days)')
    email.add_image(yearly, 'Cumulative Yearly Returns')


async def run(test: bool = False):
    signals = [Cubby(), IBS(), Butterfly3X(), Factor(), GlobalReversal(), BiasV2(fractional=True)]
    recipients = TEST_RECIPIENTS if test else RECIPIENTS
    email = Email(f'{today()} Signal Performance Report', recipients=recipients)
    for signal in signals:
        logger.info(f'Generating report for {signal.name}')
        await generate(signal, email)
    logger.info(f'Sending email')
    email.send()
