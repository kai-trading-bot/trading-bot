import click

from mode.utils.imports import symbol_by_name

from src.constant import *
from src.config import SIGNALS, NOTIONAL, IB_MARS_ACCT, IB_KALLY_ACCT
from src.execution.signal import DailySignal
from src.execution import Portfolio, SignalExecutor
from src.execution.ib_executor import IBExecutor
from src.execution.utils import parse, coro
from src.utils import catch
from src.utils.logger import logger
from src.utils.time import today


@catch('Cannot find signal from module')
def signal_from_name(name: str, module: str = 'src.execution.signals') -> DailySignal:
    """ Give the name of the signal, instantiate the signal class.
        Capture exception and turn it to error log.
    """
    return symbol_by_name(f'{module}:{name}')()


@click.group()
def bot():
    """ Main entry point for all crypto_bot commands. """
    ...


@bot.group()
def execute():
    """ Main entry point for trade execution. """
    ...


@bot.group()
def report():
    ...


@bot.group()
def alert():
    """ Run alerts. """
    ...


@execute.command('signal')
@click.argument('signal')
@click.option('--notional', default=10000, help='Notional amount')
@click.option('--debug', is_flag=True, default=False, help='debug mode')
@coro
async def execute_signal(signal: str, notional: float, debug: bool):
    """ Execute one single signal

    $ bot execute signal MOC --debug
    """
    executor = SignalExecutor(signal=signal_from_name(signal), notional=notional, debug=debug)
    logger.info(f'Signal execution: {signal} Notional={notional} Debug={debug}')
    await executor.run()


@execute.command('portfolio')
@click.option('--signals', default=SIGNALS, help='Name of the signals (multiple signals should be separated by comma)')
@click.option('--notional', default=NOTIONAL, help='Notional amount')
@click.option('--live', is_flag=True, default=False, help='if True, use live execution mode')
@click.option('--test', is_flag=True, default=False, help='if True, only run in test mode')
@click.option('--mars', is_flag=True, default=False, help='if True, run with mars account')
@click.option('--client_id', default=None)
@coro
async def execute_portfolio(signals: str, notional: int, live: bool, test: bool, mars: bool, client_id: int):
    """ Portfolio execution

    $ bot execute portfolio --signals=Cubby,IBS,Butterfly3X,Factor --notional=50000 --live
    """
    signals = [signal_from_name(name) for name in parse(signals)]
    portfolio = Portfolio(signals=signals)
    mode = LIVE if live else PAPER
    account = IB_MARS_ACCT if mars else IB_KALLY_ACCT
    date = today()
    info = f'Portfolio={portfolio.name} Signals="{signals}" Notional={notional} Date={date} ' \
           f'Mode={mode} Test={test} Account={account}'
    logger.info(info)
    executor = IBExecutor(portfolio=portfolio, notional=notional, date=date, mode=mode,
                          account=account, client_id=client_id)
    await executor.test_run() if test else await executor.run()
    click.echo(info)


@execute.command('bias')
@click.option('--notional', default=NOTIONAL, help='Notional amount')
@click.option('--live', is_flag=True, default=False, help='if True, use live execution mode')
@click.option('--test', is_flag=True, default=False, help='if True, only run in test mode')
@coro
async def execute_bias(notional: int, live: bool, test: bool):
    """ Execute portfolio `Bias`

        $ bot execute bias --notional=50000 --live
    """
    signals = [signal_from_name('Bias')]
    portfolio = Portfolio(signals=signals)
    account = IB_MARS_ACCT  # Only support mars account to be safe
    mode = LIVE if live else PAPER
    date = today()
    info = f'Portfolio=Bias, Notional={notional} Date={date} Mode={mode} Test={test} Account={account}'
    logger.info(info)
    executor = IBExecutor(portfolio=portfolio, notional=notional, date=date, mode=mode, account=account)
    await executor.test_run() if test else await executor.run()
    click.echo(info)


@execute.command('alpaca')
@click.option('--signals', help='Comma separated list of signal names')
@click.option('--notional', default=NOTIONAL, help='Notional amount')
@click.option('--live', is_flag=True, help='Whether to execute in live mode')
@click.option('--dry-run', is_flag=True, help='If true, only print out trades without placing the orders.')
@coro
async def execute_alpaca(signals: str, notional: int, live: bool, dry_run: bool):
    """ Execute signals on Alpaca. Currently only paper trading is supported.

    Example:
          $ bot execute alpaca --notional=1000
    """
    from src.enum import TradingMode
    from src.execution.alpaca_executor import AlpacaExecutor
    if live:
        raise Exception('Live mode with Alpaca is not supported.')
    signals = [signal_from_name(name) for name in parse(signals)]
    portfolio = Portfolio(signals=signals)
    executor = AlpacaExecutor(portfolio=portfolio, notional=notional, mode=TradingMode.PAPER,
                              dry_run=dry_run, date=today())
    logger.info(f'Execute with Alpaca: Notional={notional}, Mode=PAPER, Portfolio={portfolio}, Date={today()}')
    await executor.run()


@report.command('signal')
@click.option('--test', is_flag=True, default=False, help='if True, run in test mode')
@coro
async def report_signal(test: bool):
    from src.execution.report import run
    logger.info(f'Running signal report. Test={test}')
    await run(test=test)


@report.command('dip')
@coro
async def report_dip():
    from src.execution.alert import DailyPriceAlert
    await DailyPriceAlert().run()


@report.command('ipo')
def upcoming_ipo():
    import src.data.ipo as ipo
    logger.info(f'Fetching upcoming IPOs')
    ipo.run()


@alert.command('dip')
@click.option('--symbols',
              default="XLK,QQQ,SPY,USMV,MSFT,AAPL,GOOG,AMZN,FB,CRM,MSCI,SPGI,CME",
              help="Comma separated list of symbols")
@coro
async def alert_the_dip(symbols):
    """ Usage:
        $ bot alert dip --symbols AAPL,TSLA
    """
    from src.execution.alert import DipAlert
    symbols = parse(symbols)
    dip = DipAlert(symbols)
    await dip.run()


def main():
    bot()
