import pytest

from src.execution.signal import DailySignal, Long
from src.execution.signals import *

NOTIONAL = 10000
DATE = '2021-02-26'


async def run_signal(signal: DailySignal, notional: float = NOTIONAL, date: str = DATE):
    await signal.fetch()
    await signal.update(notional)
    trade_list = signal.get_trade_list(date)
    assert len(trade_list) == len(signal.tickers)
    await signal.backtest(notional)
    # Make sure all of the following methods can execute.
    signal.pct_return()
    signal.yearly_stats()
    signal.cumulative_returns()


@pytest.mark.asyncio
@pytest.mark.skip(reason="Yahoo request is flaky")
async def test_cubby():
    await run_signal(Cubby())


@pytest.mark.asyncio
@pytest.mark.skip(reason="Yahoo request is flaky")
async def test_ibs():
    await run_signal(IBS())


@pytest.mark.asyncio
@pytest.mark.skip(reason="Yahoo request is flaky")
async def test_butterfly():
    await run_signal(Butterfly3X())


@pytest.mark.asyncio
@pytest.mark.skip(reason="Yahoo request is flaky")
async def test_factor():
    await run_signal(Factor())


@pytest.mark.asyncio
@pytest.mark.skip(reason="Yahoo request is flaky")
async def test_long():
    await run_signal(Long('SPY'))


@pytest.mark.asyncio
@pytest.mark.skip(reason="Yahoo request is flaky")
async def test_bias():
    await run_signal(Bias())
