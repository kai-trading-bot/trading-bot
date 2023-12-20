from click.testing import CliRunner
from unittest.mock import AsyncMock

from src.cli import *
from src.config import IB_MARS_ACCT


def test_bot():
    runner = CliRunner()
    result = runner.invoke(bot, ['--help'])
    assert result.exit_code == 0


def test_execute_portfolio(monkeypatch):
    executor = AsyncMock(IBExecutor)
    monkeypatch.setattr(IBExecutor, 'run', executor.run)
    monkeypatch.setattr(IBExecutor, 'test_run', executor.test_run)
    runner = CliRunner()
    result = runner.invoke(execute_portfolio, ['--mars', '--live'])
    assert result.exit_code == 0
    assert f'Account={IB_MARS_ACCT}' in result.output


def test_execute_bias(monkeypatch):
    executor = AsyncMock(IBExecutor)
    monkeypatch.setattr(IBExecutor, 'run', executor.run)
    monkeypatch.setattr(IBExecutor, 'test_run', executor.test_run)
    runner = CliRunner()

    # Cannot run with paper mode
    result = runner.invoke(execute_bias, ['--notional=10000'])
    assert result.exit_code == 1
    assert type(result.exception) == ValueError
    executor.reset_mock()

    # Live mode with test run
    result = runner.invoke(execute_bias, ['--live',  '--notional=10000', '--test'])
    assert result.exit_code == 0
    for msg in ["Notional=10000", "Mode=LIVE", "Test=True", f"Account={IB_MARS_ACCT}"]:
        assert msg in result.output
    executor.test_run.assert_called_once()
    executor.run.assert_not_called()
    executor.reset_mock()

    # Live mode
    result = runner.invoke(execute_bias, ['--live', '--notional=50000'])
    assert result.exit_code == 0
    for msg in ["Notional=50000", "Mode=LIVE", "Test=False", f"Account={IB_MARS_ACCT}"]:
        assert msg in result.output
    executor.test_run.assert_not_called()
    executor.run.assert_called_once()
    executor.reset_mock()
