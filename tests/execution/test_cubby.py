import pytest

from src.execution import Portfolio
from src.execution.signals import Cubby, IBS, Butterfly3X, Factor, GlobalReversal


@pytest.mark.asyncio
async def test_portfolio_cubby():
    portfolio = Portfolio([Cubby(), IBS(), Butterfly3X(), Factor(), GlobalReversal()])
    notional = 60000
    await portfolio.run(notional)
    assert portfolio.weights is not None
    expected = {
        'DGRO': 58.0,
        'EWH': 243.0,
        'EWJ': 6.0,
        'EWT': -95.0,
        'HDV': -64.0,
        'MIDU': -33.0,
        'MTUM': 16.0,
        'QUAL': 21.0,
        'SPXL': 79.0,
        'TMF': 398.0,
        'TQQQ': 12.0,
        'UDOW': 28.0,
        'USMV': 40.0,
        'VLUE': -59.0
    }
    assert portfolio.get_trade_list('2021-11-30') == expected
