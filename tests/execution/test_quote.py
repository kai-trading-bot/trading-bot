import pytest

from src.model.quote import Quote

quote1 = Quote(100, 105)
quote2 = Quote(100, 110)
quote3 = Quote(100.34, 101.25)


@pytest.mark.parametrize("quote, expected", [
    (quote1, 102.5),
    (quote3, 100.80),
])
def test_mid(quote, expected):
    assert quote.mid == expected


@pytest.mark.parametrize("quote", [
    Quote(0, 1), Quote(1, 0), Quote(3, 2), Quote(-1, 5)
])
def test_quote_exception(quote):
    assert quote.erroneous
    with pytest.raises(Exception):
        price = quote.mid
