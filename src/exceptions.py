class AlpacaException(Exception):
    """ Alpaca related exceptions. """
    ...


class NotTradableException(AlpacaException):
    def __init__(self, symbol: str):
        super().__init__(f'{symbol} is not tradable.')


class NotShortableException(AlpacaException):
    def __init__(self, symbol: str) -> None:
        super().__init__(f'{symbol} is not shortable.')


class QuoteException(Exception):
    """ Exception when quote fetching failed. """
