__all__ = [
    'SignalException',
    'ExecutionException',
    'QuoteException',
]


class SignalException(Exception):
    """ Exception throw from signal """


class ExecutionException(Exception):
    """ Exception from execution """


class QuoteException(Exception):
    """ Exception when quote fetching failed. """
