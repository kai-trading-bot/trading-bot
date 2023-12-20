from .bias import Bias, BiasV1, BiasV2
from .closure import Closure
from .cubby import Cubby
from .dummy import Dummy
from .factor import Factor
from .global_reversal import GlobalReversal
from .ibs import IBS
from .ironman import Ironman
from .mirror import Mirror
from .slope import Slope
from .butterfly import Butterfly, Butterfly2X, Butterfly3X
from .moc import MOC

from typing import *

__all__ = [
    'Bias',
    'BiasV1',
    'BiasV2',
    'Butterfly',
    'Butterfly2X',
    'Butterfly3X',
    'Closure',
    'Cubby',
    'Dummy',
    'Factor',
    'GlobalReversal',
    'IBS',
    'Ironman',
    'Mirror',
    'MOC',
    'Slope',
]

T = TypeVar('T')


class UnionFind(Generic[T]):

    def __init__(self):
        self.parent = dict()

    def add(self, x: T) -> None:
        if x in self.parent:
            return
        self.parent[x] = x

    def find(self, x: T) -> T:
        if x not in self.parent:
            raise Exception(f'{x} does not exist.')
        parent = self.parent[x]
        while parent != self.parent[parent]:
            grandparent = self.parent[parent]
            self.parent[x] = grandparent
            parent = grandparent
        return parent

    def union(self, x: T, y: T) -> T:
        parent_x = self.find(x)
        parent_y = self.find(y)
        if parent_x == parent_y:
            return
        self.parent[parent_x] = parent_y
        return y

    def connected_components(self) -> List[Set[T]]:
        roots = set([self.find(element) for element in self.parent.keys()])
        return [set([key for key, value in self.parent.items() if value == root]) for root in roots]


def analysis():
    """ Find which signals share the same symbols. """
    import importlib
    from collections import defaultdict
    from functools import reduce
    from src.utils.logger import logger
    module = importlib.import_module('src.execution.signals')
    m = defaultdict(set)
    uf = UnionFind()
    for cls in __all__:
        signal = getattr(module, cls)()
        uf.add(signal.name)
        try:
            tickers = signal.tickers
            for ticker in tickers:
                m[ticker].add(signal)
        except Exception as e:
            logger.debug(f'{signal} Error: {e}')
    for symbol, signals in m.items():
        if len(signals) > 1:
            print(f'{signals} should be combined since they share {symbol}')
            reduce((lambda x, y: uf.union(x.name, y.name)), signals)
    print(uf.connected_components())
