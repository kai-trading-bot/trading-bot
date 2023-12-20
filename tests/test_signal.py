import numpy as np
import pandas as pd
from src.signals.reversal import reversal

__author__ = 'kqureshi'

n, scale = 20, 100
MEAN, TOL = 0, 1e-3


class Data:

    @staticmethod
    def zero_mean():
        data = pd.DataFrame(np.random.rand(n, n))
        return ((2 * data) - 1) / scale


def test_reversal():
    data = Data().zero_mean()
    signal = reversal(data).unstack()
    assert signal.shape == data.shape
    assert np.allclose(signal.mean().mean(), MEAN, TOL, 2)


def test_momentum():
    ...


def test_svr():
    ...


def test_dispersion():
    ...


def test_volmult():
    ...
