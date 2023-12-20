import numpy as np
import pandas as pd
import statsmodels.formula.api as smf
from src.data.data_loader import Stock
from multiprocessing import cpu_count, Pool
from typing import *

__author__ = 'kqureshi'


class Pair:

    def __init__(self):

        self.cov_type = 'HAC'
        self.windows = [20, 60, 120, 250, 500]

    @classmethod
    def _window(cls, data: pd.DataFrame, window: int) -> List[pd.DataFrame]:
        """
        Rolling window helper
        """
        data_list = []
        for j in range(len(data) - window + 1):
            data_list.append(data.iloc[j: j + window])
            j += 1
        return data_list

    @classmethod
    def reg(cls, data: pd.DataFrame, x: str = None, y: str = None):
        x = data.columns[0] if not x else x
        y = data.columns[1] if not y else y
        model = smf.ols('{} ~ + {}'.format(y, x), data=data)
        result = model.fit(cov_type=Pair.cov_type, cov_kwds={'maxlags': 5})
        return result.params[x]

    @classmethod
    def spread(cls, y: str, x: str, tickers: List[str], window: int) -> pd.Series:
        data = Stock.daily(tickers=tickers, diff=False)
        prices = np.log(data)
        prices.index = pd.DatetimeIndex(prices.index)
        data_list = _window(data=prices, window=window)
        betas = pd.Series(list(Pool(cpu_count()).map(reg, data_list)), index=pd.DatetimeIndex(prices.index[(window - 1):]))
        return prices[y] - (prices[x] * betas)
