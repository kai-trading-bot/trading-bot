import numpy as np
import cvxpy as cp
import pandas as pd
from typing import Union, Optional

__author__ = 'kqureshi'


def standardize(data: pd.DataFrame, mode: str = 'xs', as_integer: bool = False):
    axis = 0 if mode == 'xs' else 1
    data = -data.sub(data.mean(axis=axis)).div(data.std(axis=axis))
    return data if not as_integer else data.astype(int)
