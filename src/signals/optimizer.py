import cvxpy as cp
import numpy as np
import pandas as pd

from typing import *

__author__ = 'kqureshi'


def check_dcp(prob: cp.Problem) -> None:
    if not prob.is_dcp():
        raise Exception(f'Problem is not DCP. Please make sure the input data does not contain NaN.')


def optimize_risk_adj_return(mu: np.ndarray,
                             sigma: np.ndarray,
                             w: cp.Variable = None,
                             risk_factor: float = 0.075,
                             w_lower: float = 0.01,
                             w_upper: float = 0.15) -> np.ndarray:
    if w is None:
        w = cp.Variable(mu.shape[0])
    gamma = cp.Parameter(value=risk_factor, nonneg=True)
    ret = mu.T @ w
    risk = cp.quad_form(w, sigma)
    lb = cp.Parameter(value=w_lower, nonneg=True)
    ub = cp.Parameter(value=w_upper, nonneg=True)
    obj = cp.Maximize(ret - gamma * risk)
    constraints = [cp.sum(w) == 1, w >= lb, w <= ub]
    prob = cp.Problem(obj, constraints)
    check_dcp(prob)
    prob.solve()
    return w.value


def optimize_min_vol(mu: np.ndarray,
                     sigma: Union[pd.DataFrame, np.ndarray],
                     w: cp.Variable = None,
                     risk_factor: float = 0.075,
                     w_lower: float = 0.0,
                     w_upper: float = 0.1,
                     constraints: List = None) -> np.ndarray:
    if w is None:
        w = cp.Variable(mu.shape[0])
    if constraints is None:
        constraints = []
    gamma = cp.Parameter(value=risk_factor, nonneg=True)
    risk = cp.quad_form(w, sigma)
    lb = cp.Parameter(value=w_lower, nonneg=True)
    ub = cp.Parameter(value=w_upper, nonneg=True)
    obj = cp.Minimize(gamma * risk)
    constraints = [cp.sum(w) == 1, w >= lb, w <= ub] + constraints
    prob = cp.Problem(obj, constraints)
    check_dcp(prob)
    prob.solve()
    return w.value
