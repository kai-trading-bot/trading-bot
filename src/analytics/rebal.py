import cvxpy as cp
from multiprocessing import cpu_count, Pool
from src.utils.logger import logger
from src.utils.fe import *

__author__ = 'kqureshi'

MIN_WEIGHT = 0.25
MAX_WEIGHT = 0.75
WINDOW = 20


def fetch(data: Tuple[int, pd.DataFrame], solver: Union[str, None] = None) -> List[float]:
    """
    Single-period re-balance intended for multiprocessing
    :param data:
    :param solver:
    :return:
    """
    j, table = data
    data = table.iloc[j: j + WINDOW]
    N, rets = data.shape[1], np.array(data)
    gamma = cp.Parameter(nonneg=True)
    gamma.value = AVERSION
    w = cp.Variable(N)
    ret = rets.mean(0).reshape(N, -1).T * w
    risk = cp.quad_form(w, np.cov(rets.T))
    prob = cp.Problem(cp.Maximize(ret - gamma * risk), [cp.sum(w) == SUM_WEIGHT, w >= MIN_WEIGHT, w <= MAX_WEIGHT])
    try:
        prob.solve() if not solver else prob.solve(solver=solver)
    except:
        prob.solve(solver=DEFAULT_SOLVER)
    finally:
        prob.solve(solver='SCS')
    ref = (prob.solution.__dict__['primal_vars'])
    logger.info('{} complete'.format(data.index[-1]))
    try:
        ref = ref[list(ref.keys())[0]]
        if len(ref) > 0:
            return ref
    except IndexError:
        pass


def rebalance(data: pd.DataFrame, window: int = WINDOW) -> pd.DataFrame:
    """
    MP-enabled optimization
    Example:
    data = pd.DataFrame(np.random.rand(100,5), columns=['a', 'b', 'c', 'd', 'e'])
    data = (2 * data) - 1
    df = rebalance(data=data)
    :param data:
    :return:
    """

    iterations = list(range(len(data) - window))
    data_list = [tuple([iteration, data]) for iteration in iterations]
    return pd.DataFrame([[1 / len(data.columns)] * len(data.columns) if v is None else v for v in
                         Pool(cpu_count()).map(fetch, data_list)], columns=data.columns,
                        index=list(data.index[WINDOW:])).round(3)
