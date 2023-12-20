import cvxpy as cp
import matplotlib.pyplot as plt
from src.analytics.performance import Statistics
from src.analytics.ts import TimeSeries
from src.utils.fe import *

__author__ = 'kqureshi'


class Signal:
    """
    Signal to holdings
    """

    @staticmethod
    def holdings(signal: pd.DataFrame, pad: bool = True, xs: bool = True,
                 threshold: Optional[float] = 0.5) -> pd.DataFrame:
        """
        Baseline method to convert single signal to holdings
        :param signal:
        :param pad:
        :param xs:
        :param threshold:
        :return:
        """
        signal = TimeSeries.sparse_padding(signal) if pad else signal
        if xs:
            cs_mean, cs_std = signal.mean(axis=1), signal.std(axis=1)
            signal = (signal.sub(cs_mean, axis=0).div(cs_std, axis=0))
            holdings = signal.div(signal.abs().sum(axis=1), axis=0)
        else:
            signal = (signal.sub(signal.mean(axis=0))).div(signal.std(axis=0))
            holdings = pd.Series(np.where(signal > threshold, -1, np.where(signal < -threshold, 1, 0)),
                                 index=signal.index)
        return holdings.shift()

    @staticmethod
    def trade_list(holdings: pd.DataFrame, prices: pd.DataFrame, notional: int = 10000) -> None:
        """
        Generate trade list from holdings, price, and target notional
        :param holdings:
        :param prices:
        :param notional:
        :return:
        """
        return ((notional * holdings).div(prices, axis=1)).round().replace(np.inf, np.nan).fillna(0).astype(int)

    @staticmethod
    def _dollar_exposure(trade_list: pd.DataFrame, prices: pd.DataFrame) -> pd.Series:
        """
        Check dollar exposure of trade list
        :param trade_list:
        :param prices:
        :return:
        """
        assert (list(trade_list.columns) == list(prices.columns))
        return (trade_list * prices).sum(axis=1)

    @staticmethod
    def dollar_turnover(price: pd.DataFrame, positions: pd.DataFrame) -> pd.Series:
        """
        Percentage of book turned over
        :param prices:
        :param positions:
        :return:
        """
        notional = positions * price
        return notional.diff().abs().sum(axis=1).div(notional.abs().sum(axis=1)).fillna(0)

    @staticmethod
    def turnover(price: pd.DataFrame, positions: pd.DataFrame) -> pd.Series:
        # # https://github.com/quantopian/pyfolio/blob/master/pyfolio/txn.py#L149
        notional = positions * price
        AGB = notional.abs().sum(axis=1)
        denom = AGB.rolling(2).mean()
        denom.iloc[0] = AGB.iloc[0] / 2
        return notional.sum(axis=1).div(denom, axis='index').fillna(0)

    @staticmethod
    def _pnl(holdings: pd.DataFrame, returns: pd.DataFrame) -> pd.Series:
        """
        Use sum of weighted returns to measure gross pnl
        :param holdings:
        :param returns:
        :return:
        """
        return holdings.mul(returns).sum(axis=1)

    def event_study(self, signal: pd.DataFrame, returns: pd.DataFrame, buckets: int = 5,
                    window: int = 10) -> Union['plt', pd.DataFrame]:
        ...

    def performance(self, signal: pd.DataFrame, returns: pd.DataFrame, pad: bool = True) -> pd.Series:
        """
        In progress: Single helper method to measure raw signal performance
        :param signal:
        :param returns:
        :param pad:
        :return:
        """
        return self._daily_pnl(holdings=self._holdings(signal, pad=pad), returns=returns)


class Combination:

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

    @staticmethod
    def uniform(signals: Union[pd.DataFrame, List[pd.Series]]) -> pd.Series:
        """
        Benchmark for signal combination testing
        """
        if isinstance(signals, List):
            signals = pd.concat(signals, axis=1)
        return signals.mean(axis=1)

    @classmethod
    def spo(cls, signals: pd.DataFrame, style: str = 'sharpe', aversion: float = 1,
            min_ret: Optional[float] = 0.05, min_weight: Optional[float] = 0,
            max_weight: Optional[float] = 1) -> pd.Series:
        """
        Minimum variance signal combination, single-period
        """

        mu = np.mean((np.array(signals)), 0).reshape(signals.shape[1], 1)
        sigma = np.array(signals.cov())
        w = cp.Variable(signals.shape[1])
        gamma = cp.Parameter(nonneg=(aversion >= 0))
        gamma.value = aversion
        prob = cp.Problem(cp.Maximize(mu.T * w - (gamma * cp.quad_form(w, sigma.T.dot(sigma)))),
                          [cp.sum(w) == 1, w >= min_weight, w <= max_weight]) if style == 'sharpe' else cp.Problem(
            (cp.Minimize(cp.quad_form(w, sigma))),
            [cp.sum(w) == 1, mu.T * w >= min_ret, w >= min_weight, w <= max_weight])
        prob.solve()
        return pd.Series(w.value, index=signals.columns)

    @classmethod
    def info_rate(cls, signals: pd.DataFrame) -> pd.Series:
        features, track = list(signals.columns), signals.copy().shift()
        for j in range(len(features)):
            feature = track.columns[j]
            track['t{}'.format(j)] = track[feature].expanding().mean() / track[feature].expanding().std()
        for j in range(len(features)):
            track['w{}'.format(j)] = track['t{}'.format(j)] / track.filter(regex='t').abs().sum(axis=1)
        return (track.w0 * signals[signals.columns[0]] + track.w1 * signals[signals.columns[1]]).dropna()

    @classmethod
    def _win_weights(cls, signal_1: pd.Series, signal_2: pd.Series, partitions: int = 20) -> List[float]:
        vec = {}
        weights = [(1 / partitions) * j for j in list(range(1, partitions + 1))]
        for weight in weights:
            weight_1, weight_2 = weight, 1 - weight
            vec[weight] = Statistics().win_rate((weight_1 * signal_1) + (weight_2 * signal_2))
        opt_weight = pd.Series(vec).idxmax()
        return [opt_weight, 1 - opt_weight]

    @classmethod
    def _min_weights(cls, signal_1: pd.Series, signal_2: pd.Series, partitions: int = 20) -> List[float]:
        vec = {}
        weights = [(1 / partitions) * j for j in list(range(1, partitions + 1))]
        for weight in weights:
            weight_1, weight_2 = weight, 1 - weight
            vec[weight] = Statistics().min((weight_1 * signal_1) + (weight_2 * signal_2))
        opt_weight = pd.Series(vec).idxmax()
        return [opt_weight, 1 - opt_weight]

    @classmethod
    def win_weights(cls, signals: pd.DataFrame, window: int = 20) -> pd.Series:
        opt_weight = pd.DataFrame([Combination._win_weights(data[data.columns[0]], data[data.columns[1]]) for data in
                                   Combination._window(signals, window)], index=signals.index[window - 1:])
        opt_weight.columns = signals.columns
        return opt_weight.mul(signals, axis=1).sum(axis=1)

    @classmethod
    def min_weights(cls, signals: pd.DataFrame, window: int = 20) -> pd.Series:
        opt_weight = pd.DataFrame([Combination._min_weights(data[data.columns[0]], data[data.columns[0]]) for data in
                                   Combination._window(signals, window)], index=signals.index[window - 1:])
        opt_weight.columns = signals.columns
        return opt_weight.mul(signals, axis=1).sum(axis=1)

    @classmethod
    def _mvo(cls, signals: pd.DataFrame, min_weight: float = 0) -> pd.Series:
        return Combination.spo(signals=signals, min_weight=min_weight, aversion=1000)

    @classmethod
    def mvo(cls, signals: pd.DataFrame, window: int = 20, min_weight: float = 0.2) -> pd.Series:
        """

        """
        weights = pd.concat([Combination.spo(signals=data, min_weight=min_weight, aversion=1000)
                             for data in Combination._window(signals.shift(), window)], axis=1,
                            keys=signals.shift().index[window - 1:]).T
        return signals.mul(weights).sum(axis=1)

    @classmethod
    def rebuild(cls, signals: pd.DataFrame, raw: bool = False) -> pd.DataFrame:
        """

        """
        results = signals.copy()
        results['uniform'] = signals.mean(axis=1)
        results['gmm'] = Combination.info_rate(signals=signals)
        results['robust_1'] = Combination.min_weights(signals=signals)
        results['robust_2'] = Combination.win_weights(signals=signals)
        results['mvo'] = Combination.mvo(signals=signals)
        if raw:
            return results
        return results.stack().groupby(level=1).apply(
            Statistics.basic_stats).sort_values('Sharpe', ascending=False).reset_index(level=1, drop=True)

    @staticmethod
    def mpo(self):
        """
        Multi-period optimization
        """
        ...


class Smoothing:

    @staticmethod
    def by_halflife(holdings: pd.DataFrame, returns: pd.DataFrame, criteria: List[str] = ['Sharpe'],
                    lb: Optional[float] = 5, ub: Optional[float] = 120, freq: str = 'yearly') -> pd.DataFrame:
        """
        Performance by halflife
        :param holdings:
        :param returns:
        :param criteria:
        :param lb:
        :param ub:
        :return:
        """
        perf = {}
        halflifes = [2 ** j for j in range(int(np.log(lb)), int(np.log(ub)) + 1)]
        for halflife in halflifes:
            hds = pd.concat([TimeSeries.ewma(holdings[col].fillna(0), halflife=halflife) for col in holdings.columns],
                            axis=1)
            result = hds.mul(returns).sum(axis=1)
            if freq == 'yearly':
                perf[halflife] = Statistics().yearly_stats(result.replace(0, np.nan).dropna())[criteria]
            else:
                perf[halflife] = Statistics().basic_stats(result.replace(0, np.nan).dropna())[criteria]
        return pd.concat([pd.DataFrame(perf[key]) for key in perf.keys()], axis=1, keys=halflifes)
