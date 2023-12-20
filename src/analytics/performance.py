import sys
import numpy as np
import os
import sys
from itertools import groupby
from src.utils.fe import *

__author__ = 'kqureshi'


class Statistics:
    
    ANNUAL = 252
    QUANTILES = 10
    sample_years = 3

    @classmethod
    def max(cls, data: pd.Series) -> float:
        return data.max()

    @classmethod
    def min(cls, data: pd.Series) -> float:
        return data.min()

    @classmethod
    def quantile(cls, data: pd.Series, quantile: float) -> float:
        return data.quantile(quantile)

    @classmethod
    def sharpe(cls, data: pd.Series) -> float:
        return np.sqrt(cls.ANNUAL) * data.mean() / data.std()

    @classmethod
    def sortino(cls, data: pd.Series) -> float:
        return np.sqrt(cls.ANNUAL) * data.mean() / data[data < 0].std()

    @classmethod
    def cumulative_returns(
        cls,
        data: Union[pd.Series, pd.DataFrame],
        ytd: bool = False,
        window: Optional[int] = None,
        differenced: bool = True,
        notional: bool = False,
        history: bool = True,
    ) -> Union[pd.Series, pd.DataFrame, float]:
        """ Compute cumulative returns

        :param data: input data (series or dataframe)
        :param ytd: whether to start from beginning of the current year
        :param window: size of the cumulative return window data[-window:]
        :param differenced: true means the data is in percent change
        :param notional: if notional is true, compute cumulative return using cumsum
        :param history: return all data points if true else return the last one
        :return: a series representing cumulative return
        """
        if window is not None:
            data = data.iloc[-window:]
        elif ytd:
            data = data.loc[str(datetime.datetime.today().year)]
        if not differenced:
            data = data.pct_change()
        if notional:
            data = round(data.cumsum(), 2)
        else:
            data = ((1 + data).cumprod() - 1)
        return data if history else data.iloc[-1]

    @classmethod
    def cagr(cls, data: pd.Series) -> float:
        return (((1 + data).cumprod())[-1] / 1) * (1 / float(len(data) / cls.ANNUAL)) - 1

    @classmethod
    def win_rate(cls, data: pd.Series) -> float:
        try:
            signs = np.sign(data.dropna()).value_counts().loc[[1, -1]]
            return signs.loc[1.0] / (signs.sum())
        except:
            return np.nan

    @classmethod
    def calmar(cls, data: pd.Series) -> float:
        return -(((data.loc[data.index[-1] - pd.DateOffset(years=cls.sample_years):data.index[-1]].groupby(
            data.loc[data.index[-1] - pd.DateOffset(years=cls.sample_years):data.index[-1]].index.year
        ).apply(lambda x: ((1 + x).cumprod() - 1)[-1]))).mean()) / (data.loc[data.index[-1] - pd.DateOffset(
            years=cls.sample_years):data.index[-1]].min())

    @classmethod
    def qd(cls, data: pd.Series) -> pd.Series:
        return pd.Series([data.quantile(ele / cls.QUANTILES) for ele in range(cls.QUANTILES + 1)
                          ][1:-1], index=[(ele / cls.QUANTILES) for ele in range(cls.QUANTILES + 1)][1:-1])

    @classmethod
    def basic_stats(cls, data: pd.Series) -> pd.DataFrame:
        return pd.Series(
            [cls.sharpe(data), cls.sortino(data), cls.win_rate(data), np.max(data), np.min(data), data.skew(),
             data.kurt(), np.size(data)], index=[
                'Sharpe', 'Sortino', 'Win', 'Max', 'Min', 'Skew', 'Kurtosis', 'Count']).to_frame().T

    @classmethod
    def monthly_stats(cls, data: pd.Series) -> pd.DataFrame:
        return pd.DataFrame(data.groupby(data.index.month).agg(
            Sharpe=cls.sharpe, Sortino=cls.sortino, Win=cls.win_rate, CAGR=cls.cagr,
            Max=np.max, Min=np.min, Count=np.size))

    @classmethod
    def yearly_stats(cls, data: pd.Series) -> pd.DataFrame:
        return pd.DataFrame(data.groupby(data.index.year).agg(
            Sharpe=cls.sharpe, Sortino=cls.sortino, Win=cls.win_rate, CAGR=cls.cagr,
            Max=np.max, Min=np.min, Count=np.size))

    @classmethod
    def ym_stats(cls, data: pd.Series) -> pd.DataFrame:
        return pd.DataFrame(data.groupby([data.index.year, data.index.month]).agg(
            Sharpe=cls.sharpe, Sortino=cls.sortino, Win=cls.win_rate, CAGR=cls.cagr,
            Max=np.max, Min=np.min, Count=np.size)).unstack()

    @classmethod
    def tow(cls, data: pd.Series) -> pd.DataFrame:
        return pd.DataFrame(data.groupby(data.index.week).agg(
            Sharpe=cls.sharpe, Sortino=cls.sortino, Win=cls.win_rate, CAGR=cls.cagr,
            Max=np.max, Min=np.min, Count=np.size))

    @classmethod
    def tom(cls, data: pd.Series) -> pd.DataFrame:
        return pd.DataFrame(data.groupby(data.index.month).agg(
            Sharpe=cls.sharpe, Sortino=cls.sortino, Win=cls.win_rate, CAGR=cls.cagr,
            Max=np.max, Min=np.min, Count=np.size))

    @classmethod
    def trend(cls, data: pd.Series) -> pd.DataFrame:
        def fetch(data: pd.Series) -> pd.Series:
            pos, max_len, cum_pos = 0, 0, 0
            for k, g in groupby((np.sign(data))):
                if k == 1:
                    pat_size = len(list(g))
                    pos, max_len = (pos, max_len) if pat_size < max_len else (cum_pos, pat_size)
                    cum_pos += pat_size
                else:
                    cum_pos += len(list(g))
            return (100 * data.iloc[pos:pos + max_len]).reset_index().rename(columns={0: 'returns (%)', 'index': DATE})

        return pd.concat([fetch(data), fetch(-data)], axis=1, keys=['rally', 'tank'])

    @classmethod
    def yearly_trend(cls, data: pd.Series, trend: str = 'tank') -> pd.DataFrame:

        vec, labels = [], []
        years = list((data.index.year.unique()))
        for year in years:
            labels.append(cls.trend(data.loc[str(year)])[trend][YAHOO_DATE][0].strftime(DATE_FORMAT))
            vec.append((1 + cls.trend(data.loc[str(year)])[trend]['returns (%)'] / 100).cumprod() - 1)
        df = pd.concat(vec, axis=1, keys=labels)
        return df if trend == 'rally' else -df


class Report:

    @staticmethod
    def plot_curve(data: pd.Series) -> None:
        Statistics().cumulative_returns(data.dropna()).plot()
        plt.title('Cumulative Returns (%)')
        plt.ylabel('%')

    @staticmethod
    def plot_attribution():
        ...

    @staticmethod
    def plot_study():
        ...

    @staticmethod
    def plot_turnover():
        ...

    def build(cls):
        ...
