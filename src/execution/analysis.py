import numpy as np
import pandas as pd

from src.analytics.performance import Statistics
from src.constant import *


class Analysis:
    @staticmethod
    def yearly_stats(data: pd.Series, notional: bool = False) -> pd.DataFrame:
        df = pd.DataFrame(data.groupby(data.index.year).agg(
            Sharpe=Statistics.sharpe,
            Sortino=Statistics.sortino,
            Win=Statistics.win_rate,
            Max=np.max,
            Min=np.min,
            Return=lambda x: Statistics.cumulative_returns(x, history=False, notional=notional),
            Count=np.size
        )).round(4)
        df.index.name = 'Year'
        return df

    @staticmethod
    def monthly_stats(data: pd.Series, notional: bool = False) -> pd.DataFrame:
        df = pd.DataFrame(data.groupby([data.index.year, data.index.month]).agg(
            Sharpe=Statistics.sharpe,
            Sortino=Statistics.sortino,
            Win=Statistics.win_rate,
            Max=np.max,
            Min=np.min,
            Return=lambda x: Statistics.cumulative_returns(x, history=False, notional=notional),
            Count=np.size
        )).round(4)
        df.index.set_names(['Year', 'Month'], inplace=True)
        return df

    @staticmethod
    def summarize(returns: pd.Series, notional: bool = False) -> pd.DataFrame:
        windows = [1, 5, 10, 20, 60, 120]
        cr = pd.DataFrame([Statistics.cumulative_returns(returns, window=window, history=False, notional=notional)
                           for window in windows], index=windows, columns=['returns']).T
        quantiles = pd.concat(
            [returns.iloc[-window:].describe() for window in windows], axis=1, keys=windows).loc[['mean', 'min', 'max']]
        summary = pd.concat([cr, quantiles], axis='index').T.round(4)
        summary.reset_index(inplace=True)
        summary['window'] = summary['index'].apply(lambda window: str(window) + ' ' + ('Days' if window > 1 else 'Day'))
        return summary.set_index('window').drop('index', axis=1)

    @staticmethod
    def underwater(returns: pd.Series) -> pd.Series:
        """ Returns here must be cumulative returns. """
        cr = Statistics.cumulative_returns(returns)
        running_max = np.maximum.accumulate(cr.copy())
        return cr / running_max - 1

    @staticmethod
    def drawdowns(returns: pd.Series, top: int = 5) -> pd.DataFrame:
        cr = Statistics.cumulative_returns(returns)
        uw = Analysis.underwater(returns)
        drawdowns = []
        for _ in range(top):
            valley = uw.idxmin()
            peak = uw[:valley][uw[:valley] == 0].index[-1]
            try:
                recovery = uw[valley:][uw[valley:] == 0].index[0]
            except IndexError:
                recovery = np.nan
            if not pd.isnull(recovery):
                uw.drop(uw[peak:recovery].index[1:-1], inplace=True)
            else:
                # drawdown has not ended yet
                uw = uw.loc[:peak]
            drawdowns.append((peak, valley, recovery))
            if len(returns) == 0 or len(uw) == 0 or np.min(uw) == 0:
                break
        df = pd.DataFrame(index=list(range(top)),
                          columns=['Percent Drawdown', PEAK, VALLEY, RECOVERY, DURATION])
        for i, (peak, valley, recovery) in enumerate(drawdowns):
            if pd.isnull(recovery):
                df.loc[i, DURATION] = np.nan
            else:
                df.loc[i, DURATION] = len(pd.date_range(peak, recovery, freq='B'))
            df.loc[i, PEAK] = (peak.to_pydatetime().strftime('%Y-%m-%d'))
            df.loc[i, VALLEY] = (valley.to_pydatetime().strftime('%Y-%m-%d'))
            if isinstance(recovery, float):
                df.loc[i, RECOVERY] = recovery
            else:
                df.loc[i, RECOVERY] = (recovery.to_pydatetime().strftime('%Y-%m-%d'))
            df.loc[i, 'Percent Drawdown'] = ((cr.loc[peak] - cr.loc[valley]) / cr.loc[peak]) * 100

        df[PEAK] = pd.to_datetime(df[PEAK])
        df[VALLEY] = pd.to_datetime(df[VALLEY])
        df[RECOVERY] = pd.to_datetime(df[RECOVERY])
        return df

    @staticmethod
    def turnover(positions: pd.DataFrame, window: int = 10) -> None:
        # TODO: this is not the official definition of turnover
        return positions.diff().rolling(window).mean()

    @staticmethod
    def long_short(positions: pd.DataFrame):
        long = positions[positions > 0].count(axis=1)
        short = positions[positions < 0].count(axis=1)
        return long, short

    @staticmethod
    def _corr(ret1: Union[pd.Series, pd.DataFrame], ret2: pd.Series) -> Dict:
        data = dict()
        if isinstance(ret1, pd.DataFrame):
            columns = ret1.columns
            for col in columns:
                corr = ret1[col].corr(ret2)
                data[col] = corr
        else:
            data['Corr'] = ret1.corr(ret2)
        return data

    @staticmethod
    def correlation(ret1: Union[pd.Series, pd.DataFrame],
                    ret2: pd.Series,
                    yearly: bool = True) -> pd.DataFrame:
        data = []
        if yearly:
            years = ret1.index.year.unique().tolist()[-9:]
            for year in years:
                r1, r2 = ret1[str(year)], ret2[str(year)]
                corr = Analysis._corr(r1, r2)
                data.append({'Year': year, **corr})
        corr = Analysis._corr(ret1, ret2)
        data.append({'Year': 'Overall', **corr})
        return pd.DataFrame(data)
