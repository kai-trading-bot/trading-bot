import pandas_market_calendars as mcal
from src.utils.fe import *

ArrayLike: Union[np.array, pd.Series, List]

__author__ = 'kqureshi'

class TimeSeries:
    @staticmethod
    def ewma(data: np.array, halflife: float) -> np.array:
        """
        Simple ewma
        """

        alpha = 1 - np.exp(np.log(0.5) / halflife)
        alpha_reverse = 1 - alpha
        n = data.shape[0]
        powers = alpha_reverse ** (np.arange(n + 1))
        scale_arr = 1 / powers[:-1]
        return (data[0] * powers[1:]) + ((data * (alpha * alpha_reverse ** (n - 1)) *
                                          scale_arr).cumsum() * scale_arr[::-1])

    @staticmethod
    def sparse_padding(ts: pd.Series, pad: bool = True, exchange: str = 'NYSE') -> pd.Series:
        ts.index = pd.DatetimeIndex(ts.index)
        dates = mcal.get_calendar(exchange)
        dates = [pd.Timestamp(ele.date()) for ele in dates.valid_days(start_date=ts.index[0], end_date=ts.index[-1])]
        ts = pd.Series(0, index=dates) + ts
        if pad:
            return ts.fillna(0)
        return ts

    @staticmethod
    def winsorize(data: pd.Series, lb: float = 0.05, ub: float = 0.95) -> pd.Series:
        quantiles = data.quantile([lb, ub])
        q_05, q_95 = quantiles.loc[lb], quantiles.loc[ub]
        return pd.Series(np.where(data.values <= q_05, q_05, np.where(data >= q_95, q_95, data)), index=data.index)

    @staticmethod
    def align_returns(signal: pd.Series, returns: pd.Series, lag: int = 1) -> pd.DataFrame:
        return pd.concat([signal.shift(lag), returns], axis=1).fillna(0)

    @staticmethod
    def _align_index(data_list: List[pd.DataFrame]) -> List[pd.DataFrame]:
        return [d.loc[list(set(list(itertools.chain(*[d.index for d in data_list]))))].sort_index() for d in data_list]

    @staticmethod
    def _align_columns(data_list: List[pd.DataFrame]) -> List[pd.DataFrame]:
        return [d[list(set(list(itertools.chain(*[d.columns for d in data_list]))))] for d in data_list]

    @staticmethod
    def to_wide(data: pd.DataFrame) -> pd.DataFrame:
        if isinstance(data.index[0], tuple):
            return data.unstack()
        return data

    @staticmethod
    def to_long(data: pd.DataFrame) -> pd.DataFrame:
        if isinstance(data.index[0], tuple):
            return data
        return data.stack()

class Conditioner:

    @staticmethod
    def volatility_ratio(data: pd.Series, window_1: int, window_2: int) -> pd.Series:
        return ((data.rolling(window_1).std() / data.rolling(window_2).std()) - 1).shift()

    @staticmethod
    def volume_ratio(data: pd.Series, window: int, clip: int = 5) -> pd.Series:
        return (data.div(data.rolling(window).mean()) - 1).shift()

    @staticmethod
    def rolling_max(data: pd.Series, window: int) -> pd.Series:
        """ Compute the ratio between 1 - current_price / rolling_max_price """
        return 1 - (data.div(data.rolling(window + 1).max()))
