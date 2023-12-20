import math
import numpy as np
import pandas as pd

from pandas.tseries.offsets import BDay
from scipy.stats import skew, kurtosis

from src.constant import *
from src.analytics.performance import Statistics
from src.utils.logger import logger
from src.utils.time import NYSE

MULTIPLIER = 100
LONG = '_long'
SHORT = '_short'


class OptionStrategy:
    DELTA_THRESHOLD: float = 0.05
    DTE_PERCENT: float = 0.1

    def __init__(self, frequency: Optional[str] = WEEKLY) -> None:
        """

        :param frequency: rebalance frequency (DAILY, WEEKLY, MONTHLY and None - no rebalance)
        """
        self.frequency = frequency

    def pick(self, df: pd.DataFrame, delta: float, dte: int, first: bool = True) -> pd.DataFrame:
        """ Pick the daily optimal option contract based on the given delta and dte.

            Formula: minimize abs delta subject to abs_delta < DELTA_THRESHOLD and abs_dte < ceil(DTE_PERCENT * dte)

        :param df: input option daily dataframe
        :param delta: target option delta
        :param dte: target option business days to expiration
        :param first: whether to return only the first row each date that has the minimum abs delta. If first = False,
                      this method will only adds abs_delta and abs_dte and return the filtered dataframe.
        """
        df[ABS_DELTA] = np.abs(delta - np.abs(df[DELTA]))
        df[ABS_DTE] = np.abs(dte - np.abs(df[DTE]))
        df = df[(df[ABS_DELTA] < self.DELTA_THRESHOLD) & (df[ABS_DTE] < math.ceil(self.DTE_PERCENT * dte))]
        if df.empty:
            raise Exception(f'No option found with delta={delta}, dte={dte}.')
        if first:
            return df.sort_values([DATE, ABS_DELTA]).groupby(DATE).first().reset_index()
        else:
            return df

    def check_dup(self, df: pd.DataFrame) -> None:
        # If there are duplicated index, report data error and drop all duplicated index.
        dup = df[df.index.duplicated()]
        if not dup.empty:
            logger.error(f'Find duplicated index. Please check the data source:\n{dup.index}')

    def dedup(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.loc[df.index.drop_duplicates(keep=False)]

    def daily_rebalance(self, data: pd.DataFrame, options: pd.DataFrame) -> pd.DataFrame:
        """ Daily rebalance

        :param data:
        :param options:
        :return:
        """
        options[DATE_ENTER] = options[DATE]
        options[DATE_EXIT] = options[DATE_ENTER] + BDay(1)  # TODO: BDay is not necessarily a market open date
        options = options[[EXP, STRIKE, DATE_ENTER, DATE_EXIT, DTE, DELTA, PRICE]]
        df = pd.merge(data[[EXP, STRIKE, DATE, BID, ASK]], options, on=[EXP, STRIKE], suffixes=('', '_enter'))
        df = df[(df[DATE] > df[DATE_ENTER]) & (df[DATE] <= df[DATE_EXIT])].sort_values(DATE)

        # Merge dataframe to calculate option PnL.
        price_df = df[[DATE, PRICE, DATE_ENTER, DATE_EXIT, STRIKE, EXP]]
        price_df[DATE] = price_df[DATE] + BDay(1)
        df = df.merge(price_df, on=[DATE, DATE_ENTER, DATE_EXIT, STRIKE, EXP], how='left', suffixes=('', '_prev'))
        # Note if previous day's option price is not available, by default we use price_enter for the
        # previous day's option price.
        df[PRICE_PREV] = df[PRICE_PREV].fillna(df[PRICE_ENTER])

        return df

    def weekly_rebalance(self, data: pd.DataFrame, options: pd.DataFrame, dow: str = 'Friday') -> pd.DataFrame:
        """ Rebalance weekly (Friday)

            If the rebalance day is a holiday, it will choose the previous market open day to do rebalance.

        :param data: option daily data
        :param options: picked options with target delta and dte
        :param dow: rebalance weekday (default Friday)
        """
        start = options[DATE].min()
        end = options[DATE].max()
        df = pd.DataFrame()
        df[DATE] = pd.bdate_range(start, end)
        market_open = NYSE.schedule(start, end).index
        df[HOLIDAY] = ~df[DATE].isin(market_open)
        df[DOW] = df[DATE].dt.day_name()
        df = df[df[DOW] == dow]
        df[DATE_ENTER] = df.apply(lambda row: row[DATE] - BDay(1) if row[HOLIDAY] else row[DATE], axis=1)
        df[DATE_EXIT] = df[DATE_ENTER].shift(-1)
        df = df.merge(options, left_on=[DATE_ENTER], right_on=[DATE], how='left')
        df = df.merge(data[[EXP, STRIKE, DATE, PRICE, STOCK_PRICE]], on=[EXP, STRIKE],
                      how='left', suffixes=('_enter', ''))
        df = df[(df[DATE] > df[DATE_ENTER]) & (df[DATE] <= df[DATE_EXIT])].sort_values(DATE)

        # Merge dataframe to calculate option PnL.
        price_df = df[[DATE, PRICE, DATE_ENTER, DATE_EXIT, STRIKE, EXP]]
        price_df[DATE] = price_df[DATE] + BDay(1)
        df = df.merge(price_df, on=[DATE, DATE_ENTER, DATE_EXIT, STRIKE, EXP], how='left', suffixes=('', '_prev'))
        # Note if previous day's option price is not available, by default we use price_enter for the
        # previous day's option price.
        df[PRICE_PREV] = df[PRICE_PREV].fillna(df[PRICE_ENTER])

        return df[[DATE, DATE_ENTER, DATE_EXIT, STRIKE, EXP, DELTA, DTE, PRICE_ENTER, PRICE, PRICE_PREV, STOCK_PRICE]]

    def monthly_rebalance(self):
        raise NotImplementedError()

    def no_rebalance(self):
        raise NotImplementedError()

    def rebalance(self, data: pd.DataFrame, options: pd.DataFrame, *args: Any, **kwargs: Any) -> pd.DataFrame:
        if self.frequency == DAILY:
            return self.daily_rebalance(data, options)
        if self.frequency == WEEKLY:
            return self.weekly_rebalance(data, options, *args, **kwargs)
        if self.frequency == MONTHLY:
            return self.monthly_rebalance()
        return self.no_rebalance()

    def _compute_pnl(self, df: pd.DataFrame, short: bool, multiplier: int = MULTIPLIER) -> pd.Series:
        """ Compute option pnl """
        columns = list(df.columns)
        if PRICE not in columns or PRICE_PREV not in columns:
            raise Exception(f'Unable to calculate option pnl: {PRICE} and/or {PRICE_PREV} '
                            f'not found in df columns: {columns}')
        return (df[PRICE] - df[PRICE_PREV]) * (-1 if short else 1) * multiplier

    def single_leg(self, data: pd.DataFrame, delta: float, dte: int, short: bool = False) -> pd.DataFrame:
        """ Single leg option strategy

        :param data: option data (call or put)
        :param delta: target delta
        :param dte: target business days to expiration
        :param short: whether this is a short option
        """
        options = self.pick(data, delta, dte)
        df = self.rebalance(data, options)
        df[PNL] = self._compute_pnl(df, short=short)
        df.set_index(DATE, inplace=True)
        self.check_dup(df)
        return df

    def spread(self, data: pd.DataFrame, long_delta: float, short_delta: float, long_dte: int, short_dte: int):
        """ Spread option strategy

        :param data: option data (call or put)
        :param long_delta: target delta for long leg
        :param short_delta: target delta for short leg
        :param long_dte: target dte for long leg
        :param short_dte: target dte for short leg
        """
        if long_delta == short_delta:
            # Calendar spread. Same strike and different expiration.
            assert long_dte != short_dte, f'Calendar spread cannot have long dte {long_dte} = short dte {short_dte}'
            same_attr, diff_attr = STRIKE, EXP
        elif long_dte == short_dte:
            # Vertical spread. Same dte and different strike price.
            assert long_delta != short_delta, \
                   f'Vertical spread cannot have long delta {long_delta} = short delta {short_delta}'
            same_attr, diff_attr = EXP, STRIKE
        else:
            raise Exception(f'Unable to detect reasonable strategy based on given delta and dte.')

        long = self.pick(data, long_delta, long_dte, first=False)
        short = self.pick(data, short_delta, short_dte, first=False)
        options = pd.merge(long, short.drop(STOCK_PRICE, axis=1), on=[DATE, same_attr], suffixes=(LONG, SHORT))

        # Two option must have different strike price or expiration date
        options = options[options[diff_attr + LONG] != options[diff_attr + SHORT]]
        options[ABS_DELTA] = options[ABS_DELTA + LONG] + options[ABS_DELTA + SHORT]
        options = options.sort_values([DATE, ABS_DELTA]).groupby(DATE).first().reset_index()

        # Split chosen options into long and short dataframe
        base_cols, columns = [DATE, same_attr], [diff_attr, DTE, DELTA, PRICE, ABS_DELTA, ABS_DTE]
        long_cols = [col + LONG for col in columns]
        short_cols = [col + SHORT for col in columns]
        option_long = options[base_cols + long_cols].rename(columns=dict(zip(long_cols, columns)))
        option_short = options[base_cols + short_cols].rename(columns=dict(zip(short_cols, columns)))

        # Rebalance and calculate PnL
        df_long = self.rebalance(data, option_long)
        df_short = self.rebalance(data, option_short)
        df_long[PNL] = self._compute_pnl(df_long, short=False)
        df_short[PNL] = self._compute_pnl(df_short, short=True)
        df = pd.merge(df_long, df_short.drop(STOCK_PRICE, axis=1), on=[DATE, DATE_ENTER, DATE_EXIT, same_attr],
                      suffixes=(LONG, SHORT))
        df[PNL] = df[PNL + LONG] + df[PNL + SHORT]
        df.set_index(DATE, inplace=True)
        self.check_dup(df)
        return df

    def covered_call(self, data: pd.DataFrame, stock: pd.Series, delta: float, dte: int,
                     stock_qty: Union[int, pd.Series] = 100) -> pd.DataFrame:
        """ Covered call strategy

        :param data: Option daily data (right = CALL)
        :param stock: Yahoo daily stock close
        :param delta: target option delta
        :param dte: target option dte
        :param stock_qty: daily quantity of underlying stock
        """
        index = stock.index
        if isinstance(stock_qty, int):
            stock_qty = pd.Series([stock_qty] * len(index), index=index)

        # Short Options
        option_qty = stock_qty.mul(1.0).div(MULTIPLIER).dropna().round().astype(int)
        df = self.single_leg(data, delta, dte, short=True)
        # Dedup is required to prevent multiplication issue
        df = self.dedup(df)
        df[OPTION_PNL] = df[PNL].mul(option_qty, axis=0)

        # Stock PNL. The Orats data stock price can be different from Yahoo so here we use Yahoo stock price
        # as source of truth.
        stock_pnl = pd.Series(stock.diff().mul(stock_qty.shift()), name=STOCK_PNL)
        df[STOCK_PNL] = stock_pnl
        df[PNL] = df[STOCK_PNL] + df[OPTION_PNL]
        return df

    def vertical_spread(self, data: pd.DataFrame, long_delta: float, short_delta: float, dte: int) -> pd.DataFrame:
        """ Vertical spread strategy

        :param data: call or put option daily data
        :param long_delta: target delta for long leg
        :param short_delta: target delta for short leg
        :param dte: target business days to expiration
        """
        return self.spread(data, long_delta, short_delta, dte, dte)

    def calendar_spread(self, data: pd.DataFrame, delta: float, long_dte: int, short_dte: int) -> pd.DataFrame:
        """ Calendar spread strategy

        :param data: call or put option daily data
        :param delta: target delta
        :param long_dte: target business days to expiration for long leg
        :param short_dte: target business days to expiration for short leg
        """
        return self.spread(data, delta, delta, long_dte, short_dte)

    @staticmethod
    def yearly_stats(data: pd.Series):
        df = pd.DataFrame(data.groupby(data.index.year).agg(
            Sharpe=Statistics.sharpe,
            Sortino=Statistics.sortino,
            Skew=skew,
            Kurtosis=kurtosis,
            Win=Statistics.win_rate,
            Max=np.max,
            Min=np.min,
            Return=lambda x: Statistics.cumulative_returns(x, history=False, notional=True),
            Count=np.size
        )).round(4)
        df.index.name = 'Year'
        return df
