import abc
import pandas as pd

from typing import List, Union


class Data:
    """ Abstract data class. Please see concrete class for usage. """

    @abc.abstractmethod
    async def daily(self,
                    tickers: Union[str, List[str]],
                    start: str = None,
                    end: str = None,
                    field: Union[str, List[str]] = None) -> pd.DataFrame:
        ...

    @abc.abstractmethod
    async def minute(self,
                     tickers: Union[str, List[str]],
                     start: str = None,
                     end: str = None,
                     field: str = None,
                     regular_hours: bool = False) -> pd.DataFrame:
        ...

    @staticmethod
    def regular_hours(df: pd.DataFrame) -> pd.DataFrame:
        """ Filter dataframe for regular market hours. """
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index)
        return df.between_time("09:30", "15:59")

    def _concat(self, symbols: List[str], dfs: List[pd.DataFrame]) -> pd.DataFrame:
        """ Concat dataframe together with multi-index for columns. """
        results = []
        for symbol, df in zip(symbols, dfs):
            if df is not None:
                df.columns = pd.MultiIndex.from_product([[symbol, ], df.columns])
                results.append(df)
        return pd.concat(results, axis=1)

    def _field(self, df: pd.DataFrame, field: str) -> pd.DataFrame:
        """ Filter dataframe for a specific field """
        return df[[field]]

    def _field_xs(self, df: pd.DataFrame, field: str) -> pd.DataFrame:
        """ Filter dataframe with multi-index for a specific field. """
        return df.xs(field, level=1, axis=1)

    def _between(self, df: pd.DataFrame, start: str = None, end: str = None) -> pd.DataFrame:
        """ Filter dataframe between start and end """
        start = df.index[0] if not start else start
        end = df.index[-1] if not end else end
        return df.loc[start:end]

    def _diff(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.pct_change()
