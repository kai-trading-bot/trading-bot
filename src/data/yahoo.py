import pandas as pd

from typing import List, Optional, Union

from src.data.base import Data
from src.data.helpers.async_yahoo import YahooDailyReader
from src.utils.time import timeit, today
from src.utils.fe import START


class Yahoo(Data):
    """ Usage:
        >>> from src.data import Yahoo
        >>> df = await Yahoo().daily(['AAPL', 'FB'])
        >>> df['AAPL']
        >>> df = await Yahoo().daily('AAPL', start='2018-01-02', end='2018-12-31', field=Yahoo.CLOSE)
    """

    OPEN: str = 'Open'
    HIGH: str = 'High'
    LOW: str = 'Low'
    CLOSE: str = 'Close'
    ADJ_CLOSE: str = 'Adj Close'
    DATE: str = 'Date'
    VOLUME: str = 'Volume'

    @timeit
    async def daily(self,
                    tickers: Union[str, List[str]],
                    start: str = START,
                    end: str = today(),
                    field: Optional[Union[str, List[str]]] = ADJ_CLOSE) -> pd.DataFrame:
        df = await YahooDailyReader(tickers, start, end, chunksize=300).read()
        df = df.reset_index()
        df = df.set_index(Yahoo.DATE)
        df = df[~df.index.duplicated(keep='last')]
        if field is not None:
            df = df[field]
        return df

    async def minute(self,
                     tickers: Union[str, List[str]],
                     start: str = None,
                     end: str = None,
                     field: str = None,
                     regular_hours: bool = False) -> pd.DataFrame:
        raise NotImplementedError(f"Yahoo data does not support minute by minute level")
