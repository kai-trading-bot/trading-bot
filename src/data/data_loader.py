import datetime
import glob
import logging
import numpy as np
import os
import pandas as pd
import pandas_datareader.data as web
import pyarrow as pa
import pyarrow.parquet as pq
from multiprocessing import Pool, cpu_count
from pandas_datareader.famafrench import get_available_datasets
from typing import Dict, List, Optional, Tuple, Union
from src.utils.tools.files import Git, Parquet
from src.utils.fe import *

Timestamp = Union[datetime.datetime, int, float]
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

__author__ = 'kqureshi'


class Stock:

    @staticmethod
    def daily(tickers: List[str], start: Optional[str] = None, diff: bool = True, field: Optional[str] = None) -> pd.DataFrame:

        """
        Usage: data = yahoo_data(tickers = ['SPY', 'QQQ'])
        (1) Set diff = False for prices rather than returns
        (2) Start date
        """

        vec = []
        ticker_list = []
        start_date = START if not start else pd.to_datetime(start).strftime(DATE_FORMAT)
        end_date = pd.datetime.today().strftime(DATE_FORMAT)
        field = YAHOO_LABEL if not field else field
        for ticker in tickers:
            try:
                df = web.DataReader(ticker, 'yahoo', start_date, end_date).reset_index()
                df = df.set_index(YAHOO_DATE)[field]
                vec.append(df[~df.index.duplicated(keep='last')])
                ticker_list.append(ticker)
            except Exception as e:
                print(e)
                pass
        if diff:
            return pd.concat([df.pct_change() for df in vec], axis=1, keys=ticker_list)
        else:
            return pd.concat(vec, axis=1, keys=ticker_list)


class Factor:

    @staticmethod
    def fama_french_factors() -> pd.DataFrame:
        """
        Fetch 4 FF Factors
        :return:
        """
        research_factors = web.DataReader('F-F_Research_Data_Factors_daily', 'famafrench', start=START)[0]
        momentum_factor = web.DataReader('F-F_Momentum_Factor_daily', 'famafrench', start=START)[0]
        df = (research_factors.join(momentum_factor).dropna()) / 100
        df.index = df.index.tz_localize('utc').date
        df.columns = df.columns.str.strip()
        return df

    def list_ff_datasets(self) -> List[str]:
        return get_available_datasets()


class Data:

    def __init__(self):
        self.src = os.getcwd() + '/data/{}/{}_data.parquet.gz'
        self.open = 'open'
        self.high = 'high'
        self.low = 'low'
        self.close = 'close'
        self.volume = 'volume'
        self.qv = 'quote_av'
        self.trades = 'trades'
        self.tbbv = 'tb_base_av'
        self.tbqv = 'tb_quote_av'
        self.default_field = 'close'

    @staticmethod
    def _format_timestamp(date: str, timestring: str) -> Timestamp:
        """
        Cast strings of date and time to timestamp
        """
        return pd.to_datetime('{} {}'.format(date, timestring))

    def _read(self, data: Union[Tuple[str, str]], start: Union[None, Timestamp] = None,
              end: Union[None, Timestamp] = None) -> pd.DataFrame:
        """
        Private function to read parquet.gz files based on tuples of sym/field
        """
        sym, columns = data[0], data[1]
        try:
            data = pq.read_table(self.src.format(sym)).to_pandas()[columns]
            if start and end:
                return data.loc[start:end]
            elif start:
                return data.loc[start:]
            elif end:
                return data.loc[:end]
            return data
        except KeyError as ke:
            LOGGER.info('{} for {}'.format(ke, sym))
            pass

    def build_binance(self, syms: Union[None, List[str]] = None, cache: bool = True,
                      field: str = 'close') -> pd.DataFrame:
        """
        Rebuild wide dataframes per field for cross-sectional signal generation
        """
        syms = [ele.split('/')[-1].split('.')[0].replace('_data', '') for ele in
                glob.glob(self.src.replace('{}', '*'))] if not syms else syms
        data = list(zip(syms, [field] * len(syms)))
        data = pd.concat(Pool(cpu_count()).imap(self._read, data), axis=1, keys=syms).fillna(np.nan)
        if cache:
            pq.write_table(pa.Table.from_pandas(data, preserve_index=True), self.src.format(field), compression='gzip')
        else:
            return data

    def build(self, pattern: str, cols: List[str], asset: str, index: List[str] = ['timestamp', 'symbol']) -> None:

        """
        asset = 'futures'
        pattern = '{}/data/{}/*.parquet.gz'.format(Git.get_root(), asset)
        cols = ['close']
        Data().build(pattern=pattern, cols=cols, asset=asset)
        :param pattern: File string pattern
        :param cols: columns to include in frame
        :param index: indexing for table
        :return:
        """

        files = Parquet().build(pattern=pattern, cols=cols, index=index)
        data = Parquet().multi_read(data_list=files, axis=0)
        data = data[cols[0]].unstack().reset_index()
        data.timestamp = pd.to_datetime(data.timestamp.str[:-6])
        data = data.set_index('timestamp')
        pq.write_table(pa.Table.from_pandas(data, preserve_index=True), self.src.format(asset, cols[0]),
                       compression='gzip')

    def rebuild(self) -> None:
        """
        End-user method to rebuild feature frames
        Example: Data().rebuild()
        """
        [self.build(field=field) for field in [self.close, self.volume, self.trades, self.high, self.low, self.open]]

    def load(self, syms: Union[None, List[str]] = None, field: str = 'close', start: Union[None, Timestamp] = None,
             end: Union[None, Timestamp] = None) -> pd.DataFrame:

        """
        End-user method to access feature
        Example: Data().load(syms=['BTCTUSD', 'DASHBUSD'], field = 'close')
        """
        data = pq.read_table(self.src.format(field)).to_pandas()
        syms = list(data.columns) if not syms else syms
        start = data.index[0] if not start else start
        end = data.index[-1] if not end else end
        return data[syms].loc[start:end]


def _read(path: str, cols: List[str] = [CLOSE]) -> pd.DataFrame:
    df = Parquet().read(path, cols=cols)
    df[TIMESTAMP] = pd.to_datetime(df[TIMESTAMP])
    return df.set_index([TIMESTAMP, SYMBOL])


def mp_read(data: Tuple[Any, Any]) -> pd.DataFrame:
    path, cols = data[0], data[1]
    df = Parquet().read(path, cols=cols)
    df[TIMESTAMP] = pd.to_datetime(df[TIMESTAMP])
    return df.set_index([TIMESTAMP, SYMBOL])
