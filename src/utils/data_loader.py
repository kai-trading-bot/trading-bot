import cryptocompare
import datetime
import glob
import io
import logging
import math
import numpy as np
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import time
from binance.client import Client
from dateutil import parser
from multiprocessing import Pool, cpu_count
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)
Timestamp = Union[datetime.datetime, int, float]

__author__ = 'kqureshi'


class Cryptocompare:

    def __init__(self):

        '''Default settings (only 7 days of data, not worth creating .ini)'''
        self._URL_HIST_PRICE_MINUTE = 'https://min-api.cryptocompare.com/data/histominute?fsym={}&tsym={}&limit={}&e={}&toTs={}'
        self.SYMS = ['BTC', 'ETH', 'XRP', 'USDT', 'BCH', 'LTC', 'BSV', 'EOS', 'BNB', 'XTZ', 'XLM', 'FTT', 'LINK', 'ADA',
                     'TRX',
                     'XMR', 'USDC', 'ETC', 'CRO', 'DASH']
        self.START = datetime.datetime.now() - datetime.timedelta(days=7)
        self.CURR = 'USD'
        self.LIMIT = 2000
        self.PATH = os.getcwd() + '/data/{}.parquet.gz'

    @staticmethod
    def _query_cryptocompare(url: str, errorCheck: bool = True) -> Optional[Dict]:
        """Sourced from cryptocompare"""
        try:
            response = requests.get(url).json()
        except Exception as e:
            return None
        if errorCheck and (response.get('Response') == 'Error'):
            return None
        return response

    @staticmethod
    def _format_parameter(parameter: object) -> str:
        if isinstance(parameter, list):
            return ','.join(parameter)
        else:
            return str(parameter)

    @staticmethod
    def _format_timestamp(timestamp: Timestamp) -> int:
        if isinstance(timestamp, datetime.datetime):
            return int(time.mktime(timestamp.timetuple()))
        return int(timestamp)

    def get_historical_price_minute(self, coin: str, exchange: str = 'CCCAGG', toTs: Timestamp = time.time(),
                                    curr: str = 'USD', limit: int = 1000) -> Optional[Dict]:
        response = self._query_cryptocompare(
            self._URL_HIST_PRICE_MINUTE.format(coin, self._format_parameter(curr), limit, exchange,
                                               self._format_timestamp(toTs)))
        if response:
            return response['Data']
        return None

    @staticmethod
    def _fetch_coins(verbose: bool = True) -> List[str]:
        return cryptocompare.get_coin_list(format=verbose)

    @staticmethod
    def _fetch_price(sym: str, base: str = 'USD', verbose: bool = False) -> pd.DataFrame:
        return pd.DataFrame(cryptocompare.get_price(sym, curr=base, full=verbose))

    def _fetch_history(self, sym: str, base: str = 'USD', max_time: Timestamp = time.time(),
                       limit: Union[None, int] = 2000):
        return pd.DataFrame(self.get_historical_price_minute(sym, curr=base, toTs=max_time))

    def store(self, sym: str, base: str = 'USD') -> None:
        vec = []
        _time = self._format_timestamp(datetime.datetime.now())
        while _time > self._format_timestamp(self.START):
            try:
                vec.append(self._fetch_history(sym=sym, base=base, limit=self.LIMIT, max_time=_time))
                _time -= self.LIMIT
            except Exception as error:
                logging.error(error)
                break
                pass
        data = pd.concat(vec, axis=0)
        data['timestamp'] = pd.Series([datetime.datetime.fromtimestamp(ele) for ele in list(data['time'])],
                                      index=data.index)
        data = data.drop('time', axis=1).rename(
            columns={'volumefrom': 'volume', 'volumeto': 'volume_{}'.format(base)}).set_index('timestamp')
        pq.write_table(pa.Table.from_pandas(data.sort_index(), preserve_index=True), self.PATH.format(sym),
                       compression='gzip')

    def rebuild(self, syms: List[str]) -> None:
        syms = self._fetch_coins() if not syms else syms
        for sym in syms:
            self.store(sym=sym)
            logging.info('Stored {}'.format(sym))

    def mp_rebuild(self, syms: List[str]) -> None:
        Pool(cpu_count()).map(self.store, syms)


class CryptoDownload:

    def __init__(self):

        self.FREQ = '1min'
        self.EXCHANGE = 'gemini'
        self.START = 2015
        self.END = 2019
        self.BASE = 'https://www.cryptodatadownload.com/cdd/'
        self.SYMS = ['BTCUSD', 'ETHUSD', 'LTCUSD', 'ZECUSD']

    def rebuild(self) -> None:
        vec = []
        for sym in self.SYMS:
            for year in range(self.START, self.END + 1):
                url = '{}{}_{}_{}_{}.csv'.format(self.BASE, self.EXCHANGE, sym, year, self.FREQ)
                df = pd.read_csv(io.StringIO(requests.get(url, verify=False).content.decode('utf-8')))
                if len(df) > 0:
                    vec.append(df[df.columns[0]])
        pq.write_table(pa.Table.from_pandas(pd.concat(vec, axis=0).sort_index(), preserve_index=True),
                       self.PATH.format('data'), compression='gzip')


class Binance:

    def __init__(self):
        self.api_key = 'xMUtV6o51c46J71lMalKTSNKmwJIJlCkWiIdWFZ1SxPYP6X41sjUkC39pXJOhZn5'
        self.api_secret = '55h0TLQTaMvV6L4qw9wuaBIgP2DmsndfmWHBYXNOCsXrDg9WFScWeR2r583kIreb'
        self.binsizes = {"1m": 1, "5m": 5, "1h": 60, "1d": 1440}
        self.batch_size = 750
        self.PATH = str(Path.home()) + '/PycharmProjects/crypto_bot/data/{}_data.parquet.gz'
        self.LABELS = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_av',
                       'trades', 'tb_base_av', 'tb_quote_av', 'ignore']
        self.src = 'binance'
        self.START = '1 Jan 2017'
        self.TIMESTAMP_FORMAT = "%d %b %Y %H:%M:%S"
        self.TIMESTAMP = 'timestamp'

    def minute_data(self, symbol, kline_size, data, source, client) -> Tuple[Timestamp, Timestamp]:
        if len(data) > 0:
            old = parser.parse(data["timestamp"].iloc[-1])
        elif source == "binance":
            old = datetime.datetime.strptime(self.START, '%d %b %Y')
        elif source == "bitmex":
            old = \
                client.Trade.Trade_getBucketed(symbol=symbol, binSize=kline_size, count=1, reverse=False).result()[
                    0][0][self.TIMESTAMP]
        if source == "binance": new = pd.to_datetime(
            client.get_klines(symbol=symbol, interval=kline_size)[-1][0], unit='ms')
        if source == "bitmex": new = \
            client.Trade.Trade_getBucketed(symbol=symbol, binSize=kline_size, count=1, reverse=True).result()[0][0][
                self.TIMESTAMP]
        return old, new

    def get_binance(self, symbol: str, kline_size: str, save=False) -> Union[None, pd.DataFrame]:
        binance_client = Client(api_key=Binance().api_key, api_secret=Binance().api_secret)
        filename = self.PATH.format(symbol, kline_size)
        if os.path.isfile(filename):
            data_df = pq.read_table(filename).to_pandas()
        else:
            data_df = pd.DataFrame()
        oldest_point, newest_point = self.minute_data(symbol, kline_size, data_df, source=self.src,
                                                      client=binance_client)
        delta_min = (newest_point - oldest_point).total_seconds() / 60
        available_data = math.ceil(delta_min / self.binsizes[kline_size])
        if oldest_point == datetime.datetime.strptime(self.START, '%d %b %Y'):
            logging.info('Downloading all available {} data for {}.'.format(kline_size, symbol))
        else:
            logging.info('Downloading {} minutes of new data available for {}, i.e. {} instances of {} data.').format(
                delta_min, symbol, available_data, kline_size)
        klines = binance_client.get_historical_klines(symbol, kline_size, oldest_point.strftime(self.TIMESTAMP_FORMAT),
                                                      newest_point.strftime(self.TIMESTAMP_FORMAT))
        data = pd.DataFrame(klines, columns=self.LABELS)
        data[self.TIMESTAMP] = pd.to_datetime(data[self.TIMESTAMP], unit='ms')
        if len(data_df) > 0:
            data_df = data_df.append(pd.DataFrame(data)).set_index(self.TIMESTAMP)
        else:
            data_df = data.set_index(self.TIMESTAMP)
        if save:
            pq.write_table(pa.Table.from_pandas(data_df, preserve_index=True), filename, compression='gzip')
        else:
            return data_df

    @staticmethod
    def _fetch(symbol: str, freq: str = '1m', save: bool = True) -> None:
        return Binance().get_binance(symbol=symbol, kline_size=freq, save=save)

    def rebuild(self, base: Union[None, str] = 'USD') -> None:
        binance_client = Client(api_key=Binance().api_key, api_secret=Binance().api_secret)
        universe = [ele for ele in pd.DataFrame(binance_client.get_all_tickers())['symbol'].tolist() if
                    ele[-3:] == base]
        Pool(cpu_count()).map(self._fetch, universe)


class Data:

    def __init__(self):
        self.src = os.getcwd() + '/data/{}_data.parquet.gz'
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
            logger.info('{} for {}'.format(ke, sym))
            pass

    def build(self, syms: Union[None, List[str]] = None, cache: bool = True, field: str = 'close') -> pd.DataFrame:
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
