"""
This class is deprecated. Polygon now cannot be accessed via Alpaca.
"""
import aiohttp
import datetime
import dateutil.parser
import pandas as pd

from alpaca_trade_api.polygon.entity import (
    Aggsv2, Aggsv2Set, Trade, TradesV2, Quote, QuotesV2,
    Exchange, SymbolTypeMap, ConditionMap, Company, Dividends, Splits,
    Earnings, Financials, NewsList, Ticker, DailyOpenClose, Symbol
)
from alpaca_trade_api.common import URL, DATE

from src.config import AlpacaConfig
from src.constant import *
from src.data.base import Data
from src.utils.logger import logger

Exchanges = List[Exchange]
Tickers = List[Ticker]
Symbols = List[Symbol]


def _is_list_like(o) -> bool:
    """
    returns True if o is either a list, a set or a tuple
    that way we could accept ['AAPL', 'GOOG'] or ('AAPL', 'GOOG') etc.
    """
    return isinstance(o, (list, set, tuple))


def format_date_for_api_call(date):
    """
    we support different date formats:
    - string
    - datetime.date
    - datetime.datetime
    - timestamp number
    - pd.Timestamp
    that gives the user the freedom to use the API in a very flexible way
    """
    if isinstance(date, datetime.datetime):
        return date.date().isoformat()
    elif isinstance(date, datetime.date):
        return date.isoformat()
    elif isinstance(date, str):  # string date
        return dateutil.parser.parse(date).date().isoformat()
    elif isinstance(date, int) or isinstance(date, float):
        # timestamp number
        return int(date)
    else:
        raise Exception(f"Unsupported date format: {date}")


def fix_daily_bar_date(date, timespan):
    """
    the polygon api does not include the end date for daily bars, or this:
    historic_agg_v2("SPY", 1, "day", _from="2020-07-22", to="2020-07-24").df
    results in this:
    timestamp
    2020-07-22 00:00:00-04:00  324.62  327.20  ...  57917101.0  325.8703
    2020-07-23 00:00:00-04:00  326.47  327.23  ...  75841843.0  324.3429
    the 24th data is missing
    for minute bars, it does include the end date
    so basically this method will add 1 day (if 'to' is not today, we don't
    have today's data until tomorrow) to the 'to' field
    """
    if timespan == 'day':
        date = dateutil.parser.parse(date)
        today = datetime.datetime.utcnow().date()
        if today != date.date():
            date = date + datetime.timedelta(days=1)
        date = date.date().isoformat()
    return date


class Polygon:

    def __init__(self):
        self._base_url = 'https://api.polygon.io/'
        self._session = None
        self._api_key = AlpacaConfig.LIVE_KEY_ID
        self._staging = False

    async def connect(self):
        self._session = aiohttp.ClientSession()
        logger.info(f'Polygon connected.')

    async def disconnect(self):
        await self._session.close()
        self._session = None
        logger.info(f'Polygon disconnected.')

    def is_connected(self) -> bool:
        return self._session is not None and not self._session.closed

    async def minute(self,
                     symbol: str,
                     start: str,
                     end: str,
                     field: Optional[str] = None,
                     regular_hours: bool = True) -> pd.DataFrame:
        """ Return minute data. Not end is not necessarily respected if too much data """
        if not self.is_connected():
            await self.connect()
        data = await self.historic_agg_v2(symbol, 1, 'minute', start, end)
        df = data.df
        if regular_hours:
            df = Data.regular_hours(df)
        return df

    async def quotes(self, symbol: str, date: DATE, timestamp: Optional[int] = None) -> pd.DataFrame:
        """ Get historical quotes for the given symbol. Max 50000 rows. """
        if not self.is_connected():
            await self.connect()
        quotes = await self.historic_quotes_v2(symbol, date, timestamp)
        df = pd.DataFrame([quote._raw for quote in quotes])
        # TODO: sip_timestamp vs participant_timestamp
        df['timestamp'] = pd.to_datetime(df['sip_timestamp'])
        df = df.set_index('timestamp')
        return df[[BID_PRICE, BID_SIZE, BID_EXCHANGE, ASK_PRICE, ASK_SIZE, ASK_EXCHANGE]]

    async def trades(self, symbol: str, date: DATE, timestamp: Optional[int] = None) -> pd.DataFrame:
        """ Return the historical trades for symbol. Max 50000 rows.

            To get next page, set the timestamp to be the last timestamp
            timestamp = df.index.astype(np.int64)[-1]
        """
        if not self.is_connected():
            await self.connect()
        trades = await self.historic_trades_v2(symbol, date, timestamp)
        df = pd.DataFrame([trade._raw for trade in trades])
        # TODO: sip_timestamp vs participant_timestamp
        df['timestamp'] = pd.to_datetime(df['sip_timestamp'])
        df = df.set_index('timestamp')
        return df[[PRICE, SIZE, EXCHANGE]]

    async def _request(self, method: str, path: str, params: dict = None, version: str = 'v1'):
        """
        :param method: GET, POST, ...
        :param path: url part path (without the domain name)
        :param params: dictionary with params of the request
        :param version: v1 or v2
        :return: response
        """
        url = URL(self._base_url + version + path)
        params = params or {}
        params['apiKey'] = self._api_key
        if self._staging:
            params['apiKey'] += '-staging'
        async with self._session.request(method, url, params=params) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def get(self, path: str, params: dict = None, version: str = 'v1'):
        return await self._request('GET', path, params=params, version=version)

    async def historic_trades_v2(self,
                                 symbol: str,
                                 date: DATE,
                                 timestamp: int = None,
                                 timestamp_limit: int = None,
                                 reverse: bool = None,
                                 limit: int = None) -> TradesV2:
        """
        polygon.io/docs/#get_v2_ticks_stocks_trades__ticker___date__anchor
        :param symbol
        :param date: DATE in this format YYYY-MM-DD
        :param timestamp: timestamp integer
        :param timestamp_limit: timestamp integer. offset, used for pagination.
        :param reverse: bool
        :param limit: max 50000
        :return:
        """
        path = '/ticks/stocks/trades/{}/{}'.format(symbol, date)
        params = {}
        if timestamp is not None:
            params['timestamp'] = timestamp
        if timestamp_limit is not None:
            params['timestampLimit'] = timestamp_limit
        if reverse is not None:
            params['reverse'] = reverse
        if limit is not None:
            params['limit'] = limit
        raw = await self.get(path, params, 'v2')

        return TradesV2(raw)

    async def historic_quotes_v2(self,
                                 symbol: str,
                                 date: DATE,
                                 timestamp: int = None,
                                 timestamp_limit: int = None,
                                 reverse: bool = None,
                                 limit: int = None) -> QuotesV2:
        """
        polygon.io/docs/#get_v2_ticks_stocks_nbbo__ticker___date__anchor
        :param symbol
        :param date: DATE in this format YYYY-MM-DD
        :param timestamp: timestamp integer. offset, used for pagination.
        :param timestamp_limit: timestamp integer
        :param reverse: bool
        :param limit: max 50000
        :return:
        """
        path = '/ticks/stocks/nbbo/{}/{}'.format(symbol, date)
        params = {}
        if timestamp is not None:
            params['timestamp'] = timestamp
        if timestamp_limit is not None:
            params['timestampLimit'] = timestamp_limit
        if reverse is not None:
            params['reverse'] = reverse
        if limit is not None:
            params['limit'] = limit
        raw = await self.get(path, params, 'v2')

        return QuotesV2(raw)

    async def historic_agg_v2(self,
                              symbol: str,
                              multiplier: int,
                              timespan: str,
                              _from,
                              to,
                              unadjusted: bool = False,
                              limit: int = None) -> Aggsv2:
        """
        :param symbol:
        :param multiplier: Size of the timespan multiplier (distance between
               samples. e.g if it's 1 we get for daily 2015-01-05, 2015-01-06,
                            2015-01-07, 2015-01-08.
                            if it's 3 we get 2015-01-01, 2015-01-04,
                            2015-01-07, 2015-01-10)
        :param timespan: Size of the time window: minute, hour, day, week,
               month, quarter, year
        :param _from: acceptable types: isoformat string, timestamp int,
               datetime object
        :param to: same as _from
        :param unadjusted
        :param limit: max samples to retrieve
        :return:
        """
        path_template = '/aggs/ticker/{symbol}/range/{multiplier}/' \
                        '{timespan}/{_from}/{to}'
        path = path_template.format(
            symbol=symbol,
            multiplier=multiplier,
            timespan=timespan,
            _from=format_date_for_api_call(_from),
            to=fix_daily_bar_date(format_date_for_api_call(to), timespan)
        )
        params = {'unadjusted': str(unadjusted)}
        if limit:
            params['limit'] = limit
        raw = await self.get(path, params, version='v2')
        return Aggsv2(raw)

    async def last_trade(self, symbol: str) -> Trade:
        path = '/last/stocks/{}'.format(symbol)
        raw = await self.get(path)
        return Trade(raw['last'])

    async def last_quote(self, symbol: str) -> Quote:
        path = '/last_quote/stocks/{}'.format(symbol)
        raw = await self.get(path)
        return Quote(raw['last'])

    async def previous_day_bar(self, symbol: str) -> Aggsv2:
        path = '/aggs/ticker/{}/prev'.format(symbol)
        raw = await self.get(path, version='v2')
        return Aggsv2(raw)

    async def _get_symbol(self, symbol: str, resource: str, entity):
        multi = _is_list_like(symbol)
        symbols = symbol if multi else [symbol]
        if len(symbols) > 50:
            raise ValueError('too many symbols: {}'.format(len(symbols)))
        params = {
            'symbols': ','.join(symbols),
        }
        path = '/meta/symbols/{}'.format(resource)
        res = await self.get(path, params=params)
        if isinstance(res, list):
            res = {o['symbol']: o for o in res}
        retmap = {sym: entity(res[sym]) for sym in symbols if sym in res}
        if not multi:
            return retmap.get(symbol)
        return retmap

    async def dividends(self, symbol: str) -> Dividends:
        return await self._get_symbol(symbol, 'dividends', Dividends)

    async def splits(self, symbol: str) -> Splits:
        path = f'/reference/splits/{symbol}'
        raw = await self.get(path, version='v2')
        return Splits(raw['results'])

    async def earnings(self, symbol: str) -> Earnings:
        return await self._get_symbol(symbol, 'earnings', Earnings)

    async def financials(self, symbol: str) -> Financials:
        return await self._get_symbol(symbol, 'financials', Financials)

    async def news(self, symbol: str) -> NewsList:
        path = '/meta/symbols/{}/news'.format(symbol)
        raw = await self.get(path)
        return NewsList(raw)

    async def gainers_losers(self, direction: str = "gainers") -> Tickers:
        path = '/snapshot/locale/us/markets/stocks/{}'.format(direction)
        raw = await self.get(path, version='v2')
        return [Ticker(ticker) for ticker in raw['tickers']]
