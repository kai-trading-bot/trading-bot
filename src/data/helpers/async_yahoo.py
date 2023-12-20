import asyncio
import datetime
import json
import re
import time
import warnings
from abc import ABCMeta
from urllib.parse import urlencode

import aiohttp
import numpy as np
import requests
from pandas import DataFrame, isnull, notnull, to_datetime
from pandas import concat, read_csv
from pandas_datareader._utils import RemoteDataError
from pandas_datareader._utils import (
    SymbolWarning,
    _init_session,
    _sanitize_dates,
)
from pandas_datareader.compat import (
    PANDAS_0230,
    StringIO,
    binary_type,
    bytes_to_str,
    string_types,
)
from pandas_datareader.yahoo.headers import DEFAULT_HEADERS


class _BaseReader:
    """
    Parameters
    ----------
    symbols : {str, List[str]}
        String symbol of like of symbols
    start : string, int, date, datetime, Timestamp
        Starting date. Parses many different kind of date
        representations (e.g., 'JAN-01-2010', '1/1/10', 'Jan, 1, 1980')
    end : string, int, date, datetime, Timestamp
        Ending date
    retry_count : int, default 3
        Number of times to retry query request.
    pause : float, default 0.1
        Time, in seconds, of the pause between retries.
    session : Session, default None
        requests.sessions.Session instance to be used
    freq : {str, None}
        Frequency to use in select readers
    """

    _chunk_size = 1024 * 1024
    _format = "string"

    def __init__(
        self,
        symbols,
        start=None,
        end=None,
        retry_count=3,
        pause=0.1,
        timeout=30,
        session=None,
        freq=None,
    ):

        self.symbols = symbols

        start, end = _sanitize_dates(start or self.default_start_date, end)
        self.start = start
        self.end = end

        if not isinstance(retry_count, int) or retry_count < 0:
            raise ValueError("'retry_count' must be integer larger than 0")
        self.retry_count = retry_count
        self.pause = pause
        self.timeout = timeout
        self.pause_multiplier = 1
        self.session = _init_session(session)
        self.freq = freq
        self.headers = None

    def close(self):
        """Close network session"""
        self.session.close()

    @property
    def default_start_date(self):
        """Default start date for reader. Defaults to 5 years before current date"""
        today = datetime.date.today()
        return today - datetime.timedelta(days=365 * 5)

    @property
    def url(self):
        """API URL"""
        # must be overridden in subclass
        raise NotImplementedError

    @property
    def params(self):
        """Parameters to use in API calls"""
        return None

    async def read(self):
        """Read data from connector"""
        try:
            return await self._read_one_data(self.url, self.params)
        finally:
            self.close()

    async def _read_one_data(self, url, params):
        """ read one data from specified URL """
        if self._format == "string":
            out = await self._read_url_as_StringIO(url, params=params)
        elif self._format == "json":
            raise Exception(f'Do not support json format')
        else:
            raise NotImplementedError(self._format)
        return self._read_lines(out)

    async def _read_url_as_StringIO(self, url, params=None):
        """
        Open url (and retry)
        """
        print(f"reading url as string io: {url}")
        text = await self._get_response(url, params=params)
        out = StringIO()
        if len(text) == 0:
            service = self.__class__.__name__
            raise IOError(
                "{} request returned no data; check URL for invalid "
                "inputs: {}".format(service, self.url)
            )
        if isinstance(text, binary_type):
            out.write(bytes_to_str(text))
        else:
            out.write(text)
        out.seek(0)
        return out

    async def _get_response(self, url, params=None, headers=None) -> str:
        """ send raw HTTP request to get requests.Response from the specified url
        Parameters
        ----------
        url : str
            target URL
        params : dict or None
            parameters passed to the URL
        """
        headers = headers or self.headers
        pause = self.pause
        last_response_text = ""
        for _ in range(self.retry_count + 1):
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                async with session.get(url, params=params, headers=headers) as response:
                    if response.status == requests.codes.ok:
                        return await response.text()
            await asyncio.sleep(pause)

            # Increase time between subsequent requests, per subclass.
            pause *= self.pause_multiplier

        if params is not None and len(params) > 0:
            url = url + "?" + urlencode(params)
        msg = "Unable to read URL: {0}".format(url)
        if last_response_text:
            msg += "\nResponse Text:\n{0}".format(last_response_text)

        raise RemoteDataError(msg)

    def _read_lines(self, out):
        rs = read_csv(out, index_col=0, parse_dates=True, na_values=("-", "null"))[::-1]
        # Needed to remove blank space character in header names
        rs.columns = list(map(lambda x: x.strip(), rs.columns.values.tolist()))

        # Yahoo! Finance sometimes does this awesome thing where they
        # return 2 rows for the most recent business day
        if len(rs) > 2 and rs.index[-1] == rs.index[-2]:  # pragma: no cover
            rs = rs[:-1]
        # Get rid of unicode characters in index name.
        try:
            rs.index.name = rs.index.name.decode("unicode_escape").encode(
                "ascii", "ignore"
            )
        except AttributeError:
            # Python 3 string has no decode method.
            rs.index.name = rs.index.name.encode("ascii", "ignore").decode()

        return rs


class _DailyBaseReader(_BaseReader, metaclass=ABCMeta):
    """ Base class for Google / Yahoo daily reader """

    def __init__(
        self,
        symbols=None,
        start=None,
        end=None,
        retry_count=3,
        pause=0.1,
        session=None,
        chunksize=25,
    ):
        super(_DailyBaseReader, self).__init__(
            symbols=symbols,
            start=start,
            end=end,
            retry_count=retry_count,
            pause=pause,
            session=session,
        )
        self.chunksize = chunksize

    def _get_params(self, *args, **kwargs):
        raise NotImplementedError

    async def read(self):
        """Read data"""
        # If a single symbol, (e.g., 'GOOG')
        if isinstance(self.symbols, (string_types, int)):
            df = await self._read_one_data(self.url, params=self._get_params(self.symbols))
        # Or multiple symbols, (e.g., ['GOOG', 'AAPL', 'MSFT'])
        elif isinstance(self.symbols, DataFrame):
            df = await self._dl_mult_symbols(self.symbols.index)
        else:
            df = await self._dl_mult_symbols(self.symbols)
        return df

    async def _dl_mult_symbols(self, symbols):
        stocks = {}
        failed = []
        passed = []

        async def query(symbol: str):
            try:
                return await self._read_one_data(self.url, self._get_params(symbol))
            except (IOError, KeyError):
                return None

        for sym_group in _in_chunks(symbols, self.chunksize):
            tasks = [query(symbol) for symbol in sym_group]
            dfs = await asyncio.gather(*tasks)
            for symbol, df in zip(sym_group, dfs):
                if df is not None:
                    passed.append(symbol)
                    stocks[symbol] = df
                else:
                    failed.append(symbol)
                    msg = "Failed to read symbol: {0!r}, replacing with NaN."
                    warnings.warn(msg.format(symbol), SymbolWarning)

        if len(passed) == 0:
            msg = "No data fetched using {0!r}"
            raise RemoteDataError(msg.format(self.__class__.__name__))
        try:
            if len(stocks) > 0 and len(failed) > 0 and len(passed) > 0:
                df_na = stocks[passed[0]].copy()
                df_na[:] = np.nan
                for sym in failed:
                    stocks[sym] = df_na
            if PANDAS_0230:
                result = concat(stocks, sort=True).unstack(level=0)
            else:
                result = concat(stocks).unstack(level=0)
            result.columns.names = ["Attributes", "Symbols"]
            return result
        except AttributeError:
            # cannot construct a panel with just 1D nans indicating no data
            msg = "No data fetched using {0!r}"
            raise RemoteDataError(msg.format(self.__class__.__name__))


def _in_chunks(seq, size):
    """
    Return sequence in 'chunks' of size defined by size
    """
    return (seq[pos : pos + size] for pos in range(0, len(seq), size))


class YahooDailyReader(_DailyBaseReader):

    def __init__(
        self,
        symbols=None,
        start=None,
        end=None,
        retry_count=3,
        pause=0.1,
        session=None,
        adjust_price=False,
        ret_index=False,
        chunksize=1,
        interval="d",
        get_actions=False,
        adjust_dividends=True,
    ):
        super(YahooDailyReader, self).__init__(
            symbols=symbols,
            start=start,
            end=end,
            retry_count=retry_count,
            pause=pause,
            session=session,
            chunksize=chunksize,
        )

        # Ladder up the wait time between subsequent requests to improve
        # probability of a successful retry
        self.pause_multiplier = 2.5

        if session is None:
            self.headers = DEFAULT_HEADERS
        else:
            self.headers = session.headers

        self.adjust_price = adjust_price
        self.ret_index = ret_index
        self.interval = interval
        self._get_actions = get_actions

        if self.interval not in ["d", "wk", "mo", "m", "w"]:
            raise ValueError(
                "Invalid interval: valid values are  'd', 'wk' and 'mo'. 'm' and 'w' "
                "have been implemented for backward compatibility. 'v' has been moved "
                "to the yahoo-actions or yahoo-dividends APIs."
            )
        elif self.interval in ["m", "mo"]:
            self.pdinterval = "m"
            self.interval = "mo"
        elif self.interval in ["w", "wk"]:
            self.pdinterval = "w"
            self.interval = "wk"

        self.interval = "1" + self.interval
        self.adjust_dividends = adjust_dividends

    @property
    def get_actions(self):
        return self._get_actions

    @property
    def url(self):
        return "https://finance.yahoo.com/quote/{}/history"

    # Test test_get_data_interval() crashed because of this issue, probably
    # whole yahoo part of package wasn't
    # working properly
    def _get_params(self, symbol):
        # This needed because yahoo returns data shifted by 4 hours ago.
        four_hours_in_seconds = 14400
        unix_start = int(time.mktime(self.start.timetuple()))
        unix_start += four_hours_in_seconds
        day_end = self.end.replace(hour=23, minute=59, second=59)
        unix_end = int(time.mktime(day_end.timetuple()))
        unix_end += four_hours_in_seconds

        params = {
            "period1": unix_start,
            "period2": unix_end,
            "interval": self.interval,
            "frequency": self.interval,
            "filter": "history",
            "symbol": symbol,
        }
        return params

    async def _read_one_data(self, url, params):
        """ read one data from specified symbol """

        symbol = params["symbol"]
        del params["symbol"]
        url = url.format(symbol)

        text = await self._get_response(url, params=params)
        ptrn = r"root\.App\.main = (.*?);\n}\(this\)\);"
        try:
            j = json.loads(re.search(ptrn, text, re.DOTALL).group(1))
            data = j["context"]["dispatcher"]["stores"]["HistoricalPriceStore"]
        except KeyError:
            msg = "No data fetched for symbol {} using {}"
            raise RemoteDataError(msg.format(symbol, self.__class__.__name__))

        # price data
        prices = DataFrame(data["prices"])
        prices.columns = [col.capitalize() for col in prices.columns]
        prices["Date"] = to_datetime(to_datetime(prices["Date"], unit="s").dt.date)

        if "Data" in prices.columns:
            prices = prices[prices["Data"].isnull()]
        prices = prices[["Date", "High", "Low", "Open", "Close", "Volume", "Adjclose"]]
        prices = prices.rename(columns={"Adjclose": "Adj Close"})

        prices = prices.set_index("Date")
        prices = prices.sort_index().dropna(how="all")

        if self.ret_index:
            prices["Ret_Index"] = _calc_return_index(prices["Adj Close"])
        if self.adjust_price:
            prices = _adjust_prices(prices)

        # dividends & splits data
        if self.get_actions and data["eventsData"]:

            actions = DataFrame(data["eventsData"])
            actions.columns = [col.capitalize() for col in actions.columns]
            actions["Date"] = to_datetime(
                to_datetime(actions["Date"], unit="s").dt.date
            )

            types = actions["Type"].unique()
            if "DIVIDEND" in types:
                divs = actions[actions.Type == "DIVIDEND"].copy()
                divs = divs[["Date", "Amount"]].reset_index(drop=True)
                divs = divs.set_index("Date")
                divs = divs.rename(columns={"Amount": "Dividends"})
                prices = prices.join(divs, how="outer")

            if "SPLIT" in types:

                def split_ratio(row):
                    if float(row["Numerator"]) > 0:
                        return eval(row["Splitratio"])
                    else:
                        return 1

                splits = actions[actions.Type == "SPLIT"].copy()
                splits["SplitRatio"] = splits.apply(split_ratio, axis=1)
                splits = splits.reset_index(drop=True)
                splits = splits.set_index("Date")
                splits["Splits"] = splits["SplitRatio"]
                prices = prices.join(splits["Splits"], how="outer")

                if "DIVIDEND" in types and not self.adjust_dividends:
                    # dividends are adjusted automatically by Yahoo
                    adj = (
                        prices["Splits"].sort_index(ascending=False).fillna(1).cumprod()
                    )
                    prices["Dividends"] = prices["Dividends"] / adj

        return prices


def _adjust_prices(hist_data, price_list=None):
    """
    Return modifed DataFrame with adjusted prices based on
    'Adj Close' price. Adds 'Adj_Ratio' column.
    """
    if price_list is None:
        price_list = "Open", "High", "Low", "Close"
    adj_ratio = hist_data["Adj Close"] / hist_data["Close"]

    data = hist_data.copy()
    for item in price_list:
        data[item] = hist_data[item] * adj_ratio
    data["Adj_Ratio"] = adj_ratio
    del data["Adj Close"]
    return data


def _calc_return_index(price_df):
    """
    Return a returns index from a input price df or series. Initial value
    (typically NaN) is set to 1.
    """
    df = price_df.pct_change().add(1).cumprod()
    mask = notnull(df.iloc[1]) & isnull(df.iloc[0])
    if mask:
        df.loc[df.index[0]] = 1

    # Check for first stock listings after starting date of index in ret_index
    # If True, find first_valid_index and set previous entry to 1.
    if not mask:
        tstamp = df.first_valid_index()
        t_idx = df.index.get_loc(tstamp) - 1
        df.iloc[t_idx] = 1
    return df
