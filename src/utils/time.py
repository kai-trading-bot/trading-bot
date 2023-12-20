import arrow
import asyncio
import contextlib
import pandas as pd
import pandas_market_calendars as mcal
import pytz
import time

from datetime import date, datetime, timedelta, timezone
from pandas.tseries.offsets import BDay
from typing import *
from src.utils.logger import logger

EST = pytz.timezone("US/Eastern")
PST = pytz.timezone("US/Pacific")
NYSE = mcal.get_calendar('NYSE')
ISO_FORMAT = '%Y-%m-%d'
COMPACT_FORMAT = '%Y%m%d'


def to_est(timestamp: datetime) -> str:
    """ Convert given timestamp to a string with eastern timezone.
        DO NOT USE.
    """
    if timestamp.tzinfo is None:
        raise Exception(f"Unable to cast a timezone naive timestamp to EST.")
    return timestamp.astimezone(EST).isoformat()


def to_pst(timestamp: Union[str, datetime]) -> str:
    if isinstance(timestamp, str):
        timestamp = arrow.get(timestamp).datetime
    if timestamp.tzinfo is None:
        raise Exception(f"Unable to cast a timezone naive timestamp to EST.")
    return timestamp.astimezone(PST).isoformat()


def to_date(timestamp: str) -> str:
    return arrow.get(timestamp).date().isoformat()


def today(to_string: bool = True) -> Union[date, str]:
    return date.today().isoformat() if to_string else date.today()


def yesterday(to_string: bool = True,
              market_open: bool = True,
              base: Optional[Union[str, date]] = None) -> Union[date, str]:
    """ Return yesterday's date

    :param to_string: whether to return date in string format (default true)
    :param market_open: whether to return the previous market open date (default true)
    :param base: (testing only) today's date
    """
    base = arrow.get(base).date() if base is not None else today(to_string=False)
    d = (base - BDay(1)).date()
    while market_open and not MarketCalendar.open_at_date(d):
        d = (d - BDay(1)).date()
    return d.isoformat() if to_string else d


def now(tz: Optional[pytz.timezone] = timezone.utc) -> datetime:
    return datetime.now(tz)


def is_market_open(buffer: timedelta = timedelta(seconds=10)) -> bool:
    """ Check if market is open currently - buffer(10 seconds) """
    return MarketCalendar.open_at_time(datetime.now(EST) - buffer)


def timeit(func):
    """ A decorator function to benchmark function execution time.
        Usage:
        >>> @timeit
        ... def func():
        ...     print("Hello")

        >>> @timeit
        ... async def func():
        ...     print("Async Hello")

    :param func: arbitrary function or async function
    :return:
    """
    async def process(func: Callable, *args: Any, **params: Any):
        if asyncio.iscoroutinefunction(func):
            return await func(*args, **params)
        else:
            return func(*args, **params)

    async def helper(*args: Any, **params: Any):
        start = time.time()
        result = await process(func, *args, **params)
        logger.debug(f'Time {func.__name__}: {round(time.time() - start, 3)}')
        return result

    return helper


@contextlib.contextmanager
def stopwatch(message: str):
    t0 = time.time()
    try:
        yield
    finally:
        t1 = time.time()
        logger.debug(f"Total time for {message}: {round(t1 - t0, 3)} seconds.")


def iso_to_compact(date: Union[str, datetime]):
    """ Convert an iso date (2020-01-01) into compact format (20200101) """
    if isinstance(date, datetime):
        return date.strftime(COMPACT_FORMAT)
    return datetime.strptime(date, ISO_FORMAT).strftime(COMPACT_FORMAT)


def compact_to_iso(date: str):
    """ Convert an compact format date into isoformat """
    return datetime.strptime(date, COMPACT_FORMAT).strftime(ISO_FORMAT)


def _sanitize(timestamp: Any) -> pd.Timestamp:
    """ Sanitize a timestamp. Throw error if the timestamp is not timezone-aware. """
    if timestamp is None:
        logger.warning(f'No timestamp specified, use current EST time')
        timestamp = now(EST)
    ts = pd.Timestamp(timestamp)
    if ts.tz is None:
        raise Exception(f'Please provide a timestamp with timezone.')
    return ts


class MarketCalendar:
    """ Market Calendar Helper function

        Usage:
        >>> MarketCalendar.open_at_time('2021-08-06 12:30:00-07:00')
        >>> MarketCalendar.open_at_date('2021-08-06')
    """

    @staticmethod
    def open_at_time(timestamp: Any = now(EST)) -> bool:
        """ Check if the market is open with the given timestamp.
            Note the timestamp must be timezone-aware.

        :param timestamp: timestamp in any format (string, pd.Timestamp, datetime, etc)
        """
        ts = pd.Timestamp(timestamp)
        if ts.tz is None:
            raise Exception(f'Please provide a timestamp with timezone: {timestamp}')
        schedule = NYSE.schedule(ts, ts + pd.Timedelta(days=1))
        return NYSE.open_at_time(schedule, ts)

    @staticmethod
    def open_at_date(timestamp: Any = now(EST)) -> bool:
        """ Check if the market is open on the date of the given timestamp.

        :param timestamp: timestamp in any format (string, pd.Timestamp, datetime, etc)
        """
        ts = pd.Timestamp(timestamp)
        schedule = NYSE.schedule(ts, ts + pd.Timedelta(days=1))
        return ts.date().isoformat() in schedule.index

    @staticmethod
    def next_close(timestamp: Any = now(EST)) -> datetime:
        # TODO: this function is confusing.
        ts = pd.Timestamp(timestamp)
        schedule = NYSE.schedule(ts, ts + pd.Timedelta(days=4))
        df = schedule[schedule["market_close"] > ts]
        return df["market_close"].iloc[0].to_pydatetime()

    @staticmethod
    def prev_open_date(timestamp: Any = now(EST)) -> str:
        """ Return previous market open date (isoformat) given the ts or the current time. """
        ts = pd.Timestamp(timestamp)
        schedule = NYSE.schedule(ts - pd.Timedelta(days=4), ts)
        current_date = ts.date().isoformat()
        df = schedule[schedule < current_date].dropna()
        return df.index[-1].date().isoformat()

    @staticmethod
    def most_recent_open_date(timestamp: Any = now(EST)) -> str:
        ts = pd.Timestamp(timestamp)
        if MarketCalendar.open_at_date(ts):
            return ts.date().isoformat()
        else:
            return MarketCalendar.prev_open_date(ts)
