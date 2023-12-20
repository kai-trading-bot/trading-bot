import arrow
import pandas as pd
import pytest
import pytz

from datetime import datetime

from src.utils.time import EST, MarketCalendar, yesterday


@pytest.mark.parametrize("timestamp, expected", [
    (pd.Timestamp("2020-01-02 09:30:00", tz='US/Eastern'), True),
    ("2020-01-03T10:00:00Z", False),
    (pd.Timestamp("2020-01-04 13:00:00", tz='US/Eastern'), False),
    (pd.Timestamp("2019-12-24 10:00:00", tz='US/Eastern'), True),  # Christmas Eve
    (pd.Timestamp("2019-12-24 13:00:00", tz='US/Eastern'), False),  # Christmas Eve (Half Day)
    (pd.Timestamp("2019-12-25 09:40:00", tz="US/Eastern"), False),  # Christmas
    (pd.Timestamp("2020-05-22 16:05:00", tz="US/Eastern"), False),
    ("2020-05-25T10:00:00-04:00", False),  # Memorial Day
])
def test_open_at_time(timestamp, expected):
    res = MarketCalendar.open_at_time(timestamp)
    assert res == expected


@pytest.mark.parametrize("timestamp, expected", [
    ("2020-01-01", False),
    (pd.Timestamp("2020-01-02 09:30:00", tz='US/Eastern'), True),
    ("2020-01-03T10:00:00Z", True),
    (pd.Timestamp("2020-01-04 13:00:00", tz='US/Eastern'), False),
    (pd.Timestamp("2019-12-24 10:00:00", tz='US/Eastern'), True),   # Christmas Eve (Half Day)
    (pd.Timestamp("2019-12-24 13:00:00", tz='US/Eastern'), True),   # Christmas Eve (Half Day)
    (pd.Timestamp("2019-12-25 09:40:00", tz="US/Eastern"), False),  # Christmas
    (pd.Timestamp("2020-05-22 16:05:00", tz="US/Eastern"), True),
    ("2020-05-25 10:00:00-07:00", False),  # Memorial Day
])
def test_open_at_date(timestamp, expected):
    res = MarketCalendar.open_at_date(timestamp)
    assert res == expected


def test_invalid_open_at_time():
    timestamp = '2021-08-06T13:23:34'
    with pytest.raises(Exception):
        MarketCalendar.open_at_time(timestamp)
    assert MarketCalendar.open_at_date(timestamp) is True


@pytest.mark.freeze_time("2020-05-16 15:00:00-04:00")
@pytest.mark.parametrize("ts, expected", [
    ("2020-05-15 15:50:00-04:00", datetime(2020, 5, 15, 20, tzinfo=pytz.UTC)),
    ("2020-05-22 16:01:00-04:00", datetime(2020, 5, 26, 20, tzinfo=pytz.UTC)),
    ("2020-12-24 12:59:00-05:00", datetime(2020, 12, 24, 18, tzinfo=pytz.UTC)),
])
def test_next_close(ts, expected):
    assert MarketCalendar.next_close(ts) == expected


@pytest.mark.parametrize("ts, expected", [
    ("2020-07-02 12:45:00-07:00", '2020-07-01'),
    ('2020-07-04 12:45:00-07:00', '2020-07-02'),
])
def test_prev_open_date(ts, expected):
    assert MarketCalendar.prev_open_date(ts) == expected


@pytest.mark.freeze_time("2020-09-19 14:55:00-07:00")
@pytest.mark.parametrize("base, expected", [
    (None, '2020-09-18'),
    ('2020-05-25', '2020-05-22'),
    ('2020-05-26', '2020-05-22'),
    ('2020-12-25', '2020-12-24'),
    ('2020-12-26', '2020-12-24'),
])
def test_yesterday(base, expected):
    assert yesterday(base=base) == expected
