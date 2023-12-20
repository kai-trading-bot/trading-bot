import datetime
import itertools
import logging
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import sys
from pandas.tseries.offsets import BDay
from typing import *

'''Common'''
ANNUAL = 252
START = '2008-01-01'
NOTIONAL = 10000
DATE = 'date'
TIMESTAMP = 'timestamp'
TICKER = 'ticker'
SYMBOL = 'symbol'
SIGNAL = 'signal'
HOLDINGS = 'holdings'
CLOSE = 'close'
QUANTILE, QUANTILES = 'q', 10
CURRENCY = 'USD'
DATE_FORMAT = '%Y-%m-%d'
DATE_BUFFER = 0.2
PATH = '/'.join(os.path.abspath(os.path.dirname('__file__')).split('/')[:5])
TODAY, YESTERDAY = datetime.datetime.today().strftime(DATE_FORMAT), (datetime.datetime.today() - BDay(1)).strftime(DATE_FORMAT)

'''Order types'''
MARKET_ORDER = 'MKT'
MOC_ORDER = 'MOC'

'''Interactive Brokers'''
period = {'1 min': '1 D', '1 hour': '1 M', '1 day': '1 Y'}
HOST, PORT = "localhost", 7496
PRICE_LABEL = 'C'
BID, ASK, LAST = 'bid', 'ask', 'last'

'''Futures'''
tick_values = {'ES': 50, 'NQ': 20, 'EMD': 100, 'YM': 5}
min_tick = {'ES': 0.25, 'NQ': 0.25, 'EMD': 0.1, 'YM': 1}
exchanges = {'ES': 'GLOBEX', 'NQ': 'GLOBEX', 'EMD': 'GLOBEX', 'YM': 'ECBOT'}
month_codes = {3: '1', 6: '2', 9: '3', 12: '4'}
month_letters = {3: 'H', 6: 'M', 9: 'U', 12: 'Z'}

"""Solver"""
WINDOW = 10
AVERSION = 1
SUM_WEIGHT = 1
MIN_WEIGHT, MAX_WEIGHT = 0, 1
START = '2007-01-02'
LABEL = 'Adj Close'
DEFAULT_SOLVER = 'ECOS'

"""Yahoo"""
YAHOO_LABEL = 'Adj Close'
YAHOO_DATE = 'Date'

"""Logging"""
LOGGER_COLOR = "green"
TEMPLATE_1 = "{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}"
TEMPLATE_2 = "<" + LOGGER_COLOR + ">{time}</" + LOGGER_COLOR + "> <level>{message}</level>"
LOGDIR = '/log/'

"""Plotting"""
plt.style.use('ggplot')
FONTSIZE = 6
ROTATION = 45

"""Quandl Token"""
QUANDL_TOKEN = 'KvPhh9pmGWXbhEmCz2KQ'

"""Type"""
ArrayLike = Union[pd.Series, pd.DataFrame]
DateLike = Union[pd.Timestamp, str, datetime.datetime]

""" Data """
minute_data = PATH + '/data/{}/minute/{}.parquet.gz'
