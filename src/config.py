import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

# Storage
CEF_TICKER_PATH = 'data/cef.csv.gz'
ETF_TICKER_PATH = 'data/etf.csv.gz'
SP500_TICKER_PATH = 'data/sp500.csv.gz'
EQUITY_TICKER_PATH = 'data/equity.csv.gz'

# Local data directory
DATA_DIR = str(os.getenv('DATA_DIR', Path.home() / 'data'))
OPTION_DATA_DIR = os.getenv('OPTION_DATA_DIR', '~/data')

# Email
EMAIL_USER = os.getenv('EMAIL_USER', '')
EMAIL_PW = os.getenv('EMAIL_PW', '')
EMAIL_EIN = 'ein.kro@gmail.com'
EMAIL_KAI = 'kqureshi@mit.edu'
TEST_RECIPIENTS = [EMAIL_EIN]
RECIPIENTS = [EMAIL_EIN, EMAIL_KAI]

# Slack
SLACK_API_TOKEN = os.getenv("SLACK_API_TOKEN", "")
SLACK_REPORT_CHANNEL = '#report'
SLACK_TEST_CHANNEL = '#test'
SLACK_USERNAME = os.getenv('SLACK_USERNAME')
SLACK_EMOJI = os.getenv('SLACK_EMOJI')

# IB
IB_HOST = '127.0.0.1'
IB_LIVE_PORT = 4001
IB_PAPER_PORT = 4002
IB_MARS_PORT = 4003
IB_KALLY_ACCT = 'U4546294'
IB_MARS_ACCT = 'U4337882'

# IB Flex Report
IB_FLEX_REPORT_TOKEN = os.getenv('IB_FLEX_REPORT_TOKEN', '')
CHANGE_IN_NAV_YTD_QUERY = '522184'
CHANGE_IN_NAV_MTD_QUERY = '522183'
CHANGE_IN_NAV_365_QUERY = '522182'
MTD_YTD_PERFORMANCE_QUERY = '522150'

# Portfolio
# TODO: temporarily disable underperformed signals
# SIGNALS = 'Cubby,IBS,Butterfly3X,Factor,GlobalReversal'
SIGNALS = 'Cubby,IBS'
NOTIONAL = 31000

# Alpaca
ALPACA_LIVE_URL = 'https://api.alpaca.markets'
ALPACA_PAPER_URL = 'https://paper-api.alpaca.markets'
ALPACA_DATA_URL = 'https://data.alpaca.markets'
ALPACA_PAPER_KEY_ID = os.getenv('ALPACA_PAPER_KEY_ID', '')
ALPACA_PAPER_SECRET_KEY = os.getenv('ALPACA_PAPER_SECRET_KEY', '')
ALPACA_LIVE_KEY_ID = os.getenv('ALPACA_LIVE_KEY_ID', '')
ALPACA_LIVE_SECRET_KEY = os.getenv('ALPACA_LIVE_SECRET_KEY', '')

# Robinhood
ROBINHOOD_USERNAME = os.getenv('ROBINHOOD_USERNAME')
ROBINHOOD_PASSWORD = os.getenv('ROBINHOOD_PASSWORD')

# InfluxDB
INFLUXDB_HOST = os.getenv('INFLUXDB_HOST', '127.0.0.1')
INFLUXDB_PORT = int(os.getenv('INFLUXDB_PORT', 8086))
INFLUXDB_DB_ALPACA = 'alpaca'
QUOTES = 'quotes'
TRADES = 'trades'
MINUTE_BARS = 'minute_bars'
TRADE_UPDATES = 'trade_updates'

# Google Cloud Storage
# https://cloud.google.com/storage/docs/authentication/managing-hmackeys#create
GOOGLE_ACCESS_KEY_ID = os.getenv("GOOGLE_ACCESS_KEY_ID")
GOOGLE_ACCESS_KEY_SECRET = os.getenv("GOOGLE_ACCESS_KEY_SECRET")
BUCKET = "kally-bot"
