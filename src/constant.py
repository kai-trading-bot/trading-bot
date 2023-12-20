from datetime import datetime
from typing import *

# Cli
CRITICAL = 'CRITICAL'

# IB TODO: move to config
HOST = "127.0.0.1"
LIVE = "LIVE"
LIVE_PORT = 4001
PAPER = "PAPER"
PAPER_PORT = 4002
PORTS = dict(PAPER=PAPER_PORT, LIVE=LIVE_PORT)

# Order Side
BUY = 'BUY'
SELL = 'SELL'

# Order Type
LIMIT = 'LMT'
MARKET = 'MKT'

# Security Type
STK = 'STK'
STOCK = "STK"
OPT = 'OPT'
OPTION = "OPT"
FUT = 'FUT'
FUTURE = "FUT"

# Exchange
SMART = "SMART"
GLOBEX = "GLOBEX"

# Currency
USD = "USD"
CAD = 'CAD'

# Option
CALL = 'C'
PUT = 'P'

# Time In Force
DAY = 'DAY'

# Yahoo
OPEN = 'open'
CLOSE = 'close'
HIGH = 'high'
LOW = 'low'
RETURN = 'return'

# IB Error Msg
NOT_SHORTABLE = (201, "Order rejected - reason:The contract is not available for short sale")
EXCHANGE_CLOSED = (201, "Order rejected - reason:The exchange is closed")
REGULATORY_RESTRICTIONS = (201, "Order rejected - reason:Order Reject: This account cannot trade this security due "
                                "to Regulatory Restrictions")
NO_SEC_DEF = (200, "No security definition has been found for the request")

# Constants
TICKER: str = 'ticker'
DATE: str = 'date'
EXP: str = 'exp'
STRIKE: str = 'strike'
CP: str = 'cp'
DOW: str = 'dow'
DTE: str = 'dte'
DELTA: str = 'delta'
BID: str = 'bid'
ASK: str = 'ask'
MID: str = 'mid'
PRICE: str = 'price'
OPTION_PRICE = 'optionPrice'
ABS_DELTA: str = 'abs_delta'
ABS_DTE: str = 'abs_dte'
FRIDAY = 'Friday'
EPS = 'eps'
DELTA_RATIO = 'delta_ratio'
DTE_RATIO = 'dte_ratio'
PNL = 'pnl'
DATE_EXIT = 'date_exit'
IV = 'iv'
UP = 'up'
UND_PRICE = 'und_price'
BID_SIZE = 'bid_size'
ASK_SIZE = 'ask_size'
VOLUME = 'volume'
OPEN_INTEREST = 'open_interest'
GAMMA = 'gamma'
THETA = 'theta'
VEGA = 'vega'
TIMESTAMP = 'timestamp'
DATE_ENTER = 'date_enter'
DATE_EXIT = 'date_exit'
PRICE_ENTER = 'price_enter'
PRICE_EXIT = 'price_exit'
PRICE_PREV = 'price_prev'
HOLIDAY = 'holiday'
STOCK_PRICE = 'stock_price'
STOCK_PNL = 'stock_pnl'
OPTION_PNL = 'option_pnl'
DAILY = 'daily'
WEEKLY = 'weekly'
MONTHLY = 'monthly'
POSITION = 'position'
SYMBOL = 'symbol'
SEC_TYPE = 'sec_type'
MARKET_PRICE = 'market_price'
MARKET_VALUE = 'market_value'

# Polygon
PRICE = 'price'
SIZE = 'size'
CONDITIONS = 'conditions'
EXCHANGE = 'exchange'
BID_PRICE = 'bid_price'
ASK_PRICE = 'ask_price'
BID_EXCHANGE = 'bid_exchange'
ASK_EXCHANGE = 'ask_exchange'

# Plotting
PEAK = 'Peak'
VALLEY = 'Valley'
RECOVERY = 'Recovery'
DURATION = 'Duration'


# IB order algo strategy
class AlgoStrategy:
    ADAPTIVE = 'Adaptive'


# IB adaptive order priority
class Priority:
    URGENT = 'Urgent'
    NORMAL = 'Normal'
    PATIENT = 'Patient'


class OrderType:
    MKT = 'MKT'
    MARKET = 'market'
    LMT = 'LMT'
    LIMIT = 'limit'
    MOC = 'MOC'
    MTL = 'MTL'


class OrderSide:
    BUY = 'BUY'
    SELL = 'SELL'


# IB Account Tag
EQUITY_WITH_LOAN_VALUE = 'EquityWithLoanValue'
AVAILABLE_FUNDS = 'AvailableFunds'
BUYING_POWER = 'BuyingPower'
CUSHION = 'Cushion'
STOCK_MARKET_VALUE = 'StockMarketValue'
UNREALIZED_PNL = 'UnrealizedPnL'
REALIZED_PNL = 'RealizedPnL'

# IPO
EXPECTED_DATE = 'expected_date'
ISSUER = 'issuer'
SECTOR = 'sector'
BOOKRUNNER = 'bookrunner'
MARKET_CAP = 'market_cap'
OUTSTANDING_SHARES = 'outstanding_shares'
PRICE_RANGE = 'price_range'
TECHNOLOGY = 'Technology'
FINANCIALS = 'Financials'
CONSUMER_SERVICES = 'Consumer Services'
