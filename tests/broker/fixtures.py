from alpaca_trade_api.entity import (
    Account, AccountConfigurations, AccountActivity,
    Asset, Order, Position, BarSet, Clock, Calendar,
    Aggs, Trade, Quote, PortfolioHistory
)

POSITION = Position({
    'asset_class': 'us_equity',
    'asset_id': 'dce2ac30-c928-4416-be25-2213d057f30a',
    'avg_entry_price': '143.56',
    'change_today': '0.0024803637866887',
    'cost_basis': '4306.8',
    'current_price': '145.5',
    'exchange': 'NASDAQ',
    'lastday_price': '145.14',
    'market_value': '4365',
    'qty': '30',
    'side': 'long',
    'symbol': 'TQQQ',
    'unrealized_intraday_pl': '10.8',
    'unrealized_intraday_plpc': '0.0024803637866887',
    'unrealized_pl': '58.2',
    'unrealized_plpc': '0.0135135135135135'})
