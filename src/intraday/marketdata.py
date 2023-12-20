""" This class is deprecated. """
import alpaca_trade_api as tradeapi
import click

from alpaca_trade_api.entity import Entity
from alpaca_trade_api.polygon.entity import Agg, Quote, Trade
from typing import List

from src.config import AlpacaConfig, INFLUXDB_DB_ALPACA, QUOTES, TRADES, MINUTE_BARS, TRADE_UPDATES
from src.storage.influxdb import InfluxDB
from src.utils import parse
from src.utils.logger import logger, get_logger
from src.utils.time import now, PST

opts = dict(
    key_id=AlpacaConfig.PAPER_KEY_ID,
    secret_key=AlpacaConfig.PAPER_SECRET_KEY,
    base_url=AlpacaConfig.PAPER_URL,
    data_stream='polygon',
)


def unix_now() -> int:
    # Current unix timestamp in MS (compatible with Polygon)
    return int(now(PST).timestamp() * 1000)


class Handler:

    def __init__(self):
        self.db = InfluxDB(INFLUXDB_DB_ALPACA)
        self.logger = get_logger('alpaca_marketdata')

    async def handle_minute_bars(self, data: Agg) -> None:
        try:
            point = {
                'time': data.start,
                'measurement': MINUTE_BARS,
                'tags': {'symbol': data.symbol},
                'fields': {
                    'average': float(data.average),
                    'close': float(data.close),
                    'high': float(data.high),
                    'low': float(data.low),
                    'open': float(data.open),
                    'total_volume': data.totalvolume,
                    'volume': data.volume,
                    'vwap': float(data.vwap),
                    'end': data._raw['end'],
                    'received_at': unix_now(),
                }
            }
            await self.db.write(point)
        except Exception as e:
            self.logger.error(f'Minute Bar Error: {e}. Data: {data}')

    async def handle_quote(self, data: Quote) -> None:
        try:
            point = {
                'time': data.timestamp,
                'measurement': QUOTES,
                'tags': {'symbol': data.symbol},
                'fields': {
                    'bid_exchange': data.bidexchange,
                    'bid_price': float(data.bidprice),
                    'bid_size': data.bidsize,
                    'ask_exchange': data.askexchange,
                    'ask_price': float(data.askprice),
                    'ask_size': data.asksize,
                    'received_at': unix_now(),
                }
            }
            await self.db.write(point)
        except Exception as e:
            self.logger.error(f'Quote error: {e}. Data: {data}')

    async def on_trade(self, data: Trade):
        try:
            point = {
                'time': data.timestamp,
                'measurement': TRADES,
                'tag': {'symbol': data.symbol},
                'fields': {
                    'price': float(data.price),
                    'size': data.size,
                    'exchange': data.exchange,
                    'received_at': unix_now(),
                }
            }
            await self.db.write(point)
        except Exception as e:
            self.logger.error(f'Trade error: {e}. {data}')

    async def handle_trade_updates(self, data):
        try:
            raw = data._raw
            order = Entity(data.order)
            position_qty = raw.get('position_qty', 0)
            price = raw.get('price', 0)
            qty = raw.get('qty', 0)
            point = {
                'time': order.updated_at,
                'measurement': TRADE_UPDATES,
                'tags': {
                    'symbol': order.symbol,
                    'event': data.event,
                },
                'fields': {
                    'price': float(price),
                    'qty': int(qty),
                    'position_qty': int(position_qty),  # Total number of shares
                    'order_side': order.side,
                    'order_qty': int(order.qty),
                    'order_type': order.order_type,
                    'filled_qty': int(order.filled_qty),
                    'filled_avg_price': float(order.filled_avg_price),
                    'limit_price': float(data.order.get('limit_price', 0)),
                    'status': order.status,
                    'created_at': order.created_at,
                    'filled_at': order.filled_at,
                    'received_at': unix_now(),
                }
            }
            await self.db.write(point)
        except Exception as e:
            self.logger.error(f'Trade Updates Error: {e}. Data: {data}')


def run(subscriptions: List[str]):
    conn = tradeapi.StreamConn(**opts)
    handler = Handler()
    logger.info(f'Alpaca Stream connected.')

    # Polygon quote
    @conn.on(r'^Q$')
    async def on_quote(conn, channel, data: Quote):
        # logger.debug(f'Quote: {data}')
        await handler.handle_quote(data)

    # Aggregated minute bar
    # Note Polygon generates each minute bar 4 seconds after the minute,
    # meaning the bar for 09:30 is pushed around 09:31.04.
    @conn.on(r'^AM$')
    async def on_bars(conn, channel, data: Agg):
        await handler.handle_minute_bars(data)

    @conn.on(r'^T$')
    async def on_trade(conn, channel, data: Trade):
        await handler.on_trade(data)

    @conn.on(r'trade_updates')
    async def on_trade_updates(conn, channel, data):
        await handler.handle_trade_updates(data)

    conn.run(subscriptions)


@click.group()
def cli():
    ...


@click.command('quotes')
@click.option('-s', '--symbols', default='', help=f'A list of symbols')
def run_quotes(symbols: str):
    subscriptions = [f'Q.{symbol}' for symbol in parse(symbols) if symbol]
    logger.info(f'Quotes subscription: {subscriptions}')
    run(subscriptions)


@click.command('trades')
@click.option('-s', '--symbols', default='', help='A list of symbols')
def run_trades(symbols: str):
    subscriptions = [f'T.{symbol}' for symbol in parse(symbols) if symbol]
    logger.info(f'Trades subscription: {subscriptions}')
    run(subscriptions)


@click.command('minute_bars')
@click.option('-s', '--symbols', default='', help=f'A list of symbols')
def run_minute_bars(symbols: str):
    subscriptions = [f'AM.{symbol}' for symbol in parse(symbols) if symbol]
    logger.info(f'Minute bars subscription: {subscriptions}')
    run(subscriptions)


@click.command('trade_updates')
def run_trade_updates():
    run(['trade_updates'])


# Alpaca limits the connection to one account per one connection.
# For quotes and trades the upper bound of # subscription is 20.
# This can be fixed to use proxy agent in the future.
@click.command('run')
@click.option('-q', '--quotes', default='', help='A common separate list of symbols to receive quotes (max 20)')
@click.option('-t', '--trades', default='', help='A common separate list of symbols to receive trades (max 20)')
@click.option('-am', '--minute_bars', default='', help='A common separate list of symbols to receive trades minute bars')
def run_all(quotes: str, trades: str, minute_bars: str):
    q = [f'Q.{symbol}' for symbol in parse(quotes) if symbol]
    t = [f'T.{symbol}' for symbol in parse(trades) if symbol]
    am = [f'AM.{symbol}' for symbol in parse(minute_bars) if symbol]
    logger.info(f'Quotes: {q}, Trades: {t}, Minute Bars: {am}')
    run(q + t + am)


if __name__ == '__main__':
    cli.add_command(run_quotes)
    cli.add_command(run_minute_bars)
    cli.add_command(run_trade_updates)
    cli.add_command(run_all)
    cli()
