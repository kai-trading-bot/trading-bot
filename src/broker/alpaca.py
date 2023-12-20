import aiohttp
import asyncio
from alpaca_trade_api.common import URL, FLOAT
from alpaca_trade_api.entity import (
    Account, AccountConfigurations, AccountActivity,
    Asset, Order, Position, Clock, Calendar, PortfolioHistory)
from alpaca_trade_api.rest import RetryException, DATA_V2_MAX_LIMIT
from typing import *

from src.exceptions import NotTradableException, NotShortableException
from src.constant import OrderSide, OrderType
from src.config import *
from src.model import Quote
from src.utils.logger import logger

Positions = List[Position]
Orders = List[Order]
Assets = List[Asset]
AccountActivities = List[AccountActivity]
Calendars = List[Calendar]


class AlpacaOrderStatus:
    NEW = 'new'
    PARTIALLY_FILLED = 'partially_filled'
    FILLED = 'filled'
    DONE_FOR_DAY = 'done_for_day'
    CANCELED = 'canceled'
    EXPIRED = 'expired'
    REPLACED = 'replaced'
    PENDING_CANCEL = 'pending_cancel'
    PENDING_REPLACE = 'pending_replace'
    ACCEPTED = 'accepted'
    PENDING_NEW = 'pending_new'
    ACCEPTED_FOR_BIDDING = 'accepted_for_bidding'
    STOPPED = 'stopped'
    REJECTED = 'rejected'
    SUSPENDED = 'suspended'
    CALCULATED = 'calculated'

    @staticmethod
    def is_active(status: str) -> bool:
        return status in (
            AlpacaOrderStatus.NEW,
            AlpacaOrderStatus.PARTIALLY_FILLED,
            AlpacaOrderStatus.ACCEPTED,
            AlpacaOrderStatus.PENDING_NEW
        )


class Alpaca:
    """ An asyncio implementation of Alpaca API
        Link: https://github.com/alpacahq/alpaca-trade-api-python/blob/master/alpaca_trade_api/rest.py
    """

    def __init__(self,
                 key_id: str = ALPACA_PAPER_KEY_ID,
                 secret_key: str = ALPACA_PAPER_SECRET_KEY,
                 base_url: str = ALPACA_PAPER_URL):
        self._key_id = key_id
        self._secret_key = secret_key
        self.base_url = URL(base_url)
        self.data_url = URL(ALPACA_DATA_URL)
        self.api_version = 'v2'
        self._retry = 3
        self._retry_wait = 3
        self._retry_codes = [429, 504]

    async def _request(self,
                       method: str,
                       path: str,
                       data: Dict = None,
                       base_url: URL = None,
                       api_version: str = None):
        base_url = base_url or self.base_url
        version = api_version or self.api_version
        url = URL(base_url + '/' + version + path)
        headers = {'APCA-API-KEY-ID': self._key_id, 'APCA-API-SECRET-KEY': self._secret_key}
        opts = {
            'headers': headers,
            # Since we allow users to set endpoint URL via env var,
            # human error to put non-SSL endpoint could exploit
            # uncanny issues in non-GET request redirecting http->https.
            # It's better to fail early if the URL isn't right.
            'allow_redirects': False,
        }
        if method.upper() == 'GET':
            opts['params'] = data
        else:
            opts['json'] = data

        retry = self._retry
        while retry >= 0:
            try:
                return await self._one_request(method, url, opts, retry)
            except RetryException:
                retry_wait = self._retry_wait
                logger.warning(f'sleep {retry_wait} seconds and retrying {url} {retry} more time(s)...')
                await asyncio.sleep(retry_wait)
                retry -= 1

    async def _one_request(self, method: str, url: URL, opts: dict, retry: int):
        """
        Perform one request, possibly raising RetryException in the case
        the response is 429. Otherwise, if error text contain "code" string,
        then it decodes to json object and returns APIError.
        Returns the body json in the 200 status.
        """
        async with aiohttp.ClientSession() as session:
            async with session.request(method, url, **opts) as resp:
                if resp.status in self._retry_codes and retry > 0:
                    raise RetryException()
                resp.raise_for_status()
                text = await resp.text()
                if text != '':
                    return await resp.json()
                return None

    async def get(self, path: str, data: Dict = None):
        return await self._request('GET', path, data)

    async def post(self, path: str, data: Dict = None):
        return await self._request('POST', path, data)

    async def put(self, path, data: Dict = None):
        return await self._request('PUT', path, data)

    async def patch(self, path: str, data: Optional[Dict] = None):
        return await self._request('PATCH', path, data)

    async def delete(self, path: str, data: Optional[Dict] = None):
        return await self._request('DELETE', path, data)

    async def data_get(self, path: str, data: Optional[Dict] = None, api_version: str = 'v1'):
        return await self._request('GET', path, data, base_url=self.data_url, api_version=api_version)

    async def _data_get(self,
                        endpoint: str,
                        symbol_or_symbols: Union[str, List[str]],
                        api_version: str = 'v2',
                        endpoint_base: str = 'stocks',
                        **kwargs) -> Dict:
        """ Get data for V2 APIs.
        Source: https://github.com/alpacahq/alpaca-trade-api-python/blob/03bb3e9153810256bfa6ca7a3fb1aca67a7d0437/alpaca_trade_api/rest.py#L596
        """
        page_token = None
        total_items = 0
        limit = kwargs.get('limit')
        while True:
            actual_limit = None
            if limit:
                actual_limit = min(int(limit) - total_items, DATA_V2_MAX_LIMIT)
                if actual_limit < 1:
                    break
            data = kwargs
            data['limit'] = actual_limit
            data['page_token'] = page_token
            if isinstance(symbol_or_symbols, str):
                path = f'/{endpoint_base}/{symbol_or_symbols}/{endpoint}'
            else:
                path = f'/{endpoint_base}/{endpoint}'
                data['symbols'] = ','.join(symbol_or_symbols)
            resp = await self.data_get(path, data=data, api_version=api_version)
            if isinstance(symbol_or_symbols, str):
                for item in resp.get(endpoint, []) or []:
                    yield item
                    total_items += 1
            else:
                by_symbol = resp.get(endpoint, {}) or {}
                for sym, items in sorted(by_symbol.items()):
                    for item in items or []:
                        item['S'] = sym
                        yield item
                        total_items += 1
            page_token = resp.get('next_page_token')
            if not page_token:
                break

    # -*- Account -*-

    async def get_account(self) -> Account:
        """Get the account"""
        resp = await self.get('/account')
        return Account(resp)

    async def get_account_configurations(self) -> AccountConfigurations:
        """Get account configs"""
        resp = await self.get('/account/configurations')
        return AccountConfigurations(resp)

    def get_side(self, quantity: int) -> str:
        return OrderSide.BUY.lower() if quantity > 0 else OrderSide.SELL.lower()

    def opposite_signs(self, x: int, y: int) -> bool:
        return (x ^ y) < 0

    # -*- Market data -*-

    async def get_quote(self, symbol: str) -> Quote:
        """ Get the latest quote of the give symbol.

        Args:
            symbol (str): The stock symbol.

        Note:
            Alpaca's market data V2 free plan DOES NOT provide consolidated (accurate) market data.
            The exchange it uses is IEX. Quote's bid and ask range can be larger than the NBBO.
            Please do not use this method in production!

        Example:
            >>> quote = await self.get_quote('MSFT')
            >>> quote.to_dict()
            >>> {'symbol': 'MSFT',
                 'bid': 329.85,
                 'ask': 345,
                 'mid': 337.43,
                 'timestamp': '2021-11-22T09:41:19.775560-08:00'}
        """
        resp = await self.data_get(f'/stocks/{symbol}/quotes/latest', api_version='v2')
        quote = Quote(bid=resp['quote']['bp'], ask=resp['quote']['ap'], timestamp=resp['quote']['t'])
        quote.symbol = resp['symbol']
        return quote

    # -*- Rebalance -*
    # This section is deprecated.

    async def rebalance(self, symbol: str, target_qty: int, type: str, dry_run: bool = False) -> List[Order]:
        """ Rebalance the symbol's position into target_qty. This method also handles:
            1) Flip long / short positions by flattening the position first to avoid Forbidden Error.
            2) Throw exception if symbol is not shortable or tradable.

            :param symbol: symbol to rebalance
            :param target_qty: target position
            :param type: order type (market or limit)
            :param dry_run: whether to log the order quantity as opposed to submitting order
        """
        position, asset = await asyncio.gather(*[self.get_position(symbol), self.get_asset(symbol)])
        current_qty = 0 if position is None else int(position.qty)
        if not asset.tradable or asset.status != 'active':
            raise NotTradableException(symbol)
        if (not asset.shortable or not asset.easy_to_borrow) and target_qty < 0:
            raise NotShortableException(symbol)
        if self.opposite_signs(current_qty, target_qty):
            # Two trades: flatten current position + enter new position
            # TODO: the first order needs to be executed before the second one. Maybe use clear_position
            qtys = [-current_qty, target_qty]
        else:
            qtys = [target_qty - current_qty]
        logger.info(f'Rebalance {symbol}: current={current_qty}, target={target_qty}, order(s) to be placed: {qtys}')
        if dry_run:
            return []
        return await asyncio.gather(*[self.submit(symbol, qty, type) for qty in qtys if qty != 0])

    def shortable(self, asset: Asset):
        return asset.shortable and asset.easy_to_borrow

    async def submit(self, symbol: str, quantity: int, type: str, quote: Optional[Quote] = None) -> Order:
        """ Submit an order. Raise exception on error (ClientResponseError)

            :param symbol: Stock symbol
            :param quantity: Order quantity in either positive or negative
            :param type: Order type. Only support `limit` or `market`
            :param quote: Optional quote
        """
        assert quantity != 0, f'{symbol} order quantity cannot be 0'
        side = self.get_side(quantity)
        if type not in (OrderType.MARKET, OrderType.LIMIT):
            raise Exception(f'Order type {type} is not supported.')
        if type == OrderType.LIMIT and quote is None:
            quote = await self.get_quote(symbol)
        price = quote.mid if quote else None
        order = await self.submit_order(symbol, abs(quantity), side, order_type=type, limit_price=price)
        logger.info(f'Order submitted: {symbol} Side={side}, Qty={abs(quantity)}, Price={price}')
        return order

    # -*- Order -*-

    async def list_orders(self,
                          status: str = None,
                          limit: int = None,
                          after: str = None,
                          until: str = None,
                          direction: str = None,
                          params=None,
                          nested: bool = None,
                          symbols: List[str] = None) -> Orders:
        """
        Get a list of orders
        https://docs.alpaca.markets/web-api/orders/#get-a-list-of-orders
        :param status: open, closed or all. Defaults to open.
        :param limit: Defaults to 50 and max is 500
        :param after: timestamp
        :param until: timestamp
        :param direction: asc or desc.
        :param params: refer to documentation
        :param nested: should the data be nested like json. If true, the result will roll up multi-leg orders under the
                       legs field of primary order.
        :param symbols: list of str (symbols)
        """
        if params is None:
            params = dict()
        if limit is not None:
            params['limit'] = limit
        if after is not None:
            params['after'] = after
        if until is not None:
            params['until'] = until
        if direction is not None:
            params['direction'] = direction
        if status is not None:
            params['status'] = status
        if nested is not None:
            params['nested'] = nested
        if symbols is not None:
            params['symbols'] = ",".join(symbols)
        url = '/orders'
        resp = await self.get(url, params)
        return [Order(o) for o in resp]

    async def submit_order(self,
                           symbol: str,
                           qty: float = None,
                           side: str = "buy",
                           order_type: str = "market",
                           time_in_force: str = "day",
                           limit_price: str = None,
                           stop_price: str = None,
                           client_order_id: str = None,
                           extended_hours: bool = None,
                           order_class: str = None,
                           take_profit: dict = None,
                           stop_loss: dict = None,
                           trail_price: str = None,
                           trail_percent: str = None,
                           notional: float = None):
        """
        :param symbol: symbol or asset ID
        :param qty: float. Mutually exclusive with "notional".
        :param side: buy or sell
        :param order_type: market, limit, stop, stop_limit or trailing_stop
        :param time_in_force: day, gtc, opg, cls, ioc, fok
        :param limit_price: str of float
        :param stop_price: str of float
        :param client_order_id:
        :param extended_hours: bool. If true, order will be eligible to execute
               in premarket/afterhours.
        :param order_class: simple, bracket, oco or oto
        :param take_profit: dict with field "limit_price" e.g
               {"limit_price": "298.95"}
        :param stop_loss: dict with fields "stop_price" and "limit_price" e.g
               {"stop_price": "297.95", "limit_price": "298.95"}
        :param trail_price: str of float
        :param trail_percent: str of float
        :param notional: float. Mutually exclusive with "qty".
        """
        params = {
            'symbol': symbol,
            'side': side,
            'type': order_type,
            'time_in_force': time_in_force
        }
        if qty is not None:
            params['qty'] = qty
        if notional is not None:
            params['notional'] = notional
        if limit_price is not None:
            params['limit_price'] = FLOAT(limit_price)
        if stop_price is not None:
            params['stop_price'] = FLOAT(stop_price)
        if client_order_id is not None:
            params['client_order_id'] = client_order_id
        if extended_hours is not None:
            params['extended_hours'] = extended_hours
        if order_class is not None:
            params['order_class'] = order_class
        if take_profit is not None:
            if 'limit_price' in take_profit:
                take_profit['limit_price'] = FLOAT(take_profit['limit_price'])
            params['take_profit'] = take_profit
        if stop_loss is not None:
            if 'limit_price' in stop_loss:
                stop_loss['limit_price'] = FLOAT(stop_loss['limit_price'])
            if 'stop_price' in stop_loss:
                stop_loss['stop_price'] = FLOAT(stop_loss['stop_price'])
            params['stop_loss'] = stop_loss
        if trail_price is not None:
            params['trail_price'] = trail_price
        if trail_percent is not None:
            params['trail_percent'] = trail_percent
        resp = await self.post('/orders', params)
        return Order(resp)

    async def get_order(self, order_id: str) -> Order:
        return Order(await self.get(f'/orders/{order_id}'))

    async def replace_order(self,
                            order_id: str,
                            qty: str = None,
                            limit_price: Union[str, float] = None,
                            stop_price: str = None,
                            trail: str = None,
                            time_in_force: str = None,
                            client_order_id: str = None) -> Order:
        """
        :param order_id:
        :param qty: str of int
        :param limit_price: str of float
        :param stop_price: str of float
        :param trail: str of float, represents trailing_price or
               trailing_percent. determined by the original order.
        :param time_in_force: day, gtc, opg, cls, ioc, fok
        note: you cannot replace type of order. so, it was trailing_stop(e.g)
              it will remain trailing_stop.
        """
        params = {}
        if qty is not None:
            params['qty'] = qty
        if limit_price is not None:
            params['limit_price'] = FLOAT(limit_price)
        if stop_price is not None:
            params['stop_price'] = FLOAT(stop_price)
        if trail is not None:
            params['trail'] = FLOAT(trail)
        if time_in_force is not None:
            params['time_in_force'] = time_in_force
        if client_order_id is not None:
            params['client_order_id'] = client_order_id
        resp = await self.patch('/orders/{}'.format(order_id), params)
        return Order(resp)

    async def cancel_order(self, order_id: str) -> None:
        """Cancel an order"""
        await self.delete('/orders/{}'.format(order_id))

    async def cancel_all_orders(self) -> None:
        """Cancel all open orders"""
        await self.delete('/orders')

    # -*- Positions -*-

    async def list_positions(self) -> Positions:
        """Get a list of open positions"""
        resp = await self.get('/positions')
        return [Position(o) for o in resp]

    async def get_position(self, symbol: str) -> Optional[Position]:
        """ Get an open position

            Return None if position not found.
        """
        try:
            resp = await self.get(f'/positions/{symbol}')
            return Position(resp)
        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                return None
            raise e

    async def close_position(self, symbol: str) -> Order:
        """Liquidates the position for the given symbol at market price"""
        resp = await self.delete('/positions/{}'.format(symbol))
        return Order(resp)

    async def close_all_positions(self) -> Orders:
        """Liquidates all open positions at market price"""
        resp = await self.delete('/positions')
        return [Order(o) for o in resp]

    async def list_assets(self, status=None, asset_class=None) -> Assets:
        """Get a list of assets"""
        params = {
            'status': status,
            'asset_class': asset_class,
        }
        resp = await self.get('/assets', params)
        return [Asset(o) for o in resp]

    async def get_asset(self, symbol: str) -> Asset:
        """Get an asset"""
        resp = await self.get(f'/assets/{symbol}')
        return Asset(resp)

    # -*- Clock and Calendar -*-

    async def get_clock(self) -> Clock:
        """ Get market clock

        Example:
            >>> await self.get_clock()
            >>> Clock({ 'is_open': True,
                        'next_close': '2021-11-22T16:00:00-05:00',
                        'next_open': '2021-11-23T09:30:00-05:00',
                        'timestamp': '2021-11-22T13:34:45.356244622-05:00'})
        """
        return Clock(await self.get('/clock'))

    async def is_market_open(self) -> bool:
        return (await self.get_clock()).is_open

    async def get_calendar(self, start: str = None, end: str = None) -> Calendars:
        """ Get the market calendar

        Args:
            start (str): isoformat date string eg '2006-01-02T15:04:05Z' or '2006-01-02'
            end (str): isoformat date string (exclusive)

        Returns:
            An array of Calendars ::
                {
                  "date": "2018-01-03",
                  "open": "09:30",
                  "close": "16:00"
                }
            If the market is closed, the date will not show up in the list of calendars.
        """
        params = {}
        if start is not None:
            params['start'] = start
        if end is not None:
            params['end'] = end
        resp = await self.get('/calendar', data=params)
        return [Calendar(o) for o in resp]

    # -*- Activities -*-

    async def get_activities(self,
                             activity_types: str = None,
                             until: str = None,
                             after: str = None,
                             direction: str = None,
                             date: str = None,
                             page_size: int = None,
                             page_token: str = None) -> AccountActivities:
        """
        go to alpaca.markets/docs/api-documentation/api-v2/account-activities/
        :param activity_types: go to documnetation to see available types
        :param until: isoformat timestamp
        :param after: isoformat timestamp
        :param direction: asc or sesc. default is desc
        :param date: str. can't be sued with until/after
        :param page_size:
        :param page_token:
        :return:
        """
        url = '/account/activities'
        params = {}
        if isinstance(activity_types, list):
            params['activity_types'] = ','.join(activity_types)
        elif activity_types is not None:
            url += '/{}'.format(activity_types)
        if after is not None:
            params['after'] = after
        if until is not None:
            params['until'] = until
        if direction is not None:
            params['direction'] = direction
        if date is not None:
            params['date'] = date
        if page_size is not None:
            params['page_size'] = page_size
        if page_token is not None:
            params['page_token'] = page_token
        resp = await self.get(url, data=params)
        return [AccountActivity(o) for o in resp]

    async def get_portfolio_history(self,
                                    date_start: str = None,
                                    date_end: str = None,
                                    period: str = None,
                                    timeframe=None,
                                    extended_hours: bool = None) -> PortfolioHistory:
        """
        alpaca.markets/docs/api-documentation/api-v2/portfolio-history/
        :param date_start: YYYY-MM-DD
        :param date_end: YYYY-MM-DD
        :param period: The duration of the data in <number> + <unit>
               such as 1D, where <unit> can be D for day, W for week,
               M for month and A for year. Defaults to 1M.
        :param timeframe: The resolution of time window. 1Min, 5Min, 15Min,
               1H, or 1D
        :param extended_hours: bool. If true, include extended hours in the
               result.
        """
        params = {}
        if date_start is not None:
            params['date_start'] = date_start
        if date_end is not None:
            params['date_end'] = date_end
        if period is not None:
            params['period'] = period
        if timeframe is not None:
            params['timeframe'] = timeframe
        if extended_hours is not None:
            params['extended_hours'] = extended_hours
        resp = await self.get('/account/portfolio/history', data=params)
        return PortfolioHistory(resp)
