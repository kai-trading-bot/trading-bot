from alpaca_trade_api.entity import Entity


ORDER_UPDATE_EVENT = Entity({
    'event': 'new',
    'order': {
        'asset_class': 'us_equity',
        'asset_id': '82748036-831f-4b5e-8b09-9cd76ada4ac1',
        'canceled_at': None,
        'client_order_id': 'ec032414-eebb-4ace-986e-712a0b134270',
        'created_at': '2020-11-10T20:45:42.045692Z',
        'expired_at': None,
        'extended_hours': False,
        'failed_at': None,
        'filled_at': None,
        'filled_avg_price': None,
        'filled_qty': '0',
        'hwm': None,
        'id': 'bd97681f-06b7-4c97-a306-8ff31fc7101b',
        'legs': None,
        'limit_price': '34.25',
        'order_class': '',
        'order_type': 'limit',
        'qty': '126',
        'replaced_at': None,
        'replaced_by': None,
        'replaces': None,
        'side': 'sell',
        'status': 'new',
        'stop_price': None,
        'submitted_at': '2020-11-10T20:45:42.043816Z',
        'symbol': 'TMF',
        'time_in_force': 'day',
        'trail_percent': None,
        'trail_price': None,
        'type': 'limit',
        'updated_at': '2020-11-10T20:45:42.070381Z'
    }
})

FILL_EVENT = Entity({
    'event': 'fill',
    'order': {
        'asset_class': 'us_equity',
        'asset_id': '82748036-831f-4b5e-8b09-9cd76ada4ac1',
        'canceled_at': None,
        'client_order_id': 'ec032414-eebb-4ace-986e-712a0b134270',
        'created_at': '2020-11-10T20:45:42.045692Z',
        'expired_at': None,
        'extended_hours': False,
        'failed_at': None,
        'filled_at': '2020-11-10T20:45:42.084766Z',
        'filled_avg_price': '34.25',
        'filled_qty': '126',
        'hwm': None,
        'id': 'bd97681f-06b7-4c97-a306-8ff31fc7101b',
        'legs': None,
        'limit_price': '34.25',
        'order_class': '',
        'order_type': 'limit',
        'qty': '126',
        'replaced_at': None,
        'replaced_by': None,
        'replaces': None,
        'side': 'sell',
        'status': 'filled',
        'stop_price': None,
        'submitted_at': '2020-11-10T20:45:42.043816Z',
        'symbol': 'TMF',
        'time_in_force': 'day',
        'trail_percent': None,
        'trail_price': None,
        'type': 'limit',
        'updated_at': '2020-11-10T20:45:42.092147Z'},
    'position_qty': '128',
    'price': '34.25',
    'qty': '126',
    'timestamp': '2020-11-10T20:45:42.084766762Z'
})

AGG1 = {
    'average': 329.8933,
    'close': 327.92,
    'end': 1604344500000,
    'high': 328.12,
    'low': 327.84,
    'open': 328.09,
    'start': 1604344440000,
    'symbol': 'SPY',
    'totalvolume': 44345235,
    'volume': 111748,
    'vwap': 327.9489
}

# Aggregate bar minute data example
AGG2 = {
    'average': 329.8877,
    'close': 327.7443,
    'end': 1604344560000,
    'high': 327.93,
    'low': 327.69,
    'open': 327.9,
    'start': 1604344500000,
    'symbol': 'SPY',
    'totalvolume': 44463010,
    'volume': 117775,
    'vwap': 327.7926
}

QUOTE = {
    'askexchange': 19,
    'askprice': 361.96,
    'asksize': 12,
    'bidexchange': 8,
    'bidprice': 361.94,
    'bidsize': 3,
    'condition': 1,
    'symbol': 'SPY',
    'timestamp': 1604945390427
}
