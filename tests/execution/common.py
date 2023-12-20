import arrow
import ib_insync as ib
import json
import random

from dacite import from_dict, Config
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import *

from src.model.quote import Quote


def parse(name: str) -> ib.Trade:
    path = Path(__file__).resolve().parent
    with open(path / name, 'r') as f:
        raw = f.read()
    data = json.loads(raw)
    data = data['Trade']
    contract = from_dict(ib.Contract, data['contract']['Stock'])
    order = from_dict(ib.Order, data['order']['LimitOrder'])
    order_status = from_dict(ib.OrderStatus, data['orderStatus']['OrderStatus'], config=Config(check_types=False))
    fills = [ib.Fill(contract=from_dict(ib.Contract, fill['contract']['Stock']),
                     execution=from_dict(ib.Execution, fill['execution']['Execution'], config=Config(check_types=False)),
                     commissionReport=from_dict(ib.CommissionReport, fill['commissionReport']['CommissionReport']),
                     time=arrow.get(fill['time']).datetime) for fill in data['fills']]
    return ib.Trade(contract=contract, order=order, orderStatus=order_status, fills=fills, log=[])


def GLTR() -> ib.Trade:
    return parse('GLTR.json')


def SPXL() -> ib.Trade:
    return parse('SPXL.json')


def quotes() -> List[Quote]:
    bids = [83.88 + random.random() for _ in range(20)]
    asks = [bid + 1 + random.random() for bid in bids]
    timestamps = [datetime.now(timezone.utc) + timedelta(minutes=x) for x in range(len(bids))]
    return [Quote(bid=bid, ask=ask, timestamp=timestamp) for bid, ask, timestamp in zip(bids, asks, timestamps)]
