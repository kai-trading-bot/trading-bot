import asyncio
import pandas as pd

from datetime import datetime
from mode import Service
from overrides import overrides
from typing import Dict, List, Optional

from src.broker.alpaca import Alpaca
from src.config import INFLUXDB_DB_ALPACA, MINUTE_BARS
from src.constant import OrderType
from src.intraday.signals import reversal
from src.storage.influxdb import InfluxDB
from src.utils.logger import get_logger
from src.utils.slack import Slack
from src.utils.time import now

BUFFER = 0.5
REPORT_CHANNEL = '#intraday-report'


class DayTrader(Service):

    def __init__(self, name: str = 'dummy'):
        super().__init__()
        self.name = name
        self.influxdb = InfluxDB(INFLUXDB_DB_ALPACA)
        self.alpaca = Alpaca()
        self.slack = Slack(REPORT_CHANNEL)
        self.logger = get_logger(f'trader_{name}')

    async def on_start(self) -> None:
        await self.influxdb.connect()
        await self.alpaca.connect()

    async def on_stop(self) -> None:
        await self.influxdb.disconnect()
        await self.alpaca.disconnect()

    @Service.timer(60)
    async def heartbeat(self) -> None:
        self.logger.info(f'Heartbeat')

    @Service.task
    async def _on_minute(self, wake_up_second: int = 4):
        while not self.should_stop:
            # TODO: stop when market is closed
            ts = now()
            if ts.second != wake_up_second:
                seconds = (60 + wake_up_second - ts.second) % 60
                await asyncio.sleep(seconds)
            await asyncio.sleep(BUFFER)
            self.logger.info(f'Wake up: {now()}')
            await self.on_minute()
            await asyncio.sleep(BUFFER)

    async def on_minute(self):
        ...


class DummyTrader(DayTrader):

    async def on_minute(self):
        self.logger.info('Do Nothing!')


class ReversalTrader(DayTrader):

    def __init__(self, symbols: List[str], window: int = 20):
        super().__init__('reversal')
        self.symbols = symbols
        self.window = window

    async def get_data(self, window: int, start: Optional[datetime] = None):
        where_clause = " OR ".join(f"symbol = '{symbol}'" for symbol in self.symbols)
        query = f"""
        SELECT close FROM {MINUTE_BARS} WHERE ({where_clause})
        AND time >= now() - {window + 1}m
        GROUP BY symbol
        """
        self.logger.info(query)
        # InfluxDB returns a dictionary for query with group by.
        data = await self.influxdb.query(query)
        self.logger.info(data)
        return data

    def transform(self, data) -> pd.DataFrame:
        dfs = []
        symbols = []
        for key, df in data.items():
            if not df.empty:
                symbol = key.split(',')[1].split('=')[1]
                symbols.append(symbol)
                dfs.append(df['close'])
        return pd.concat(dfs, keys=symbols, axis=1)

    @overrides
    async def on_minute(self):
        data = await self.get_data(self.window)
        if not data:
            self.logger.warning(f'No data fetched from InfluxDB')
            return
        prices = self.transform(data)
        positions = reversal(prices)
        self.logger.info(positions)
        # TODO: ues timestamp instead of -1
        target = positions.iloc[-1].to_dict()
        self.logger.info(f'Target: {target}')
        tasks = [self.alpaca.rebalance(symbol, target_qty, OrderType.MARKET) for symbol, target_qty in target.items()]
        orders = await asyncio.gather(*tasks)
        await self.slack.info(f'Reversal Signal: Submitted {len(orders)} orders', Target=target, Timestamp=now())


if __name__ == '__main__':
    from mode import Worker
    trader = ReversalTrader(['SPY', 'QQQ', 'DIA', 'IWM', 'MDY'])
    Worker(trader, quiet=True, loglevel='INFO').execute_from_commandline()
