import pandas as pd

from aioinflux import InfluxDBClient
from typing import Dict, Optional, Union

from src.config import INFLUXDB_HOST, INFLUXDB_PORT
from src.utils.logger import logger


class InfluxDB:

    def __init__(self, db: str = 'default', host: str = INFLUXDB_HOST, port: int = INFLUXDB_PORT):
        self.db = db
        self.client = InfluxDBClient(host=host, port=port, db=db, output='dataframe')

    async def connect(self):
        await self.client.create_session()
        logger.info(f'InfluxDB connected. DB: {self.db}')

    async def disconnect(self):
        await self.client.close()
        logger.info('InfluxDB disconnected')

    async def write(self, point: Dict):
        try:
            await self.client.write(point)
        except Exception as e:
            logger.error(f'Write error: {e}')

    async def query(self, query: str) -> Optional[Union[pd.DataFrame, Dict]]:
        logger.debug(f'Sending query: {query}')
        try:
            return await self.client.query(query)
        except Exception as e:
            logger.error(f'Query error: {e}')
