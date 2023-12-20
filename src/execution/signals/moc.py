import pandas as pd
from tenacity import retry, wait_fixed, stop_after_attempt, retry_if_exception_type
from typing import *
from urllib.error import HTTPError

from src.execution.exceptions import SignalException
from src.execution.signal import DailySignal
from src.storage import GCS
from src.utils.fe import TODAY, DATE_BUFFER, BDay, DATE, TICKER
from src.utils.logger import logger


URL = 'https://tsx.com/files/trading/tsx-moc-imbalance.csv'
COLS = ['ticker', 'sign', 'price', 'volume']
MAPPER = {'BUY': 1, 'SELL': -1}
WINDOW = 20
NOTIONAL = 100000
START = pd.to_datetime(TODAY) - (int(1 + DATE_BUFFER) * BDay(WINDOW))
DATE_FORMAT = '%Y%m%d'
HISTORY = 'alt/moc/tsx/{}.csv.gz'


class MOC(DailySignal):
    _imbalance: pd.DataFrame = None

    def __init__(self):
        super().__init__()
        self.storage = GCS()

    @property
    def tickers(self) -> List[str]:
        return []

    @property
    def backtestable(self) -> bool:
        return False

    @retry(reraise=True, wait=wait_fixed(60), stop=stop_after_attempt(3), retry=retry_if_exception_type(HTTPError))
    async def _fetch(self) -> None:
        # Sometimes this might hit 404 error. Retry if necessary.
        df = pd.read_table(URL, sep=',')
        df = df.drop('MKT_CODE', axis=1)
        df = df.rename(columns={
            'DATE': DATE,
            'SYMBOL': TICKER,
            'IMBALANCE_SIDE': 'sign',
            'IMBALANCE_VOLUME': 'volume',
            'IMBALANCE_REFERENCE_PRICE': 'price',
        })
        if df.empty:
            raise SignalException(f'No data fetched for {TODAY}')
        logger.info(f'Uploading imbalance info')
        self.storage.write_csv(df, HISTORY.format(pd.datetime.today().strftime(DATE_FORMAT)))
        self._imbalance = df

    async def _update(self, notional: float) -> None:
        ...
