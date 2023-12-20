import numpy as np
import pandas as pd

from datetime import datetime
from typing import *

from src.analytics.signal import Signal
from src.analytics.ts import TimeSeries
from src.config import CEF_TICKER_PATH
from src.data.fetcher import YahooDataFetcher
from src.storage import GCS
from src.execution.signal import DailySignal
from src.utils.logger import logger
from src.utils.time import MarketCalendar


NOTIONAL = 100000
START = '2012-01-01'
# Outlier
EXCLUDED = {'SRV', 'SRF'}
# These symbols are not shortable on IB
NOT_SHORTABLE = {
    'NMZ', 'OIA', 'BLE', 'PCQ', 'MGF', 'MUA', 'HEQ', 'MHE', 'GCV', 'NXR', 'BHV', 'NEV', 'BAF', 'NXC',
    'BQH', 'BFO', 'MIN', 'NOM', 'MYD', 'BKT', 'BTA', 'ECF', 'DUC', 'NBB', 'HPI', 'GUT', 'NUV', 'MFT',
    'NIM', 'VBF', 'NPV', 'RFI', 'CMU', 'CXE', 'PAI', 'CIF', 'MFV', 'GLU', 'BKK', 'NXQ', 'FGB', 'VCF',
    'UTF', 'GFY', 'MHF', 'PFD', 'PMM', 'WEA', 'PYN', 'DNP', 'PPT', 'FPF',
    # 2020-07-06
    'MZA', 'MQY', 'JLS', 'EIM', 'PPR', 'NMI'
}

CEF_DATA = 'data/signals/closure/cef_close.parquet.gz'
BASKET_DATA = 'data/signals/closure/basket_close.parquet.gz'


class Closure(DailySignal):

    def __init__(self):
        super().__init__()
        self.storage = GCS()
        self._tickers = None
        self._cef_price = None
        self._basket_price = None

    @property
    def tickers(self) -> List[str]:
        if not self._tickers:
            tickers = self.storage.read_csv(CEF_TICKER_PATH).ticker.unique().tolist()
            self._tickers = [ticker for ticker in tickers if ticker not in EXCLUDED]
        return self._tickers

    async def fetch(self, date: str = None) -> None:
        if self.prices is None:
            cef_price = self.storage.read_parquet(CEF_DATA)
            basket_price = self.storage.read_parquet(BASKET_DATA)
            na = cef_price.columns[cef_price.isnull().any()]
            cef_price, basket_price = TimeSeries._align_index([cef_price, basket_price])
            cols = list(set([col[1:-1] for col in list(basket_price.columns)]) & set(cef_price.columns) -
                        set(na) - NOT_SHORTABLE - EXCLUDED)
            basket_price, cef_price = basket_price[["X{}X".format(j) for j in cols]], cef_price[cols]
            logger.info(f'Closure effective number of tickers: {len(cols)}')
            self._basket_price = basket_price
            self._cef_price = cef_price
            if date is not None:
                self._cef_price.loc[pd.Timestamp(date)] = None
                self._basket_price.loc[pd.Timestamp(date)] = None
            self.prices = cef_price

    async def update(self, date: str, notional: float) -> Dict[str, float]:
        """ Closure update generates a list of weights. """
        await super().update(date, notional)
        cef_price, basket_price = self._cef_price.copy(), self._basket_price.copy()
        spread = pd.DataFrame(np.log(cef_price).values - np.log(basket_price).values,
                              index=cef_price.index, columns=cef_price.columns)
        self.weights = Signal.holdings(signal=spread.div(spread.rolling(5).mean(), axis=0), pad=False)
        logger.error(self.weights)
        self.positions = self.weights.mul(notional).div(cef_price).replace(np.inf, np.nan).fillna(0).round().astype(int)
        return self.weights.loc[date].to_dict()

    async def _rebuild(self, name: str, tickers: List[str], data_path: str, check_quality: bool = True) -> None:
        logger.info(f'Rebuilding {data_path} with {len(tickers)} tickers.')
        fetcher = YahooDataFetcher(name=name, tickers=tickers, start=START, check_quality=check_quality)
        try:
            df = fetcher.run()
            info = fetcher.report()
            self.storage.write_parquet(df, data_path, use_pyarrow=True)
        except Exception as e:
            ...

    async def rebuild(self) -> None:
        cef = self.tickers
        basket = ["X{}X".format(ticker) for ticker in cef]
        await self._rebuild("CEF", cef, CEF_DATA)
        await self._rebuild("CEF_BASKET", basket, BASKET_DATA, check_quality=False)
