import asyncio
import click
import numpy as np
import pandas as pd
import requests
import os
import sys
import time

from dotenv import load_dotenv
from mode import Service
from pathlib import Path
from src.storage import GCS
from typing import *

from src.utils.logger import logger, get_logger
from src.utils.slack import Slack
from src.utils.time import stopwatch

load_dotenv()
API_TOKEN = os.getenv("ORATS_API_TOKEN", "")

# https://docs.orats.io/data-api-guide/data.html#strikes-history
URL = "https://api.orats.io/data/hist/strikes?tickers={}&tradeDate={}"
HEADERS = {"Authorization": API_TOKEN}

ETFS = ['SPY', 'SPXL', 'TQQQ', 'UDOW', 'TMF', 'VXX', 'SPXS', 'SDOW',
        'SQQQ', 'SOXL', 'SOXS', 'UVXY', 'SPXU', 'UPRO', 'QQQ']  # QQQ might not be complete

# ETF2 = ['DIA', 'TLT', 'TMF', 'MDY', 'IWM', 'QQQ', 'USO', 'USMV']

ETF3 = ['EEM', 'IWM', 'HYG', 'EFA', 'FXI', 'GDX', 'EWZ', 'GLD', 'XOP', 'SLV', 'XLF', 'USO', 'TLT',
        'XLE', 'KRE', 'SMH', 'OIH', 'XLU', 'MDY', 'TNA', 'QQQ', 'DIA']


class Orats(Service):

    # EMPTY_DATA is used to keep track of symbols that do not have option to speed up fetching process.
    EMPTY_DATA: Set = set()

    def __init__(self, tickers: List[str] = None, batch_size: int = 500, max_rows: int = 10000):
        super().__init__()
        self.storage = GCS()
        self.path = 'data/option/orats/daily/{ticker}/{dt}_{ticker}.parquet.gz'
        self.logger = get_logger('orats')
        self._tickers = tickers
        self.batch_size = batch_size
        self.max_rows = max_rows
        self.slack = Slack("#report")

    @property
    def tickers(self):
        """
        Available types:
        ['Equity', 'Bond', 'Commodity', 'Real Estate', 'Preferred Stock',
           'Multi-Asset', 'Volatility', 'Alternatives', 'Currency']
        """
        if not self._tickers:
            types = ('Equity', 'Bond', 'Commodity', 'Volatility')
            df = self.storage.read_csv('data/etf.csv.gz')
            df = df[df.type.isin(types)]
            tickers = df.ticker.tolist()
            tickers = [ticker for ticker in tickers if ticker not in ETFS + ETF3]
            start = 340
            self._tickers = tickers[start:start + 10 * self.batch_size]  # TODO: change me in next run
            self.logger.info(f'Tickers: {self._tickers}')
        return self._tickers

    async def run(self, date: str, start_ticker: str = None) -> None:
        self.logger.warning(f'Current empty data: {self.EMPTY_DATA}')
        try:
            tickers = self.tickers
            tickers = [ticker for ticker in tickers if ticker not in self.EMPTY_DATA]
            if start_ticker is not None:
                self.logger.info(f'Starting with {start_ticker}')
                start_idx = tickers.index(start_ticker)
                tickers = tickers[start_idx:]
            self.logger.info(f'Starting to run {date} with {len(tickers)} tickers.')
            with stopwatch(f'{date} and {len(tickers)} tickers'):
                self.smart_fetch(tickers, date)
        except Exception as e:
            self.logger.exception(e)
            await self.slack.send_text(f'Orats query failed on {date}: {e}. Please take a look')

    def smart_fetch(self, tickers: List[str], date: str):
        i = 0
        while i < len(tickers):
            j = i + self.batch_size
            chunk = tickers[i:j]
            df = self.fetch(chunk, date)
            if df.shape[0] >= self.max_rows:
                self.logger.warning(f'Data batch for {date} and {chunk} might not be complete: {df.shape[0]}. '
                                    f'i={i}, j={j}, tickers={len(tickers)}')
                # TODO: this is not the best way to reuse query.
                last_ticker = df.iloc[-1].ticker
                i = tickers.index(last_ticker)
            else:
                i = j
            if df.shape[0] > 0:
                self.store(chunk, date, df)
            else:
                self.logger.warning(f'Fetched no data for {date} and {chunk}. Skip')
            self.logger.info(f'Batch finished for {date} {chunk} ({tickers.index(chunk[-1])}/{len(tickers)})')
        self.logger.info(f'Smart fetch completed for {date} and {len(tickers)} tickers')

    def fetch(self, tickers: List[str], date: str) -> pd.DataFrame:
        try:
            self.logger.info(f'Fetching {date} for {tickers}')
            url = URL.format(",".join(tickers), date)
            response = requests.get(url, headers=HEADERS)
            response.raise_for_status()
            data = response.json()
            if data.get('data') is None:
                self.logger.error(f'Data fetched is None: {data}')
                raise requests.RequestException
            df = pd.DataFrame(data['data'])
            # Error: pyarrow.lib.ArrowInvalid: ('PyLong is too large to fit int64')
            cols = list({'extCallValue', 'phi', 'gamma', 'theta', 'vega', 'theta',
                         'driftlessTheta'}.intersection(set(df.columns)))
            df[df[cols] > sys.maxsize] = np.nan
            self.logger.info(f'Fetched data {df.shape}')
            return df
        except requests.RequestException as e:
            self.logger.error(f'Fetch failed: {e}')
            time.sleep(30)
            return self.fetch(tickers, date)

    def store(self, tickers: List[str], date: str, df: pd.DataFrame) -> None:
        for ticker in tickers:
            ticker_df = df[df.ticker == ticker]
            self.logger.info(f'Uploading {ticker} with data {ticker_df.shape}')
            if ticker_df.shape[0] == 0:
                self.logger.error(f'Empty data: {date} {ticker}')
                self.EMPTY_DATA.add(ticker)
                continue
            path = self.path.format(dt=date.replace("-", ""), ticker=ticker)
            try:
                self.storage.write_parquet(ticker_df, path)
            except Exception as e:
                self.logger.error(f'Write parquet failed {ticker}: {e}')
        self.logger.info(f'Finished store for {date} {tickers}')

    def load(self, ticker: str, date: str = None) -> pd.DataFrame:
        path = self.path.format(dt=date.replace("-", ""), ticker=ticker)
        df = self.storage.read_parquet(path)
        return df

    def rebuild(self, ticker: str, output_dir: Optional[str] = None) -> None:
        """ Rebuild all the available data for ticker.
            TODO: add the ability to rebuild by years if data is too large.
            gsutil cp <path> gs://kai-trading-bot/<path>
        """
        prefix = f"data/option/orats/daily/{ticker}"
        output = f'data/option/orats/daily/rebuild/{ticker}.parquet.gz'
        files = self.storage.peek(prefix)
        dfs = []
        self.logger.info(f'Loading {len(files)} daily parquet files.')
        start_time = time.time()
        for i, file in enumerate(files):
            if i % 100 == 0:
                self.logger.info(f'Fetching {i}. Total {len(files)}.')
            df = self.storage.read_parquet(file)
            dfs.append(df)
        df = pd.concat(dfs)
        duration = round((time.time() - start_time) / 60, 3)
        filename = f'{ticker}.parquet.gz'
        output = Path(output_dir).joinpath(filename) if output_dir is not None else str(Path.home().joinpath(filename))
        self.logger.info(f'Rebuild dataframe for {ticker}: {df.shape}. Took {duration} minutes. Writing to {output}')
        df.to_parquet(output)
        # TODO: google has a bug currently to upload big dataframe directly to google cloud
        # self.storage.write_parquet(ret, output, use_pyarrow=True)

def verify():
    self = Orats()
    ticker = 'UDOW'
    date = '2020-06-19'
    df = self.load(ticker, date)


async def test():
    self = Orats(ETFS, batch_size=len(ETFS), max_rows=100000)
    date = '2017-07-17'
    await self.run(date)


@click.command()
@click.option("--start", default='2012-01-01', help="start date")
@click.option("--end", default='2017-12-31', help="end date")
@click.option("--selective", is_flag=True)
def run(start: str, end: str, selective: bool):
    dates = pd.bdate_range(start, end)
    dates = sorted([date.date().isoformat() for date in dates], reverse=True)
    loop = asyncio.get_event_loop()
    if selective:
        self = Orats(ETF3)
    else:
        self = Orats()
    logger.info(f'Date start {start}, end {end}, {len(dates)} days.')
    for date in dates:
        loop.run_until_complete(self.run(date))


if __name__ == "__main__":
    run()
