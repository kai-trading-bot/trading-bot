import click
import pandas as pd
import pandas_datareader.data as web
import time
import textwrap
import traceback

from datetime import datetime
from typing import *

from src.config import *
from src.data import Yahoo, DataQuality
from src.storage import GCS
from src.utils.batch import in_chunks
from src.utils.logger import logger
from src.utils.slack import Slack
from src.utils.time import PST


START = '2007-01-02'

class YahooDataFetcher:
    """ A data fetcher for fetching large amount of tickers reliably at once
        and return the merged dataframe.
    """

    def __init__(self,
                 name: str,
                 tickers: List[str],
                 start: str = START,
                 field: str = Yahoo.ADJ_CLOSE,
                 sleep: int = 60,
                 channel: str = "#test",
                 check_quality: bool = True) -> None:
        self.name = name
        self.tickers = tickers
        self.start = start
        self.field = field
        self.df = pd.DataFrame()
        self.sleep = sleep
        self.slack = Slack(channel, run_async=False)
        self.passed = []
        self.failed = dict()
        self.date = datetime.now(PST)
        self.duration = None
        self.check_quality = check_quality
        self._chunk_size = 20

    def run(self) -> pd.DataFrame:
        start_time = time.time()
        for chunk in in_chunks(self.tickers, self._chunk_size):
            self._fetch(chunk)
        # Re-fetch failed tickers one more time
        failed = list(self.failed.keys())
        self.failed = dict()
        logger.info(f'Re-fetching failed tickers: {len(failed)}')
        for chunk in in_chunks(failed, self._chunk_size):
            self._fetch(chunk)
        self.duration = round((time.time() - start_time) / 60, 3)
        return self.df

    def _fetch(self, chunk: List[str]):
        logger.info(f'Fetching {chunk}. passed={len(self.passed)} failed={len(self.failed)} total={len(self.tickers)}')
        data = web.DataReader(chunk, 'yahoo', self.start)
        data = data[self.field]
        passed = self.check(data, chunk)
        self.df = pd.concat([self.df, data[passed]], axis=1)
        self.passed += passed
        time.sleep(self.sleep)

    def check(self, df: pd.DataFrame, tickers: List[str]) -> List[str]:
        if not self.check_quality:
            return tickers
        passed = []
        for ticker in tickers:
            if ticker not in df:
                self.failed[ticker] = f'Ticker not present in df.'
                continue
            data = df[ticker]
            # Check 1: Percent NA.
            percent_na = DataQuality.percent_na(data, valid_only=False)
            if percent_na > 0.9:
                self.failed[ticker] = f'Percent NA too large: {percent_na}'
                continue
            # Check 2: No NA should be present in between dates with price data.
            na = DataQuality.na_rows(data, valid_only=True)
            if not na.empty:
                self.failed[ticker] = f'NA value between data points:\n{na.to_string(header=False)}'
                continue
            # Check 3: big percent jump
            jump = DataQuality.pct_jump(data)
            if not jump.empty:
                self.failed[ticker] = f'Big pct jump in data\n{jump.to_string(header=False)}'
                continue
            # Check 4: big percent drop
            drop = DataQuality.pct_drop(data)
            if not drop.empty:
                self.failed[ticker] = f'Big pct drop in data\n{drop.to_string(header=False)}'
                continue
            passed.append(ticker)
        return passed

    def report(self) -> str:
        failed_reason = '\n'.join(["- {}: {}".format(ticker, error) for ticker, error in self.failed.items()][:10])
        try:
            info = textwrap.dedent(f"""
            {self.name} Data Fetcher Report:
            Start Date: {self.date.isoformat()}
            Duration: {self.duration} minutes
            DF Shape: {self.df.shape}
            Quality Check: {self.check_quality}
            Passed: {len(self.passed)} ({round(len(self.passed) / len(self.tickers) * 100, 3)}%)
            Failed: {len(self.failed)} ({round(len(self.failed) / len(self.tickers) * 100, 3)}%)
            First Date: {self.df.index[0]}
            Last Date: {self.df.index[-1]}
            Last Row NaN Columns: {list(self.df.iloc[-1][self.df.iloc[-1].isnull()].to_dict())}
            Failed Reason (First 10):\n{failed_reason}
            """)
            return info
        except Exception as e:
            logger.exception(e)
            return ''


class LongRunningDataFetcher:
    """ A data fetcher used for fetching large list of individual tickers. """

    PATH = "data/{asset}/daily/{ticker}.parquet.gz"

    def __init__(self,
                 asset: str = 'etf',
                 ticker_path: str = ETF_TICKER_PATH,
                 start: str = START,
                 rebuild: str = None,
                 channel="#test"):
        self.asset = asset
        self.start = start
        self.batch_size = 10
        self.ticker_path = ticker_path
        self.storage = GCS()
        self.sleep_interval = 60
        self.passed = dict()
        self.failed = dict()
        self.slack = Slack(channel, run_async=False)
        self.date = datetime.today()
        self.start_time = None
        self._tickers = None
        self.rebuild = rebuild
        self.close = pd.DataFrame()

    @property
    def tickers(self) -> List[str]:
        if self._tickers is None:
            tickers = self.storage.read_csv(self.ticker_path).ticker.unique().tolist()
            tickers = [self.format_ticker(ticker) for ticker in tickers]
            self._tickers = tickers
        return self._tickers

    def format_ticker(self, ticker: str) -> str:
        return ticker

    def run(self) -> None:
        self.start_time = time.time()
        try:
            for chunk in in_chunks(self.tickers, self.batch_size):
                logger.info(f'Fetching {chunk}. passed={len(self.passed)} failed={len(self.failed)} total={len(self.tickers)}')
                data = web.DataReader(chunk, 'yahoo', self.start)
                data.columns = data.columns.swaplevel()  # (Adj Close, SPY) -> (SPY, Adj Close)
                data = data.sort_index(axis=1, level=0)
                passed, failed = self.check(data, chunk)
                for ticker in passed:
                    df = data[ticker]
                    # Convert back to multi index: [Open, High, Low, ..] -> [(SPY, Open), ..]
                    df.columns = pd.MultiIndex.from_product([[ticker, ], df.columns])
                    self.storage.write_parquet(df, self.PATH.format(asset=self.asset, ticker=ticker), use_pyarrow=True)
                time.sleep(self.sleep_interval)
        except Exception as e:
            logger.exception(e)
            self.slack.text(f'Long running data fetcher error\n{traceback.format_exc()}')

    def check(self, df: pd.DataFrame, tickers: List[str]) -> Tuple[List[str], List[str]]:
        passed = []
        failed = []
        for ticker in tickers:
            if ticker not in df:
                failed.append(ticker)
                self.log_failure(ticker, "No data fetched from Yahoo")
                continue
            data = df[ticker]
            # NA rows in between dataframe
            na = DataQuality.na_rows(data, valid_only=True)
            if not na.empty:
                self.log_failure(ticker, f'NA value between data points:\n{na}')
                failed.append(ticker)
                continue
            # Large price jump or drop
            jump, drop = DataQuality.pct_jump(data), DataQuality.pct_drop(data)
            if not jump.empty or drop.empty:
                self.log_failure(ticker, f'Big pct jump or dump in data\n{jump}\n{drop}')
                failed.append(ticker)
                continue
            passed.append(ticker)
            self.passed[ticker] = df.shape
        return passed, failed

    def report(self):
        failed_reason = '\n'.join(["{}: {}".format(ticker, error) for ticker, error in self.failed.items()])
        msg = f"""\
        {self.asset.upper()} Data Fetcher Report:
        Start Time: {self.date}
        Total Duration: {round((time.time() - self.start_time) / 3600, 2)} hours
        Total Tickers: {len(self.tickers)}
        Succeeded: {len(self.passed)}
        Failed: {len(self.failed)}
        Reason Failed: \n{failed_reason}
        """
        self.slack.text(textwrap.dedent(msg))

    def log_failure(self, ticker, msg: str):
        logger.error(f"{ticker}: {msg}")
        self.failed[ticker] = msg


@click.command()
@click.option('--asset', default='etf')
@click.option('--ticker_path', default=ETF_TICKER_PATH)
@click.option('--start', default=START)
@click.option('--channel', default='#test')
def main(asset: str, ticker_path: str, start: str, channel: str):
    assert asset in ('etf', 'cef'), f'Invalid asset {asset}'
    assert ticker_path in (ETF_TICKER_PATH, CEF_TICKER_PATH), f'Invalid ticker path {ticker_path}'
    fetcher = LongRunningDataFetcher(asset=asset, ticker_path=ticker_path, start=start, channel=channel)
    fetcher.run()
    fetcher.report()


if __name__ == "__main__":
    main()
