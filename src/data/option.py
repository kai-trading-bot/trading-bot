import numpy as np
import pandas as pd

from src.config import OPTION_DATA_DIR
from src.constant import *
from src.utils.logger import logger


class Option:
    """ Option data loader

        Usage:
        >>> data = Option().daily('SPY', right=CALL)
    """

    def daily(self, ticker: str, right: str = CALL, start: Optional[str] = None) -> pd.DataFrame:
        df = pd.read_parquet(OPTION_DATA_DIR + f'/{ticker}.parquet.gz')
        logger.debug(f'Parquet data loaded for {ticker}')
        if start is not None:
            df = df[df['tradeDate'] >= start]
        return Option._clean(df, right)

    @staticmethod
    def _clean(df: pd.DataFrame, right: str) -> pd.DataFrame:
        df = df.copy().reset_index(drop=True)
        df[TICKER] = df['ticker']
        df[DATE] = pd.to_datetime(df['tradeDate'])
        df[EXP] = pd.to_datetime(df['expirDate'])
        df[STRIKE] = df['strike']
        df[DTE] = np.busday_count([d.date() for d in df[DATE]], [d.date() for d in df[EXP]])
        df[STOCK_PRICE] = df['stockPrice']
        if right == CALL:
            df[PRICE] = df['callValue']
            df[DELTA] = df['delta']
        elif right == PUT:
            df[PRICE] = df['putValue']
            df[DELTA] = df['delta'] - 1
        return df[[DATE, EXP, STRIKE, DTE, DELTA, PRICE, STOCK_PRICE]]
