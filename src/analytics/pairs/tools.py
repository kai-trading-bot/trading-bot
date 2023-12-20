import numpy as np
from numpy.linalg import inv
from multiprocessing import Pool, cpu_count
from src.data.data_loader import mp_read
from src.utils.fe import *

__author__ = 'kqureshi'

WINDOW = 20
LEVERAGE = 1
NOTIONAL = 10000

class Data:

    @staticmethod
    def load(symbols: List[str], cols: List[str], index: List[str], asset: str,
             pattern: Optional[str] = None, format_timestamp: bool = True) -> pd.DataFrame:
        """

        """
        pattern = minute_data if not pattern else pattern
        data_list = [tuple([pattern.format(asset, sym), cols + index]) for sym in symbols]
        data = pd.concat((Pool(cpu_count()).map(mp_read, data_list)), axis=0).unstack()
        if format_timestamp:
            data = data.reset_index()
            data.timestamp = pd.to_datetime(data.timestamp.astype(str).str[:-6])
            return data.set_index(TIMESTAMP)
        return data
