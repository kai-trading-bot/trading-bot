import pandas as pd

from src.model.trade import Trade
from src.storage import Storage


class Recorder:
    def __init__(self):
        self.trades = []

    def record_trade(self, trade: Trade) -> None:
        self.trades.append(trade)

    def dump_csv(self, storage: Storage, key: str) -> None:
        df = pd.DataFrame(self.trades)
        storage.write_csv(df, filename=key)
