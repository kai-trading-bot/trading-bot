import numpy as np
import pandas as pd

from typing import Dict, List

from src.analytics.signal import Signal
from src.analytics.ts import TimeSeries
from src.data import Yahoo
from src.execution.signal import DailySignal
from src.utils.logger import logger

HALFLIFE: int = 252
PAIRS = {
    'SPXL': [3, 'SPY'],
    'SH': [-1, 'SPY'],
    'SSO': [2, 'SPY'],
    'SDS': [-2, 'SPY'],
    'SPXU': [-3, 'SPY'],
    'UPRO': [3, 'SPY'],
    'SPXS': [-3, 'SPY'],
}


# TODO: this signal doesn't work. Position contains lots of NaN value.
class Levarb(DailySignal):

    @property
    def tickers(self) -> List[str]:
        return list(set(list(PAIRS.keys()) + [ele[1] for ele in list(PAIRS.values())]))

    async def update(self, date: str, notional: float) -> Dict[str, float]:
        returns = self.prices.pct_change()
        signal_dict = {}
        for pair in PAIRS.keys():
            try:
                leverage, child = PAIRS[pair]
                parent = returns[pair]
                parent, child = TimeSeries._align_index([parent, returns[child]])
                spread = parent - (leverage * child)
                signal_dict[pair] = spread
            except KeyError as e:
                logger.error(f"Failed to compute for pair {pair}: {e}")
                pass
        signal = pd.DataFrame(signal_dict).dropna()
        # TODO: fix division by zero error
        signal = signal.div(
            pd.concat([TimeSeries.ewma(abs(signal[col]), halflife=HALFLIFE) for col in signal.columns], axis=1,
                      keys=signal.columns))
        self.weights = Signal().holdings(signal=signal, pad=False)
        self.prices = self.prices.drop('SPY', axis=1)
        # TODO: due to division by zero error the positions can have NaN value.
        self.positions = self.weights.mul(notional).div(self.prices).dropna().round().astype(int)
        return self.positions.loc[date].to_dict()
