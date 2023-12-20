from typing import List
from src.execution.signal import DailySignal

TICKERS = ['PALL', 'SGOL', 'GLTR', 'PPLT', 'SIVR']
HEAD = TICKERS[0]
HEDGE_WINDOW = 20
DATE_BUFFER = 0.2


class Ironman(DailySignal):

    @property
    def tickers(self) -> List[str]:
        return TICKERS

    async def _update(self, notional: float) -> None:
        betas = -self.prices\
            .pct_change().dropna()\
            .rolling(HEDGE_WINDOW).corr()[HEAD].dropna()\
            .unstack().shift().drop(HEAD, axis=1)
        betas[HEAD] = 1
        self.weights = betas.div(betas.abs().sum(axis=1), axis=0).dropna()
        self.positions = self.weights.mul(notional).div(self.prices).round().dropna().astype(int)
