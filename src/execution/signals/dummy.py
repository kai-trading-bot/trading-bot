from typing import List, Dict

from src.execution.signal import DailySignal


class Dummy(DailySignal):
    def __init__(self, symbol: str = 'BILI', quantity: int = 1) -> None:
        super().__init__()
        self.symbol = symbol
        self.quantity = quantity

    @property
    def backtestable(self) -> bool:
        return False

    @property
    def tickers(self) -> List[str]:
        return [self.symbol]

    async def update(self, date: str, notional: float) -> Dict[str, int]:
        return {self.symbol: self.quantity}
