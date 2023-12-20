from src.analytics.signal import Signal
from src.analytics.ts import TimeSeries
from src.data.yahoo import Yahoo
from src.storage import GCS
from src.execution.signal import DailySignal
from src.utils.logger import logger

from src.utils.fe import *

NOTIONAL = 100000
START = '2012-01-01'
# These symbols are not shortable on IB
NOT_SHORTABLE = {'NMZ', 'OIA', 'BLE', 'PCQ', 'MGF', 'MUA', 'HEQ', 'MHE', 'GCV', 'NXR', 'BHV', 'NEV', 'BAF', 'NXC',
                 'BQH', 'BFO', 'MIN', 'NOM', 'MYD', 'BKT', 'BTA', 'ECF', 'DUC', 'NBB', 'HPI', 'GUT', 'NUV', 'MFT',
                 'NIM', 'VBF', 'NPV', 'RFI', 'CMU', 'CXE', 'PAI', 'CIF', 'MFV', 'GLU', 'BKK', 'NXQ', 'FGB', 'VCF',
                 'UTF', 'GFY', 'MHF', 'PFD', 'PMM', 'WEA', 'PYN', 'DNP', 'PPT', 'FPF'}


class Closure(DailySignal):

    def __init__(self):
        super().__init__()
        self._tickers = None
        self._basket = None
        self._cef_price = None
        self._basket_price = None

    @property
    def tickers(self) -> List[str]:
        if not self._tickers:
            tickers = GCS().read_csv("data/signals/cef.csv.gz", index_col=0, header=None)[1].tolist()
            self._tickers = list(set(tickers) - NOT_SHORTABLE)
            logger.info(f'Total tickers: {len(tickers)}. Not Shortable: {len(NOT_SHORTABLE)}. '
                        f'Shortable: {len(self._tickers)}')
            self._basket = ["X{}X".format(ticker) for ticker in self._tickers]
        return self._tickers

    async def fetch(self, date: str = None) -> None:
        if not self.prices:
            self.prices = await Yahoo().daily(tickers=self.tickers, start=START)
            null_cols = self.prices.columns[self.prices.isnull().any()]
            self.prices.drop(null_cols, axis=1, inplace=True)
            self._cef_price = self.prices
            ix = self.prices.columns.tolist()
            basket = ["X{}X".format(ticker) for ticker in ix]
            self._basket_price = await Yahoo().daily(tickers=basket, start=START)

    async def update(self, date: str, notional: float = NOTIONAL) -> Dict[str, int]:
        basket_price, cef_price = TimeSeries._align_index([self._basket_price, self.prices])
        cols = list(set([col[1:-1] for col in list(basket_price.columns)]) & set(cef_price.columns))
        basket_price, cef_price = basket_price[["X{}X".format(j) for j in cols]], cef_price[cols]
        spread = pd.DataFrame(np.log(cef_price).values - np.log(basket_price).values, index=cef_price.index,
                              columns=cef_price.columns)
        self.weights = Signal().holdings(signal=spread, pad=False)
        self.positions = -(notional * self.weights).div(cef_price).replace(np.inf, np.nan).fillna(0).round().astype(int)
        return self.positions.loc[date].to_dict()
