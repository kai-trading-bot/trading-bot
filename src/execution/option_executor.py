import ib_insync as ib
import math
import numpy as np
import pandas as pd

from overrides import overrides

from src.constant import *
from src.execution.executor import Executor
from src.execution.utils import dedup, flatten
from src.utils import fmt
from src.utils.time import iso_to_compact, today

DELTA_THRESHOLD = 0.05
DTE_PERCENT = 0.1


class OptionExecutor(Executor):
    """ A base executor for all options """

    async def option_chain(self, contract: ib.Contract) -> pd.DataFrame:
        """ Get the option chain dataframe from the given IB contract. """
        chain = await self.marketdata.get_option_chain(contract)
        self.logger.info(f'{contract.localSymbol} option chain: {chain}')
        data = [[expiry, strike, cp] for expiry in chain.expirations for strike in chain.strikes for cp in (CALL, PUT)]
        return pd.DataFrame(data, columns=[EXP, STRIKE, CP])

    @staticmethod
    def filter_delta(df: pd.DataFrame, delta: float, threshold: float = DELTA_THRESHOLD) -> pd.DataFrame:
        assert DELTA in df.columns, f'Unable to filter delta: Column {DELTA} is not in df.columns {df.columns}'
        df[ABS_DELTA] = np.abs(delta - np.abs(df[DELTA]))
        return df[df[ABS_DELTA] <= threshold].sort_values(ABS_DELTA)

    @staticmethod
    def filter_expiry(df: pd.DataFrame, dte: int, percent_error: float = DTE_PERCENT) -> pd.DataFrame:
        """ Filter an option dataframe by expiry. The percent_error is used to calculate the upper bound
            of absolute difference between target dte and actual dte. For example:
            - dte = 10, percent_error = 0.1, and threshold = 1, which means the actual dte must be in [9, 11]
        """
        assert EXP in df.columns, f'Unable to filter expiry: Column {EXP} is not in df.columns {df.columns}'
        if DTE not in df.columns:
            df[DATE] = today()
            df[EXP] = pd.to_datetime(df[EXP])
            df[DTE] = np.busday_count([d.date() for d in df[DATE]], [d.date() for d in df[EXP]])
        df[ABS_DTE] = abs(dte - df[DTE])
        return df[df[ABS_DTE] <= math.ceil(percent_error * dte)]

    @overrides
    def on_error(self, req_id: int, error_code: int, error_string: str, contract: ib.Contract = None) -> None:
        if error_string.startswith('No security definition has been found'):
            return
        super().on_error(req_id, error_code, error_string, contract)


class OptionSnapshot(OptionExecutor):
    STRIKE_PERCENT: float = 0.2
    MAX_EXP: str = '20210131'
    PATH: str = 'data/option/ib/eod/{ticker}/{date}_{ticker}.csv.gz'
    FUTURES: Tuple[str] = ('ES', 'NQ')

    def __init__(self, tickers: List[str], *args: Any, **kwargs: Any) -> None:
        name = '_'.join(tickers)
        super().__init__(f'option_snapshot', *args, **kwargs)
        self.tickers = tickers

    @overrides
    async def run(self):
        details = dict()
        await self.slack.info(f'Option Snapshot Started', text=f'Tickers: {self.tickers}')
        for ticker in self.tickers:
            self.logger.info(f'Running {ticker}')
            details[ticker] = await self._run(ticker)
        await self.slack.info('Option Marketdata EOD Snapshot Finished', Details='\n'.join(fmt(details)))

    async def _run(self, ticker: str) -> str:
        try:
            if ticker in self.FUTURES:
                df = await self._fetch_fop(ticker)
            else:
                df = await self._fetch(ticker)
            self.storage.write_csv(df, self.PATH.format(date=iso_to_compact(self.date), ticker=ticker))
            self.logger.info(f'Data stored for {ticker}')
            return f'Option data fetched: {df.shape[0]} rows'
        except Exception as e:
            self.logger.error(f'Option snapshot failed for {ticker}: {e}')
            return f'Error: {str(e)}'

    async def _fetch(self, ticker: str) -> pd.DataFrame:
        contract = ib.Stock(ticker, SMART, USD)
        contract = await self.qualify_one(contract)
        quote = await self.marketdata.get_quote(contract)
        chain = await self.marketdata.get_option_chain(contract)
        strike_lower, strike_upper = (1 - self.STRIKE_PERCENT) * quote.mid, (1 + self.STRIKE_PERCENT) * quote.mid
        selected = [
            ib.Option(ticker, exp, strike, cp, SMART)
            for cp in (CALL, PUT)
            for strike in chain.strikes
            for exp in chain.expirations
            if strike_lower <= strike <= strike_upper and exp <= self.MAX_EXP
        ]
        self.logger.info(f'Get {len(selected)} potential option contracts.')
        contracts = await self.qualify_multi(selected)
        quotes = await self.marketdata.get_quotes(contracts)
        self.logger.info(f'Get {len(quotes)} option quotes.')
        df = pd.DataFrame.from_records([quote.to_dict() for quote in quotes])
        if df.empty:
            raise Exception(f'No option contract fetched')
        return df.sort_values([CP, EXP, STRIKE]).reset_index(drop=True)

    async def _fetch_fop(self, ticker: str, expiration: str = '20201218', percent: float = 0.2) -> pd.DataFrame:
        fut = ib.Future(ticker, expiration, GLOBEX)
        contract = await self.qualify_one(fut)
        quote = await self.marketdata.get_quote(contract)
        strike_lower, strike_upper = (1 - percent) * quote.mid, (1 + percent) * quote.mid
        chains = await self.ib.reqSecDefOptParamsAsync(contract.symbol, contract.exchange, contract.secType, contract.conId)
        data = [[chain.tradingClass, expiration, chain.strikes] for chain in chains for expiration in chain.expirations]
        df = pd.DataFrame(data, columns=[TICKER, EXP, STRIKE]).sort_values(EXP)
        expiries = dedup(df[EXP].to_list())
        strikes = dedup(flatten(df[STRIKE].to_list()))
        contracts = [
            ib.FuturesOption(ticker, expiry, strike, cp, GLOBEX)
            for expiry in expiries for strike in strikes for cp in (CALL, PUT)
            if strike_lower <= strike <= strike_upper
        ]
        self.logger.info(f'{ticker} has {len(contracts)} chosen options.')
        contracts = await self.qualify_multi(contracts)
        quotes = await self.marketdata.get_quotes(contracts)
        self.logger.info(f'{ticker} get {len(quotes)} option quotes.')
        df = pd.DataFrame.from_records([quote.to_dict() for quote in quotes])
        if df.empty:
            raise Exception(f'No option contract fetched')
        return df.sort_values([CP, EXP, STRIKE]).reset_index(drop=True)
