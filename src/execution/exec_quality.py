import ib_insync as ib
import pandas as pd

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import *

from src.constant import OrderType
from src.model.quote import Quote
from src.utils import catch
from src.utils.file import save_df_plot
from src.utils.time import now


@dataclass
class Record:
    created_at: datetime = datetime.now(timezone.utc)  # Note time here need to be timezone-aware
    updated_at: datetime = datetime.now(timezone.utc)
    quotes: List[Quote] = field(default_factory=list)
    prices: List[float] = field(default_factory=list)
    orders: List[str] = field(default_factory=list)  # A list of order ids.


class ExecutionQuality:
    """ Record and report execution quality """

    def __init__(self) -> None:
        self.quality: Dict[str, Record] = dict()

    def record(self, trade: ib.Trade, quote: Quote) -> None:
        ticker = trade.contract.localSymbol
        record = self.quality.get(ticker, Record())
        record.quotes.append(quote)
        record.prices.append(self.order_price(trade))
        record.updated_at = now()
        self.quality[ticker] = record

    def get(self, trade: ib.Trade) -> Optional[Record]:
        ticker = trade.contract.localSymbol
        return self.quality.get(ticker)

    def order_price(self, trade: ib.Trade) -> Optional[float]:
        if trade.order.orderType == OrderType.LMT:
            return trade.order.lmtPrice
        else:
            # If the trade is a newly created market order
            return trade.orderStatus.avgFillPrice

    @catch('Error generating execution quality')
    def report(self, trades: List[ib.Trade], string: bool = True, worst_first: bool = True) -> Union[pd.DataFrame, str]:
        """ Generate execution report. Return either dataframe or formatted string.

        :param trades: a list of ib trades
        :param string: whether to format the dataframe into printable strings
        :param worst_first: whether to sort the result by worst execution quality (%spread)
        """
        df = pd.DataFrame([self.evaluate(trade) for trade in trades])
        if df.empty:
            return '' if string else df
        df = df[['ticker', 'side', 'type', 'quantity', 'price', 'spread', 'cost', 'duration']]
        if worst_first:
            df = df.sort_values('spread', ascending=False).reset_index(drop=True)
        if string:
            template = '{side} {quantity} {ticker} @ {price} %Spread={spread} Cost={cost} Time={duration}'
            records = df.to_dict('records')
            return '\n'.join([template.format(**record) for record in records])
        else:
            return df

    @catch('Error converting trade to quality dictionary')
    def evaluate(self, trade: ib.Trade) -> Dict:
        """ Evaluate trade execution quality """
        ticker = trade.contract.localSymbol
        side = trade.order.action
        type = trade.order.orderType
        price = self.order_price(trade)
        quantity = trade.order.totalQuantity
        status = trade.orderStatus.status
        quality = self.quality[ticker]
        quote = quality.quotes[-1]
        spread = ExecutionQuality.effective_spread(price, quote)
        cost = ExecutionQuality.txn_cost(trade)
        executed_at = trade.fills[-1].time if trade.fills else None
        duration = round((executed_at - quality.created_at).seconds / 60, 2) if executed_at else None
        return dict(ticker=ticker, side=side, type=type, quantity=quantity, price=price, bid=quote.bid,
                    ask=quote.ask, spread=spread, status=status, cost=cost, duration=duration,
                    created=quality.created_at, executed=executed_at)

    @staticmethod
    def effective_spread(price: float, quote: Quote) -> float:
        return round(2 * (abs(price - quote.mid) / quote.mid) * 100, 2)

    @staticmethod
    def txn_cost(trade: ib.Trade) -> float:
        return round(sum([fill.commissionReport.commission for fill in trade.fills]), 2)

    @catch('Failed to get quotes change')
    def quote_change(self, ticker: str, plot: bool = True) -> Optional[Union[pd.DataFrame, str]]:
        quotes = self.quality[ticker].quotes
        df = pd.DataFrame([[quote.timestamp, quote.bid, quote.ask] for quote in quotes],
                          columns=['timestamp', 'bid', 'ask'])
        df = df.set_index('timestamp')
        df['price'] = self.quality[ticker].prices
        if plot:
            ax = df.plot(title=f'{ticker} Quote Change')
            return save_df_plot(ax)
        return df
