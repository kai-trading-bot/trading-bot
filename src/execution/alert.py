from dataclasses import dataclass, asdict
from overrides import overrides
from src.analytics.ts import Conditioner
from src.config import TEST_RECIPIENTS
from src.data import Yahoo
from src.execution.plotting import *
from src.utils import Slack, Email, logger
from src.utils.time import today


PRICE = "price"
RANK = "rank"
DATE = "Date"

@dataclass
class Alert:
    symbol: str
    last_price: float
    percentile: float
    threshold: float
    alert: bool
    plot: str

    def to_slack_msg(self) -> str:
        msg = f"{self.symbol}: {self.percentile} (current price: {self.last_price})"
        if self.alert:
            msg = ":alert: " + msg
        return msg


class DipAlert:
    """ Send alerts when price drops below a threshold.

    More specifically, when percentile(1 - (current_price / rolling_max_price)) >= threshold
    The default rolling window is 20 days, and default threshold is 0.75.
    """

    def __init__(self, symbols: List[str], window: int = 20, threshold: float = 0.75) -> None:
        self.symbols = symbols
        self.window = window
        self.threshold = threshold
        self.prices = None

    async def fetch(self):
        if self.prices is None:
            self.prices = await Yahoo().daily(self.symbols)

    @classmethod
    def summary(cls, prices: pd.Series, window: int = 20):
        signal = Conditioner.rolling_max(prices, window).dropna()
        rank = signal.rank(pct=True)
        frame = dict(price=prices, signal=signal, rank=rank)
        return pd.DataFrame(frame)

    @classmethod
    def plot(
        cls,
        df: pd.DataFrame,
        figsize: Optional[Tuple] = (12, 6),
        ax: Optional[plt.Axes] = None,
        title: str = ''
    ) -> plt.Axes:
        """ Plot the time-series and also mark the data points above 1) 75% and 2) 90%.

        :param df: the summary df generated from `self.summary`
        :param figsize: the plot figure size
        :param ax: an optional plot axes
        :param title: an optional title for the plot
        """
        if ax is None:
            ax = df[PRICE].plot(figsize=figsize)
        else:
            df[PRICE].plot(figsize=figsize, ax=ax)
        df[df[RANK] >= 0.75].reset_index().plot.scatter(x=DATE, y=PRICE, marker="o", color="red", ax=ax, label="75%")
        df[df[RANK] >= 0.9].reset_index().plot.scatter(x=DATE, y=PRICE, marker="o", color="indigo", ax=ax, label="90%")
        ax.set_title(title)
        return ax

    @classmethod
    def plot_by_year(
        cls,
        df: pd.DataFrame,
        start_year: int = 2012,
        end_year: int = 2021,
        title: str = ''
    ) -> plt.Figure:
        """ Plot the time-series year by year """
        fig = plt.figure()
        for i, year in enumerate(range(start_year, end_year + 1)):
            ax = fig.add_subplot(6, 2, i + 1)
            sub = df.loc[str(year)]
            cls.plot(sub, figsize=(10, 18), ax=ax, title=year)
        fig.tight_layout(pad=1.0)
        fig.suptitle(title, y=1.02, fontsize=15)
        return fig

    @classmethod
    def plot_trailing(cls,
                      df: pd.DataFrame,
                      lookback: int = 504,
                      figsize: Tuple = (12, 6),
                      title: str = "") -> plt.Figure:
        """ Plot the time-series from the last data point minus the lookback period (days). """
        return cls.plot(df.iloc[-lookback:], figsize=figsize, title=title).get_figure()

    def display_today(self, symbol: str, window: int = 20, historical: bool = False):
        from IPython.core.display import display, HTML
        df = self.summary(self.prices[symbol], window)
        data = df.iloc[-1]
        self.plot_trailing(df, title=symbol)
        if historical:
            self.plot_by_year(df, title=f"{symbol} Historical")
        display(HTML(f'<center><H3>{symbol} Percentile Today: {round(data[RANK], 4)}</H3><center>'))
        plt.show()

    def update_one(self, symbol: str, window: int = 20, threshold: float = 0.75) -> Alert:
        df = self.summary(self.prices[symbol], window)
        data = df.iloc[-1]
        fig = self.plot_trailing(df, title=symbol)
        filename = save_plot(f'dip_alert_{symbol}_{today()}.png', fig=fig)
        alert = Alert(
            symbol=symbol,
            last_price=round(data["price"], 2),
            percentile=round(data["rank"], 4),
            threshold=threshold,
            alert=data["rank"] >= threshold,
            plot=filename,
        )
        return alert

    async def run(self):
        logger.info(f'Running dip alert for {self.symbols}')
        await self.fetch()
        alerts = [self.update_one(symbol, self.window, self.threshold) for symbol in self.symbols]
        slack = Slack(channel="#alert", icon_emoji=":stockrocket:")
        details = [alert.to_slack_msg() for alert in alerts]
        response = await slack.send(
            "Buy the Dip", window=self.window, threshold=self.threshold, alerts="\n".join(details))
        thread_ts = response["ts"]
        if thread_ts is not None:
            for alert in alerts:
                if alert.alert:
                    await slack.send_image(alert.plot, thread_ts=thread_ts)
        logger.info('Dip alert finished')


class DailyPriceAlert(DipAlert):
    TECH = ["XLK", "QQQ", "AAPL", "MSFT", "GOOG", "AMZN", "FB", "CRM", "NFLX", "NVDA"]
    FINANCIAL = ["XLF", "CME", "SPGI"]
    GLOBAL = ["DOW", "SPY", "USMV", "VOO"]

    def __init__(self):
        super().__init__(self.TECH + self.FINANCIAL + self.GLOBAL)

    @overrides
    async def run(self):
        email = Email(f"{today()} Price Alert", recipients=TEST_RECIPIENTS)
        await self.fetch()
        alerts = [self.update_one(symbol, self.window, self.threshold) for symbol in self.symbols]
        df = pd.DataFrame([asdict(alert) for alert in alerts]) \
            .sort_values(by="percentile", ascending=False) \
            .reset_index(drop=True)[["symbol", "percentile", "last_price"]]
        email.add_dataframe(df, "Rank")
        for alert in alerts:
            email.add_image(alert.plot, f"{alert.symbol}")
        email.send()
