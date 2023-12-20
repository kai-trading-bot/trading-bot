import numpy as np
import pandas as pd
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import seaborn as sns

from matplotlib.ticker import FuncFormatter
from pathlib import Path

from src.analytics.performance import Statistics
from src.constant import *

plt.style.use('seaborn-muted')
PLOT_PATH = Path.home() / 'plots'
PLOT_PATH.mkdir(exist_ok=True)
FIGSIZE = (10, 6)
RETURNS = 'Returns'


class Formatter:

    @staticmethod
    def percentage(x, pos):
        return '%.0f%%' % x


PERCENT_FORMATTER = FuncFormatter(FuncFormatter(Formatter.percentage))


def plot_cumulative_returns(returns: Union[pd.Series, pd.DataFrame],
                            notional: bool = False,
                            title: Optional[str] = None) -> None:
    cr = Statistics.cumulative_returns(returns, notional=notional)
    multiplier = 1 if notional else 100
    title = 'Cumulative Returns ({})'.format('$' if notional else '%') if title is None else title
    cr.mul(multiplier).plot(figsize=FIGSIZE)
    ax = plt.gca()
    if not notional:
        ax.yaxis.set_major_formatter(PERCENT_FORMATTER)
    plt.title(title)
    plt.ylabel(RETURNS)
    plt.tight_layout(pad=1.0)


def plot_cumulative_returns_by_year(returns: Union[pd.Series, pd.DataFrame],
                                    notional: bool = False,
                                    title: Optional[str] = None) -> None:
    years = returns.index.year.unique().tolist()[-9:]
    multiplier = 1 if notional else 100
    data = [Statistics.cumulative_returns(returns[str(year)], notional=notional).mul(multiplier) for year in years]
    fig = plt.figure()
    for i, df in enumerate(data):
        ax = fig.add_subplot(3, 3, i + 1)
        df.plot(figsize=(15, 10), ax=ax)
        ax.set_title(df.index[0].year, fontsize=12)
        ax.legend(fontsize='x-small')
        ax.set_xlabel('')
        ax.set_ylabel(RETURNS)
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
        if not notional:
            ax.yaxis.set_major_formatter(PERCENT_FORMATTER)
    fig.tight_layout(pad=1.0)
    title = 'Cumulative Returns ({}) by Year'.format('$' if notional else '%') if title is None else title
    fig.suptitle(title, y=1.02, fontsize=15)


def plot_cumulative_returns_by_signal(signal_returns: pd.DataFrame,
                                      portfolio_return: pd.Series,
                                      notional: bool = False) -> None:
    """ Compare the percent return by each signal vs percent return by the portfolio. """
    num_cols = 3
    num_rows = int(np.ceil(len(signal_returns.columns) / num_cols))
    multiplier = 1 if notional else 100
    data = [(signal, Statistics.cumulative_returns(signal_returns[signal], notional=notional).mul(multiplier))
            for signal in signal_returns.columns]
    portfolio_data = Statistics.cumulative_returns(portfolio_return, notional=notional).mul(multiplier)
    fig = plt.figure()
    figsize = (15, 8)
    for i, (signal, df) in enumerate(data):
        ax = fig.add_subplot(num_rows, num_cols, i + 1)
        df.plot(figsize=figsize, ax=ax)
        portfolio_data.plot(figsize=figsize, ax=ax)
        ax.set_title(signal, fontsize=12)
        ax.set_xlabel('')
        ax.set_ylabel(RETURNS)
        ax.legend(fontsize='x-small')
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
        if not notional:
            ax.yaxis.set_major_formatter(PERCENT_FORMATTER)
    title = 'Cumulative Returns ({}) by Signal'.format('$' if notional else '%')
    fig.suptitle(title, y=1.02, fontsize=15)
    fig.tight_layout(pad=1.05)


# TODO: maybe try plot_holdings() and then use plt.gcf() and plt.clf()
def plot_holdings(positions: Dict[str, int], prices: Dict[str, int], title: Optional[str] = None) -> None:
    notional = dict()
    for ticker, quantity in positions.items():
        assert ticker in prices and prices[ticker] > 0, \
            f'{ticker} does not have a price or the price is invalid! Please check your data.'
        notional[ticker] = prices[ticker] * quantity
    data = pd.Series(notional)
    data.sort_values(ascending=False).plot.bar(figsize=FIGSIZE)
    plt.xticks(rotation=45)
    title = title if title is not None else 'Current Holdings by Notional Value'
    plt.title(title)


def plot_summary_stats(summary: pd.DataFrame, summary_ref: pd.DataFrame, notional: bool = False) -> None:
    """ Plot the summary stats for the given dataframe and reference

    :param summary: a dataframe returned by method `summarize` in analysis.py
    :param summary_ref: a benchmark summary dataframe
    :param notional: whether the underlying number is calculated based on $ or %
    """
    columns = list(summary.columns)
    fig = plt.figure()
    multiplier = 1 if notional else 100
    for i, column in enumerate(columns):
        ax = fig.add_subplot(2, 2, i + 1)
        df = pd.concat([summary[column], summary_ref[column]], axis=1, keys=['Portfolio', 'Benchmark']).mul(multiplier)
        df.plot.bar(figsize=(12, 10), ax=ax, rot=0)
        if not notional:
            ax.yaxis.set_major_formatter(PERCENT_FORMATTER)
        ax.set_title(column.capitalize(), fontsize=12)
        ax.set_xlabel('')
        ax.legend(fontsize='medium')
    fig.suptitle(f'Summary Stats (as of {datetime.today().date().isoformat()})', y=1.02, fontsize=15)
    fig.tight_layout(pad=1.0)


def save_plot(name: str = 'plot', fig: Optional[plt.Figure] = None, dpi: Optional[int] = 60) -> str:
    """ Save the given figure to a .png file. If fig is None, use the plt.gcf() as default fig.
        Return the complete file path as a string.
    """
    if not name.endswith('.png'):
        name = name + '.png'
    filename = str(PLOT_PATH / name)
    if fig is not None:
        fig.savefig(filename, dpi=dpi)
    else:
        plt.savefig(filename, dpi=dpi)
    plt.clf()
    return filename


def plot_long_short(long: pd.Series, short: pd.Series) -> None:
    fig = plt.figure(figsize=FIGSIZE)
    ax = plt.gca()
    lf = ax.fill_between(long.index, 0, long.values, color='g', alpha=0.5)
    sf = ax.fill_between(short.index, 0, short.values, color='r', alpha=0.5)
    fig.tight_layout(pad=1.0)
    plt.title('Long Short Ticker Count')


def plot_drawdowns(returns: pd.Series, drawdowns: pd.DataFrame) -> None:
    cr = Statistics.cumulative_returns(returns)
    cr.plot(figsize=FIGSIZE)
    ax = plt.gca()
    lim = ax.get_ylim()
    colors = sns.cubehelix_palette(len(drawdowns))[::-1]
    for i, (peak, recovery) in drawdowns[[PEAK, RECOVERY]].iterrows():
        if pd.isnull(recovery):
            recovery = cr.index[-1]
        ax.fill_between((peak, recovery), lim[0], lim[1], alpha=.4, color=colors[i])
    ax.set_ylim(lim)
    ax.set_title('Top Drawdown Periods')
    ax.set_ylabel('Cumulative returns')
    ax.set_xlabel('')


def plot_underwater(underwater: pd.Series) -> None:
    underwater = underwater.mul(100)
    underwater.plot(kind='area', color='coral', alpha=0.7, figsize=FIGSIZE)
    ax = plt.gca()
    y_axis_formatter = FuncFormatter(Formatter.percentage)
    ax.yaxis.set_major_formatter(FuncFormatter(y_axis_formatter))
    plt.ylabel('Drawdown')
    plt.xlabel('')
    plt.title('Underwater Plot')


def plot_correlations(df: pd.DataFrame, col: str) -> None:
    """ Rolling correlations for col with the rest of the dataframe. """
    data = []
    windows = [5, 10, 20, 60, 120]
    palette = sns.color_palette()
    fig = plt.figure()
    for window in windows:
        data.append(df.rolling(window).corr().xs(col, level=1).drop(col, axis=1))
    for i, df in enumerate(data):
        ax = fig.add_subplot(5, 1, i + 1)
        df.plot(figsize=(10, 10), ax=ax, color=palette[i])
        ax.set_title(f'{windows[i]} Days', fontsize=12)
        ax.set_xlabel('')
        plt.xticks(rotation='0')
    fig.suptitle(f'Correlation with {col}', y=1.02, fontsize=15)
    fig.tight_layout(pad=1.0)
