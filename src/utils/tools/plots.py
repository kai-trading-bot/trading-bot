import plotly.graph_objs as go
import warnings
from plotly.offline import init_notebook_mode, iplot
from src.utils.fe import *
warnings.filterwarnings("ignore")


__author__ = 'kq'

init_notebook_mode(connected=True)


def interactive_plot(data: pd.DataFrame, title: str, yaxis: str, xaxis: str = 'date', mode: str = 'lines') -> None:
    """
    Usage:
    interactive_plot(data=data, title='Attribution', yaxis='%')
    """
    data = [go.Line(x=data.index, y=data[data.columns[j]].values, mode=mode, name=data.columns[j].split('_')[1]) for j
            in range(len(data.columns))]
    logger.info('Plotting data...')
    layout = go.Layout(title=title, xaxis=dict(title=xaxis), yaxis=dict(title=yaxis))
    fig = go.Figure(data=data, layout=layout)
    iplot(fig)
