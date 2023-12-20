import matplotlib.pyplot as plt
import os

from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

PLOT_PATH = os.getenv('PLOT_PATH', Path.home().joinpath('plots'))
PLOT_PATH.mkdir(exist_ok=True)

def save_df_plot(ax: plt.Axes, name: str = None) -> str:
    """ Save the given ax to a file with given name and return the complete file path.

    :param ax: return value of df.plot()
    :param name: optional name for the plot file
    :return: absolute path for the saved plot file
    """
    name = name or ax.title._text
    filename = str(PLOT_PATH.joinpath(name + '.png'))
    ax.figure.savefig(filename)
    return filename
