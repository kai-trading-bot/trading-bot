import pandas as pd

from typing import *

from IPython.core.display import display, HTML


def display_dfs(dfs: List[pd.DataFrame], captions: Optional[List[str]] = None) -> None:
    output = ""
    captions = captions if captions is not None else [str(i) for i in range(len(dfs))]
    combined = dict(zip(captions, dfs))
    for caption, df in combined.items():
        output += df.style.set_table_attributes("style='display:inline'").set_caption(caption)._repr_html_()
        output += "\xa0\xa0\xa0"
    display(HTML(output))
