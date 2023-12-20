import pandas as pd

from pathlib import Path
from typing import *


class Storage:
    """ A base class for storage. """

    def upload(self, filename: str, key: Union[str, Path]) -> None:
        ...

    def download(self, filename: str, key: Union[str, Path]) -> Any:
        ...

    def delete(self, key: Union[str, Path]) -> Any:
        ...

    def peek(self, prefix: Union[str, Path]) -> List[str]:
        ...

    def write_csv(self, df: pd.DataFrame, filename: str, **kwargs: Any) -> None:
        raise NotImplementedError()

    def read_csv(self, filename: str, **kwargs: Any) -> pd.DataFrame:
        raise NotImplementedError()

    def write_json(self, data: Any, filename: str) -> None:
        ...

    def read_json(self, filename: str) -> Any:
        ...

    def write_parquet(self, df: pd.DataFrame, filename: str, **kwargs: Any) -> None:
        ...

    def read_parquet(self, filename: str, **kwargs: Any) -> pd.DataFrame:
        ...

    def to_parquet(self, df: pd.DataFrame, filename: str, **kwargs: Any) -> None:
        """ Alias to write_parquet """
        self.write_parquet(df, filename, **kwargs)
