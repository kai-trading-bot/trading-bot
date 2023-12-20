import json
import pandas as pd

from pathlib import Path
from typing import *

from src.config import DATA_DIR
from src.storage.base import Storage
from src.utils.logger import logger


class LocalStorage(Storage):

    def __init__(self, data_dir: str = DATA_DIR) -> None:
        Path(data_dir).mkdir(parents=True, exist_ok=True)
        self.data_dir = Path(data_dir)

    def write_json(self, data: Any, filename: str) -> None:
        with open(self.data_dir / filename, 'w+') as f:
            json.dump(data, f, default=str)

    def read_json(self, filename: str) -> Any:
        with open(self.data_dir / filename, 'r+') as f:
            return json.load(f)

    def write_csv(self, df: pd.DataFrame, filename: str, **kwargs: Any) -> None:
        path = self.data_dir / filename
        logger.debug(f"Writing csv to {path}")
        df.to_csv(path, **kwargs)

    def read_csv(self, filename: str, **kwargs: Any) -> pd.DataFrame:
        return pd.read_csv(self.data_dir / filename, **kwargs)
