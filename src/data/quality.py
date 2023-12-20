import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from typing import *


class DataQuality:

    @classmethod
    def is_empty(self, s: pd.Series) -> bool:
        return s.empty

    @classmethod
    def percent_na(self, s: pd.Series, valid_only: bool = True) -> float:
        """ Return the percentage of NA for the series."""
        if valid_only:
            s = self.valid(s)
        if not s.size:
            return 0
        return round(s.isna().sum() / s.size, 3)

    @classmethod
    def na_rows(self, s: pd.Series, valid_only: bool = True) -> pd.Series:
        """ Return the series where the data is NA. """
        if valid_only:
            s = self.valid(s)
        return s[s.isnull()]

    @classmethod
    def valid(self, s: pd.Series) -> pd.Series:
        """ Return the valid section of the given series. Valid section starts from the first non NA row and ends
            with the last non-NA row.
        """
        fst = s.first_valid_index()
        lst = s.last_valid_index()
        return s.loc[fst:lst]

    @classmethod
    def pct_jump(self, s: pd.Series, threshold: float = 1) -> pd.Series:
        pct = s.pct_change()
        return pct[pct > threshold].dropna()

    @classmethod
    def pct_drop(self, s: pd.Series, threshold: float = 0.9) -> pd.Series:
        pct = s.pct_change()
        return pct[pct < -threshold].dropna()

    @classmethod
    def distribution(self, df: pd.DataFrame, field: str = 'mean'):
        """ Return the distribution for the given dataframe with specific field name

        :param df: dataframe with multiple columns with same numeric representation
        :param field: count, mean, std, min, 25%, 50%, 75%, max
        :return:
        """
        return df.describe().T[field].describe()
