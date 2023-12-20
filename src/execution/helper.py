import ib_insync as ib
import pandas as pd

from ib_insync.util import dataclassAsDict
from collections import defaultdict
from typing import *

from src.execution.utils import to_float

BEFORE = 'Before'
CHANGE = 'Change'


class Helper:
    """ A collection of static helper methods for execution. """

    @staticmethod
    def get_ib_client_id(s: str) -> int:
        """ Generate a random client_id for connecting to IB based on the name. """
        return abs(hash(s)) % (10 ** 3)

    @staticmethod
    def format_ib_order_state(states: Dict[str, ib.OrderState]) -> str:
        """ Format a dictionary of ib.OrderState into human readable information

        :param states: dictionary of key(str) to ib order state
        """
        if not states:
            return 'N/A'
        data = [dataclassAsDict(state) for ticker, state in states.items()]
        df = pd.DataFrame(data)
        attributes = {
            'initMargin': 'Initial Margin', 'maintMargin': 'Maintenance Margin', 'equityWithLoan': 'Equity with Loan'}
        msgs = []
        for key in attributes:
            tmp = df[[key + BEFORE, key + CHANGE]].astype(float).round(2)
            before = tmp[key + BEFORE][0]
            change = tmp[key + CHANGE].sum()
            after = round(before + change, 2)
            msgs.append(f'{attributes[key]}: before={before}, change={change}, after={after}')
        # IB's commission can be derped of having gigantic value.
        min_commission = round(df['minCommission'].where(df['minCommission'] < 1_000_000).sum(), 2)
        max_commission = round(df['maxCommission'].where(df['maxCommission'] < 1_000_000).sum(), 2)
        msgs.append(f'Commission(underestimated): min={min_commission}, max={max_commission}')
        # Add ticker specific info
        msgs.append(f'Ticker specific margin change:')
        for ticker, state in states.items():
            msgs.append(f'- {ticker}: init={to_float(state.initMarginChange)}, '
                        f'maint={to_float(state.maintMarginChange)}, '
                        f'equity={to_float(state.equityWithLoanChange)}')
        return '\n'.join(msgs)
