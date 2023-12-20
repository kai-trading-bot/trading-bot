from rich import print
from src.execution.exec_quality import ExecutionQuality
from src.utils import fmt
from src.utils.logger import logger

from tests.execution.common import *


def test_report():
    quality = ExecutionQuality()
    trade = GLTR()
    for quote in quotes():
        quality.record(trade, quote)
    df = quality.report([trade], string = False)
    string = quality.report([trade], string=True)
    print(fmt(df))
    assert df.shape[0] == 1
