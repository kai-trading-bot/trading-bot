import asyncio
import os
from pathlib import Path
from src.execution.portfolio import Portfolio
from src.execution.signals import Cubby, IBS, Butterfly3X, Factor

signals = [Cubby(), IBS(), Butterfly3X(), Factor()]
notional = int(os.getenv("NOTIONAL", 10000))
retries = int(os.getenv("RETRIES", 3))
port = Portfolio(signals, notional)
DIR = Path.cwd() / "data"
SLEEP = 60


async def backtest(retries: int):
    try:
        # aiohttp.client_exceptions.ClientPayloadError: Response payload is not completed
        # This seems to be environment dependent issue that can occur in docker.
        await port.backtest()
    except Exception as e:
        if retries > 0:
            print(f"Failed to generate return: {e}. Retrying...")
            await asyncio.sleep(SLEEP)
            await backtest(retries - 1)
        else:
            raise e


async def gen():
    await backtest(retries=retries)
    # .csv suffix is ignored in this repo. Use csv2 suffix to bypass the gitignore.
    port.returns.to_csv(DIR / "portfolio_returns.csv2")

if __name__ == "__main__":
    asyncio.run(gen())
