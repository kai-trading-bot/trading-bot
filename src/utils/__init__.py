import asyncio
import inspect
import pandas as pd
import time

from functools import wraps
from mode.utils.objects import shortlabel
from tabulate import tabulate
from typing import *

from .email import Email
from .opsgenie import OpsGenie
from .logger import logger, get_logger
from .slack import Slack


def timeit(func):
    async def process(func, *args, **kwargs):
        if asyncio.iscoroutinefunction(func):
            return await func(*args, **kwargs)
        else:
            return func(*args, **kwargs)

    async def helper(*args, **params):
        start = time.time()
        result = await process(func, *args, **params)
        print(f">>> {func.__name__} {args}: {round(time.time() - start, 3)} seconds")
        return result

    return helper


def batch(batch_size: int = 10):
    def decorator(func):
        async def wrapper(self, items, *args, **kwargs) -> pd.DataFrame:
            results = []
            for i in range(0, len(items), batch_size):
                chunk = items[i:i+batch_size]
                result = await func(self, chunk, *args, **kwargs)
                results.append(result)
            return pd.concat(results).drop_duplicates().reset_index(drop=True)
        return wrapper
    return decorator


def log(message: str) -> None:
    func = inspect.currentframe().f_back.f_code
    print(f"[{func.co_name}] {message}")


def _fmt_signature(func: Callable, *args: Any, **kwargs: Any) -> str:
    arg = [shortlabel(a) for a in args]
    kw = [f'{k}={shortlabel(v)}' for k, v in kwargs.items()]
    signature = ', '.join(arg + kw)
    return f'[{shortlabel(func)}]({signature})'


def catch(error_msg: str = '', fallback: Any = None, log_exception: bool = False) -> Callable:
    """ A decorator for catching exceptions. Usage:
        >>> @catch('Error generating string', '')
        >>> def generate():
        >>>     ...

    """
    def decorator(func: Callable):
        @wraps(func)
        def inner(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                msg = f'{error_msg} {_fmt_signature(func, *args, **kwargs)} {e}'
                logger.exception(msg) if log_exception else logger.error(msg)
                return fallback
        return inner
    return decorator


def catch_async(error_msg: str = '', fallback: Any = None, log_exception: bool = False) -> Callable:
    def decorator(func: Callable):
        @wraps(func)
        async def inner(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                msg = f'{error_msg} {_fmt_signature(func, *args, **kwargs)} {e}'
                logger.exception(msg) if log_exception else logger.error(msg)
                return fallback
        return inner
    return decorator


@catch('Error format data', '')
def fmt(data: Any) -> Union[str, List[str]]:
    if data is None or not len(data):
        return ''
    if isinstance(data, List):
        return [str(item) for item in data]
    if isinstance(data, Dict):
        return [f'{k}: {v}' for k, v in data.items() if v is not None]
    if isinstance(data, pd.DataFrame):
        if isinstance(data.columns, pd.MultiIndex):
            # This is to prevent non-str column type
            headers = ['\n'.join([str(c) for c in column]) for column in list(data.columns)]
        else:
            headers = 'keys'
        return tabulate(data, headers=headers, tablefmt='presto', numalign='right')


def parse(text: str, separator: str = ',') -> List[str]:
    return [word.strip() for word in text.split(separator)]
