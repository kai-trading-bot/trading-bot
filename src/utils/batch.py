import asyncio
import inspect
import pandas as pd
import time


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


def in_chunks(seq, size):
    """
    Return sequence in 'chunks' of size defined by size
    """
    return (seq[pos: pos + size] for pos in range(0, len(seq), size))
