import aiohttp
import asyncio
from typing import Dict

semaphore = asyncio.Semaphore(100)


async def get_json(url: str, params: Dict = None):
    async with aiohttp.ClientSession() as session:
        async with semaphore, session.get(url, params=params) as resp:
            return await resp.json()


async def get_text(url: str, params: Dict = None):
    async with aiohttp.ClientSession() as session:
        async with semaphore, session.get(url, params=params) as resp:
            return await resp.text()
