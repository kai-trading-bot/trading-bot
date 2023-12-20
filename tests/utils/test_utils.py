import pytest
from src.utils import catch, catch_async, _fmt_signature


def test_catch():
    @catch('Error', None)
    def func(throw: bool):
        if throw:
            raise Exception(f'Failed')
        return 0

    assert func(throw=False) == 0
    assert func(throw=True) is None


def test_catch_with_multi_decorators():
    class Test:
        def __init__(self, throw: bool):
            self.throw = throw

        @property
        @catch('Error', -1)
        def value(self):
            if self.throw:
                raise Exception('Failed')
            return 0

    assert Test(throw=False).value == 0
    assert Test(throw=True).value == -1


@pytest.mark.asyncio
async def test_catch_async():
    @catch_async('Error', None)
    async def func(throw: bool):
        if throw:
            raise Exception(f'Failed')
        return 0

    assert await func(throw=False) == 0
    assert await func(throw=True) is None


def test__fmt_signature():
    def func():
        return 0

    class Test:
        def func(self):
            return 0

    signature = _fmt_signature(func, Test().func(), Key=0, Value=Exception)
    print(signature)
