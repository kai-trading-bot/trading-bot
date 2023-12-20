import pytest
from src.execution.signals import BiasV2


@pytest.mark.asyncio
async def test_weights_sum_to_one(monkeypatch):
    monkeypatch.setattr('src.execution.signals.bias.MIN_WEIGHT', 0.01)
    bias = BiasV2()
    await bias.fetch()
    await bias.update(10000)
    assert set(bias.weights.sum(axis=1).round(2).unique()) == {1, 0}
