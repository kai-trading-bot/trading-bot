import logging
import pytest
from _pytest.logging import caplog as _caplog
from loguru import logger


"""
https://loguru.readthedocs.io/en/0.4.1/resources/migration.html#making-things-work-with-pytest-and-caplog
"""
@pytest.fixture
def caplog(_caplog):
    class PropogateHandler(logging.Handler):
        def emit(self, record):
            logging.getLogger(record.name).handle(record)

    handler_id = logger.add(PropogateHandler(), format="{message}")
    yield _caplog
    logger.remove(handler_id)
