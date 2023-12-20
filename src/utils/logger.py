import logging
import loguru
import sys

from dotenv import load_dotenv
from loguru import logger
from pathlib import Path

ib_logger = logging.getLogger("ib_insync")
ib_logger.setLevel(logging.CRITICAL)
load_dotenv()
LOG_PATH: Path = Path.home().joinpath('log')

FORMAT = (
    "<green>[{time:YYYY-MM-DD HH:mm:ss.SSS}]</green> <level>[{level}]</level> "
    "<cyan>[{module}:{function}:{line}]</cyan> <level>{message}</level>"
)

config = {
    "handlers": [
        {"sink": sys.stdout, "format": FORMAT, 'diagnose': False, 'level': "INFO"},
    ],
    "levels": [
        {"name": "DEBUG", "color": "<blue><dim>"},
        {"name": "INFO", "color": "<white>"},
        {"name": "WARNING", "color": "<yellow>"},
        {"name": "ERROR", "color": "<red>"},
        {"name": "CRITICAL", 'color': '<red><bold>'},
    ]
}
# Default logger
logger.configure(**config)


def get_logger(name: str = None, level: str = 'DEBUG') -> loguru.logger:
    """ Get a new logger with given log level

    :param name: an optional name parameter that is used to write the log to file.
    :param level: minimum logging level.
    """
    if name is not None:
        filename = str(LOG_PATH.joinpath(f'{name}.log'))
        logger.add(filename, format=FORMAT, level=level, diagnose=False,
                   rotation='1 day', retention='10 days', compression='zip')
    return logger
