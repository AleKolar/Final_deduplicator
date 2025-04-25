import logging
import sys
from logging import Logger


def setup_logger(name: str = "app", level: str = "INFO") -> Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Удаление существующих обработчиков
    if logger.hasHandlers():
        logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger

logger = setup_logger()