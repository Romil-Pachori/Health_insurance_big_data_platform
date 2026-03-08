import logging
from typing import Optional


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(level)
    handler: logging.Handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    )
    logger.addHandler(handler)
    logger.propagate = False
    return logger


def set_global_level(level: Optional[str]) -> None:
    if not level:
        return
    numeric = getattr(logging, level.upper(), logging.INFO)
    logging.getLogger().setLevel(numeric)

