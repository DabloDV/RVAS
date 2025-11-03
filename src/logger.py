import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from .config import SETTINGS

def get_logger(name: str = "etl") -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    level = getattr(logging, SETTINGS.log_level.upper(), logging.INFO)
    logger.setLevel(level)


    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    ))
    logger.addHandler(ch)

    #File
    SETTINGS.log_dir.mkdir(parents=True, exist_ok=True)
    fh_path = SETTINGS.log_dir / "pipeline.log"
    fh = RotatingFileHandler(fh_path, maxBytes=5_000_000, backupCount=3, encoding="utf-8")
    fh.setLevel(level)
    fh.setFormatter(logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    ))
    logger.addHandler(fh)

    logger.debug("Logger initialized")
    return logger