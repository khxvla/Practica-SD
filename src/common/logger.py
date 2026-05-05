"""Logging utilities."""
import logging
import sys
from src.common.config import LOG_LEVEL

def get_logger(name):
    """Get a configured logger."""
    logger = logging.getLogger(name)
    
    # Avoid duplicate handlers
    if logger.hasHandlers():
        return logger
    
    logger.setLevel(LOG_LEVEL)
    
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    return logger
