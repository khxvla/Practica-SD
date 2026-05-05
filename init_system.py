#!/usr/bin/env python
"""Initialization script for the ticket system."""

import os
import sys

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.common.logger import get_logger
from src.common.redis_backend import RedisBackend

logger = get_logger(__name__)


def main():
    """Initialize the system."""
    logger.info("Initializing Ticket Acquisition System...")

    try:
        backend = RedisBackend()
        logger.info("Connected to Redis")

        backend.clear_all()
        logger.info("Cleared previous state")

        backend.initialize_system()
        logger.info("System initialized")

        stats = backend.get_stats()
        logger.info("System Status:")
        logger.info(f"  Unnumbered tickets sold: {stats['unnumbered_sold']}/20000")
        logger.info(f"  Numbered tickets available: {stats['numbered_available']}/20000")
        logger.info(f"  Numbered tickets sold: {stats['numbered_sold']}")
        logger.info("Initialization complete!")
        return True

    except Exception as e:
        logger.error(f"Initialization failed: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
