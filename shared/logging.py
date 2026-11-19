from __future__ import annotations

import logging
import sys


def configure_logging(service_name: str, level: int = logging.INFO) -> logging.Logger:
    """Configure uniform structured-friendly stdout logging for a microservice."""
    logging.basicConfig(
        level=level,
        format=f"%(asctime)s | %(levelname)s | {service_name} | %(name)s | %(message)s",
        stream=sys.stdout,
        force=True,
    )
    return logging.getLogger(service_name)
