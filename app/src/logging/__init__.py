"""The service's logger (use: ``from src.logging import logger``), as in Rockwell.

Logs go to stdout for the container runtime to collect. Never log secrets, tokens or patient data.
"""

from __future__ import annotations

import logging
import sys

from src.config import Service

LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
_HANDLER_NAME = "nickel-stdout"


def build_logger(name: str, level: str) -> logging.Logger:
    """A logger writing to stdout; calling it again for the same name adds no second handler."""
    log = logging.getLogger(name)
    log.setLevel(level)
    log.propagate = False
    if not any(handler.get_name() == _HANDLER_NAME for handler in log.handlers):
        handler = logging.StreamHandler(sys.stdout)
        handler.set_name(_HANDLER_NAME)
        handler.setFormatter(logging.Formatter(LOG_FORMAT))
        log.addHandler(handler)
    return log


logger = build_logger(Service.name, Service.log_level)
