"""The service's logger (use: ``from src.logging import logger``), as in Rockwell.

Logs go to stdout for the container runtime to collect. Never log secrets, tokens or patient data:
ids, counts, hashes and error types only (a clinic's file names carry patient names, so paths and
file names count as patient data, and so does a library's exception text, which can quote them).

The logger exists at import under a fixed name; its level comes from the settings of whatever starts
(``main.create_app`` calls ``configure_logging`` with the service's; the command line sets its own).
"""

from __future__ import annotations

import logging
import sys

LOGGER_NAME = "nickel-connection"
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


def configure_logging(level: str) -> None:
    """Set the service logger's level from the settings (``ServiceSettings.log_level``)."""
    logger.setLevel(level)


logger = build_logger(LOGGER_NAME, "INFO")
