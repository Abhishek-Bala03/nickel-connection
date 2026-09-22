"""Service settings, read once at start-up (use: ``from src.config import Service``).

The same pattern as Rockwell: ``conf/application.conf`` holds the settings, and ``${VAR}`` values in it
come from the environment. A variable that is not set reads as empty, and the default below applies.
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from configparser import BasicInterpolation, ConfigParser
from dataclasses import dataclass
from pathlib import Path

from src import __version__

CONF_FILE = Path(__file__).resolve().parents[2] / "conf" / "application.conf"
_VARIABLE = re.compile(r"\$\{(\w+)\}")
_LOG_LEVELS = ("CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG")


class EnvInterpolation(BasicInterpolation):
    """Fill ``${VAR}`` from the environment; an unset variable becomes an empty value."""

    def __init__(self, environ: Mapping[str, str]) -> None:
        super().__init__()
        self._environ = environ

    def before_get(self, parser, section, option, value, defaults):
        value = super().before_get(parser, section, option, value, defaults)
        return _VARIABLE.sub(lambda match: self._environ.get(match.group(1), ""), value)


@dataclass(frozen=True)
class ServiceSettings:
    name: str
    environment: str
    version: str
    log_level: str
    cors_origins: tuple[str, ...]


def load_settings(
    path: Path = CONF_FILE, environ: Mapping[str, str] | None = None
) -> ServiceSettings:
    environ = os.environ if environ is None else environ
    parser = ConfigParser(interpolation=EnvInterpolation(environ))
    if not parser.read(path, encoding="utf-8"):
        raise FileNotFoundError(f"service configuration not found: {path}")

    def get(section: str, option: str, default: str) -> str:
        return parser.get(section, option, fallback="").strip() or default

    log_level = get("LOGGING", "level", "INFO").upper()
    if log_level not in _LOG_LEVELS:
        raise ValueError(f"log level must be one of {', '.join(_LOG_LEVELS)}")
    origins = get("SERVICE", "cors_origins", "")
    return ServiceSettings(
        name=get("SERVICE", "name", "nickel-connection"),
        environment=get("SERVICE", "environment", "development"),
        # Baked into the image at build time (Dockerfile ARG APP_VERSION), as in Rockwell.
        version=environ.get("APP_VERSION", "").strip() or __version__,
        log_level=log_level,
        cors_origins=tuple(o.strip() for o in origins.split(",") if o.strip()),
    )


Service = load_settings()
