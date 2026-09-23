"""Service settings, read once at start-up (use: ``from src.config import Service``).

The same pattern as Rockwell: ``conf/application.conf`` holds the settings, and ``${VAR}`` values in it
come from the environment. A variable that is not set reads as empty, and the default below applies.

Source credentials (``KAGGLE_API_TOKEN``, ``HF_TOKEN``) are not settings: the source libraries read
them from the environment themselves, and nothing here reads or logs them.
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from configparser import BasicInterpolation, ConfigParser
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

from src import __version__
from src.utility.keys import parse_tenant_id

APP_DIR = Path(__file__).resolve().parents[2]
CONF_FILE = APP_DIR / "conf" / "application.conf"
MANIFEST_DIR = APP_DIR / "manifests" / "documents"
DEFAULT_MAX_FILE_BYTES = 100 * 1024 * 1024
_VARIABLE = re.compile(r"\$\{(\w+)\}")
# Every enumerated setting is checked at start-up: a typo stops the service at once instead of
# failing the first job hours later.
_LOG_LEVELS = ("CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG")
_ENVIRONMENTS = ("development", "qa", "production")


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
    # [CONNECTOR]: the tenant everything is ingested for (None until the deployment sets it), the
    # ingest bridge's size cap, and where the document manifests are.
    tenant_id: UUID | None
    max_file_bytes: int
    manifest_dir: Path
    # [STORAGE]: the root of the development File Store, outbox, receipts and job records.
    data_dir: Path
    # [JOBS]: how many jobs may wait in the queue; a POST /jobs/* beyond it is a 429.
    max_queued_jobs: int


def _tenant(raw: str) -> UUID | None:
    if not raw:
        return None
    try:
        return parse_tenant_id(raw)
    except ValueError as exc:
        raise ValueError(f"tenant_id: {exc}") from exc


def _positive_int(name: str, raw: str, default: int) -> int:
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a whole number, not {raw!r}") from exc
    if value < 1:
        raise ValueError(f"{name} must be a positive number, not {raw!r}")
    return value


def load_settings(
    path: Path = CONF_FILE, environ: Mapping[str, str] | None = None
) -> ServiceSettings:
    environ = os.environ if environ is None else environ
    parser = ConfigParser(interpolation=EnvInterpolation(environ))
    if not parser.read(path, encoding="utf-8"):
        raise FileNotFoundError(f"service configuration not found: {path}")

    def get(section: str, option: str, default: str = "") -> str:
        return parser.get(section, option, fallback="").strip() or default

    log_level = get("LOGGING", "level", "INFO").upper()
    if log_level not in _LOG_LEVELS:
        raise ValueError(
            f"log level {log_level!r} is not one of {', '.join(_LOG_LEVELS)}"
        )
    environment = get("SERVICE", "environment", "development").lower()
    if environment not in _ENVIRONMENTS:
        raise ValueError(
            f"environment {environment!r} is not one of {', '.join(_ENVIRONMENTS)}"
        )
    origins = get("SERVICE", "cors_origins", "")
    return ServiceSettings(
        name=get("SERVICE", "name", "nickel-connection"),
        environment=environment,
        # Baked into the image at build time (Dockerfile ARG APP_VERSION), as in Rockwell.
        version=environ.get("APP_VERSION", "").strip() or __version__,
        log_level=log_level,
        cors_origins=tuple(o.strip() for o in origins.split(",") if o.strip()),
        tenant_id=_tenant(get("CONNECTOR", "tenant_id")),
        max_file_bytes=_positive_int(
            "max_file_bytes", get("CONNECTOR", "max_file_bytes"), DEFAULT_MAX_FILE_BYTES
        ),
        manifest_dir=Path(get("CONNECTOR", "manifest_dir") or MANIFEST_DIR),
        data_dir=Path(get("STORAGE", "data_dir", ".data")),
        max_queued_jobs=_positive_int(
            "jobs max_queued", get("JOBS", "max_queued"), 100
        ),
    )


Service = load_settings()
