"""Service settings: ``load_settings()`` reads them, and whoever starts something passes them on.

The same pattern as Rockwell: ``conf/application.conf`` holds the settings, and ``${VAR}`` values in it
come from the environment. A variable that is not set reads as empty, and the default below applies.
Nothing reads the settings from a module global: ``main`` loads them once for the app it creates
(``create_app(settings)``), the command line loads them for its run, and the connector, the handlers
and the job worker take them as arguments. Importing this module reads nothing.

Source credentials (``KAGGLE_API_TOKEN``, ``HF_TOKEN``) are not settings: the source libraries read
them from the environment themselves, and nothing here reads or logs them. Two kinds of secret are
read here: the service's own token (what callers must present, ``src.services.auth``) and the S3 File
Store's credentials. Each comes from its variable (``NICKEL_SERVICE_TOKEN``), or from the file its
``_FILE`` variable names (``NICKEL_SERVICE_TOKEN_FILE``: a Vault or K8s secret mount), and none of
them is in the settings' repr.
"""

from __future__ import annotations

import os
import re
from collections.abc import Callable, Mapping
from configparser import BasicInterpolation, ConfigParser
from dataclasses import dataclass, field
from pathlib import Path
from uuid import UUID

from botocore.utils import is_valid_endpoint_url, is_valid_ipv6_endpoint_url

from src import __version__
from src.utility.endpoints import check_internal_endpoint
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
_FILE_STORES = ("local", "s3")
# Each S3 call's deadline: a 100 MB original (the default NICKEL_MAX_FILE_BYTES) at 1 MB/s, with room
# for the client's retries. Raise it with the cap.
DEFAULT_S3_CALL_DEADLINE_S = 300
# What S3 takes as a bucket name: 3 to 63 lower-case letters, digits, dots and hyphens, a letter or
# digit at both ends (and no "..", checked apart).
_BUCKET = re.compile(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]")
_REGION = re.compile(r"[a-z0-9]+(-[a-z0-9]+)*")


class EnvInterpolation(BasicInterpolation):
    """Fill ``${VAR}`` from the environment; an unset variable becomes an empty value."""

    def __init__(self, environ: Mapping[str, str]) -> None:
        super().__init__()
        self._environ = environ

    def before_get(self, parser, section, option, value, defaults):
        value = super().before_get(parser, section, option, value, defaults)
        return _VARIABLE.sub(lambda match: self._environ.get(match.group(1), ""), value)


@dataclass(frozen=True, kw_only=True)
class S3Settings:
    """``[FILE_STORE]`` for ``NICKEL_FILE_STORE=s3``: the S3 API the originals go to (SeaweedFS in
    development; the object store the SeaweedFS-vs-RustFS bake-off picks in production), checked at
    start-up (``load_settings``). The credentials are out of the repr, as the service token is.
    """

    endpoint_url: str
    region: str
    originals_bucket: str
    access_key_id: str = field(repr=False)
    secret_access_key: str = field(repr=False)
    # A PEM bundle for an https endpoint behind a private CA; None: the default bundle. Never
    # AWS_CA_BUNDLE: the S3 client reads nothing from the environment (src.db.file_store.s3).
    ca_bundle: Path | None = None
    # NICKEL_S3_ALLOW_PUBLIC_ENDPOINT: the endpoint rule (src.utility.endpoints) is off, and every
    # store opened says so in the log.
    allow_public_endpoint: bool = False
    # Production: plain http to loopback only (src.utility.endpoints).
    require_https: bool = False
    # NICKEL_S3_CALL_DEADLINE_SECONDS: the most any one call to the store may take, retries and
    # all (src.db.file_store.s3).
    call_deadline_s: float = DEFAULT_S3_CALL_DEADLINE_S


@dataclass(frozen=True)
class ServiceSettings:
    name: str
    environment: str
    version: str
    log_level: str
    cors_origins: tuple[str, ...]
    # [SERVICE] token: what a caller must present as ``Authorization: Bearer <token>``
    # (src.services.auth). Empty when unset: main.create_app refuses to start on it. Out of the
    # repr, so a settings object that ends up in a log or an error cannot leak it.
    service_token: str = field(repr=False)
    # [CONNECTOR]: the tenant everything is ingested for (None until the deployment sets it), the
    # ingest bridge's size cap, and where the document manifests are.
    tenant_id: UUID | None
    max_file_bytes: int
    manifest_dir: Path
    # [STORAGE]: the root of the development File Store, outbox, receipts and job records.
    data_dir: Path
    # [JOBS]: how many jobs may wait in the queue; a POST /jobs/* beyond it is a 429.
    max_queued_jobs: int
    # [FILE_STORE]: where the originals go. None: the local File Store under data_dir (the default,
    # NICKEL_FILE_STORE=local); the S3 API's settings for NICKEL_FILE_STORE=s3.
    s3: S3Settings | None


def _tenant(raw: str) -> UUID | None:
    if not raw:
        return None
    try:
        return parse_tenant_id(raw)
    except ValueError as exc:
        raise ValueError(f"tenant_id: {exc}") from exc


def _secret(name: str, value: str, secret_file: str) -> str:
    """The secret ``name`` names: its value, or the content of the file ``<name>_FILE`` names
    (stripped: a mounted secret usually ends in a newline). One source only: with both set, an
    operator could rotate the file and change nothing. An error names the variable, never the value.
    """
    if value and secret_file:
        raise ValueError(f"set {name} or {name}_FILE, not both")
    if not secret_file:
        return value
    try:
        # utf-8-sig: a byte order mark (Windows editors, PowerShell) is not part of the secret.
        return Path(secret_file).read_text(encoding="utf-8-sig").strip()
    except OSError as exc:
        # The error's type only: the OS message names the file's path.
        raise ValueError(f"{name}_FILE cannot be read: {type(exc).__name__}") from None
    except UnicodeDecodeError:
        # Its message would quote a byte of the secret.
        raise ValueError(f"{name}_FILE is not UTF-8 text") from None


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


def _flag(name: str, raw: str) -> bool:
    """A true/false setting; unset is false."""
    value = raw.lower()
    if value not in ("", "true", "false"):
        raise ValueError(f"{name} must be true or false, not {raw!r}")
    return value == "true"


def check_s3_endpoint(s3: S3Settings) -> None:
    """The S3 File Store's endpoint, by the internal-endpoint rule (``src.utility.endpoints``) and
    as a URL the S3 client takes: at start-up, and again each time a job opens the store (the name
    may resolve elsewhere by then). ValueError names the setting, never a credential."""
    check_internal_endpoint(
        s3.endpoint_url,
        setting="NICKEL_S3_ENDPOINT_URL",
        override="NICKEL_S3_ALLOW_PUBLIC_ENDPOINT",
        allow_public=s3.allow_public_endpoint,
        https_off_loopback=s3.require_https,
    )
    if not (
        is_valid_endpoint_url(s3.endpoint_url)
        or is_valid_ipv6_endpoint_url(s3.endpoint_url)
    ):
        raise ValueError(
            "NICKEL_S3_ENDPOINT_URL has a host name the S3 client refuses (such as one with "
            "an underscore)"
        )


def _s3_settings(get: Callable[..., str], environment: str) -> S3Settings:
    """``[FILE_STORE]`` for the S3 backend, every value checked before anything is contacted: the
    endpoint (``check_s3_endpoint``; https off loopback everywhere but development), the bucket and
    the region by what S3 accepts, both credentials present, each from one source (outside
    development, a mounted file: never the variable itself), and the call deadline.
    """
    endpoint = get("FILE_STORE", "s3_endpoint_url")
    if not endpoint:
        raise ValueError(
            "NICKEL_S3_ENDPOINT_URL is not set: the File Store's S3 API, e.g. "
            "http://seaweedfs:8333"
        )
    allow_public = _flag(
        "NICKEL_S3_ALLOW_PUBLIC_ENDPOINT", get("FILE_STORE", "s3_allow_public_endpoint")
    )
    bucket = get("FILE_STORE", "s3_originals_bucket", "originals")
    if not _BUCKET.fullmatch(bucket) or ".." in bucket:
        raise ValueError(
            f"NICKEL_S3_ORIGINALS_BUCKET {bucket!r} is not a bucket name: 3 to 63 lower-case "
            "letters, digits, dots and hyphens"
        )
    region = get("FILE_STORE", "s3_region", "us-east-1")
    if not _REGION.fullmatch(region):
        raise ValueError(f"NICKEL_S3_REGION {region!r} is not a region name")
    credentials = {
        name: _secret(
            name, get("FILE_STORE", option), get("FILE_STORE", f"{option}_file")
        )
        for name, option in (
            ("NICKEL_S3_ACCESS_KEY_ID", "s3_access_key_id"),
            ("NICKEL_S3_SECRET_ACCESS_KEY", "s3_secret_access_key"),
        )
    }
    if environment != "development":
        for name, option in (
            ("NICKEL_S3_ACCESS_KEY_ID", "s3_access_key_id"),
            ("NICKEL_S3_SECRET_ACCESS_KEY", "s3_secret_access_key"),
        ):
            if get("FILE_STORE", option):
                raise ValueError(
                    f"{name} is for development: in {environment} the S3 credentials come "
                    f"from {name}_FILE (a Vault or CSI mount), never the variable itself"
                )
    missing = [name for name, value in credentials.items() if not value]
    if missing:
        raise ValueError(
            f"{' and '.join(missing)} not set (nor the _FILE variant): the S3 File Store "
            "takes its credentials from the settings only"
        )
    ca_bundle = get("FILE_STORE", "s3_ca_bundle")
    if ca_bundle and not Path(ca_bundle).is_file():
        raise ValueError(f"NICKEL_S3_CA_BUNDLE {ca_bundle!r} is not a file")
    s3 = S3Settings(
        endpoint_url=endpoint,
        region=region,
        originals_bucket=bucket,
        access_key_id=credentials["NICKEL_S3_ACCESS_KEY_ID"],
        secret_access_key=credentials["NICKEL_S3_SECRET_ACCESS_KEY"],
        ca_bundle=Path(ca_bundle) if ca_bundle else None,
        allow_public_endpoint=allow_public,
        # Plain http to a private name is for a development stack only: anywhere else a name that
        # resolves elsewhere later could take the originals, and TLS (the name's certificate) is
        # what stops that. Loopback stays allowed.
        require_https=environment != "development",
        call_deadline_s=_positive_int(
            "NICKEL_S3_CALL_DEADLINE_SECONDS",
            get("FILE_STORE", "s3_call_deadline_seconds"),
            DEFAULT_S3_CALL_DEADLINE_S,
        ),
    )
    check_s3_endpoint(s3)
    return s3


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
    # Production unless said otherwise: the API docs stay off on a deployment that sets nothing.
    environment = get("SERVICE", "environment", "production").lower()
    if environment not in _ENVIRONMENTS:
        raise ValueError(
            f"environment {environment!r} is not one of {', '.join(_ENVIRONMENTS)}"
        )
    origins = get("SERVICE", "cors_origins", "")
    # Production keeps the originals in the S3 File Store, never on a pod's disk; development and
    # qa default to local disk.
    production = environment == "production"
    file_store = get("FILE_STORE", "backend", "s3" if production else "local").lower()
    if file_store not in _FILE_STORES:
        raise ValueError(
            f"file store {file_store!r} is not one of {', '.join(_FILE_STORES)}"
        )
    if production and file_store == "local":
        raise ValueError(
            "NICKEL_FILE_STORE=local is for development and qa: production keeps the originals "
            "in the S3 File Store"
        )
    return ServiceSettings(
        name=get("SERVICE", "name", "nickel-connection"),
        environment=environment,
        # Baked into the image at build time (Dockerfile ARG APP_VERSION), as in Rockwell.
        version=environ.get("APP_VERSION", "").strip() or __version__,
        log_level=log_level,
        cors_origins=tuple(o.strip() for o in origins.split(",") if o.strip()),
        service_token=_secret(
            "NICKEL_SERVICE_TOKEN",
            get("SERVICE", "token"),
            get("SERVICE", "token_file"),
        ),
        tenant_id=_tenant(get("CONNECTOR", "tenant_id")),
        max_file_bytes=_positive_int(
            "max_file_bytes", get("CONNECTOR", "max_file_bytes"), DEFAULT_MAX_FILE_BYTES
        ),
        manifest_dir=Path(get("CONNECTOR", "manifest_dir") or MANIFEST_DIR),
        data_dir=Path(get("STORAGE", "data_dir", ".data")),
        max_queued_jobs=_positive_int(
            "jobs max_queued", get("JOBS", "max_queued"), 100
        ),
        s3=_s3_settings(get, environment) if file_store == "s3" else None,
    )
