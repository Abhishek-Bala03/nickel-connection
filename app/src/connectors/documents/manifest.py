"""Document source manifests: one YAML file per source (``manifests/documents/<id>.yaml``).

A subset of the connector manifest contract (CN-01: id, stream, mode, route, auth, schedule,
endpoints, rate_limit, formats, expectations, contract, phi, owner, phase, shadow, raw_retention),
with CN-01's nesting for the fields that exist here (``extra="forbid"`` rejects anything else):
``auth: {type, vault_path}``, ``contract: {baa, dpa}`` and ``schedule: {class}``. Not here yet, and
listed in the docs' "What is temporary" table: ``endpoints``, ``rate_limit``, ``expectations``,
``schedule``'s ``interval``, ``overlap`` and ``backfill``, and ``formats`` in CN-01's shape
(``{schema_id, validate}``: here it is a list of the formats the edge check sniffs). A document
source has a source block (``kaggle``, ``huggingface``, ``http`` or ``local``) in place of
endpoints. The governance fields are required, so CI rejects a manifest missing ``contract``,
``auth``, ``phi`` or ``owner``.

What this connector reads:

- ``fetch``: the transport, which decides the source block and the message ids.
- ``auth.type``: must fit ``fetch`` (``none`` for http and local, ``kaggle_token`` for kaggle,
  ``hf_token`` for a gated huggingface repo and ``none`` for a public one). Until CN-09's runner
  hands secrets over, the SDKs read the token from the environment themselves (``KAGGLE_API_TOKEN``,
  ``HF_TOKEN``), which is for dev sources only: outside the ``dev`` phase a token needs
  ``auth.vault_path``.
- ``phi`` and ``contract.baa``: a PHI source without a business associate agreement is refused.
- ``phase``: how far the source is cleared. ``dev`` runs in development only, ``qa`` in development and
  qa, ``prod`` anywhere; the connector refuses a manifest the service's environment is not cleared for.
  ``fetch: kaggle`` and ``fetch: huggingface`` are ``dev`` only: their SDKs download on their own,
  outside the egress rule (the host allow-list and the pinned address), until they go through it.
- ``shadow``: true stores the originals and the receipts but publishes nothing to ``raw_data`` (the
  receipt says the item is held), the "sandbox test > shadow mode > enable" flow of the Connections UI.
- ``raw_retention``: ``forever`` or ``<N>d`` (1 to 3650 days). Either way the originals go to the
  ``documents`` object class (``src.utility.keys``), which is in every object key and envelope; the
  value is recorded on each stored object's sidecar, and expiry is the File Store lifecycle rule's job
  (nickel-connection issue #6), not this connector's.
- ``max_file_bytes``, ``include``, ``exclude`` and ``max_files``: what a run takes.

Validated here, read elsewhere: ``auth.vault_path`` is where the connector runner (CN-09) will read
the credential (null for public data); ``schedule.class`` is the frequency class the scheduler reads
(the old string, ``schedule: on_demand``, still reads as ``{class: F4}`` for one release, with a
warning); ``owner`` is for the registrar and monitoring (CN-05); ``id``, ``stream``, ``mode``,
``description`` and ``license`` are registry data. ``route`` is the lab route (L1-L9) the fidelity
ranking reads, an envelope field, not a transport.

``DocumentManifest.load`` raises a ManifestError for a file that is not a manifest, with a
content-free message (``manifest does not validate: license string_too_short``): a job's record and
the relay's log quote it, and the command line exits 2 with it.
"""

from __future__ import annotations

from collections.abc import Collection
from fnmatch import fnmatchcase
from pathlib import Path
from typing import Literal

import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    ValidationError,
    ValidationInfo,
    field_validator,
    model_validator,
)

from src.logging import logger
from src.utility.keys import ObjectClass

from .sources import HTTPS_PORT

Fetch = Literal["kaggle", "huggingface", "http", "local"]
AuthType = Literal["none", "kaggle_token", "hf_token"]
Phase = Literal["dev", "qa", "prod"]
# CN-01's frequency classes: F1 continuous, F2 daily summary, F3 episodic, F4 on demand, F5 backfill
# (its own lane). A document source is F4 unless its manifest says otherwise.
FrequencyClass = Literal["F1", "F2", "F3", "F4", "F5"]
# Auth types whose credential the SDK reads from the environment until CN-09's runner hands it
# over from Vault: fine for a dev source, not beyond (``auth.vault_path`` then).
_TOKEN_AUTH = frozenset({"kaggle_token", "hf_token"})
# Fetches that go through an SDK, which downloads on its own: outside the egress rule (the host
# allow-list and the pinned address), so dev only until they go through it.
_SDK_FETCHES = frozenset({"kaggle", "huggingface"})
MANIFEST_ID_PATTERN = r"^[a-z0-9][a-z0-9-]{2,62}$"
RAW_RETENTION_PATTERN = r"^(forever|[1-9]\d{0,3}d)$"  # forever, or a number of days
MAX_RETENTION_DAYS = 3650
# The phases each environment runs (src.config validates the environment names).
_CLEARED_FOR: dict[str, frozenset[str]] = {
    "development": frozenset({"dev", "qa", "prod"}),
    "qa": frozenset({"qa", "prod"}),
    "production": frozenset({"prod"}),
}
# The auth.type each transport takes. Kaggle always needs its token; a public Hugging Face repo
# needs none, a gated one the hub token; web links and folders take no credential here.
_AUTH_FOR_FETCH: dict[str, frozenset[str]] = {
    "kaggle": frozenset({"kaggle_token"}),
    "huggingface": frozenset({"none", "hf_token"}),
    "http": frozenset({"none"}),
    "local": frozenset({"none"}),
}
# The string ``schedule`` was before it took CN-01's shape, still read for one release (deprecated).
_OLD_SCHEDULE = "on_demand"
_PROBLEMS_SHOWN = 5  # a ManifestError names at most this many problems


class ManifestError(Exception):
    """A manifest file that is not a manifest: not YAML, or it does not validate. The message is
    content-free: field names, pydantic's error types, the line and column of a YAML error, and
    the messages of this module's own validators (they name fields and allowed values, and a
    link's host, never a value from the file). pydantic's and the YAML parser's own messages
    quote the file: a folder's path, a link with its credential."""


def _yaml_problem(exc: yaml.YAMLError) -> str:
    """The parser's error type and, when it knows it, where: never its message or its snippet."""
    mark = getattr(exc, "problem_mark", None)
    if mark is None:
        return type(exc).__name__
    return f"{type(exc).__name__} at line {mark.line + 1}, column {mark.column + 1}"


def _problems(exc: ValidationError, fields: Collection[str]) -> str:
    """Each problem as the top-level field it is on and pydantic's error type, as a stored
    envelope's are (``src.handler.ingest``), plus the message of a ValueError from this module's
    validators. A key the model does not know (``extra_forbidden``) is the file's own text: it
    goes by its error type alone."""
    problems = set()
    for error in exc.errors():
        problem = error["type"]
        if problem == "value_error":
            problem += f" ({error['ctx']['error']})"
        field = error["loc"][0] if error["loc"] else None
        problems.add(f"{field} {problem}" if field in fields else problem)
    ordered = sorted(problems)
    shown = ", ".join(ordered[:_PROBLEMS_SHOWN])
    if len(ordered) > _PROBLEMS_SHOWN:
        shown += f" and {len(ordered) - _PROBLEMS_SHOWN} more"
    return shown


class _Spec(BaseModel):
    model_config = ConfigDict(extra="forbid")


class AuthSpec(_Spec):
    """CN-01 ``auth``: how the source authenticates, and where its credential is kept."""

    type: AuthType
    vault_path: str | None = Field(
        description="Vault path of the credential (the connector runner reads it); "
        "null for public data."
    )


class ContractSpec(_Spec):
    """CN-01 ``contract``: the agreements behind the source. Both keys are given, null when
    there is none (public data)."""

    baa: str | None
    dpa: str | None


class ScheduleSpec(_Spec):
    """CN-01 ``schedule``, with the key this connector has: ``{class: F4}``. ``interval``,
    ``overlap`` and ``backfill`` come with the scheduler (NIK-2209-03)."""

    frequency_class: FrequencyClass = Field(
        default="F4",
        alias="class",
        description="F1 continuous, F2 daily summary, F3 episodic, F4 on demand, F5 backfill.",
    )


class KaggleSpec(_Spec):
    handle: str = Field(
        pattern=r"^[\w.-]+/[\w.-]+(/versions/\d+)?$",
        description="owner/dataset[/versions/N]",
    )
    files: list[str] | None = Field(
        default=None, description="Fetch only these paths instead of the whole dataset."
    )


class HuggingFaceSpec(_Spec):
    repo_id: str = Field(pattern=r"^[\w.-]+/[\w.-]+$")
    revision: str | None = Field(
        default=None,
        description="Branch, tag or commit; pin a commit for repeatable pulls.",
    )
    repo_type: Literal["dataset", "model", "space"] = "dataset"


class HttpSpec(_Spec):
    urls: list[HttpUrl] = Field(min_length=1)

    @field_validator("urls")
    @classmethod
    def _https_on_443_without_a_credential(cls, urls: list[HttpUrl]) -> list[HttpUrl]:
        """The egress rule at load time (``sources.HttpSource`` applies it to every hop as well),
        and no ``user:password@`` in a link: it would go out as Basic auth and, as the item's
        message id, onto ``raw_data`` and every receipt. A credential belongs in Vault
        (``auth.vault_path``). The error names the host only, never the credential.
        """
        for url in urls:
            if url.scheme != "https" or url.port != HTTPS_PORT:
                raise ValueError(
                    f"{url.scheme}://{url.host}: http.urls must be https on port {HTTPS_PORT}"
                )
            if url.username is not None or url.password is not None:
                raise ValueError(
                    f"https://{url.host}: http.urls must carry no user or password "
                    "(a credential belongs in Vault: auth.vault_path)"
                )
        return urls


class LocalSpec(_Spec):
    path: Path


class DocumentManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(pattern=MANIFEST_ID_PATTERN)
    description: str
    stream: Literal["documents"]
    mode: Literal["pull"]
    route: str = Field(pattern=r"^L[1-9]$")  # the lab route (L1-L9): PDFs are L7
    fetch: Fetch
    auth: AuthSpec
    schedule: ScheduleSpec = Field(default_factory=ScheduleSpec)
    formats: list[Literal["pdf"]] = Field(default_factory=lambda: ["pdf"], min_length=1)
    phi: bool
    owner: str = Field(min_length=1)
    phase: Phase
    shadow: bool = True
    raw_retention: str = Field(default="forever", pattern=RAW_RETENTION_PATTERN)
    contract: ContractSpec
    license: str = Field(min_length=1)
    include: list[str] = Field(default_factory=lambda: ["*.pdf"], min_length=1)
    exclude: list[str] = Field(default_factory=list)
    max_files: int | None = Field(default=None, ge=1)
    max_file_bytes: int = Field(default=50 * 1024 * 1024, ge=1)
    kaggle: KaggleSpec | None = None
    huggingface: HuggingFaceSpec | None = None
    http: HttpSpec | None = None
    local: LocalSpec | None = None

    @model_validator(mode="after")
    def _one_fetch_block(self) -> DocumentManifest:
        blocks = {
            "kaggle": self.kaggle,
            "huggingface": self.huggingface,
            "http": self.http,
            "local": self.local,
        }
        present = sorted(name for name, block in blocks.items() if block is not None)
        if present != [self.fetch]:
            raise ValueError(
                f"fetch is {self.fetch!r}, so exactly the {self.fetch!r} block must be set (found {present})"
            )
        return self

    @model_validator(mode="after")
    def _auth_fits_the_fetch(self) -> DocumentManifest:
        allowed = _AUTH_FOR_FETCH[self.fetch]
        if self.auth.type not in allowed:
            raise ValueError(
                f"fetch {self.fetch!r} takes auth.type {' or '.join(sorted(allowed))}, "
                f"not {self.auth.type!r}"
            )
        return self

    @model_validator(mode="after")
    def _a_token_beyond_dev_comes_from_vault(self) -> DocumentManifest:
        """Environment tokens (``KAGGLE_API_TOKEN``, ``HF_TOKEN``) are for dev sources only:
        beyond ``dev``, a token manifest names where the runner reads it (``auth.vault_path``).
        """
        if (
            self.auth.type in _TOKEN_AUTH
            and self.phase != "dev"
            and self.auth.vault_path is None
        ):
            raise ValueError(
                f"auth.type {self.auth.type!r} in phase {self.phase!r} needs auth.vault_path: "
                "a token from the environment is for dev sources only"
            )
        return self

    @model_validator(mode="after")
    def _sdk_fetches_are_dev_only(self) -> DocumentManifest:
        """The Kaggle and Hugging Face SDKs download on their own, outside the egress rule (the
        host allow-list and the pinned address): dev only, until they go through it."""
        if self.fetch in _SDK_FETCHES and self.phase != "dev":
            raise ValueError(
                f"fetch {self.fetch!r} is for the dev phase only, not {self.phase!r}: its SDK "
                "downloads outside the egress rule (the host allow-list and the pinned address)"
            )
        return self

    @model_validator(mode="after")
    def _phi_needs_a_baa(self) -> DocumentManifest:
        if self.phi and self.contract.baa is None:
            raise ValueError(
                "a PHI source needs a business associate agreement: set contract.baa"
            )
        return self

    @field_validator("schedule", mode="before")
    @classmethod
    def _the_old_schedule_string(cls, value: object, info: ValidationInfo) -> object:
        """Deprecated, for one release: ``schedule`` was a string before it took CN-01's shape,
        and manifests on disk still say ``schedule: on_demand``. That one reads as
        ``{class: F4}``, with a warning at every load; any other string is refused, and the error
        does not quote it."""
        if not isinstance(value, str):
            return value
        if value != _OLD_SCHEDULE:
            raise ValueError(
                "schedule is a mapping, {class: F1..F5}; of the old strings only "
                f"{_OLD_SCHEDULE} is still read, as {{class: F4}}"
            )
        logger.warning(
            "manifest %s: schedule: %s is deprecated and read as {class: F4} for this release "
            "only; write schedule: {class: F4}",
            info.data.get("id", "?"),  # validated already, so a manifest id (or absent)
            _OLD_SCHEDULE,
        )
        return {"class": "F4"}

    @field_validator("raw_retention")
    @classmethod
    def _at_most_ten_years(cls, value: str) -> str:
        # The pattern has already passed: "forever" or "<digits>d".
        if value != "forever" and int(value[:-1]) > MAX_RETENTION_DAYS:
            raise ValueError(f"raw_retention is at most {MAX_RETENTION_DAYS}d")
        return value

    @property
    def retention_class(self) -> ObjectClass:
        """The File Store object class the originals go to: documents, whatever the retention (the
        sidecar records ``raw_retention``; the lifecycle rule expires a class by it)."""
        return ObjectClass.DOCUMENTS

    def cleared_for(self, environment: str) -> bool:
        """Whether this manifest's phase lets it run in the service's environment."""
        return self.phase in _CLEARED_FOR.get(environment, frozenset())

    def wants(self, rel_path: str) -> bool:
        """Glob match on the path inside the source, case-insensitive; ``*`` also crosses folders."""
        path = rel_path.lower()
        included = any(fnmatchcase(path, pattern.lower()) for pattern in self.include)
        excluded = any(fnmatchcase(path, pattern.lower()) for pattern in self.exclude)
        return included and not excluded

    @classmethod
    def load(cls, path: Path | str) -> DocumentManifest:
        """Read and validate a manifest file. An OSError when the file cannot be read; a
        ManifestError, content-free, when what it holds is not a manifest (text that is not
        UTF-8 is a YAML error, the parser's ReaderError)."""
        raw = Path(path).read_bytes()
        try:
            data = yaml.safe_load(raw)
        except yaml.YAMLError as exc:
            raise ManifestError(f"manifest is not YAML: {_yaml_problem(exc)}") from None
        try:
            return cls.model_validate(data)
        except ValidationError as exc:
            problems = _problems(exc, cls.model_fields)
            raise ManifestError(f"manifest does not validate: {problems}") from None
