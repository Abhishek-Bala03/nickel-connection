"""Document source manifests: one YAML file per source (``manifests/documents/<id>.yaml``).

A subset of the connector manifest contract (CN-01: id, stream, mode, route, auth, schedule,
endpoints, rate_limit, formats, expectations, contract, phi, owner, phase, shadow, raw_retention),
with CN-01's nesting for the fields that exist here, so the shipped manifests survive the alignment
(``extra="forbid"`` rejects anything else): ``auth: {type, vault_path}`` and ``contract: {baa, dpa}``.
``endpoints``, ``rate_limit`` and ``expectations`` are not here yet; a document source has a source
block (``kaggle``, ``huggingface``, ``http`` or ``local``) in place of endpoints. The governance
fields are required, so CI rejects a manifest missing ``contract``, ``auth``, ``phi`` or ``owner``.

What this connector reads:

- ``fetch``: the transport, which decides the source block and the message ids.
- ``auth.type``: must fit ``fetch`` (``none`` for http and local, ``kaggle_token`` for kaggle,
  ``hf_token`` for a gated huggingface repo and ``none`` for a public one). Until CN-09's runner
  hands secrets over, the SDKs read the token from the environment themselves (``KAGGLE_API_TOKEN``,
  ``HF_TOKEN``).
- ``phi`` and ``contract.baa``: a PHI source without a business associate agreement is refused.
- ``phase``: how far the source is cleared. ``dev`` runs in development only, ``qa`` in development and
  qa, ``prod`` anywhere; the connector refuses a manifest the service's environment is not cleared for.
- ``shadow``: true stores the originals and the receipts but publishes nothing to ``raw_data`` (the
  receipt says the item is held), the "sandbox test > shadow mode > enable" flow of the Connections UI.
- ``raw_retention``: ``forever`` or ``<N>d`` (1 to 3650 days). Either way the originals go to the
  ``documents`` object class (``src.utility.keys``), which is in every object key and envelope; the
  value is recorded on each stored object's sidecar, and expiry is the File Store lifecycle rule's job
  (a PL task), not this connector's.
- ``max_file_bytes``, ``include``, ``exclude`` and ``max_files``: what a run takes.

Validated here, read elsewhere: ``auth.vault_path`` is where the connector runner (CN-09) will read
the credential (null for public data); ``owner`` is for the registrar and monitoring (CN-05); ``id``,
``stream``, ``mode``, ``description`` and ``license`` are registry data. ``route`` is the lab route
(L1-L9) the fidelity ranking reads, an envelope field, not a transport.
"""

from __future__ import annotations

from fnmatch import fnmatchcase
from pathlib import Path
from typing import Literal

import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    field_validator,
    model_validator,
)

from src.utility.keys import ObjectClass

from .sources import HTTPS_PORT

Fetch = Literal["kaggle", "huggingface", "http", "local"]
AuthType = Literal["none", "kaggle_token", "hf_token"]
Phase = Literal["dev", "qa", "prod"]
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
    schedule: str = "on_demand"
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
    def _phi_needs_a_baa(self) -> DocumentManifest:
        if self.phi and self.contract.baa is None:
            raise ValueError(
                "a PHI source needs a business associate agreement: set contract.baa"
            )
        return self

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
        data = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        return cls.model_validate(data)
