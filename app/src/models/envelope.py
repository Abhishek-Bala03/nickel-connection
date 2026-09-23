"""Raw-data envelope: what the Transform layer needs to pick a message up from Kafka ``raw_data``.

v0.2 is a draft for CN-01; v0.1 had no consumers, and a v0.1 line does not validate here. Once the
team agrees it, it moves to nickel-schemas, where JSON Schema is the payload contract for Kafka
topics. ``python -m src.cli schema envelope`` prints the JSON Schema.

Two fields that look alike:

- ``fetch`` is how the payload was fetched (kaggle, huggingface, http, local, nango, ...): the
  connection layer's own business, and what v0.1 called ``route``.
- ``route`` is the lab route, L1 to L9, that the Transform layer's fidelity ranking (D-07) reads;
  the manifest says which. Specialty-lab PDFs are L7, the lowest tier.

``idempotency_key`` is the Transform layer's dedup key (D-00): sha256 over tenant, source, message
id and payload hash, NUL-separated. D-00 names (source, message id); the tenant is in because keys
are tenant-first, and the payload hash because a document source can serve new bytes under the same
path (a new version is a new receipt, and must not be dropped as a repeat). A retry of one receipt
keeps its key. ``trace_id`` is the id of the run that published the message (a job's id, or a fresh
one for a command-line run) and ``attempt`` the receipt's publish-attempt count, this one included,
so a repeat delivery says so itself.

A reader ignores fields it does not know (``extra="ignore"``), so a producer can add an optional field
without every consumer rejecting the first message that carries it. The same class builds envelopes,
so building is not strict either: the bridge's tests pin every field it sets, which is where a typo
would show.
"""

from __future__ import annotations

import hashlib
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

ENVELOPE_VERSION = "0.2"
SHA256_PATTERN = r"^[0-9a-f]{64}$"
ROUTE_PATTERN = r"^L[1-9]$"
TRACE_ID_PATTERN = r"^[0-9a-f]{32}$"  # the W3C trace-id form, as a UUID's hex is
Mode = Literal["pull", "stream", "push"]


def idempotency_key_of(
    tenant_id: str | UUID, source: str, message_id: str, payload_sha256: str
) -> str:
    """The dedup key (D-00), tenant first: sha256 over tenant, source, message id and payload
    hash, NUL-separated, UTF-8. The same receipt gives the same key on every attempt."""
    parts = (str(tenant_id), source, message_id, payload_sha256)
    return hashlib.sha256("\0".join(parts).encode("utf-8")).hexdigest()


class Quality(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)

    flags: list[str] = Field(
        default_factory=list,
        description="What a layer flagged about the message; the connection layer flags nothing.",
    )


class Envelope(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)

    envelope_version: Literal["0.2"] = ENVELOPE_VERSION
    ingest_id: UUID = Field(
        description="UUIDv7 minted when the payload was received; the receipt's primary key."
    )
    tenant_id: UUID = Field(
        description="Owning tenant. Resolved by the platform, never read from the payload."
    )
    source: str = Field(
        description="Manifest id of the source, e.g. 'hf-medical-lab-reports'."
    )
    fetch: str = Field(
        description="How the payload was fetched: kaggle, huggingface, http, local, nango, ..."
    )
    route: str = Field(
        pattern=ROUTE_PATTERN,
        description="The lab route (L1-L9) the fidelity ranking reads; from the manifest. "
        "Specialty-lab PDFs are L7.",
    )
    mode: Mode = Field(description="The manifest's mode: pull, stream or push.")
    connector: str
    connector_version: str
    connection_id: str | None = Field(
        default=None, description="Nango connection id; null for public datasets."
    )
    source_patient_id: str | None = Field(
        default=None,
        description="Patient id as the source knows it (PHI). Null when the source does not say.",
    )
    message_id: str = Field(
        description="Stable id of the item at the source; with source, the downstream dedup key."
    )
    idempotency_key: str = Field(
        pattern=SHA256_PATTERN,
        description="sha256 over tenant, source, message id and payload hash (D-00); a retry "
        "of the same receipt keeps it.",
    )
    trace_id: str = Field(
        pattern=TRACE_ID_PATTERN,
        description="The run that published this message: a job's id, or a fresh id for a "
        "command-line run.",
    )
    attempt: int = Field(
        ge=1, description="The receipt's publish-attempt count, this one included."
    )
    content_type: str
    received_at: datetime = Field(
        description="When the receipt was first written; a retry keeps it."
    )
    event_time: datetime | None = Field(
        default=None, description="When the source says the item was produced."
    )
    retention_class: str = Field(
        description="File Store object class; decides how long the original is kept."
    )
    schema_id: str | None = Field(
        default=None,
        description="Registered schema of an inline payload; null for a file by pointer.",
    )
    consent_ref: str | None = Field(
        default=None,
        description="The consent the payload came under; null for public datasets.",
    )
    quality: Quality = Field(default_factory=Quality)
    payload_sha256: str = Field(pattern=SHA256_PATTERN)
    payload_bytes: int = Field(ge=1)
    payload_ref: str | None = Field(
        default=None,
        description="File Store object key when the payload goes by pointer.",
    )
    payload: dict[str, Any] | None = Field(
        default=None, description="Inline JSON payload for small messages."
    )
    source_meta: dict[str, str | int | None] = Field(
        default_factory=dict,
        description="Where exactly the item came from: dataset, path, licence, URL.",
    )

    @field_validator("received_at", "event_time")
    @classmethod
    def _timezone_aware(cls, value: datetime | None) -> datetime | None:
        if value is not None and value.utcoffset() is None:
            raise ValueError("timestamps must be timezone-aware")
        return value

    @model_validator(mode="after")
    def _exactly_one_payload(self) -> Envelope:
        if (self.payload_ref is None) == (self.payload is None):
            raise ValueError("exactly one of payload_ref or payload must be set")
        return self
