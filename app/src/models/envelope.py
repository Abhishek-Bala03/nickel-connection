"""Raw-data envelope: what the Transform layer needs to pick a message up from Kafka ``raw_data``.

v0.3 is a draft for CN-01; once the team agrees it, it moves to nickel-schemas, where JSON Schema is
the payload contract for Kafka topics. ``python -m src.cli schema envelope`` prints the JSON Schema
of the reader model (below). v0.3 adds three nullable fields of the agreed envelope (``member_id``,
``device_id``, ``fhir_release``), takes the contract's ingestion modes (pull, stream, listener,
upload, mobile; no push: Phase 1 has no inbound webhooks) and names the payload's sha256
``payload_hash``, as the contract does (v0.2 said ``payload_sha256``).

Two models, one shape:

- ``Envelope`` is the reader: what a consumer, and the republish relay reading a stored envelope,
  validate with. It is tolerant: it ignores fields it does not know (``extra="ignore"``), takes any
  ``0.x`` version (``^0\\.\\d+$``: an additive minor version must not be rejected by every older
  reader) and refuses another major, and it still reads a v0.2 envelope's ``payload_sha256`` as
  ``payload_hash``.
- ``ProducedEnvelope`` is what this service builds (the ingest bridge): strict. It forbids unknown
  fields, so a misspelled one is an error instead of a field silently dropped, it takes the old
  name as unknown, and it writes the current version only.

A message goes out in the current version. The relay publishes a stored v0.2 envelope as v0.3 (the
same values, ``payload_hash`` for ``payload_sha256`` and the new fields null), the one older version
it upgrades. A stored envelope newer than the version this service writes (``newer_than_written``:
written before a rollback) is never relabelled, which would drop the fields this reader does not
know: the relay sets it aside, and a pull of the item publishes an envelope it builds itself.

Two fields that look alike:

- ``fetch`` is how the payload was fetched (kaggle, huggingface, http, local, nango, ...): the
  connection layer's own business, and what v0.1 called ``route``.
- ``route`` is the lab route, L1 to L9, that the Transform layer's fidelity ranking (D-07) reads;
  the manifest says which. Specialty-lab PDFs are L7, the lowest tier.

``idempotency_key`` is the Transform layer's dedup key (D-00): sha256 over tenant, source, message
id and payload hash, NUL-separated. D-00 names (source, message id); the tenant is in because keys
are tenant-first, and the payload hash because a document source can serve new bytes under the same
path (a new version is a new receipt, and must not be dropped as a repeat). A retry of one receipt
keeps its key. The formula is held as it is until decision record 0001
(``docs/decisions/0001-idempotency-key.md``, Proposed) is decided: it recommends D-00's formula, with
the document connector's message id carrying the version (``path@<sha256>``), so that an HL7 resend
under the same control id stays a repeat.

``trace_id`` is the id of the run that published the message (a job's id, or a fresh one for a
command-line run) and ``attempt`` the receipt's publish-attempt count, this one included, so a repeat
delivery says so itself.
"""

from __future__ import annotations

import hashlib
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

ENVELOPE_VERSION = "0.3"
# What a reader takes: any 0.x. A minor version adds optional fields only, which a reader ignores;
# another major version is another contract.
READABLE_VERSION_PATTERN = r"^0\.\d+$"
SHA256_PATTERN = r"^[0-9a-f]{64}$"
ROUTE_PATTERN = r"^L[1-9]$"
TRACE_ID_PATTERN = r"^[0-9a-f]{32}$"  # the W3C trace-id form, as a UUID's hex is
# The contract's ingestion modes: pull (a connector fetches), stream (a long-lived feed such as SSE),
# listener (a protocol listener: HL7 MLLP, DICOM), upload (a file upload), mobile (the mobile edge).
Mode = Literal["pull", "stream", "listener", "upload", "mobile"]


def newer_than_written(version: str) -> bool:
    """Whether a readable (0.x) version is newer than the one this service writes. Such a message
    (stored before a rollback, say) must not go out again relabelled as this version: the fields
    this reader does not know would be dropped from it."""
    return int(version.split(".")[1]) > int(ENVELOPE_VERSION.split(".")[1])


def idempotency_key_of(
    tenant_id: str | UUID, source: str, message_id: str, payload_hash: str
) -> str:
    """The dedup key (D-00), tenant first: sha256 over tenant, source, message id and payload
    hash, NUL-separated, UTF-8. The same receipt gives the same key on every attempt."""
    parts = (str(tenant_id), source, message_id, payload_hash)
    return hashlib.sha256("\0".join(parts).encode("utf-8")).hexdigest()


class Quality(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)

    flags: list[str] = Field(
        default_factory=list,
        description="What a layer flagged about the message; the connection layer flags nothing.",
    )


class Envelope(BaseModel):
    """The reader (module docstring): tolerant of unknown fields and of any 0.x version."""

    model_config = ConfigDict(extra="ignore", frozen=True)

    envelope_version: str = Field(
        default=ENVELOPE_VERSION,
        pattern=READABLE_VERSION_PATTERN,
        description="The contract version the producer wrote: any 0.x reads; this service "
        f"writes {ENVELOPE_VERSION}.",
    )
    ingest_id: UUID = Field(
        description="UUIDv7 minted when the payload was received; the receipt's primary key."
    )
    tenant_id: UUID = Field(
        description="Owning tenant. Resolved by the platform, never read from the payload."
    )
    member_id: str | None = Field(
        default=None,
        description="The Nickel member the payload belongs to, when its producer knows it (the "
        "mobile edge knows its signed-in member). Null otherwise: identity resolution (Layer 3) "
        "decides. Always null from the document connector.",
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
    mode: Mode = Field(
        description="The ingestion mode: pull, stream, listener, upload or mobile."
    )
    connector: str
    connector_version: str
    connection_id: str | None = Field(
        default=None, description="Nango connection id; null for public datasets."
    )
    source_patient_id: str | None = Field(
        default=None,
        description="Patient id as the source knows it (PHI). Null when the source does not say.",
    )
    device_id: str | None = Field(
        default=None,
        description="The device a reading came from (a wearable, a CGM, a phone), as its source "
        "names it. Null for anything that is not device data: always, from the document "
        "connector.",
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
    fhir_release: str | None = Field(
        default=None,
        description="The FHIR release of a FHIR payload (R4, R4B, R5, ...), from its source. Null "
        "for anything that is not FHIR: always, from the document connector (PDFs).",
    )
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
    payload_hash: str = Field(
        pattern=SHA256_PATTERN,
        # A v0.2 envelope, stored on a receipt before the rename, says payload_sha256.
        validation_alias=AliasChoices("payload_hash", "payload_sha256"),
        description="sha256 of the payload's bytes (v0.2 called it payload_sha256).",
    )
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


class ProducedEnvelope(Envelope):
    """What this service builds and publishes (module docstring): strict. An unknown or misspelled
    field is an error, the v0.2 name ``payload_sha256`` is unknown here, and the version is the
    current one only."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    envelope_version: Literal["0.3"] = Field(
        default=ENVELOPE_VERSION,
        description="The version this service writes: always the current one.",
    )
    payload_hash: str = Field(
        pattern=SHA256_PATTERN, description="sha256 of the payload's bytes."
    )
