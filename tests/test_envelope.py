import json
from datetime import UTC, datetime
from uuid import UUID

import pytest
from pydantic import ValidationError

from src.models.envelope import Envelope, Quality, idempotency_key_of

TENANT = "0192a3b4-0000-7000-8000-000000000001"
BASE = {
    "ingest_id": "0192a3b4-0000-7000-8000-00000000000a",
    "tenant_id": TENANT,
    "source": "test-docs",
    "fetch": "local",
    "route": "L7",
    "mode": "pull",
    "connector": "documents",
    "connector_version": "0.1.0",
    "message_id": "local:docs/a.pdf",
    "idempotency_key": "cd" * 32,
    "trace_id": "0192a3b40000700080000000000000ab",
    "attempt": 1,
    "content_type": "application/pdf",
    "received_at": datetime(2026, 9, 22, tzinfo=UTC),
    "retention_class": "documents",
    "payload_sha256": "ab" * 32,
    "payload_bytes": 10,
}


def test_pointer_envelope_round_trips_through_json():
    envelope = Envelope(**BASE, payload_ref="t/documents/ab/abab")
    data = envelope.model_dump(mode="json")
    assert data["envelope_version"] == "0.2"
    assert Envelope.model_validate(data) == envelope


def test_small_payloads_can_travel_inline():
    assert Envelope(**BASE, payload={"a": 1}).payload == {"a": 1}


def test_exactly_one_of_pointer_or_inline_payload():
    with pytest.raises(ValidationError, match="exactly one"):
        Envelope(**BASE)
    with pytest.raises(ValidationError, match="exactly one"):
        Envelope(**BASE, payload_ref="k", payload={"a": 1})


def test_naive_timestamps_are_rejected():
    naive = {**BASE, "received_at": datetime(2026, 9, 22), "payload_ref": "k"}
    with pytest.raises(ValidationError, match="timezone"):
        Envelope(**naive)


def test_the_nullable_fields_default_to_null_and_quality_to_no_flags():
    data = Envelope(**BASE, payload_ref="k").model_dump(mode="json")
    # Always null from the document connector: public datasets have no Nango connection, no
    # patient id and no event time (docs/document-ingestion.md says so).
    assert (data["connection_id"], data["source_patient_id"], data["event_time"]) == (
        None,
        None,
        None,
    )
    # Nullable by contract: a file by pointer has no registered schema, a public dataset no consent.
    assert (data["schema_id"], data["consent_ref"]) == (None, None)
    assert data["quality"] == {"flags": []}
    flagged = Envelope(
        **BASE, payload_ref="k", quality={"flags": ["ocr_low_confidence"]}
    )
    assert flagged.quality == Quality(flags=["ocr_low_confidence"])


def test_a_v0_1_line_does_not_validate():
    """v0.1 said ``route`` for the transport. A v0.2 reader must not take ``local`` for a lab route,
    nor accept a line without the dedup key, the run and the attempt; there were no consumers.
    """
    per_run = ("fetch", "mode", "idempotency_key", "trace_id", "attempt")
    v01 = {k: v for k, v in BASE.items() if k not in per_run}
    v01.update(envelope_version="0.1", route="local", payload_ref="k")
    with pytest.raises(ValidationError) as caught:
        Envelope(**v01)
    wrong = {error["loc"][0] for error in caught.value.errors()}
    assert {"envelope_version", "route", *per_run} <= wrong


@pytest.mark.parametrize(
    "field, bad",
    [
        ("route", "local"),
        ("route", "L0"),
        ("route", "L10"),
        ("route", "l7"),
        ("mode", "batch"),
        ("idempotency_key", "cd" * 31),
        ("idempotency_key", "CD" * 32),
        # The 32-hex form (a UUID's .hex), not the hyphenated UUID, and not shorter.
        ("trace_id", "0192a3b4-0000-7000-8000-00000000000a"),
        ("trace_id", "ab" * 15),
        ("attempt", 0),
    ],
)
def test_the_contract_fields_are_checked(field, bad):
    with pytest.raises(ValidationError, match=field):
        Envelope(**{**BASE, field: bad}, payload_ref="k")


def test_the_idempotency_key_is_tenant_first_over_source_message_id_and_payload_hash():
    key = idempotency_key_of(TENANT, "test-docs", "local:docs/a.pdf", "ab" * 32)
    # sha256 of "<tenant>\0test-docs\0local:docs/a.pdf\0abab..." (UTF-8), computed once by hand.
    assert key == "021a8699c4c0feba8adfe9e924b19e88cb39caa9bcf71548eb0af61db9d7af80"
    assert (
        idempotency_key_of(UUID(TENANT), "test-docs", "local:docs/a.pdf", "ab" * 32)
        == key
    )
    other_tenant = "0192a3b4-0000-7000-8000-000000000002"
    assert (
        idempotency_key_of(other_tenant, "test-docs", "local:docs/a.pdf", "ab" * 32)
        != key
    )
    # New bytes under the same path are a new version, not a repeat.
    assert idempotency_key_of(TENANT, "test-docs", "local:docs/a.pdf", "cd" * 32) != key
    # NUL-separated: a character moved across a boundary changes the key.
    assert idempotency_key_of(TENANT, "test-doc", "slocal:docs/a.pdf", "ab" * 32) != key
    assert (
        Envelope(**{**BASE, "idempotency_key": key}, payload_ref="k").idempotency_key
        == key
    )


def test_a_reader_ignores_fields_it_does_not_know_yet():
    """A newer producer may add an optional field; a reader on this contract must not reject the
    message for it (``extra`` is ignored, not forbidden). The same rule applies when building.
    """
    envelope = Envelope(**BASE, payload_ref="k")
    data = {**envelope.model_dump(mode="json"), "new_optional_field": "x"}
    data["quality"] = {**data["quality"], "score": 0.9}  # the nested block too
    assert Envelope.model_validate(data) == envelope
    assert Envelope.model_validate_json(json.dumps(data)) == envelope
    assert Envelope(**BASE, payload_ref="k", new_optional_field="x") == envelope
    assert "new_optional_field" not in envelope.model_dump()
    assert Envelope.model_json_schema().get("additionalProperties") is not False


def test_json_schema_is_exportable():
    schema = Envelope.model_json_schema()
    assert "payload_sha256" in schema["properties"]
    assert schema["properties"]["route"]["pattern"] == "^L[1-9]$"
    assert schema["properties"]["trace_id"]["pattern"] == "^[0-9a-f]{32}$"
    assert schema["$defs"]["Quality"]["properties"]["flags"]["type"] == "array"
