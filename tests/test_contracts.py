"""CN-01 artifacts, manifest drafts, and the producer's actual wire envelope."""

import copy
import json
import sqlite3
from datetime import UTC, datetime
from uuid import UUID

import pytest
import yaml

from src.contracts import EXAMPLE, envelope_schema, inspect_manifest, schema, validator
from src.db.manifest_drafts import ManifestDraftStore
from src.models.envelope import ProducedEnvelope


@pytest.fixture
def manifest():
    return yaml.safe_load(EXAMPLE.read_text())


@pytest.mark.parametrize(
    "path",
    [
        ("contract", "baa"),
        ("contract", "dpa"),
        ("auth", "vault_path"),
        ("phi",),
        ("owner",),
    ],
)
def test_ci_acceptance_fields_are_required(manifest, path):
    target = manifest
    for key in path[:-1]:
        target = target[key]
    del target[path[-1]]
    assert not validator(schema("manifest")).is_valid(manifest)
    _, errors = inspect_manifest(yaml.safe_dump(manifest))
    assert any(path[-1] in error["rule"] for error in errors)


def test_examples_and_semantic_constraints(manifest):
    assert validator(schema("manifest")).is_valid(manifest)
    # Required but nullable references are allowed for public development data.
    assert manifest["contract"] == {"baa": None, "dpa": None}
    for changes in [
        {"phi": True},
        {"owner": " "},
        {"adapter": {"id": "documents", "version": "latest"}},
        {"formats": {"validate": True, "schema_id": None}},
        {"auth": {"type": "oauth2", "vault_path": None}, "phase": "prod"},
        {"raw_retention": "3651d"},
        {"endpoints": ["https://secret@example.com/"]},
    ]:
        assert not validator(schema("manifest")).is_valid({**manifest, **changes})
    approved_refs = copy.deepcopy(manifest)
    approved_refs.update(phi=True, phase="prod")
    approved_refs["contract"]["baa"] = "agreement-123"
    approved_refs["auth"] = {
        "type": "oauth2",
        "vault_path": "secret/connection/reference",
    }
    assert validator(schema("manifest")).is_valid(approved_refs)


@pytest.mark.parametrize(
    "text",
    [
        "a: 1\na: 2",
        "x: &x [*x]",
        "!!python/object:secret {}",
        "x: .nan",
        "date: 2026-09-30",
        "[" * 30 + "0" + "]" * 30,
        "x" * 65537,
        "x: [",
        "? [x]\n: y",
    ],
)
def test_malformed_or_expansive_yaml_is_rejected_without_echoing(text):
    _, errors = inspect_manifest(text)
    assert errors and text not in json.dumps(errors)


def test_unknown_fields_are_not_echoed(manifest):
    manifest["do-not-log-secret"] = "secret-value"
    _, errors = inspect_manifest(yaml.safe_dump(manifest))
    assert "secret" not in json.dumps(errors)


def wire():
    return ProducedEnvelope(
        ingest_id="0192a3b4-0000-7000-8000-00000000000a",
        tenant_id="0192a3b4-0000-7000-8000-000000000001",
        source="sample",
        fetch="local",
        route="L7",
        mode="pull",
        connector="documents",
        connector_version="0.1.0",
        message_id="one",
        idempotency_key="ab" * 32,
        trace_id="cd" * 16,
        attempt=1,
        content_type="application/pdf",
        received_at=datetime.now(UTC),
        retention_class="documents",
        payload_hash="ab" * 32,
        payload_bytes=10,
        payload_ref="object/key",
    ).model_dump(mode="json")


def test_envelope_artifact_matches_producer_and_enforces_wire_rules():
    assert schema("envelope") == envelope_schema()
    check = validator(schema("envelope"))
    body = wire()
    check.validate(body)
    check.validate({**body, "payload_ref": None, "payload": {"entry": []}})
    for changes in [
        {"payload": {}},
        {"payload_ref": None},
        {"envelope_version": "0.2"},
        {"received_at": "2026-09-30T10:00:00"},
        {"tenant_id": "invalid"},
        {"payload_sha256": "ab" * 32},
        {"payload_bytes": 0},
    ]:
        assert not check.is_valid({**body, **changes}), changes
    for key in ["envelope_version", "payload_hash", "ingest_id"]:
        assert not check.is_valid({k: v for k, v in body.items() if k != key})


def test_drafts_validate_persist_audit_and_scope_by_tenant(service, settings):
    text = EXAMPLE.read_text()
    with service() as api:
        contracts = api.get("/contracts")
        assert contracts.status_code == 200
        assert contracts.json()["envelope_schema"] == envelope_schema()
        assert api.post(
            "/plugin-manifests/validate", json={"manifest": text}
        ).json() == {"valid": True, "errors": []}
        assert not api.post(
            "/plugin-manifests/validate", json={"manifest": "owner: team"}
        ).json()["valid"]
        assert (
            api.post("/plugin-manifests", json={"manifest": text}).status_code == 422
        )  # authenticated actor required
        headers = {"X-Authenticated-User": "author"}
        assert (
            api.post(
                "/plugin-manifests", json={"manifest": "owner: team"}, headers=headers
            ).status_code
            == 400
        )
        created = api.post(
            "/plugin-manifests", json={"manifest": text}, headers=headers
        )
        assert created.status_code == 200, created.text
        draft = created.json()
        assert (
            draft["version"] == 1
            and draft["status"] == "draft"
            and draft["changed_by"] == "author"
        )
        assert (
            api.post(
                "/plugin-manifests", json={"manifest": text}, headers=headers
            ).status_code
            == 409
        )
        assert (
            api.post(
                "/plugin-manifests",
                json={"manifest": text, "version": 1, "draft_id": "other"},
                headers=headers,
            ).status_code
            == 400
        )
        updated = api.post(
            "/plugin-manifests",
            json={"manifest": text, "version": 1, "draft_id": draft["id"]},
            headers={"X-Authenticated-User": "reviewer"},
        )
        assert updated.json()["version"] == 2
        assert api.get("/plugin-manifests").json() == [updated.json()]
        # A saved manifest is never a runnable plugin or an implicit approval.
        assert "public-documents" not in [
            p["id"] for p in api.get("/connectors/catalog").json()
        ]
        assert (
            api.post("/plugins/public-documents/provision", json={}).status_code == 400
        )
    with service(tenant_id=UUID("0192a3b4-0000-7000-8000-000000000002")) as api:
        assert api.get("/plugin-manifests").json() == []
    with sqlite3.connect(settings.data_dir / "connectors.sqlite") as db:
        assert db.execute(
            "SELECT version,changed_by FROM manifest_draft_audit ORDER BY version"
        ).fetchall() == [(1, "author"), (2, "reviewer")]


def test_store_survives_reopen(tmp_path):
    path = tmp_path / "drafts.sqlite"
    store = ManifestDraftStore(path)
    expected = store.save("tenant", "id", "manifest", 0, "actor")
    assert ManifestDraftStore(path).list("tenant") == [expected]
