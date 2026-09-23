"""The republish relay (docs/document-ingestion.md, "Republishing open receipts"): open receipts go
out again from the envelopes they store, without their sources, as a job, from the command line, or
through ``documents.republish_documents``."""

import dataclasses
import io
import json
import logging
import shutil
import sqlite3
import time
import types
from uuid import UUID

import pytest

from src.cli import main
from src.connectors.documents import DocumentConnector, DocumentManifest
from src.db import receipts as receipts_module
from src.db.jobs import Job, JobStatus, JobStore
from src.db.layout import DataLayout
from src.db.outbox import JsonlOutbox
from src.db.receipts import Receipt, ReceiptStatus, ReceiptStore
from src.handler import documents
from src.handler.ingest import IngestBridge
from src.handler.jobs import JobResult, JobRunner, build_runner

STAMP = "2026-09-22T00:00:00+00:00"
TRACE = "0192a3b40000700080000000000000ee"


def _outbox(settings):
    return JsonlOutbox(DataLayout(settings.data_dir).outbox)


def _receipts(settings):
    return ReceiptStore(DataLayout(settings.data_dir).receipts_db)


def _pull(settings, folder, manifest_writer, manifest_id="local-docs", **over):
    manifest_writer(folder, settings.manifest_dir, manifest_id=manifest_id, **over)
    manifest = documents.load_manifest(settings, manifest_id)
    return documents.pull_documents(settings, manifest)


@pytest.fixture
def blocked_outbox(settings):
    """A file where the outbox folder should be, so every publish fails; returns the unblocker."""
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    blocker = settings.data_dir / "outbox"
    blocker.write_text("a file where the outbox folder should be")
    return blocker.unlink


def test_a_failed_publish_is_republished_later_without_the_source(
    settings, pdf_folder, manifest_writer, blocked_outbox
):
    pull = _pull(settings, pdf_folder, manifest_writer)
    assert pull.error == "publish failed; the pull stopped"
    with _receipts(settings) as receipts:
        [failed] = receipts.recent(tenant_id=str(settings.tenant_id))
    assert (failed.status, failed.attempts) == (ReceiptStatus.PUBLISH_FAILED, 1)

    blocked_outbox()  # the broker is back
    shutil.rmtree(pdf_folder)  # and the source is gone: the stored envelope is enough
    summary = documents.republish_documents(settings, trace_id=TRACE)
    assert summary.counts() == {
        "open_receipts": 1,
        "published": 1,
        "duplicates": 0,
        "held_back": 0,
        "bad_envelope": 0,
        "no_envelope": 0,
    }
    assert summary.error is None
    [message] = _outbox(settings).read("raw_data")
    envelope = message["value"]
    assert message["id"] == envelope["ingest_id"] == failed.ingest_id
    assert (envelope["attempt"], envelope["trace_id"]) == (2, TRACE)
    assert envelope["received_at"] == failed.received_at.replace("+00:00", "Z")
    with _receipts(settings) as receipts:
        published = receipts.get(failed.ingest_id, tenant_id=str(settings.tenant_id))
    assert (published.status, published.attempts) == (ReceiptStatus.PUBLISHED, 2)
    # Nothing open any more: the next run has nothing to do.
    assert documents.republish_documents(settings).counts()["open_receipts"] == 0


def test_held_items_wait_until_their_manifest_is_enabled(
    settings, pdf_folder, pdf_bytes, manifest_writer
):
    (pdf_folder / "b.pdf").write_bytes(pdf_bytes + b"% b\n")
    assert _pull(settings, pdf_folder, manifest_writer, shadow=True).held == 2
    # Still in shadow mode: held back (the manifest is read once per source, not per receipt).
    summary = documents.republish_documents(settings)
    assert (summary.open_receipts, summary.held_back, summary.published) == (2, 2, 0)
    # Enabled, but not cleared for this environment (a dev source in production): held back.
    manifest_writer(pdf_folder, settings.manifest_dir, shadow=False, phase="dev")
    production = dataclasses.replace(settings, environment="production")
    assert documents.republish_documents(production).held_back == 2
    # No manifest any more: held back.
    (settings.manifest_dir / "local-docs.yaml").unlink()
    assert documents.republish_documents(settings).held_back == 2
    assert _outbox(settings).read("raw_data") == []
    # Enabled: out they go, under the ingest ids they were held under.
    manifest_writer(pdf_folder, settings.manifest_dir, shadow=False)
    summary = documents.republish_documents(settings, trace_id=TRACE)
    assert (summary.held_back, summary.published) == (0, 2)
    with _receipts(settings) as receipts:
        held = receipts.recent(tenant_id=str(settings.tenant_id))
    assert {m["value"]["ingest_id"] for m in _outbox(settings).read("raw_data")} == {
        r.ingest_id for r in held
    }
    assert {(r.status, r.detail) for r in held} == {(ReceiptStatus.PUBLISHED, None)}


def test_a_receipt_another_run_published_meanwhile_is_a_duplicate_for_the_relay(
    settings, pdf_folder, manifest_writer, blocked_outbox, monkeypatch
):
    _pull(settings, pdf_folder, manifest_writer)
    blocked_outbox()
    real_republish = IngestBridge.republish

    def other_run_first(self, receipt, trace_id):
        # Between this run's listing and its publish, another run publishes the receipt.
        real_republish(self, receipt, "0192a3b40000700080000000000000ff")
        return real_republish(self, receipt, trace_id)

    monkeypatch.setattr(IngestBridge, "republish", other_run_first)
    summary = documents.republish_documents(settings, trace_id=TRACE)
    assert (summary.open_receipts, summary.published, summary.duplicates) == (1, 0, 1)
    [message] = _outbox(settings).read("raw_data")  # the other run's, and no second one
    assert message["value"]["trace_id"] == "0192a3b40000700080000000000000ff"


def test_a_receipt_without_a_stored_envelope_is_counted_not_guessed_at(
    settings, tmp_path, manifest_writer
):
    manifest_writer(tmp_path, settings.manifest_dir)  # local-docs, enabled
    with _receipts(settings) as receipts:
        receipts.add(
            Receipt(
                ingest_id="0192a3b4-0000-7000-8000-0000000000aa",
                tenant_id=str(settings.tenant_id),
                source="local-docs",
                message_id="m",
                payload_sha256="ab" * 32,
                status=ReceiptStatus.RECEIVED,
                received_at=STAMP,
                updated_at=STAMP,
                payload_ref="k",
            )
        )
        # Another tenant's open receipt in the same file is not this deployment's to publish
        # (its "envelope" would not even validate: the relay must never reach it).
        receipts.add(
            Receipt(
                ingest_id="0192a3b4-0000-7000-8000-0000000000bb",
                tenant_id="0192a3b4-0000-7000-8000-000000000002",
                source="local-docs",
                message_id="m",
                payload_sha256="ab" * 32,
                status=ReceiptStatus.RECEIVED,
                received_at=STAMP,
                updated_at=STAMP,
                payload_ref="k",
                envelope="{}",
            )
        )
    summary = documents.republish_documents(settings)
    assert (summary.open_receipts, summary.no_envelope, summary.published) == (
        1,
        1,
        0,
    )
    assert _outbox(settings).read("raw_data") == []


def test_too_large_receipts_are_not_the_relays_business(
    settings, pdf_folder, manifest_writer
):
    # No stored original, so nothing to point an envelope at: the source must list it again.
    # Both kinds: the bridge's own check (a payload in hand, as a pushed upload would meet it)
    # and a pull's receipt for a file over the cap at the source (metadata only).
    manifest_writer(pdf_folder, settings.manifest_dir)
    manifest = documents.load_manifest(settings, "local-docs")
    capped = dataclasses.replace(settings, max_file_bytes=10)
    with documents.ingest_bridge(capped) as bridge:
        connector = DocumentConnector(
            manifest, bridge, environment="development", max_file_bytes=10**6
        )
        assert connector.run().too_large == 1
    big = pdf_folder.parent / "big"
    big.mkdir()
    (big / "b.pdf").write_bytes(b"%PDF-" + b"x" * 100)
    manifest_writer(big, settings.manifest_dir, manifest_id="big-docs")
    documents.pull_documents(capped, documents.load_manifest(settings, "big-docs"))
    assert documents.receipt_counts(settings) == {"too_large": 2}
    assert documents.republish_documents(settings).counts()["open_receipts"] == 0


def test_a_publish_failure_stops_the_republish_and_the_receipts_say_so(
    settings, pdf_folder, manifest_writer, blocked_outbox
):
    other = pdf_folder.parent / "other"
    shutil.copytree(pdf_folder, other)
    _pull(settings, pdf_folder, manifest_writer)
    _pull(settings, other, manifest_writer, manifest_id="other-docs")
    summary = documents.republish_documents(settings)
    assert summary.error == "publish failed; the republish stopped"
    # Two were open; it stopped at the first (the oldest) and never reached the second.
    assert (summary.open_receipts, summary.published) == (2, 0)
    assert any(line.startswith("ABORTED") for line in summary.lines())
    with _receipts(settings) as receipts:
        attempts = {
            r.source: (r.status, r.attempts)
            for r in receipts.recent(tenant_id=str(settings.tenant_id))
        }
    assert attempts == {
        "local-docs": (ReceiptStatus.PUBLISH_FAILED, 2),
        "other-docs": (ReceiptStatus.PUBLISH_FAILED, 1),
    }
    blocked_outbox()
    assert documents.republish_documents(settings).published == 2


def test_the_source_filter_and_the_limit(
    settings, pdf_folder, manifest_writer, blocked_outbox
):
    for name in ("b-docs", "c-docs"):
        folder = pdf_folder.parent / name
        shutil.copytree(pdf_folder, folder)
        _pull(settings, folder, manifest_writer, manifest_id=name)
    _pull(settings, pdf_folder, manifest_writer, manifest_id="a-docs")
    blocked_outbox()
    one = documents.republish_documents(settings, source="c-docs")
    assert (one.source, one.open_receipts, one.published) == ("c-docs", 1, 1)
    assert "source          c-docs" in one.lines()
    capped = documents.republish_documents(settings, limit=1)
    assert (capped.open_receipts, capped.published) == (2, 1)  # one of the two left
    assert [m["value"]["source"] for m in _outbox(settings).read("raw_data")] == [
        "c-docs",
        "b-docs",  # oldest first
    ]
    rest = documents.republish_documents(settings)
    assert (rest.source, rest.published) == (None, 1)
    assert "source          all" in rest.lines()


def test_the_republish_job_runs_and_reports_its_counts(
    service, settings, pdf_folder, manifest_writer, blocked_outbox, wait_for
):
    _pull(settings, pdf_folder, manifest_writer)
    blocked_outbox()
    with service() as client:
        response = client.post("/jobs/documents/republish", json={})
        assert response.status_code == 202
        job = wait_for(client, response.json()["job_id"])
        assert job["status"] == "succeeded"
        assert job["kind"] == "documents.republish"
        assert job["params"] == {"source": None, "limit": None}
        assert job["counts"] == {
            "open_receipts": 1,
            "published": 1,
            "duplicates": 0,
            "held_back": 0,
            "bad_envelope": 0,
            "no_envelope": 0,
        }
        assert job["error"] is None
        [document] = client.get("/documents").json()["documents"]
        assert document["status"] == "published"
    [message] = _outbox(settings).read("raw_data")
    assert message["value"]["trace_id"] == UUID(job["job_id"]).hex


def test_a_republish_job_that_cannot_publish_fails_with_its_counts(
    service, settings, pdf_folder, manifest_writer, blocked_outbox, wait_for
):
    _pull(settings, pdf_folder, manifest_writer)
    with service() as client:
        job_id = client.post(
            "/jobs/documents/republish", json={"source": "local-docs", "limit": 5}
        ).json()["job_id"]
        job = wait_for(client, job_id)
    assert (job["status"], job["error"]) == (
        "failed",
        "publish failed; the republish stopped",
    )
    assert job["params"] == {"source": "local-docs", "limit": 5}
    assert job["counts"]["open_receipts"] == 1


@pytest.mark.parametrize(
    "body", [{"source": "Bad ID"}, {"limit": 0}, {"surprise": 1}, {"source": ""}]
)
def test_a_malformed_republish_request_is_refused(service, body):
    with service() as client:
        response = client.post("/jobs/documents/republish", json=body)
        assert response.status_code == 422
        assert response.json()["error"]["code"] == "validation_error"


def test_a_republish_needs_a_tenant(service):
    with service(tenant_id=None) as client:
        response = client.post("/jobs/documents/republish", json={})
        assert response.status_code == 503
        assert response.json()["error"]["code"] == "unavailable"
        assert client.get("/jobs").json() == {"jobs": []}


def test_republish_from_the_command_line(
    settings, pdf_folder, manifest_writer, blocked_outbox
):
    _pull(settings, pdf_folder, manifest_writer)
    env = {
        "NICKEL_TENANT_ID": str(settings.tenant_id),
        "NICKEL_DATA_DIR": str(settings.data_dir),
        "NICKEL_MANIFEST_DIR": str(settings.manifest_dir),  # the source must be enabled
    }
    out = io.StringIO()
    assert main(["documents", "republish", "--source", "local-docs"], env, out) == 1
    assert "ABORTED         publish failed; the republish stopped" in out.getvalue()
    blocked_outbox()
    out = io.StringIO()
    assert main(["documents", "republish", "--limit", "10"], env, out) == 0
    assert "published       1" in out.getvalue()
    assert "trace_id        " in out.getvalue()


# ── The source rules apply to every open receipt ─────────────────────────────
def _left_received(settings, folder, manifest_writer, monkeypatch):
    """The process dies between the receipt write and the publish: the receipt says received."""

    def power_cut(self, *args, **kwargs):
        raise RuntimeError("power cut")

    with monkeypatch.context() as patch:
        patch.setattr(JsonlOutbox, "publish", power_cut)
        with pytest.raises(RuntimeError, match="power cut"):
            _pull(settings, folder, manifest_writer)


def _left_publish_failed(settings, folder, manifest_writer, monkeypatch):
    """The broker is down during the pull: the receipt says publish_failed."""
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    blocker = settings.data_dir / "outbox"
    blocker.write_text("a file where the outbox folder should be")
    assert _pull(settings, folder, manifest_writer).error == (
        "publish failed; the pull stopped"
    )
    blocker.unlink()  # the broker is back


def _manifest_removed(settings, folder, manifest_writer):
    (settings.manifest_dir / "local-docs.yaml").unlink()
    return settings


def _back_in_shadow(settings, folder, manifest_writer):
    manifest_writer(folder, settings.manifest_dir, shadow=True)
    return settings


def _not_cleared_in_production(settings, folder, manifest_writer):
    manifest_writer(folder, settings.manifest_dir, phase="dev")
    return dataclasses.replace(settings, environment="production")


@pytest.mark.parametrize(
    "leave_open",
    [_left_received, _left_publish_failed],
    ids=["received", "publish_failed"],
)
@pytest.mark.parametrize(
    "revoke",
    [_manifest_removed, _back_in_shadow, _not_cleared_in_production],
    ids=["manifest removed", "back in shadow", "phase not cleared in production"],
)
def test_an_open_receipt_of_a_source_that_is_not_enabled_is_held_back_never_sent(
    settings, tmp_path, pdf_bytes, manifest_writer, monkeypatch, leave_open, revoke
):
    """The review's probe: a source revoked after a failed publish, and the relay in production
    still sent its Jane_Doe.pdf. Every open status obeys the rules a pull obeys: the manifest is
    found, cleared for this environment and out of shadow mode, or the receipt waits."""
    folder = tmp_path / "clinic-drop"
    folder.mkdir()
    (folder / "Jane_Doe.pdf").write_bytes(pdf_bytes)
    leave_open(settings, folder, manifest_writer, monkeypatch)
    tenant = str(settings.tenant_id)
    with _receipts(settings) as receipts:
        [left] = receipts.recent(tenant_id=tenant)
    assert left.status in (ReceiptStatus.RECEIVED, ReceiptStatus.PUBLISH_FAILED)

    relay_settings = revoke(settings, folder, manifest_writer)
    summary = documents.republish_documents(relay_settings, trace_id=TRACE)
    assert summary.counts() == {
        "open_receipts": 1,
        "published": 0,
        "duplicates": 0,
        "held_back": 1,
        "bad_envelope": 0,
        "no_envelope": 0,
    }
    assert summary.error is None
    assert _outbox(settings).read("raw_data") == []
    with _receipts(settings) as receipts:
        waiting = receipts.get(left.ingest_id, tenant_id=tenant)
    assert (waiting.status, waiting.attempts) == (left.status, left.attempts)

    # Enabled again (a prod manifest, out of shadow): it goes out under its own ingest id.
    manifest_writer(folder, settings.manifest_dir)
    assert documents.republish_documents(relay_settings).published == 1
    [message] = _outbox(settings).read("raw_data")
    assert message["id"] == left.ingest_id


@pytest.mark.parametrize(
    "broken", ["not YAML", "invalid", "unreadable", "another source's"]
)
def test_a_manifest_that_cannot_be_read_or_is_another_sources_holds_its_receipts_back(
    settings, pdf_folder, manifest_writer, blocked_outbox, records, monkeypatch, broken
):
    """A broken manifest stops only its own source's receipts, and says so in the log by the
    source id and what is wrong, content-free: never the file's text, which pydantic would quote,
    or the OS's, which quotes the path.
    """
    _pull(settings, pdf_folder, manifest_writer)
    blocked_outbox()
    path = settings.manifest_dir / "local-docs.yaml"
    if broken == "not YAML":
        path.write_text("id: [local-docs\n", encoding="utf-8")
    elif broken == "invalid":
        manifest_writer(pdf_folder, settings.manifest_dir, license="")
    elif broken == "unreadable":

        def denied(cls, path):
            raise PermissionError(13, "Permission denied", "/manifests/Jane_Doe.yaml")

        monkeypatch.setattr(DocumentManifest, "load", classmethod(denied))
    else:  # a file under this source's name that is another source's manifest
        manifest_writer(pdf_folder, settings.manifest_dir, manifest_id="other-docs")
        (settings.manifest_dir / "other-docs.yaml").replace(path)
    summary = documents.republish_documents(settings)
    assert (summary.held_back, summary.published, summary.error) == (1, 0, None)
    warnings = [r.getMessage() for r in records if r.levelno == logging.WARNING]
    expected = {
        "not YAML": ["manifest is not YAML: ParserError at line 2, column 1"],
        "invalid": ["manifest does not validate: license string_too_short"],
        "unreadable": ["the manifest could not be read (PermissionError)"],
        "another source's": [],
    }[broken]
    assert warnings == [
        f"republish: local-docs: {problem}; its receipts wait" for problem in expected
    ]


def test_a_bad_stored_envelope_is_set_aside_and_the_healthy_one_behind_it_goes_out(
    settings, tmp_path, pdf_bytes, manifest_writer, records
):
    """The review's probe: one stored envelope that did not validate raised out of the relay, on
    every run, and everything behind it stayed stuck. A corrupt one and an older-version one sit in
    front of a healthy one here: both are set aside with a content-free reason on the receipt, and
    the healthy one is published."""
    folder = tmp_path / "drop"
    folder.mkdir()
    for name in ("a-Jane_Doe_MRN12345.pdf", "b-John_Roe.pdf", "c-healthy.pdf"):
        (folder / name).write_bytes(pdf_bytes + name.encode())
    assert _pull(settings, folder, manifest_writer, shadow=True).held == 3
    tenant = str(settings.tenant_id)
    with _receipts(settings) as receipts:
        corrupt, older, healthy = reversed(receipts.recent(tenant_id=tenant))
    # What v0.1 looked like: `route` said the transport, and the run fields did not exist.
    v01 = {
        key: value
        for key, value in json.loads(older.envelope).items()
        if key not in ("fetch", "mode", "idempotency_key", "trace_id", "attempt")
    }
    v01.update(envelope_version="0.1", route="local")
    with sqlite3.connect(DataLayout(settings.data_dir).receipts_db) as db:
        update = "UPDATE ingest_receipts SET envelope = ? WHERE ingest_id = ?"
        db.execute(update, ('{"ingest_id": "0192', corrupt.ingest_id))  # cut short
        db.execute(update, (json.dumps(v01), older.ingest_id))
    manifest_writer(folder, settings.manifest_dir)  # out of shadow: all three may go

    summary = documents.republish_documents(settings, trace_id=TRACE)
    assert summary.counts() == {
        "open_receipts": 3,
        "published": 1,
        "duplicates": 0,
        "held_back": 0,
        "bad_envelope": 2,
        "no_envelope": 0,
    }
    assert summary.error is None
    [message] = _outbox(settings).read("raw_data")
    assert message["id"] == healthy.ingest_id
    with _receipts(settings) as receipts:
        after = {r.ingest_id: r for r in receipts.recent(tenant_id=tenant)}
    assert after[healthy.ingest_id].status is ReceiptStatus.PUBLISHED
    # Set aside: still open, the reason on the receipt, pydantic's types and field names only.
    assert (after[corrupt.ingest_id].status, after[corrupt.ingest_id].detail) == (
        ReceiptStatus.HELD,
        "stored envelope set aside: json_invalid",
    )
    reason = after[older.ingest_id].detail
    assert after[older.ingest_id].status is ReceiptStatus.HELD
    assert reason.startswith("stored envelope set aside: attempt missing, ")
    assert reason.endswith(" more")
    assert "Jane" not in reason and "Roe" not in reason and "local:" not in reason
    assert [r.getMessage() for r in records if r.levelno == logging.WARNING] == [
        f"republish: receipt {corrupt.ingest_id}: stored envelope set aside: json_invalid",
        f"republish: receipt {older.ingest_id}: {reason}",
    ]
    # The next run counts them again and goes on: nothing is stuck behind them.
    again = documents.republish_documents(settings)
    assert (again.open_receipts, again.bad_envelope, again.published) == (2, 2, 0)


# ── The relay reads a page at a time, and only the sources that may go out ───
def test_the_relay_holds_one_page_of_receipts_at_a_time(
    settings, tmp_path, pdf_bytes, manifest_writer, monkeypatch
):
    """The review's probe: 50k held receipts and limit=1 took 0.78 s and 78 MB, every row loaded
    first. Now each publish happens with at most one page loaded ahead of it."""
    folder = tmp_path / "drop"
    folder.mkdir()
    for n in range(5):
        (folder / f"{n}.pdf").write_bytes(pdf_bytes + bytes([n]))
    assert _pull(settings, folder, manifest_writer, shadow=True).held == 5
    manifest_writer(folder, settings.manifest_dir)
    monkeypatch.setattr(receipts_module, "PAGE_SIZE", 2, raising=False)
    loaded = []
    real_to_receipt = receipts_module._to_receipt

    def counted(row):
        loaded.append(row[0])
        return real_to_receipt(row)

    monkeypatch.setattr(receipts_module, "_to_receipt", counted)
    loaded_at_publish = []
    real_republish = IngestBridge.republish

    def republish(self, receipt, trace_id):
        loaded_at_publish.append(len(loaded))
        return real_republish(self, receipt, trace_id)

    monkeypatch.setattr(IngestBridge, "republish", republish)
    assert documents.republish_documents(settings).published == 5
    assert loaded_at_publish == [2, 2, 4, 4, 5]


def test_the_receipts_of_a_source_that_is_not_enabled_are_counted_never_loaded(
    settings, tmp_path, pdf_bytes, manifest_writer, monkeypatch
):
    for name in ("shadow-docs", "enabled-docs"):
        folder = tmp_path / name
        folder.mkdir()
        for n in range(3):
            (folder / f"{n}.pdf").write_bytes(pdf_bytes + name.encode() + bytes([n]))
        _pull(settings, folder, manifest_writer, manifest_id=name, shadow=True)
    manifest_writer(
        tmp_path / "enabled-docs", settings.manifest_dir, manifest_id="enabled-docs"
    )
    loaded = []
    real_to_receipt = receipts_module._to_receipt

    def counted(row):
        loaded.append(row[2])  # the source
        return real_to_receipt(row)

    monkeypatch.setattr(receipts_module, "_to_receipt", counted)
    summary = documents.republish_documents(settings)
    assert (summary.open_receipts, summary.held_back, summary.published) == (6, 3, 3)
    assert set(loaded) == {
        "enabled-docs"
    }  # the shadow source's rows never left the database


def test_a_stored_v0_2_envelope_still_relays_and_goes_out_as_v0_3(
    settings, pdf_folder, manifest_writer
):
    """Receipts written before v0.3 store envelopes that say envelope_version 0.2 and
    payload_sha256. The relay reads them through the reader's alias and publishes them in the
    current version: the same values, payload_hash for payload_sha256, the new fields null.
    """
    assert _pull(settings, pdf_folder, manifest_writer, shadow=True).held == 1
    tenant = str(settings.tenant_id)
    with _receipts(settings) as receipts:
        [held] = receipts.recent(tenant_id=tenant)
    v02 = json.loads(held.envelope)
    for added in ("member_id", "device_id", "fhir_release"):
        del v02[added]
    v02["payload_sha256"] = v02.pop("payload_hash")
    v02["envelope_version"] = "0.2"
    with sqlite3.connect(DataLayout(settings.data_dir).receipts_db) as db:
        db.execute(
            "UPDATE ingest_receipts SET envelope = ? WHERE ingest_id = ?",
            (json.dumps(v02), held.ingest_id),
        )
    manifest_writer(pdf_folder, settings.manifest_dir)  # enabled
    summary = documents.republish_documents(settings, trace_id=TRACE)
    assert (summary.published, summary.bad_envelope) == (1, 0)
    [message] = _outbox(settings).read("raw_data")
    envelope = message["value"]
    assert envelope["envelope_version"] == "0.3"
    assert envelope["payload_hash"] == v02["payload_sha256"] == held.payload_sha256
    assert "payload_sha256" not in envelope
    assert (envelope["member_id"], envelope["device_id"], envelope["fhir_release"]) == (
        None,
        None,
        None,
    )
    assert (envelope["ingest_id"], envelope["idempotency_key"]) == (
        held.ingest_id,
        v02["idempotency_key"],
    )


# ── The relay runs at least once per start ───────────────────────────────────
def test_the_worker_queues_one_republish_when_it_starts(
    service, settings, pdf_folder, manifest_writer, blocked_outbox, wait_for
):
    """The review: nothing said how often the relay runs, so without a caller it never did. The
    scheduler calls it on a fixed cadence (docs/jobs-api.md); until then, and after every restart,
    the worker queues one itself: a publish that failed before a restart gets out unasked.
    """
    _pull(
        settings, pdf_folder, manifest_writer
    )  # publish_failed: the outbox is blocked
    blocked_outbox()  # the broker is back, and the service restarts
    with service(republish_at_start=True) as client:
        [job] = client.get("/jobs").json()["jobs"]
        assert (job["kind"], job["params"]) == (
            "documents.republish",
            {"source": None, "limit": None},
        )
        done = wait_for(client, job["job_id"])
    assert (done["status"], done["counts"]["published"]) == ("succeeded", 1)
    [message] = _outbox(settings).read("raw_data")
    assert message["value"]["trace_id"] == UUID(job["job_id"]).hex


def test_no_start_up_republish_without_a_tenant(service):
    """The relay needs a tenant: a deployment without one queues nothing at start."""
    with service(tenant_id=None, republish_at_start=True) as client:
        assert client.get("/jobs").json() == {"jobs": []}


def test_the_start_up_jobs_run_after_the_ones_a_previous_process_left_queued(
    tmp_path, settings
):
    store = JobStore(tmp_path / "jobs.sqlite")
    store.add(Job("j-left", "ok", JobStatus.QUEUED, {}, STAMP))
    ran = []

    def handler(job):
        ran.append((job.job_id, job.params))
        return JobResult({"done": 1})

    runner = JobRunner(settings, store, {"ok": handler}, startup=[("ok", {"n": 1})])
    runner.start()
    deadline = time.monotonic() + 5
    while len(ran) < 2:
        assert time.monotonic() < deadline
        time.sleep(0.01)
    runner.stop()
    assert ran[0] == ("j-left", {})
    assert ran[1][1] == {"n": 1}


# ── A set-aside receipt: written once, bounded by the limit, never moved back ─
def _held_with_bad_envelopes(settings, tmp_path, pdf_bytes, manifest_writer, count):
    """``count`` held receipts whose stored envelope does not validate, their source enabled."""
    folder = tmp_path / "drop"
    folder.mkdir()
    for n in range(count):
        (folder / f"{n}.pdf").write_bytes(pdf_bytes + bytes([n]))
    assert _pull(settings, folder, manifest_writer, shadow=True).held == count
    with sqlite3.connect(DataLayout(settings.data_dir).receipts_db) as db:
        db.execute("UPDATE ingest_receipts SET envelope = '{broken'")
    manifest_writer(folder, settings.manifest_dir)  # out of shadow


def _receipt_rows(settings):
    with sqlite3.connect(DataLayout(settings.data_dir).receipts_db) as db:
        return db.execute(
            "SELECT ingest_id, status, detail, updated_at FROM ingest_receipts "
            "ORDER BY received_at, rowid"
        ).fetchall()


def test_a_set_aside_receipt_is_written_and_logged_once_not_on_every_run(
    settings, tmp_path, pdf_bytes, manifest_writer, records
):
    """The re-review's probe: every run re-read each set-aside receipt, rewrote its status, detail
    and updated_at (a commit each) and logged a warning: 5,000 of them took 15 s, then 30 s. The
    reason is written once; a run that meets it again writes and logs nothing."""
    _held_with_bad_envelopes(settings, tmp_path, pdf_bytes, manifest_writer, 1)
    first = documents.republish_documents(settings)
    [(ingest_id, status, detail, stamped)] = _receipt_rows(settings)
    assert (first.bad_envelope, status, detail) == (
        1,
        "held",
        "stored envelope set aside: json_invalid",
    )
    again = documents.republish_documents(settings)
    assert again.bad_envelope == 1  # still counted: it is still open, and still bad
    assert _receipt_rows(settings) == [
        (ingest_id, "held", detail, stamped)
    ]  # not rewritten
    warnings = [r.getMessage() for r in records if r.levelno == logging.WARNING]
    assert warnings == [
        f"republish: receipt {ingest_id}: {detail}"
    ]  # once, for both runs


def test_the_limit_bounds_every_receipt_a_run_takes_up_set_aside_ones_included(
    settings, tmp_path, pdf_bytes, manifest_writer
):
    """The limit counted only published and duplicate receipts, so a run with limit=1 still read
    and rewrote every set-aside one. It bounds every receipt taken up now."""
    _held_with_bad_envelopes(settings, tmp_path, pdf_bytes, manifest_writer, 5)
    summary = documents.republish_documents(settings, limit=2)
    assert (summary.open_receipts, summary.bad_envelope) == (5, 2)
    details = [row[2] for row in _receipt_rows(settings)]
    assert (
        details
        == ["stored envelope set aside: json_invalid"] * 2
        + ["held for shadow mode"] * 3
    )
    # Receipts with no envelope count too.
    with sqlite3.connect(DataLayout(settings.data_dir).receipts_db) as db:
        db.execute("UPDATE ingest_receipts SET envelope = NULL")
    assert documents.republish_documents(settings, limit=3).no_envelope == 3


def test_a_set_aside_never_moves_back_a_receipt_another_run_moved_meanwhile(
    settings, tmp_path, pdf_bytes, manifest_writer
):
    """The re-review's probe: the relay read a held receipt, another run moved it to received,
    and the set-aside write, set_status(<the status read>, ...), moved it back to held. The write
    is the detail alone now, and only while the status is still the one read."""
    _held_with_bad_envelopes(settings, tmp_path, pdf_bytes, manifest_writer, 1)
    tenant = str(settings.tenant_id)
    with documents.ingest_bridge(settings) as bridge:
        [stale] = bridge.receipts.in_status(
            ReceiptStatus.HELD, {"local-docs"}, tenant_id=tenant
        )
        with _receipts(settings) as other:  # another run's own connection
            other.set_status(stale.ingest_id, ReceiptStatus.RECEIVED, tenant_id=tenant)
        result = bridge.republish(stale, TRACE)
    assert result.outcome.value == "bad_envelope"
    [(_, status, detail, _stamp)] = _receipt_rows(settings)
    assert (status, detail) == ("received", None)  # the other run's word stands


def test_a_stored_envelope_newer_than_this_service_writes_is_set_aside_never_relabelled(
    settings, pdf_folder, manifest_writer, records
):
    """The re-review's probe: a stored 0.4 envelope (written before a rollback, with a field this
    0.3 reader does not know) went out relabelled 0.3, the field dropped. It is set aside now:
    only v0.2 is upgraded."""
    assert _pull(settings, pdf_folder, manifest_writer, shadow=True).held == 1
    tenant = str(settings.tenant_id)
    with _receipts(settings) as receipts:
        [held] = receipts.recent(tenant_id=tenant)
    newer = json.loads(held.envelope)
    newer.update(envelope_version="0.4", key_formula="d00-v2")
    with sqlite3.connect(DataLayout(settings.data_dir).receipts_db) as db:
        db.execute(
            "UPDATE ingest_receipts SET envelope = ? WHERE ingest_id = ?",
            (json.dumps(newer), held.ingest_id),
        )
    manifest_writer(pdf_folder, settings.manifest_dir)  # enabled
    summary = documents.republish_documents(settings)
    assert (summary.published, summary.bad_envelope) == (0, 1)
    assert _outbox(settings).read("raw_data") == []
    with _receipts(settings) as receipts:
        kept = receipts.get(held.ingest_id, tenant_id=tenant)
    assert (kept.status, kept.detail) == (
        ReceiptStatus.HELD,
        "stored envelope is newer (0.4)",
    )
    assert json.loads(kept.envelope) == newer  # the stored one stays as it was


def test_the_start_up_republish_is_not_queued_again_while_one_still_waits(settings):
    """The re-review's probe: five starts whose worker never took a job (a crash loop) left five
    start-up republishes queued, past the queue's bound, and the scheduler then got a 429. One
    waits, whatever the number of starts; a republish the scheduler queued for one source is
    another job and does not count."""
    store = JobStore(DataLayout(settings.data_dir).jobs_db)
    store.add(
        Job(
            "j-sched",
            "documents.republish",
            JobStatus.QUEUED,
            {"source": "local-docs", "limit": None},
            STAMP,
        )
    )
    for _ in range(5):
        runner = build_runner(settings)
        runner._thread = types.SimpleNamespace(
            start=lambda: None
        )  # it dies before running
        runner.start()
    queued = [(job.kind, job.params) for job in store.recent(50)]
    assert sorted(queued, key=str) == sorted(
        [
            ("documents.republish", {"source": "local-docs", "limit": None}),
            ("documents.republish", {"source": None, "limit": None}),
        ],
        key=str,
    )
