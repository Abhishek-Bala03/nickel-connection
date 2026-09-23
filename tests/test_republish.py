"""The republish relay (docs/document-ingestion.md, "Republishing open receipts"): open receipts go
out again from the envelopes they store, without their sources, as a job, from the command line, or
through ``documents.republish_documents``."""

import dataclasses
import io
import shutil
from uuid import UUID

import pytest

from src.cli import main
from src.connectors.documents import DocumentConnector
from src.db.layout import DataLayout
from src.db.outbox import JsonlOutbox
from src.db.receipts import Receipt, ReceiptStatus, ReceiptStore
from src.handler import documents
from src.handler.ingest import IngestBridge

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


def test_a_receipt_without_a_stored_envelope_is_counted_not_guessed_at(settings):
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
    # A pull skips an oversized file at the source (no receipt); a too_large receipt comes from
    # the bridge's own check, as a pushed upload would meet it.
    manifest_writer(pdf_folder, settings.manifest_dir)
    manifest = documents.load_manifest(settings, "local-docs")
    capped = dataclasses.replace(settings, max_file_bytes=10)
    with documents.ingest_bridge(capped) as bridge:
        connector = DocumentConnector(
            manifest, bridge, environment="development", max_file_bytes=10**6
        )
        assert connector.run().skipped == 1
    with _receipts(settings) as receipts:
        [receipt] = receipts.recent(tenant_id=str(settings.tenant_id))
    assert receipt.status is ReceiptStatus.TOO_LARGE
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
    assert (summary.open_receipts, summary.published) == (1, 0)  # stopped at the first
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
    assert (capped.open_receipts, capped.published) == (1, 1)
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
    }
    out = io.StringIO()
    assert main(["documents", "republish", "--source", "local-docs"], env, out) == 1
    assert "ABORTED         publish failed; the republish stopped" in out.getvalue()
    blocked_outbox()
    out = io.StringIO()
    assert main(["documents", "republish", "--limit", "10"], env, out) == 0
    assert "published       1" in out.getvalue()
    assert "trace_id        " in out.getvalue()
