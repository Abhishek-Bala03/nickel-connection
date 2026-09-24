"""A file over the size cap leaves a too_large receipt (docs/document-ingestion.md, "Receipts"):
metadata only, keyed by a surrogate (one per item at the source), shown by GET /documents and the
counts, and superseded once the item comes in within the cap.

The review's point: an over-cap file was skipped before the download with no receipt at all, so an
oversize lab PDF never showed up anywhere, and the too_large status could no longer happen.
"""

import dataclasses
import hashlib
import io

import httpx

from src.cli import main
from src.connectors.documents import DocumentConnector, DocumentManifest
from src.connectors.documents import sources as sources_module
from src.connectors.documents.sources import HttpSource, KaggleSource
from src.db import receipts as receipts_module
from src.db.file_store import LocalFileStore
from src.db.layout import DataLayout
from src.db.outbox import JsonlOutbox
from src.db.receipts import Receipt, ReceiptStatus, ReceiptStore
from src.handler import documents
from src.handler.ingest import IngestBridge, RawItem, SourceContext
from src.utility.keys import ObjectClass

TENANT = "0192a3b4-0000-7000-8000-000000000001"
PUBLIC = "93.184.216.34"


def _surrogate(tenant, source, message_id):
    """What the docs promise the receipt's key is, computed here from the definition."""
    parts = ("too_large", str(tenant), source, message_id)
    return hashlib.sha256("\0".join(parts).encode("utf-8")).hexdigest()


def _pull(settings, cap=None):
    run = settings if cap is None else dataclasses.replace(settings, max_file_bytes=cap)
    return documents.pull_documents(run, documents.load_manifest(run, "local-docs"))


def _receipts(settings):
    with ReceiptStore(DataLayout(settings.data_dir).receipts_db) as receipts:
        return receipts.recent(limit=50, tenant_id=str(settings.tenant_id))


def _manifest(fetch, **block):
    auth = {"kaggle": "kaggle_token"}.get(fetch, "none")
    return DocumentManifest.model_validate(
        {
            "id": "lab-docs",
            "description": "test",
            "stream": "documents",
            "mode": "pull",
            "route": "L7",
            "fetch": fetch,
            "auth": {"type": auth, "vault_path": None},
            "phi": False,
            "owner": "divij",
            "phase": "dev",
            "shadow": False,
            "contract": {"baa": None, "dpa": None},
            "license": "test only",
            "include": ["*"],
            fetch: block,
        }
    )


def test_a_file_over_the_cap_leaves_a_receipt_the_listing_and_the_counts_show(
    service, settings, pdf_folder, pdf_bytes, manifest_writer, monkeypatch
):
    (pdf_folder / "a.pdf").rename(pdf_folder / "Jane_Doe_labs.pdf")
    manifest_writer(pdf_folder, settings.manifest_dir)
    cap = len(pdf_bytes) - 1

    def never_read(*args):
        raise AssertionError("a file listed over the cap must not be read")

    monkeypatch.setattr(sources_module, "_read_capped", never_read)
    summary = _pull(settings, cap)
    assert (summary.matched, summary.published, summary.skipped) == (1, 0, 0)
    assert summary.counts()["too_large"] == 1  # its own count, not a failed fetch
    assert documents.receipt_counts(settings) == {"too_large": 1}
    with service() as client:
        [document] = client.get("/documents?status=too_large").json()["documents"]
        listing = client.get("/documents").text
    assert document["status"] == "too_large"
    assert document["detail"] == f"{len(pdf_bytes)} bytes; the cap is {cap}"
    assert document["payload_ref"] is None
    assert document["payload_sha256"] is None  # no bytes were read: no hash to show
    assert "Jane" not in listing  # the message id (the path) stays in the receipt
    # Metadata only: nothing stored, no envelope, nothing on raw_data. The receipt is stored under
    # the surrogate key.
    [receipt] = _receipts(settings)
    assert receipt.envelope is None
    assert receipt.payload_sha256 == _surrogate(
        settings.tenant_id, "local-docs", "local:src/Jane_Doe_labs.pdf"
    )
    assert not list((settings.data_dir / "filestore").rglob("*"))
    assert JsonlOutbox(DataLayout(settings.data_dir).outbox).read("raw_data") == []


def test_repeated_pulls_of_an_over_cap_file_add_no_rows(
    settings, pdf_folder, pdf_bytes, manifest_writer
):
    manifest_writer(pdf_folder, settings.manifest_dir)
    cap = len(pdf_bytes) - 1
    _pull(settings, cap)
    [marker] = _receipts(settings)
    again = _pull(settings, cap)
    assert again.counts()["too_large"] == 1
    assert _receipts(settings) == [
        marker
    ]  # the same row, not even its updated_at moved
    # A new cap, still too small: the same row says so.
    _pull(settings, cap - 1)
    [updated] = _receipts(settings)
    assert (updated.ingest_id, updated.detail) == (
        marker.ingest_id,
        f"{len(pdf_bytes)} bytes; the cap is {cap - 1}",
    )


def test_once_the_cap_is_raised_the_file_goes_through_and_supersedes_its_receipt(
    settings, pdf_folder, pdf_bytes, manifest_writer
):
    manifest_writer(pdf_folder, settings.manifest_dir)
    _pull(settings, len(pdf_bytes) - 1)
    [marker] = _receipts(settings)
    raised = _pull(settings)  # NICKEL_MAX_FILE_BYTES back at its default
    assert (raised.published, raised.counts()["too_large"]) == (1, 0)
    sha = hashlib.sha256(pdf_bytes).hexdigest()
    by_id = {receipt.ingest_id: receipt for receipt in _receipts(settings)}
    closed = by_id.pop(marker.ingest_id)
    [published] = by_id.values()
    assert (closed.status, closed.detail) == (
        "superseded",
        f"superseded by the receipt of payload {sha}",
    )
    assert (published.status, published.payload_sha256) == ("published", sha)
    assert documents.receipt_counts(settings) == {"published": 1, "superseded": 1}
    [message] = JsonlOutbox(DataLayout(settings.data_dir).outbox).read("raw_data")
    assert message["id"] == published.ingest_id
    assert (
        documents.republish_documents(settings).open_receipts == 0
    )  # nothing left open
    # Over the cap again (a bigger version at the same path): the same receipt reopens.
    (pdf_folder / "a.pdf").write_bytes(pdf_bytes + b"%" * 100)
    cap = len(pdf_bytes) + 10
    assert _pull(settings, cap).counts()["too_large"] == 1
    reopened = {r.ingest_id: r for r in _receipts(settings)}[marker.ingest_id]
    assert (reopened.status, reopened.detail) == (
        "too_large",
        f"{len(pdf_bytes) + 100} bytes; the cap is {cap}",
    )
    assert documents.receipt_counts(settings) == {"published": 1, "too_large": 1}


def test_a_download_that_passes_the_cap_while_spooling_leaves_a_receipt(bridge):
    def handler(request):
        return httpx.Response(200, content=iter([b"%PDF-1.4 ", b"x" * 50]))

    source = HttpSource(
        ["https://lab.example/Jane_Doe.pdf"],
        max_bytes=10,
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        resolver=lambda host: [PUBLIC],
    )
    manifest = _manifest("http", urls=["https://lab.example/Jane_Doe.pdf"])
    summary = DocumentConnector(
        manifest, bridge, source, environment="development", max_file_bytes=10
    ).run()
    assert (summary.skipped, summary.counts()["too_large"]) == (0, 1)
    [marker] = bridge.receipts.recent(tenant_id=TENANT)
    assert (marker.status, marker.detail) == (
        "too_large",
        "over the 10-byte cap; the download stopped there",
    )
    assert marker.payload_sha256 == _surrogate(
        TENANT, "lab-docs", "https://lab.example/Jane_Doe.pdf"
    )


def test_an_sdk_file_over_the_cap_on_disk_leaves_a_receipt(bridge, tmp_path):
    (tmp_path / "big.pdf").write_bytes(b"%PDF-" + b"x" * 20)
    source = KaggleSource(
        "owner/dataset",
        files=["big.pdf"],
        max_bytes=10,
        download=lambda handle, path=None: str(tmp_path / path),
    )
    manifest = _manifest("kaggle", handle="owner/dataset")
    summary = DocumentConnector(
        manifest, bridge, source, environment="development", max_file_bytes=10
    ).run()
    assert (summary.skipped, summary.counts()["too_large"]) == (0, 1)
    [marker] = bridge.receipts.recent(tenant_id=TENANT)
    assert (marker.status, marker.detail) == ("too_large", "25 bytes; the cap is 10")


class Interleaving:
    """A receipt store whose first ``add`` lets ``rival`` run to completion first."""

    def __init__(self, inner, rival):
        self._inner, self._rival = inner, rival

    def __getattr__(self, name):
        return getattr(self._inner, name)

    def add(self, receipt):
        if self._rival is not None:
            rival, self._rival = self._rival, None
            rival()
        return self._inner.add(receipt)


def test_two_pulls_receipting_one_over_cap_item_at_once_share_one_receipt(
    tmp_path, receipts, ctx
):
    kwargs = {
        "tenant_id": TENANT,
        "store": LocalFileStore(tmp_path / "fs"),
        "publisher": JsonlOutbox(tmp_path / "out"),
    }
    first = IngestBridge(receipts=receipts, **kwargs)
    won = []

    def rival():
        won.append(first.too_large(ctx, "local:docs/big.pdf", size=20, cap=10))

    second = IngestBridge(receipts=Interleaving(receipts, rival), **kwargs)
    result = second.too_large(ctx, "local:docs/big.pdf", size=20, cap=10)
    [winner] = won
    assert result.ingest_id == winner.ingest_id
    assert receipts.counts(tenant_id=TENANT) == {"too_large": 1}


def test_the_surrogate_key_is_tenant_first_over_source_and_message_id():
    from src.db.receipts import too_large_key

    key = too_large_key(TENANT, "local-docs", "local:src/a.pdf")
    assert key == _surrogate(TENANT, "local-docs", "local:src/a.pdf")
    other = "0192a3b4-0000-7000-8000-000000000002"
    assert key != too_large_key(other, "local-docs", "local:src/a.pdf")
    assert key != too_large_key(TENANT, "other-docs", "local:src/a.pdf")
    assert key != too_large_key(TENANT, "local-docs", "local:src/b.pdf")
    # NUL-separated: a character moved across a boundary changes the key.
    assert key != too_large_key(TENANT, "local-doc", "slocal:src/a.pdf")


def test_a_receipt_keyed_by_a_payload_hash_is_never_superseded(
    tmp_path, receipts, ctx, pdf_bytes
):
    """Only a pull's surrogate-keyed receipt is superseded. The bridge's own too_large receipt is
    keyed by the payload's hash and goes through under its ingest id once the cap is raised
    (tests/test_ingest.py); another version of the item coming in leaves it as it is."""
    kwargs = {
        "tenant_id": TENANT,
        "store": LocalFileStore(tmp_path / "fs"),
        "publisher": JsonlOutbox(tmp_path / "out"),
        "receipts": receipts,
    }
    big = IngestBridge(max_bytes=10, **kwargs).ingest(ctx, _item(pdf_bytes))
    IngestBridge(**kwargs).ingest(ctx, _item(pdf_bytes + b"% another version"))
    assert receipts.get(big.ingest_id, tenant_id=TENANT).status == "too_large"
    assert receipts.counts(tenant_id=TENANT) == {"published": 1, "too_large": 1}


def _item(data, message_id="local:docs/a.pdf"):
    return RawItem(message_id=message_id, data=data, source_meta={"path": "a.pdf"})


# ── Round-3 re-review: the marker shows no hash; no reopening for the same size ─
def test_the_api_and_the_command_line_show_no_hash_for_a_marker(
    service, settings, tmp_path, pdf_bytes, manifest_writer
):
    """The re-review's probe: GET /documents and `documents status` returned a pull's surrogate as
    if it were the payload's hash, beside the bridge's own too_large receipt, which does carry the
    real one. The marker shows null ("-" at the command line); the bridge's receipt its hash.
    """
    folder = tmp_path / "drop"
    folder.mkdir()
    big = pdf_bytes + b"%" * 1000
    (folder / "big.pdf").write_bytes(big)
    manifest_writer(folder, settings.manifest_dir)
    _pull(settings, len(pdf_bytes) + 10)  # the pull's marker
    layout = DataLayout(settings.data_dir)
    with ReceiptStore(layout.receipts_db) as receipts:
        bridge = IngestBridge(
            tenant_id=TENANT,
            store=LocalFileStore(layout.filestore),
            publisher=JsonlOutbox(layout.outbox),
            receipts=receipts,
            max_bytes=len(pdf_bytes) + 10,
        )
        ctx = SourceContext(
            source="upload-docs",
            fetch="local",
            route="L7",
            mode="upload",
            connector="documents",
            connector_version="0.1.0",
            retention_class=ObjectClass.DOCUMENTS,
            formats=("pdf",),
            trace_id="c" * 32,
        )
        bridge.ingest(
            ctx, RawItem(message_id="upload:1", data=big)
        )  # a payload in hand
    with service() as client:
        documents_ = client.get("/documents?status=too_large").json()["documents"]
    shown = {d["source"]: d["payload_sha256"] for d in documents_}
    assert shown == {"local-docs": None, "upload-docs": hashlib.sha256(big).hexdigest()}
    out = io.StringIO()
    env = {
        "NICKEL_ENV": "development",  # the local File Store (production takes only S3)
        "NICKEL_TENANT_ID": str(settings.tenant_id),
        "NICKEL_DATA_DIR": str(settings.data_dir),
    }
    assert main(["documents", "status"], env, out) == 0
    lines = {
        line.split()[2]: line.split()[-1] for line in out.getvalue().splitlines()[1:]
    }
    assert lines == {
        "local-docs": "-",
        "upload-docs": hashlib.sha256(big).hexdigest()[:12],
    }


def test_is_marker_is_the_surrogate_key_on_a_too_large_or_superseded_receipt():
    marker = Receipt(
        ingest_id="i1",
        tenant_id=TENANT,
        source="s",
        message_id="m",
        payload_sha256=_surrogate(TENANT, "s", "m"),
        status=ReceiptStatus.TOO_LARGE,
        received_at="t",
        updated_at="t",
    )
    assert marker.is_marker
    assert dataclasses.replace(marker, status=ReceiptStatus.SUPERSEDED).is_marker
    # The bridge's own too_large receipt carries the payload's hash: not a marker.
    assert not dataclasses.replace(marker, payload_sha256="ab" * 32).is_marker
    # Bytes built as the surrogate's preimage: the pull takes the marker's row over as the item's
    # receipt, whose key is then really the payload's hash: no longer a marker.
    assert not dataclasses.replace(marker, status=ReceiptStatus.PUBLISHED).is_marker


def test_a_superseded_marker_is_not_reopened_for_the_size_the_item_came_in_at(
    settings, pdf_folder, pdf_bytes, manifest_writer
):
    """The re-review's probe: the cap lowered after the item was published, and an unchanged file
    reopened a too_large receipt beside the published one. The size seen is the size it came in
    at: most likely the same item, already delivered, so nothing reopens (a new size does).
    """
    manifest_writer(pdf_folder, settings.manifest_dir)
    _pull(settings, len(pdf_bytes) - 1)  # the marker
    _pull(settings)  # the cap raised: published, the marker superseded
    lowered = _pull(settings, len(pdf_bytes) - 1)
    assert (
        lowered.counts()["too_large"] == 1
    )  # still over the cap in force, and said so
    assert documents.receipt_counts(settings) == {"published": 1, "superseded": 1}


def test_an_ingest_with_no_marker_for_its_item_writes_nothing_for_one(
    settings, pdf_folder, manifest_writer, monkeypatch
):
    """Every ingested item looks for a marker to supersede, duplicates included. With none there
    (every item but the few once over the cap) that is one read on the unique index: no UPDATE,
    no commit."""
    manifest_writer(pdf_folder, settings.manifest_dir)
    _pull(settings)  # published
    statements = []
    real_connect = receipts_module.connect

    def connect(path):
        db = real_connect(path)
        db.set_trace_callback(statements.append)
        return db

    monkeypatch.setattr(receipts_module, "connect", connect)
    again = _pull(settings)  # a re-pull: a duplicate
    assert again.duplicates == 1
    assert not [
        s for s in statements if "SET status = ?, detail = ?" in s or "superseded" in s
    ]
