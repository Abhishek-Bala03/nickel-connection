"""GET /documents as a caller sees it: receipt metadata, never content, one tenant's only."""

import dataclasses

import pytest

from src.db.layout import DataLayout
from src.db.receipts import Receipt, ReceiptStatus, ReceiptStore
from src.handler import documents

OTHER_TENANT = "0192a3b4-0000-7000-8000-000000000002"
STAMP = "2026-09-22T00:00:00+00:00"


@pytest.fixture
def pulled(settings, pdf_folder, pdf_bytes, manifest_writer):
    """Two sources pulled: local-docs (a.pdf, b.pdf, fake.pdf rejected) and other-docs (a.pdf)."""
    (pdf_folder / "b.pdf").write_bytes(pdf_bytes + b"% b\n")
    (pdf_folder / "fake.pdf").write_bytes(b"not a pdf")
    other = pdf_folder.parent / "other"
    other.mkdir()
    (other / "a.pdf").write_bytes(pdf_bytes)
    for folder, manifest_id in ((pdf_folder, "local-docs"), (other, "other-docs")):
        manifest_writer(folder, settings.manifest_dir, manifest_id=manifest_id)
        documents.pull_documents(
            settings, documents.load_manifest(settings, manifest_id)
        )
    return settings


def test_documents_are_the_receipts_newest_first(client, pulled):
    listed = client.get("/documents").json()["documents"]
    assert [(d["source"], d["status"]) for d in listed] == [
        ("other-docs", "published"),
        ("local-docs", "rejected"),
        ("local-docs", "published"),
        ("local-docs", "published"),
    ]
    # The receipt, and nothing of the content: no text, no values, no derived result, and not
    # the message id either (for a file source it is the path at the source, and a clinic's file
    # names can carry patient names).
    assert set(listed[0]) == {
        "ingest_id",
        "tenant_id",
        "source",
        "payload_sha256",
        "status",
        "received_at",
        "updated_at",
        "payload_ref",
        "detail",
    }
    assert listed[1]["detail"].startswith("content does not match")
    assert "fake.pdf" not in client.get("/documents").text


def test_documents_can_be_filtered(client, pulled):
    published = client.get("/documents?status=published").json()["documents"]
    assert [d["status"] for d in published] == ["published"] * 3
    one_source = client.get("/documents?source=local-docs&status=rejected").json()
    [rejected] = one_source["documents"]
    assert (rejected["source"], rejected["payload_ref"]) == ("local-docs", None)
    assert len(client.get("/documents?limit=2").json()["documents"]) == 2
    assert client.get("/documents?status=lost").status_code == 422
    assert client.get("/documents?limit=0").status_code == 422
    assert client.get("/documents?source=nope").json() == {"documents": []}


def test_the_listing_is_this_tenants_only(client, pulled):
    """The receipts file is shared, and the listing reads it as the deployment's tenant: another
    tenant's rows (here written by hand, as another deployment would) never show."""
    with ReceiptStore(DataLayout(pulled.data_dir).receipts_db) as receipts:
        receipts.add(
            Receipt(
                ingest_id="0192a3b4-0000-7000-8000-0000000000bb",
                tenant_id=OTHER_TENANT,
                source="local-docs",
                message_id="local:theirs/Jane-Roe-labs.pdf",
                payload_sha256="ab" * 32,
                status=ReceiptStatus.PUBLISHED,
                received_at=STAMP,
                updated_at=STAMP,
                payload_ref="k",
            )
        )
    listed = client.get("/documents?limit=500").json()["documents"]
    assert len(listed) == 4
    assert {d["tenant_id"] for d in listed} == {str(pulled.tenant_id)}
    assert client.get("/documents?source=local-docs&status=published").json() == {
        "documents": [d for d in listed if d["source"] == "local-docs"][1:]
    }
    assert "Jane-Roe" not in client.get("/documents?limit=500").text


def test_the_api_is_the_connection_layer_and_nothing_more(client):
    # The connection layer ends at raw_data: a pull, the job's state, the receipts. No route reads,
    # derives or serves a document's content; that is the Document worker's, downstream.
    paths = client.app.openapi()["paths"]
    assert {
        (method.upper(), path)
        for path, operations in paths.items()
        for method in operations
    } == {
        ("GET", "/health"),
        ("GET", "/ready"),
        ("GET", "/version"),
        ("POST", "/jobs/documents/pull"),
        ("POST", "/jobs/documents/republish"),
        ("GET", "/jobs"),
        ("GET", "/jobs/{job_id}"),
        ("GET", "/documents"),
    }


def test_a_pull_applies_the_deployments_size_cap_at_the_source(
    settings, pdf_folder, manifest_writer
):
    """The cap in force is the smaller of the manifest's and this deployment's
    (NICKEL_MAX_FILE_BYTES): a file over the deployment's cap is stopped at the source, before it
    is read, and leaves a too_large receipt (before, it left none, and never showed up).
    """
    small = dataclasses.replace(settings, max_file_bytes=16)
    manifest_writer(pdf_folder, small.manifest_dir)
    summary = documents.pull_documents(
        small, documents.load_manifest(small, "local-docs")
    )
    assert (summary.matched, summary.too_large, summary.published) == (1, 1, 0)
    assert documents.receipt_counts(small) == {"too_large": 1}
