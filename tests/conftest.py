import logging

import pytest
from fastapi.testclient import TestClient

import main
from src.db.file_store import LocalFileStore
from src.db.outbox import JsonlOutbox
from src.db.receipts import ReceiptStore
from src.handler.ingest import IngestBridge, SourceContext
from src.logging import logger
from src.utility.keys import ObjectClass

TENANT = "0192a3b4-0000-7000-8000-000000000001"
# The smallest thing that sniffs as a PDF: the header within the first 1024 bytes.
PDF = b"%PDF-1.4\n1 0 obj<</Type/Catalog>>endobj\ntrailer<</Root 1 0 R>>\n%%EOF\n"


@pytest.fixture
def tenant():
    return TENANT


@pytest.fixture
def pdf_bytes():
    return PDF


@pytest.fixture
def receipts(tmp_path):
    store = ReceiptStore(tmp_path / "receipts.sqlite")
    yield store
    store.close()


@pytest.fixture
def bridge(tmp_path, receipts):
    return IngestBridge(
        tenant_id=TENANT,
        store=LocalFileStore(tmp_path / "filestore"),
        publisher=JsonlOutbox(tmp_path / "outbox"),
        receipts=receipts,
    )


@pytest.fixture
def ctx():
    return SourceContext(
        source="test-docs",
        fetch="local",
        route="L7",
        mode="pull",
        connector="documents",
        connector_version="0.1.0",
        retention_class=ObjectClass.DOCUMENTS,
        formats=("pdf",),
        trace_id="0192a3b40000700080000000000000ab",
    )


@pytest.fixture
def records():
    """Log records from the service logger (it does not propagate, so caplog cannot see them)."""
    captured: list[logging.LogRecord] = []
    handler = logging.Handler()
    handler.emit = captured.append
    logger.addHandler(handler)
    yield captured
    logger.removeHandler(handler)


@pytest.fixture
def client():
    with TestClient(main.app) as test_client:  # runs the lifespan too
        yield test_client
