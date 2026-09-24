import os

# The service does not start without its token (src.services.auth), and importing main starts it. Set
# before the imports below, and forced rather than defaulted: the tests never depend on the shell.
SERVICE_TOKEN = "test-service-token-0123456789abcdef0123456789abcdef"
os.environ["NICKEL_SERVICE_TOKEN"] = SERVICE_TOKEN
os.environ.pop("NICKEL_SERVICE_TOKEN_FILE", None)
# Nor, in production (the default, forced here too), without the S3 File Store: a loopback endpoint,
# which nothing contacts (the fixtures below run on local stores; the S3 tests set their own).
for _name in [name for name in os.environ if name.startswith("NICKEL_S3_")]:
    del os.environ[_name]
for _name in ("NICKEL_ENV", "NICKEL_FILE_STORE"):
    os.environ.pop(_name, None)
os.environ["NICKEL_S3_ENDPOINT_URL"] = "http://127.0.0.1:8333"
os.environ["NICKEL_S3_ACCESS_KEY_ID"] = "test-access-key"
os.environ["NICKEL_S3_SECRET_ACCESS_KEY"] = "test-secret-0123456789abcdef"

import dataclasses
import logging
import time
from uuid import UUID

import pytest
import yaml
from fastapi.testclient import TestClient

import main
from src.config import load_settings
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
def pdf_folder(tmp_path, pdf_bytes):
    """A folder with one PDF, for local-route manifests."""
    folder = tmp_path / "src"
    folder.mkdir()
    (folder / "a.pdf").write_bytes(pdf_bytes)
    return folder


@pytest.fixture
def manifest_writer(tmp_path):
    """Writes a valid local-route manifest for a folder and returns the file's path."""

    def write(folder, directory=None, manifest_id="local-docs", **over):
        data = {
            "id": manifest_id,
            "description": "test",
            "stream": "documents",
            "mode": "pull",
            "route": "L7",
            "fetch": "local",
            "auth": {"type": "none", "vault_path": None},
            "phi": False,
            "owner": "divij",
            "phase": "prod",  # a clinic's own folder; the phase tests set theirs
            "shadow": False,
            "contract": {"baa": None, "dpa": None},
            "license": "test only",
            "local": {"path": str(folder)},
            **over,
        }
        path = (directory or tmp_path) / f"{manifest_id}.yaml"
        path.write_text(yaml.safe_dump(data), encoding="utf-8")
        return path

    return write


@pytest.fixture
def settings(tmp_path):
    """The service settings on temporary data: a tenant, an empty manifest folder, a fresh data dir
    and the local File Store."""
    manifests = tmp_path / "manifests"
    manifests.mkdir()
    # A development deployment, as the tests' dev-phase manifests need (production is the default and
    # clears only prod manifests); tests of production behaviour set the environment themselves.
    return dataclasses.replace(
        load_settings(),
        environment="development",
        data_dir=tmp_path / "data",
        manifest_dir=manifests,
        tenant_id=UUID(TENANT),
        s3=None,
    )


@pytest.fixture
def service(settings):
    """A TestClient factory for the real app on those settings: ``with service() as client:`` runs the
    lifespan (the job worker) too. Overrides replace settings. The client sends the service token
    (tests/test_auth.py covers going without). The worker's start-up republish is off unless a test
    asks for it (``republish_at_start=True``), so the jobs a test sees are the ones it queued.
    """

    def make(republish_at_start=False, **overrides):
        return TestClient(
            main.create_app(
                dataclasses.replace(settings, **overrides),
                republish_at_start=republish_at_start,
            ),
            headers={"Authorization": f"Bearer {SERVICE_TOKEN}"},
        )

    return make


@pytest.fixture
def client(service):
    with service() as test_client:  # runs the lifespan too
        yield test_client


def _wait_for(client, job_id, timeout=10.0):
    """Poll GET /jobs/{id} the way the scheduler does, until the job is done."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        job = client.get(f"/jobs/{job_id}").json()
        if job["status"] in ("succeeded", "failed"):
            return job
        time.sleep(0.01)
    raise AssertionError(f"job {job_id} did not finish: {job}")


@pytest.fixture
def wait_for():
    return _wait_for
