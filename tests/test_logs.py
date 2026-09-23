"""No log record carries a file name or a path (round 3: "paths in logs"): a clinic's file
names carry patient names, so the logs carry ids, counts, hashes and error types only."""

import dataclasses
import io
import logging
from pathlib import Path

import pytest

from src.cli import main
from src.connectors.documents import DocumentManifest
from src.connectors.documents.sources import (
    FetchError,
    HuggingFaceSource,
    KaggleSource,
    LocalDirSource,
)
from src.db.layout import DataLayout
from src.db.outbox import JsonlOutbox, PublishError
from src.db.receipts import ReceiptStore
from src.handler import documents
from src.logging import logger

# What the review's probe logged at DEBUG, and what an SDK's error quotes.
PATIENT = ("Smith_John_DOB1970", "Jane_Doe_MRN12345")


@pytest.fixture
def debug_level():
    before = logger.level
    logger.setLevel(logging.DEBUG)
    yield
    logger.setLevel(before)


def _leaks(records):
    return [
        record.getMessage()
        for record in records
        if any(name in record.getMessage() for name in PATIENT)
    ]


def test_a_pull_logs_ingest_ids_never_the_files_paths(
    settings, tmp_path, pdf_bytes, manifest_writer, records, debug_level
):
    folder = tmp_path / "Smith_John_DOB1970"
    clinic = folder / "Jane_Doe_MRN12345"
    clinic.mkdir(parents=True)
    (clinic / "labs.pdf").write_bytes(pdf_bytes)  # published
    (clinic / "notes.pdf").write_bytes(b"not a pdf")  # rejected
    (clinic / "scan.pdf").write_bytes(pdf_bytes + b"%" * 1000)  # over the cap
    manifest_writer(folder, settings.manifest_dir)
    run = dataclasses.replace(settings, max_file_bytes=len(pdf_bytes) + 10)
    summary = documents.pull_documents(run, documents.load_manifest(run, "local-docs"))
    assert (summary.published, summary.rejected, summary.too_large) == (1, 1, 1)
    with ReceiptStore(DataLayout(settings.data_dir).receipts_db) as receipts:
        ids = {r.ingest_id for r in receipts.recent(tenant_id=str(settings.tenant_id))}
    debug = [r.getMessage() for r in records if r.levelno == logging.DEBUG]
    # One line per item, by its ingest id.
    assert sorted(line.split()[1] for line in debug) == sorted(ids)
    assert _leaks(records) == []


def test_source_failures_log_the_locator_and_the_type_never_a_path(tmp_path, records):
    # A clinic's drop folder, somewhere under a patient's folder: the locator is the folder's
    # own name, as in the job record; the path above it stays out.
    with pytest.raises(FetchError):
        next(
            LocalDirSource(tmp_path / PATIENT[0] / "clinic-drop", max_bytes=10).files()
        )

    def refused(*args, **kwargs):
        raise RuntimeError(
            "404 Client Error for https://huggingface.co/datasets/o/d/resolve/main/"
            "Smith_John_DOB1970/Jane_Doe_MRN12345_labs.pdf"
        )

    kaggle = KaggleSource("o/d", files=["x.pdf"], max_bytes=10, download=refused)
    [item] = list(kaggle.files())
    with pytest.raises(FetchError):
        item.fetch()

    class Entry:
        path, size = "Smith_John_DOB1970/Jane_Doe_MRN12345_labs.pdf", 1

    class Api:
        def list_repo_tree(self, *args, **kwargs):
            return iter([Entry()])

    hub = HuggingFaceSource("o/d", max_bytes=10, api=Api(), download=refused)
    [item] = list(hub.files())
    with pytest.raises(FetchError):
        item.fetch()
    assert [r.getMessage() for r in records] == [
        "local source clinic-drop: not a folder",
        "kaggle o/d: RuntimeError",
        "huggingface o/d@main: RuntimeError",
    ]
    assert _leaks(records) == []


def test_an_outbox_failure_logs_the_type_and_the_reason_never_the_path(
    tmp_path, records
):
    blocker = tmp_path / "Jane_Doe_MRN12345"
    blocker.write_text("a file where the outbox folder should be")
    with pytest.raises(PublishError):
        JsonlOutbox(blocker).publish("raw_data", "k", {})
    assert _leaks(records) == []


def test_a_locked_or_vanished_file_is_one_skip_and_its_path_goes_nowhere(
    settings, tmp_path, pdf_bytes, manifest_writer, records, debug_level, monkeypatch
):
    """The re-review's probe: without -v, an OSError's reason quoted the absolute path
    ("[Errno 13] Permission denied: '...Jane_Doe_MRN12345_labs.pdf'"), and a file gone between
    the folder walk and its stat escaped the listing: a traceback with the path, and the rest of
    the folder never pulled. Each is now one skip, and the reason is the error's type.
    """
    folder = tmp_path / PATIENT[0]
    folder.mkdir()
    (folder / "a_first.pdf").write_bytes(pdf_bytes + b"a")
    locked = (
        folder / f"{PATIENT[1]}_labs.pdf"
    )  # another process holds it (a scanner writing)
    moved = (
        folder / f"{PATIENT[1]}_second.pdf"
    )  # moved away between the listing and its fetch
    walked = (
        folder / f"y_{PATIENT[1]}_later.pdf"
    )  # moved away between the walk and its stat
    for n, path in enumerate((locked, moved, walked)):
        path.write_bytes(pdf_bytes + bytes([n]))
    (folder / "zz_last.pdf").write_bytes(
        pdf_bytes + b"z"
    )  # after all of them in the listing
    real_read = Path.read_bytes

    def read_bytes(self):
        if self.name == locked.name:
            raise PermissionError(13, "Permission denied", str(self))
        return real_read(self)

    monkeypatch.setattr(Path, "read_bytes", read_bytes)
    real_wants = DocumentManifest.wants

    def wants(self, rel_path):
        # The clinic's own script moves files while the pull runs: one after its listing, one
        # before the walk reaches its stat (the listing stats each file as it comes up).
        if rel_path.endswith("_labs.pdf"):
            moved.unlink()
            walked.unlink()
        return real_wants(self, rel_path)

    monkeypatch.setattr(DocumentManifest, "wants", wants)
    manifest_writer(folder, settings.manifest_dir)
    env = {
        "NICKEL_TENANT_ID": str(settings.tenant_id),
        "NICKEL_DATA_DIR": str(settings.data_dir),
    }
    out = io.StringIO()
    code = main(
        ["documents", "pull", str(settings.manifest_dir / "local-docs.yaml")], env, out
    )
    text = out.getvalue()
    assert code == 0  # not an aborted pull
    assert "published       2" in text  # the files before and after them both came in
    assert "skipped         3" in text
    assert sorted(line for line in text.splitlines() if line.startswith("  - ")) == [
        "  - fetch failed: FileNotFoundError",
        "  - fetch failed: FileNotFoundError",
        "  - fetch failed: PermissionError",
    ]
    assert [name for name in PATIENT if name in text] == []
    assert _leaks(records) == []
    # With -v the operator asked for the paths: the path inside the source, not the OS's text.
    for name, data in ((moved.name, b"m"), (walked.name, b"w")):
        (folder / name).write_bytes(pdf_bytes + data)
    out = io.StringIO()
    main(
        ["-v", "documents", "pull", str(settings.manifest_dir / "local-docs.yaml")],
        env,
        out,
    )
    assert f"  - {locked.name}: fetch failed: PermissionError" in out.getvalue()
    assert str(tmp_path) not in out.getvalue()
