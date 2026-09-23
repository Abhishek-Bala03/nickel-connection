import dataclasses
import errno
import json
import os
import sqlite3
import subprocess
import sys
import threading
import time
import types
from pathlib import Path

import pytest

from src.config import APP_DIR
from src.db import outbox as outbox_module
from src.db.file_store import LocalFileStore, local
from src.db.jobs import Job, JobStatus, JobStore
from src.db.layout import DataLayout
from src.db.outbox import JsonlOutbox, PublishError
from src.db.receipts import Receipt, ReceiptStatus, ReceiptStore

TENANT = "0192a3b4-0000-7000-8000-000000000001"
STAMP = "2026-09-22T00:00:00+00:00"


# ── Layout ───────────────────────────────────────────────────────────────────
def test_the_data_layout_is_one_root(tmp_path):
    layout = DataLayout(tmp_path)
    assert layout.filestore == tmp_path / "filestore"
    assert layout.outbox == tmp_path / "outbox"
    assert layout.receipts_db == tmp_path / "receipts.sqlite"
    assert layout.jobs_db == tmp_path / "jobs.sqlite"
    # Nothing derived from a document's content lives here: the connection layer keeps originals,
    # envelopes, receipts and job records, and no more.
    assert {name for name in dir(layout) if not name.startswith("_")} == {
        "root",
        "filestore",
        "outbox",
        "receipts_db",
        "jobs_db",
    }


# ── File Store ───────────────────────────────────────────────────────────────
def test_put_is_write_once_and_keeps_metadata(tmp_path):
    store = LocalFileStore(tmp_path)
    key = "t/documents/ab/abc"
    assert store.put(
        key, b"one", content_type="application/pdf", metadata={"source": "s"}
    )
    assert not store.put(key, b"two", content_type="application/pdf", metadata={})
    assert store.get(key) == b"one"
    assert store.metadata(key) == {
        "bytes": 3,
        "content_type": "application/pdf",
        "source": "s",
    }
    assert not list(tmp_path.rglob("*.tmp"))


@pytest.mark.parametrize("key", ["", "/abs", "a/../b", "a//b", "a\\b", "./a"])
def test_unsafe_keys_are_rejected(tmp_path, key):
    store = LocalFileStore(tmp_path)
    with pytest.raises(ValueError, match="invalid object key"):
        store.get(key)


def test_the_file_store_interface_is_put_and_get():
    """``exists`` was dead code (nothing but a test called it); the Document worker reads originals
    with ``get``. The ingest bridge's size cap is the one setting, not an alias of it.
    """
    from src.handler import ingest

    assert not hasattr(LocalFileStore, "exists")
    assert not hasattr(ingest, "DEFAULT_MAX_BYTES")


def test_a_crash_at_the_first_rename_cannot_strand_an_object(tmp_path, monkeypatch):
    real_replace = os.replace
    renamed = []

    def crash_after_the_first_rename(src, dst):
        real_replace(src, dst)  # the rename lands, then the power goes
        renamed.append(str(dst))
        if len(renamed) == 1:
            raise OSError("power cut")

    monkeypatch.setattr(local.os, "replace", crash_after_the_first_rename)
    store = LocalFileStore(tmp_path)
    key = "t/documents/ab/abc"
    with pytest.raises(OSError, match="power cut"):
        store.put(key, b"one", content_type="application/pdf", metadata={})
    # Whatever landed first must not be the object without its metadata: the next put writes the
    # object whole, with its sidecar, instead of returning False for ever.
    assert store.put(key, b"one", content_type="application/pdf", metadata={})
    assert store.get(key) == b"one"
    assert store.metadata(key) == {"bytes": 3, "content_type": "application/pdf"}
    assert not list(tmp_path.rglob("*.tmp"))


DIRECTORY_FD = 987654  # never a real descriptor


def _durability_spies(monkeypatch, events, *, directories):
    """Record the fsyncs and renames a put makes. Directory fsyncs go through os.open, os.fsync and
    os.close on a fake descriptor, so the POSIX path runs on any OS; ``directories`` sets the OS
    switch the way the tests want it."""
    real_fsync, real_close, real_replace = os.fsync, os.close, os.replace

    def opened(path, flags, *args):
        assert flags == os.O_RDONLY
        assert Path(path).is_dir()
        events.append(("dir", Path(path)))
        return DIRECTORY_FD

    def fsync(fd):
        if fd == DIRECTORY_FD:
            events.append("dir-fsync")
            return
        real_fsync(fd)
        events.append("fsync")

    def close(fd):
        if fd != DIRECTORY_FD:
            real_close(fd)

    def replace(src, dst):
        real_replace(src, dst)
        events.append(
            ("rename", str(dst).rsplit(".", 1)[-1] if "." in str(dst) else "object")
        )

    monkeypatch.setattr(local, "_FSYNC_DIRECTORIES", directories)
    monkeypatch.setattr(local.os, "open", opened)
    monkeypatch.setattr(local.os, "fsync", fsync)
    monkeypatch.setattr(local.os, "close", close)
    monkeypatch.setattr(local.os, "replace", replace)


def test_put_fsyncs_each_file_before_its_rename(tmp_path, monkeypatch):
    events = []
    _durability_spies(monkeypatch, events, directories=False)  # as on Windows
    LocalFileStore(tmp_path).put(
        "t/documents/ab/abc", b"one", content_type="application/pdf", metadata={}
    )
    # sidecar first (fsync, rename), object second (fsync, rename); no directory is opened
    assert events == ["fsync", ("rename", "json"), "fsync", ("rename", "object")]


def test_on_posix_each_rename_and_each_new_directory_is_made_durable(
    tmp_path, monkeypatch
):
    """A power loss after put returned could undo the rename, and a published receipt never
    writes the object again: the directory is fsynced after each rename, and the parent of every
    directory mkdir created, top-down."""
    events = []
    _durability_spies(monkeypatch, events, directories=True)  # as on Linux
    root = tmp_path / "fs"  # the root itself does not exist yet
    LocalFileStore(root).put(
        "t/documents/ab/abc", b"one", content_type="application/pdf", metadata={}
    )
    leaf = root / "t" / "documents" / "ab"
    assert events == [
        ("dir", tmp_path),  # fs was created in tmp_path
        "dir-fsync",
        ("dir", root),  # t in fs
        "dir-fsync",
        ("dir", root / "t"),  # documents in t
        "dir-fsync",
        ("dir", root / "t" / "documents"),  # ab in documents
        "dir-fsync",
        "fsync",
        ("rename", "json"),
        ("dir", leaf),
        "dir-fsync",
        "fsync",
        ("rename", "object"),
        ("dir", leaf),
        "dir-fsync",
    ]
    # A second object in the same directory creates nothing: only the two renames are made durable.
    del events[:]
    LocalFileStore(root).put(
        "t/documents/ab/abd", b"two", content_type="application/pdf", metadata={}
    )
    assert [e for e in events if isinstance(e, tuple) and e[0] == "dir"] == [
        ("dir", leaf),
        ("dir", leaf),
    ]


def test_the_directory_fsync_switch_follows_the_os():
    assert local._FSYNC_DIRECTORIES is (os.name == "posix")


# ── Outbox ───────────────────────────────────────────────────────────────────
def test_outbox_appends_one_line_per_message(tmp_path):
    outbox = JsonlOutbox(tmp_path / "outbox")
    outbox.publish("raw_data", "k1", {"a": 1})
    outbox.publish("raw_data", "k2", {"a": 2})
    rows = outbox.read("raw_data")
    assert rows[0] == {"topic": "raw_data", "key": "k1", "value": {"a": 1}}
    assert [row["key"] for row in rows] == ["k1", "k2"]


def test_topics_are_separate_files(tmp_path):
    outbox = JsonlOutbox(tmp_path)
    outbox.publish("raw_data", "k", {"a": 1})
    outbox.publish("other_topic", "k", {"b": 2})
    assert [row["value"] for row in outbox.read("other_topic")] == [{"b": 2}]
    assert (tmp_path / "other_topic.jsonl").is_file()


def test_reading_an_unused_topic_is_empty(tmp_path):
    assert JsonlOutbox(tmp_path).read("raw_data") == []


def test_topic_names_are_validated(tmp_path):
    outbox = JsonlOutbox(tmp_path)
    with pytest.raises(ValueError, match="topic"):
        outbox.publish("../evil", "k", {})


def test_a_write_failure_is_a_publish_error_that_names_no_path(tmp_path, records):
    blocker = tmp_path / "not-a-folder"
    blocker.write_text("x")
    outbox = JsonlOutbox(blocker)
    with pytest.raises(PublishError) as caught:
        outbox.publish("raw_data", "k", {})
    # A receipt keeps this text as its detail and GET /documents shows it: the topic and the
    # error's type, never the OS message, which names the outbox file and goes to the log.
    cause = type(caught.value.__cause__).__name__
    assert str(caught.value) == f"could not append to the raw_data outbox: {cause}"
    assert blocker.name not in str(caught.value)
    # (The OS quotes the path with repr, so on Windows its backslashes come doubled: the name.)
    assert [blocker.name in r.getMessage() for r in records] == [True]


def test_the_bytes_on_disk_are_the_same_on_every_os(tmp_path):
    outbox = JsonlOutbox(tmp_path)
    outbox.publish("raw_data", "k", {"n": 1}, idempotency_key="m-1")
    outbox.publish("raw_data", "k", {"n": 2})
    assert (tmp_path / "raw_data.jsonl").read_bytes() == (
        b'{"topic":"raw_data","key":"k","id":"m-1","value":{"n":1}}\n'
        b'{"topic":"raw_data","key":"k","value":{"n":2}}\n'
    )  # binary append: no \r\n on Windows


def test_each_append_holds_the_topic_lock_around_the_check_the_write_and_the_fsync(
    tmp_path, monkeypatch
):
    events = []
    real_fsync = os.fsync
    monkeypatch.setattr(outbox_module, "_LOCK", lambda fd: events.append("lock"))
    monkeypatch.setattr(outbox_module, "_UNLOCK", lambda fd: events.append("unlock"))
    monkeypatch.setattr(
        outbox_module, "_torn_tail", lambda path: events.append("tail check") or False
    )

    def fsync(fd):
        real_fsync(fd)
        events.append("fsync")

    monkeypatch.setattr(outbox_module.os, "fsync", fsync)
    JsonlOutbox(tmp_path).publish("raw_data", "k", {"n": 1})
    assert events == ["lock", "tail check", "fsync", "unlock"]
    assert (tmp_path / "raw_data.jsonl.lock").is_file()


def test_the_lock_is_flock_on_posix_and_msvcrt_locking_on_windows(monkeypatch):
    """Both sides run here, whatever the OS: the modules are injected."""
    calls = []
    posix = types.SimpleNamespace(
        LOCK_EX=2, LOCK_UN=8, flock=lambda fd, op: calls.append(("flock", fd, op))
    )
    monkeypatch.setitem(sys.modules, "fcntl", posix)
    lock, unlock = outbox_module._lock_functions()
    lock(7)
    unlock(7)
    assert calls == [("flock", 7, 2), ("flock", 7, 8)]  # LOCK_EX blocks until held

    calls.clear()
    busy = [None, None]  # the first two tries find the byte locked by another process

    def locking(fd, mode, nbytes):
        calls.append(("locking", fd, mode, nbytes))
        if mode == 2 and busy:  # LK_NBLCK: refused at once, not after a second's sleep
            busy.pop()
            raise OSError(13, "Permission denied")

    windows = types.SimpleNamespace(LK_NBLCK=2, LK_UNLCK=0, locking=locking)
    monkeypatch.setitem(sys.modules, "fcntl", None)  # no fcntl: Windows
    monkeypatch.setitem(sys.modules, "msvcrt", windows)
    monkeypatch.setattr(outbox_module, "_LOCK_RETRY_S", 0)
    lock, unlock = outbox_module._lock_functions()
    lock(7)
    unlock(7)
    assert calls == [("locking", 7, 2, 1)] * 3 + [("locking", 7, 0, 1)]


def test_a_lock_that_cannot_be_taken_is_a_publish_error_not_a_hang(
    tmp_path, monkeypatch
):
    """Windows: only a locking violation (another process holds the byte) is waited out. Any
    other failure, a bad descriptor or a file system without byte-range locks, never clears: it
    is raised at once and the publish fails as a PublishError, instead of the job thread polling
    the lock for ever with nothing appended and nothing reported.
    """
    tries = []

    def locking(fd, mode, nbytes):
        tries.append(mode)
        if len(tries) > 20:
            raise RuntimeError("still polling: the lock loop never gives up")
        raise OSError(errno.EBADF, "Bad file descriptor")

    windows = types.SimpleNamespace(LK_NBLCK=2, LK_UNLCK=0, locking=locking)
    monkeypatch.setitem(sys.modules, "fcntl", None)
    monkeypatch.setitem(sys.modules, "msvcrt", windows)
    monkeypatch.setattr(outbox_module, "_LOCK_RETRY_S", 0)
    lock, _unlock = outbox_module._lock_functions()
    monkeypatch.setattr(outbox_module, "_LOCK", lock)
    with pytest.raises(PublishError, match="raw_data outbox"):
        JsonlOutbox(tmp_path).publish("raw_data", "k", {"n": 1})
    assert tries == [2]  # raised on the first try, not retried
    assert not (tmp_path / "raw_data.jsonl").exists()  # nothing was appended


_WRITER = """
import sys
from pathlib import Path
from src.db.outbox import JsonlOutbox
directory, worker, count = Path(sys.argv[1]), sys.argv[2], int(sys.argv[3])
outbox = JsonlOutbox(directory)
for n in range(count):
    outbox.publish("raw_data", worker, {"n": n}, idempotency_key=f"{worker}-{n}")
"""


def test_writers_in_several_processes_lose_no_record(tmp_path):
    """Four processes appending to one outbox at once: every record is there, whole. Without the
    lock, the review's run of 4 x 300 on Windows kept 754-824 of 1200 records."""
    workers, count = 4, 75
    processes = [
        subprocess.Popen(
            [sys.executable, "-c", _WRITER, str(tmp_path), f"w{i}", str(count)],
            cwd=APP_DIR,
            env={**os.environ, "PYTHONPATH": str(APP_DIR)},
        )
        for i in range(workers)
    ]
    assert [process.wait(timeout=120) for process in processes] == [0] * workers
    raw = (tmp_path / "raw_data.jsonl").read_bytes()
    assert b"\r\n" not in raw
    lines = raw.split(b"\n")
    assert lines.pop() == b""  # the file ends on a newline: no torn tail
    assert len(lines) == workers * count
    ids = [json.loads(line)["id"] for line in lines]  # every line parses whole
    assert sorted(ids) == sorted(
        f"w{i}-{n}" for i in range(workers) for n in range(count)
    )


# ── Receipts ─────────────────────────────────────────────────────────────────
def _receipt(ingest_id="i1", source="s", message_id="m", status=ReceiptStatus.RECEIVED):
    return Receipt(
        ingest_id=ingest_id,
        tenant_id=TENANT,
        source=source,
        message_id=message_id,
        payload_sha256="ab" * 32,
        status=status,
        received_at=STAMP,
        updated_at=STAMP,
        payload_ref="k",
    )


def test_a_receipt_is_built_by_keyword_only():
    # Ten strings in a row: a swapped source and message id would pass every type check.
    with pytest.raises(TypeError):
        Receipt("i1", TENANT, "s", "m", "ab" * 32, ReceiptStatus.RECEIVED, STAMP, STAMP)


def test_receipt_add_find_get(receipts):
    receipts.add(_receipt())
    assert receipts.find(TENANT, "s", "m", "ab" * 32).ingest_id == "i1"
    assert receipts.get("i1", tenant_id=TENANT).status is ReceiptStatus.RECEIVED
    assert receipts.find(TENANT, "s", "other", "ab" * 32) is None
    assert receipts.get("missing", tenant_id=TENANT) is None


def test_one_receipt_per_tenant_source_message_and_hash(receipts):
    assert receipts.add(_receipt())
    same_item_again = _receipt(ingest_id="i2")
    # The unique index decides quietly: no IntegrityError, the first receipt stays.
    assert not receipts.add(same_item_again)
    assert receipts.get("i2", tenant_id=TENANT) is None
    assert receipts.find(TENANT, "s", "m", "ab" * 32).ingest_id == "i1"


def test_status_moves_on_and_unknown_ids_fail(receipts):
    receipts.add(_receipt())
    receipts.set_status("i1", ReceiptStatus.PUBLISH_FAILED, detail="broker down")
    moved = receipts.get("i1", tenant_id=TENANT)
    assert (moved.status, moved.detail) == (ReceiptStatus.PUBLISH_FAILED, "broker down")
    with pytest.raises(KeyError):
        receipts.set_status("missing", ReceiptStatus.PUBLISHED)


def test_the_envelope_is_written_with_the_receipt_and_kept_across_updates(receipts):
    receipts.add(dataclasses.replace(_receipt(), envelope='{"as": "first built"}'))
    assert receipts.get("i1", tenant_id=TENANT).envelope == '{"as": "first built"}'
    # A status update without an envelope keeps it; one with a new envelope replaces it.
    receipts.set_status("i1", ReceiptStatus.PUBLISH_FAILED, detail="broker down")
    assert receipts.get("i1", tenant_id=TENANT).envelope == '{"as": "first built"}'
    receipts.set_status("i1", ReceiptStatus.RECEIVED, envelope='{"as": "rebuilt"}')
    assert receipts.get("i1", tenant_id=TENANT).envelope == '{"as": "rebuilt"}'
    assert receipts.find(TENANT, "s", "m", "ab" * 32).envelope == '{"as": "rebuilt"}'


def test_counts_and_recent_per_source(receipts):
    receipts.add(_receipt("i1", "a", "m1", ReceiptStatus.PUBLISHED))
    receipts.add(_receipt("i2", "a", "m2", ReceiptStatus.REJECTED))
    receipts.add(_receipt("i3", "b", "m3", ReceiptStatus.PUBLISHED))
    assert receipts.counts(tenant_id=TENANT) == {"published": 2, "rejected": 1}
    assert receipts.counts("a", tenant_id=TENANT) == {"published": 1, "rejected": 1}
    assert len(receipts.recent(tenant_id=TENANT)) == 3
    assert [r.source for r in receipts.recent("b", limit=5, tenant_id=TENANT)] == ["b"]


def test_recent_filters_by_status_and_with_status_is_oldest_first(receipts):
    receipts.add(_receipt("i1", "a", "m1", ReceiptStatus.PUBLISHED))
    receipts.add(_receipt("i2", "a", "m2", ReceiptStatus.REJECTED))
    receipts.add(_receipt("i3", "b", "m3", ReceiptStatus.PUBLISHED))
    rejected = receipts.recent(status=ReceiptStatus.REJECTED, tenant_id=TENANT)
    assert [r.ingest_id for r in rejected] == ["i2"]
    assert [
        r.ingest_id
        for r in receipts.recent("a", status=ReceiptStatus.PUBLISHED, tenant_id=TENANT)
    ] == ["i1"]
    assert [
        r.ingest_id
        for r in receipts.with_status(ReceiptStatus.PUBLISHED, tenant_id=TENANT)
    ] == [
        "i1",
        "i3",
    ]
    assert [
        r.ingest_id
        for r in receipts.with_status(ReceiptStatus.PUBLISHED, "b", tenant_id=TENANT)
    ] == ["i3"]


def test_a_publish_attempt_is_counted_on_open_receipts_only(receipts):
    receipts.add(_receipt("i1"))
    receipts.add(_receipt("i2", "s", "m2", ReceiptStatus.PUBLISHED))
    assert receipts.begin_attempt("i1") == 1
    assert receipts.begin_attempt("i1") == 2
    assert receipts.get("i1", tenant_id=TENANT).attempts == 2
    assert receipts.begin_attempt("i2") is None  # final: nothing left to publish
    assert receipts.get("i2", tenant_id=TENANT).attempts == 0
    with pytest.raises(KeyError):
        receipts.begin_attempt("missing")


def test_receipt_files_from_before_the_attempt_count_and_the_envelope_are_extended(
    tmp_path,
):
    path = tmp_path / "receipts.sqlite"
    with sqlite3.connect(path) as db:
        db.execute(
            "CREATE TABLE ingest_receipts (ingest_id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, "
            "source TEXT NOT NULL, message_id TEXT NOT NULL, payload_sha256 TEXT NOT NULL, "
            "status TEXT NOT NULL, received_at TEXT NOT NULL, updated_at TEXT NOT NULL, "
            "payload_ref TEXT, detail TEXT, "
            "UNIQUE (tenant_id, source, message_id, payload_sha256))"
        )
        db.execute(
            "INSERT INTO ingest_receipts VALUES ('i-old', ?, 's', 'm', ?, 'received', ?, ?, "
            "'k', NULL)",
            (TENANT, "ab" * 32, STAMP, STAMP),
        )
    with ReceiptStore(path) as receipts:
        old = receipts.get("i-old", tenant_id=TENANT)
        assert (old.attempts, old.envelope) == (0, None)  # no envelope to republish
        assert receipts.begin_attempt("i-old") == 1
        assert receipts.set_status("i-old", ReceiptStatus.RECEIVED, envelope="{}")
    with ReceiptStore(
        path
    ) as receipts:  # opened again: the columns are not added twice
        extended = receipts.get("i-old", tenant_id=TENANT)
        assert (extended.attempts, extended.envelope) == (1, "{}")


def test_two_processes_opening_an_old_receipt_file_add_the_columns_once(tmp_path):
    """The first open after an upgrade adds the new columns. Another process (the CLI next to the
    service) that opens the file meanwhile must not read the old columns, then fail on the second
    ALTER with "duplicate column name": the check and the upgrade happen under the write lock.
    """
    path = tmp_path / "receipts.sqlite"
    other = sqlite3.connect(path, isolation_level=None)
    other.execute("PRAGMA journal_mode = WAL")
    other.execute(
        "CREATE TABLE ingest_receipts (ingest_id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, "
        "source TEXT NOT NULL, message_id TEXT NOT NULL, payload_sha256 TEXT NOT NULL, "
        "status TEXT NOT NULL, received_at TEXT NOT NULL, updated_at TEXT NOT NULL, "
        "payload_ref TEXT, detail TEXT, "
        "UNIQUE (tenant_id, source, message_id, payload_sha256))"
    )
    other.execute(
        "BEGIN IMMEDIATE"
    )  # the other process is upgrading the file right now
    failures: list[BaseException] = []

    def open_store():
        try:
            ReceiptStore(path).close()
        except BaseException as exc:  # reported below, not lost in the thread
            failures.append(exc)

    opener = threading.Thread(target=open_store)
    opener.start()
    time.sleep(0.3)  # the opener is now waiting on the other process's write lock
    other.execute(
        "ALTER TABLE ingest_receipts ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0"
    )
    other.execute("ALTER TABLE ingest_receipts ADD COLUMN envelope TEXT")
    other.execute("COMMIT")
    opener.join(timeout=10)
    other.close()
    assert failures == []
    with ReceiptStore(path) as receipts:
        receipts.add(_receipt())
        assert receipts.get("i1", tenant_id=TENANT).attempts == 0


def test_a_receipt_store_closes_with_its_block(tmp_path):
    with ReceiptStore(tmp_path / "r.sqlite") as receipts:
        receipts.add(_receipt())
    with pytest.raises(sqlite3.ProgrammingError):
        receipts.get("i1", tenant_id=TENANT)


# ── Job records ──────────────────────────────────────────────────────────────
def _job(job_id="j1", status=JobStatus.QUEUED, created_at=STAMP):
    return Job(job_id, "documents.pull", status, {"manifest": "m"}, created_at)


def test_job_records_round_trip(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite")
    store.add(_job())
    assert store.get("j1") == _job()
    assert store.get("missing") is None
    store.start("j1", "2026-09-22T00:00:01+00:00")
    assert store.get("j1").status is JobStatus.RUNNING
    store.finish(
        "j1", JobStatus.SUCCEEDED, {"published": 2}, None, "2026-09-22T00:00:02+00:00"
    )
    finished = store.get("j1")
    assert finished.status is JobStatus.SUCCEEDED
    assert finished.counts == {"published": 2}
    assert finished.started_at == "2026-09-22T00:00:01+00:00"
    assert finished.finished_at == "2026-09-22T00:00:02+00:00"
    assert finished.error is None


def test_jobs_are_listed_newest_first(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite")
    store.add(_job("j1", created_at="2026-09-22T00:00:01+00:00"))
    store.add(_job("j2", created_at="2026-09-22T00:00:02+00:00"))
    assert [job.job_id for job in store.recent()] == ["j2", "j1"]
    assert [job.job_id for job in store.recent(limit=1)] == ["j2"]


def test_only_queued_jobs_start_and_only_running_jobs_finish(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite")
    store.add(_job("j1", JobStatus.SUCCEEDED))
    store.start("j1", STAMP)
    assert store.get("j1").status is JobStatus.SUCCEEDED
    store.finish("j1", JobStatus.FAILED, {}, "late", STAMP)
    assert store.get("j1").error is None


def test_fail_settles_an_open_job_and_leaves_a_finished_one(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite")
    store.add(_job("j-queued"))
    store.add(_job("j-running", JobStatus.RUNNING))
    store.add(_job("j-done", JobStatus.SUCCEEDED))
    assert store.fail("j-queued", "could not start", STAMP)
    assert store.fail("j-running", "could not start", STAMP)
    assert not store.fail("j-done", "late", STAMP)
    assert not store.fail("missing", "late", STAMP)
    failed = store.get("j-queued")
    assert (failed.status, failed.error, failed.finished_at) == (
        JobStatus.FAILED,
        "could not start",
        STAMP,
    )
    assert store.get("j-running").status is JobStatus.FAILED
    assert (store.get("j-done").status, store.get("j-done").error) == (
        JobStatus.SUCCEEDED,
        None,
    )


def test_recovery_interrupts_running_jobs_and_returns_the_queued_ones(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite")
    store.add(_job("j-running", JobStatus.RUNNING))
    store.add(_job("j-done", JobStatus.SUCCEEDED))
    store.add(_job("j-queued-2", created_at="2026-09-22T00:00:02+00:00"))
    store.add(_job("j-queued-1", created_at="2026-09-22T00:00:01+00:00"))
    assert store.recover("2026-09-23T00:00:00+00:00") == (
        1,
        ["j-queued-1", "j-queued-2"],
    )
    interrupted = store.get("j-running")
    assert (interrupted.status, interrupted.error) == (JobStatus.FAILED, "interrupted")
    assert interrupted.finished_at == "2026-09-23T00:00:00+00:00"
    assert store.get("j-done").status is JobStatus.SUCCEEDED


# ── Outbox delivery: at-least-once ───────────────────────────────────────────
def test_a_repeat_is_appended_again_under_the_same_id(tmp_path):
    """At-least-once: the publisher does not look for repeats (the scan before each append was a
    check-then-act with no lock across processes, and read the whole file every run). The line
    keeps the message's id and the envelope its dedup key, and consumers drop the repeat.
    """
    outbox = JsonlOutbox(tmp_path)
    outbox.publish("raw_data", "k", {"n": 0})  # no key
    outbox.publish("raw_data", "k", {"n": 1}, idempotency_key="m-1")
    outbox.publish("raw_data", "k", {"n": 1}, idempotency_key="m-1")  # a retry
    outbox.publish("raw_data", "k", {"n": 2}, idempotency_key="m-2")
    rows = JsonlOutbox(tmp_path).read("raw_data")  # another process reads the same
    assert [(row.get("id"), row["value"]["n"]) for row in rows] == [
        (None, 0),
        ("m-1", 1),
        ("m-1", 1),
        ("m-2", 2),
    ]
    assert not hasattr(JsonlOutbox, "holds")


def test_a_torn_line_left_by_a_crashed_writer_does_not_swallow_the_next_message(
    tmp_path, records
):
    outbox = JsonlOutbox(tmp_path)
    outbox.publish("raw_data", "k", {"n": 1}, idempotency_key="m-1")
    path = tmp_path / "raw_data.jsonl"
    with path.open("ab") as handle:  # the disk filled, or the power went, mid-line
        handle.write(b'{"topic":"raw_data","key":"k","id":"m-torn"')
    # The torn line was never acknowledged (its publish raised), so its receipt is still open and it
    # comes back under the same id. The message published after it must not be glued to it: it is
    # durable, so it must be readable, here and by another process.
    outbox.publish("raw_data", "k", {"n": 2}, idempotency_key="m-2")
    with path.open("ab") as handle:
        handle.write(b"\n")  # a blank line is nothing
    assert [row["id"] for row in outbox.read("raw_data")] == ["m-1", "m-2"]
    assert [row["id"] for row in JsonlOutbox(tmp_path).read("raw_data")] == [
        "m-1",
        "m-2",
    ]
    assert any("torn line" in r.getMessage() for r in records)


# ── Receipt transitions, tenant filters, storage settings ────────────────────
def test_terminal_receipts_do_not_move(receipts):
    receipts.add(_receipt("i1", status=ReceiptStatus.PUBLISHED))
    receipts.add(_receipt("i2", "s", "m2", ReceiptStatus.REJECTED))
    receipts.add(_receipt("i3", "s", "m3", ReceiptStatus.TOO_LARGE))
    receipts.add(_receipt("i4", "s", "m4", ReceiptStatus.PUBLISH_FAILED))
    # published and rejected are final here: a late or stale update is refused, quietly
    assert not receipts.set_status("i1", ReceiptStatus.PUBLISH_FAILED, detail="late")
    assert not receipts.set_status("i2", ReceiptStatus.PUBLISHED)
    assert (
        receipts.get("i1", tenant_id=TENANT).status,
        receipts.get("i1", tenant_id=TENANT).detail,
    ) == (
        ReceiptStatus.PUBLISHED,
        None,
    )
    assert receipts.get("i2", tenant_id=TENANT).status is ReceiptStatus.REJECTED
    # a size reject is retried once the cap is raised; a failed publish is retried
    assert receipts.set_status("i3", ReceiptStatus.PUBLISHED)
    assert receipts.set_status("i4", ReceiptStatus.REJECTED, detail="not a pdf now")
    assert receipts.get("i3", tenant_id=TENANT).status is ReceiptStatus.PUBLISHED
    assert receipts.get("i4", tenant_id=TENANT).status is ReceiptStatus.REJECTED
    assert not receipts.set_status("i4", ReceiptStatus.PUBLISHED)


def test_receipts_are_read_per_tenant(receipts):
    other = "0192a3b4-0000-7000-8000-000000000002"
    receipts.add(_receipt("i1", "a", "m1", ReceiptStatus.PUBLISHED))
    receipts.add(
        Receipt(
            ingest_id="i2",
            tenant_id=other,
            source="a",
            message_id="m1",
            payload_sha256="ab" * 32,
            status=ReceiptStatus.PUBLISHED,
            received_at=STAMP,
            updated_at=STAMP,
        )
    )
    receipts.add(
        Receipt(
            ingest_id="i3",
            tenant_id=other,
            source="a",
            message_id="m2",
            payload_sha256="cd" * 32,
            status=ReceiptStatus.REJECTED,
            received_at=STAMP,
            updated_at=STAMP,
        )
    )
    assert receipts.get("i2", tenant_id=TENANT) is None
    assert receipts.get("i2", tenant_id=other).ingest_id == "i2"
    assert receipts.counts(tenant_id=other) == {"published": 1, "rejected": 1}
    assert receipts.counts("a", tenant_id=TENANT) == {"published": 1}
    assert [r.ingest_id for r in receipts.recent(tenant_id=other)] == ["i3", "i2"]
    assert [
        r.ingest_id
        for r in receipts.recent("a", status=ReceiptStatus.PUBLISHED, tenant_id=other)
    ] == ["i2"]
    assert [
        r.ingest_id
        for r in receipts.with_status(ReceiptStatus.PUBLISHED, tenant_id=TENANT)
    ] == ["i1"]
    assert [
        r.ingest_id
        for r in receipts.with_status(ReceiptStatus.PUBLISHED, "a", tenant_id=other)
    ] == ["i2"]
    # A tenant never sees another's rows, and there is no read that spans tenants.
    assert receipts.counts(tenant_id=TENANT) == {"published": 1}
    assert receipts.get("i1", tenant_id=other) is None
    assert [r.ingest_id for r in receipts.recent(tenant_id=TENANT)] == ["i1"]


@pytest.mark.parametrize(
    "read",
    [
        lambda receipts: receipts.get("i1"),
        lambda receipts: receipts.counts(),
        lambda receipts: receipts.recent(),
        lambda receipts: receipts.with_status(ReceiptStatus.PUBLISHED),
    ],
)
def test_every_read_names_a_tenant(receipts, read):
    """A missing tenant is a TypeError, not everyone's receipts: the old ``(? IS NULL OR
    tenant_id = ?)`` filter matched every tenant when a caller left it out, as two did.
    """
    receipts.add(_receipt("i1", status=ReceiptStatus.PUBLISHED))
    with pytest.raises(TypeError, match="tenant_id"):
        read(receipts)


def test_the_receipts_file_is_shared_safely(tmp_path):
    with ReceiptStore(tmp_path / "receipts.sqlite") as receipts:
        db = receipts._db
        assert db.execute("PRAGMA journal_mode").fetchone() == ("wal",)
        assert db.execute("PRAGMA busy_timeout").fetchone() == (5000,)
        indexes = {
            row[1]: row for row in db.execute("PRAGMA index_list(ingest_receipts)")
        }
        columns = [
            row[2]
            for row in db.execute(
                "PRAGMA index_info(ix_ingest_receipts_status_source_received)"
            )
        ]
        assert "ix_ingest_receipts_status_source_received" in indexes
        assert columns == ["status", "source", "received_at"]


def test_a_held_receipt_is_open(receipts):
    receipts.add(_receipt("i1", status=ReceiptStatus.HELD))
    assert receipts.set_status("i1", ReceiptStatus.PUBLISHED)
    assert receipts.get("i1", tenant_id=TENANT).status is ReceiptStatus.PUBLISHED
    assert not receipts.set_status(
        "i1", ReceiptStatus.HELD
    )  # never back from published
