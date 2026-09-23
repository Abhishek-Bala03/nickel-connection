"""Job records for the jobs API: what was queued, when it ran, and how it ended.

SQLite under the data root. Every call opens its own short connection, so the API's request threads and
the worker thread share the store safely. Counts and an error message only: never document content.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any


class JobStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


# The error of a job a previous process left running: the next start fails it with this.
INTERRUPTED = "interrupted"


@dataclass(frozen=True)
class Job:
    job_id: str
    kind: str
    status: JobStatus
    params: dict[str, Any]
    created_at: str
    started_at: str | None = None
    finished_at: str | None = None
    counts: dict[str, int] = field(default_factory=dict)
    error: str | None = None
    # The caller's Idempotency-Key: one job per key and kind (src.handler.jobs.JobRunner.submit).
    idempotency_key: str | None = None


_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id          TEXT PRIMARY KEY,
    kind            TEXT NOT NULL,
    status          TEXT NOT NULL,
    params          TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    started_at      TEXT,
    finished_at     TEXT,
    counts          TEXT NOT NULL,
    error           TEXT,
    idempotency_key TEXT
)
"""
# Records from before the key (NIK-2209-05) get the column; a unique index lets NULLs repeat, so jobs
# without a key are unconstrained and two with the same key and kind cannot exist, whatever the caller.
_ADD_KEY_COLUMN = "ALTER TABLE jobs ADD COLUMN idempotency_key TEXT"
_KEY_INDEX = (
    "CREATE UNIQUE INDEX IF NOT EXISTS jobs_kind_idempotency_key "
    "ON jobs (kind, idempotency_key)"
)
_INSERT = (
    "INSERT INTO jobs (job_id, kind, status, params, created_at, started_at, finished_at, counts, "
    "error, idempotency_key) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)
_GET = (
    "SELECT job_id, kind, status, params, created_at, started_at, finished_at, counts, error, "
    "idempotency_key FROM jobs WHERE job_id = ?"
)
_FIND = (
    "SELECT job_id, kind, status, params, created_at, started_at, finished_at, counts, error, "
    "idempotency_key FROM jobs WHERE kind = ? AND idempotency_key = ?"
)
_RECENT = (
    "SELECT job_id, kind, status, params, created_at, started_at, finished_at, counts, error, "
    "idempotency_key FROM jobs ORDER BY created_at DESC, rowid DESC LIMIT ?"
)
# Statuses are passed as parameters (the JobStatus values), never spelled into the SQL.
_QUEUED = "SELECT job_id FROM jobs WHERE status = ? ORDER BY created_at ASC, rowid ASC"
_START = "UPDATE jobs SET status = ?, started_at = ? WHERE job_id = ? AND status = ?"
_FINISH = (
    "UPDATE jobs SET status = ?, counts = ?, error = ?, finished_at = ? "
    "WHERE job_id = ? AND status = ?"
)
_INTERRUPT = "UPDATE jobs SET status = ?, error = ?, finished_at = ? WHERE status = ?"
# A job that left the queue but could not start: from queued (its record could not be read) or
# running (the start landed, the read did not), never from a finished state.
_FAIL = (
    "UPDATE jobs SET status = ?, error = ?, finished_at = ? "
    "WHERE job_id = ? AND status IN (?, ?)"
)


def _to_job(row: tuple) -> Job:
    return Job(
        job_id=row[0],
        kind=row[1],
        status=JobStatus(row[2]),
        params=json.loads(row[3]),
        created_at=row[4],
        started_at=row[5],
        finished_at=row[6],
        counts=json.loads(row[7]),
        error=row[8],
        idempotency_key=row[9],
    )


class JobStore:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as db:
            db.execute(_SCHEMA)
            columns = {row[1] for row in db.execute("PRAGMA table_info(jobs)")}
            if "idempotency_key" not in columns:
                db.execute(_ADD_KEY_COLUMN)
            db.execute(_KEY_INDEX)

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        db = sqlite3.connect(self.path)
        try:
            with db:
                yield db
        finally:
            db.close()

    def add(self, job: Job) -> None:
        with self._connect() as db:
            db.execute(
                _INSERT,
                (
                    job.job_id,
                    job.kind,
                    job.status.value,
                    json.dumps(job.params, sort_keys=True),
                    job.created_at,
                    job.started_at,
                    job.finished_at,
                    json.dumps(job.counts, sort_keys=True),
                    job.error,
                    job.idempotency_key,
                ),
            )

    def get(self, job_id: str) -> Job | None:
        with self._connect() as db:
            row = db.execute(_GET, (job_id,)).fetchone()
        return _to_job(row) if row else None

    def find(self, kind: str, idempotency_key: str) -> Job | None:
        """The job queued under this key for this kind, if there is one."""
        with self._connect() as db:
            row = db.execute(_FIND, (kind, idempotency_key)).fetchone()
        return _to_job(row) if row else None

    def recent(self, limit: int = 20) -> list[Job]:
        with self._connect() as db:
            rows = db.execute(_RECENT, (limit,)).fetchall()
        return [_to_job(row) for row in rows]

    def start(self, job_id: str, at: str) -> None:
        with self._connect() as db:
            db.execute(
                _START, (JobStatus.RUNNING.value, at, job_id, JobStatus.QUEUED.value)
            )

    def finish(
        self,
        job_id: str,
        status: JobStatus,
        counts: Mapping[str, int],
        error: str | None,
        at: str,
    ) -> None:
        with self._connect() as db:
            db.execute(
                _FINISH,
                (
                    status.value,
                    json.dumps(dict(counts), sort_keys=True),
                    error,
                    at,
                    job_id,
                    JobStatus.RUNNING.value,
                ),
            )

    def fail(self, job_id: str, error: str, at: str) -> bool:
        """Fail a job that is still queued or running, with a content-free reason, and return
        True; False if there is no such open record (finished already, or never written).
        """
        with self._connect() as db:
            cursor = db.execute(
                _FAIL,
                (
                    JobStatus.FAILED.value,
                    error,
                    at,
                    job_id,
                    JobStatus.QUEUED.value,
                    JobStatus.RUNNING.value,
                ),
            )
        return cursor.rowcount == 1

    def recover(self, at: str) -> tuple[int, list[str]]:
        """At start-up: jobs left running by a previous process failed as ``interrupted``; jobs still
        queued are returned, oldest first, so the worker picks them up again."""
        with self._connect() as db:
            interrupted = db.execute(
                _INTERRUPT,
                (JobStatus.FAILED.value, INTERRUPTED, at, JobStatus.RUNNING.value),
            ).rowcount
            queued = [row[0] for row in db.execute(_QUEUED, (JobStatus.QUEUED.value,))]
        return interrupted, queued
