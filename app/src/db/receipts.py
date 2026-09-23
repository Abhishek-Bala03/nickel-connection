"""Ingest receipts (CN-03): one row per payload the connection layer received, and how far it got.

SQLite for development; the same table moves to Postgres with the other ingest tables. A receipt is
unique per (tenant, source, message id, payload hash): re-pulling the same item is a no-op, and a changed
item under the same message id is a new version with its own receipt. The receipt keeps the envelope
as first built and counts the publish attempts, so an open item can be published again without its
source (a retry, or the republish relay) and every envelope says which attempt it is.

Statuses, all set by the connection layer::

    received ──> published            the envelope is durable on raw_data (final here)
             └─> publish_failed       retried under the same ingest id on the next run
    held                              stored under shadow mode, not published; published once the
                                      manifest leaves shadow mode
    too_large                         over NICKEL_MAX_FILE_BYTES; retried once the cap is raised
    rejected                          empty, or not a declared format: the same bytes can never pass (final)

Only ``received``, ``held``, ``publish_failed`` and ``too_large`` move; ``set_status`` refuses to move
a final receipt, so a late or stale update cannot undo a publish. What happens to a message after ``published``
(validation, transform, dedup, storage) is the Transform layer's, recorded on its own tables, never here.

The file is shared by the job worker, the API threads and the CLI, each on its own connection: it runs
in WAL mode (readers never block the writer) with a busy timeout instead of an immediate "database is
locked" error.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

BUSY_TIMEOUT_MS = 5000


class ReceiptStatus(StrEnum):
    RECEIVED = "received"
    HELD = "held"
    PUBLISHED = "published"
    PUBLISH_FAILED = "publish_failed"
    TOO_LARGE = "too_large"
    REJECTED = "rejected"


# The statuses a receipt can still move from; ``published`` and ``rejected`` are final.
OPEN_STATUSES = (
    ReceiptStatus.RECEIVED,
    ReceiptStatus.HELD,
    ReceiptStatus.PUBLISH_FAILED,
    ReceiptStatus.TOO_LARGE,
)


@dataclass(frozen=True, kw_only=True)
class Receipt:
    """Built by keyword only: ten positional strings in a row is where a swapped column hides."""

    ingest_id: str
    tenant_id: str
    source: str
    message_id: str
    payload_sha256: str
    status: ReceiptStatus
    received_at: str
    updated_at: str
    payload_ref: str | None = None
    detail: str | None = None
    # Publish attempts so far; the envelope of each one carries its number.
    attempts: int = 0
    # The envelope as first built (JSON), written with the receipt, so a later run (a retry, the
    # republish relay) publishes it without the source. None for a receipt that has none: rejected,
    # too_large, or written before the envelope was kept here.
    envelope: str | None = None


_SCHEMA = """
CREATE TABLE IF NOT EXISTS ingest_receipts (
    ingest_id      TEXT PRIMARY KEY,
    tenant_id      TEXT NOT NULL,
    source         TEXT NOT NULL,
    message_id     TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL,
    status         TEXT NOT NULL,
    received_at    TEXT NOT NULL,
    updated_at     TEXT NOT NULL,
    payload_ref    TEXT,
    detail         TEXT,
    attempts       INTEGER NOT NULL DEFAULT 0,
    envelope       TEXT,
    UNIQUE (tenant_id, source, message_id, payload_sha256)
)
"""
# Files from before a column get it at open (``PRAGMA table_info`` says which are missing).
_ADDED_COLUMNS = {
    "attempts": (
        "ALTER TABLE ingest_receipts ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0"
    ),
    "envelope": "ALTER TABLE ingest_receipts ADD COLUMN envelope TEXT",
}
# What the jobs and the listing ask for: one status (and source), in received order.
_INDEX = """
CREATE INDEX IF NOT EXISTS ix_ingest_receipts_status_source_received
ON ingest_receipts (status, source, received_at)
"""
# Every statement is a complete literal with ? placeholders; nothing is assembled from values. An optional
# filter is passed twice: ``(? IS NULL OR column = ?)`` matches everything when the filter is None.
# Ties on the timestamp break on insertion order (rowid): ids minted in the same tick are unordered.
_FIND = """
SELECT ingest_id, tenant_id, source, message_id, payload_sha256, status, received_at, updated_at,
       payload_ref, detail, attempts, envelope
FROM ingest_receipts
WHERE tenant_id = ? AND source = ? AND message_id = ? AND payload_sha256 = ?
"""
_GET = """
SELECT ingest_id, tenant_id, source, message_id, payload_sha256, status, received_at, updated_at,
       payload_ref, detail, attempts, envelope
FROM ingest_receipts
WHERE ingest_id = ? AND tenant_id = ?
"""
_EXISTS = "SELECT 1 FROM ingest_receipts WHERE ingest_id = ?"
_RECENT = """
SELECT ingest_id, tenant_id, source, message_id, payload_sha256, status, received_at, updated_at,
       payload_ref, detail, attempts, envelope
FROM ingest_receipts
WHERE tenant_id = ? AND (? IS NULL OR source = ?) AND (? IS NULL OR status = ?)
ORDER BY received_at DESC, rowid DESC
LIMIT ?
"""
_WITH_STATUS = """
SELECT ingest_id, tenant_id, source, message_id, payload_sha256, status, received_at, updated_at,
       payload_ref, detail, attempts, envelope
FROM ingest_receipts
WHERE status = ? AND tenant_id = ? AND (? IS NULL OR source = ?)
ORDER BY received_at ASC, rowid ASC
"""
# Two pulls of one item at once both miss ``find``; the unique index decides, and the loser's insert is
# a no-op (rowcount 0) instead of an IntegrityError that kills its run.
_INSERT = (
    "INSERT INTO ingest_receipts (ingest_id, tenant_id, source, message_id, payload_sha256, status, "
    "received_at, updated_at, payload_ref, detail, attempts, envelope) "
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
    "ON CONFLICT (tenant_id, source, message_id, payload_sha256) DO NOTHING"
)
# Only an open receipt moves (the four OPEN_STATUSES): rowcount 0 for a final one. The payload ref
# and the envelope are kept unless new ones are given.
_UPDATE = (
    "UPDATE ingest_receipts SET status = ?, detail = ?, updated_at = ?, "
    "payload_ref = COALESCE(?, payload_ref), envelope = COALESCE(?, envelope) "
    "WHERE ingest_id = ? AND status IN (?, ?, ?, ?)"
)
# A publish attempt is counted before it is made, on an open receipt only (rowcount 0 for a final
# one); the count is read back in the same transaction.
_BEGIN_ATTEMPT = (
    "UPDATE ingest_receipts SET attempts = attempts + 1, updated_at = ? "
    "WHERE ingest_id = ? AND status IN (?, ?, ?, ?)"
)
_ATTEMPTS = "SELECT attempts FROM ingest_receipts WHERE ingest_id = ?"
_COUNTS = (
    "SELECT status, COUNT(*) FROM ingest_receipts "
    "WHERE tenant_id = ? AND (? IS NULL OR source = ?) "
    "GROUP BY status ORDER BY status"
)


def _to_receipt(row: tuple) -> Receipt:
    return Receipt(
        ingest_id=row[0],
        tenant_id=row[1],
        source=row[2],
        message_id=row[3],
        payload_sha256=row[4],
        status=ReceiptStatus(row[5]),
        received_at=row[6],
        updated_at=row[7],
        payload_ref=row[8],
        detail=row[9],
        attempts=row[10],
        envelope=row[11],
    )


class ReceiptStore:
    """Open it where it is used (a job, a request, a CLI run) and close it there: a SQLite connection
    belongs to one thread. ``with ReceiptStore(path) as receipts:`` does both.

    Every read is scoped to one tenant: ``tenant_id`` is a required keyword on ``get``, ``counts``,
    ``recent`` and ``with_status`` (a read without it is a TypeError, not everyone's rows), as the
    multi-tenancy rule asks. ``find`` takes the tenant as part of the item's identity.
    """

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(self.path)
        self._db.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
        self._db.execute("PRAGMA journal_mode = WAL")
        with self._db:
            # The write lock first: a second process that opens an old file meanwhile (the CLI
            # next to the service) waits here, then sees the columns the first one added, instead
            # of reading the old ones and failing on a second ALTER with "duplicate column name".
            self._db.execute("BEGIN IMMEDIATE")
            self._db.execute(_SCHEMA)
            columns = {
                row[1] for row in self._db.execute("PRAGMA table_info(ingest_receipts)")
            }
            for name, statement in _ADDED_COLUMNS.items():
                if name not in columns:
                    self._db.execute(statement)
            self._db.execute(_INDEX)

    def close(self) -> None:
        self._db.close()

    def __enter__(self) -> ReceiptStore:
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()

    def find(
        self, tenant_id: str, source: str, message_id: str, payload_sha256: str
    ) -> Receipt | None:
        row = self._db.execute(
            _FIND, (tenant_id, source, message_id, payload_sha256)
        ).fetchone()
        return _to_receipt(row) if row else None

    def get(self, ingest_id: str, *, tenant_id: str) -> Receipt | None:
        """The receipt, if it is this tenant's; None otherwise (another tenant's is not there)."""
        row = self._db.execute(_GET, (ingest_id, tenant_id)).fetchone()
        return _to_receipt(row) if row else None

    def add(self, receipt: Receipt) -> bool:
        """Insert the receipt and return True; False if the item (tenant, source, message id, hash)
        already has one, which is then the receipt to use (``find`` it)."""
        with self._db:
            cursor = self._db.execute(
                _INSERT,
                (
                    receipt.ingest_id,
                    receipt.tenant_id,
                    receipt.source,
                    receipt.message_id,
                    receipt.payload_sha256,
                    receipt.status.value,
                    receipt.received_at,
                    receipt.updated_at,
                    receipt.payload_ref,
                    receipt.detail,
                    receipt.attempts,
                    receipt.envelope,
                ),
            )
        return cursor.rowcount == 1

    def set_status(
        self,
        ingest_id: str,
        status: ReceiptStatus,
        *,
        detail: str | None = None,
        payload_ref: str | None = None,
        envelope: str | None = None,
    ) -> bool:
        """Move an open receipt to ``status`` and return True. False if the receipt is final
        (published or rejected), which a late or stale update must not undo. KeyError if unknown.
        ``payload_ref`` and ``envelope`` are set when given (a too_large receipt has neither until
        the original is stored) and kept otherwise.
        """
        with self._db:
            cursor = self._db.execute(
                _UPDATE,
                (
                    status.value,
                    detail,
                    datetime.now(UTC).isoformat(),
                    payload_ref,
                    envelope,
                    ingest_id,
                    *(open_status.value for open_status in OPEN_STATUSES),
                ),
            )
        if cursor.rowcount == 1:
            return True
        if not self._exists(ingest_id):
            raise KeyError(ingest_id)
        return False

    def begin_attempt(self, ingest_id: str) -> int | None:
        """Count a publish attempt on an open receipt and return the count, this attempt included:
        what the envelope's ``attempt`` says. None if the receipt is final (another run published it
        meanwhile, so there is nothing left to publish). KeyError if unknown.
        """
        with self._db:
            cursor = self._db.execute(
                _BEGIN_ATTEMPT,
                (
                    datetime.now(UTC).isoformat(),
                    ingest_id,
                    *(open_status.value for open_status in OPEN_STATUSES),
                ),
            )
            if cursor.rowcount == 1:
                return self._db.execute(_ATTEMPTS, (ingest_id,)).fetchone()[0]
        if not self._exists(ingest_id):
            raise KeyError(ingest_id)
        return None

    def _exists(self, ingest_id: str) -> bool:
        return self._db.execute(_EXISTS, (ingest_id,)).fetchone() is not None

    def counts(self, source: str | None = None, *, tenant_id: str) -> dict[str, int]:
        """The tenant's receipts by status, optionally for one source."""
        return dict(self._db.execute(_COUNTS, (tenant_id, source, source)))

    def recent(
        self,
        source: str | None = None,
        limit: int = 10,
        *,
        status: ReceiptStatus | None = None,
        tenant_id: str,
    ) -> list[Receipt]:
        """The tenant's newest receipts first, optionally one source and one status."""
        status_value = None if status is None else status.value
        rows = self._db.execute(
            _RECENT, (tenant_id, source, source, status_value, status_value, limit)
        )
        return [_to_receipt(row) for row in rows]

    def with_status(
        self, status: ReceiptStatus, source: str | None = None, *, tenant_id: str
    ) -> list[Receipt]:
        """The tenant's receipts in one status, oldest first (the order work is picked up in)."""
        rows = self._db.execute(_WITH_STATUS, (status.value, tenant_id, source, source))
        return [_to_receipt(row) for row in rows]
