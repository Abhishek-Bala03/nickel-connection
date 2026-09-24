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
    too_large                         over the size cap in force, nothing stored; see below
    superseded                        a pull's too_large receipt whose item came in since
    rejected                          empty, or not a declared format: the same bytes can never pass (final)

``set_status`` moves only ``received``, ``held``, ``publish_failed`` and ``too_large`` (the open
statuses) and refuses a final one, so a late or stale update cannot undo a publish. ``published`` and
``rejected`` are final. ``superseded`` is final for ``set_status`` too; only a pull's too_large
marker (below) reaches it, and only ``mark_too_large`` reopens it. What happens to a message after
``published`` (validation, transform, dedup, storage) is the Transform layer's, recorded on its own
tables, never here.

A too_large receipt comes from one of two places. The bridge's own check of a payload in hand keys it
by the payload's hash, and it goes through under the same ingest id once the cap is raised. A pull
that skips a file for size (listed over the cap, a download that passed it, an SDK file over it on
disk) never has the bytes, so there is no hash to key it by: its ``payload_sha256`` holds a surrogate,
``too_large_key(tenant, source, message id)``, one per item at the source (``Receipt.is_marker``;
the API and the command line show it as no hash at all). Repeated pulls find it and add no row. When
a later pull gets the item's bytes within the cap, whatever becomes of them, the marker is
``superseded`` (its detail names the payload hash that took over); a pull that finds the item over
the cap again reopens it as ``too_large``, unless the size it sees is the one the item came in at,
which is most likely the same item, already delivered. Those two moves filter on the surrogate. A
payload hash equals it only for bytes built as its exact preimage, and then the pull simply finds
the marker as that item's receipt and carries it through, the key by then really the payload's
hash: harmless. docs/decisions/0002-too-large-receipts-for-pulls.md records the design (Proposed).

The file is shared by the job worker, the API threads and the CLI, each on its own connection, under
the service's one SQLite policy (``src.db.sqlite``: WAL mode and a busy timeout, as the job records).
Every read and every write names the tenant it is for.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Collection, Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

from src.db.sqlite import connect


class ReceiptStatus(StrEnum):
    RECEIVED = "received"
    HELD = "held"
    PUBLISHED = "published"
    PUBLISH_FAILED = "publish_failed"
    TOO_LARGE = "too_large"
    SUPERSEDED = "superseded"
    REJECTED = "rejected"


def too_large_key(tenant_id: str, source: str, message_id: str) -> str:
    """The identity of a too_large receipt a pull leaves for an item it never fetched whole (module
    docstring): sha256 over "too_large", the tenant, the source and the message id, NUL-separated,
    UTF-8. It goes where the payload hash would go. Stable across pulls whatever size the item is
    listed at. A payload's hash equals it only for bytes built as this exact text, which is
    harmless (module docstring)."""
    parts = ("too_large", tenant_id, source, message_id)
    return hashlib.sha256("\0".join(parts).encode("utf-8")).hexdigest()


# The statuses ``set_status`` moves a receipt from. ``published`` and ``rejected`` are final;
# ``superseded`` is final for ``set_status`` too, and only ``mark_too_large`` reopens a marker from it.
OPEN_STATUSES = (
    ReceiptStatus.RECEIVED,
    ReceiptStatus.HELD,
    ReceiptStatus.PUBLISH_FAILED,
    ReceiptStatus.TOO_LARGE,
)
# The statuses a pull's too_large marker is ever in.
_MARKER_STATUSES = (ReceiptStatus.TOO_LARGE, ReceiptStatus.SUPERSEDED)


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

    @property
    def is_marker(self) -> bool:
        """Whether this is a pull's too_large marker: too_large or superseded, with the item's
        surrogate key (``too_large_key``) where a payload hash would be, not the hash of any
        bytes it had."""
        return self.status in _MARKER_STATUSES and self.payload_sha256 == too_large_key(
            self.tenant_id, self.source, self.message_id
        )


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
# What the relay reads a page at a time: one tenant's receipts in one status, in received order.
# The index ends in received_at, and every SQLite index entry ends in the rowid, so a page is a range
# scan in (received_at, rowid) order that stops at its LIMIT: no sort in a temp B-tree, no full scan.
# The counts by status use it too.
_INDEX = """
CREATE INDEX IF NOT EXISTS ix_ingest_receipts_tenant_status_received
ON ingest_receipts (tenant_id, status, received_at)
"""
# The index before it, (status, source, received_at), served only the scan that loaded every row of
# a status at once; a file from before gets the new one and loses the old.
_DROP_OLD_INDEX = "DROP INDEX IF EXISTS ix_ingest_receipts_status_source_received"
# Receipts the relay reads per page: memory holds one page of them (each carries its envelope, about
# 1 KB), however many are open.
PAGE_SIZE = 256
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
_EXISTS = "SELECT 1 FROM ingest_receipts WHERE ingest_id = ? AND tenant_id = ?"
_RECENT = """
SELECT ingest_id, tenant_id, source, message_id, payload_sha256, status, received_at, updated_at,
       payload_ref, detail, attempts, envelope
FROM ingest_receipts
WHERE tenant_id = ? AND (? IS NULL OR source = ?) AND (? IS NULL OR status = ?)
ORDER BY received_at DESC, rowid DESC
LIMIT ?
"""
# Lists of values (statuses, sources) travel as one JSON array parameter, read by json_each: the
# statement stays a complete literal whatever the number of values.
#
# The tenant's receipts in some statuses, counted by status and source without loading a row.
_COUNTS_BY_SOURCE = """
SELECT status, source, COUNT(*) FROM ingest_receipts
WHERE tenant_id = ? AND status IN (SELECT value FROM json_each(?))
  AND (? IS NULL OR source = ?)
GROUP BY status, source
"""
# One page: the tenant's receipts in one status, from the given sources only, after the last page's
# (received_at, rowid) (keyset pagination; the first page starts after ('', 0), before every row).
# ``received_at >= ?`` is the range the index seeks to; the OR settles ties on the timestamp.
_PAGE = """
SELECT ingest_id, tenant_id, source, message_id, payload_sha256, status, received_at, updated_at,
       payload_ref, detail, attempts, envelope, rowid
FROM ingest_receipts
WHERE tenant_id = ? AND status = ? AND source IN (SELECT value FROM json_each(?))
  AND received_at >= ? AND (received_at > ? OR rowid > ?)
ORDER BY received_at, rowid
LIMIT ?
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
# is kept unless a new one is given. The envelope is written once, the first time one is given, and
# never replaced: it stays the envelope as first built, which every attempt publishes. Every write
# names the tenant as well as the ingest id: a write for one tenant can never touch another's row.
_UPDATE = (
    "UPDATE ingest_receipts SET status = ?, detail = ?, updated_at = ?, "
    "payload_ref = COALESCE(?, payload_ref), envelope = COALESCE(envelope, ?) "
    "WHERE ingest_id = ? AND tenant_id = ? AND status IN (?, ?, ?, ?)"
)
# A publish attempt is counted before it is made, on an open receipt only (rowcount 0 for a final
# one); the count is read back in the same transaction.
_BEGIN_ATTEMPT = (
    "UPDATE ingest_receipts SET attempts = attempts + 1, updated_at = ? "
    "WHERE ingest_id = ? AND tenant_id = ? AND status IN (?, ?, ?, ?)"
)
_ATTEMPTS = "SELECT attempts FROM ingest_receipts WHERE ingest_id = ? AND tenant_id = ?"
# Why a receipt's stored envelope was set aside: the detail alone, never the status, only while the
# receipt is still in the status the relay read it in (a run that moved it meanwhile wins), and only
# when the reason is new (a run that finds the same bad envelope again writes nothing).
_SET_ASIDE = (
    "UPDATE ingest_receipts SET detail = ?, updated_at = ? "
    "WHERE ingest_id = ? AND tenant_id = ? AND status = ? AND detail IS NOT ?"
)
# A pull's too_large receipt (keyed by its surrogate) moves only between too_large and superseded:
# over the cap again (reopened), or a new size or cap in its detail; and superseded once the item's
# bytes have a receipt of their own.
_MARK_TOO_LARGE = (
    "UPDATE ingest_receipts SET status = ?, detail = ?, updated_at = ? "
    "WHERE ingest_id = ? AND tenant_id = ? AND payload_sha256 = ? AND status IN (?, ?)"
)
_SUPERSEDE = (
    "UPDATE ingest_receipts SET status = ?, detail = ?, updated_at = ? "
    "WHERE tenant_id = ? AND source = ? AND message_id = ? AND payload_sha256 = ? AND status = ?"
)
# The size an item came in at: its latest published receipt's stored envelope's payload_bytes, read
# in the database (NULL when that envelope is not JSON, or there is no such receipt).
_PUBLISHED_SIZE = """
SELECT CASE WHEN json_valid(envelope) THEN json_extract(envelope, '$.payload_bytes') END
FROM ingest_receipts
WHERE tenant_id = ? AND source = ? AND message_id = ? AND status = ?
ORDER BY received_at DESC, rowid DESC
LIMIT 1
"""
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

    Every read and every write is scoped to one tenant: ``tenant_id`` is a required keyword on
    ``get``, ``counts``, ``counts_by_source``, ``recent``, ``in_status``, ``set_status`` and
    ``begin_attempt`` (a call without it is a TypeError, not everyone's rows), as the multi-tenancy
    rule asks. ``find`` and ``add`` take the tenant as part of the item's identity.
    """

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self._db = connect(self.path)
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
            self._db.execute(_DROP_OLD_INDEX)
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
        tenant_id: str,
        detail: str | None = None,
        payload_ref: str | None = None,
        envelope: str | None = None,
    ) -> bool:
        """Move this tenant's open receipt to ``status`` and return True. False if the receipt is
        final (published or rejected), which a late or stale update must not undo. KeyError if the
        tenant has no such receipt. ``payload_ref`` is set when given (a too_large receipt has none
        until the original is stored) and kept otherwise; ``envelope`` is set only on a receipt
        that has none yet: the envelope first built is never replaced.
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
                    tenant_id,
                    *(open_status.value for open_status in OPEN_STATUSES),
                ),
            )
        if cursor.rowcount == 1:
            return True
        if not self._exists(ingest_id, tenant_id):
            raise KeyError(ingest_id)
        return False

    def begin_attempt(self, ingest_id: str, *, tenant_id: str) -> int | None:
        """Count a publish attempt on this tenant's open receipt and return the count, this attempt
        included: what the envelope's ``attempt`` says. None if the receipt is final (another run
        published it meanwhile, so there is nothing left to publish). KeyError if the tenant has no
        such receipt.
        """
        with self._db:
            cursor = self._db.execute(
                _BEGIN_ATTEMPT,
                (
                    datetime.now(UTC).isoformat(),
                    ingest_id,
                    tenant_id,
                    *(open_status.value for open_status in OPEN_STATUSES),
                ),
            )
            if cursor.rowcount == 1:
                return self._db.execute(_ATTEMPTS, (ingest_id, tenant_id)).fetchone()[0]
        if not self._exists(ingest_id, tenant_id):
            raise KeyError(ingest_id)
        return None

    def set_aside(self, receipt: Receipt, *, detail: str) -> bool:
        """Record why ``receipt``'s stored envelope was set aside: its detail only, while it is
        still in the status it was read in. True if that wrote something; False if the same
        reason was there already, or another run moved the receipt meanwhile."""
        with self._db:
            cursor = self._db.execute(
                _SET_ASIDE,
                (
                    detail,
                    datetime.now(UTC).isoformat(),
                    receipt.ingest_id,
                    receipt.tenant_id,
                    receipt.status.value,
                    detail,
                ),
            )
        return cursor.rowcount == 1

    def mark_too_large(self, marker: Receipt, *, detail: str) -> bool:
        """A pull's too_large receipt (``marker``, keyed by ``too_large_key``) says so again, with
        this detail: reopened if it was superseded, or its size or cap brought up to date. False
        if nothing moved (the receipt is not such a marker any more)."""
        with self._db:
            cursor = self._db.execute(
                _MARK_TOO_LARGE,
                (
                    ReceiptStatus.TOO_LARGE.value,
                    detail,
                    datetime.now(UTC).isoformat(),
                    marker.ingest_id,
                    marker.tenant_id,
                    marker.payload_sha256,
                    ReceiptStatus.TOO_LARGE.value,
                    ReceiptStatus.SUPERSEDED.value,
                ),
            )
        return cursor.rowcount == 1

    def supersede_too_large(
        self, tenant_id: str, source: str, message_id: str, *, detail: str
    ) -> bool:
        """The item's bytes have a receipt of their own now: the too_large receipt a pull left for
        it (keyed by ``too_large_key``) is superseded, with ``detail`` saying by what. False if
        there was none open. Every ingested item calls this, so the usual case (no marker) is one
        read on the unique index, with no write and no commit."""
        marker = self.find(
            tenant_id, source, message_id, too_large_key(tenant_id, source, message_id)
        )
        if marker is None or marker.status is not ReceiptStatus.TOO_LARGE:
            return False
        with self._db:
            cursor = self._db.execute(
                _SUPERSEDE,
                (
                    ReceiptStatus.SUPERSEDED.value,
                    detail,
                    datetime.now(UTC).isoformat(),
                    tenant_id,
                    source,
                    message_id,
                    too_large_key(tenant_id, source, message_id),
                    ReceiptStatus.TOO_LARGE.value,
                ),
            )
        return cursor.rowcount == 1

    def published_size(
        self, tenant_id: str, source: str, message_id: str
    ) -> int | None:
        """The size the item came in at, by its latest published receipt's stored envelope;
        None when it has no published receipt, or that envelope is not readable."""
        row = self._db.execute(
            _PUBLISHED_SIZE,
            (tenant_id, source, message_id, ReceiptStatus.PUBLISHED.value),
        ).fetchone()
        return row[0] if row else None

    def _exists(self, ingest_id: str, tenant_id: str) -> bool:
        return self._db.execute(_EXISTS, (ingest_id, tenant_id)).fetchone() is not None

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

    def counts_by_source(
        self,
        statuses: Iterable[ReceiptStatus],
        source: str | None = None,
        *,
        tenant_id: str,
    ) -> dict[tuple[ReceiptStatus, str], int]:
        """How many of the tenant's receipts are in each of ``statuses``, by (status, source),
        optionally for one source. Counted in the database: no row is loaded."""
        wanted = json.dumps([status.value for status in statuses])
        rows = self._db.execute(_COUNTS_BY_SOURCE, (tenant_id, wanted, source, source))
        return {(ReceiptStatus(status), name): count for status, name, count in rows}

    def in_status(
        self, status: ReceiptStatus, sources: Collection[str], *, tenant_id: str
    ) -> Iterator[Receipt]:
        """The tenant's receipts in one status from ``sources`` only, oldest first (the order work
        is picked up in), read a page of ``PAGE_SIZE`` at a time: keyset pagination on received
        time and insertion order, so memory holds one page however many there are, and a receipt
        the caller moves on (published, say) between pages neither comes back nor makes the next
        page skip one. Receipts of other sources are filtered out by the query, before they load.
        """
        if not sources:
            return
        wanted = json.dumps(sorted(sources))
        size = PAGE_SIZE
        received_at, rowid = "", 0
        while True:
            rows = self._db.execute(
                _PAGE,
                (
                    tenant_id,
                    status.value,
                    wanted,
                    received_at,
                    received_at,
                    rowid,
                    size,
                ),
            ).fetchall()
            yield from [_to_receipt(row) for row in rows]  # this page, and no more
            if len(rows) < size:
                return
            received_at, rowid = rows[-1][6], rows[-1][12]
