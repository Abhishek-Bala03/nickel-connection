"""Connectors an administrator added in the Connections UI, and what each pull of one stored.

SQLite under the data root (``connectors.sqlite``), on the service's connection policy
(``src.db.sqlite``). Every call opens its own short connection, like the job records. A connector
holds references only (the Nango integration and connection ids): the credentials stay in Nango
(decision 19). ``pulls`` maps a job to the stored original of each stream, so the Executions view can
show what a run brought in.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

from src.db.sqlite import connect

SOURCES = ("nango", "nickel_internal")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS connectors (
    id                  TEXT PRIMARY KEY,
    tenant_id           TEXT NOT NULL,
    name                TEXT NOT NULL CHECK (length(name) BETWEEN 1 AND 120),
    description         TEXT NOT NULL DEFAULT '' CHECK (length(description) <= 500),
    source              TEXT NOT NULL CHECK (source IN ('nango', 'nickel_internal')),
    provider            TEXT NOT NULL,
    nango_integration   TEXT NOT NULL,
    nango_connection_id TEXT NOT NULL,
    patient_id          TEXT,
    streams             TEXT NOT NULL,
    enabled             INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    version             INTEGER NOT NULL CHECK (version >= 1),
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL,
    changed_by          TEXT,
    UNIQUE (tenant_id, name),
    UNIQUE (tenant_id, nango_integration, nango_connection_id)
);
CREATE TABLE IF NOT EXISTS pulls (
    job_id      TEXT NOT NULL,
    tenant_id   TEXT NOT NULL,
    connector_id TEXT NOT NULL REFERENCES connectors (id),
    stream      TEXT NOT NULL,
    outcome     TEXT NOT NULL,
    ingest_id   TEXT NOT NULL,
    payload_ref TEXT,
    bytes       INTEGER NOT NULL CHECK (bytes >= 0),
    records     INTEGER NOT NULL CHECK (records >= 0),
    fetched_at  TEXT NOT NULL,
    PRIMARY KEY (job_id, stream)
);
"""
_SELECT = (
    "SELECT id, tenant_id, name, description, source, provider, nango_integration, "
    "nango_connection_id, patient_id, streams, enabled, version, created_at, updated_at, "
    "changed_by FROM connectors WHERE tenant_id = ?"
)
_INSERT = (
    "INSERT INTO connectors (id, tenant_id, name, description, source, provider, "
    "nango_integration, nango_connection_id, patient_id, streams, enabled, version, created_at, "
    "updated_at, changed_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)


class ConnectorConflictError(RuntimeError):
    """A duplicate name or connection, or an edit of a version that is no longer current."""


@dataclass(frozen=True)
class Connector:
    id: str
    tenant_id: str
    name: str
    description: str
    source: str
    provider: str
    nango_integration: str
    nango_connection_id: str
    patient_id: str | None
    streams: list[str]
    enabled: bool
    version: int
    created_at: str
    updated_at: str
    changed_by: str | None = None


@dataclass(frozen=True)
class Pull:
    job_id: str
    connector_id: str
    stream: str
    outcome: str
    ingest_id: str
    payload_ref: str | None
    bytes: int
    records: int
    fetched_at: str
    tenant_id: str = field(default="", repr=False)


def _connector(row: tuple) -> Connector:
    values = list(row)
    values[9] = json.loads(values[9])
    values[10] = bool(values[10])
    return Connector(*values)


class ConnectorStore:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        with self._db() as db:
            db.executescript(_SCHEMA)

    @contextmanager
    def _db(self) -> Iterator[sqlite3.Connection]:
        db = connect(self.path)
        try:
            with db:
                yield db
        finally:
            db.close()

    def list(self, tenant_id: str) -> list[Connector]:
        with self._db() as db:
            rows = db.execute(
                _SELECT + " ORDER BY name",
                (tenant_id,),
            ).fetchall()
        return [_connector(row) for row in rows]

    def get(self, tenant_id: str, connector_id: str) -> Connector | None:
        with self._db() as db:
            row = db.execute(
                _SELECT + " AND id = ?",
                (tenant_id, connector_id),
            ).fetchone()
        return _connector(row) if row else None

    def add(self, connector: Connector) -> None:
        row = (
            connector.id,
            connector.tenant_id,
            connector.name,
            connector.description,
            connector.source,
            connector.provider,
            connector.nango_integration,
            connector.nango_connection_id,
            connector.patient_id,
            json.dumps(connector.streams),
            int(connector.enabled),
            connector.version,
            connector.created_at,
            connector.updated_at,
            connector.changed_by,
        )
        try:
            with self._db() as db:
                db.execute(
                    _INSERT,
                    row,
                )
        except sqlite3.IntegrityError as exc:
            raise ConnectorConflictError(
                "A connector with this name or Nango connection exists already"
            ) from exc

    def update(self, connector: Connector, *, expected_version: int) -> Connector:
        """Update editable fields while preserving the Nango connection reference."""
        try:
            with self._db() as db:
                changed = db.execute(
                    "UPDATE connectors SET name = ?, description = ?, "
                    "provider = ?, streams = ?, enabled = ?, "
                    "version = version + 1, updated_at = ?, changed_by = ? "
                    "WHERE tenant_id = ? AND id = ? AND version = ?",
                    (
                        connector.name,
                        connector.description,
                        connector.provider,
                        json.dumps(connector.streams),
                        int(connector.enabled),
                        connector.updated_at,
                        connector.changed_by,
                        connector.tenant_id,
                        connector.id,
                        expected_version,
                    ),
                ).rowcount
        except sqlite3.IntegrityError as exc:
            raise ConnectorConflictError(
                "The change conflicts with an existing connector"
            ) from exc

        if not changed:
            raise ConnectorConflictError(
                "The connector changed since it was loaded. Reload and try again."
            )

        return self.get(connector.tenant_id, connector.id)

    def record_pull(self, pull: Pull) -> None:
        with self._db() as db:
            db.execute(
                "INSERT OR REPLACE INTO pulls (job_id, tenant_id, connector_id, stream, outcome, "
                "ingest_id, payload_ref, bytes, records, fetched_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    pull.job_id,
                    pull.tenant_id,
                    pull.connector_id,
                    pull.stream,
                    pull.outcome,
                    pull.ingest_id,
                    pull.payload_ref,
                    pull.bytes,
                    pull.records,
                    pull.fetched_at,
                ),
            )

    def pulls(self, tenant_id: str, job_id: str) -> list[Pull]:
        with self._db() as db:
            rows = db.execute(
                "SELECT job_id, connector_id, stream, outcome, ingest_id, payload_ref, bytes, "
                "records, fetched_at, tenant_id FROM pulls WHERE tenant_id = ? AND job_id = ? "
                "ORDER BY fetched_at",
                (tenant_id, job_id),
            ).fetchall()
        return [Pull(*row) for row in rows]
