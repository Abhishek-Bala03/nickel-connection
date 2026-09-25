"""Versioned SQLite storage for schedules and bounded execution history."""

import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

from .models import RunRecord, Schedule

_SCHEMA_VERSION = 1

_SCHEDULE_INSERT = """
    INSERT INTO schedules (
        id, tenant_id, name, connector_type, connection_id, trigger_type,
        interval_seconds, cron_expression, timezone, max_retries,
        retry_delay_seconds, timeout_seconds, enabled, created_at,
        updated_at, changed_by, version
    ) VALUES (
        :id, :tenant_id, :name, :connector_type, :connection_id, :trigger_type,
        :interval_seconds, :cron_expression, :timezone, :max_retries,
        :retry_delay_seconds, :timeout_seconds, :enabled, :created_at,
        :updated_at, :changed_by, :version
    )
"""

_RUN_INSERT = """
    INSERT INTO scheduler_runs (
        id, tenant_id, schedule_id, connection_id, status, started_at,
        finished_at, items_processed, error_code
    ) VALUES (
        :id, :tenant_id, :schedule_id, :connection_id, :status, :started_at,
        :finished_at, :items_processed, :error_code
    )
"""


class ScheduleConflictError(RuntimeError):
    """The saved schedule changed since the administrator loaded it."""


def _utc_text(value: datetime) -> str:
    if value.utcoffset() is None:
        raise ValueError("Timestamp must include a timezone")
    return value.astimezone(UTC).isoformat(timespec="microseconds")


def _schedule_values(schedule: Schedule) -> dict:
    values = schedule.model_dump(mode="json", exclude={"next_run_at"})
    values["enabled"] = int(schedule.enabled)
    values["created_at"] = _utc_text(schedule.created_at)
    values["updated_at"] = _utc_text(schedule.updated_at)
    return values


def _run_values(record: RunRecord) -> dict:
    values = record.model_dump(mode="json")
    values["started_at"] = _utc_text(record.started_at)
    values["finished_at"] = (
        _utc_text(record.finished_at) if record.finished_at is not None else None
    )
    return values


def _schedule_from_row(row: sqlite3.Row) -> Schedule:
    values = dict(row)
    values["enabled"] = bool(values["enabled"])
    return Schedule.model_validate(values)


def _run_from_row(row: sqlite3.Row) -> RunRecord:
    values = dict(row)
    values.pop("sequence")
    return RunRecord.model_validate(values)


class ScheduleStore:
    """One-thread storage; configuration is independent of runtime triggers."""

    def __init__(self, path: str | Path):
        self._connection = sqlite3.connect(str(path), timeout=5.0)
        self._connection.row_factory = sqlite3.Row
        try:
            self._connection.execute("PRAGMA busy_timeout = 5000")
            self._connection.execute("PRAGMA journal_mode = WAL")
            self._initialize_schema()
        except Exception:
            self._connection.close()
            raise

    def _initialize_schema(self) -> None:
        # Explicit BEGIN includes schema changes in the migration transaction.
        with self._connection:
            self._connection.execute("BEGIN IMMEDIATE")
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS scheduler_schema (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    version INTEGER NOT NULL CHECK (version >= 1)
                ) STRICT
                """
            )
            version = self._connection.execute(
                "SELECT version FROM scheduler_schema WHERE id = 1"
            ).fetchone()
            if version is not None:
                if version["version"] != _SCHEMA_VERSION:
                    raise RuntimeError("Unsupported scheduler database schema version")
                return

            existing = {
                row["name"]
                for row in self._connection.execute(
                    """
                    SELECT name FROM sqlite_master
                    WHERE type = 'table' AND name IN ('schedules', 'scheduler_runs')
                    """
                )
            }
            if existing:
                columns = {
                    row["name"]
                    for row in self._connection.execute("PRAGMA table_info(schedules)")
                }
                if (
                    existing != {"schedules", "scheduler_runs"}
                    or "definition" not in columns
                ):
                    raise RuntimeError("Unrecognized scheduler database schema")
                self._connection.execute(
                    "ALTER TABLE schedules RENAME TO legacy_schedules"
                )
                self._connection.execute(
                    "ALTER TABLE scheduler_runs RENAME TO legacy_runs"
                )

            self._create_tables()
            if existing:
                self._migrate_legacy_rows()
                self._connection.execute("DROP TABLE legacy_runs")
                self._connection.execute("DROP TABLE legacy_schedules")

            self._create_indexes()
            self._connection.execute(
                "INSERT INTO scheduler_schema (id, version) VALUES (1, ?)",
                (_SCHEMA_VERSION,),
            )

    def _create_tables(self) -> None:
        self._connection.execute(
            """
            CREATE TABLE schedules (
                id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                name TEXT NOT NULL CHECK (length(trim(name)) BETWEEN 1 AND 120),
                connector_type TEXT NOT NULL
                    CHECK (length(trim(connector_type)) BETWEEN 1 AND 80),
                connection_id TEXT NOT NULL
                    CHECK (length(trim(connection_id)) BETWEEN 1 AND 200),
                trigger_type TEXT NOT NULL CHECK (trigger_type IN ('interval', 'cron')),
                interval_seconds INTEGER CHECK (interval_seconds BETWEEN 1 AND 31536000),
                cron_expression TEXT CHECK (length(trim(cron_expression)) BETWEEN 1 AND 100),
                timezone TEXT NOT NULL CHECK (timezone = 'UTC'),
                max_retries INTEGER NOT NULL CHECK (max_retries BETWEEN 0 AND 5),
                retry_delay_seconds INTEGER NOT NULL CHECK (retry_delay_seconds BETWEEN 1 AND 3600),
                timeout_seconds INTEGER NOT NULL CHECK (timeout_seconds BETWEEN 1 AND 3600),
                enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
                created_at TEXT NOT NULL CHECK (julianday(created_at) IS NOT NULL),
                updated_at TEXT NOT NULL CHECK (julianday(updated_at) IS NOT NULL),
                changed_by TEXT CHECK (length(trim(changed_by)) BETWEEN 1 AND 255),
                version INTEGER NOT NULL CHECK (version >= 1),
                CHECK (julianday(updated_at) >= julianday(created_at)),
                CHECK (
                    (trigger_type = 'interval' AND interval_seconds IS NOT NULL
                        AND cron_expression IS NULL)
                    OR (trigger_type = 'cron' AND cron_expression IS NOT NULL
                        AND interval_seconds IS NULL)
                )
            ) STRICT
            """
        )
        self._connection.execute(
            """
            CREATE TABLE scheduler_runs (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                id TEXT NOT NULL UNIQUE,
                tenant_id TEXT NOT NULL,
                schedule_id TEXT NOT NULL,
                connection_id TEXT NOT NULL
                    CHECK (length(trim(connection_id)) BETWEEN 1 AND 200),
                status TEXT NOT NULL
                    CHECK (status IN ('running', 'succeeded', 'failed', 'interrupted')),
                started_at TEXT NOT NULL CHECK (julianday(started_at) IS NOT NULL),
                finished_at TEXT CHECK (
                    finished_at IS NULL OR julianday(finished_at) IS NOT NULL
                ),
                items_processed INTEGER NOT NULL CHECK (items_processed >= 0),
                error_code TEXT,
                CHECK (
                    (status = 'running' AND finished_at IS NULL)
                    OR (status != 'running' AND finished_at IS NOT NULL
                        AND julianday(finished_at) >= julianday(started_at))
                )
            ) STRICT
            """
        )

    def _create_indexes(self) -> None:
        for statement in (
            "CREATE INDEX schedules_tenant ON schedules (tenant_id, id)",
            """CREATE UNIQUE INDEX one_active_connection_run
               ON scheduler_runs (tenant_id, connection_id) WHERE status = 'running'""",
            """CREATE INDEX scheduler_runs_tenant
               ON scheduler_runs (tenant_id, sequence DESC)""",
            """CREATE INDEX scheduler_runs_schedule
               ON scheduler_runs (tenant_id, schedule_id, sequence DESC)""",
            """CREATE INDEX scheduler_runs_retention
               ON scheduler_runs (tenant_id, finished_at, sequence)
               WHERE status != 'running'""",
        ):
            self._connection.execute(statement)

    def _migrate_legacy_rows(self) -> None:
        """Validate all legacy data; any error rolls back the whole migration."""
        for row in self._connection.execute(
            "SELECT * FROM legacy_schedules ORDER BY rowid"
        ):
            schedule = Schedule.model_validate_json(row["definition"])
            if (
                str(schedule.id) != row["id"]
                or str(schedule.tenant_id) != row["tenant_id"]
            ):
                raise ValueError("Legacy schedule identity mismatch")
            self._connection.execute(_SCHEDULE_INSERT, _schedule_values(schedule))

        for row in self._connection.execute("SELECT * FROM legacy_runs ORDER BY rowid"):
            record = RunRecord.model_validate_json(row["definition"])
            if (
                str(record.id) != row["id"]
                or str(record.tenant_id) != row["tenant_id"]
                or record.connection_id != row["connection_id"]
                or record.status != row["status"]
            ):
                raise ValueError("Legacy execution identity or status mismatch")
            self._connection.execute(_RUN_INSERT, _run_values(record))

    def add(self, schedule: Schedule) -> None:
        with self._connection:
            self._connection.execute(_SCHEDULE_INSERT, _schedule_values(schedule))

    def get(self, tenant_id: UUID, schedule_id: UUID) -> Schedule | None:
        row = self._connection.execute(
            "SELECT * FROM schedules WHERE tenant_id = ? AND id = ?",
            (str(tenant_id), str(schedule_id)),
        ).fetchone()
        return None if row is None else _schedule_from_row(row)

    def list_schedules(self, tenant_id: UUID) -> list[Schedule]:
        rows = self._connection.execute(
            "SELECT * FROM schedules WHERE tenant_id = ? ORDER BY id",
            (str(tenant_id),),
        ).fetchall()
        return [_schedule_from_row(row) for row in rows]

    def update(self, schedule: Schedule) -> Schedule:
        """Compare the supplied version atomically and return the saved schedule."""
        values = _schedule_values(schedule)
        values["updated_at"] = _utc_text(max(datetime.now(UTC), schedule.updated_at))
        with self._connection:
            cursor = self._connection.execute(
                """
                UPDATE schedules SET
                    name = :name, trigger_type = :trigger_type,
                    interval_seconds = :interval_seconds,
                    cron_expression = :cron_expression, timezone = :timezone,
                    max_retries = :max_retries, retry_delay_seconds = :retry_delay_seconds,
                    timeout_seconds = :timeout_seconds, enabled = :enabled,
                    updated_at = :updated_at, changed_by = :changed_by,
                    version = version + 1
                WHERE tenant_id = :tenant_id AND id = :id AND version = :version
                    AND connection_id = :connection_id AND connector_type = :connector_type
                """,
                values,
            )
            saved = self.get(schedule.tenant_id, schedule.id)
            if cursor.rowcount == 0:
                if saved is None:
                    raise KeyError("Schedule not found for this tenant")
                raise ScheduleConflictError(
                    "Schedule changed or connection differs; reload before saving"
                )
        # The row exists after a successful UPDATE in the same transaction.
        assert saved is not None
        return saved

    def start_run(self, schedule: Schedule, started_at: datetime) -> RunRecord | None:
        """Reserve this tenant's connection, without hiding other constraint errors."""
        record = RunRecord(
            schedule_id=schedule.id,
            tenant_id=schedule.tenant_id,
            connection_id=schedule.connection_id,
            started_at=started_at,
        )
        with self._connection:
            cursor = self._connection.execute(
                _RUN_INSERT
                + """ ON CONFLICT (tenant_id, connection_id)
                      WHERE status = 'running' DO NOTHING""",
                _run_values(record),
            )
        return record if cursor.rowcount == 1 else None

    def finish_run(self, record: RunRecord) -> None:
        if record.status == "running" or record.finished_at is None:
            raise ValueError("A completed run needs a final status and finish time")
        if record.finished_at < record.started_at:
            raise ValueError("Finish time cannot precede start time")

        with self._connection:
            cursor = self._connection.execute(
                """
                UPDATE scheduler_runs SET status = :status, finished_at = :finished_at,
                    items_processed = :items_processed, error_code = :error_code
                WHERE id = :id AND tenant_id = :tenant_id AND status = 'running'
                    AND schedule_id = :schedule_id AND connection_id = :connection_id
                    AND started_at = :started_at
                """,
                _run_values(record),
            )
            if cursor.rowcount == 0:
                raise KeyError("Running execution not found for this tenant")

    def list_runs(
        self, tenant_id: UUID, limit: int = 50, schedule_id: UUID | None = None
    ) -> list[RunRecord]:
        """Return bounded history in insertion order, scoped to this tenant."""
        if type(limit) is not int or not 1 <= limit <= 200:
            raise ValueError("limit must be an integer between 1 and 200")
        if schedule_id is None:
            rows = self._connection.execute(
                """SELECT * FROM scheduler_runs WHERE tenant_id = ?
                   ORDER BY sequence DESC LIMIT ?""",
                (str(tenant_id), limit),
            ).fetchall()
        else:
            rows = self._connection.execute(
                """SELECT * FROM scheduler_runs WHERE tenant_id = ? AND schedule_id = ?
                   ORDER BY sequence DESC LIMIT ?""",
                (str(tenant_id), str(schedule_id), limit),
            ).fetchall()
        return [_run_from_row(row) for row in rows]

    def list_running_runs(self, tenant_id: UUID) -> list[RunRecord]:
        rows = self._connection.execute(
            """SELECT * FROM scheduler_runs WHERE tenant_id = ? AND status = 'running'
               ORDER BY sequence""",
            (str(tenant_id),),
        ).fetchall()
        return [_run_from_row(row) for row in rows]

    def prune_runs(
        self, tenant_id: UUID, before: datetime, batch_size: int = 1000
    ) -> int:
        """Delete one bounded batch of finished history; never remove active runs."""
        if type(batch_size) is not int or not 1 <= batch_size <= 10000:
            raise ValueError("batch_size must be an integer between 1 and 10000")
        cutoff = _utc_text(before)
        with self._connection:
            cursor = self._connection.execute(
                """
                DELETE FROM scheduler_runs WHERE sequence IN (
                    SELECT sequence FROM scheduler_runs
                    WHERE tenant_id = ? AND status != 'running' AND finished_at < ?
                    ORDER BY finished_at, sequence LIMIT ?
                )
                """,
                (str(tenant_id), cutoff, batch_size),
            )
        return cursor.rowcount

    def close(self) -> None:
        self._connection.close()
