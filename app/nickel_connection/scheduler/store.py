"""SQLite storage for schedules and execution history."""

import sqlite3
from datetime import datetime
from pathlib import Path
from uuid import UUID

from .models import RunRecord, Schedule


class ScheduleStore:
    """Persist schedules and prevent overlapping connection runs."""

    def __init__(self, path: str | Path):
        self._connection = sqlite3.connect(str(path))

        with self._connection:
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS schedules (
                    id TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL,
                    definition TEXT NOT NULL
                )
                """
            )
            self._connection.execute(
                """
                CREATE INDEX IF NOT EXISTS schedules_tenant
                ON schedules (tenant_id)
                """
            )
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS scheduler_runs (
                    id TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL,
                    connection_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    definition TEXT NOT NULL
                )
                """
            )
            self._connection.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS one_active_connection_run
                ON scheduler_runs (tenant_id, connection_id)
                WHERE status = 'running'
                """
            )
            self._connection.execute(
                """
                CREATE INDEX IF NOT EXISTS scheduler_runs_tenant
                ON scheduler_runs (tenant_id)
                """
            )

    def add(self, schedule: Schedule) -> None:
        with self._connection:
            self._connection.execute(
                """
                INSERT INTO schedules (id, tenant_id, definition)
                VALUES (?, ?, ?)
                """,
                (
                    str(schedule.id),
                    str(schedule.tenant_id),
                    schedule.model_dump_json(),
                ),
            )

    def get(self, tenant_id: UUID, schedule_id: UUID) -> Schedule | None:
        row = self._connection.execute(
            """
            SELECT definition FROM schedules
            WHERE tenant_id = ? AND id = ?
            """,
            (str(tenant_id), str(schedule_id)),
        ).fetchone()

        if row is None:
            return None

        return Schedule.model_validate_json(row[0])

    def list_schedules(self, tenant_id: UUID) -> list[Schedule]:
        rows = self._connection.execute(
            """
            SELECT definition FROM schedules
            WHERE tenant_id = ?
            ORDER BY id
            """,
            (str(tenant_id),),
        ).fetchall()

        return [Schedule.model_validate_json(row[0]) for row in rows]

    def update(self, schedule: Schedule) -> None:
        with self._connection:
            cursor = self._connection.execute(
                """
                UPDATE schedules
                SET definition = ?
                WHERE tenant_id = ? AND id = ?
                """,
                (
                    schedule.model_dump_json(),
                    str(schedule.tenant_id),
                    str(schedule.id),
                ),
            )

            if cursor.rowcount == 0:
                raise KeyError("Schedule not found for this tenant")

    def start_run(self, schedule: Schedule, started_at: datetime) -> RunRecord | None:
        """Reserve this connection; return None if already running."""
        record = RunRecord(
            schedule_id=schedule.id,
            tenant_id=schedule.tenant_id,
            connection_id=schedule.connection_id,
            started_at=started_at,
        )

        with self._connection:
            cursor = self._connection.execute(
                """
                INSERT INTO scheduler_runs (
                    id, tenant_id, connection_id, status, definition
                )
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT DO NOTHING
                """,
                (
                    str(record.id),
                    str(record.tenant_id),
                    record.connection_id,
                    record.status,
                    record.model_dump_json(),
                ),
            )

        return record if cursor.rowcount == 1 else None

    def finish_run(self, record: RunRecord) -> None:
        """Complete a running execution and release its connection."""
        if record.status == "running" or record.finished_at is None:
            raise ValueError("A completed run needs a final status and finish time")

        if record.finished_at < record.started_at:
            raise ValueError("Finish time cannot precede start time")

        with self._connection:
            cursor = self._connection.execute(
                """
                UPDATE scheduler_runs
                SET status = ?, definition = ?
                WHERE id = ? AND tenant_id = ? AND status = 'running'
                """,
                (
                    record.status,
                    record.model_dump_json(),
                    str(record.id),
                    str(record.tenant_id),
                ),
            )

            if cursor.rowcount == 0:
                raise KeyError("Running execution not found for this tenant")

    def list_runs(
        self,
        tenant_id: UUID,
        limit: int = 50,
    ) -> list[RunRecord]:
        """Return the latest runs for this tenant, newest first."""
        if type(limit) is not int or not 1 <= limit <= 200:
            raise ValueError("limit must be an integer between 1 and 200")

        rows = self._connection.execute(
            """
            SELECT definition FROM scheduler_runs
            WHERE tenant_id = ?
            ORDER BY rowid DESC
            LIMIT ?
            """,
            (str(tenant_id), limit),
        ).fetchall()

        return [RunRecord.model_validate_json(row[0]) for row in rows]

    def list_running_runs(self, tenant_id: UUID) -> list[RunRecord]:
        """Return all active runs for recovery, without a history limit."""
        rows = self._connection.execute(
            """
            SELECT definition FROM scheduler_runs
            WHERE tenant_id = ? AND status = 'running'
            ORDER BY rowid
            """,
            (str(tenant_id),),
        ).fetchall()

        return [RunRecord.model_validate_json(row[0]) for row in rows]

    def close(self) -> None:
        self._connection.close()
