"""Tests for persistent, tenant-scoped schedule storage."""

import sqlite3
from contextlib import closing
from datetime import UTC, datetime, timedelta, timezone
from uuid import uuid4

import pytest

from src.scheduler.models import RunRecord, Schedule
from src.scheduler.store import ScheduleConflictError, ScheduleStore

NOW = datetime(2026, 9, 23, 12, 0, tzinfo=UTC)


@pytest.fixture
def store(tmp_path):
    instance = ScheduleStore(tmp_path / "scheduler.db")
    yield instance
    instance.close()


@pytest.fixture
def schedule():
    return Schedule(
        tenant_id=uuid4(),
        name="Demo source",
        connector_type="demo",
        connection_id="demo-connection",
        interval_seconds=30,
        created_at=NOW,
        updated_at=NOW,
    )


def test_add_and_get_schedule(store, schedule):
    store.add(schedule)

    restored = store.get(schedule.tenant_id, schedule.id)

    assert restored == schedule


def test_missing_schedule_returns_none(store):
    assert store.get(uuid4(), uuid4()) is None


def test_empty_store_returns_empty_list(store):
    assert store.list_schedules(uuid4()) == []


def test_list_only_returns_own_tenant_schedules(store, schedule):
    other = schedule.model_copy(update={"id": uuid4(), "tenant_id": uuid4()})
    store.add(schedule)
    store.add(other)

    assert store.list_schedules(schedule.tenant_id) == [schedule]
    assert store.list_schedules(other.tenant_id) == [other]


def test_other_tenant_cannot_read_schedule(store, schedule):
    store.add(schedule)

    assert store.get(uuid4(), schedule.id) is None


def test_update_frequency_and_enabled_status(store, schedule):
    store.add(schedule)

    schedule.interval_seconds = 60
    schedule.enabled = False
    store.update(schedule)

    restored = store.get(schedule.tenant_id, schedule.id)
    assert restored is not None
    assert restored.interval_seconds == 60
    assert restored.enabled is False


def test_update_missing_schedule_fails(store, schedule):
    with pytest.raises(KeyError, match="Schedule not found"):
        store.update(schedule)


def test_other_tenant_cannot_update_schedule(store, schedule):
    store.add(schedule)
    changed = schedule.model_copy(update={"tenant_id": uuid4(), "interval_seconds": 90})

    with pytest.raises(KeyError, match="Schedule not found"):
        store.update(changed)

    assert store.get(schedule.tenant_id, schedule.id) == schedule


def test_duplicate_id_does_not_overwrite_schedule(store, schedule):
    store.add(schedule)
    changed = schedule.model_copy(update={"interval_seconds": 90})

    with pytest.raises(sqlite3.IntegrityError):
        store.add(changed)

    assert store.get(schedule.tenant_id, schedule.id) == schedule


def test_schedule_survives_reopening_database(tmp_path, schedule):
    path = tmp_path / "scheduler.db"
    first = ScheduleStore(path)
    try:
        first.add(schedule)
    finally:
        first.close()

    second = ScheduleStore(path)
    try:
        assert second.get(schedule.tenant_id, schedule.id) == schedule
    finally:
        second.close()


def test_start_run_records_execution(store, schedule):
    record = store.start_run(schedule, NOW)

    assert record is not None
    assert record.status == "running"
    assert record.schedule_id == schedule.id
    assert store.list_runs(schedule.tenant_id) == [record]


def test_same_connection_cannot_overlap(store, schedule):
    first = store.start_run(schedule, NOW)
    other_schedule = schedule.model_copy(update={"id": uuid4()})
    second = store.start_run(other_schedule, NOW)

    assert first is not None
    assert second is None
    assert len(store.list_runs(schedule.tenant_id)) == 1


def test_different_connections_can_run(store, schedule):
    other = schedule.model_copy(
        update={"id": uuid4(), "connection_id": "another-connection"}
    )

    assert store.start_run(schedule, NOW) is not None
    assert store.start_run(other, NOW) is not None


def test_runs_are_isolated_between_tenants(store, schedule):
    other = schedule.model_copy(update={"id": uuid4(), "tenant_id": uuid4()})
    first = store.start_run(schedule, NOW)
    second = store.start_run(other, NOW)

    assert first is not None
    assert second is not None
    assert store.list_runs(schedule.tenant_id) == [first]
    assert store.list_runs(other.tenant_id) == [second]


@pytest.mark.parametrize("status", ["succeeded", "failed", "interrupted"])
def test_finishing_run_releases_connection(store, schedule, status):
    record = store.start_run(schedule, NOW)
    assert record is not None

    record.status = status
    record.finished_at = record.started_at
    store.finish_run(record)

    assert store.list_runs(schedule.tenant_id) == [record]
    assert store.start_run(schedule, NOW) is not None


def test_cannot_finish_run_twice(store, schedule):
    record = store.start_run(schedule, NOW)
    assert record is not None
    record.status = "succeeded"
    record.finished_at = record.started_at
    store.finish_run(record)

    with pytest.raises(KeyError, match="Running execution not found"):
        store.finish_run(record)


@pytest.mark.parametrize(
    ("status", "finished_at"),
    [
        ("running", None),
        ("succeeded", None),
        ("succeeded", datetime(2026, 9, 22, tzinfo=UTC)),
    ],
)
def test_invalid_completion_keeps_connection_reserved(
    store, schedule, status, finished_at
):
    record = store.start_run(schedule, NOW)
    assert record is not None
    record.status = status
    record.finished_at = finished_at

    with pytest.raises(ValueError):
        store.finish_run(record)

    assert store.start_run(schedule, NOW) is None


def test_other_tenant_cannot_finish_run(store, schedule):
    record = store.start_run(schedule, NOW)
    assert record is not None
    record.tenant_id = uuid4()
    record.status = "succeeded"
    record.finished_at = record.started_at

    with pytest.raises(KeyError, match="Running execution not found"):
        store.finish_run(record)

    assert store.start_run(schedule, NOW) is None


def test_run_reservation_survives_reopening(tmp_path, schedule):
    path = tmp_path / "runs.db"
    first = ScheduleStore(path)
    try:
        record = first.start_run(schedule, NOW)
        assert record is not None
    finally:
        first.close()

    second = ScheduleStore(path)
    try:
        assert second.list_runs(schedule.tenant_id) == [record]
        assert second.start_run(schedule, NOW) is None
    finally:
        second.close()


def test_run_history_is_limited_and_newest_first(store, schedule):
    records = []

    for _ in range(55):
        record = store.start_run(schedule, NOW)
        assert record is not None
        record.status = "succeeded"
        record.finished_at = record.started_at
        store.finish_run(record)
        records.append(record)

    assert store.list_runs(schedule.tenant_id) == list(reversed(records[-50:]))
    assert store.list_runs(schedule.tenant_id, limit=2) == list(reversed(records[-2:]))
    assert store.list_runs(schedule.tenant_id, limit=200) == list(reversed(records))


@pytest.mark.parametrize("limit", [0, -1, 201, True, 1.5, "50"])
def test_invalid_history_limit_is_rejected(store, schedule, limit):
    with pytest.raises(ValueError, match="limit must be"):
        store.list_runs(schedule.tenant_id, limit=limit)


def test_running_query_excludes_finished_runs_and_other_tenants(store, schedule):
    finished = store.start_run(schedule, NOW)
    assert finished is not None
    finished.status = "succeeded"
    finished.finished_at = finished.started_at
    store.finish_run(finished)

    active = store.start_run(schedule, NOW)
    assert active is not None

    other = schedule.model_copy(update={"tenant_id": uuid4()})
    other_active = store.start_run(other, NOW)
    assert other_active is not None

    assert store.list_running_runs(schedule.tenant_id) == [active]
    assert store.list_running_runs(other.tenant_id) == [other_active]
    assert store.list_running_runs(uuid4()) == []


@pytest.fixture
def database(store, tmp_path):
    with closing(sqlite3.connect(tmp_path / "scheduler.db")) as connection:
        yield connection


def finish_run(store, schedule, when=NOW, status="succeeded"):
    record = store.start_run(schedule, when)
    assert record is not None
    record.status = status
    record.finished_at = when
    store.finish_run(record)
    return record


def make_legacy_database(path, schedules, runs):
    with closing(sqlite3.connect(path)) as connection:
        connection.executescript(
            """
            CREATE TABLE schedules (
                id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, definition TEXT NOT NULL
            );
            CREATE INDEX schedules_tenant ON schedules (tenant_id);
            CREATE TABLE scheduler_runs (
                id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL,
                connection_id TEXT NOT NULL, status TEXT NOT NULL,
                definition TEXT NOT NULL
            );
            CREATE UNIQUE INDEX one_active_connection_run
                ON scheduler_runs (tenant_id, connection_id) WHERE status = 'running';
            CREATE INDEX scheduler_runs_tenant ON scheduler_runs (tenant_id);
            """
        )
        with connection:
            for schedule in schedules:
                connection.execute(
                    "INSERT INTO schedules VALUES (?, ?, ?)",
                    (
                        str(schedule.id),
                        str(schedule.tenant_id),
                        schedule.model_dump_json(),
                    ),
                )
            for record in runs:
                connection.execute(
                    "INSERT INTO scheduler_runs VALUES (?, ?, ?, ?, ?)",
                    (
                        str(record.id),
                        str(record.tenant_id),
                        record.connection_id,
                        record.status,
                        record.model_dump_json(),
                    ),
                )


def test_runtime_next_run_is_not_persisted(store, schedule, database):
    schedule.next_run_at = NOW
    store.add(schedule)

    saved = store.get(schedule.tenant_id, schedule.id)

    assert saved is not None
    assert saved.next_run_at is None
    assert saved.model_dump(exclude={"next_run_at"}) == schedule.model_dump(
        exclude={"next_run_at"}
    )
    columns = {row[1] for row in database.execute("PRAGMA table_info(schedules)")}
    assert "definition" not in columns
    assert "next_run_at" not in columns


def test_cron_schedule_survives_storage(store, schedule):
    data = schedule.model_dump()
    data.update(
        trigger_type="cron", interval_seconds=None, cron_expression="0 9 * * mon-fri"
    )
    cron = Schedule.model_validate(data)
    store.add(cron)

    assert store.get(cron.tenant_id, cron.id) == cron


def test_update_returns_new_version_and_preserves_creation_metadata(store, schedule):
    store.add(schedule)
    original_created_at = schedule.created_at
    schedule.created_at = NOW - timedelta(days=10)
    schedule.changed_by = "admin-subject"
    schedule.name = "Edited"

    saved = store.update(schedule)

    assert saved.version == 2
    assert saved.created_at == original_created_at
    assert saved.updated_at >= NOW
    assert saved.changed_by == "admin-subject"
    assert saved.name == "Edited"
    assert schedule.version == 1
    assert store.update(saved).version == 3


def test_stale_update_from_second_connection_is_rejected(store, schedule, tmp_path):
    store.add(schedule)
    second = ScheduleStore(tmp_path / "scheduler.db")
    try:
        stale = second.get(schedule.tenant_id, schedule.id)
        assert stale is not None
        schedule.name = "First admin"
        saved = store.update(schedule)
        stale.name = "Second admin"

        with pytest.raises(ScheduleConflictError, match="reload"):
            second.update(stale)

        assert second.get(schedule.tenant_id, schedule.id) == saved
    finally:
        second.close()


@pytest.mark.parametrize("field", ["connection_id", "connector_type"])
def test_update_cannot_reassign_connection(store, schedule, field):
    store.add(schedule)
    changed = schedule.model_copy(update={field: "different"})

    with pytest.raises(ScheduleConflictError):
        store.update(changed)

    assert store.get(schedule.tenant_id, schedule.id) == schedule


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE schedules SET interval_seconds = 0",
        "UPDATE schedules SET interval_seconds = 31536001",
        "UPDATE schedules SET interval_seconds = 1.5",
        "UPDATE schedules SET max_retries = 6",
        "UPDATE schedules SET retry_delay_seconds = 0",
        "UPDATE schedules SET timeout_seconds = 3601",
        "UPDATE schedules SET enabled = 2",
        "UPDATE schedules SET version = 0",
        "UPDATE schedules SET trigger_type = 'unknown'",
        "UPDATE schedules SET trigger_type = 'cron'",
        "UPDATE schedules SET interval_seconds = NULL",
        "UPDATE schedules SET cron_expression = '0 9 * * *'",
        "UPDATE schedules SET timezone = 'Europe/London'",
        "UPDATE schedules SET name = '   '",
        "UPDATE schedules SET changed_by = ''",
        "UPDATE schedules SET created_at = 'invalid'",
        "UPDATE schedules SET updated_at = 'invalid'",
        "UPDATE schedules SET updated_at = '2020-01-01T00:00:00Z'",
    ],
)
def test_database_rejects_invalid_schedule_values(store, schedule, database, statement):
    store.add(schedule)

    with pytest.raises(sqlite3.IntegrityError):
        with database:
            database.execute(statement)

    assert store.get(schedule.tenant_id, schedule.id) == schedule


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE scheduler_runs SET status = 'unknown'",
        "UPDATE scheduler_runs SET status = 'succeeded'",
        "UPDATE scheduler_runs SET items_processed = -1",
        "UPDATE scheduler_runs SET items_processed = 1.5",
        "UPDATE scheduler_runs SET started_at = 'invalid'",
        "UPDATE scheduler_runs SET finished_at = 'invalid'",
        "UPDATE scheduler_runs SET finished_at = '2026-09-24T12:00:00Z'",
        """UPDATE scheduler_runs SET status = 'failed',
           finished_at = '2020-01-01T00:00:00Z'""",
    ],
)
def test_database_rejects_invalid_run_values(store, schedule, database, statement):
    record = store.start_run(schedule, NOW)
    assert record is not None

    with pytest.raises(sqlite3.IntegrityError):
        with database:
            database.execute(statement)

    assert store.list_runs(schedule.tenant_id) == [record]


def test_duplicate_run_id_is_not_hidden_as_connection_conflict(
    store, schedule, monkeypatch
):
    from src.scheduler import store as store_module

    record = store.start_run(schedule, NOW)
    assert record is not None
    record.status = "succeeded"
    record.finished_at = NOW
    store.finish_run(record)
    duplicate = record.model_copy(update={"status": "running", "finished_at": None})
    monkeypatch.setattr(store_module, "RunRecord", lambda **kwargs: duplicate)

    with pytest.raises(sqlite3.IntegrityError):
        store.start_run(schedule, NOW)


def test_history_filter_is_tenant_scoped_and_applies_limit(store, schedule):
    first = finish_run(store, schedule)
    other_schedule = schedule.model_copy(update={"id": uuid4()})
    finish_run(store, other_schedule)
    latest = finish_run(store, schedule)
    other_tenant = schedule.model_copy(update={"tenant_id": uuid4()})
    foreign = finish_run(store, other_tenant)

    assert store.list_runs(schedule.tenant_id, schedule_id=schedule.id) == [
        latest,
        first,
    ]
    assert store.list_runs(schedule.tenant_id, limit=1, schedule_id=schedule.id) == [
        latest
    ]
    assert store.list_runs(other_tenant.tenant_id, schedule_id=schedule.id) == [foreign]
    assert store.list_runs(schedule.tenant_id, schedule_id=uuid4()) == []


def test_retention_is_bounded_and_preserves_active_recent_and_other_tenant_runs(
    store, schedule
):
    for status in ("succeeded", "failed", "interrupted"):
        finish_run(store, schedule, NOW - timedelta(days=31), status)
    cutoff = NOW - timedelta(days=30)
    boundary = finish_run(store, schedule, cutoff)
    recent = finish_run(store, schedule, NOW)
    active = store.start_run(schedule, NOW - timedelta(days=60))
    assert active is not None
    other = schedule.model_copy(update={"tenant_id": uuid4()})
    foreign = finish_run(store, other, NOW - timedelta(days=90))

    assert store.prune_runs(schedule.tenant_id, cutoff, batch_size=2) == 2
    assert store.prune_runs(schedule.tenant_id, cutoff, batch_size=2) == 1
    assert store.prune_runs(schedule.tenant_id, cutoff) == 0
    assert store.list_runs(schedule.tenant_id) == [active, recent, boundary]
    assert store.list_runs(other.tenant_id) == [foreign]


def test_retention_normalizes_offset_to_utc(store, schedule):
    finish_run(store, schedule, NOW)
    offset = timezone(timedelta(hours=5, minutes=30))
    cutoff = (NOW + timedelta(seconds=1)).astimezone(offset)

    assert store.prune_runs(schedule.tenant_id, cutoff) == 1


@pytest.mark.parametrize("batch_size", [0, -1, 10001, True, 1.5, "50"])
def test_retention_rejects_invalid_batch_size(store, schedule, batch_size):
    with pytest.raises(ValueError, match="batch_size"):
        store.prune_runs(schedule.tenant_id, NOW, batch_size=batch_size)


def test_retention_rejects_naive_cutoff(store, schedule):
    with pytest.raises(ValueError, match="timezone"):
        store.prune_runs(schedule.tenant_id, NOW.replace(tzinfo=None))


def test_sqlite_uses_wal_and_busy_timeout(store):
    assert store._connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    assert store._connection.execute("PRAGMA busy_timeout").fetchone()[0] == 5000


def test_legacy_migration_preserves_tenants_history_order_and_reservations(
    tmp_path, schedule
):
    path = tmp_path / "legacy.db"
    schedule.next_run_at = NOW
    other = schedule.model_copy(update={"id": uuid4(), "tenant_id": uuid4()})
    finished = RunRecord(
        schedule_id=schedule.id,
        tenant_id=schedule.tenant_id,
        connection_id=schedule.connection_id,
        started_at=NOW,
        status="succeeded",
        finished_at=NOW,
        items_processed=5,
    )
    active = finished.model_copy(
        update={"id": uuid4(), "status": "running", "finished_at": None}
    )
    foreign = active.model_copy(
        update={"id": uuid4(), "tenant_id": other.tenant_id, "schedule_id": other.id}
    )
    make_legacy_database(path, [schedule, other], [finished, active, foreign])

    migrated = ScheduleStore(path)
    try:
        restored = migrated.get(schedule.tenant_id, schedule.id)
        assert restored is not None
        assert restored.next_run_at is None
        assert restored.model_dump(exclude={"next_run_at"}) == schedule.model_dump(
            exclude={"next_run_at"}
        )
        assert migrated.list_runs(schedule.tenant_id) == [active, finished]
        assert migrated.list_runs(other.tenant_id) == [foreign]
        assert migrated.start_run(schedule, NOW) is None
    finally:
        migrated.close()

    reopened = ScheduleStore(path)
    try:
        assert reopened.list_runs(schedule.tenant_id) == [active, finished]
    finally:
        reopened.close()

    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute(
            "SELECT version FROM scheduler_schema"
        ).fetchone() == (1,)
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        assert "legacy_schedules" not in tables
        assert "legacy_runs" not in tables


def test_migration_accepts_original_records_without_audit_fields(tmp_path, schedule):
    path = tmp_path / "original.db"
    make_legacy_database(path, [schedule], [])
    original = schedule.model_dump_json(
        exclude={
            "created_at",
            "updated_at",
            "changed_by",
            "version",
            "trigger_type",
            "cron_expression",
            "timezone",
        }
    )
    with closing(sqlite3.connect(path)) as connection:
        with connection:
            connection.execute("UPDATE schedules SET definition = ?", (original,))

    migrated = ScheduleStore(path)
    try:
        restored = migrated.get(schedule.tenant_id, schedule.id)
        assert restored is not None
        assert restored.changed_by is None
        assert restored.version == 1
        assert restored.interval_seconds == schedule.interval_seconds
    finally:
        migrated.close()


@pytest.mark.parametrize(
    ("statement", "value"),
    [
        ("UPDATE schedules SET definition = ?", "invalid-json"),
        ("UPDATE schedules SET id = ?", str(uuid4())),
        ("UPDATE schedules SET tenant_id = ?", str(uuid4())),
        ("UPDATE scheduler_runs SET id = ?", str(uuid4())),
        ("UPDATE scheduler_runs SET tenant_id = ?", str(uuid4())),
        ("UPDATE scheduler_runs SET connection_id = ?", "different"),
        ("UPDATE scheduler_runs SET status = ?", "failed"),
    ],
)
def test_failed_migration_rolls_back_original_tables(
    tmp_path, schedule, statement, value
):
    path = tmp_path / "invalid.db"
    record = RunRecord(
        schedule_id=schedule.id,
        tenant_id=schedule.tenant_id,
        connection_id=schedule.connection_id,
        started_at=NOW,
    )
    make_legacy_database(path, [schedule], [record])
    with closing(sqlite3.connect(path)) as connection:
        with connection:
            connection.execute(statement, (value,))
        original_schedules = connection.execute("SELECT * FROM schedules").fetchall()
        original_runs = connection.execute("SELECT * FROM scheduler_runs").fetchall()

    with pytest.raises(ValueError):
        ScheduleStore(path)

    with closing(sqlite3.connect(path)) as connection:
        assert (
            connection.execute("SELECT * FROM schedules").fetchall()
            == original_schedules
        )
        assert (
            connection.execute("SELECT * FROM scheduler_runs").fetchall()
            == original_runs
        )
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        assert "legacy_schedules" not in tables
        assert "legacy_runs" not in tables
        assert "scheduler_schema" not in tables


def test_unknown_schema_version_is_rejected(tmp_path):
    path = tmp_path / "future.db"
    initial = ScheduleStore(path)
    initial.close()
    with closing(sqlite3.connect(path)) as connection:
        with connection:
            connection.execute("UPDATE scheduler_schema SET version = 999")

    with pytest.raises(RuntimeError, match="Unsupported"):
        ScheduleStore(path)


@pytest.mark.parametrize(
    "schema",
    [
        "CREATE TABLE schedules (id TEXT, definition TEXT)",
        "CREATE TABLE schedules (id TEXT); CREATE TABLE scheduler_runs (id TEXT)",
    ],
)
def test_unrecognized_schema_is_rejected_without_replacement(tmp_path, schema):
    path = tmp_path / "unrecognized.db"
    with closing(sqlite3.connect(path)) as connection:
        connection.executescript(schema)
        original = connection.execute(
            "SELECT name, sql FROM sqlite_master ORDER BY name"
        ).fetchall()

    with pytest.raises(RuntimeError, match="Unrecognized"):
        ScheduleStore(path)

    with closing(sqlite3.connect(path)) as connection:
        assert (
            connection.execute(
                "SELECT name, sql FROM sqlite_master ORDER BY name"
            ).fetchall()
            == original
        )


def test_failed_initialization_closes_connection(tmp_path, monkeypatch):
    path = tmp_path / "future.db"
    initial = ScheduleStore(path)
    initial.close()
    with closing(sqlite3.connect(path)) as connection:
        with connection:
            connection.execute("UPDATE scheduler_schema SET version = 999")

    real_connect = sqlite3.connect
    opened = []

    def tracked_connect(*args, **kwargs):
        connection = real_connect(*args, **kwargs)
        opened.append(connection)
        return connection

    monkeypatch.setattr(sqlite3, "connect", tracked_connect)
    with pytest.raises(RuntimeError, match="Unsupported"):
        ScheduleStore(path)

    assert len(opened) == 1
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        opened[0].execute("SELECT 1")
