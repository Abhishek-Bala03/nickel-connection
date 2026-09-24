"""Tests for exclusive ownership, recovery, and service lifecycle."""

import sqlite3
from datetime import UTC, datetime, timedelta
from threading import Event
from uuid import UUID

import pytest

from nickel_connection.scheduler import service
from nickel_connection.scheduler.models import Schedule
from nickel_connection.scheduler.runner import ConnectorRunner
from nickel_connection.scheduler.store import ScheduleStore

TENANT_ID = UUID("00000000-0000-4000-8000-000000000001")
NOW = datetime(2026, 9, 23, 12, 0, tzinfo=UTC)


def make_schedule(number=1, tenant_id=TENANT_ID):
    return Schedule(
        id=UUID(int=number),
        tenant_id=tenant_id,
        name=f"Source {number}",
        connector_type="demo",
        connection_id=f"connection-{number}",
        interval_seconds=30,
        next_run_at=NOW,
    )


@pytest.fixture
def store(tmp_path):
    instance = ScheduleStore(tmp_path / "scheduler.db")
    yield instance
    instance.close()


@pytest.fixture
def runtime(monkeypatch):
    stop = Event()
    state = {
        "ticks": 0,
        "closed": False,
        "fail_init": False,
        "fail_tick": False,
        "fail_close": False,
        "store": None,
    }

    class FakeEngine:
        def __init__(self, store, runner, tenant_id):
            state["store"] = store
            state["records_at_start"] = store.list_runs(tenant_id)

            if state["fail_init"]:
                raise RuntimeError("Engine initialization failed")

        def tick(self):
            state["ticks"] += 1
            stop.set()

            if state["fail_tick"]:
                raise RuntimeError("Engine tick failed")

        def close(self):
            state["closed"] = True

            if state["fail_close"]:
                raise RuntimeError("Engine close failed")

    monkeypatch.setattr(service, "SchedulerEngine", FakeEngine)
    return stop, state


def test_second_owner_is_rejected(tmp_path):
    database = tmp_path / "scheduler.db"

    with service.database_lock(database):
        with pytest.raises(RuntimeError, match="already owns"):
            with service.database_lock(database):
                pass

    # Leaving the first context releases ownership.
    with service.database_lock(database):
        pass


def test_exception_releases_database_lock(tmp_path):
    database = tmp_path / "scheduler.db"

    with pytest.raises(ValueError, match="Test failure"):
        with service.database_lock(database):
            raise ValueError("Test failure")

    with service.database_lock(database):
        pass


def test_recovery_marks_running_record_interrupted(store):
    schedule = make_schedule()
    record = store.start_run(schedule, NOW)
    assert record is not None

    recovered = service.recover_interrupted_runs(
        store,
        TENANT_ID,
        NOW + timedelta(seconds=10),
    )

    assert recovered == 1

    saved = store.list_runs(TENANT_ID)[0]
    assert saved.id == record.id
    assert saved.status == "interrupted"
    assert saved.error_code == "scheduler_restarted"
    assert saved.finished_at == NOW + timedelta(seconds=10)
    assert store.start_run(schedule, NOW) is not None


def test_recovery_preserves_finished_records_and_other_tenants(store):
    finished = store.start_run(make_schedule(), NOW)
    assert finished is not None
    finished.status = "succeeded"
    finished.finished_at = NOW
    store.finish_run(finished)

    other_tenant = UUID(int=999)
    other = store.start_run(make_schedule(2, other_tenant), NOW)
    assert other is not None

    assert service.recover_interrupted_runs(store, TENANT_ID, NOW) == 0
    assert store.list_runs(TENANT_ID) == [finished]
    assert store.list_runs(other_tenant) == [other]


def test_recovery_handles_clock_moving_backwards(store):
    started_at = NOW + timedelta(minutes=5)
    record = store.start_run(make_schedule(), started_at)
    assert record is not None

    service.recover_interrupted_runs(store, TENANT_ID, NOW)

    assert store.list_runs(TENANT_ID)[0].finished_at == started_at


def test_service_ticks_and_closes_resources(tmp_path, runtime):
    stop, state = runtime
    database = tmp_path / "data" / "scheduler.db"

    service.serve(
        database,
        TENANT_ID,
        ConnectorRunner({}),
        stop,
        poll_seconds=0.01,
    )

    assert database.exists()
    assert state["ticks"] == 1
    assert state["closed"] is True

    with pytest.raises(sqlite3.ProgrammingError):
        state["store"].list_schedules(TENANT_ID)

    with service.database_lock(database):
        pass


def test_service_recovers_before_engine_initialization(tmp_path, runtime):
    stop, state = runtime
    database = tmp_path / "scheduler.db"
    existing = ScheduleStore(database)

    try:
        existing.start_run(make_schedule(), NOW)
    finally:
        existing.close()

    service.serve(database, TENANT_ID, ConnectorRunner({}), stop)

    records = state["records_at_start"]
    assert len(records) == 1
    assert records[0].status == "interrupted"


def test_existing_owner_prevents_recovery(tmp_path, runtime):
    stop, state = runtime
    database = tmp_path / "scheduler.db"
    existing = ScheduleStore(database)

    try:
        existing.start_run(make_schedule(), NOW)

        with service.database_lock(database):
            with pytest.raises(RuntimeError, match="already owns"):
                service.serve(
                    database,
                    TENANT_ID,
                    ConnectorRunner({}),
                    stop,
                )

        assert existing.list_runs(TENANT_ID)[0].status == "running"
        assert state["store"] is None
    finally:
        existing.close()


def test_preexisting_stop_request_skips_ticks(tmp_path, runtime):
    stop, state = runtime
    stop.set()

    service.serve(
        tmp_path / "scheduler.db",
        TENANT_ID,
        ConnectorRunner({}),
        stop,
    )

    assert state["ticks"] == 0
    assert state["closed"] is True


@pytest.mark.parametrize("failure", ["fail_init", "fail_tick", "fail_close"])
def test_service_failure_closes_database_and_releases_lock(tmp_path, runtime, failure):
    stop, state = runtime
    state[failure] = True
    database = tmp_path / "scheduler.db"

    with pytest.raises(RuntimeError, match="Engine"):
        service.serve(
            database,
            TENANT_ID,
            ConnectorRunner({}),
            stop,
        )

    with pytest.raises(sqlite3.ProgrammingError):
        state["store"].list_schedules(TENANT_ID)

    with service.database_lock(database):
        pass


@pytest.mark.parametrize(
    "poll_seconds",
    [0, -1, float("inf"), float("nan")],
)
def test_invalid_poll_interval_is_rejected(tmp_path, poll_seconds):
    database = tmp_path / "scheduler.db"

    with pytest.raises(ValueError, match="poll_seconds"):
        service.serve(
            database,
            TENANT_ID,
            ConnectorRunner({}),
            Event(),
            poll_seconds=poll_seconds,
        )

    assert not database.exists()


def test_recovery_is_not_limited_by_run_history(store):
    for number in range(1, 61):
        assert store.start_run(make_schedule(number), NOW) is not None

    assert len(store.list_runs(TENANT_ID)) == 50
    assert len(store.list_running_runs(TENANT_ID)) == 60

    assert service.recover_interrupted_runs(store, TENANT_ID, NOW) == 60
    assert store.list_running_runs(TENANT_ID) == []

    records = store.list_runs(TENANT_ID, limit=200)
    assert len(records) == 60
    assert all(record.status == "interrupted" for record in records)
