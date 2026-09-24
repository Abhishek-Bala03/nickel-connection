"""Tests for persistent, tenant-scoped schedule storage."""

import sqlite3
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from nickel_connection.scheduler.models import Schedule
from nickel_connection.scheduler.store import ScheduleStore


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
        next_run_at=datetime(2026, 9, 23, 12, 0, tzinfo=UTC),
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
    record = store.start_run(schedule, schedule.next_run_at)

    assert record is not None
    assert record.status == "running"
    assert record.schedule_id == schedule.id
    assert store.list_runs(schedule.tenant_id) == [record]


def test_same_connection_cannot_overlap(store, schedule):
    first = store.start_run(schedule, schedule.next_run_at)
    other_schedule = schedule.model_copy(update={"id": uuid4()})
    second = store.start_run(other_schedule, schedule.next_run_at)

    assert first is not None
    assert second is None
    assert len(store.list_runs(schedule.tenant_id)) == 1


def test_different_connections_can_run(store, schedule):
    other = schedule.model_copy(
        update={"id": uuid4(), "connection_id": "another-connection"}
    )

    assert store.start_run(schedule, schedule.next_run_at) is not None
    assert store.start_run(other, other.next_run_at) is not None


def test_runs_are_isolated_between_tenants(store, schedule):
    other = schedule.model_copy(update={"id": uuid4(), "tenant_id": uuid4()})
    first = store.start_run(schedule, schedule.next_run_at)
    second = store.start_run(other, other.next_run_at)

    assert first is not None
    assert second is not None
    assert store.list_runs(schedule.tenant_id) == [first]
    assert store.list_runs(other.tenant_id) == [second]


@pytest.mark.parametrize("status", ["succeeded", "failed", "interrupted"])
def test_finishing_run_releases_connection(store, schedule, status):
    record = store.start_run(schedule, schedule.next_run_at)
    assert record is not None

    record.status = status
    record.finished_at = record.started_at
    store.finish_run(record)

    assert store.list_runs(schedule.tenant_id) == [record]
    assert store.start_run(schedule, schedule.next_run_at) is not None


def test_cannot_finish_run_twice(store, schedule):
    record = store.start_run(schedule, schedule.next_run_at)
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
    record = store.start_run(schedule, schedule.next_run_at)
    assert record is not None
    record.status = status
    record.finished_at = finished_at

    with pytest.raises(ValueError):
        store.finish_run(record)

    assert store.start_run(schedule, schedule.next_run_at) is None


def test_other_tenant_cannot_finish_run(store, schedule):
    record = store.start_run(schedule, schedule.next_run_at)
    assert record is not None
    record.tenant_id = uuid4()
    record.status = "succeeded"
    record.finished_at = record.started_at

    with pytest.raises(KeyError, match="Running execution not found"):
        store.finish_run(record)

    assert store.start_run(schedule, schedule.next_run_at) is None


def test_run_reservation_survives_reopening(tmp_path, schedule):
    path = tmp_path / "runs.db"
    first = ScheduleStore(path)
    try:
        record = first.start_run(schedule, schedule.next_run_at)
        assert record is not None
    finally:
        first.close()

    second = ScheduleStore(path)
    try:
        assert second.list_runs(schedule.tenant_id) == [record]
        assert second.start_run(schedule, schedule.next_run_at) is None
    finally:
        second.close()
