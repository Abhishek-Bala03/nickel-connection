"""Tests for scheduling, execution history, and concurrency limits."""

from concurrent.futures import Future
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from nickel_connection.scheduler.engine import RunUnavailableError, SchedulerEngine
from nickel_connection.scheduler.models import Schedule
from nickel_connection.scheduler.runner import (
    ConnectorRunner,
    ConnectorTimeoutError,
    DemoConnector,
    RetryableConnectorError,
    RunResult,
)
from nickel_connection.scheduler.store import ScheduleStore

NOW = datetime(2026, 9, 23, 12, 0, tzinfo=UTC)
TENANT_ID = UUID("00000000-0000-4000-8000-000000000001")


class FakeExecutor:
    """Capture submissions and let tests control completion."""

    def __init__(self):
        self.jobs = []
        self.fail_submission = False
        self.stopped = False

    def submit(self, function, *args):
        if self.fail_submission:
            raise RuntimeError("Executor unavailable")

        future = Future()
        self.jobs.append((future, function, args))
        return future

    def shutdown(self, wait=True):
        self.stopped = True
        for future, function, args in self.jobs:
            if not future.done():
                try:
                    future.set_result(function(*args))
                except Exception as error:
                    future.set_exception(error)


@pytest.fixture
def setup(tmp_path):
    store = ScheduleStore(tmp_path / "scheduler.db")
    executor = FakeExecutor()
    current_time = [NOW]
    runner = ConnectorRunner({"demo": DemoConnector()})
    engine = SchedulerEngine(
        store,
        runner,
        TENANT_ID,
        max_workers=2,
        clock=lambda: current_time[0],
        monotonic_clock=lambda: (current_time[0] - NOW).total_seconds(),
        executor=executor,
    )

    yield store, engine, executor, current_time

    engine.close()
    store.close()


def make_schedule(number=1, **changes):
    values = {
        "id": UUID(int=number),
        "tenant_id": TENANT_ID,
        "name": f"Source {number}",
        "connector_type": "demo",
        "connection_id": f"connection-{number}",
        "interval_seconds": 30,
        "next_run_at": NOW,
    }
    values.update(changes)
    return Schedule(**values)


def test_due_schedule_starts_and_advances_next_run(setup):
    store, engine, executor, _ = setup
    schedule = make_schedule()
    store.add(schedule)

    started = engine.tick()

    assert len(started) == 1
    assert started[0].status == "running"
    assert len(executor.jobs) == 1

    saved = store.get(TENANT_ID, schedule.id)
    assert saved is not None
    assert saved.next_run_at == NOW + timedelta(seconds=30)


@pytest.mark.parametrize(
    "changes",
    [
        {"enabled": False},
        {"next_run_at": NOW + timedelta(seconds=1)},
        {"tenant_id": UUID(int=999)},
    ],
)
def test_skip_disabled_future_and_other_tenant_schedules(setup, changes):
    store, engine, executor, _ = setup
    store.add(make_schedule(**changes))

    assert engine.tick() == []
    assert executor.jobs == []


def test_success_is_saved_on_next_tick(setup):
    store, engine, executor, current_time = setup
    store.add(make_schedule())
    engine.tick()

    executor.jobs[0][0].set_result(RunResult(items_processed=5, message="Completed"))
    current_time[0] = NOW + timedelta(seconds=1)
    engine.tick()

    record = store.list_runs(TENANT_ID)[0]
    assert record.status == "succeeded"
    assert record.items_processed == 5
    assert record.finished_at == current_time[0]
    assert record.error_code is None


def test_failure_records_safe_error_code(setup):
    store, engine, executor, _ = setup
    store.add(make_schedule())
    engine.tick()

    executor.jobs[0][0].set_exception(
        ConnectionError("private-token-must-not-be-saved")
    )
    engine.tick()

    record = store.list_runs(TENANT_ID)[0]
    assert record.status == "failed"
    assert record.error_code == "connector_failed"
    assert "private-token" not in record.model_dump_json()


def test_running_connection_does_not_overlap(setup):
    store, engine, executor, current_time = setup
    store.add(make_schedule())
    engine.tick()

    current_time[0] = NOW + timedelta(seconds=60)

    assert engine.tick() == []
    assert len(executor.jobs) == 1


def test_concurrency_limit_leaves_remaining_job_due(setup):
    store, engine, executor, _ = setup
    for number in range(1, 4):
        store.add(make_schedule(number))

    assert len(engine.tick()) == 2
    assert len(executor.jobs) == 2

    waiting = store.get(TENANT_ID, UUID(int=3))
    assert waiting is not None
    assert waiting.next_run_at == NOW

    executor.jobs[0][0].set_result(RunResult(items_processed=0, message="Completed"))

    started = engine.tick()

    assert len(started) == 1
    assert started[0].schedule_id == UUID(int=3)


def test_missed_intervals_produce_one_run(setup):
    store, engine, executor, _ = setup
    store.add(make_schedule(next_run_at=NOW - timedelta(hours=1)))

    assert len(engine.tick()) == 1
    assert engine.tick() == []
    assert len(executor.jobs) == 1

    saved = store.get(TENANT_ID, UUID(int=1))
    assert saved is not None
    assert saved.next_run_at == NOW + timedelta(seconds=30)


def test_schedule_runs_again_after_completion_and_interval(setup):
    store, engine, executor, current_time = setup
    store.add(make_schedule())
    engine.tick()
    executor.jobs[0][0].set_result(RunResult(items_processed=0, message="Completed"))
    engine.tick()

    current_time[0] = NOW + timedelta(seconds=30)

    assert len(engine.tick()) == 1
    assert len(store.list_runs(TENANT_ID)) == 2


def test_dispatch_failure_releases_connection(setup):
    store, engine, executor, _ = setup
    schedule = make_schedule()
    store.add(schedule)
    executor.fail_submission = True

    with pytest.raises(RuntimeError, match="Executor unavailable"):
        engine.tick()

    record = store.list_runs(TENANT_ID)[0]
    assert record.status == "failed"
    assert record.error_code == "dispatch_failed"
    assert store.start_run(schedule, NOW) is not None


def test_close_collects_results_and_prevents_further_ticks(setup):
    store, engine, executor, _ = setup
    store.add(make_schedule())
    engine.tick()

    engine.close()
    engine.close()

    assert executor.stopped is True
    assert store.list_runs(TENANT_ID)[0].status == "succeeded"

    with pytest.raises(RuntimeError, match="closed"):
        engine.tick()


def test_invalid_worker_count_is_rejected(setup):
    store, _, _, _ = setup

    with pytest.raises(ValueError, match="max_workers"):
        SchedulerEngine(
            store,
            ConnectorRunner({}),
            TENANT_ID,
            max_workers=0,
        )


def test_default_executor_can_start_and_close(setup):
    store, _, _, _ = setup
    engine = SchedulerEngine(
        store,
        ConnectorRunner({}),
        TENANT_ID,
    )

    assert engine.tick() == []
    engine.close()


def test_retry_waits_then_succeeds_with_same_run_id(setup):
    store, engine, executor, current_time = setup
    schedule = make_schedule(max_retries=1, retry_delay_seconds=5)
    store.add(schedule)

    started = engine.tick()
    run_id = started[0].id
    executor.jobs[0][0].set_exception(RetryableConnectorError("Temporary failure"))
    engine.tick()

    assert len(executor.jobs) == 1
    assert store.list_runs(TENANT_ID)[0].status == "running"
    assert store.start_run(schedule, NOW) is None

    current_time[0] = NOW + timedelta(seconds=4)
    engine.tick()
    assert len(executor.jobs) == 1

    current_time[0] = NOW + timedelta(seconds=5)
    engine.tick()
    assert len(executor.jobs) == 2
    assert executor.jobs[1][2][1] == run_id

    executor.jobs[1][0].set_result(RunResult(items_processed=3, message="Completed"))
    engine.tick()

    records = store.list_runs(TENANT_ID)
    assert len(records) == 1
    assert records[0].id == run_id
    assert records[0].status == "succeeded"
    assert records[0].items_processed == 3


def test_retry_backoff_doubles_and_stops_at_limit(setup):
    store, engine, executor, current_time = setup
    store.add(
        make_schedule(
            max_retries=2,
            retry_delay_seconds=5,
            interval_seconds=300,
        )
    )
    engine.tick()

    executor.jobs[0][0].set_exception(RetryableConnectorError())
    engine.tick()

    current_time[0] = NOW + timedelta(seconds=5)
    engine.tick()
    executor.jobs[1][0].set_exception(RetryableConnectorError())
    engine.tick()

    current_time[0] = NOW + timedelta(seconds=14)
    engine.tick()
    assert len(executor.jobs) == 2

    current_time[0] = NOW + timedelta(seconds=15)
    engine.tick()
    assert len(executor.jobs) == 3

    executor.jobs[2][0].set_exception(RetryableConnectorError())
    engine.tick()

    record = store.list_runs(TENANT_ID)[0]
    assert record.status == "failed"
    assert record.error_code == "connector_failed"

    current_time[0] = NOW + timedelta(seconds=60)
    engine.tick()
    assert len(executor.jobs) == 3


def test_retryable_failure_with_retries_disabled_finishes(setup):
    store, engine, executor, _ = setup
    store.add(make_schedule(max_retries=0))
    engine.tick()

    executor.jobs[0][0].set_exception(RetryableConnectorError())
    engine.tick()

    assert len(executor.jobs) == 1
    assert store.list_runs(TENANT_ID)[0].status == "failed"


def test_non_retryable_failure_is_not_retried(setup):
    store, engine, executor, _ = setup
    store.add(make_schedule(max_retries=2))
    engine.tick()

    executor.jobs[0][0].set_exception(ValueError("Invalid configuration"))
    engine.tick()

    assert len(executor.jobs) == 1
    assert store.list_runs(TENANT_ID)[0].status == "failed"


def test_retry_submission_failure_finishes_run(setup):
    store, engine, executor, current_time = setup
    store.add(make_schedule(max_retries=1))
    engine.tick()

    executor.jobs[0][0].set_exception(RetryableConnectorError())
    engine.tick()

    executor.fail_submission = True
    current_time[0] = NOW + timedelta(seconds=5)

    with pytest.raises(RuntimeError, match="Executor unavailable"):
        engine.tick()

    record = store.list_runs(TENANT_ID)[0]
    assert record.status == "failed"
    assert record.error_code == "dispatch_failed"


def test_close_interrupts_waiting_retry_and_releases_connection(setup):
    store, engine, executor, _ = setup
    schedule = make_schedule(max_retries=1)
    store.add(schedule)
    engine.tick()

    executor.jobs[0][0].set_exception(RetryableConnectorError())
    engine.tick()
    engine.close()

    record = store.list_runs(TENANT_ID)[0]
    assert record.status == "interrupted"
    assert record.error_code == "scheduler_stopped"
    assert store.start_run(schedule, NOW) is not None


def test_close_does_not_start_new_retries(setup):
    store, engine, executor, _ = setup
    store.add(make_schedule(max_retries=2))
    engine.tick()

    executor.jobs[0][0].set_exception(RetryableConnectorError())
    engine.close()

    assert len(executor.jobs) == 1
    assert store.list_runs(TENANT_ID)[0].status == "failed"


def test_schedule_update_failure_releases_reservation(setup, monkeypatch):
    store, engine, executor, _ = setup
    schedule = make_schedule()
    store.add(schedule)

    def fail_update(schedule):
        raise RuntimeError("Storage unavailable")

    monkeypatch.setattr(store, "update", fail_update)

    with pytest.raises(RuntimeError, match="Storage unavailable"):
        engine.tick()

    assert executor.jobs == []
    record = store.list_runs(TENANT_ID)[0]
    assert record.status == "failed"
    assert record.error_code == "dispatch_failed"
    assert store.start_run(schedule, NOW) is not None


def test_overdue_worker_keeps_reservation_until_it_exits(setup):
    store, engine, executor, current_time = setup
    schedule = make_schedule(
        timeout_seconds=5,
        interval_seconds=300,
    )
    store.add(schedule)
    engine.tick()

    current_time[0] = NOW + timedelta(seconds=6)
    engine.tick()

    assert store.list_runs(TENANT_ID)[0].status == "running"
    assert store.start_run(schedule, current_time[0]) is None
    assert len(executor.jobs) == 1

    executor.jobs[0][0].set_result(RunResult(items_processed=5, message="Late result"))
    engine.tick()

    record = store.list_runs(TENANT_ID)[0]
    assert record.status == "failed"
    assert record.error_code == "connector_timeout"
    assert record.items_processed == 0
    assert store.start_run(schedule, current_time[0]) is not None


def test_timely_result_remains_successful_when_collected_later(setup):
    store, engine, executor, current_time = setup
    store.add(make_schedule(timeout_seconds=5, interval_seconds=300))
    engine.tick()

    current_time[0] = NOW + timedelta(seconds=4)
    executor.jobs[0][0].set_result(RunResult(items_processed=2, message="Completed"))

    current_time[0] = NOW + timedelta(seconds=20)
    engine.tick()

    record = store.list_runs(TENANT_ID)[0]
    assert record.status == "succeeded"
    assert record.items_processed == 2


def test_timeout_retry_gets_a_new_attempt_deadline(setup):
    store, engine, executor, current_time = setup
    store.add(
        make_schedule(
            timeout_seconds=5,
            max_retries=1,
            retry_delay_seconds=2,
            interval_seconds=300,
        )
    )
    engine.tick()

    current_time[0] = NOW + timedelta(seconds=5)
    executor.jobs[0][0].set_result(RunResult(items_processed=1, message="Too late"))
    engine.tick()

    assert len(executor.jobs) == 1
    assert store.list_runs(TENANT_ID)[0].status == "running"

    current_time[0] = NOW + timedelta(seconds=7)
    engine.tick()
    assert len(executor.jobs) == 2

    current_time[0] = NOW + timedelta(seconds=8)
    executor.jobs[1][0].set_result(RunResult(items_processed=3, message="Completed"))
    engine.tick()

    records = store.list_runs(TENANT_ID)
    assert len(records) == 1
    assert records[0].status == "succeeded"
    assert records[0].items_processed == 3


def test_connector_reported_timeout_is_recorded(setup):
    store, engine, executor, _ = setup
    store.add(make_schedule())
    engine.tick()

    executor.jobs[0][0].set_exception(ConnectorTimeoutError())
    engine.tick()

    record = store.list_runs(TENANT_ID)[0]
    assert record.status == "failed"
    assert record.error_code == "connector_timeout"


def test_manual_run_starts_before_due_time(setup):
    store, engine, executor, _ = setup
    schedule = make_schedule(next_run_at=NOW + timedelta(hours=1))
    store.add(schedule)

    record = engine.run_now(schedule.id)

    assert record.schedule_id == schedule.id
    assert len(executor.jobs) == 1
    saved = store.get(TENANT_ID, schedule.id)
    assert saved.next_run_at == NOW + timedelta(seconds=30)


def test_manual_run_rejects_missing_schedule(setup):
    _, engine, _, _ = setup

    with pytest.raises(KeyError):
        engine.run_now(UUID(int=999))


def test_manual_run_rejects_disabled_schedule(setup):
    store, engine, _, _ = setup
    schedule = make_schedule(enabled=False)
    store.add(schedule)

    with pytest.raises(RunUnavailableError, match="Enable"):
        engine.run_now(schedule.id)


def test_manual_run_rejects_overlapping_connection(setup):
    store, engine, _, _ = setup
    schedule = make_schedule()
    store.add(schedule)
    engine.run_now(schedule.id)

    with pytest.raises(RunUnavailableError, match="active run"):
        engine.run_now(schedule.id)


def test_manual_run_respects_capacity(setup):
    store, engine, _, _ = setup
    for number in range(1, 4):
        store.add(make_schedule(number))

    engine.run_now(UUID(int=1))
    engine.run_now(UUID(int=2))

    with pytest.raises(RunUnavailableError, match="capacity"):
        engine.run_now(UUID(int=3))


def test_manual_run_rejects_closed_engine(setup):
    _, engine, _, _ = setup
    engine.close()

    with pytest.raises(RuntimeError, match="closed"):
        engine.run_now(UUID(int=1))
