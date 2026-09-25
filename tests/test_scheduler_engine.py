"""APScheduler lifecycle, execution, deadlines, and failure isolation tests."""

import asyncio
import sqlite3
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from apscheduler.events import EVENT_JOB_ERROR, JobExecutionEvent
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from src.scheduler.engine import RunUnavailableError, SchedulerEngine
from src.scheduler.models import Schedule
from src.scheduler.runner import ConnectorTimeoutError, RunResult
from src.scheduler.store import ScheduleStore

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


class AsyncRunnerStub:
    """Controllable async I/O without real network requests."""

    def __init__(self):
        self.calls = []
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.block = False
        self.cancelled = False
        self.error = None
        self.result = RunResult(items_processed=5, message="Completed")

    async def run(self, schedule, run_id):
        self.calls.append((schedule, run_id))
        self.started.set()
        try:
            if self.block:
                await self.release.wait()
            if self.error is not None:
                raise self.error
            return self.result
        except asyncio.CancelledError:
            self.cancelled = True
            raise


@pytest.fixture
def tenant_id():
    return uuid4()


@pytest.fixture
def schedule(tenant_id):
    return Schedule(
        tenant_id=tenant_id,
        name="Demo source",
        connector_type="demo",
        connection_id="demo-connection",
        interval_seconds=3600,
    )


@pytest.fixture
async def runtime(tmp_path, tenant_id):
    store = ScheduleStore(tmp_path / "scheduler.db")
    runner = AsyncRunnerStub()
    engine = SchedulerEngine(store, runner, tenant_id)
    engine.start()
    try:
        yield store, engine, runner
    finally:
        await engine.close()
        store.close()


def add_schedule(runtime, schedule):
    store, engine, _ = runtime
    store.add(schedule)
    return engine.sync_schedule(schedule)


async def wait_for_run(store, tenant_id, run_id):
    async with asyncio.timeout(3):
        while True:
            record = next(
                item for item in store.list_runs(tenant_id) if item.id == run_id
            )
            if record.status != "running":
                return record
            await asyncio.sleep(0.005)


async def wait_for_tasks(engine):
    async with asyncio.timeout(3):
        while engine._tasks:
            await asyncio.sleep(0.005)


async def test_sync_runner_is_rejected(runtime, tenant_id):
    class SyncRunner:
        def run(self, schedule, run_id):
            return RunResult(items_processed=0, message="Sync")

    store, _, _ = runtime
    with pytest.raises(TypeError, match="asynchronous"):
        SchedulerEngine(store, SyncRunner(), tenant_id)


@pytest.mark.parametrize("days", [0, -1, 3651, True, 1.5, "30"])
async def test_invalid_retention_is_rejected(runtime, tenant_id, days):
    store, _, runner = runtime
    with pytest.raises(ValueError, match="retention_days"):
        SchedulerEngine(store, runner, tenant_id, retention_days=days)


async def test_start_rebuilds_only_tenant_jobs_from_database(tmp_path, schedule):
    path = tmp_path / "rebuild.db"
    original = ScheduleStore(path)
    paused = schedule.model_copy(update={"id": uuid4(), "enabled": False})
    foreign = schedule.model_copy(update={"id": uuid4(), "tenant_id": uuid4()})
    for item in (schedule, paused, foreign):
        original.add(item)
    original.close()

    store = ScheduleStore(path)
    engine = SchedulerEngine(store, AsyncRunnerStub(), schedule.tenant_id)
    try:
        assert not engine.healthy
        engine.start()
        assert engine.healthy
        assert engine.with_next_run(schedule).next_run_at is not None
        assert engine.with_next_run(paused).next_run_at is None
        assert engine._scheduler.get_job(str(foreign.id)) is None
        assert isinstance(engine._scheduler._jobstores["default"], MemoryJobStore)
        assert store.get(schedule.tenant_id, schedule.id).next_run_at is None
        assert len(engine._scheduler.get_jobs()) == 3
    finally:
        await engine.close()
        store.close()


async def test_interval_job_uses_coalescing_and_one_instance(runtime, schedule):
    _, engine, _ = runtime
    result = add_schedule(runtime, schedule)
    job = engine._scheduler.get_job(str(schedule.id))

    assert isinstance(job.trigger, IntervalTrigger)
    assert job.max_instances == 1
    assert job.coalesce is True
    assert job.misfire_grace_time == schedule.interval_seconds
    assert result.next_run_at == job.next_run_time
    assert schedule.next_run_at is None


async def test_cron_job_uses_utc(runtime, schedule):
    _, engine, _ = runtime
    data = schedule.model_dump()
    data.update(trigger_type="cron", interval_seconds=None, cron_expression="0 9 * * *")
    cron = Schedule.model_validate(data)
    result = add_schedule(runtime, cron)
    job = engine._scheduler.get_job(str(cron.id))

    assert isinstance(job.trigger, CronTrigger)
    assert job.misfire_grace_time == 60
    assert result.next_run_at.hour == 9
    assert result.next_run_at.minute == 0
    assert result.next_run_at.utcoffset() == timedelta(0)


async def test_unchanged_trigger_keeps_next_run_and_replaces_job(runtime, schedule):
    store, engine, _ = runtime
    original = add_schedule(runtime, schedule)
    schedule.name = "Renamed"
    updated = store.update(schedule)
    result = engine.sync_schedule(updated)

    assert result.next_run_at == original.next_run_at
    assert len(engine._scheduler.get_jobs()) == 2
    assert store.get(schedule.tenant_id, schedule.id).version == 2


async def test_interval_change_reschedules_job(runtime, schedule):
    store, engine, _ = runtime
    original = add_schedule(runtime, schedule)
    schedule.interval_seconds = 7200
    result = engine.sync_schedule(store.update(schedule))

    assert result.next_run_at > original.next_run_at
    assert engine._scheduler.get_job(str(schedule.id)).misfire_grace_time == 7200


async def test_pause_and_resume(runtime, schedule):
    store, engine, _ = runtime
    add_schedule(runtime, schedule)
    schedule.enabled = False
    paused = store.update(schedule)
    assert engine.sync_schedule(paused).next_run_at is None

    paused.enabled = True
    resumed = store.update(paused)
    assert engine.sync_schedule(resumed).next_run_at is not None


async def test_unknown_runtime_job_has_no_next_run(runtime, schedule):
    _, engine, _ = runtime
    assert engine.with_next_run(schedule).next_run_at is None


async def test_foreign_schedule_cannot_be_registered_or_read(runtime, schedule):
    _, engine, _ = runtime
    schedule.tenant_id = uuid4()
    with pytest.raises(ValueError, match="different tenant"):
        engine.sync_schedule(schedule)
    with pytest.raises(ValueError, match="different tenant"):
        engine.with_next_run(schedule)


async def test_manual_run_saves_result_without_changing_configuration(
    runtime, schedule
):
    store, engine, runner = runtime
    configured = add_schedule(runtime, schedule)
    accepted = engine.run_now(schedule.id)

    assert accepted.status == "running"
    result = await wait_for_run(store, schedule.tenant_id, accepted.id)

    assert result.status == "succeeded"
    assert result.items_processed == 5
    assert result.error_code is None
    assert result.finished_at >= result.started_at
    assert runner.calls[0][1] == accepted.id
    assert accepted.status == "running"
    assert store.get(schedule.tenant_id, schedule.id).version == 1
    assert engine.with_next_run(schedule).next_run_at == configured.next_run_at


async def test_manual_run_rejects_missing_and_foreign_schedule(runtime, schedule):
    store, engine, _ = runtime
    with pytest.raises(KeyError, match="Schedule not found"):
        engine.run_now(uuid4())
    foreign = schedule.model_copy(update={"tenant_id": uuid4()})
    store.add(foreign)
    with pytest.raises(KeyError, match="Schedule not found"):
        engine.run_now(foreign.id)


async def test_manual_run_rejects_disabled_schedule(runtime, schedule):
    _, engine, _ = runtime
    schedule.enabled = False
    add_schedule(runtime, schedule)
    with pytest.raises(RunUnavailableError, match="Enable"):
        engine.run_now(schedule.id)


async def test_manual_run_rejects_paused_scheduler(runtime, schedule):
    _, engine, _ = runtime
    add_schedule(runtime, schedule)
    engine._scheduler.pause()
    assert not engine.healthy
    with pytest.raises(RunUnavailableError, match="unavailable"):
        engine.run_now(schedule.id)


async def test_manual_and_scheduled_runs_share_connection_reservation(
    runtime, schedule
):
    store, engine, runner = runtime
    add_schedule(runtime, schedule)
    second = schedule.model_copy(update={"id": uuid4()})
    add_schedule(runtime, second)
    runner.block = True
    accepted = engine.run_now(schedule.id)
    await asyncio.wait_for(runner.started.wait(), timeout=2)

    with pytest.raises(RunUnavailableError, match="active run"):
        engine.run_now(second.id)
    await engine._run_scheduled(second.id)

    assert len(runner.calls) == 1
    assert len(store.list_runs(schedule.tenant_id)) == 1
    runner.release.set()
    await wait_for_run(store, schedule.tenant_id, accepted.id)


async def test_apscheduler_really_fires_and_coalesces_missed_intervals(
    runtime, schedule
):
    store, engine, runner = runtime
    add_schedule(runtime, schedule)
    engine._scheduler.modify_job(
        str(schedule.id),
        next_run_time=datetime.now(UTC) - timedelta(hours=3, milliseconds=10),
    )
    await asyncio.wait_for(runner.started.wait(), timeout=2)
    await wait_for_run(store, schedule.tenant_id, runner.calls[0][1])

    assert len(runner.calls) == 1
    assert engine.with_next_run(schedule).next_run_at > datetime.now(UTC)


@pytest.mark.parametrize("disabled", [False, True])
async def test_scheduled_callback_rechecks_database(runtime, schedule, disabled):
    store, engine, runner = runtime
    if disabled:
        schedule.enabled = False
        store.add(schedule)
    await engine._run_scheduled(schedule.id)
    assert runner.calls == []
    assert store.list_runs(schedule.tenant_id) == []


async def test_transient_dispatch_error_does_not_stop_future_runs(
    runtime, schedule, monkeypatch, caplog
):
    store, engine, runner = runtime
    add_schedule(runtime, schedule)
    original = store.get

    def fail_once(*args):
        raise sqlite3.OperationalError("private-token-do-not-log")

    with monkeypatch.context() as patch:
        patch.setattr(store, "get", fail_once)
        await engine._run_scheduled(schedule.id)

    assert store.get == original
    assert engine.healthy
    assert "OperationalError" in caplog.text
    assert "private-token" not in caplog.text
    await engine._run_scheduled(schedule.id)
    assert len(runner.calls) == 1
    assert store.list_runs(schedule.tenant_id)[0].status == "succeeded"


@pytest.mark.parametrize(
    "error", [ValueError("private-token"), ConnectionError("private-token")]
)
async def test_runner_failure_is_recorded_without_secret_text(
    runtime, schedule, error, caplog
):
    store, engine, runner = runtime
    add_schedule(runtime, schedule)
    runner.error = error
    accepted = engine.run_now(schedule.id)
    failed = await wait_for_run(store, schedule.tenant_id, accepted.id)

    assert failed.status == "failed"
    assert failed.error_code == "connector_failed"
    assert "private-token" not in failed.model_dump_json()
    assert "private-token" not in caplog.text
    assert engine.healthy

    runner.error = None
    next_run = engine.run_now(schedule.id)
    assert (
        await wait_for_run(store, schedule.tenant_id, next_run.id)
    ).status == "succeeded"


async def test_invalid_processed_count_is_a_failed_run(runtime, schedule):
    store, engine, runner = runtime
    add_schedule(runtime, schedule)
    runner.result = RunResult(items_processed=-1, message="Invalid")
    accepted = engine.run_now(schedule.id)
    failed = await wait_for_run(store, schedule.tenant_id, accepted.id)
    assert failed.error_code == "connector_failed"
    assert failed.items_processed == 0


async def test_deadline_cancels_waiting_async_io(runtime, schedule):
    store, engine, runner = runtime
    schedule.timeout_seconds = 1
    add_schedule(runtime, schedule)
    runner.block = True
    accepted = engine.run_now(schedule.id)
    result = await wait_for_run(store, schedule.tenant_id, accepted.id)

    assert runner.cancelled
    assert result.status == "failed"
    assert result.error_code == "connector_timeout"
    assert store.list_running_runs(schedule.tenant_id) == []
    assert len(runner.calls) == 1


async def test_runner_reported_timeout_is_recorded(runtime, schedule):
    store, engine, runner = runtime
    add_schedule(runtime, schedule)
    runner.error = ConnectorTimeoutError("private-token")
    accepted = engine.run_now(schedule.id)
    assert (
        await wait_for_run(store, schedule.tenant_id, accepted.id)
    ).error_code == "connector_timeout"


async def test_returned_result_is_not_discarded_after_wall_clock_jump(
    runtime, schedule, monkeypatch
):
    store, engine, runner = runtime
    add_schedule(runtime, schedule)
    runner.block = True
    now = datetime.now(UTC)
    monkeypatch.setattr(engine, "_clock", lambda: now)
    accepted = engine.run_now(schedule.id)
    await asyncio.wait_for(runner.started.wait(), timeout=2)
    now += timedelta(hours=1)
    runner.release.set()
    result = await wait_for_run(store, schedule.tenant_id, accepted.id)
    assert result.status == "succeeded"
    assert result.items_processed == 5


async def test_transient_history_error_retries_save_without_refetching(
    runtime, schedule, monkeypatch, caplog
):
    store, engine, runner = runtime
    add_schedule(runtime, schedule)
    original = store.finish_run
    attempts = []

    def fail_once(record):
        attempts.append(record.id)
        if len(attempts) == 1:
            raise sqlite3.OperationalError("private-token")
        original(record)

    monkeypatch.setattr(store, "finish_run", fail_once)
    accepted = engine.run_now(schedule.id)
    result = await wait_for_run(store, schedule.tenant_id, accepted.id)
    assert result.status == "succeeded"
    assert attempts == [accepted.id, accepted.id]
    assert len(runner.calls) == 1
    assert "private-token" not in caplog.text


@pytest.mark.parametrize(
    "error", [sqlite3.OperationalError("private-token"), RuntimeError("private-token")]
)
async def test_failed_history_write_keeps_reservation_and_other_jobs_work(
    runtime, schedule, monkeypatch, error, caplog
):
    store, engine, runner = runtime
    add_schedule(runtime, schedule)
    attempts = []

    def fail_save(record):
        attempts.append(record.id)
        raise error

    with monkeypatch.context() as patch:
        patch.setattr(store, "finish_run", fail_save)
        accepted = engine.run_now(schedule.id)
        await wait_for_tasks(engine)

    assert len(attempts) == (3 if isinstance(error, sqlite3.OperationalError) else 1)
    assert store.list_running_runs(schedule.tenant_id)[0].id == accepted.id
    assert len(runner.calls) == 1
    assert "private-token" not in caplog.text
    with pytest.raises(RunUnavailableError, match="active run"):
        engine.run_now(schedule.id)

    other = schedule.model_copy(update={"id": uuid4(), "connection_id": "other"})
    add_schedule(runtime, other)
    next_run = engine.run_now(other.id)
    assert (
        await wait_for_run(store, other.tenant_id, next_run.id)
    ).status == "succeeded"
    assert engine.healthy


async def test_retention_job_removes_only_expired_finished_history(runtime, schedule):
    store, engine, _ = runtime
    now = datetime.now(UTC)
    old = store.start_run(schedule, now - timedelta(days=31))
    old.status = "succeeded"
    old.finished_at = old.started_at
    store.finish_run(old)
    active = store.start_run(schedule, now - timedelta(days=40))

    await engine._prune_history()

    assert store.list_runs(schedule.tenant_id) == [active]
    assert engine._scheduler.get_job("history-retention") is not None


async def test_retention_failure_is_logged_without_stopping_scheduler(
    runtime, monkeypatch, caplog
):
    store, engine, _ = runtime

    def fail_prune(*args):
        raise sqlite3.OperationalError("private-token")

    monkeypatch.setattr(store, "prune_runs", fail_prune)
    await engine._prune_history()
    assert engine.healthy
    assert "scheduler_retention_failed" in caplog.text
    assert "private-token" not in caplog.text


async def test_job_error_listener_logs_only_exception_type(runtime, caplog):
    _, engine, _ = runtime
    event = JobExecutionEvent(
        EVENT_JOB_ERROR,
        "probe",
        "default",
        datetime.now(UTC),
        exception=ValueError("private-token"),
    )
    engine._scheduler._dispatch_event(event)
    assert "scheduler_job_error error_type=ValueError" in caplog.text
    assert "private-token" not in caplog.text


async def test_close_cancels_active_manual_run(runtime, schedule):
    store, engine, runner = runtime
    add_schedule(runtime, schedule)
    runner.block = True
    accepted = engine.run_now(schedule.id)
    await asyncio.wait_for(runner.started.wait(), timeout=2)

    await asyncio.wait_for(engine.close(), timeout=2)

    result = store.list_runs(schedule.tenant_id)[0]
    assert result.id == accepted.id
    assert result.status == "interrupted"
    assert result.error_code == "scheduler_stopped"
    assert runner.cancelled
    assert not engine.healthy
    assert not engine._tasks
    assert engine.with_next_run(schedule).next_run_at is None


async def test_close_cancels_active_scheduled_run(runtime, schedule):
    store, engine, runner = runtime
    add_schedule(runtime, schedule)
    runner.block = True
    pending = asyncio.create_task(engine._run_scheduled(schedule.id))
    await asyncio.wait_for(runner.started.wait(), timeout=2)

    await asyncio.wait_for(engine.close(), timeout=2)

    assert pending.cancelled()
    assert store.list_runs(schedule.tenant_id)[0].status == "interrupted"
    assert runner.cancelled


async def test_close_recovers_manual_task_cancelled_before_first_step(
    runtime, schedule
):
    store, engine, runner = runtime
    add_schedule(runtime, schedule)
    accepted = engine.run_now(schedule.id)
    await engine.close()
    assert runner.calls == []
    assert store.list_runs(schedule.tenant_id)[0].id == accepted.id
    assert store.list_runs(schedule.tenant_id)[0].status == "interrupted"


async def test_closed_engine_rejects_operations_and_callbacks_do_nothing(
    runtime, schedule
):
    _, engine, runner = runtime
    await engine.close()
    await engine.close()
    with pytest.raises(RuntimeError, match="closed"):
        engine.start()
    with pytest.raises(RuntimeError, match="not running"):
        engine.sync_schedule(schedule)
    with pytest.raises(RuntimeError, match="not running"):
        engine.run_now(schedule.id)
    await engine._run_scheduled(schedule.id)
    await engine._prune_history()
    assert runner.calls == []


async def test_unstarted_engine_closes_cleanly(runtime, tenant_id, schedule):
    store, _, runner = runtime
    engine = SchedulerEngine(store, runner, tenant_id, clock=lambda: datetime.now(UTC))
    with pytest.raises(RuntimeError, match="not running"):
        engine.sync_schedule(schedule)
    assert not engine.healthy
    await engine.close()
