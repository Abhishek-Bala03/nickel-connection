"""Execute due schedules with retries, deadlines, and manual runs."""

from collections.abc import Callable
from concurrent.futures import Executor, Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import partial
from time import monotonic
from uuid import UUID

from .models import RunRecord, Schedule
from .runner import ConnectorRunner, ConnectorTimeoutError, RetryableConnectorError
from .store import ScheduleStore


class RunUnavailableError(RuntimeError):
    """A manual run cannot start under the current scheduler state."""


@dataclass
class PendingJob:
    """Execution state retained until a run finishes."""

    schedule: Schedule
    record: RunRecord
    attempts: int = 1
    deadline: float = 0.0
    completed_at: float | None = None


class SchedulerEngine:
    """Coordinate connector jobs from one calling thread."""

    def __init__(
        self,
        store: ScheduleStore,
        runner: ConnectorRunner,
        tenant_id: UUID,
        *,
        max_workers: int = 4,
        clock: Callable[[], datetime] | None = None,
        monotonic_clock: Callable[[], float] | None = None,
        executor: Executor | None = None,
    ):
        if max_workers < 1:
            raise ValueError("max_workers must be positive")

        self._store = store
        self._runner = runner
        self._tenant_id = tenant_id
        self._max_workers = max_workers
        self._clock = clock or (lambda: datetime.now(UTC))
        self._monotonic = monotonic_clock or monotonic
        self._executor = (
            executor
            if executor is not None
            else ThreadPoolExecutor(max_workers=max_workers)
        )
        self._pending: dict[Future, PendingJob] = {}
        self._retries: dict[UUID, tuple[float, PendingJob]] = {}
        self._closed = False

    def tick(self) -> list[RunRecord]:
        """Collect results, retry eligible jobs, and start due schedules."""
        if self._closed:
            raise RuntimeError("Scheduler engine is closed")

        now = self._clock()
        self._collect_finished(now)
        self._retry_due(now)
        started = []

        for schedule in self._store.list_schedules(self._tenant_id):
            active_count = len(self._pending) + len(self._retries)
            if active_count >= self._max_workers:
                break

            if not schedule.enabled or schedule.next_run_at > now:
                continue

            record = self._start(schedule, now)
            if record is not None:
                started.append(record)

        return started

    def run_now(self, schedule_id: UUID) -> RunRecord:
        """Start an enabled schedule through the normal execution path."""
        if self._closed:
            raise RuntimeError("Scheduler engine is closed")

        schedule = self._store.get(self._tenant_id, schedule_id)
        if schedule is None:
            raise KeyError("Schedule not found")

        if not schedule.enabled:
            raise RunUnavailableError("Enable the schedule before running it")

        now = self._clock()
        self._collect_finished(now)

        active_count = len(self._pending) + len(self._retries)
        if active_count >= self._max_workers:
            raise RunUnavailableError("Scheduler is at capacity")

        record = self._start(schedule, now)
        if record is None:
            raise RunUnavailableError("This connection already has an active run")

        return record

    def _start(self, schedule: Schedule, now: datetime) -> RunRecord | None:
        record = self._store.start_run(schedule, now)
        if record is None:
            return None

        job = PendingJob(schedule=schedule, record=record)

        try:
            # Missed intervals produce one run, followed by a fresh interval.
            schedule.next_run_at = now + timedelta(seconds=schedule.interval_seconds)
            self._store.update(schedule)
        except Exception:
            self._fail(job, now, "dispatch_failed")
            raise

        self._submit(job, now)
        return record

    def _submit(self, job: PendingJob, now: datetime) -> None:
        job.completed_at = None
        job.deadline = self._monotonic() + job.schedule.timeout_seconds

        try:
            future = self._executor.submit(
                self._runner.run,
                job.schedule,
                job.record.id,
            )
        except Exception:
            self._fail(job, now, "dispatch_failed")
            raise

        self._pending[future] = job
        future.add_done_callback(partial(self._mark_completed, job))

    def _mark_completed(self, job: PendingJob, future: Future) -> None:
        """Capture completion time without accessing the database."""
        job.completed_at = self._monotonic()

    def _collect_finished(self, now: datetime) -> None:
        for future, job in list(self._pending.items()):
            # Keep the reservation until the worker has actually finished.
            if job.completed_at is None:
                continue

            try:
                if job.completed_at >= job.deadline:
                    raise ConnectorTimeoutError()

                result = future.result()
                job.record.items_processed = result.items_processed
            except ConnectorTimeoutError:
                self._retry_or_fail(job, now, "connector_timeout")
            except RetryableConnectorError:
                self._retry_or_fail(job, now, "connector_failed")
            except Exception:
                # Exception text may contain credentials or patient data.
                self._fail(job, now, "connector_failed")
            else:
                job.record.status = "succeeded"
                job.record.finished_at = max(now, job.record.started_at)
                self._store.finish_run(job.record)

            del self._pending[future]

    def _retry_or_fail(self, job: PendingJob, now: datetime, error_code: str) -> None:
        if self._closed or job.attempts > job.schedule.max_retries:
            self._fail(job, now, error_code)
            return

        delay = job.schedule.retry_delay_seconds * (2 ** (job.attempts - 1))
        retry_at = self._monotonic() + delay
        self._retries[job.record.id] = (retry_at, job)

    def _retry_due(self, now: datetime) -> None:
        elapsed_now = self._monotonic()

        for run_id, (retry_at, job) in list(self._retries.items()):
            if retry_at > elapsed_now:
                continue

            del self._retries[run_id]
            job.attempts += 1
            self._submit(job, now)

    def _fail(self, job: PendingJob, now: datetime, error_code: str) -> None:
        job.record.status = "failed"
        job.record.finished_at = max(now, job.record.started_at)
        job.record.error_code = error_code
        self._store.finish_run(job.record)

    def close(self) -> None:
        """Finish submitted work and interrupt retries not yet submitted."""
        if self._closed:
            return

        self._closed = True
        self._executor.shutdown(wait=True)
        now = self._clock()
        self._collect_finished(now)

        for _, job in self._retries.values():
            job.record.status = "interrupted"
            job.record.finished_at = max(now, job.record.started_at)
            job.record.error_code = "scheduler_stopped"
            self._store.finish_run(job.record)

        self._retries.clear()
