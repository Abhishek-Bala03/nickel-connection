"""Application-owned schedules executed by a single APScheduler instance."""

import asyncio
import inspect
import logging
import sqlite3
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Protocol
from uuid import UUID

from apscheduler.events import EVENT_JOB_ERROR, EVENT_SCHEDULER_SHUTDOWN
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.schedulers.base import STATE_RUNNING
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from .models import RunRecord, Schedule
from .runner import ConnectorTimeoutError, RunResult
from .store import ScheduleStore

logger = logging.getLogger(__name__)


class RunUnavailableError(RuntimeError):
    """A manual run cannot start under the current scheduler state."""


class AsyncRunner(Protocol):
    """Cancellation-safe runner; retries reuse run_id as their idempotency key."""

    async def run(self, schedule: Schedule, run_id: UUID) -> RunResult:
        """Perform bounded async requests, including any retry back-off."""
        ...


class SchedulerEngine:
    """Own runtime triggers; the application database owns their configuration."""

    def __init__(
        self,
        store: ScheduleStore,
        runner: AsyncRunner,
        tenant_id: UUID,
        *,
        clock: Callable[[], datetime] | None = None,
        retention_days: int = 30,
    ):
        if not inspect.iscoroutinefunction(runner.run):
            raise TypeError("Scheduler requires an asynchronous runner")
        if type(retention_days) is not int or not 1 <= retention_days <= 3650:
            raise ValueError("retention_days must be between 1 and 3650")

        self._store = store
        self._runner = runner
        self._tenant_id = tenant_id
        self._clock = clock or (lambda: datetime.now(UTC))
        self._retention_days = retention_days
        self._tasks: set[asyncio.Task] = set()
        self._closed = False
        self._scheduler = AsyncIOScheduler(
            timezone=UTC,
            jobstores={"default": MemoryJobStore()},
            job_defaults={"max_instances": 1, "coalesce": True},
        )
        self._scheduler.add_listener(self._job_error, EVENT_JOB_ERROR)

    @property
    def healthy(self) -> bool:
        return not self._closed and self._scheduler.state == STATE_RUNNING

    def start(self) -> None:
        """Rebuild the in-memory job store before enabling execution."""
        if self._closed:
            raise RuntimeError("Scheduler engine is closed")
        schedules = self._store.list_schedules(self._tenant_id)
        self._scheduler.start(paused=True)
        for schedule in schedules:
            self.sync_schedule(schedule)
        self._scheduler.add_job(
            self._prune_history,
            trigger="interval",
            minutes=5,
            id="history-retention",
            replace_existing=True,
            misfire_grace_time=300,
        )
        self._scheduler.resume()

    def sync_schedule(self, schedule: Schedule) -> Schedule:
        """Call after a committed create/update; preserve unchanged trigger timing."""
        self._require_running()
        if schedule.tenant_id != self._tenant_id:
            raise ValueError("Schedule belongs to a different tenant")

        if schedule.trigger_type == "interval":
            trigger = IntervalTrigger(
                seconds=schedule.interval_seconds,
                start_date=self._clock() + timedelta(seconds=schedule.interval_seconds),
                timezone=UTC,
            )
            grace = schedule.interval_seconds
        else:
            trigger = CronTrigger.from_crontab(schedule.cron_expression, timezone=UTC)
            # Cron has minute precision; avoid replaying an old wall-clock event.
            grace = 60

        job_id = str(schedule.id)
        previous = self._scheduler.get_job(job_id)
        next_run = trigger.get_next_fire_time(None, self._clock())
        if previous is not None and str(previous.trigger) == str(trigger):
            next_run = previous.next_run_time

        job = self._scheduler.add_job(
            self._run_scheduled,
            trigger=trigger,
            args=[schedule.id],
            id=job_id,
            name=job_id,
            replace_existing=True,
            next_run_time=next_run,
            misfire_grace_time=grace,
        )
        if not schedule.enabled:
            self._scheduler.pause_job(job_id)
        elif job.next_run_time is None:
            self._scheduler.resume_job(job_id)
        return self.with_next_run(schedule)

    def with_next_run(self, schedule: Schedule) -> Schedule:
        """Attach APScheduler's current next run without writing it to the DB."""
        if schedule.tenant_id != self._tenant_id:
            raise ValueError("Schedule belongs to a different tenant")
        job = self._scheduler.get_job(str(schedule.id))
        return schedule.model_copy(
            update={
                "next_run_at": (
                    job.next_run_time
                    if job is not None and schedule.enabled and not self._closed
                    else None
                )
            }
        )

    def run_now(self, schedule_id: UUID) -> RunRecord:
        """Reserve a run synchronously so the API can return an accepted run ID."""
        self._require_running()
        if not self.healthy:
            raise RunUnavailableError("Scheduler is unavailable")
        schedule = self._store.get(self._tenant_id, schedule_id)
        if schedule is None:
            raise KeyError("Schedule not found")
        if not schedule.enabled:
            raise RunUnavailableError("Enable the schedule before running it")

        record = self._store.start_run(schedule, self._clock())
        if record is None:
            raise RunUnavailableError("This connection already has an active run")

        deadline = asyncio.get_running_loop().time() + schedule.timeout_seconds
        task = asyncio.create_task(self._execute(schedule, record, deadline))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return record.model_copy()

    async def _run_scheduled(self, schedule_id: UUID) -> None:
        if self._closed:
            return
        task = asyncio.current_task()
        self._tasks.add(task)
        try:
            schedule = self._store.get(self._tenant_id, schedule_id)
            if schedule is None or not schedule.enabled:
                return
            record = self._store.start_run(schedule, self._clock())
            if record is not None:
                deadline = asyncio.get_running_loop().time() + schedule.timeout_seconds
                await self._execute(schedule, record, deadline)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            # Do not log exception text: it can contain credentials or payloads.
            logger.error(
                "scheduler_dispatch_failed error_type=%s", type(error).__name__
            )
        finally:
            self._tasks.discard(task)

    async def _execute(
        self, schedule: Schedule, record: RunRecord, deadline: float
    ) -> None:
        try:
            async with asyncio.timeout_at(deadline):
                result = await self._runner.run(schedule, record.id)
            record.items_processed = result.items_processed
            record.job_id = result.job_id
            record.status = "succeeded"
        except asyncio.CancelledError:
            record.status = "interrupted"
            record.error_code = "scheduler_stopped"
            raise
        except (TimeoutError, ConnectorTimeoutError):
            record.status = "failed"
            record.error_code = "connector_timeout"
        except Exception as error:
            record.status = "failed"
            record.error_code = "connector_failed"
            record.job_id = getattr(error, "job_id", None)
            logger.error("scheduler_run_failed error_type=%s", type(error).__name__)
        finally:
            record.finished_at = max(self._clock(), record.started_at)
            await self._finish(record)

    async def _finish(self, record: RunRecord) -> None:
        # Retry saving the result, never repeat the connector to fix a DB error.
        for attempt in range(3):
            try:
                self._store.finish_run(record)
                return
            except sqlite3.OperationalError as error:
                logger.error(
                    "scheduler_history_failed error_type=%s", type(error).__name__
                )
                if attempt < 2:
                    await asyncio.sleep(0.1 * (2**attempt))
            except Exception as error:
                logger.error(
                    "scheduler_history_failed error_type=%s", type(error).__name__
                )
                return
        # Keep this connection's reservation if saving still fails.
        # Other schedules continue; recovery handles this row on restart.

    async def _prune_history(self) -> None:
        if self._closed:
            return
        try:
            cutoff = self._clock() - timedelta(days=self._retention_days)
            self._store.prune_runs(self._tenant_id, cutoff)
        except Exception as error:
            logger.error(
                "scheduler_retention_failed error_type=%s", type(error).__name__
            )

    @staticmethod
    def _job_error(event) -> None:
        logger.error(
            "scheduler_job_error error_type=%s", type(event.exception).__name__
        )

    def _require_running(self) -> None:
        if self._closed or not self._scheduler.running:
            raise RuntimeError("Scheduler engine is not running")

    async def close(self) -> None:
        """Cancel async work and await cleanup before the caller closes storage."""
        if self._closed:
            return
        self._closed = True
        if self._scheduler.running:
            self._scheduler.pause()

        tasks = tuple(self._tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        if self._scheduler.running:
            stopped = asyncio.Event()
            self._scheduler.add_listener(
                lambda event: stopped.set(), EVENT_SCHEDULER_SHUTDOWN
            )
            self._scheduler.shutdown(wait=False)
            await stopped.wait()

        # A manual task cancelled before its first step cannot run its finally block.
        for record in self._store.list_running_runs(self._tenant_id):
            record.status = "interrupted"
            record.finished_at = max(self._clock(), record.started_at)
            record.error_code = "scheduler_stopped"
            await self._finish(record)
