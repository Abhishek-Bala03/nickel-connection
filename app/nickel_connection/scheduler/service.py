"""Run one scheduler service against a local SQLite database."""

import fcntl
import math
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from threading import Event
from uuid import UUID

from .engine import SchedulerEngine
from .runner import ConnectorRunner
from .store import ScheduleStore


@contextmanager
def database_lock(database: Path) -> Iterator[None]:
    """Hold exclusive ownership until the scheduler has stopped."""
    lock_path = database.with_name(f"{database.name}.lock")

    with lock_path.open("a") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError(
                "Another scheduler already owns this database"
            ) from error

        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def recover_interrupted_runs(
    store: ScheduleStore,
    tenant_id: UUID,
    now: datetime,
) -> int:
    """Recover every abandoned run after exclusive ownership is obtained."""
    records = store.list_running_runs(tenant_id)

    for record in records:
        record.status = "interrupted"
        record.finished_at = max(now, record.started_at)
        record.error_code = "scheduler_restarted"
        store.finish_run(record)

    return len(records)


def serve(
    database: Path,
    tenant_id: UUID,
    runner: ConnectorRunner,
    stop: Event,
    *,
    poll_seconds: float = 1.0,
) -> None:
    """Run until shutdown is requested; finish workers before unlocking."""
    if not math.isfinite(poll_seconds) or poll_seconds <= 0:
        raise ValueError("poll_seconds must be finite and positive")

    database = database.resolve()
    database.parent.mkdir(parents=True, exist_ok=True)

    with database_lock(database):
        store = ScheduleStore(database)
        engine = None

        try:
            recover_interrupted_runs(store, tenant_id, datetime.now(UTC))
            engine = SchedulerEngine(store, runner, tenant_id)

            while not stop.is_set():
                engine.tick()
                stop.wait(poll_seconds)
        finally:
            try:
                if engine is not None:
                    engine.close()
            finally:
                store.close()
