"""Startup recovery for the single scheduler API process."""

from datetime import datetime
from uuid import UUID

from .store import ScheduleStore


def recover_interrupted_runs(
    store: ScheduleStore, tenant_id: UUID, now: datetime
) -> int:
    records = store.list_running_runs(tenant_id)
    for record in records:
        record.status = "interrupted"
        record.finished_at = max(now, record.started_at)
        record.error_code = "scheduler_restarted"
        store.finish_run(record)
    return len(records)
