from datetime import UTC, datetime, timedelta
from uuid import uuid4

from src.scheduler.models import Schedule
from src.scheduler.service import recover_interrupted_runs
from src.scheduler.store import ScheduleStore


def test_recovery_all_running_rows_and_tenant_isolation(tmp_path):
    now = datetime.now(UTC)
    tenant = uuid4()
    store = ScheduleStore(tmp_path / "scheduler.db")
    try:
        for i in range(55):
            schedule = Schedule(
                name="test",
                tenant_id=tenant,
                connector_type="demo",
                connection_id=str(i),
                interval_seconds=30,
            )
            store.add(schedule)
            store.start_run(schedule, now + timedelta(seconds=1))
        other = Schedule(
            name="other",
            tenant_id=uuid4(),
            connector_type="demo",
            connection_id="other",
            interval_seconds=30,
        )
        store.add(other)
        store.start_run(other, now)
        assert recover_interrupted_runs(store, tenant, now) == 55
        assert recover_interrupted_runs(store, tenant, now) == 0
        assert len(store.list_running_runs(other.tenant_id)) == 1
        assert all(
            run.status == "interrupted"
            and run.error_code == "scheduler_restarted"
            and run.finished_at >= run.started_at
            for run in store.list_runs(tenant, limit=200)
        )
    finally:
        store.close()
