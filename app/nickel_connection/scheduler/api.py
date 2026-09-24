"""Local configuration API with an embedded scheduler."""

import asyncio
import os
from contextlib import asynccontextmanager, suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

from fastapi import FastAPI, HTTPException

from .engine import RunUnavailableError, SchedulerEngine
from .models import RunRecord, Schedule, ScheduleSettings
from .runner import ConnectorRunner, DemoConnector
from .service import database_lock, recover_interrupted_runs
from .store import ScheduleStore


def build_app(
    database: Path,
    tenant_id: UUID,
    runner: ConnectorRunner | None = None,
) -> FastAPI:
    """Build the local demo API for one deployment-configured tenant."""
    database = database.resolve()
    connector_runner = (
        runner if runner is not None else ConnectorRunner({"demo": DemoConnector()})
    )

    async def scheduler_loop(app: FastAPI) -> None:
        while True:
            try:
                app.state.engine.tick()
            except Exception:
                app.state.scheduler_failed = True
                return

            await asyncio.sleep(1.0)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        database.parent.mkdir(parents=True, exist_ok=True)

        with database_lock(database):
            store = ScheduleStore(database)
            engine = None
            task = None

            try:
                recover_interrupted_runs(store, tenant_id, datetime.now(UTC))
                engine = SchedulerEngine(store, connector_runner, tenant_id)
                app.state.store = store
                app.state.engine = engine
                app.state.scheduler_failed = False
                task = asyncio.create_task(scheduler_loop(app))
                yield
            finally:
                try:
                    if task is not None:
                        task.cancel()
                        with suppress(asyncio.CancelledError):
                            await task

                    if engine is not None:
                        engine.close()
                finally:
                    store.close()

    app = FastAPI(title="Nickel Scheduler — Local Demo", lifespan=lifespan)

    def require_healthy() -> None:
        if app.state.scheduler_failed:
            raise HTTPException(
                status_code=503,
                detail="Scheduler stopped after an internal failure",
            )

    def get_schedule(schedule_id: UUID) -> Schedule:
        schedule = app.state.store.get(tenant_id, schedule_id)
        if schedule is None:
            raise HTTPException(status_code=404, detail="Schedule not found")
        return schedule

    def validate_connector(settings: ScheduleSettings) -> None:
        if settings.connector_type != "demo":
            raise HTTPException(
                status_code=422,
                detail="Only the demo connector is available in this version",
            )

    @app.get("/health")
    async def health() -> dict[str, str]:
        require_healthy()
        return {"status": "ok", "mode": "demo"}

    @app.get("/schedules", response_model=list[Schedule])
    async def list_schedules():
        return app.state.store.list_schedules(tenant_id)

    @app.post("/schedules", response_model=Schedule, status_code=201)
    async def create_schedule(settings: ScheduleSettings):
        require_healthy()
        validate_connector(settings)

        schedule = Schedule(
            **settings.model_dump(),
            tenant_id=tenant_id,
            next_run_at=datetime.now(UTC)
            + timedelta(seconds=settings.interval_seconds),
        )
        app.state.store.add(schedule)
        return schedule

    @app.put("/schedules/{schedule_id}", response_model=Schedule)
    async def update_schedule(schedule_id: UUID, settings: ScheduleSettings):
        require_healthy()
        validate_connector(settings)
        previous = get_schedule(schedule_id)

        if (
            settings.connection_id != previous.connection_id
            or settings.connector_type != previous.connector_type
        ):
            raise HTTPException(
                status_code=409,
                detail="Create a new schedule to select a different connection",
            )

        next_run_at = previous.next_run_at
        if (
            settings.interval_seconds != previous.interval_seconds
            or settings.enabled
            and not previous.enabled
        ):
            next_run_at = datetime.now(UTC) + timedelta(
                seconds=settings.interval_seconds
            )

        updated = Schedule(
            **settings.model_dump(),
            id=previous.id,
            tenant_id=tenant_id,
            next_run_at=next_run_at,
        )
        app.state.store.update(updated)
        return updated

    @app.post(
        "/schedules/{schedule_id}/run",
        response_model=RunRecord,
        status_code=202,
    )
    async def run_schedule(schedule_id: UUID):
        require_healthy()
        try:
            return app.state.engine.run_now(schedule_id)
        except KeyError as error:
            raise HTTPException(status_code=404, detail="Schedule not found") from error
        except RunUnavailableError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error

    @app.get("/runs", response_model=list[RunRecord])
    async def list_runs():
        return app.state.store.list_runs(tenant_id)

    return app


def create_app() -> FastAPI:
    """Load deployment configuration for Uvicorn's application factory."""
    database = Path(os.environ["NICKEL_SCHEDULER_DB"])
    tenant_id = UUID(os.environ["NICKEL_TENANT_ID"])
    return build_app(database, tenant_id)
