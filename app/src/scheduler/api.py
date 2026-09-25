"""Authenticated configuration API; one APScheduler worker per deployment."""

import logging
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated
from uuid import UUID

import httpx
from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Query
from pydantic import TypeAdapter

from src.error import register_error_handlers
from src.middlewares.security_headers import SecurityHeadersMiddleware
from src.services.auth import ServiceToken, ServiceTokenMiddleware, check_token

from .engine import RunUnavailableError, SchedulerEngine
from .models import RunRecord, Schedule, ScheduleSettings, ScheduleUpdate
from .registry import Connection, ConnectorRegistry
from .runner import ConnectorRunner, DemoConnector, JobsConnector
from .service import recover_interrupted_runs
from .store import ScheduleConflictError, ScheduleStore

logger = logging.getLogger(__name__)


async def refresh_nango_connections(
    client: httpx.AsyncClient, registry: ConnectorRegistry
) -> bool:
    """Take the enabled Nango connectors from the connection service; False when it cannot say."""
    try:
        response = await client.get("/connectors", timeout=5)
        response.raise_for_status()
        registry.replace_remote(
            [
                Connection(id=c["id"], name=c["name"], connector_type="nango")
                for c in response.json()
                if c.get("source") == "nango" and c.get("enabled")
            ]
        )
        return True
    except (httpx.HTTPError, ValueError, KeyError, TypeError) as error:
        logger.warning(
            "scheduler_connectors_unavailable error_type=%s", type(error).__name__
        )
        return False


@dataclass(frozen=True)
class Settings:
    service_token: str
    environment: str = "production"


def build_app(
    database: Path,
    tenant_id: UUID,
    *,
    service_token: str,
    registry: ConnectorRegistry,
    runner=None,
    development: bool = False,
    upstream: str = "http://nickel-connection:8000",
) -> FastAPI:
    check_token(service_token)
    if not development and any(
        c.connector_type == "demo" for c in registry.connections
    ):
        raise ValueError("Demo connections require development mode")
    url = httpx.URL(upstream)
    if (
        url.scheme not in {"http", "https"}
        or not url.host
        or url.userinfo
        or url.query
        or url.fragment
    ):
        raise ValueError("Invalid connection service URL")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        database.parent.mkdir(parents=True, exist_ok=True)
        store = ScheduleStore(database)
        async with httpx.AsyncClient(
            base_url=upstream,
            headers={"Authorization": f"Bearer {service_token}"},
            follow_redirects=False,
            trust_env=False,
        ) as client:
            jobs = JobsConnector(client, registry)
            connectors = {"documents": jobs, "nango": jobs}
            if development:
                connectors["demo"] = DemoConnector()
            app.state.client = client
            engine = SchedulerEngine(
                store, runner or ConnectorRunner(connectors), tenant_id
            )
            app.state.store = store
            app.state.engine = engine
            try:
                recover_interrupted_runs(store, tenant_id, datetime.now(UTC))
                remote = await refresh_nango_connections(client, registry)
                # Refuse unsafe/removed deployment connections before starting jobs. Nango
                # schedules are checked when their run starts if the connection service is down.
                for schedule in store.list_schedules(tenant_id):
                    if schedule.connector_type != "nango" or remote:
                        registry.validate(schedule)
                engine.start()
                yield
            finally:
                try:
                    await engine.close()
                finally:
                    store.close()

    app = FastAPI(
        title="Nickel Scheduler",
        lifespan=lifespan,
        docs_url="/docs" if development else None,
        redoc_url=None,
        openapi_url="/openapi.json" if development else None,
    )
    app.state.settings = Settings(
        service_token, "development" if development else "production"
    )
    register_error_handlers(app)
    app.add_middleware(ServiceTokenMiddleware)
    app.add_middleware(SecurityHeadersMiddleware)
    router = APIRouter(dependencies=[ServiceToken])

    def healthy():
        if not app.state.engine.healthy:
            raise HTTPException(503, "Scheduler unavailable")

    def actor(
        x_authenticated_user: Annotated[str, Header(min_length=1, max_length=255)]
    ):
        # Only the trusted, service-authenticated gateway may supply this header.
        value = x_authenticated_user.strip()
        if not value or any(ord(char) < 32 for char in value):
            raise HTTPException(422, "Invalid authenticated identity")
        return value

    async def validate(settings):
        if settings.connector_type == "nango" and not await refresh_nango_connections(
            app.state.client, registry
        ):
            raise HTTPException(503, "The connection service is unavailable")
        try:
            registry.validate(settings)
        except ValueError as error:
            raise HTTPException(422, str(error)) from error

    def sync(schedule):
        try:
            return app.state.engine.sync_schedule(schedule)
        except Exception as error:
            logger.error("scheduler_sync_failed error_type=%s", type(error).__name__)
            raise HTTPException(
                503,
                "Settings saved; scheduler synchronization failed. Restart the scheduler.",
            ) from error

    @app.get("/health")
    async def health():
        healthy()
        return {
            "status": "ok",
            "mode": (
                "demo"
                if registry.connections
                and all(c.connector_type == "demo" for c in registry.connections)
                else "jobs"
            ),
        }

    @router.get("/connectors")
    async def connectors():
        return registry.connectors

    @router.get("/connections")
    async def connections():
        await refresh_nango_connections(app.state.client, registry)
        return [
            {"id": c.id, "name": c.name, "connector_type": c.connector_type}
            for c in registry.connections
        ]

    @router.get("/schedules", response_model=list[Schedule])
    async def schedules():
        return [
            app.state.engine.with_next_run(s)
            for s in app.state.store.list_schedules(tenant_id)
        ]

    @router.post("/schedules", response_model=Schedule, status_code=201)
    async def create(settings: ScheduleSettings, changed_by: str = Depends(actor)):
        healthy()
        await validate(settings)
        schedule = Schedule(
            **settings.model_dump(), tenant_id=tenant_id, changed_by=changed_by
        )
        app.state.store.add(schedule)
        return sync(schedule)

    @router.put("/schedules/{schedule_id}", response_model=Schedule)
    async def update(
        schedule_id: UUID, settings: ScheduleUpdate, changed_by: str = Depends(actor)
    ):
        healthy()
        await validate(settings)
        previous = app.state.store.get(tenant_id, schedule_id)
        if previous is None:
            raise HTTPException(404, "Schedule not found")
        updated = Schedule(
            **settings.model_dump(),
            id=previous.id,
            tenant_id=tenant_id,
            created_at=previous.created_at,
            changed_by=changed_by,
        )
        try:
            saved = app.state.store.update(updated)
        except ScheduleConflictError as error:
            raise HTTPException(
                409,
                "Schedule changed. Reload the edit form before saving; connections cannot be changed.",
            ) from error
        return sync(saved)

    @router.post(
        "/schedules/{schedule_id}/run", response_model=RunRecord, status_code=202
    )
    async def run(schedule_id: UUID, changed_by: str = Depends(actor)):
        healthy()
        try:
            record = app.state.engine.run_now(schedule_id)
            logger.info(
                "scheduler_manual_run run_id=%s actor=%s", record.id, changed_by
            )
            return record
        except KeyError as error:
            raise HTTPException(404, "Schedule not found") from error
        except RunUnavailableError as error:
            raise HTTPException(409, str(error)) from error

    @router.get("/runs", response_model=list[RunRecord])
    async def runs(
        limit: Annotated[int, Query(ge=1, le=200)] = 50, schedule_id: UUID | None = None
    ):
        return app.state.store.list_runs(
            tenant_id, limit=limit, schedule_id=schedule_id
        )

    app.include_router(router)
    return app


def create_app() -> FastAPI:
    token_file = os.environ.get("NICKEL_SERVICE_TOKEN_FILE")
    token = (
        Path(token_file).read_text().strip()
        if token_file
        else os.environ.get("NICKEL_SERVICE_TOKEN", "")
    )
    development = os.environ.get("NICKEL_ENV") == "development"
    connection_file = os.environ.get("NICKEL_SCHEDULER_CONNECTIONS")
    connections = (
        TypeAdapter(list[Connection]).validate_json(Path(connection_file).read_text())
        if connection_file
        else []
    )
    return build_app(
        Path(os.environ["NICKEL_SCHEDULER_DB"]),
        UUID(os.environ["NICKEL_TENANT_ID"]),
        service_token=token,
        registry=ConnectorRegistry(connections, demo=development),
        development=development,
        upstream=os.environ.get(
            "NICKEL_CONNECTION_URL", "http://nickel-connection:8000"
        ),
    )
