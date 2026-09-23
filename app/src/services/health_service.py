"""Liveness, readiness and version: for the Docker health check, K3s probes and the connections UI.

``/health`` and ``/ready`` are ``async`` on purpose, although they await nothing yet: they then run on
the event loop, so a probe still answers while blocking routes hold every worker thread, instead of
queueing behind them until K3s restarts a healthy pod. SonarQube's S7503 is silenced for them.
"""

from fastapi import APIRouter, Request

from src.models import HealthResponse, ReadyResponse, VersionResponse

health_router = APIRouter(tags=["health"])


@health_router.get("/health")
async def health() -> HealthResponse:  # NOSONAR(S7503)
    """The process is up."""
    return HealthResponse(status="ok")


@health_router.get("/ready")
async def ready() -> ReadyResponse:  # NOSONAR(S7503)
    """The service can take traffic. Dependencies (database, Kafka, Nango) add their checks here."""
    return ReadyResponse(status="ready", checks={})


@health_router.get("/version")
def version(request: Request) -> VersionResponse:
    settings = request.app.state.settings  # the app's own (main.create_app)
    return VersionResponse(
        name=settings.name, version=settings.version, environment=settings.environment
    )
