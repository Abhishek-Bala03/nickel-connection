"""Liveness, readiness and version: for the Docker health check, K3s probes and the connections UI.

``/health`` and ``/ready`` are ``async`` on purpose, although they await nothing yet: they then run on
the event loop, so a probe still answers while blocking routes hold every worker thread, instead of
queueing behind them until K3s restarts a healthy pod. SonarQube's S7503 is silenced for them.
"""

from fastapi import APIRouter

from src.config import Service
from src.models import HealthResponse, ReadyResponse, VersionResponse

health_router = APIRouter(tags=["health"])


@health_router.get("/health")
async def health() -> HealthResponse:  # NOSONAR: async on purpose, see above
    """The process is up."""
    return HealthResponse(status="ok")


@health_router.get("/ready")
async def ready() -> ReadyResponse:  # NOSONAR: async on purpose, see above
    """The service can take traffic. Dependencies (database, Kafka, Nango) add their checks here."""
    return ReadyResponse(status="ready", checks={})


@health_router.get("/version")
def version() -> VersionResponse:
    return VersionResponse(
        name=Service.name, version=Service.version, environment=Service.environment
    )
