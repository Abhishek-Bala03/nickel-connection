"""Liveness, readiness and version: for the Docker health check, K3s probes and the connections UI."""

from fastapi import APIRouter

from src.config import Service
from src.models import HealthResponse, ReadyResponse, VersionResponse

health_router = APIRouter(tags=["health"])


@health_router.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    """The process is up."""
    return HealthResponse(status="ok")


@health_router.get("/ready", response_model=ReadyResponse)
def ready() -> ReadyResponse:
    """The service can take traffic. Dependencies (database, Kafka, Nango) add their checks here."""
    return ReadyResponse(status="ready", checks={})


@health_router.get("/version", response_model=VersionResponse)
def version() -> VersionResponse:
    return VersionResponse(
        name=Service.name, version=Service.version, environment=Service.environment
    )
