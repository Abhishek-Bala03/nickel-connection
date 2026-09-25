"""Async connector adapters with bounded requests and idempotent submission."""

import asyncio
from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

import httpx

from .models import Schedule
from .registry import ConnectorRegistry


class RetryableConnectorError(RuntimeError):
    """A transient request failure."""


class ConnectorTimeoutError(RetryableConnectorError):
    """The execution deadline expired."""


@dataclass(frozen=True)
class RunContext:
    run_id: UUID
    tenant_id: UUID
    connection_id: str
    timeout_seconds: int = 60


@dataclass(frozen=True)
class RunResult:
    items_processed: int
    message: str
    job_id: str | None = None


class ConnectionJobFailedError(RuntimeError):
    """The connection service ran the job and it failed; its record says why."""

    def __init__(self, job_id: str):
        super().__init__("Connection job failed")
        self.job_id = job_id


# connector type -> (the jobs API path, the request body for a connection, the count reported)
_JOB_KINDS = {
    "documents": (
        "/jobs/documents/pull",
        lambda c: {"manifest": c.manifest},
        "published",
    ),
    "nango": ("/jobs/nango/pull", lambda c: {"connector_id": c.id}, "records"),
}


class Connector(Protocol):
    async def run(self, schedule: Schedule, run_id: UUID) -> RunResult: ...


class DemoConnector:
    async def run(self, schedule: Schedule, run_id: UUID) -> RunResult:
        return RunResult(0, "Simulation completed; no source data was fetched.")


class JobsConnector:
    def __init__(
        self,
        client: httpx.AsyncClient,
        registry: ConnectorRegistry,
        *,
        poll_seconds: float = 1.0,
    ):
        self._client = client
        self._registry = registry
        self._poll_seconds = poll_seconds

    async def _request(
        self, method: str, path: str, schedule: Schedule, **kwargs
    ) -> dict:
        for attempt in range(schedule.max_retries + 1):
            try:
                response = await self._client.request(
                    method, path, timeout=min(10, schedule.timeout_seconds), **kwargs
                )
                if response.status_code not in {429, 500, 502, 503, 504}:
                    response.raise_for_status()
                    return response.json()
            except httpx.TransportError:
                pass
            if attempt == schedule.max_retries:
                raise RetryableConnectorError("Connection jobs API unavailable")
            await asyncio.sleep(schedule.retry_delay_seconds * (2**attempt))
        raise AssertionError("Unreachable retry state")  # pragma: no cover

    async def run(self, schedule: Schedule, run_id: UUID) -> RunResult:
        connection = self._registry.validate(schedule)
        path, body, count_key = _JOB_KINDS[connection.connector_type]
        # The outer deadline includes submission, all retries and status polling.
        async with asyncio.timeout(schedule.timeout_seconds):
            job = await self._request(
                "POST",
                path,
                schedule,
                headers={"Idempotency-Key": str(run_id)},
                json=body(connection),
            )
            job_id = UUID(job["job_id"])
            while True:
                job = await self._request("GET", f"/jobs/{job_id}", schedule)
                if job["status"] == "succeeded":
                    count = job["counts"].get(count_key, 0)
                    if type(count) is not int or count < 0:
                        raise ValueError("Invalid job result")
                    return RunResult(count, "Connection job completed", str(job_id))
                if job["status"] == "failed":
                    raise ConnectionJobFailedError(str(job_id))
                if job["status"] not in {"queued", "running"}:
                    raise ValueError("Invalid job status")
                await asyncio.sleep(self._poll_seconds)


class ConnectorRunner:
    def __init__(self, connectors: dict[str, Connector]):
        self._connectors = dict(connectors)

    async def run(self, schedule: Schedule, run_id: UUID) -> RunResult:
        connector = self._connectors.get(schedule.connector_type)
        if connector is None:
            raise ValueError("Unregistered connector")
        return await connector.run(schedule, run_id)
