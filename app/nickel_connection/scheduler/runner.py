"""Common execution interface for scheduled connectors."""

from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from .models import Schedule


class RetryableConnectorError(RuntimeError):
    """A temporary connector failure that may succeed on retry."""


class ConnectorTimeoutError(RetryableConnectorError):
    """A connector attempt exceeded its allowed duration."""


@dataclass(frozen=True)
class RunContext:
    """Information identifying one connector execution."""

    run_id: UUID
    tenant_id: UUID
    connection_id: str
    timeout_seconds: int = 60


@dataclass(frozen=True)
class RunResult:
    """Summary returned after a successful connector execution."""

    items_processed: int
    message: str


class Connector(Protocol):
    """Interface implemented by each connector adapter."""

    def run(self, context: RunContext) -> RunResult:
        """Apply request timeouts; return on success or raise on failure."""
        ...


class ConnectorRunner:
    """Route a schedule to its registered connector."""

    def __init__(self, connectors: dict[str, Connector]):
        self._connectors = dict(connectors)

    def run(self, schedule: Schedule, run_id: UUID) -> RunResult:
        connector = self._connectors.get(schedule.connector_type)

        if connector is None:
            raise ValueError(f"Unknown connector type: {schedule.connector_type}")

        context = RunContext(
            run_id=run_id,
            tenant_id=schedule.tenant_id,
            connection_id=schedule.connection_id,
            timeout_seconds=schedule.timeout_seconds,
        )
        return connector.run(context)


class DemoConnector:
    """Simulated connector for demonstrating scheduler execution."""

    def run(self, context: RunContext) -> RunResult:
        return RunResult(
            items_processed=0,
            message="Simulation completed; no source data was fetched.",
        )
