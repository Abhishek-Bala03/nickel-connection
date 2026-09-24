"""Tests for connector routing and execution."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from nickel_connection.scheduler.models import Schedule
from nickel_connection.scheduler.runner import (
    ConnectorRunner,
    DemoConnector,
    RunContext,
    RunResult,
)


@pytest.fixture
def schedule():
    return Schedule(
        tenant_id=uuid4(),
        name="Demo source",
        connector_type="demo",
        connection_id="demo-connection",
        interval_seconds=30,
        next_run_at=datetime(2026, 9, 23, 12, 0, tzinfo=UTC),
    )


class RecordingConnector:
    def __init__(self):
        self.contexts = []

    def run(self, context: RunContext) -> RunResult:
        self.contexts.append(context)
        return RunResult(items_processed=3, message="Completed")


class FailingConnector:
    def run(self, context: RunContext) -> RunResult:
        raise ConnectionError("Source unavailable")


def test_pass_execution_context_to_selected_connector(schedule):
    selected = RecordingConnector()
    other = RecordingConnector()
    runner = ConnectorRunner({"demo": selected, "other": other})
    run_id = uuid4()

    result = runner.run(schedule, run_id)

    assert selected.contexts == [
        RunContext(
            run_id=run_id,
            tenant_id=schedule.tenant_id,
            connection_id=schedule.connection_id,
        )
    ]
    assert other.contexts == []
    assert result == RunResult(items_processed=3, message="Completed")


def test_unknown_connector_is_rejected(schedule):
    runner = ConnectorRunner({})

    with pytest.raises(ValueError, match="Unknown connector type"):
        runner.run(schedule, uuid4())


def test_connector_failure_reaches_caller(schedule):
    runner = ConnectorRunner({"demo": FailingConnector()})

    with pytest.raises(ConnectionError, match="Source unavailable"):
        runner.run(schedule, uuid4())


def test_demo_is_clearly_identified_as_simulation(schedule):
    runner = ConnectorRunner({"demo": DemoConnector()})

    result = runner.run(schedule, uuid4())

    assert result.items_processed == 0
    assert "Simulation" in result.message
    assert "no source data was fetched" in result.message


def test_pass_configured_timeout_to_connector(schedule):
    schedule.timeout_seconds = 15
    connector = RecordingConnector()
    runner = ConnectorRunner({"demo": connector})

    runner.run(schedule, uuid4())

    assert connector.contexts[0].timeout_seconds == 15
