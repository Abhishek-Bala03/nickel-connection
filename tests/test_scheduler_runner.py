import json
from uuid import uuid4

import httpx
import pytest

from src.scheduler.models import Schedule
from src.scheduler.registry import Connection, ConnectorRegistry
from src.scheduler.runner import (
    ConnectorRunner,
    DemoConnector,
    JobsConnector,
    RetryableConnectorError,
)


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
def schedule():
    return Schedule(
        tenant_id=uuid4(),
        name="Documents",
        connector_type="documents",
        connection_id="docs",
        interval_seconds=300,
    )


def registry():
    return ConnectorRegistry(
        [
            Connection(
                id="docs",
                name="Documents",
                connector_type="documents",
                manifest="test-docs",
            )
        ]
    )


@pytest.mark.anyio
async def test_demo_and_unknown(schedule):
    runner = ConnectorRunner({"demo": DemoConnector()})
    with pytest.raises(ValueError):
        await runner.run(schedule, uuid4())
    schedule.connector_type = "demo"
    assert (await runner.run(schedule, uuid4())).items_processed == 0


@pytest.mark.anyio
async def test_jobs_poll_and_retry_same_key(schedule, monkeypatch):
    schedule.max_retries = 1
    calls = []
    job_id = uuid4()

    async def no_wait(seconds):
        return None

    monkeypatch.setattr("src.scheduler.runner.asyncio.sleep", no_wait)

    def handler(request):
        calls.append(request)
        if len(calls) == 1:
            return httpx.Response(503)
        if request.method == "POST":
            assert json.loads(request.content) == {"manifest": "test-docs"}
            return httpx.Response(202, json={"job_id": str(job_id)})
        return httpx.Response(
            200,
            json={
                "status": "running" if len(calls) == 3 else "succeeded",
                "counts": {"published": 3},
            },
        )

    run_id = uuid4()
    async with httpx.AsyncClient(
        base_url="https://connection.test", transport=httpx.MockTransport(handler)
    ) as client:
        result = await JobsConnector(client, registry()).run(schedule, run_id)
    assert result.items_processed == 3
    assert (
        calls[0].headers["Idempotency-Key"]
        == calls[1].headers["Idempotency-Key"]
        == str(run_id)
    )
    assert calls[-1].url.path == f"/jobs/{job_id}"


@pytest.mark.anyio
@pytest.mark.parametrize(
    "result",
    [
        {"status": "failed"},
        {"status": "unknown"},
        {"status": "succeeded", "counts": {"published": -1}},
        {"status": "succeeded", "counts": {"published": True}},
    ],
)
async def test_invalid_and_failed_jobs(schedule, result):
    def handler(request):
        return httpx.Response(
            200, json={"job_id": str(uuid4())} if request.method == "POST" else result
        )

    async with httpx.AsyncClient(
        base_url="https://connection.test", transport=httpx.MockTransport(handler)
    ) as client:
        with pytest.raises((ValueError, RuntimeError)):
            await JobsConnector(client, registry()).run(schedule, uuid4())


@pytest.mark.anyio
async def test_transport_exhaustion_and_nonretryable(schedule):
    def failed(request):
        raise httpx.ConnectError("private details", request=request)

    async with httpx.AsyncClient(
        base_url="https://connection.test", transport=httpx.MockTransport(failed)
    ) as client:
        with pytest.raises(RetryableConnectorError, match="unavailable"):
            await JobsConnector(client, registry()).run(schedule, uuid4())
    async with httpx.AsyncClient(
        base_url="https://connection.test",
        transport=httpx.MockTransport(lambda r: httpx.Response(401)),
    ) as client:
        with pytest.raises(httpx.HTTPStatusError):
            await JobsConnector(client, registry()).run(schedule, uuid4())


@pytest.mark.anyio
async def test_deadline_includes_request(schedule):
    import asyncio

    schedule.timeout_seconds = 1

    async def hung(request):
        await asyncio.Event().wait()

    async with httpx.AsyncClient(
        base_url="https://connection.test", transport=httpx.MockTransport(hung)
    ) as client:
        with pytest.raises(TimeoutError):
            await JobsConnector(client, registry()).run(schedule, uuid4())


def test_registry_rejects_invalid_connections(schedule):
    with pytest.raises(ValueError, match="Duplicate"):
        ConnectorRegistry(registry().connections * 2)
    with pytest.raises(ValueError, match="Unregistered"):
        ConnectorRegistry([Connection(id="x", name="x", connector_type="unknown")])
    with pytest.raises(ValueError, match="manifest"):
        ConnectorRegistry([Connection(id="x", name="x", connector_type="documents")])
    schedule.connection_id = "missing"
    with pytest.raises(ValueError, match="available"):
        registry().validate(schedule)
    schedule.connection_id = "docs"
    schedule.interval_seconds = 1
    with pytest.raises(ValueError, match="Minimum"):
        registry().validate(schedule)
    cron = schedule.model_copy(
        update={
            "trigger_type": "cron",
            "interval_seconds": None,
            "cron_expression": "* * * * *",
        }
    )
    with pytest.raises(ValueError, match="daily"):
        registry().validate(cron)
    cron.cron_expression = "30 8 * * *"
    assert registry().validate(cron).id == "docs"
