"""The scheduler's Nango connector type: connections from the connection service, the nango.pull job,
the run's job id, and schema v2 (docs/demo-nango-wearables.md)."""

import json
import sqlite3
from contextlib import closing
from datetime import UTC, datetime
from uuid import uuid4

import httpx
import pytest
from fastapi.testclient import TestClient

from src.scheduler import api
from src.scheduler.api import build_app, refresh_nango_connections
from src.scheduler.models import Schedule
from src.scheduler.registry import Connection, ConnectorRegistry
from src.scheduler.runner import ConnectionJobFailedError, JobsConnector, RunResult
from src.scheduler.store import ScheduleStore

TOKEN = "scheduler-test-token-0123456789abcdef"
HEADERS = {"Authorization": f"Bearer {TOKEN}", "X-Authenticated-User": "admin"}
CONNECTOR_ID = "0192a3b4-0000-7000-8000-000000000c01"
CONNECTORS = [
    {"id": CONNECTOR_ID, "name": "Oura", "source": "nango", "enabled": True},
    {
        "id": "0192a3b4-0000-7000-8000-000000000c02",
        "name": "Off",
        "source": "nango",
        "enabled": False,
    },
]
NANGO = dict(
    name="Oura every minute",
    connector_type="nango",
    connection_id=CONNECTOR_ID,
    interval_seconds=60,
)


@pytest.fixture
def anyio_backend():
    return "asyncio"


def test_the_registry_takes_nango_connections_from_the_service_only():
    with pytest.raises(ValueError, match="connection service"):
        ConnectorRegistry([Connection(id="x", name="x", connector_type="nango")])
    registry = ConnectorRegistry([])
    assert {c.id: c.minimum_interval_seconds for c in registry.connectors}[
        "nango"
    ] == 60
    registry.replace_remote(
        [
            Connection(id="n", name="n", connector_type="nango"),
            Connection(id="d", name="d", connector_type="demo"),
        ]
    )
    assert [c.id for c in registry.connections] == ["n"]
    with pytest.raises(ValueError, match="Minimum interval"):
        registry.validate(
            Schedule(
                tenant_id=uuid4(),
                **{**NANGO, "connection_id": "n", "interval_seconds": 30},
            )
        )


@pytest.mark.anyio
async def test_refresh_keeps_enabled_nango_connectors():
    registry = ConnectorRegistry([])
    async with httpx.AsyncClient(
        base_url="http://c.test",
        transport=httpx.MockTransport(lambda r: httpx.Response(200, json=CONNECTORS)),
    ) as client:
        assert await refresh_nango_connections(client, registry)
    assert [(c.id, c.connector_type) for c in registry.connections] == [
        (CONNECTOR_ID, "nango")
    ]
    async with httpx.AsyncClient(
        base_url="http://c.test",
        transport=httpx.MockTransport(lambda r: httpx.Response(503)),
    ) as client:
        assert not await refresh_nango_connections(client, registry)
    assert len(registry.connections) == 1  # the last good list stays


@pytest.mark.anyio
async def test_a_nango_run_queues_the_connector_and_reports_records():
    registry = ConnectorRegistry([])
    registry.replace_remote(
        [Connection(id=CONNECTOR_ID, name="Oura", connector_type="nango")]
    )
    schedule = Schedule(tenant_id=uuid4(), **NANGO)
    job_id = uuid4()
    seen = []

    def upstream(request):
        seen.append(request)
        if request.method == "POST":
            assert request.url.path == "/jobs/nango/pull"
            assert json.loads(request.content) == {"connector_id": CONNECTOR_ID}
            return httpx.Response(202, json={"job_id": str(job_id)})
        return httpx.Response(
            200, json={"status": "succeeded", "counts": {"records": 94, "published": 0}}
        )

    async with httpx.AsyncClient(
        base_url="http://c.test", transport=httpx.MockTransport(upstream)
    ) as client:
        result = await JobsConnector(client, registry).run(schedule, uuid4())
    assert result == RunResult(94, "Connection job completed", str(job_id))

    async with httpx.AsyncClient(
        base_url="http://c.test",
        transport=httpx.MockTransport(
            lambda r: (
                httpx.Response(202, json={"job_id": str(job_id)})
                if r.method == "POST"
                else httpx.Response(200, json={"status": "failed"})
            )
        ),
    ) as client:
        with pytest.raises(ConnectionJobFailedError) as failed:
            await JobsConnector(client, registry).run(schedule, uuid4())
    assert failed.value.job_id == str(job_id)


class _Runner:
    def __init__(self, fail):
        self.fail = fail

    async def run(self, schedule, run_id):
        if self.fail:
            raise ConnectionJobFailedError("job-failed")
        return RunResult(7, "ok", "job-ok")


def _app(tmp_path, monkeypatch, upstream, runner=None):
    real = httpx.AsyncClient
    monkeypatch.setattr(
        api.httpx,
        "AsyncClient",
        lambda **kwargs: real(**kwargs, transport=httpx.MockTransport(upstream)),
    )
    return build_app(
        tmp_path / "schedule.db",
        uuid4(),
        service_token=TOKEN,
        registry=ConnectorRegistry([]),
        runner=runner,
        upstream="http://connection.test",
    )


def _wait_for_runs(client, count):
    for _ in range(200):
        runs = client.get("/runs").json()
        if len(runs) >= count and all(r["status"] != "running" for r in runs):
            return runs
    raise AssertionError(runs)


@pytest.mark.parametrize("fail", [False, True])
def test_a_run_keeps_the_connection_job_id(tmp_path, monkeypatch, fail):
    app = _app(
        tmp_path,
        monkeypatch,
        lambda r: httpx.Response(200, json=CONNECTORS),
        _Runner(fail),
    )
    with TestClient(app, headers=HEADERS) as client:
        assert client.get("/connections").json() == [
            {"id": CONNECTOR_ID, "name": "Oura", "connector_type": "nango"}
        ]
        schedule = client.post("/schedules", json=NANGO)
        assert schedule.status_code == 201, schedule.text
        assert client.post(f"/schedules/{schedule.json()['id']}/run").status_code == 202
        (run,) = _wait_for_runs(client, 1)
    assert run["job_id"] == ("job-failed" if fail else "job-ok")
    assert run["status"] == ("failed" if fail else "succeeded")


def test_nango_schedules_need_the_connection_service(tmp_path, monkeypatch):
    app = _app(tmp_path, monkeypatch, lambda r: httpx.Response(503))
    with TestClient(app, headers=HEADERS) as client:
        assert client.get("/connections").json() == []
        assert client.post("/schedules", json=NANGO).status_code == 503


def test_startup_tolerates_a_missing_connection_service_for_nango_schedules(
    tmp_path, monkeypatch
):
    up = {"ok": True}
    app = _app(
        tmp_path,
        monkeypatch,
        lambda r: (
            httpx.Response(200, json=CONNECTORS) if up["ok"] else httpx.Response(503)
        ),
    )
    with TestClient(app, headers=HEADERS) as client:
        assert client.post("/schedules", json=NANGO).status_code == 201
    up["ok"] = False
    with TestClient(app, headers=HEADERS) as client:  # starts, and keeps the schedule
        assert len(client.get("/schedules").json()) == 1


def test_a_v1_database_gains_the_job_id_column(tmp_path):
    path = tmp_path / "schedule.db"
    ScheduleStore(path).close()
    with closing(sqlite3.connect(path)) as db, db:
        db.execute("ALTER TABLE scheduler_runs DROP COLUMN job_id")
        db.execute("UPDATE scheduler_schema SET version = 1")
    store = ScheduleStore(path)
    try:
        schedule = Schedule(tenant_id=uuid4(), **NANGO)
        store.add(schedule)
        record = store.start_run(schedule, datetime.now(UTC))
        record.status, record.finished_at, record.job_id = (
            "succeeded",
            datetime.now(UTC),
            "j-1",
        )
        store.finish_run(record)
        assert store.list_runs(schedule.tenant_id)[0].job_id == "j-1"
    finally:
        store.close()
    with closing(sqlite3.connect(path)) as db:
        assert db.execute("SELECT version FROM scheduler_schema").fetchone() == (2,)
