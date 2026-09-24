"""Tests for schedule configuration and manual execution through HTTP."""

from datetime import UTC, datetime
from threading import Event
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient

from nickel_connection.scheduler.api import build_app, create_app
from nickel_connection.scheduler.models import Schedule
from nickel_connection.scheduler.runner import ConnectorRunner, RunResult
from nickel_connection.scheduler.store import ScheduleStore

TENANT_ID = UUID("00000000-0000-4000-8000-000000000001")


@pytest.fixture
def settings():
    return {
        "name": "Demo source",
        "connector_type": "demo",
        "connection_id": "demo-connection",
        "interval_seconds": 300,
        "enabled": True,
    }


@pytest.fixture
def client(tmp_path):
    release = Event()

    class ControlledConnector:
        def run(self, context):
            release.wait(timeout=5)
            return RunResult(items_processed=0, message="Simulation completed")

    app = build_app(
        tmp_path / "scheduler.db",
        TENANT_ID,
        runner=ConnectorRunner({"demo": ControlledConnector()}),
    )

    with TestClient(app) as test_client:
        try:
            yield test_client
        finally:
            release.set()


def test_health(client):
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "mode": "demo"}


def test_create_and_list_schedule(client, settings):
    response = client.post("/schedules", json=settings)

    assert response.status_code == 201
    created = response.json()
    assert created["tenant_id"] == str(TENANT_ID)
    assert created["name"] == settings["name"]
    assert client.get("/schedules").json() == [created]


def test_client_cannot_supply_tenant(client, settings):
    settings["tenant_id"] = str(uuid4())

    assert client.post("/schedules", json=settings).status_code == 422


def test_invalid_interval_is_rejected(client, settings):
    settings["interval_seconds"] = 0

    assert client.post("/schedules", json=settings).status_code == 422


def test_unknown_connector_is_rejected(client, settings):
    settings["connector_type"] = "not-installed"

    assert client.post("/schedules", json=settings).status_code == 422


def test_pause_resume_and_change_frequency(client, settings):
    created = client.post("/schedules", json=settings).json()
    url = f"/schedules/{created['id']}"

    settings["enabled"] = False
    paused = client.put(url, json=settings)
    assert paused.status_code == 200
    assert paused.json()["enabled"] is False
    assert paused.json()["next_run_at"] == created["next_run_at"]

    settings["enabled"] = True
    resumed = client.put(url, json=settings)
    assert resumed.status_code == 200
    assert resumed.json()["enabled"] is True

    settings["interval_seconds"] = 60
    updated = client.put(url, json=settings)
    assert updated.status_code == 200
    assert updated.json()["interval_seconds"] == 60
    assert updated.json()["id"] == created["id"]


def test_connection_cannot_be_reassigned(client, settings):
    created = client.post("/schedules", json=settings).json()
    settings["connection_id"] = "another-connection"

    response = client.put(f"/schedules/{created['id']}", json=settings)

    assert response.status_code == 409


def test_missing_schedule_returns_not_found(client, settings):
    schedule_id = uuid4()

    assert client.put(f"/schedules/{schedule_id}", json=settings).status_code == 404
    assert client.post(f"/schedules/{schedule_id}/run").status_code == 404


def test_manual_run_is_recorded_and_cannot_overlap(client, settings):
    created = client.post("/schedules", json=settings).json()
    url = f"/schedules/{created['id']}/run"

    started = client.post(url)
    assert started.status_code == 202
    assert started.json()["status"] == "running"

    assert client.post(url).status_code == 409

    records = client.get("/runs").json()
    assert len(records) == 1
    assert records[0]["id"] == started.json()["id"]


def test_paused_schedule_cannot_run_manually(client, settings):
    settings["enabled"] = False
    created = client.post("/schedules", json=settings).json()

    response = client.post(f"/schedules/{created['id']}/run")

    assert response.status_code == 409
    assert client.get("/runs").json() == []


def test_factory_reads_environment(tmp_path, monkeypatch):
    monkeypatch.setenv("NICKEL_SCHEDULER_DB", str(tmp_path / "scheduler.db"))
    monkeypatch.setenv("NICKEL_TENANT_ID", str(TENANT_ID))

    with TestClient(create_app()) as client:
        assert client.get("/health").status_code == 200


@pytest.mark.parametrize("limit", ["0", "-1", "201", "abc", "1.5"])
def test_invalid_run_history_limit_returns_422(client, limit):
    response = client.get("/runs", params={"limit": limit})

    assert response.status_code == 422


def test_run_history_endpoint_applies_default_and_requested_limits(tmp_path):
    database = tmp_path / "history.db"
    now = datetime.now(UTC)
    schedule = Schedule(
        tenant_id=TENANT_ID,
        name="History test",
        connector_type="demo",
        connection_id="history-connection",
        interval_seconds=300,
        enabled=False,
        next_run_at=now,
    )
    store = ScheduleStore(database)
    run_ids = []

    try:
        store.add(schedule)

        for _ in range(55):
            record = store.start_run(schedule, now)
            assert record is not None
            record.status = "succeeded"
            record.finished_at = now
            store.finish_run(record)
            run_ids.append(str(record.id))
    finally:
        store.close()

    with TestClient(build_app(database, TENANT_ID)) as client:
        default_response = client.get("/runs")
        assert default_response.status_code == 200
        assert [run["id"] for run in default_response.json()] == list(
            reversed(run_ids[-50:])
        )

        limited_response = client.get("/runs", params={"limit": 2})
        assert limited_response.status_code == 200
        assert [run["id"] for run in limited_response.json()] == list(
            reversed(run_ids[-2:])
        )
