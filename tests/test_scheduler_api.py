import json
from unittest.mock import patch
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from src.scheduler.api import build_app, create_app
from src.scheduler.registry import Connection, ConnectorRegistry

TOKEN = "scheduler-test-token-0123456789abcdef"
HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "X-Authenticated-User": "keycloak-subject-123",
}
SETTINGS = dict(
    name="Demo",
    connector_type="demo",
    connection_id="demo-connection",
    interval_seconds=30,
)


def make(tmp_path, **kwargs):
    return build_app(
        tmp_path / "schedule.db",
        uuid4(),
        service_token=TOKEN,
        registry=ConnectorRegistry(
            [Connection(id="demo-connection", name="Demo", connector_type="demo")],
            demo=True,
        ),
        development=True,
        **kwargs,
    )


@pytest.fixture
def client(tmp_path):
    with TestClient(make(tmp_path), headers=HEADERS) as client:
        yield client


def test_authentication_before_routing(client):
    for path in ["/schedules", "/connections", "/connectors", "/runs", "/missing"]:
        assert client.get(path, headers={"Authorization": ""}).status_code == 401
    assert (
        client.post(
            "/schedules", content="broken", headers={"Authorization": ""}
        ).status_code
        == 401
    )
    assert client.get("/health", headers={"Authorization": ""}).json() == {
        "status": "ok",
        "mode": "demo",
    }
    assert client.get("/docs", headers={"Authorization": ""}).status_code == 200


def test_registry_and_create_audit(client):
    assert client.get("/connectors").json()[-1]["minimum_interval_seconds"] == 30
    assert client.get("/connections").json() == [
        {"id": "demo-connection", "name": "Demo", "connector_type": "demo"}
    ]
    assert client.get("/schedules").json() == []
    response = client.post("/schedules", json=SETTINGS)
    assert response.status_code == 201
    schedule = response.json()
    assert schedule["changed_by"] == "keycloak-subject-123"
    assert schedule["version"] == 1 and schedule["next_run_at"] is not None
    assert client.get("/schedules").json()[0]["id"] == schedule["id"]


def test_update_versions_and_pause(client):
    schedule = client.post("/schedules", json=SETTINGS).json()
    path = f"/schedules/{schedule['id']}"
    payload = {**SETTINGS, "version": 1, "enabled": False}
    updated = client.put(
        path, json=payload, headers={"X-Authenticated-User": "second-user"}
    ).json()
    assert updated["version"] == 2 and updated["next_run_at"] is None
    assert updated["changed_by"] == "second-user"
    assert updated["created_at"] == schedule["created_at"]
    assert client.put(path, json=payload).status_code == 409
    assert client.put(path, json=SETTINGS).status_code == 422
    assert client.post(path + "/run").status_code == 409
    assert client.put(f"/schedules/{uuid4()}", json=payload).status_code == 404
    payload.update(version=2, enabled=True)
    assert client.put(path, json=payload).json()["next_run_at"] is not None


def test_manual_run_and_history_filters(client):
    schedule = client.post("/schedules", json=SETTINGS).json()
    response = client.post(f"/schedules/{schedule['id']}/run")
    assert response.status_code == 202
    runs = client.get(
        "/runs", params={"schedule_id": schedule["id"], "limit": 1}
    ).json()
    assert len(runs) == 1
    assert client.get("/runs", params={"schedule_id": str(uuid4())}).json() == []
    assert client.get("/runs?limit=201").status_code == 422
    assert client.get("/runs?limit=0").status_code == 422
    assert client.post(f"/schedules/{uuid4()}/run").status_code == 404


@pytest.mark.parametrize(
    "overrides",
    [
        {"interval_seconds": 1},
        {"connection_id": "typo"},
        {"connector_type": "unknown"},
        {
            "trigger_type": "cron",
            "interval_seconds": None,
            "cron_expression": "* * * * *",
        },
    ],
)
def test_rejects_invalid_settings(client, overrides):
    assert client.post("/schedules", json={**SETTINGS, **overrides}).status_code == 422


def test_daily_cron(client):
    result = client.post(
        "/schedules",
        json={
            **SETTINGS,
            "trigger_type": "cron",
            "interval_seconds": None,
            "cron_expression": "30 8 * * *",
        },
    )
    assert result.status_code == 201
    assert result.json()["next_run_at"] is not None


def test_missing_and_invalid_actor(client):
    assert (
        client.post(
            "/schedules", json=SETTINGS, headers={"X-Authenticated-User": ""}
        ).status_code
        == 422
    )
    assert (
        client.post(
            "/schedules", json=SETTINGS, headers={"X-Authenticated-User": "   "}
        ).status_code
        == 422
    )


def test_failed_sync_and_health(client):
    with patch.object(
        client.app.state.engine, "sync_schedule", side_effect=RuntimeError("private")
    ):
        response = client.post("/schedules", json=SETTINGS)
        assert response.status_code == 503 and "private" not in response.text
    with patch(
        "src.scheduler.engine.SchedulerEngine.healthy", new=property(lambda self: False)
    ):
        assert client.get("/health").status_code == 503
        assert client.post("/schedules", json=SETTINGS).status_code == 503


def test_production_docs_and_startup_validation(tmp_path):
    with pytest.raises(ValueError, match="Demo"):
        build_app(
            tmp_path / "s.db",
            uuid4(),
            service_token=TOKEN,
            registry=ConnectorRegistry(
                [Connection(id="demo", name="Demo", connector_type="demo")], demo=True
            ),
        )
    for upstream in [
        "ftp://example.com",
        "http://user:pass@example.com",
        "http://example.com?x=1",
        "http://example.com#x",
    ]:
        with pytest.raises(ValueError, match="URL"):
            build_app(
                tmp_path / "s.db",
                uuid4(),
                service_token=TOKEN,
                registry=ConnectorRegistry([]),
                upstream=upstream,
            )
    app = build_app(
        tmp_path / "prod.db",
        uuid4(),
        service_token=TOKEN,
        registry=ConnectorRegistry([]),
    )
    with TestClient(app, headers=HEADERS) as client:
        assert client.get("/health").json()["mode"] == "jobs"
        for path in ["/docs", "/redoc", "/openapi.json"]:
            assert client.get(path).status_code == 404


def test_factory_env_and_secret_file(tmp_path, monkeypatch):
    monkeypatch.setenv("NICKEL_SCHEDULER_DB", str(tmp_path / "factory.db"))
    monkeypatch.setenv("NICKEL_TENANT_ID", str(uuid4()))
    monkeypatch.setenv("NICKEL_SERVICE_TOKEN", TOKEN)
    with TestClient(create_app(), headers=HEADERS) as client:
        assert client.get("/connections").json() == []
    secret = tmp_path / "token"
    secret.write_text(TOKEN)
    config = tmp_path / "connections.json"
    config.write_text(json.dumps([dict(id="demo", name="Demo", connector_type="demo")]))
    monkeypatch.setenv("NICKEL_SERVICE_TOKEN_FILE", str(secret))
    monkeypatch.setenv("NICKEL_ENV", "development")
    monkeypatch.setenv("NICKEL_SCHEDULER_CONNECTIONS", str(config))
    with TestClient(create_app(), headers=HEADERS) as client:
        assert client.get("/connections").json()[0]["id"] == "demo"


def test_startup_rebuilds_existing_schedules_and_rejects_removed_connections(tmp_path):
    tenant = uuid4()
    registry = ConnectorRegistry(
        [Connection(id="demo-connection", name="Demo", connector_type="demo")],
        demo=True,
    )

    def app(connections):
        return build_app(
            tmp_path / "restart.db",
            tenant,
            service_token=TOKEN,
            registry=connections,
            development=True,
        )

    with TestClient(app(registry), headers=HEADERS) as client:
        saved = client.post("/schedules", json=SETTINGS).json()
    with TestClient(app(registry), headers=HEADERS) as client:
        schedule = client.get("/schedules").json()[0]
        assert schedule["id"] == saved["id"] and schedule["next_run_at"] is not None
    with pytest.raises(ValueError, match="available"):
        with TestClient(app(ConnectorRegistry([], demo=True))):
            pass


def test_development_document_connections_are_not_reported_as_simulated(tmp_path):
    registry = ConnectorRegistry(
        [
            Connection(
                id="docs",
                name="Documents",
                connector_type="documents",
                manifest="test-docs",
            )
        ]
    )
    app = build_app(
        tmp_path / "documents.db",
        uuid4(),
        service_token=TOKEN,
        registry=registry,
        development=True,
    )
    with TestClient(app, headers=HEADERS) as client:
        assert client.get("/health").json()["mode"] == "jobs"
