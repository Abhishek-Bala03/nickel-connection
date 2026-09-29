"""Dynamic Nango discovery and linking: secrets never leave the backend."""

import json

import httpx
import pytest

from src.connectors.nango import NangoClient
from src.error import UpstreamError
from src.handler import nango as handler


def remote(monkeypatch, responder):
    monkeypatch.setattr(
        handler,
        "client",
        lambda settings: NangoClient(
            "http://nango.test",
            "private-api-key",
            transport=httpx.MockTransport(responder),
        ),
    )


def test_discovery_filters_secrets_and_scopes_connect_session(service, monkeypatch):
    def respond(req):
        assert req.headers["Authorization"] == "Bearer private-api-key"
        if req.url.path == "/integrations":
            return httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "unique_key": "oura-mock",
                            "provider": "oura",
                            "client_secret": "private",
                        }
                    ]
                },
            )
        if req.url.path == "/connections":
            assert req.url.params["page"] == "0"
            return httpx.Response(
                200,
                json={
                    "connections": [
                        {
                            "connection_id": "ours",
                            "provider_config_key": "oura-mock",
                            "credentials": {"token": "private"},
                        },
                        {
                            "connection_id": "other",
                            "provider_config_key": "dexcom-mock",
                        },
                    ]
                },
            )
        assert req.url.path == "/connect/sessions"
        assert json.loads(req.content) == {
            "allowed_integrations": ["oura-mock"],
            "end_user": {"id": "patient-1"},
        }
        return httpx.Response(
            201,
            json={
                "data": {
                    "connect_link": "https://connect.test/?session_token=temporary",
                    "token": "temporary",
                }
            },
        )

    remote(monkeypatch, respond)
    with service() as api:
        assert api.get("/nango/integrations").json() == [
            {"id": "oura-mock", "name": "oura-mock", "provider": "oura"}
        ]
        assert api.get("/nango/connections?integration=oura-mock").json() == {
            "connections": [{"id": "ours", "integration": "oura-mock"}],
            "next_page": None,
        }
        response = api.post(
            "/nango/connect-session",
            json={"integration": "oura-mock", "end_user_id": "patient-1"},
        )
        assert response.status_code == 201
        assert response.headers["cache-control"] == "no-store"
        assert set(response.json()) == {"connect_link"}
        assert (
            api.post(
                "/nango/connect-session",
                json={"integration": "missing", "end_user_id": "patient-1"},
            ).status_code
            == 400
        )
        assert api.get("/nango/connections?integration=../bad").status_code == 422
        assert (
            api.get(
                "/nango/integrations", headers={"Authorization": "Bearer wrong"}
            ).status_code
            == 401
        )


def test_explicit_remote_integration_is_persisted_and_never_provisioned(
    service, monkeypatch
):
    calls = []

    def respond(req):
        calls.append(req)
        assert req.method == "GET"
        if req.url.path == "/connections/remote-oura":
            assert req.url.params["provider_config_key"] == "oura-mock"
            return httpx.Response(200, json={})
        assert req.headers["Provider-Config-Key"] == "oura-mock"
        assert "Base-Url-Override" not in req.headers
        return httpx.Response(200, json={"data": [1]})

    remote(monkeypatch, respond)
    with service(nango_base_url_overrides={"oura": "http://vendor-sim:8080"}) as api:
        response = api.post(
            "/connectors",
            json={
                "name": "Remote",
                "provider": "oura",
                "nango_integration": "oura-mock",
                "nango_connection_id": "remote-oura",
                "streams": ["sleep"],
            },
        )
        assert response.status_code == 201, response.text
        saved = response.json()
        assert saved["nango_integration"] == "oura-mock"
        assert api.post(f"/connectors/{saved['id']}/test").json()["ok"]
    assert len(calls) == 2


@pytest.mark.parametrize("status", [401, 403, 500])
def test_auth_errors_are_not_misreported_as_missing_connection(status):
    client = NangoClient(
        "http://nango.test",
        "private",
        transport=httpx.MockTransport(
            lambda req: httpx.Response(status, json={"secret": "do-not-return"})
        ),
    )
    with pytest.raises(UpstreamError, match=str(status)) as error:
        client.has_connection("oura-mock", "id")
    assert "do-not-return" not in str(error.value)
    client.close()


def test_connection_pagination_and_integration_filter():
    client = NangoClient(
        "http://nango.test",
        "private",
        transport=httpx.MockTransport(
            lambda req: httpx.Response(
                200,
                json={
                    "connections": [
                        {"provider_config_key": "other", "connection_id": str(i)}
                        for i in range(100)
                    ]
                },
            )
        ),
    )
    assert client.connections("oura-mock", 2) == {"connections": [], "next_page": 3}
    client.close()


@pytest.mark.parametrize(
    "payload",
    [
        {"data": {"connect_link": "javascript:alert(1)"}},
        {"data": {"token": "old-server"}},
    ],
)
def test_invalid_or_missing_connect_link_is_actionable(payload):
    def respond(req):
        return httpx.Response(
            200,
            json=(
                {"data": [{"unique_key": "oura-mock"}]}
                if req.method == "GET"
                else payload
            ),
        )

    client = NangoClient(
        "http://nango.test", "private", transport=httpx.MockTransport(respond)
    )
    with pytest.raises(UpstreamError, match="Configure Connect UI"):
        client.connect_session("oura-mock", "patient")
    client.close()


def test_provider_correction_preserves_connection_and_persists(service, monkeypatch):
    remote(monkeypatch, lambda req: httpx.Response(200, json={}))
    with service() as api:
        original = api.post(
            "/connectors",
            json={
                "name": "Oura correction",
                "provider": "dexcom",
                "nango_integration": "oura-mock",
                "nango_connection_id": "existing",
                "patient_id": "patient-1",
                "streams": ["egvs"],
            },
        ).json()
        path = f"/connectors/{original['id']}"
        body = {
            "name": original["name"],
            "description": "",
            "provider": "oura",
            "streams": ["daily_sleep", "sleep"],
            "enabled": True,
            "version": original["version"],
        }
        invalid = api.put(path, json={**body, "streams": ["egvs"]})
        assert invalid.status_code == 400
        assert api.get(path).json() == original
        response = api.put(path, json=body)
        assert response.status_code == 200, response.text
        saved = api.get(path).json()
        assert saved["provider"] == "oura"
        assert saved["streams"] == ["daily_sleep", "sleep"]
        for key in ["id", "nango_integration", "nango_connection_id", "patient_id"]:
            assert saved[key] == original[key]
        assert saved["version"] == original["version"] + 1
        assert api.put(path, json=body).status_code == 409


def test_failed_provider_correction_does_not_modify_connector(service, monkeypatch):
    remote(monkeypatch, lambda req: httpx.Response(200, json={}))
    with service() as api:
        original = api.post(
            "/connectors",
            json={
                "name": "Missing connection",
                "provider": "dexcom",
                "nango_integration": "oura-mock",
                "nango_connection_id": "existing",
                "streams": ["egvs"],
            },
        ).json()
        remote(monkeypatch, lambda req: httpx.Response(404))
        path = f"/connectors/{original['id']}"
        response = api.put(
            path,
            json={
                "name": original["name"],
                "provider": "oura",
                "streams": ["sleep"],
                "version": original["version"],
            },
        )
        assert response.status_code == 400
        assert api.get(path).json() == original
