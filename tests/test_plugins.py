"""Plugin drafts persist per tenant and never provision Nango connections."""

import httpx
import pytest

from src.connectors.nango import NangoClient
from src.handler import nango


@pytest.fixture
def providers(monkeypatch):
    calls = []

    def respond(request):
        calls.append((request.method, request.url.path))
        assert request.method == "GET"
        assert request.url.path == "/providers"
        return httpx.Response(
            200,
            json={
                "data": [
                    {
                        "name": "epic",
                        "display_name": "Epic (FHIR)",
                        "auth_mode": "TWO_STEP",
                        "credentials": {"secret": "private"},
                        "proxy": {"base_url": "https://${connectionConfig.host}/api"},
                    }
                ]
            },
        )

    monkeypatch.setattr(
        nango,
        "client",
        lambda settings: NangoClient(
            "http://nango.test",
            "test-secret",
            transport=httpx.MockTransport(respond),
        ),
    )
    return calls


def draft(**changes):
    return {
        "id": "epic-fhir-draft",
        "name": "Epic draft",
        "nango_provider": "epic",
        "resources": [{"name": "patients", "method": "GET", "path": "/Patient"}],
        **changes,
    }


def test_plugin_writes_are_held_without_remote_calls(service, providers):
    with service() as api:
        assert api.post("/plugins", json=draft()).status_code == 400
        assert (
            api.post("/plugins/epic-fhir-draft/provision", json={}).status_code == 400
        )
        assert api.get("/plugins").json() == []
    assert providers == []


@pytest.mark.parametrize(
    "path",
    [
        "https://example.com/Patient",
        "//example.com/Patient",
        "/../Patient",
        "/%2e%2e/Patient",
        "/Patient?token=secret",
        "/Patient#fragment",
        "/a\\b",
    ],
)
def test_invalid_resource_paths_are_rejected(service, providers, path):
    with service() as api:
        body = draft(resources=[{"name": "patients", "path": path}])
        assert api.post("/plugins", json=body).status_code == 422
        assert api.get("/plugins").json() == []
    assert providers == []


def test_credentials_and_duplicate_streams_are_rejected(service, providers):
    with service() as api:
        assert (
            api.post("/plugins", json=draft(client_secret="private")).status_code == 422
        )
        body = draft()
        body["resources"] *= 2
        assert api.post("/plugins", json=body).status_code == 422
    assert providers == []


def test_routes_require_authentication(service, providers):
    with service() as api:
        headers = {"Authorization": "Bearer wrong"}
        for path in ("/plugins", "/nango/providers"):
            assert api.get(path, headers=headers).status_code == 401
        assert api.post("/plugins", json=draft(), headers=headers).status_code == 401
    assert providers == []
