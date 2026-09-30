"""Review regressions: tenant/provider/patient binding is enforced server-side."""

import json

import httpx
import pytest
from pydantic import ValidationError

from src.connectors.nango import NangoClient
from src.db.plugins import PluginStore
from src.error import BadRequestError, UpstreamError
from src.handler import nango as handler
from src.models.connectors import ConnectorCreate, ConnectorUpdate
from src.models.plugins import PluginCreate, PluginResource


def remote(monkeypatch, responder):
    monkeypatch.setattr(
        handler,
        "client",
        lambda settings: NangoClient(
            "https://nango.test", "secret", transport=httpx.MockTransport(responder)
        ),
    )


def connection(
    tenant, patient="patient-1", integration="nickel-sim-oura", id="authorized"
):
    return {
        "provider_config_key": integration,
        "connection_id": id,
        "tags": {"end_user_id": patient, "organization_id": str(tenant)},
    }


def test_scoped_discovery_session_and_derived_binding(service, settings, monkeypatch):
    calls = []

    def respond(req):
        calls.append(req)
        if req.url.path == "/integrations":
            return httpx.Response(
                200,
                json={
                    "data": [
                        {"unique_key": i} for i in ["nickel-sim-oura", "another-tenant"]
                    ]
                },
            )
        if req.url.path == "/connections":
            assert req.url.params["endUserId"] == "patient-1"
            assert req.url.params["endUserOrganizationId"] == str(settings.tenant_id)
            return httpx.Response(
                200,
                json={
                    "connections": [
                        connection(settings.tenant_id),
                        connection(settings.tenant_id, patient="wrong"),
                        connection("wrong-tenant"),
                        connection(settings.tenant_id, integration="wrong-integration"),
                    ]
                },
            )
        assert req.url.path == "/connect/sessions"
        assert json.loads(req.content) == {
            "allowed_integrations": ["nickel-sim-oura"],
            "end_user": {"id": "patient-1"},
            "organization": {"id": str(settings.tenant_id)},
        }
        return httpx.Response(
            201, json={"data": {"connect_link": "https://connect.test/session"}}
        )

    remote(monkeypatch, respond)
    body = {
        "name": "Ring",
        "provider": "oura",
        "patient_id": "patient-1",
        "streams": ["sleep"],
    }
    with service() as api:
        assert [i["id"] for i in api.get("/nango/integrations").json()] == [
            "nickel-sim-oura"
        ]
        assert (
            api.get("/connectors/catalog").json()[0]["nango_integration"]
            == "nickel-sim-oura"
        )
        found = api.get(
            "/nango/connections",
            params={"integration": "nickel-sim-oura", "patient_id": "patient-1"},
        )
        assert found.json()["connections"] == [
            {"id": "authorized", "integration": "nickel-sim-oura"}
        ]
        assert (
            api.post(
                "/nango/connect-session",
                json={"integration": "nickel-sim-oura", "patient_id": "patient-1"},
            ).status_code
            == 201
        )
        for bad in [
            {"nango_integration": "other"},
            {"nango_connection_id": "typed-wrong-id"},
        ]:
            assert api.post("/connectors", json={**body, **bad}).status_code == 400
        created = api.post("/connectors", json=body)
        assert created.status_code == 201, created.text
        assert created.json()["nango_connection_id"] == "authorized"
        count = len(calls)
        assert (
            api.post(
                "/nango/connect-session",
                json={"integration": "another-tenant", "patient_id": "patient-1"},
            ).status_code
            == 400
        )
        assert (
            api.get(
                "/nango/connections",
                params={"integration": "another-tenant", "patient_id": "patient-1"},
            ).status_code
            == 400
        )
        assert len(calls) == count
        assert (
            api.put(
                f"/connectors/{created.json()['id']}",
                json={
                    "name": "Ring",
                    "provider": "dexcom",
                    "streams": ["egvs"],
                    "version": 1,
                },
            ).status_code
            == 422
        )


@pytest.mark.parametrize(
    "items",
    [
        [],
        [{}],
        [{"provider_config_key": "nickel-sim-oura", "connection_id": "unbound"}],
    ],
)
def test_unbound_connections_are_not_accepted(items):
    client = NangoClient(
        "https://n.test",
        "s",
        transport=httpx.MockTransport(
            lambda _: httpx.Response(200, json={"connections": items})
        ),
    )
    assert client.patient_connection("nickel-sim-oura", "p", "t") is None
    client.close()


def test_multiple_accounts_fail_closed():
    client = NangoClient(
        "https://n.test",
        "s",
        transport=httpx.MockTransport(
            lambda _: httpx.Response(
                200,
                json={
                    "connections": [connection("t", id="a"), connection("t", id="b")]
                },
            )
        ),
    )
    with pytest.raises(BadRequestError, match="Multiple"):
        client.patient_connection("nickel-sim-oura", "patient-1", "t")
    client.close()


@pytest.mark.parametrize(
    "link",
    [
        "http://connect.test",
        "javascript:alert(1)",
        "https://user:pass@connect.test",
        None,
    ],
)
def test_connect_link_requires_https(link):
    def respond(req):
        return httpx.Response(
            200,
            json=(
                {"data": [{"unique_key": "i"}]}
                if req.url.path == "/integrations"
                else {"data": {"connect_link": link}}
            ),
        )

    client = NangoClient("https://n.test", "s", transport=httpx.MockTransport(respond))
    with pytest.raises(UpstreamError):
        client.connect_session("i", "p", "t")
    if link == "http://connect.test":
        assert (
            client.connect_session("i", "p", "t", allow_http=True)["connect_link"]
            == link
        )
    client.close()


def test_models_refuse_missing_patient_provider_edits_and_url_overrides():
    with pytest.raises(ValidationError):
        ConnectorCreate(name="Ring", provider="oura", streams=["sleep"])
    with pytest.raises(ValidationError):
        ConnectorUpdate(name="Ring", streams=["sleep"], version=1, provider="dexcom")
    for url in ["https://127.0.0.1.nip.io", "https://vendor.test"]:
        with pytest.raises(ValidationError):
            PluginCreate(
                id="clinic",
                name="Clinic",
                nango_provider="epic",
                resources=[{"name": "r", "path": "/Patient"}],
                base_url=url,
            )
    with pytest.raises(ValidationError):
        PluginResource(
            name="r", path="/Patient", method="POST", body={"resourceType": "Patient"}
        )
    assert PluginResource(name="r", path="/Patient/_search", method="POST")


def test_preexisting_runtime_plugin_is_not_executable(service, settings, monkeypatch):
    PluginStore(settings.data_dir / "connectors.sqlite").add(
        str(settings.tenant_id),
        {
            "id": "clinic",
            "name": "Clinic",
            "status": "ready",
            "nango_integration": "old",
            "resources": [{"name": "r", "path": "/Patient"}],
        },
    )
    monkeypatch.setattr(
        handler, "client", lambda _: pytest.fail("must not reach Nango")
    )
    with service() as api:
        assert all(p["id"] != "clinic" for p in api.get("/connectors/catalog").json())
        assert (
            api.post(
                "/connectors",
                json={
                    "name": "Clinic",
                    "provider": "clinic",
                    "patient_id": "p",
                    "streams": ["r"],
                },
            ).status_code
            == 400
        )
        assert (
            api.post(
                "/plugins/clinic/provision",
                json={"client_id": "id", "client_secret": "secret"},
            ).status_code
            == 400
        )
