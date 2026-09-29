"""A custom healthcare provider can be provisioned, connected, tested and pulled."""

import json
from uuid import UUID

import httpx
import pytest

from src.connectors.nango import NangoClient, count_records
from src.connectors.nango.resources import fetch_resource
from src.error import UpstreamError
from src.handler import nango
from src.models.plugins import PluginResource


@pytest.fixture
def remote(monkeypatch):
    state = {"integrations": {}, "requests": [], "fail": False, "timeout": False}

    def respond(req):
        state["requests"].append(req)
        if req.url.path == "/providers":
            return httpx.Response(
                200, json={"data": [{"name": "health-test", "auth_mode": "OAUTH2"}]}
            )
        if req.url.path == "/integrations":
            if req.method == "GET":
                return httpx.Response(
                    200, json={"data": list(state["integrations"].values())}
                )
            if state["fail"]:
                return httpx.Response(400, json={"secret": "never-return-this"})
            body = json.loads(req.content)
            state["integrations"][body["unique_key"]] = body
            if state["timeout"]:
                state["timeout"] = False
                raise httpx.ReadTimeout("private-response")
            return httpx.Response(201, json={"data": body})
        if req.url.path.startswith("/integrations/"):
            assert req.method == "PATCH"
            return httpx.Response(200, json={})
        if req.url.path == "/connect/sessions":
            return httpx.Response(
                201,
                json={
                    "data": {"connect_link": "https://connect.test/?session_token=test"}
                },
            )
        if req.url.path.startswith("/connections/"):
            return httpx.Response(200, json={})
        if req.url.path == "/proxy/Patient":
            assert req.headers["Provider-Config-Key"] in state["integrations"]
            return httpx.Response(
                200,
                json={
                    "resourceType": "Bundle",
                    "entry": [
                        {"resource": {"resourceType": "Patient", "id": "synthetic-1"}}
                    ],
                },
            )
        raise AssertionError(req.url)

    monkeypatch.setattr(
        nango,
        "client",
        lambda settings: NangoClient(
            "http://nango.test", "test-secret", transport=httpx.MockTransport(respond)
        ),
    )
    return state


def definition():
    return {
        "id": "clinic-api",
        "name": "Clinic API",
        "nango_provider": "health-test",
        "resources": [{"name": "patients", "path": "/Patient"}],
    }


def test_full_plugin_workflow(service, remote, wait_for, settings):
    with service() as api:
        assert api.post("/plugins", json=definition()).status_code == 201
        assert "clinic-api" not in [
            p["id"] for p in api.get("/connectors/catalog").json()
        ]
        config = {
            "client_id": "private-id",
            "client_secret": "private-secret",
            "scopes": "patient.read",
        }
        ready = api.post("/plugins/clinic-api/provision", json=config)
        assert ready.status_code == 200
        assert ready.headers["cache-control"] == "no-store"
        integration = ready.json()["nango_integration"]
        assert ready.json()["status"] == "ready"
        assert "private-secret" not in ready.text
        assert (
            b"private-secret"
            not in (settings.data_dir / "connectors.sqlite").read_bytes()
        )
        assert any(
            p["id"] == "clinic-api" and p["nango_integration"] == integration
            for p in api.get("/connectors/catalog").json()
        )
        assert api.post("/plugins/clinic-api/provision", json={}).status_code == 200
        assert len(remote["integrations"]) == 1
        auth = api.post(
            "/nango/connect-session",
            json={"integration": integration, "end_user_id": "synthetic"},
        )
        assert auth.status_code == 201
        account = {
            "name": "Clinic one",
            "provider": "clinic-api",
            "streams": ["patients"],
            "nango_connection_id": "synthetic-1",
            "nango_integration": integration,
        }
        assert (
            api.post(
                "/connectors", json={**account, "nango_integration": "other"}
            ).status_code
            == 400
        )
        created = api.post("/connectors", json=account)
        assert created.status_code == 201, created.text
        connector = created.json()["id"]
        assert api.post(f"/connectors/{connector}/test").json()["records"] == 1
        accepted = api.post("/jobs/nango/pull", json={"connector_id": connector})
        assert accepted.status_code == 202, accepted.text
        job = wait_for(api, accepted.json()["job_id"])
        assert job["status"] == "succeeded", job
        assert job["counts"]["records"] == 1
        pulls = api.get(f"/jobs/{job['job_id']}/pulls").json()
        assert pulls[0]["stream"] == "patients"
        second = api.post(
            "/connectors",
            json={
                **account,
                "name": "Clinic two",
                "nango_connection_id": "synthetic-2",
            },
        )
        assert second.status_code == 201
    with service(tenant_id=UUID("0192a3b4-0000-7000-8000-000000000002")) as api:
        assert api.post("/plugins/clinic-api/provision", json={}).status_code == 404


def test_failed_setup_and_timeout_can_be_retried(service, remote):
    with service() as api:
        api.post("/plugins", json=definition())
        remote["fail"] = True
        failure = api.post("/plugins/clinic-api/provision", json={})
        assert failure.status_code == 502
        assert "never-return-this" not in failure.text
        assert api.get("/plugins").json()[0]["status"] == "draft"
        remote.update(fail=False, timeout=True)
        assert api.post("/plugins/clinic-api/provision", json={}).status_code == 502
        assert api.post("/plugins/clinic-api/provision", json={}).status_code == 200
        creates = [
            r
            for r in remote["requests"]
            if r.method == "POST" and r.url.path == "/integrations"
        ]
        assert len(creates) == 2  # failed request + timed-out create; retry only reads


def test_fhir_pagination_preserves_pages_and_never_follows_remote_host():
    calls = []

    def respond(req):
        calls.append(req)
        assert req.url.host == "nango.test"
        assert req.url.path == "/proxy/Patient"
        return httpx.Response(
            200,
            json={
                "entry": [{"resource": {"id": str(len(calls))}}],
                "link": (
                    [
                        {
                            "relation": "next",
                            "url": "https://other.test/base/Patient?page=2",
                        }
                    ]
                    if len(calls) == 1
                    else []
                ),
            },
        )

    remote = NangoClient(
        "http://nango.test", "secret", transport=httpx.MockTransport(respond)
    )
    resource = PluginResource(
        name="patients", path="/Patient", pagination={"mode": "fhir"}
    )
    try:
        result = fetch_resource(remote, "i", "c", resource, max_bytes=10000)
        assert len(json.loads(result)["pages"]) == 2
        assert count_records(result) == 2
        assert calls[1].url.params["page"] == "2"
    finally:
        remote.close()


@pytest.mark.parametrize("mode", ["cursor", "offset"])
def test_other_pagination_modes(mode):
    calls = []

    def respond(req):
        calls.append(req)
        if len(calls) == 1:
            return httpx.Response(200, json={"data": [1, 2], "next_cursor": "two"})
        key, value = ("cursor", "two") if mode == "cursor" else ("offset", "2")
        assert req.url.params[key] == value
        return httpx.Response(200, json={"data": [3]})

    remote = NangoClient(
        "http://nango.test", "secret", transport=httpx.MockTransport(respond)
    )
    try:
        resource = PluginResource(
            name="r", path="/items", pagination={"mode": mode, "page_size": 2}
        )
        assert (
            count_records(fetch_resource(remote, "i", "c", resource, max_bytes=10000))
            == 3
        )
    finally:
        remote.close()


def test_post_request_and_resource_failure_limits():
    def respond(req):
        assert req.method == "POST"
        assert json.loads(req.content) == {"active": True}
        assert req.url.params["type"] == "patient"
        return httpx.Response(200, json=[1, 2])

    remote = NangoClient(
        "http://nango.test", "secret", transport=httpx.MockTransport(respond)
    )
    resource = PluginResource(
        name="r",
        method="POST",
        path="/search",
        query={"type": "patient"},
        body={"active": True},
    )
    try:
        assert (
            count_records(fetch_resource(remote, "i", "c", resource, max_bytes=1000))
            == 2
        )
        with pytest.raises(UpstreamError, match="size limit"):
            fetch_resource(remote, "i", "c", resource, max_bytes=1)
    finally:
        remote.close()


@pytest.mark.parametrize(
    "body,pattern",
    [
        ({"next_cursor": "repeat"}, "repeated"),
        ({"next_cursor": []}, "Invalid"),
    ],
)
def test_bad_pagination_fails_instead_of_silently_truncating(body, pattern):
    remote = NangoClient(
        "http://nango.test",
        "secret",
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json=body)),
    )
    resource = PluginResource(name="r", path="/items", pagination={"mode": "cursor"})
    try:
        with pytest.raises(UpstreamError, match=pattern):
            fetch_resource(remote, "i", "c", resource, max_bytes=10000)
    finally:
        remote.close()


@pytest.mark.parametrize(
    "url",
    [
        "http://clinic.test",
        "https://localhost",
        "https://127.0.0.1",
        "https://localhost.",
        "https://api.test/../private",
        "https://user:secret@api.test",
        "https://api.test?key=secret",
        "https://api.test:bad",
    ],
)
def test_invalid_base_url_is_rejected(service, remote, url):
    with service() as api:
        assert (
            api.post("/plugins", json={**definition(), "base_url": url}).status_code
            == 422
        )
    assert remote["requests"] == []


def test_base_url_is_forwarded_only_as_nango_proxy_configuration(service, remote):
    with service() as api:
        assert (
            api.post(
                "/plugins",
                json={**definition(), "base_url": "https://clinic.test/api/"},
            ).status_code
            == 201
        )
        ready = api.post("/plugins/clinic-api/provision", json={}).json()
        made = api.post(
            "/connectors",
            json={
                "name": "Clinic",
                "provider": "clinic-api",
                "streams": ["patients"],
                "nango_connection_id": "synthetic",
                "nango_integration": ready["nango_integration"],
            },
        )
        assert made.status_code == 201
        assert api.post(f"/connectors/{made.json()['id']}/test").json()["ok"]
        assert (
            remote["requests"][-1].headers["Base-Url-Override"]
            == "https://clinic.test/api"
        )


def test_reconfiguration_keeps_credentials_out_of_local_storage(service, remote):
    with service() as api:
        api.post("/plugins", json=definition())
        api.post("/plugins/clinic-api/provision", json={})
        assert (
            api.post(
                "/plugins/clinic-api/provision",
                json={
                    "client_id": "new-id",
                    "client_secret": "new-secret",
                    "integration_config": {"audience": "clinic"},
                },
            ).status_code
            == 200
        )
        assert remote["requests"][-1].method == "PATCH"
        assert "new-secret" not in api.get("/plugins").text
        for invalid in (
            {"client_id": "id"},
            {"client_id": "", "client_secret": ""},
            {"client_secret": "secret"},
        ):
            assert (
                api.post("/plugins/clinic-api/provision", json=invalid).status_code
                == 422
            )
        assert (
            api.post(
                "/plugins/clinic-api/provision",
                json={},
                headers={"Authorization": "Bearer wrong"},
            ).status_code
            == 401
        )


def test_integration_provider_conflict_does_not_activate_draft(service, remote, tenant):
    with service() as api:
        api.post("/plugins", json=definition())
        integration = f"nickel-{tenant.replace('-', '')}-clinic-api"
        remote["integrations"][integration] = {
            "unique_key": integration,
            "provider": "different",
        }
        assert api.post("/plugins/clinic-api/provision", json={}).status_code == 400
        assert api.get("/plugins").json()[0]["status"] == "draft"


def test_page_limit_and_non_json_response_are_reported():
    for response, paging, pattern in [
        (
            httpx.Response(200, json={"next_cursor": "two"}),
            {"mode": "cursor", "max_pages": 1},
            "max_pages",
        ),
        (httpx.Response(200, text="not-json"), {"mode": "none"}, "valid JSON"),
        (httpx.Response(403), {"mode": "none"}, "403"),
    ]:
        remote = NangoClient(
            "http://nango.test",
            "secret",
            transport=httpx.MockTransport(lambda _: response),
        )
        try:
            with pytest.raises(UpstreamError, match=pattern):
                fetch_resource(
                    remote,
                    "i",
                    "c",
                    PluginResource(name="r", path="/items", pagination=paging),
                    max_bytes=10000,
                )
        finally:
            remote.close()
