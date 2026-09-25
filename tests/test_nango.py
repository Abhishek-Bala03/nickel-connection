"""The Nango connector: the catalog, the Nango client, connectors, the nango.pull job and the payload
read-back (docs/demo-nango-wearables.md). Nango is a fake on httpx.MockTransport."""

import dataclasses
import json

import httpx
import pytest

from src.connectors import nango
from src.error import (
    BadRequestError,
    ConflictError,
    NotFoundError,
    UnavailableError,
    UpstreamError,
)
from src.handler import nango as handler
from src.handler.ingest import FORMATS

SIM = "http://vendor-sim:8080/oura/1"
KEY = "nango-secret-for-tests"
DAILY_SLEEP = json.dumps(
    {"data": [{"day": "2024-08-03"}, {"day": "2024-08-04"}]}
).encode()


class FakeNango:
    """Integrations and connections Nango knows, and the vendor behind its proxy."""

    def __init__(self):
        self.integrations: set[str] = set()
        self.connections: set[tuple[str, str]] = set()
        self.proxied: list[httpx.Request] = []
        self.fail_proxy = None  # a status the proxy answers with instead
        self.refuse = None  # a status POSTs answer with

    def __call__(self, request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"] == f"Bearer {KEY}"
        path = request.url.path
        if path.startswith("/proxy/"):
            self.proxied.append(request)
            if self.fail_proxy:
                return httpx.Response(self.fail_proxy, json={"error": "private body"})
            stream = path.rsplit("/", 1)[1]
            if stream == "daily_sleep":
                return httpx.Response(200, content=DAILY_SLEEP)
            return httpx.Response(200, json={"records": [1, 2, 3], "stream": stream})
        if request.method == "POST" and self.refuse:
            return httpx.Response(self.refuse)
        if path.startswith("/integrations/"):
            return httpx.Response(
                200 if path.split("/")[2] in self.integrations else 404
            )
        if path == "/integrations":
            self.integrations.add(json.loads(request.content)["unique_key"])
            return httpx.Response(200, json={})
        if path.startswith("/connections/"):
            key = (request.url.params["provider_config_key"], path.split("/")[2])
            return httpx.Response(200 if key in self.connections else 404)
        if path == "/connections":
            body = json.loads(request.content)
            assert body["credentials"] == {"type": "NONE"}
            self.connections.add((body["provider_config_key"], body["connection_id"]))
            return httpx.Response(201, json={})
        raise AssertionError(f"unexpected {request.method} {path}")


@pytest.fixture
def fake(monkeypatch):
    fake = FakeNango()
    monkeypatch.setattr(
        handler,
        "client",
        lambda settings: handler.nango.NangoClient(
            "http://nango.test", KEY, transport=httpx.MockTransport(fake)
        ),
    )
    return fake


@pytest.fixture
def nango_settings(settings):
    return dataclasses.replace(
        settings,
        nango_url="http://nango.test",
        nango_secret_key=KEY,
        nango_base_url_overrides={"oura": SIM},
    )


def new(settings, **over):
    values = dict(
        name="Oura - 1",
        description="demo ring",
        source="nango",
        provider_key="oura",
        nango_connection_id="oura-1",
        patient_id="12489275",
        streams=["daily_sleep", "sleep"],
        enabled=True,
        changed_by="admin",
    )
    return handler.create_connector(settings, **{**values, **over})


# ── Catalog and formats ──────────────────────────────────────────────────────
def test_json_is_an_ingest_format_and_nothing_else_passes_as_it():
    content_type, sniff = FORMATS["json"]
    assert content_type == "application/json"
    assert sniff(b'{"a": 1}') and sniff(b"[1]")
    assert not sniff(b"%PDF-1.7") and not sniff(b'"a string"') and not sniff(b"\xff")


def test_records_are_counted_the_way_each_vendor_lists_them():
    assert nango.count_records(DAILY_SLEEP) == 2
    assert nango.count_records(b'{"records": [1, 2, 3]}') == 3
    assert nango.count_records(b"[1, 2]") == 2
    assert nango.count_records(b'{"egvs": {}}') == 1


def test_an_unknown_provider_is_a_bad_request():
    with pytest.raises(BadRequestError):
        nango.provider("fitbit")


# ── The client ───────────────────────────────────────────────────────────────
def test_the_proxy_call_names_the_connection_and_the_override():
    fake = FakeNango()
    client = nango.NangoClient(
        "http://nango.test", KEY, transport=httpx.MockTransport(fake)
    )
    assert (
        client.proxy_get(
            "i", "c", "/v2/usercollection/daily_sleep", base_url_override=SIM
        )
        == DAILY_SLEEP
    )
    client.proxy_get("i", "c", "/v3/users/self/egvs")
    first, second = fake.proxied
    assert (
        first.headers["Connection-Id"] == "c"
        and first.headers["Provider-Config-Key"] == "i"
    )
    assert first.headers["Base-Url-Override"] == SIM
    assert "Base-Url-Override" not in second.headers


def test_client_errors_are_content_free():
    fake = FakeNango()
    fake.fail_proxy = 502
    client = nango.NangoClient(
        "http://nango.test", KEY, transport=httpx.MockTransport(fake)
    )
    with pytest.raises(UpstreamError) as caught:
        client.proxy_get("i", "c", "/x")
    assert "private" not in str(caught.value) and "502" in str(caught.value)

    def down(request):
        raise httpx.ConnectError("secret detail", request=request)

    client = nango.NangoClient(
        "http://nango.test", KEY, transport=httpx.MockTransport(down)
    )
    with pytest.raises(UpstreamError, match="ConnectError") as caught:
        client.has_connection("i", "c")
    assert "secret detail" not in str(caught.value)


def test_a_simulated_connection_is_provisioned_once(fake):
    client = handler.client(None)
    client.ensure_simulated_connection("nickel-sim-oura", "oura-1")
    client.ensure_simulated_connection("nickel-sim-oura", "oura-1")
    assert fake.integrations == {"nickel-sim-oura"}
    assert fake.connections == {("nickel-sim-oura", "oura-1")}
    assert client.has_connection("nickel-sim-oura", "oura-1")
    fake.refuse = 400
    with pytest.raises(UpstreamError, match="integration"):
        client.ensure_simulated_connection("other", "x")
    fake.integrations.add("other")
    with pytest.raises(UpstreamError, match="connection"):
        client.ensure_simulated_connection("other", "x")


def test_has_connection_raises_when_nango_fails():
    client = nango.NangoClient(
        "http://nango.test",
        KEY,
        transport=httpx.MockTransport(lambda r: httpx.Response(500)),
    )
    with pytest.raises(UpstreamError):
        client.has_connection("i", "c")


# ── Connectors ───────────────────────────────────────────────────────────────
def test_nango_must_be_configured(settings):
    with pytest.raises(UnavailableError):
        handler.client(settings)


def test_create_list_update_a_connector(nango_settings, fake):
    made = new(nango_settings, streams=["sleep", "daily_sleep", "sleep"])
    assert made.streams == ["daily_sleep", "sleep"]  # catalog order, no repeats
    assert made.nango_integration == "nickel-sim-oura" and made.version == 1
    assert ("nickel-sim-oura", "oura-1") in fake.connections
    assert handler.list_connectors(nango_settings) == [made]
    edited = handler.update_connector(
        nango_settings,
        made.id,
        name="Oura",
        description="",
        streams=["sleep"],
        enabled=False,
        version=1,
        changed_by="second",
    )
    assert (edited.name, edited.streams, edited.enabled, edited.version) == (
        "Oura",
        ["sleep"],
        False,
        2,
    )
    assert edited.changed_by == "second" and edited.nango_connection_id == "oura-1"
    with pytest.raises(ConflictError, match="Reload"):
        handler.update_connector(
            nango_settings,
            made.id,
            name="x",
            description="",
            streams=["sleep"],
            enabled=True,
            version=1,
            changed_by=None,
        )


def test_connector_refusals(nango_settings, fake):
    with pytest.raises(BadRequestError, match="not available"):
        new(nango_settings, source="nickel_internal")
    with pytest.raises(BadRequestError, match="no stream"):
        new(nango_settings, streams=["heart_rate"])
    with pytest.raises(BadRequestError, match="at least one"):
        new(nango_settings, streams=[])
    # Not simulated: the patient must have connected through Nango first.
    with pytest.raises(BadRequestError, match="connect the patient"):
        new(
            nango_settings,
            provider_key="dexcom",
            streams=["egvs"],
            nango_connection_id="d-1",
        )
    fake.connections.add(("nickel-sim-dexcom", "d-1"))
    assert new(
        nango_settings,
        name="Dexcom",
        provider_key="dexcom",
        streams=["egvs"],
        nango_connection_id="d-1",
    )
    new(nango_settings)
    with pytest.raises(ConflictError):
        new(nango_settings)  # same name
    with pytest.raises(ConflictError):
        new(nango_settings, name="other")  # same Nango connection
    first = handler.list_connectors(nango_settings)[0]
    with pytest.raises(ConflictError):
        handler.update_connector(
            nango_settings,
            first.id,
            name="Oura - 1",
            description="",
            streams=["egvs"],
            enabled=True,
            version=first.version,
            changed_by=None,
        )
    with pytest.raises(NotFoundError):
        handler.get_connector(nango_settings, "0192a3b4-0000-7000-8000-00000000dead")


def test_check_connector(nango_settings, fake):
    made = new(nango_settings)
    ok = handler.check_connector(nango_settings, made.id)
    assert ok["ok"] and ok["stream"] == "daily_sleep" and ok["records"] == 2
    assert ok["bytes"] == len(DAILY_SLEEP)
    fake.fail_proxy = 503
    failed = handler.check_connector(nango_settings, made.id)
    assert failed == {
        "ok": False,
        "stream": "daily_sleep",
        "message": failed["message"],
    }
    assert "503" in failed["message"]


# ── The pull job, through the API ────────────────────────────────────────────
def test_a_pull_stores_each_stream_and_serves_it_back(
    service, nango_settings, fake, wait_for
):
    with service(
        **{
            f.name: getattr(nango_settings, f.name)
            for f in dataclasses.fields(nango_settings)
        }
    ) as client:
        assert [p["id"] for p in client.get("/connectors/catalog").json()] == [
            "oura",
            "dexcom",
        ]
        assert client.get("/connectors/catalog").json()[0]["simulated"] is True
        created = client.post(
            "/connectors",
            json={
                "name": "Oura - 12489275",
                "provider": "oura",
                "nango_connection_id": "oura-12489275",
                "patient_id": "12489275",
                "streams": ["daily_sleep", "sleep"],
            },
            headers={"X-Authenticated-User": "local-demo-admin"},
        )
        assert created.status_code == 201, created.text
        connector = created.json()
        assert connector["changed_by"] == "local-demo-admin"
        assert client.get(f"/connectors/{connector['id']}").json() == connector
        assert client.post(f"/connectors/{connector['id']}/test").json()["ok"] is True

        queued = client.post("/jobs/nango/pull", json={"connector_id": connector["id"]})
        assert queued.status_code == 202
        job = wait_for(client, queued.json()["job_id"])
        assert job["status"] == "succeeded", job
        assert job["counts"] == {
            "streams": 2,
            "records": 5,
            "published": 2,
            "duplicates": 0,
            "rejected": 0,
        }

        pulls = client.get(f"/jobs/{job['job_id']}/pulls").json()
        assert [
            (p["stream"], p["outcome"], p["records"], p["viewable"]) for p in pulls
        ] == [
            ("daily_sleep", "published", 2, True),
            ("sleep", "published", 3, True),
        ]
        payload = client.get(f"/jobs/{job['job_id']}/pulls/daily_sleep/payload")
        assert payload.headers["content-type"] == "application/json"
        assert payload.content == DAILY_SLEEP
        assert (
            client.get(f"/jobs/{job['job_id']}/pulls/steps/payload").status_code == 404
        )

        # The same bytes again: duplicates, still viewable, still counted.
        again = wait_for(
            client,
            client.post(
                "/jobs/nango/pull", json={"connector_id": connector["id"]}
            ).json()["job_id"],
        )
        assert again["counts"]["duplicates"] == 2 and again["counts"]["records"] == 5
        assert (
            client.get(f"/jobs/{again['job_id']}/pulls/sleep/payload").status_code
            == 200
        )

        updated = client.put(
            f"/connectors/{connector['id']}",
            json={"name": "Oura", "streams": ["sleep"], "enabled": False, "version": 1},
        )
        assert updated.status_code == 200 and updated.json()["version"] == 2
        disabled = wait_for(
            client,
            client.post(
                "/jobs/nango/pull", json={"connector_id": connector["id"]}
            ).json()["job_id"],
        )
        assert disabled["status"] == "failed" and "disabled" in disabled["error"]

        unknown = client.post(
            "/jobs/nango/pull",
            json={"connector_id": "0192a3b4-0000-7000-8000-00000000dead"},
        )
        assert unknown.status_code == 404
        assert client.get("/connectors").json()[0]["name"] == "Oura"


def test_a_failed_stream_fails_the_job_content_free(
    service, nango_settings, fake, wait_for
):
    with service(
        **{
            f.name: getattr(nango_settings, f.name)
            for f in dataclasses.fields(nango_settings)
        }
    ) as client:
        connector = client.post(
            "/connectors",
            json={
                "name": "Oura",
                "provider": "oura",
                "nango_connection_id": "o",
                "streams": ["sleep"],
            },
        ).json()
        fake.fail_proxy = 502
        job = wait_for(
            client,
            client.post(
                "/jobs/nango/pull", json={"connector_id": connector["id"]}
            ).json()["job_id"],
        )
        assert (
            job["status"] == "failed"
            and "502" in job["error"]
            and "private" not in job["error"]
        )
        assert client.get(f"/jobs/{job['job_id']}/pulls").json() == []


def test_a_payload_over_the_viewer_cap_is_not_served(nango_settings, fake, monkeypatch):
    made = new(nango_settings)
    job = handler.Job(
        job_id="0192a3b4-0000-7000-8000-0000000000aa",
        kind=handler.PULL_KIND,
        status="running",
        params={"connector_id": made.id},
        created_at="now",
    )
    handler.run_pull(nango_settings, job)
    monkeypatch.setattr(handler, "MAX_VIEW_BYTES", 10)
    with pytest.raises(BadRequestError, match="viewer"):
        handler.read_payload(nango_settings, job.job_id, "daily_sleep")
