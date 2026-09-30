"""Reusable paging is read-only and preserves vendor bytes; runtime plugins are held."""

import json
from datetime import UTC, datetime

import httpx
import pytest

from src.connectors.nango import NangoClient, retry_delay
from src.connectors.nango.resources import fetch_resource
from src.db.connectors import ConnectorStore
from src.error import UpstreamError
from src.handler.nango import window_query
from src.models.plugins import PluginResource


def test_pages_are_yielded_verbatim_without_a_wrapper():
    pages = [
        b'{"entry":[1], "link":[{"relation":"next","url":"https://other.test/Patient?page=2"}]}',
        b'{"entry":[2]}',
    ]
    calls = []

    def respond(req):
        calls.append(req)
        assert req.url.host == "nango.test"
        assert "Base-Url-Override" not in req.headers
        return httpx.Response(200, content=pages[len(calls) - 1])

    client = NangoClient(
        "https://nango.test", "s", transport=httpx.MockTransport(respond)
    )
    result = fetch_resource(
        client,
        "i",
        "c",
        PluginResource(name="r", path="/Patient", pagination={"mode": "fhir"}),
        max_bytes=1000,
    )
    assert next(result) == pages[0]
    assert len(calls) == 1  # next page is not buffered before ingestion
    assert list(result) == [pages[1]]
    assert calls[1].url.params["page"] == "2"
    client.close()


@pytest.mark.parametrize("mode", ["cursor", "offset"])
def test_paging_modes(mode):
    calls = []

    def respond(req):
        calls.append(req)
        return httpx.Response(
            200,
            json=(
                {"data": [1, 2], "next_cursor": "two"}
                if len(calls) == 1
                else {"data": [3]}
            ),
        )

    client = NangoClient("https://n.test", "s", transport=httpx.MockTransport(respond))
    pages = list(
        fetch_resource(
            client,
            "i",
            "c",
            PluginResource(
                name="r", path="/items", pagination={"mode": mode, "page_size": 2}
            ),
            max_bytes=1000,
        )
    )
    assert len(pages) == 2
    assert calls[1].url.params["cursor" if mode == "cursor" else "offset"] == (
        "two" if mode == "cursor" else "2"
    )
    client.close()


@pytest.mark.parametrize(
    "body,paging,limit",
    [
        (b"not json", {}, 100),
        (b"[1,2]", {}, 1),
        (b'{"next_cursor":"repeat"}', {"mode": "cursor"}, 1000),
        (b'{"next_cursor":[]}', {"mode": "cursor"}, 1000),
        (b'{"data":[1]}', {"mode": "offset", "page_size": 1, "max_pages": 1}, 1000),
        (
            b'{"link":[{"relation":"next","url":"/fhir?_getpages=x"}]}',
            {"mode": "fhir"},
            1000,
        ),
    ],
)
def test_paging_refuses_invalid_or_unbounded_responses(body, paging, limit):
    client = NangoClient(
        "https://n.test",
        "s",
        transport=httpx.MockTransport(lambda _: httpx.Response(200, content=body)),
    )
    with pytest.raises(UpstreamError):
        list(
            fetch_resource(
                client,
                "i",
                "c",
                PluginResource(name="r", path="/Patient", pagination=paging),
                max_bytes=limit,
            )
        )
    client.close()


def test_post_search_and_transport_errors():
    def respond(req):
        assert req.method == "POST" and req.url.path == "/proxy/Patient/_search"
        assert json.loads(req.content) == {"active": True}
        return httpx.Response(200, content=b"[1]")

    client = NangoClient("https://n.test", "s", transport=httpx.MockTransport(respond))
    assert list(
        fetch_resource(
            client,
            "i",
            "c",
            PluginResource(
                name="r", path="/Patient/_search", method="POST", body={"active": True}
            ),
            max_bytes=100,
        )
    ) == [b"[1]"]
    client.close()

    def fail(req):
        raise httpx.ReadTimeout("secret")

    client = NangoClient("https://n.test", "s", transport=httpx.MockTransport(fail))
    with pytest.raises(UpstreamError, match="Nango"):
        list(
            fetch_resource(
                client, "i", "c", PluginResource(name="r", path="/r"), max_bytes=100
            )
        )
    client.close()


def test_429_retry_after_is_honored_and_bounded(monkeypatch):
    sleeps = []
    monkeypatch.setattr("src.connectors.nango.time.sleep", sleeps.append)
    replies = [
        httpx.Response(429, headers={"Retry-After": "2"}),
        httpx.Response(200, content=b"[]"),
    ]
    client = NangoClient(
        "https://n.test", "s", transport=httpx.MockTransport(lambda _: replies.pop(0))
    )
    assert client.proxy_get("i", "c", "/r") == b"[]"
    assert sleeps == [2]
    client.close()
    assert retry_delay("invalid", 1) == 2
    assert retry_delay("Wed, 01 Jan 2020 00:00:00 GMT", 0) == 0
    with pytest.raises(UpstreamError):
        retry_delay("3600", 0)
    client = NangoClient(
        "https://n.test",
        "s",
        transport=httpx.MockTransport(lambda _: httpx.Response(429)),
    )
    with pytest.raises(UpstreamError):
        client.proxy_get("i", "c", "/r")
    assert sleeps[-2:] == [1, 2]
    client.close()


def test_watermarks_are_per_tenant_connector_stream_and_monotonic(tmp_path):
    store = ConnectorStore(tmp_path / "c.sqlite")
    assert store.watermark("t", "c", "s") is None
    store.advance("t", "c", "s", "2026-09-30T12:00:00+00:00")
    store.advance("t", "c", "s", "2026-09-29T12:00:00+00:00")
    assert store.watermark("t", "c", "s") == "2026-09-30T12:00:00+00:00"
    for t, c, s in [("other", "c", "s"), ("t", "other", "s"), ("t", "c", "other")]:
        assert store.watermark(t, c, s) is None
    end = datetime(2026, 9, 30, 13, tzinfo=UTC)
    assert window_query("dexcom", "egvs", store.watermark("t", "c", "s"), end) == {
        "startDate": "2026-09-30T11:55:00",
        "endDate": "2026-09-30T13:00:00",
    }
    assert window_query("oura", "sleep", None, end) == {
        "start_date": "2026-09-28",
        "end_date": "2026-09-30",
    }
    assert window_query("dexcom", "devices", None, end) == {}
