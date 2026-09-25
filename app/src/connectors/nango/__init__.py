"""The Nango connector: pull a wearable vendor's API through Nango's proxy into the connection layer.

Nango holds the credentials and signs each request (auth + proxy only, decision 2); this connector
decides what to fetch and when (the scheduler queues a ``nango.pull`` job). Each stream's response is
one raw item for the ingest bridge: original to the File Store, receipt, envelope to ``raw_data``.
Nothing is parsed beyond counting the records, for the run's history.

What a provider exposes is fixed here, in code (``PROVIDERS``): a connector picks streams from this
list and never supplies a path or a base URL of its own.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass

import httpx

from src.error import BadRequestError, UpstreamError
from src.handler.ingest import (
    IngestBridge,
    IngestResult,
    Outcome,
    RawItem,
    SourceContext,
)
from src.utility.keys import ObjectClass

CONNECTOR = "nango"
CONNECTOR_VERSION = "0.1.0"
# ponytail: the envelope requires a lab route (L1-L9) and wearables have none. L7 is the lowest tier
# of the fidelity ranking, so a wearable payload never outranks a lab. Replace it once the envelope
# takes a null route for non-lab sources.
ROUTE = "L7"
_TIMEOUT_S = 30.0
_COUNTED = {
    Outcome.PUBLISHED: "published",
    Outcome.DUPLICATE: "duplicates",
    Outcome.REJECTED: "rejected",
}


@dataclass(frozen=True)
class Provider:
    name: str
    integration: (
        str  # the Nango integration (provider config key) connections are made under
    )
    streams: dict[str, str]  # stream -> the vendor's path


PROVIDERS: dict[str, Provider] = {
    "oura": Provider(
        name="Oura Ring",
        integration="nickel-sim-oura",
        streams={
            stream: f"/v2/usercollection/{stream}"
            for stream in (
                "daily_sleep",
                "daily_readiness",
                "daily_activity",
                "daily_spo2",
                "sleep",
            )
        },
    ),
    "dexcom": Provider(
        name="Dexcom CGM",
        integration="nickel-sim-dexcom",
        streams={
            stream: f"/v3/users/self/{stream}"
            for stream in ("egvs", "devices", "dataRange")
        },
    ),
}


def provider(key: str) -> Provider:
    found = PROVIDERS.get(key)
    if found is None:
        raise BadRequestError(f"unknown provider {key!r}")
    return found


def count_records(data: bytes) -> int:
    """Oura lists in ``data``, Dexcom in ``records``; any other object is one record."""
    body = json.loads(data)
    if isinstance(body, list):
        return len(body)
    for key in ("data", "records"):
        if isinstance(body.get(key), list):
            return len(body[key])
    return 1


class NangoClient:
    """Nango's REST API with the environment's secret key. Errors are content-free: a status and
    what was being done, never a response body (it can echo a payload)."""

    def __init__(
        self, url: str, secret_key: str, *, transport: httpx.BaseTransport | None = None
    ) -> None:
        self._http = httpx.Client(
            base_url=url,
            headers={"Authorization": f"Bearer {secret_key}"},
            timeout=_TIMEOUT_S,
            trust_env=False,
            follow_redirects=False,
            transport=transport,
        )

    def close(self) -> None:
        self._http.close()

    def _call(self, what: str, method: str, path: str, **kwargs) -> httpx.Response:
        try:
            return self._http.request(method, path, **kwargs)
        except httpx.HTTPError as exc:
            raise UpstreamError(
                f"Nango unreachable ({what}): {type(exc).__name__}"
            ) from None

    def ensure_simulated_connection(self, integration: str, connection_id: str) -> None:
        """Development only: the integration (Nango's ``unauthenticated`` provider) and the
        connection, made if missing. Real vendors connect through Nango's OAuth flow instead.
        """
        found = self._call("integration", "GET", f"/integrations/{integration}")
        if found.status_code == 404:
            made = self._call(
                "integration",
                "POST",
                "/integrations",
                json={"unique_key": integration, "provider": "unauthenticated"},
            )
            if made.status_code >= 300:
                raise UpstreamError(
                    f"Nango refused the integration ({made.status_code})"
                )
        connection = self._call(
            "connection",
            "GET",
            f"/connections/{connection_id}",
            params={"provider_config_key": integration},
        )
        if connection.status_code == 200:
            return
        made = self._call(
            "connection",
            "POST",
            "/connections",
            json={
                "provider_config_key": integration,
                "connection_id": connection_id,
                "credentials": {"type": "NONE"},
            },
        )
        if made.status_code >= 300:
            raise UpstreamError(f"Nango refused the connection ({made.status_code})")

    def has_connection(self, integration: str, connection_id: str) -> bool:
        found = self._call(
            "connection",
            "GET",
            f"/connections/{connection_id}",
            params={"provider_config_key": integration},
        )
        if found.status_code >= 500:
            raise UpstreamError(f"Nango failed ({found.status_code})")
        return found.status_code == 200

    def proxy_get(
        self,
        integration: str,
        connection_id: str,
        path: str,
        *,
        base_url_override: str | None = None,
    ) -> bytes:
        headers = {"Connection-Id": connection_id, "Provider-Config-Key": integration}
        if base_url_override:
            headers["Base-Url-Override"] = base_url_override
        response = self._call("proxy", "GET", f"/proxy{path}", headers=headers)
        if response.status_code != 200:
            raise UpstreamError(
                f"the vendor call through Nango returned {response.status_code}"
            )
        return response.content


@dataclass(frozen=True)
class StreamResult:
    stream: str
    result: IngestResult
    bytes: int
    records: int


def pull(
    client: NangoClient,
    bridge: IngestBridge,
    *,
    provider_key: str,
    integration: str,
    connection_id: str,
    streams: list[str],
    patient_id: str | None,
    trace_id: str,
    base_url_override: str | None,
    on_stream: Callable[[StreamResult], None] = lambda _: None,
) -> dict[str, int]:
    """Fetch each stream and hand it to the bridge; the job's counts. A failed stream stops the
    run (UpstreamError): the streams before it are stored and receipted, and the next run repeats
    them as duplicates."""
    spec = provider(provider_key)
    ctx = SourceContext(
        source=connection_id,
        fetch=CONNECTOR,
        route=ROUTE,
        mode="pull",
        connector=CONNECTOR,
        connector_version=CONNECTOR_VERSION,
        retention_class=ObjectClass.STREAM,
        formats=("json",),
        trace_id=trace_id,
        connection_id=connection_id,
    )
    counts = {
        "streams": 0,
        "records": 0,
        "published": 0,
        "duplicates": 0,
        "rejected": 0,
    }
    for stream in streams:
        data = client.proxy_get(
            integration,
            connection_id,
            spec.streams[stream],
            base_url_override=base_url_override,
        )
        result = bridge.ingest(
            ctx,
            RawItem(
                message_id=f"{connection_id}/{stream}",
                data=data,
                source_meta={"provider": provider_key, "stream": stream},
                source_patient_id=patient_id,
            ),
        )
        records = count_records(data) if result.payload_ref else 0
        counts["streams"] += 1
        counts["records"] += records
        key = _COUNTED.get(result.outcome)
        if key:
            counts[key] += 1
        on_stream(StreamResult(stream, result, len(data), records))
    return counts
