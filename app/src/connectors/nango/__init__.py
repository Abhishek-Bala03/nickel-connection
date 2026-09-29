"""The Nango connector: pull a wearable vendor's API through Nango's proxy into the connection layer.

Nango holds the credentials and signs each request (auth + proxy only, decision 2); this connector
decides what to fetch and when (the scheduler queues a ``nango.pull`` job). Each stream's response is
one raw item for the ingest bridge: original to the File Store, receipt, envelope to ``raw_data``.
Nothing is parsed beyond counting the records, for the run's history.

Built-in providers live in PROVIDERS. Tenant-owned plugins supply validated resource definitions
through the handler; the Nango provider controls authentication and the destination base URL.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from urllib.parse import urlsplit

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
    if not isinstance(body, dict):
        return 1
    if isinstance(body.get("pages"), list):
        return sum(count_records(json.dumps(page).encode()) for page in body["pages"])
    for key in ("data", "records", "entry"):
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

    def _json(self, what: str, method: str, path: str, **kwargs) -> dict:
        response = self._call(what, method, path, **kwargs)
        if response.status_code >= 300:
            raise UpstreamError(f"Nango refused {what} ({response.status_code})")
        try:
            result = response.json()
            if not isinstance(result, dict):
                raise ValueError
            return result
        except ValueError:
            raise UpstreamError(f"Nango returned an invalid {what} response") from None

    def providers(self) -> list[dict]:
        data = self._json("providers", "GET", "/providers").get("data")
        if not isinstance(data, list):
            raise UpstreamError("Nango returned an invalid providers response")

        providers = []
        for item in data:
            if not isinstance(item, dict):
                raise UpstreamError("Nango returned an invalid provider")

            provider_id = item.get("name")
            auth_mode = item.get("auth_mode")

            if (
                not isinstance(provider_id, str)
                or not provider_id
                or not isinstance(auth_mode, str)
                or not auth_mode
            ):
                raise UpstreamError("Nango returned an invalid provider")

            display_name = item.get("display_name")
            proxy = item.get("proxy")
            base_url = proxy.get("base_url") if isinstance(proxy, dict) else None

            # Return only metadata needed by the UI.
            providers.append(
                {
                    "id": provider_id,
                    "name": (
                        display_name
                        if isinstance(display_name, str) and display_name
                        else provider_id
                    ),
                    "auth_mode": auth_mode,
                    "base_url": base_url if isinstance(base_url, str) else None,
                }
            )

        return sorted(providers, key=lambda item: item["name"].casefold())

    def integrations(self) -> list[dict]:
        data = self._json("integrations", "GET", "/integrations").get("data")
        if not isinstance(data, list):
            raise UpstreamError("Nango returned an invalid integrations response")
        # Deliberate allowlist: never forward integration secrets to the browser.
        return [
            {
                "id": item["unique_key"],
                "provider": item.get("provider", ""),
                "name": item.get("display_name") or item["unique_key"],
            }
            for item in data
            if isinstance(item, dict) and isinstance(item.get("unique_key"), str)
        ]

    def connections(self, integration: str, page: int = 0) -> dict:
        data = self._json(
            "connections", "GET", "/connections", params={"page": page, "limit": 100}
        ).get("connections")
        if not isinstance(data, list):
            raise UpstreamError("Nango returned an invalid connections response")
        return {
            "connections": [
                {"id": item["connection_id"], "integration": integration}
                for item in data
                if isinstance(item, dict)
                and item.get("provider_config_key") == integration
                and isinstance(item.get("connection_id"), str)
            ],
            "next_page": page + 1 if len(data) == 100 else None,
        }

    def connect_session(self, integration: str, end_user_id: str) -> dict:
        # Validate the integration before granting a narrowly scoped, short-lived session.
        if not any(item["id"] == integration for item in self.integrations()):
            raise BadRequestError("Select an existing Nango integration")
        data = self._json(
            "connect session",
            "POST",
            "/connect/sessions",
            json={
                "allowed_integrations": [integration],
                "end_user": {"id": end_user_id},
            },
        ).get("data", {})
        link = data.get("connect_link") if isinstance(data, dict) else None
        if not isinstance(link, str) or urlsplit(link).scheme not in {"http", "https"}:
            raise UpstreamError(
                "Nango did not return a Connect link. Configure Connect UI on the Nango server."
            )
        return {"connect_link": link}

    def ensure_integration(
        self, integration: str, provider_id: str, name: str, options: dict
    ) -> None:
        existing = next(
            (i for i in self.integrations() if i["id"] == integration), None
        )
        if existing is None:
            response = self._call(
                "create integration",
                "POST",
                "/integrations",
                json={
                    "unique_key": integration,
                    "provider": provider_id,
                    "display_name": name,
                    **options,
                },
            )
            if response.status_code == 409:
                existing = next(
                    (i for i in self.integrations() if i["id"] == integration), None
                )
                if existing is None:
                    raise UpstreamError("Nango integration conflict; retry setup")
            elif response.status_code >= 300:
                raise UpstreamError(
                    f"Nango refused integration setup ({response.status_code})"
                )
            else:
                return
        if existing["provider"] != provider_id:
            raise BadRequestError(
                "The Nango integration belongs to a different provider"
            )
        if options:
            response = self._call(
                "configure integration",
                "PATCH",
                f"/integrations/{integration}",
                json=options,
            )
            if response.status_code >= 300:
                raise UpstreamError(
                    f"Nango refused integration configuration ({response.status_code})"
                )

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
        if found.status_code not in {200, 404}:
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
    spec: Provider | None = None,
    fetch_stream: Callable[[str], bytes] | None = None,
) -> dict[str, int]:
    """Fetch each stream and hand it to the bridge; the job's counts. A failed stream stops the
    run (UpstreamError): the streams before it are stored and receipted, and the next run repeats
    them as duplicates."""
    spec = spec or provider(provider_key)
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
        data = (
            fetch_stream(stream)
            if fetch_stream
            else client.proxy_get(
                integration,
                connection_id,
                spec.streams[stream],
                base_url_override=base_url_override,
            )
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
