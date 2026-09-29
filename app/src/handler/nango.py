"""Nango connectors: add and edit them, test one, pull one (the ``nango.pull`` job), and read back
what a pull stored.

The pieces are opened from the settings here, as ``src.handler.documents`` does for document sources.
A connector is created only against a Nango connection that exists: in development, one pointed at
the vendor simulator (``NICKEL_NANGO_BASE_URL_OVERRIDES``) is provisioned on the spot; anywhere else
the patient connects through Nango's own OAuth flow first.

Reading a stored original back is for the administrator's Executions view only: it serves exactly
the object a pull of this tenant recorded, by job and stream, and nothing else in the File Store.
"""

from __future__ import annotations

import time
from dataclasses import replace
from datetime import UTC, datetime
from uuid import UUID

from src.config import ServiceSettings
from src.connectors import nango
from src.connectors.nango.resources import fetch_resource
from src.db.connectors import Connector, ConnectorConflictError, ConnectorStore, Pull
from src.db.file_store import open_file_store
from src.db.jobs import Job
from src.db.layout import DataLayout
from src.error import (
    BadRequestError,
    ConflictError,
    NickelConnectionError,
    NotFoundError,
    UnavailableError,
)
from src.handler import plugin_catalog
from src.handler.documents import ingest_bridge, require_tenant
from src.models.plugins import PluginResource
from src.utility.ids import uuid7

PULL_KIND = "nango.pull"
# The Executions view renders what it gets; a larger stored original is downloaded, not shown.
MAX_VIEW_BYTES = 5 * 1024 * 1024


def _now() -> str:
    return datetime.now(UTC).isoformat()


def store(settings: ServiceSettings) -> ConnectorStore:
    return ConnectorStore(DataLayout(settings.data_dir).connectors_db)


def client(settings: ServiceSettings) -> nango.NangoClient:
    if not settings.nango_url or not settings.nango_secret_key:
        raise UnavailableError(
            "Nango is not configured: set NICKEL_NANGO_URL and NICKEL_NANGO_SECRET_KEY"
        )
    return nango.NangoClient(settings.nango_url, settings.nango_secret_key)


def _conflict(exc: ConnectorConflictError) -> ConflictError:
    return ConflictError(str(exc))


def _streams(
    settings: ServiceSettings, provider_key: str, streams: list[str]
) -> list[str]:
    known = plugin_catalog.provider(settings, provider_key).streams
    unknown = [s for s in streams if s not in known]
    if unknown:
        raise BadRequestError(f"{provider_key} has no stream {unknown[0]!r}")
    if not streams:
        raise BadRequestError("Pick at least one stream")
    return [s for s in known if s in streams]  # catalog order, no repeats


def get_connector(settings: ServiceSettings, connector_id: str) -> Connector:
    tenant = str(require_tenant(settings))
    found = store(settings).get(tenant, connector_id)
    if found is None:
        raise NotFoundError(f"no connector {connector_id}")
    return found


def list_connectors(settings: ServiceSettings) -> list[Connector]:
    return store(settings).list(str(require_tenant(settings)))


def create_connector(
    settings: ServiceSettings,
    *,
    name: str,
    description: str,
    source: str,
    provider_key: str,
    nango_connection_id: str,
    patient_id: str | None,
    streams: list[str],
    enabled: bool,
    changed_by: str | None,
    nango_integration: str | None = None,
) -> Connector:
    tenant = str(require_tenant(settings))
    if source != "nango":
        raise BadRequestError("Nickel internal sources are not available yet")
    spec = plugin_catalog.provider(settings, provider_key)
    integration = nango_integration or spec.integration
    plugin_catalog.check_integration(settings, provider_key, integration)
    streams = _streams(settings, provider_key, streams)
    nango_client = client(settings)
    try:
        if (
            nango_integration is None
            and provider_key in settings.nango_base_url_overrides
        ):
            nango_client.ensure_simulated_connection(
                spec.integration, nango_connection_id
            )
        elif not nango_client.has_connection(integration, nango_connection_id):
            raise BadRequestError(
                "No such Nango connection: connect the patient's account in Nango first"
            )
    finally:
        nango_client.close()
    stamp = _now()
    connector = Connector(
        id=str(uuid7()),
        tenant_id=tenant,
        name=name,
        description=description,
        source=source,
        provider=provider_key,
        nango_integration=integration,
        nango_connection_id=nango_connection_id,
        patient_id=patient_id,
        streams=streams,
        enabled=enabled,
        version=1,
        created_at=stamp,
        updated_at=stamp,
        changed_by=changed_by,
    )
    try:
        store(settings).add(connector)
    except ConnectorConflictError as exc:
        raise _conflict(exc) from None
    return connector


def update_connector(
    settings: ServiceSettings,
    connector_id: str,
    *,
    name: str,
    description: str,
    streams: list[str],
    enabled: bool,
    version: int,
    changed_by: str | None,
    provider_key: str | None = None,
) -> Connector:
    current = get_connector(settings, connector_id)
    selected_provider = provider_key or current.provider
    plugin_catalog.check_integration(
        settings, selected_provider, current.nango_integration
    )
    selected_streams = _streams(settings, selected_provider, streams)

    if selected_provider != current.provider:
        remote = client(settings)
        try:
            if not remote.has_connection(
                current.nango_integration,
                current.nango_connection_id,
            ):
                raise BadRequestError("The saved connection no longer exists in Nango")
        finally:
            remote.close()

    edited = replace(
        current,
        name=name,
        description=description,
        provider=selected_provider,
        streams=selected_streams,
        enabled=enabled,
        updated_at=_now(),
        changed_by=changed_by,
    )

    try:
        return store(settings).update(
            edited,
            expected_version=version,
        )
    except ConnectorConflictError as exc:
        raise _conflict(exc) from None


def check_connector(settings: ServiceSettings, connector_id: str) -> dict:
    """One call through Nango for the connector's first stream: does the chain answer?"""
    connector = get_connector(settings, connector_id)
    spec = plugin_catalog.provider(settings, connector.provider)
    stream = connector.streams[0]
    nango_client = client(settings)
    started = time.monotonic()
    try:
        data = (
            resource_fetcher(settings, connector, nango_client)(stream)
            if connector.provider not in nango.PROVIDERS
            else nango_client.proxy_get(
                connector.nango_integration,
                connector.nango_connection_id,
                spec.streams[stream],
                base_url_override=base_url_override(settings, connector),
            )
        )
        return {
            "ok": True,
            "stream": stream,
            "bytes": len(data),
            "records": nango.count_records(data),
            "elapsed_ms": round((time.monotonic() - started) * 1000),
        }
    except (NickelConnectionError, ValueError) as exc:
        return {"ok": False, "stream": stream, "message": str(exc)}
    finally:
        nango_client.close()


def run_pull(settings: ServiceSettings, job: Job) -> dict[str, int]:
    """The ``nango.pull`` job: each of the connector's streams through Nango into the bridge, and
    each stream's stored object recorded against the job."""
    connector = get_connector(settings, job.params["connector_id"])
    if not connector.enabled:
        raise BadRequestError("the connector is disabled")
    tenant = connector.tenant_id
    connectors = store(settings)

    def record(result: nango.StreamResult) -> None:
        connectors.record_pull(
            Pull(
                job_id=job.job_id,
                tenant_id=tenant,
                connector_id=connector.id,
                stream=result.stream,
                outcome=str(result.result.outcome),
                ingest_id=result.result.ingest_id,
                payload_ref=result.result.payload_ref,
                bytes=result.bytes,
                records=result.records,
                fetched_at=_now(),
            )
        )

    nango_client = client(settings)
    try:
        with ingest_bridge(settings) as bridge:
            return nango.pull(
                nango_client,
                bridge,
                provider_key=connector.provider,
                integration=connector.nango_integration,
                connection_id=connector.nango_connection_id,
                streams=connector.streams,
                patient_id=connector.patient_id,
                trace_id=UUID(job.job_id).hex,
                base_url_override=base_url_override(settings, connector),
                on_stream=record,
                spec=plugin_catalog.provider(settings, connector.provider),
                fetch_stream=(
                    resource_fetcher(settings, connector, nango_client)
                    if connector.provider not in nango.PROVIDERS
                    else None
                ),
            )
    finally:
        nango_client.close()


def job_pulls(settings: ServiceSettings, job_id: str) -> list[Pull]:
    return store(settings).pulls(str(require_tenant(settings)), job_id)


def read_payload(settings: ServiceSettings, job_id: str, stream: str) -> bytes:
    pull = next((p for p in job_pulls(settings, job_id) if p.stream == stream), None)
    if pull is None or pull.payload_ref is None:
        raise NotFoundError(f"job {job_id} stored nothing for stream {stream!r}")
    if pull.bytes > MAX_VIEW_BYTES:
        raise BadRequestError(
            f"the payload is {pull.bytes} bytes; the viewer shows up to {MAX_VIEW_BYTES}"
        )
    return open_file_store(settings).get(pull.payload_ref, max_bytes=MAX_VIEW_BYTES)


def base_url_override(settings: ServiceSettings, connector: Connector) -> str | None:
    # Local simulator routing applies only to the legacy local demo integration.
    if (
        connector.nango_integration
        != plugin_catalog.provider(settings, connector.provider).integration
    ):
        return None
    return settings.nango_base_url_overrides.get(connector.provider)


def resource_fetcher(settings, connector, remote):
    definition = plugin_catalog.plugin(settings, connector.provider)
    resources = {
        r["name"]: PluginResource.model_validate(r) for r in definition["resources"]
    }

    def fetch(stream):
        return fetch_resource(
            remote,
            connector.nango_integration,
            connector.nango_connection_id,
            resources[stream],
            max_bytes=settings.max_file_bytes,
            base_url=definition.get("base_url"),
        )

    return fetch
