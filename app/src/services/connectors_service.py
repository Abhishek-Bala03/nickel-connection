"""Connectors (Nango sources added in the Connections UI), their pull job, and what a pull stored."""

from typing import Annotated

from fastapi import APIRouter, Header, Path, Response

from src.connectors import nango as catalog
from src.handler import nango
from src.models.connectors import (
    ConnectorCreate,
    ConnectorResponse,
    ConnectorUpdate,
    NangoPullRequest,
    ProviderInfo,
    PullResponse,
)
from src.models.jobs import JobAccepted
from src.services.dependencies import Jobs, Settings
from src.services.jobs_service import IdempotencyKey, JobId, _accepted

connectors_router = APIRouter(tags=["connectors"])

ConnectorId = Annotated[str, Path(pattern=r"^[0-9a-f-]{36}$")]
Stream = Annotated[str, Path(pattern=r"^[A-Za-z0-9_]{1,40}$")]
# Set by the UI's gateway from the signed-in administrator; the service token already proved the
# caller is a trusted service. Kept on the row as who last changed it.
Actor = Annotated[
    str | None, Header(alias="X-Authenticated-User", min_length=1, max_length=255)
]


@connectors_router.get("/connectors/catalog")
def provider_catalog(settings: Settings) -> list[ProviderInfo]:
    """What can be connected, and the streams each provider offers."""
    return [
        ProviderInfo(
            id=key,
            name=spec.name,
            streams=list(spec.streams),
            simulated=key in settings.nango_base_url_overrides,
        )
        for key, spec in catalog.PROVIDERS.items()
    ]


@connectors_router.get("/connectors")
def list_connectors(settings: Settings) -> list[ConnectorResponse]:
    return [ConnectorResponse.of(c) for c in nango.list_connectors(settings)]


@connectors_router.post("/connectors", status_code=201)
def create_connector(
    body: ConnectorCreate, settings: Settings, actor: Actor = None
) -> ConnectorResponse:
    """Add a connector; 400 when Nango has no such connection, 409 for a duplicate."""
    return ConnectorResponse.of(
        nango.create_connector(
            settings,
            name=body.name,
            description=body.description,
            source=body.source,
            provider_key=body.provider,
            nango_connection_id=body.nango_connection_id,
            patient_id=body.patient_id,
            streams=body.streams,
            enabled=body.enabled,
            changed_by=actor,
        )
    )


@connectors_router.get("/connectors/{connector_id}")
def get_connector(connector_id: ConnectorId, settings: Settings) -> ConnectorResponse:
    return ConnectorResponse.of(nango.get_connector(settings, connector_id))


@connectors_router.put("/connectors/{connector_id}")
def update_connector(
    connector_id: ConnectorId,
    body: ConnectorUpdate,
    settings: Settings,
    actor: Actor = None,
) -> ConnectorResponse:
    """Name, description, streams and enabled; 409 when the version is not the current one."""
    return ConnectorResponse.of(
        nango.update_connector(
            settings,
            connector_id,
            name=body.name,
            description=body.description,
            streams=body.streams,
            enabled=body.enabled,
            version=body.version,
            changed_by=actor,
        )
    )


@connectors_router.post("/connectors/{connector_id}/test")
def check_connector(connector_id: ConnectorId, settings: Settings) -> dict:
    """One call through Nango for the first stream: ``ok`` and what came back, or why not."""
    return nango.check_connector(settings, connector_id)


@connectors_router.post("/jobs/nango/pull", status_code=202)
def queue_nango_pull(
    body: NangoPullRequest,
    jobs: Jobs,
    response: Response,
    idempotency_key: IdempotencyKey = None,
) -> JobAccepted:
    """Queue a pull of one connector's streams (404 for an unknown connector)."""
    nango.get_connector(jobs.settings, body.connector_id)
    job, created = jobs.submit(
        nango.PULL_KIND,
        {"connector_id": body.connector_id},
        idempotency_key=idempotency_key,
    )
    return _accepted(job, created, response)


@connectors_router.get("/jobs/{job_id}/pulls")
def job_pulls(job_id: JobId, settings: Settings) -> list[PullResponse]:
    """What each stream of a Nango pull stored (empty for any other job)."""
    return [
        PullResponse(
            stream=p.stream,
            outcome=p.outcome,
            ingest_id=p.ingest_id,
            bytes=p.bytes,
            records=p.records,
            fetched_at=p.fetched_at,
            viewable=p.payload_ref is not None and p.bytes <= nango.MAX_VIEW_BYTES,
        )
        for p in nango.job_pulls(settings, job_id)
    ]


@connectors_router.get("/jobs/{job_id}/pulls/{stream}/payload")
def pull_payload(job_id: JobId, stream: Stream, settings: Settings) -> Response:
    """The stored original of one stream of a pull, as the vendor sent it."""
    return Response(
        nango.read_payload(settings, job_id, stream),
        media_type="application/json",
        headers={"Content-Disposition": f'inline; filename="{stream}.json"'},
    )
