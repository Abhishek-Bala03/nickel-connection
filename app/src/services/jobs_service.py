"""The jobs API for the scheduler: queue a pull or a republish, read a job's state (docs/jobs-api.md)."""

from typing import Annotated

from fastapi import APIRouter, Header, Path, Query, Response

from src.db.jobs import Job
from src.error import NotFoundError
from src.handler import documents
from src.handler.jobs import PULL_KIND, REPUBLISH_KIND
from src.models.jobs import (
    JobAccepted,
    JobList,
    JobResponse,
    PullJobRequest,
    RepublishJobRequest,
)
from src.services.dependencies import Jobs

jobs_router = APIRouter(tags=["jobs"])

# Plain and short, like a request id: it goes into the job record, never into a log.
IdempotencyKey = Annotated[
    str | None,
    Header(
        pattern=r"^[A-Za-z0-9._:-]{1,64}$",
        description=(
            "Send the same key again when retrying the same request: the job queued before comes "
            "back (200) instead of a second one. Letters, digits, '.', '_', ':' and '-', up to 64."
        ),
    ),
]
# A job id is a UUID the service minted (src.utility.ids): anything else is invalid, so a 404 never
# quotes a caller's own text back (of any length, with any characters).
JobId = Annotated[
    str,
    Path(pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"),
]


def _accepted(job: Job, created: bool, response: Response) -> JobAccepted:
    """202 for a job queued now; 200 with the existing job when an Idempotency-Key replays one."""
    if not created:
        response.status_code = 200
    return JobAccepted(job_id=job.job_id, status=job.status)


@jobs_router.post("/jobs/documents/pull", status_code=202)
def queue_pull(
    body: PullJobRequest,
    jobs: Jobs,
    response: Response,
    idempotency_key: IdempotencyKey = None,
) -> JobAccepted:
    """Queue a pull of one document source (404 for an unknown manifest, 503 without a tenant, 429
    when the queue is full)."""
    documents.manifest_path(jobs.settings, body.manifest)
    documents.require_tenant(jobs.settings)
    job, created = jobs.submit(
        PULL_KIND,
        {"manifest": body.manifest, "limit": body.limit},
        idempotency_key=idempotency_key,
    )
    return _accepted(job, created, response)


@jobs_router.post("/jobs/documents/republish", status_code=202)
def queue_republish(
    body: RepublishJobRequest,
    jobs: Jobs,
    response: Response,
    idempotency_key: IdempotencyKey = None,
) -> JobAccepted:
    """Queue a republish of the open receipts from the envelopes they store: received,
    publish_failed and held ones, each only while its source is enabled (503 without a tenant,
    429 when the queue is full)."""
    documents.require_tenant(jobs.settings)
    job, created = jobs.submit(
        REPUBLISH_KIND,
        {"source": body.source, "limit": body.limit},
        idempotency_key=idempotency_key,
    )
    return _accepted(job, created, response)


@jobs_router.get("/jobs")
def list_jobs(jobs: Jobs, limit: int = Query(20, ge=1, le=200)) -> JobList:
    """The newest jobs first."""
    return JobList(jobs=[JobResponse.of(job) for job in jobs.recent(limit)])


@jobs_router.get("/jobs/{job_id}")
def get_job(job_id: JobId, jobs: Jobs) -> JobResponse:
    job = jobs.get(job_id)
    if job is None:
        raise NotFoundError(f"no job {job_id}")
    return JobResponse.of(job)
