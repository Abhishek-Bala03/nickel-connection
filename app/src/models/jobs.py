"""The jobs API: what the scheduler sends and gets back (docs/jobs-api.md)."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from src.connectors.documents import MANIFEST_ID_PATTERN
from src.db.jobs import Job, JobStatus


class PullJobRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    manifest: str = Field(
        pattern=MANIFEST_ID_PATTERN,
        description="Manifest id: the file stem under manifests/documents.",
    )
    limit: int | None = Field(
        default=None, ge=1, description="Stop after this many matching files."
    )


class RepublishJobRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: str | None = Field(
        default=None,
        pattern=MANIFEST_ID_PATTERN,
        description="Only this source's receipts (a manifest id); every source otherwise.",
    )
    limit: int | None = Field(
        default=None,
        ge=1,
        description="Take up at most this many receipts: published, found published "
        "already, set aside, or without an envelope.",
    )


class JobAccepted(BaseModel):
    job_id: str
    # queued for a job queued now (202); with an Idempotency-Key that replays one (200), that job's
    # status as it is now.
    status: JobStatus


class JobResponse(BaseModel):
    job_id: str
    kind: str
    status: JobStatus
    params: dict[str, Any]
    created_at: str
    started_at: str | None
    finished_at: str | None
    counts: dict[str, int]
    error: str | None

    @classmethod
    def of(cls, job: Job) -> JobResponse:
        return cls(
            job_id=job.job_id,
            kind=job.kind,
            status=job.status,
            params=job.params,
            created_at=job.created_at,
            started_at=job.started_at,
            finished_at=job.finished_at,
            counts=job.counts,
            error=job.error,
        )


class JobList(BaseModel):
    jobs: list[JobResponse]
