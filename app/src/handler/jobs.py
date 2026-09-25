"""Jobs: the scheduler (or an operator) queues a pull, and one worker thread runs the jobs one after
another in this process. Records live in ``jobs.sqlite`` (``src.db.jobs``).

The queue is bounded (``[JOBS] max_queued``: a 429 beyond it) and a submission can carry an
``Idempotency-Key``, so a scheduler that retries after a timeout gets the job it queued before
instead of a second one.

Three kinds of job: ``documents.pull`` brings a source in, ``documents.republish`` publishes the open
receipts again from the envelopes they store, and ``nango.pull`` brings a Nango connector's streams in
(``src.handler.nango``). The scheduler queues a republish on a fixed cadence,
and the worker queues one itself when it starts (``build_runner``). The connection layer ends at ``raw_data``, and what
happens to a document after that (parsing, OCR, extraction) is the Document worker's, a service of
its own. A job's id (a UUIDv7) is the ``trace_id`` of its run: every envelope the run publishes
carries it.

Run one server process (uvicorn's default): each process would run its own worker and, at start-up,
mark the others' running jobs as interrupted.
"""

from __future__ import annotations

import queue
import threading
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from src.config import ServiceSettings
from src.connectors.documents import ManifestError
from src.connectors.documents.sources import FetchError
from src.db.file_store import FileStoreError
from src.db.jobs import INTERRUPTED, Job, JobStatus, JobStore
from src.db.layout import DataLayout
from src.db.outbox import PublishError
from src.error import ConflictError, NickelConnectionError, TooManyRequestsError
from src.handler import documents, nango
from src.logging import logger
from src.utility.ids import uuid7

PULL_KIND = "documents.pull"
REPUBLISH_KIND = "documents.republish"
# The error of a job that left the queue but could not start (its record could not be read or
# marked running): content-free, like INTERRUPTED.
COULD_NOT_START = "could not start"
RETRY_AFTER_S = 30  # what a 429 tells the scheduler (docs/jobs-api.md)
_STOP = object()
_ERROR_CHARS = 1000
# Errors whose message is content-free by contract: the service's own, a source that could not be
# listed (FetchError: the manifest's locator, never a file's content), a manifest that does not load
# (ManifestError: field names and error types, never the file's values), a publish failure and a
# File Store failure (FileStoreError: the operation and the status, never the key). Any other
# exception may quote what it choked on (pydantic's ValidationError prints the rejected input,
# an OSError a path with a clinic's file name), so the record keeps its type alone.
_OWN_ERRORS = (
    NickelConnectionError,
    FetchError,
    ManifestError,
    PublishError,
    FileStoreError,
)


@dataclass(frozen=True)
class JobResult:
    counts: dict[str, int] = field(default_factory=dict)
    error: str | None = None


JobHandler = Callable[[Job], JobResult]


def run_pull(settings: ServiceSettings, job: Job) -> JobResult:
    manifest = documents.load_manifest(settings, job.params["manifest"])
    summary = documents.pull_documents(
        settings,
        manifest,
        limit=job.params.get("limit"),
        trace_id=UUID(job.job_id).hex,
    )
    # The counts and the content-free reason the run stopped (RunSummary.error): a publish failure
    # without the file it failed at, or the phase the manifest is not cleared for. Never a file name.
    return JobResult(summary.counts(), summary.error)


def run_republish(settings: ServiceSettings, job: Job) -> JobResult:
    summary = documents.republish_documents(
        settings,
        source=job.params.get("source"),
        limit=job.params.get("limit"),
        trace_id=UUID(job.job_id).hex,
    )
    return JobResult(summary.counts(), summary.error)


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _describe(exc: BaseException) -> str:
    """The message of one of our own errors, the type name of any other (see ``_OWN_ERRORS``)."""
    if isinstance(exc, _OWN_ERRORS):
        return (str(exc) or type(exc).__name__)[:_ERROR_CHARS]
    return type(exc).__name__


def _counts_text(counts: Mapping[str, int]) -> str:
    return ", ".join(f"{name} {value}" for name, value in counts.items()) or "-"


class JobRunner:
    """A FIFO queue of job ids and the one thread that works through it. ``startup`` names the
    jobs (kind, params) it queues itself when it starts, after the ones a previous process left
    queued."""

    def __init__(
        self,
        settings: ServiceSettings,
        store: JobStore,
        handlers: Mapping[str, JobHandler],
        *,
        startup: Sequence[tuple[str, Mapping[str, Any]]] = (),
    ) -> None:
        self.settings = settings
        self.store = store
        self.max_queued = settings.max_queued_jobs
        self._handlers = dict(handlers)
        self._startup = tuple(startup)
        self._queue: queue.Queue[Any] = queue.Queue()
        self._submit_lock = threading.Lock()
        self._stopping = threading.Event()
        self._thread = threading.Thread(
            target=self._loop, name="nickel-jobs", daemon=True
        )

    def start(self) -> None:
        interrupted, queued = self.store.recover(_now())
        if interrupted:
            logger.warning(
                "%d job(s) left running by the previous process failed as %s",
                interrupted,
                INTERRUPTED,
            )
        for job_id in queued:
            self._queue.put(job_id)
        if queued:
            logger.info("%d queued job(s) picked up again", len(queued))
        # Not held to the queue's bound: the service's own start-up jobs are not a caller to 429.
        # One that still waits from a start before (the worker never took it: a crash loop, say)
        # is not queued again, or every start would add one and fill the queue. (Job records have
        # no tenant: a deployment runs for one.)
        waiting = [self.store.get(job_id) for job_id in queued]
        for kind, params in self._startup:
            if any(job.kind == kind and job.params == dict(params) for job in waiting):
                continue
            self._add(kind, params, idempotency_key=None)
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        """Finish the running job if it is quick; a long one is marked interrupted at the next start."""
        self._stopping.set()
        self._queue.put(_STOP)
        self._thread.join(timeout)
        if self._thread.is_alive():
            logger.warning(
                "a job is still running at shutdown; it will fail as %s at the next start",
                INTERRUPTED,
            )

    def submit(
        self,
        kind: str,
        params: Mapping[str, Any],
        *,
        idempotency_key: str | None = None,
    ) -> tuple[Job, bool]:
        """Queue a job: the job, and whether it is new.

        With an idempotency key, the job queued before under the same key and kind comes back
        instead (a scheduler retry after a timeout must not double the work); the same key with
        other params is a ConflictError. A full queue is a TooManyRequestsError, and the scheduler
        comes back after ``Retry-After``. The lookup, the count and the add happen under one lock,
        so two request threads cannot both get past them.
        """
        params = dict(params)
        with self._submit_lock:
            if idempotency_key is not None:
                existing = self.store.find(kind, idempotency_key)
                if existing is not None:
                    if existing.params != params:
                        raise ConflictError(
                            f"Idempotency-Key {idempotency_key!r} was used for a different "
                            f"{kind} request"
                        )
                    return existing, False
            if self._queue.qsize() >= self.max_queued:
                raise TooManyRequestsError(
                    f"{self.max_queued} jobs are queued already; try again after Retry-After",
                    retry_after_s=RETRY_AFTER_S,
                )
            job = self._add(kind, params, idempotency_key=idempotency_key)
        return job, True

    def _add(
        self, kind: str, params: Mapping[str, Any], *, idempotency_key: str | None
    ) -> Job:
        """Record a queued job and put it on the queue."""
        job = Job(
            job_id=str(uuid7()),
            kind=kind,
            status=JobStatus.QUEUED,
            params=dict(params),
            created_at=_now(),
            idempotency_key=idempotency_key,
        )
        self.store.add(job)
        self._queue.put(job.job_id)
        logger.info("job %s %s queued", job.job_id, kind)
        return job

    def get(self, job_id: str) -> Job | None:
        return self.store.get(job_id)

    def recent(self, limit: int = 20) -> list[Job]:
        return self.store.recent(limit)

    def _loop(self) -> None:
        while not self._stopping.is_set():
            job_id = self._queue.get()
            if job_id is _STOP:
                break
            try:
                job = self._begin(job_id)
            except Exception:  # the worker outlives any one job's record trouble
                self._abandon(job_id)
                continue
            self._end(job, self._work(job))

    def _begin(self, job_id: str) -> Job:
        """Read the record and mark it running. Raises when the store cannot, or the record is
        gone (``_abandon`` then settles the record)."""
        job = self.store.get(job_id)
        if job is None:
            raise NickelConnectionError(f"job {job_id} has no record")
        self.store.start(job_id, _now())
        logger.info("job %s %s started", job_id, job.kind)
        return job

    def _abandon(self, job_id: str) -> None:
        """The job left the queue but could not start. Fail its record with a content-free
        reason, so a caller polling it (GET /jobs/{id}, an Idempotency-Key replay) sees a terminal
        state instead of ``queued`` until a restart. If even that write fails, say so and leave the
        record queued: the next start picks it up again."""
        logger.exception("job %s could not start", job_id)
        try:
            failed = self.store.fail(job_id, COULD_NOT_START, _now())
        except Exception:
            logger.exception(
                "job %s could not be failed either; it stays queued until the next start",
                job_id,
            )
            return
        if failed:
            logger.error("job %s failed: %s", job_id, COULD_NOT_START)
        else:
            logger.error("job %s has no open record to fail; dropped", job_id)

    def _work(self, job: Job) -> JobResult:
        handler = self._handlers.get(job.kind)
        try:
            if handler is None:
                raise NickelConnectionError(f"unknown job kind {job.kind!r}")
            return handler(job)
        except Exception as exc:  # a job never takes the worker down
            error = _describe(exc)
            logger.error("job %s %s failed: %s", job.job_id, job.kind, error)
            return JobResult(error=error)

    def _end(self, job: Job, result: JobResult) -> None:
        """Write the outcome. When the store cannot, the job ran but its record says running: say
        so, and let the next start fail it as interrupted (the scheduler queues it again; every
        job kind is safe to repeat)."""
        status = JobStatus.FAILED if result.error else JobStatus.SUCCEEDED
        counts = _counts_text(result.counts)
        try:
            self.store.finish(job.job_id, status, result.counts, result.error, _now())
        except Exception:
            logger.exception(
                "job %s %s %s (%s), but its record could not be written: it shows running "
                "until the next start fails it as %s",
                job.job_id,
                job.kind,
                status,
                counts,
                INTERRUPTED,
            )
            return
        logger.info("job %s %s %s: %s", job.job_id, job.kind, status, counts)


def build_runner(
    settings: ServiceSettings, *, republish_at_start: bool = True
) -> JobRunner:
    """The worker for this deployment. Unless told otherwise, it queues one republish of the open
    receipts when it starts (after the jobs a previous process left queued): the relay then runs
    at least once per start, a restart after a crash included, whether or not the scheduler calls
    it on its cadence yet (docs/jobs-api.md). Not without a tenant: the relay needs one.
    """
    handlers: dict[str, JobHandler] = {
        PULL_KIND: lambda job: run_pull(settings, job),
        REPUBLISH_KIND: lambda job: run_republish(settings, job),
        nango.PULL_KIND: lambda job: JobResult(nango.run_pull(settings, job)),
    }
    startup = []
    if republish_at_start and settings.tenant_id is not None:
        startup.append((REPUBLISH_KIND, {"source": None, "limit": None}))
    return JobRunner(
        settings,
        JobStore(DataLayout(settings.data_dir).jobs_db),
        handlers,
        startup=startup,
    )
