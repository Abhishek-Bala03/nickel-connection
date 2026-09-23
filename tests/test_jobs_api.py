"""The jobs API as the scheduler uses it (docs/jobs-api.md)."""

import logging
import sqlite3
import threading
import time
from uuid import UUID

import pytest

from src.db.jobs import Job, JobStatus, JobStore
from src.db.layout import DataLayout
from src.db.outbox import JsonlOutbox
from src.error import UnavailableError
from src.handler import documents
from src.handler.jobs import PULL_KIND, JobResult, JobRunner

STAMP = "2026-09-22T00:00:00+00:00"
RETRY_AFTER_S = "30"  # docs/jobs-api.md promises this value with a 429


@pytest.fixture
def held_pulls(monkeypatch):
    """Every pull job blocks until the returned event is set, so jobs pile up in the queue."""
    release = threading.Event()
    real_pull = documents.pull_documents

    def held(settings, manifest, *, limit=None, trace_id=None):
        release.wait(10)
        return real_pull(settings, manifest, limit=limit, trace_id=trace_id)

    monkeypatch.setattr(documents, "pull_documents", held)
    yield release
    release.set()


def _wait_until_running(client, job_id, timeout=5.0):
    deadline = time.monotonic() + timeout
    while client.get(f"/jobs/{job_id}").json()["status"] != "running":
        assert time.monotonic() < deadline
        time.sleep(0.01)


def test_a_full_queue_is_a_429_with_retry_after_and_no_record(
    service, settings, pdf_folder, manifest_writer, wait_for, held_pulls
):
    manifest_writer(pdf_folder, settings.manifest_dir)
    body = {"manifest": "local-docs"}
    with service(max_queued_jobs=2) as client:
        running = client.post("/jobs/documents/pull", json=body).json()["job_id"]
        _wait_until_running(client, running)  # out of the queue, on the worker
        queued = [
            client.post("/jobs/documents/pull", json=body).json()["job_id"]
            for _ in range(2)
        ]
        refused = client.post(
            "/jobs/documents/pull", json=body, headers={"X-Request-ID": "r-429"}
        )
        assert refused.status_code == 429
        assert refused.headers["Retry-After"] == RETRY_AFTER_S
        assert refused.json() == {
            "error": {
                "code": "too_many_requests",
                "message": "2 jobs are queued already; try again after Retry-After",
            },
            "request_id": "r-429",
        }
        assert len(client.get("/jobs").json()["jobs"]) == 3  # nothing was recorded
        held_pulls.set()
        for job_id in [running, *queued]:
            assert wait_for(client, job_id)["status"] == "succeeded"
        # Drained: the next one is taken again.
        assert client.post("/jobs/documents/pull", json=body).status_code == 202


def test_the_same_idempotency_key_returns_the_job_queued_before(
    service, settings, pdf_folder, manifest_writer, wait_for
):
    manifest_writer(pdf_folder, settings.manifest_dir)
    body = {"manifest": "local-docs"}
    key = {"Idempotency-Key": "sched-2026-09-23T10:00-pull"}
    with service() as client:
        first = client.post("/jobs/documents/pull", json=body, headers=key)
        assert first.status_code == 202
        job_id = first.json()["job_id"]
        wait_for(client, job_id)
        # The scheduler's retry after a timeout: the same job, its current status, nothing new.
        again = client.post("/jobs/documents/pull", json=body, headers=key)
        assert again.status_code == 200
        assert again.json() == {"job_id": job_id, "status": "succeeded"}
        assert len(client.get("/jobs").json()["jobs"]) == 1
        # No key: every request is a new job, as before.
        assert client.post("/jobs/documents/pull", json=body).json()["job_id"] != job_id


def test_an_idempotency_key_is_per_job_kind(tmp_path, settings):
    # The same key under another kind is another job, so two schedules that happen to pick the
    # same key never hand each other their jobs. Nothing is started: the queue just holds them.
    runner = JobRunner(
        settings,
        JobStore(tmp_path / "jobs.sqlite"),
        {"a": lambda params: JobResult(), "b": lambda params: JobResult()},
    )
    first, created = runner.submit("a", {}, idempotency_key="k")
    assert created
    other, created = runner.submit("b", {}, idempotency_key="k")
    assert created
    assert other.job_id != first.job_id
    again, created = runner.submit("a", {}, idempotency_key="k")
    assert (created, again.job_id) == (False, first.job_id)


def test_an_idempotency_key_reused_for_another_request_is_a_409(
    service, settings, pdf_folder, manifest_writer
):
    manifest_writer(pdf_folder, settings.manifest_dir)
    key = {"Idempotency-Key": "k-1"}
    with service() as client:
        client.post(
            "/jobs/documents/pull", json={"manifest": "local-docs"}, headers=key
        )
        response = client.post(
            "/jobs/documents/pull",
            json={"manifest": "local-docs", "limit": 1},
            headers=key,
        )
        assert response.status_code == 409
        assert response.json()["error"] == {
            "code": "conflict",
            "message": "Idempotency-Key 'k-1' was used for a different documents.pull request",
        }


@pytest.mark.parametrize("key", ["", "a" * 65, "has space", "tab\tkey"])
def test_a_malformed_idempotency_key_is_refused(service, key):
    with service() as client:
        response = client.post(
            "/jobs/documents/pull",
            json={"manifest": "local-docs"},
            headers={"Idempotency-Key": key},
        )
        assert response.status_code == 422
        assert response.json()["error"]["code"] == "validation_error"
        assert key not in response.text or key == ""


def test_a_replay_is_answered_even_when_the_queue_is_full(
    service, settings, pdf_folder, manifest_writer, held_pulls
):
    manifest_writer(pdf_folder, settings.manifest_dir)
    body = {"manifest": "local-docs"}
    with service(max_queued_jobs=1) as client:
        running = client.post("/jobs/documents/pull", json=body).json()["job_id"]
        _wait_until_running(client, running)
        key = {"Idempotency-Key": "k-queued"}
        queued = client.post("/jobs/documents/pull", json=body, headers=key)
        assert queued.status_code == 202
        assert client.post("/jobs/documents/pull", json=body).status_code == 429
        replay = client.post("/jobs/documents/pull", json=body, headers=key)
        assert (replay.status_code, replay.json()["job_id"]) == (
            200,
            queued.json()["job_id"],
        )
        held_pulls.set()  # let the worker finish before the lifespan stops it


def test_the_idempotency_key_survives_a_restart(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite")
    job = Job(
        "j-1",
        PULL_KIND,
        JobStatus.QUEUED,
        {"manifest": "x"},
        STAMP,
        idempotency_key="k",
    )
    store.add(job)
    assert JobStore(tmp_path / "jobs.sqlite").find(PULL_KIND, "k") == job
    assert store.find("documents.other", "k") is None
    assert store.find(PULL_KIND, "other") is None
    # One job per key and kind, whatever the caller does; no key, no constraint.
    with pytest.raises(sqlite3.IntegrityError):
        store.add(
            Job("j-2", PULL_KIND, JobStatus.QUEUED, {}, STAMP, idempotency_key="k")
        )
    store.add(Job("j-3", PULL_KIND, JobStatus.QUEUED, {}, STAMP))
    store.add(Job("j-4", PULL_KIND, JobStatus.QUEUED, {}, STAMP))


def test_job_records_from_before_the_idempotency_key_are_read_and_extended(tmp_path):
    path = tmp_path / "jobs.sqlite"
    with sqlite3.connect(path) as db:
        db.execute(
            "CREATE TABLE jobs (job_id TEXT PRIMARY KEY, kind TEXT NOT NULL, status TEXT NOT NULL, "
            "params TEXT NOT NULL, created_at TEXT NOT NULL, started_at TEXT, finished_at TEXT, "
            "counts TEXT NOT NULL, error TEXT)"
        )
        db.execute(
            "INSERT INTO jobs VALUES ('j-old', 'documents.pull', 'succeeded', '{}', ?, ?, ?, '{}', NULL)",
            (STAMP, STAMP, STAMP),
        )
    store = JobStore(path)
    assert store.get("j-old").idempotency_key is None
    store.add(Job("j-new", PULL_KIND, JobStatus.QUEUED, {}, STAMP, idempotency_key="k"))
    assert store.find(PULL_KIND, "k").job_id == "j-new"


def test_a_pull_job_runs_and_reports_its_counts(
    service, settings, pdf_folder, manifest_writer, wait_for
):
    manifest_writer(pdf_folder, settings.manifest_dir)
    with service() as client:
        response = client.post("/jobs/documents/pull", json={"manifest": "local-docs"})
        assert response.status_code == 202
        job_id = response.json()["job_id"]
        assert response.json() == {"job_id": job_id, "status": "queued"}
        job = wait_for(client, job_id)
        assert job["status"] == "succeeded"
        assert job["kind"] == "documents.pull"
        assert job["params"] == {"manifest": "local-docs", "limit": None}
        assert job["counts"] == {
            "listed": 1,
            "matched": 1,
            "published": 1,
            "duplicates": 0,
            "rejected": 0,
            "skipped": 0,
            "refused": 0,
            "held": 0,
            "new_objects": 1,
            "new_bytes": (pdf_folder / "a.pdf").stat().st_size,
        }
        assert job["error"] is None
        assert job["created_at"] <= job["started_at"]
        assert job["started_at"] <= job["finished_at"]
        [document] = client.get("/documents").json()["documents"]
        assert (document["source"], document["status"]) == ("local-docs", "published")


def test_a_job_stamps_its_id_as_the_trace_id_on_the_envelopes_it_publishes(
    service, settings, pdf_folder, manifest_writer, wait_for
):
    manifest_writer(pdf_folder, settings.manifest_dir)
    with service() as client:
        job_id = client.post(
            "/jobs/documents/pull", json={"manifest": "local-docs"}
        ).json()["job_id"]
        assert wait_for(client, job_id)["status"] == "succeeded"
    [message] = JsonlOutbox(DataLayout(settings.data_dir).outbox).read("raw_data")
    # The observability task propagates trace_id from the envelope on: the job is the run.
    assert message["value"]["trace_id"] == UUID(job_id).hex
    assert message["value"]["attempt"] == 1


def test_a_pull_limit_is_honoured(
    service, settings, pdf_folder, pdf_bytes, manifest_writer, wait_for
):
    (pdf_folder / "b.pdf").write_bytes(pdf_bytes + b"x")
    manifest_writer(pdf_folder, settings.manifest_dir)
    with service() as client:
        body = {"manifest": "local-docs", "limit": 1}
        job_id = client.post("/jobs/documents/pull", json=body).json()["job_id"]
        job = wait_for(client, job_id)
        assert job["params"]["limit"] == 1
        assert job["counts"]["matched"] == 1


def test_an_unknown_manifest_is_a_404(service):
    with service() as client:
        response = client.post(
            "/jobs/documents/pull",
            json={"manifest": "nope"},
            headers={"X-Request-ID": "r-1"},
        )
        assert response.status_code == 404
        assert response.json() == {
            "error": {"code": "not_found", "message": "no document manifest 'nope'"},
            "request_id": "r-1",
        }
        assert client.get("/jobs").json() == {"jobs": []}


@pytest.mark.parametrize(
    "body",
    [
        {"manifest": "../../etc/passwd"},
        {"manifest": "Bad ID"},
        {"manifest": "local-docs", "limit": 0},
        {"manifest": "local-docs", "surprise": 1},
        {},
    ],
)
def test_a_malformed_pull_request_is_refused(service, body):
    with service() as client:
        response = client.post("/jobs/documents/pull", json=body)
        assert response.status_code == 422
        assert response.json()["error"]["code"] == "validation_error"


def test_a_pull_needs_a_tenant(service, settings, pdf_folder, manifest_writer):
    manifest_writer(pdf_folder, settings.manifest_dir)
    with service(tenant_id=None) as client:
        response = client.post("/jobs/documents/pull", json={"manifest": "local-docs"})
        assert response.status_code == 503
        assert response.json()["error"] == {
            "code": "unavailable",
            "message": "NICKEL_TENANT_ID is not set (a UUID; see .env.example)",
        }


def test_an_unreachable_source_fails_the_job(
    service, settings, tmp_path, manifest_writer, wait_for
):
    manifest_writer(tmp_path / "missing", settings.manifest_dir)
    with service() as client:
        job_id = client.post(
            "/jobs/documents/pull", json={"manifest": "local-docs"}
        ).json()["job_id"]
        job = wait_for(client, job_id)
        assert job["status"] == "failed"
        # The locator and the failure, never the folder's path: GET /jobs returns this.
        assert job["error"] == "local missing: not a folder"
        assert job["counts"] == {}


def test_an_aborted_pull_fails_the_job_with_its_counts_but_not_the_file_name(
    service, settings, pdf_folder, manifest_writer, wait_for, records
):
    # A clinic's file names can carry patient names: the job says a publish failed, not where.
    (pdf_folder / "a.pdf").rename(pdf_folder / "Jane-Roe-labs.pdf")
    manifest_writer(pdf_folder, settings.manifest_dir)
    settings.data_dir.mkdir()
    (settings.data_dir / "outbox").write_text(
        "a file where the outbox folder should be"
    )
    with service() as client:
        job_id = client.post(
            "/jobs/documents/pull", json={"manifest": "local-docs"}
        ).json()["job_id"]
        job = wait_for(client, job_id)
        assert job["status"] == "failed"
        assert job["error"] == "publish failed; the pull stopped"
        assert job["counts"]["matched"] == 1
    assert all("Jane-Roe" not in r.getMessage() for r in records)


def test_a_manifest_not_cleared_for_the_environment_fails_the_job_saying_so(
    service, settings, pdf_folder, manifest_writer, wait_for
):
    manifest_writer(pdf_folder, settings.manifest_dir, phase="dev")
    with service(environment="production") as client:
        job_id = client.post(
            "/jobs/documents/pull", json={"manifest": "local-docs"}
        ).json()["job_id"]
        job = wait_for(client, job_id)
        assert job["status"] == "failed"
        # The truth, so the scheduler does not retry a publish that never happened.
        assert job["error"] == (
            "manifest phase 'dev' is not cleared for the production environment"
        )
        assert (job["counts"]["listed"], job["counts"]["published"]) == (0, 0)


def test_an_invalid_manifest_fails_the_job(
    service, settings, pdf_folder, manifest_writer, wait_for
):
    manifest_writer(pdf_folder, settings.manifest_dir, license="")
    with service() as client:
        job_id = client.post(
            "/jobs/documents/pull", json={"manifest": "local-docs"}
        ).json()["job_id"]
        job = wait_for(client, job_id)
        assert job["status"] == "failed"
        # pydantic's error quotes the input: a foreign error is its type alone (the CLI shows why).
        assert job["error"] == "ValidationError"


def test_jobs_run_one_after_another_and_list_newest_first(
    service, settings, pdf_folder, manifest_writer, wait_for
):
    manifest_writer(pdf_folder, settings.manifest_dir)
    with service() as client:
        body = {"manifest": "local-docs"}
        first = client.post("/jobs/documents/pull", json=body).json()["job_id"]
        second = client.post("/jobs/documents/pull", json=body).json()["job_id"]
        first_job, second_job = wait_for(client, first), wait_for(client, second)
        assert second_job["started_at"] >= first_job["finished_at"]
        assert second_job["counts"]["duplicates"] == 1  # the first pull got there first
        listed = client.get("/jobs").json()["jobs"]
        assert [job["job_id"] for job in listed] == [second, first]
        assert client.get("/jobs?limit=1").json()["jobs"] == [listed[0]]
        assert client.get("/jobs?limit=0").status_code == 422


def test_an_unknown_job_is_a_404(client):
    unknown = "01900000-0000-7000-8000-000000000000"
    response = client.get(f"/jobs/{unknown}")
    assert response.status_code == 404
    assert response.json()["error"] == {
        "code": "not_found",
        "message": f"no job {unknown}",
    }


def test_a_job_id_that_is_not_a_uuid_is_a_422_and_is_not_echoed(client):
    # Before, any string was looked up and quoted back in the 404, newlines and all.
    response = client.get("/jobs/nope%0aSECRET-MARKER%0a" + "x" * 3000)
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"
    assert "SECRET-MARKER" not in response.text


def test_jobs_left_by_the_previous_process_are_interrupted_or_picked_up(
    service, settings, pdf_folder, manifest_writer, records, wait_for
):
    manifest_writer(pdf_folder, settings.manifest_dir)
    store = JobStore(DataLayout(settings.data_dir).jobs_db)
    # Hand-made records, in the UUID form the API takes (GET /jobs/{id} refuses anything else).
    running, queued, odd = (
        "01900000-0000-7000-8000-00000000000a",
        "01900000-0000-7000-8000-00000000000b",
        "01900000-0000-7000-8000-00000000000c",
    )
    store.add(Job(running, PULL_KIND, JobStatus.RUNNING, {"manifest": "x"}, STAMP))
    store.add(
        Job(queued, PULL_KIND, JobStatus.QUEUED, {"manifest": "local-docs"}, STAMP)
    )
    store.add(Job(odd, "documents.dance", JobStatus.QUEUED, {}, STAMP))
    with service() as client:
        interrupted = client.get(f"/jobs/{running}").json()
        assert (interrupted["status"], interrupted["error"]) == (
            "failed",
            "interrupted",
        )
        assert interrupted["finished_at"] is not None
        assert wait_for(client, queued)["status"] == "succeeded"
        odd_job = wait_for(client, odd)
        assert (odd_job["status"], odd_job["error"]) == (
            "failed",
            "unknown job kind 'documents.dance'",
        )
    messages = [r.getMessage() for r in records if r.levelno >= logging.WARNING]
    assert (
        "1 job(s) left running by the previous process failed as interrupted"
        in messages
    )


def test_shutdown_does_not_wait_for_a_long_job(tmp_path, settings, records):
    release = threading.Event()
    started = threading.Event()

    def slow(params):
        started.set()
        release.wait(10)
        return JobResult({"done": 1})

    runner = JobRunner(settings, JobStore(tmp_path / "jobs.sqlite"), {"slow": slow})
    runner.start()
    job, _ = runner.submit("slow", {})
    assert started.wait(5)
    runner.stop(timeout=0.05)
    assert runner.get(job.job_id).status is JobStatus.RUNNING
    assert any("still running at shutdown" in r.getMessage() for r in records)
    release.set()
    runner.stop()
    assert runner.get(job.job_id).counts == {"done": 1}


def _flaky_get(store, broken):
    """A ``get`` whose first lookup fails, as a locked database would; ``broken`` records it."""
    real_get = store.get

    def get(job_id):
        if not broken:
            broken.append(job_id)
            raise sqlite3.OperationalError("database is locked")
        return real_get(job_id)

    return get


def test_a_job_whose_record_cannot_be_read_is_failed_and_the_worker_goes_on(
    tmp_path, settings, monkeypatch, records
):
    """Before, the job stayed queued in the store but had left the in-memory queue, so a replay
    by Idempotency-Key polled "queued" until a restart. Now it is failed with a content-free
    reason, and the replay sees that."""
    runner = JobRunner(
        settings,
        JobStore(tmp_path / "jobs.sqlite"),
        {"ok": lambda job: JobResult({"done": 1})},
    )
    real_get = runner.store.get
    broken: list[str] = []
    monkeypatch.setattr(runner.store, "get", _flaky_get(runner.store, broken))
    runner.start()
    first, _ = runner.submit("ok", {}, idempotency_key="sched-1")
    second, _ = runner.submit("ok", {})
    _wait_status(runner, second.job_id, JobStatus.SUCCEEDED)
    assert broken == [first.job_id]
    failed = real_get(first.job_id)
    assert (failed.status, failed.error) == (JobStatus.FAILED, "could not start")
    assert failed.finished_at is not None
    replay, created = runner.submit("ok", {}, idempotency_key="sched-1")
    assert (created, replay.status) == (False, JobStatus.FAILED)  # not queued for ever
    runner.stop()
    errors = [r for r in records if r.levelno == logging.ERROR]
    assert [r.getMessage() for r in errors] == [
        f"job {first.job_id} could not start",
        f"job {first.job_id} failed: could not start",
    ]
    assert errors[0].exc_info[0] is sqlite3.OperationalError


def test_a_job_that_cannot_start_nor_be_failed_stays_queued_for_the_next_start(
    tmp_path, settings, monkeypatch, records
):
    store = JobStore(tmp_path / "jobs.sqlite")
    runner = JobRunner(settings, store, {"ok": lambda job: JobResult({"done": 1})})
    real_get = store.get
    monkeypatch.setattr(runner.store, "get", _flaky_get(store, []))

    def broken_fail(*args, **kwargs):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(runner.store, "fail", broken_fail)
    runner.start()
    job, _ = runner.submit("ok", {})
    deadline = time.monotonic() + 5
    while len([r for r in records if r.levelno == logging.ERROR]) < 2:
        assert time.monotonic() < deadline
        time.sleep(0.01)
    runner.stop()
    could_not_start, could_not_fail = (r for r in records if r.levelno == logging.ERROR)
    assert could_not_start.getMessage() == f"job {job.job_id} could not start"
    assert could_not_fail.getMessage() == (
        f"job {job.job_id} could not be failed either; it stays queued until the next start"
    )
    assert could_not_fail.exc_info[0] is sqlite3.OperationalError
    assert real_get(job.job_id).status is JobStatus.QUEUED
    # The next start picks it up again, and this time it runs.
    monkeypatch.undo()
    again = JobRunner(settings, store, {"ok": lambda job: JobResult({"done": 1})})
    again.start()
    assert _wait_status(again, job.job_id, JobStatus.SUCCEEDED).counts == {"done": 1}
    again.stop()


def test_a_queued_id_whose_record_is_gone_is_dropped(tmp_path, settings, records):
    store = JobStore(tmp_path / "jobs.sqlite")
    runner = JobRunner(settings, store, {"ok": lambda job: JobResult({"done": 1})})
    gone, _ = runner.submit("ok", {})
    with sqlite3.connect(tmp_path / "jobs.sqlite") as db:
        db.execute("DELETE FROM jobs WHERE job_id = ?", (gone.job_id,))
    runner.start()
    job, _ = runner.submit("ok", {})
    _wait_status(runner, job.job_id, JobStatus.SUCCEEDED)
    runner.stop()
    assert store.get(gone.job_id) is None
    assert [r.getMessage() for r in records if r.levelno == logging.ERROR] == [
        f"job {gone.job_id} could not start",
        f"job {gone.job_id} has no open record to fail; dropped",
    ]


def _wait_status(runner, job_id, status, timeout=5.0):
    deadline = time.monotonic() + timeout
    while runner.get(job_id).status is not status:
        assert time.monotonic() < deadline, runner.get(job_id)
        time.sleep(0.01)
    return runner.get(job_id)


def test_a_job_that_raises_a_service_error_keeps_its_message(tmp_path, settings):
    def broken(params):
        raise UnavailableError("the tenant is not set")

    runner = JobRunner(settings, JobStore(tmp_path / "jobs.sqlite"), {"broken": broken})
    runner.start()
    job, _ = runner.submit("broken", {"a": 1})
    failed = _wait_status(runner, job.job_id, JobStatus.FAILED)
    runner.stop()
    assert failed.error == "the tenant is not set"
    assert failed.params == {"a": 1}


def test_a_job_that_raises_a_foreign_error_keeps_only_its_type(
    tmp_path, settings, records
):
    quoted = (
        "input_value='Hb 13.4 g/dL, Jane Roe'"  # what pydantic or an OSError may say
    )

    def broken(params):
        raise ValueError(f"1 validation error: {quoted}")

    runner = JobRunner(settings, JobStore(tmp_path / "jobs.sqlite"), {"broken": broken})
    runner.start()
    job, _ = runner.submit("broken", {})
    failed = _wait_status(runner, job.job_id, JobStatus.FAILED)
    runner.stop()
    assert failed.error == "ValueError"
    assert all("Jane Roe" not in r.getMessage() for r in records)


def test_a_job_whose_record_cannot_be_written_is_logged_truthfully_and_interrupted_next_start(
    tmp_path, settings, monkeypatch, records
):
    store = JobStore(tmp_path / "jobs.sqlite")
    runner = JobRunner(settings, store, {"ok": lambda params: JobResult({"done": 1})})

    def broken_finish(*args, **kwargs):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(runner.store, "finish", broken_finish)
    runner.start()
    job, _ = runner.submit("ok", {})
    deadline = time.monotonic() + 5
    while not [r for r in records if r.levelno == logging.ERROR]:
        assert time.monotonic() < deadline
        time.sleep(0.01)
    runner.stop()
    [error] = [r for r in records if r.levelno == logging.ERROR]
    assert error.getMessage() == (
        f"job {job.job_id} ok succeeded (done 1), but its record could not be written: "
        "it shows running until the next start fails it as interrupted"
    )
    assert error.exc_info[0] is sqlite3.OperationalError
    assert store.get(job.job_id).status is JobStatus.RUNNING
    # The next start ends it: the scheduler sees a terminal state and queues it again.
    again = JobRunner(settings, JobStore(tmp_path / "jobs.sqlite"), {})
    again.start()
    again.stop()
    ended = again.get(job.job_id)
    assert (ended.status, ended.error) == (JobStatus.FAILED, "interrupted")
    assert ended.finished_at is not None
