# Jobs API (NIK-2209-05)

Prepared by Divij N.

The contract between the scheduler (NIK-2209-03) and the connection layer: the scheduler queues a pull
(or a republish of what is still open), polls the job, and reads the counts. Four things and nothing
else: queue a pull, queue a republish, read a job, list the receipts. Everything runs inside the
service process: one worker thread takes the jobs one after another, in the order they were queued,
and the records live in `<NICKEL_DATA_DIR>/jobs.sqlite`.

## Every call needs the service token

The service is internal. Every route but the probes (`/health`, `/ready`) needs
`Authorization: Bearer <token>`, where the token is `NICKEL_SERVICE_TOKEN` (or the content of the
file `NICKEL_SERVICE_TOKEN_FILE` names; at least 32 characters, and the service does not start
without one). The scheduler and the Nickel API layer hold the same value from Vault. The check runs
before routing reads anything: a request without a valid token gets a 401 `unauthorized` with
`WWW-Authenticate: Bearer`, whatever its path, method or body, so nothing about the API (not even
whether a path exists, or what a body should look like) shows without the token. A missing token and
a wrong one get the same answer, the comparison is constant-time, and the presented value is never
logged or echoed.

```bash
export NICKEL_SERVICE_TOKEN=...   # the value the service was started with
curl -s -H "Authorization: Bearer $NICKEL_SERVICE_TOKEN" http://localhost:8000/jobs
```

Every error uses the service's one shape, `{"error": {"code", "message"}, "request_id"}`, with the
status codes below. Set `X-Request-ID` on a request to see it back in the response and the log.

## Queue a pull

`POST /jobs/documents/pull` pulls one document source (`app/manifests/documents/<manifest>.yaml`) into
the connection layer: original to the File Store, receipt, envelope to `raw_data`
(`docs/document-ingestion.md`). Re-pulling a source is idempotent (`duplicates` counts what was
already there).

```bash
curl -s -X POST http://localhost:8000/jobs/documents/pull \
  -H "Authorization: Bearer $NICKEL_SERVICE_TOKEN" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: sched-2026-09-23T10:00-hf-synthetic' \
  -d '{"manifest": "hf-synthetic-au-medical-docs"}'
# 202 {"job_id": "01a0cd4b-4956-7781-ab87-22f0a15ff421", "status": "queued"}
```

| Field | | |
|---|---|---|
| `manifest` | required | the manifest id: the file stem under `manifests/documents` (`[a-z0-9-]`, 3 to 63 characters) |
| `limit` | optional | stop after this many matching files (a smoke pull) |

| Status | When |
|---|---|
| 202 | queued; poll the job |
| 200 | an `Idempotency-Key` replay: the job queued before, with its status as it is now (below) |
| 401 `unauthorized` | no valid service token |
| 404 `not_found` | no manifest with that id |
| 409 `conflict` | the `Idempotency-Key` was used before with different parameters |
| 413 `too_large` | the request body is over 64 KiB (a pull request is a few hundred bytes); it is refused before it is read |
| 422 `validation_error` | a malformed id, `limit` < 1, an unknown field, or a malformed `Idempotency-Key` |
| 429 `too_many_requests` | the queue is full (`NICKEL_JOBS_MAX_QUEUED`, default 100; the running job is not counted). `Retry-After: 30` says when to come back; nothing is recorded |
| 503 `unavailable` | `NICKEL_TENANT_ID` is not set on this deployment |

### Idempotency-Key

A scheduler that retries after a timeout must not double the work. Send the same `Idempotency-Key`
(letters, digits, `.`, `_`, `:` and `-`, up to 64 characters) with the retry: the job queued before
under that key comes back with a 200 instead of a second job, its `status` whatever it is by then
(`queued`, `running`, `succeeded` or `failed`). The same key with other parameters is a 409. Keys are
per job kind, live in the job record (never in a log) and survive a restart. A replay is answered even
when the queue is full. Without a key, every request is a new job. A job that left the queue but could
not start (its record could not be read or marked running) is failed with the reason
`could not start`, so a replay sees a terminal state instead of `queued` until a restart.

## Queue a republish

`POST /jobs/documents/republish` publishes the tenant's open receipts again from the envelopes they
store, without going back to the sources (`docs/document-ingestion.md`, "Republishing open
receipts"): `received`, `publish_failed` and `held` ones, each only while its source's manifest is
enabled (found, cleared for this environment, out of shadow mode), as a pull requires.
Run it after a broker outage, after a crash mid-pull, or after a source leaves shadow mode. It takes
the same `Idempotency-Key`, and the same 200, 401, 409, 413, 422, 429 and 503 answers, as a pull
(no 404: it names no manifest).

**Cadence.** The scheduler (NIK-2209-03, PR #5) should queue a republish on a fixed interval, every
5 minutes, with an `Idempotency-Key` that names the slot (`republish-2026-09-24T10:05`), so a
retried call does not queue a second one. A run is cheap when nothing is open: it counts the open
receipts in the database and loads none. Until the scheduler is wired up, and after every restart
in any case, the service queues one republish itself when its job worker starts (after the jobs a
previous process left queued; not without `NICKEL_TENANT_ID`; and not while one from an earlier
start still waits, so a crash loop does not fill the queue with them), so a publish that failed
before a restart gets out without anyone asking.

```bash
curl -s -X POST http://localhost:8000/jobs/documents/republish \
  -H "Authorization: Bearer $NICKEL_SERVICE_TOKEN" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: sched-2026-09-23T10:05-republish' \
  -d '{}'
# 202 {"job_id": "01a0cd4b-5b1e-7c3a-9d02-3f8e6a1b7c44", "status": "queued"}
```

| Field | | |
|---|---|---|
| `source` | optional | only this source's receipts (a manifest id); every source otherwise |
| `limit` | optional | take up at most this many receipts, in the relay's order (`received`, then `publish_failed`, then `held`, oldest first within each): each one published, found published already, set aside or without an envelope counts, so one run examines at most this many however many stay open |

The job's `counts` are `open_receipts` (the received, publish_failed and held receipts open when the
run began), `published`, `duplicates` (published by another run meanwhile), `held_back` (open
receipts of a source that is not enabled: no manifest, not cleared for this environment, in shadow
mode, or a manifest that cannot be read; they wait, whatever their status), `bad_envelope` (the
stored envelope does not validate: set aside with the reason on the receipt, and the run goes on)
and `no_envelope` (receipts from before the envelope was kept on them; the next pull of their source
rebuilds it). A publish failure stops the job with `publish failed; the republish stopped`, and the
next republish carries on from where it stopped.

## Read a job

`GET /jobs/{job_id}` (422 `validation_error` for anything but a UUID, 404 `not_found` for an
unknown one):

```bash
curl -s -H "Authorization: Bearer $NICKEL_SERVICE_TOKEN" \
  http://localhost:8000/jobs/01a0cd4b-4956-7781-ab87-22f0a15ff421
```

```json
{
  "job_id": "01a0cd4b-4956-7781-ab87-22f0a15ff421",
  "kind": "documents.pull",
  "status": "succeeded",
  "params": {"manifest": "hf-synthetic-au-medical-docs", "limit": null},
  "created_at": "2026-09-23T08:04:18.390687+00:00",
  "started_at": "2026-09-23T08:04:18.397213+00:00",
  "finished_at": "2026-09-23T08:04:18.437686+00:00",
  "counts": {"listed": 108, "matched": 100, "published": 100, "held": 0, "duplicates": 0,
             "rejected": 0, "too_large": 0, "skipped": 0, "refused": 0, "new_objects": 84,
             "new_bytes": 52130221},
  "error": null
}
```

- `status`: `queued` -> `running` -> `succeeded` | `failed`. Poll until one of the last two.
- `kind`: `documents.pull` or `documents.republish`; `params` echoes the request. The job's id is
  also the `trace_id` on every envelope the run published (as 32 hex characters).
- `counts` of a pull (numbers only: never paths, file names or values; a republish's counts are
  listed above):
  - `listed`: files the source listed; `matched`: the ones the manifest's include and exclude
    patterns kept, up to the limit;
  - `published`: envelopes durable on `raw_data`; `held`: stored and receipted under shadow mode
    (the manifest's `shadow: true`), not published;
  - `duplicates`: items already received (the same tenant, source, message id and bytes), including
    the ones another run of the same source got to first;
  - `rejected`: empty, or not the declared format (final); `too_large`: over the size cap in force
    (the smaller of the manifest's `max_file_bytes` and `NICKEL_MAX_FILE_BYTES`): the file is not
    downloaded, or not read, and leaves a `too_large` receipt, metadata only, which a pull that gets
    it within a raised cap supersedes; `skipped`: the fetch failed; `refused`: downloads the egress
    rule stopped before the host was contacted (a download hop that is not https, that goes to a
    host the manifest does not name, or to a private address);
  - `new_objects`, `new_bytes`: what the File Store did not have yet.
- `failed` means the job itself could not run to the end: an unreachable source, an invalid manifest,
  a manifest not cleared for this environment, a publish failure (the pull or the republish stops
  there; its counts up to that point are kept), `could not start` (its record could not be read or
  marked running when its turn came), or `interrupted` (below).
- `error`: the reason when `failed`, otherwise `null`. Only the service's own messages are quoted,
  because they are content-free by contract. Any other exception is recorded by its type name alone:
  a foreign message may quote what it choked on (pydantic prints the rejected input, an `OSError` a
  path), and a clinic's file names can carry patient names. A pull that stopped at a publish failure
  says `publish failed; the pull stopped`, never at which file; a manifest the environment is not
  cleared for says so (`manifest phase 'dev' is not cleared for the production environment`), so the
  scheduler does not retry a publish that never happened. A manifest that does not load names the
  field and pydantic's error type (and the message of the manifest's own checks), never the value
  (`manifest does not validate: license string_too_short`), or the line and column of a YAML error.

`GET /jobs?limit=20` lists the newest jobs first (`limit` 1 to 200): `{"jobs": [...]}`.

## Restarts

Records survive a restart. At start-up, a job left `running` by the previous process becomes `failed`
with error `interrupted` (the scheduler queues it again), jobs still `queued` are picked up in
order, and then one `documents.republish` is queued unless one still waits ("Cadence", above). A
pull is safe to repeat: it skips what it already has. Run one server process (uvicorn's default):
each process would run its own worker, and mark the others' running jobs as interrupted at
start-up.

## Documents

`GET /documents?source=&status=&limit=20` lists the ingest receipts, newest first (`limit` 1 to 500;
`status` one of `received`, `held`, `publish_failed`, `too_large`, `superseded`, `published`,
`rejected`, which `docs/document-ingestion.md` explains). Receipt metadata only, and this deployment's tenant's only:
the connection layer never serves a document's content, and nothing derived from the content exists
here to serve. The original is in the File Store at `payload_ref`, for the Transform layer to read.
The receipt's message id (for a file source, the path at the source) is not returned: a clinic's file
names can carry patient names, and the ingest id and the payload hash identify the item.

```bash
curl -s -H "Authorization: Bearer $NICKEL_SERVICE_TOKEN" \
  'http://localhost:8000/documents?source=hf-synthetic-au-medical-docs&status=published&limit=1'
```

```json
{"documents": [{
  "ingest_id": "01a0cd4b-4978-7213-b525-246da3b6c4a0",
  "tenant_id": "0192a3b4-0000-7000-8000-000000000001",
  "source": "hf-synthetic-au-medical-docs",
  "payload_sha256": "cfa3181c...",
  "status": "published",
  "received_at": "2026-09-23T08:04:18.424362+00:00",
  "updated_at": "2026-09-23T08:04:18.434605+00:00",
  "payload_ref": "0192a3b4-0000-7000-8000-000000000001/documents/cf/cfa3181c...",
  "detail": null
}]}
```

`detail` is the service's own reason for a receipt that is not `published`: why it was rejected, the
size seen and the cap of one over it, that it is held for shadow mode, that its publish failed, that
its stored envelope was set aside, or which payload hash superseded it. A `too_large` receipt a pull
left for a file it never fetched has no payload hash to show: its `payload_sha256` and its
`payload_ref` are null (`docs/document-ingestion.md`, "Receipts").

## The same from the command line

`python -m src.cli documents pull <manifest.yaml>`, `documents republish [--source ID] [--limit N]`
and `documents status` run the same code in the foreground, without a job record and without the
token (`docs/document-ingestion.md`, `docs/docker.md`). A command-line run mints its own `trace_id`
and prints it with the summary.
