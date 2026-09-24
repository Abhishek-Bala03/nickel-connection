# Document ingestion (NIK-2209-05)

Prepared by Divij N.

Health-related PDFs come into the platform through the **document connector** of this service, the
connection layer. It follows the Layer 1 rule of the architecture: fetch the payload, keep the original
unchanged, record that it arrived, and hand an envelope to the Transform layer on `raw_data`. The
connection layer ends there. It does not read, parse, OCR, extract or judge the content, and it serves
none of it; what happens to a document after `published` is the Transform layer's, in a service of its
own ("Where parsing and extraction went", below).

## How it works

```
source (Hugging Face | web link | Kaggle | folder)
  └─ DocumentConnector ── reads the source's manifest (fetch, phase, shadow, raw_retention, patterns),
       │                  lists the files, keeps the ones the manifest wants, fetches each one;
       │                  a file over the size cap is not fetched: it leaves a too_large receipt
       └─ IngestBridge
            1. edge checks only: not empty, under the size cap, really a PDF (%PDF- header)
            2. original → File Store, key <tenant>/documents/<sha256[:2]>/<sha256>   (write-once)
            3. receipt  → ingest_receipts, the envelope kept on it (received → published | publish_failed;
                          held; too_large → superseded; rejected)
            4. envelope → raw_data (key = patient id when known, else the file hash)
```

- **Transport retry suppression, not dedupe.** A receipt is unique per tenant, source, message id
  and file hash. Pulling the same source again finds the published receipt and publishes nothing
  new; the same bytes under two names are stored once (content addressing). That is retry
  suppression at the transport: dedupe proper stays the next layer's job (D-00, on the envelope's
  `idempotency_key`), and a message can still reach `raw_data` twice (below, "Delivery"). Two pulls
  of one item at once are settled by the receipt's unique index: the pull whose insert loses counts
  the item as a duplicate, and the winner publishes it.
- **Tenant first.** Every object key starts with the tenant id, built by one function
  (`src.utility.keys.object_key`), as the multi-tenancy guidance asks. Tenant ids must be UUIDs,
  and every receipt read names the tenant it is for.
- **Nothing half-done.** An item counts as published only once its envelope is durable (the `acks=all`
  rule). A failed publish is recorded on the receipt and stops the run; re-running retries it under
  the same ingest id, and the republish relay gets it out without the source.
- **Counts and ids only.** Job records, the document listing and the logs carry numbers, ids, hashes
  and error types: never a document's content, its values, its file name or its path. The listing
  names an item by its ingest id and payload hash, not by the receipt's message id, which for a file
  source is the path at the source; the log names an item by its ingest id (one DEBUG line per
  item), a failed source by its locator and the error's type, and a failed outbox write by the
  error's type and the OS's reason, never the file. The command line prints the paths of the files
  a pull had problems with only when asked (`-v`): in a container, what it prints is the log stream.

### Receipts

One row per payload received, or per item a pull found over the size cap, and how far it got
(`src/db/receipts.py`; `GET /documents` lists them):

| Status | Meaning | Final? |
|---|---|---|
| `received` | the original is in the File Store; the envelope is not out yet | no: moves to `published` or `publish_failed` |
| `held` | stored and receipted under shadow mode, not published | no: published once the manifest leaves shadow mode, under the same ingest id |
| `publish_failed` | the envelope could not be made durable (`detail` names the error's type; the OS message, which names the outbox file, is in the log) | no: retried under the same ingest id on the next run |
| `too_large` | over the size cap in force (the smaller of the manifest's `max_file_bytes` and `NICKEL_MAX_FILE_BYTES`); nothing stored, no envelope; `detail` gives the size seen and the cap | no: a raised cap lets it through (below) |
| `superseded` | a pull's `too_large` receipt whose item has since come in within the cap; `detail` names the payload hash of the receipt that took over | for the relay and every status update, yes; a pull that finds the item over the cap again, at another size than it came in at, reopens it as `too_large` |
| `published` | the envelope is durable on `raw_data` | yes |
| `rejected` | empty, or not a declared format: the same bytes can never pass | yes |

A `too_large` receipt comes from one of two places:

- **A pull**, for a file it skips for size before it has the bytes: one listed over the cap (never
  downloaded), a download that passes the cap while it is spooled (stopped there), or a file an SDK
  put on disk over it (measured, never read). Its detail is `<size> bytes; the cap is <cap>`, or
  `over the <cap>-byte cap; the download stopped there` when no size is known. There is no content
  hash to identify it by, so its `payload_sha256` holds a surrogate:
  sha256 over `too_large`, the tenant, the source and the message id, NUL-separated
  (`src.db.receipts.too_large_key`). That makes it one receipt per item at the source: a repeated pull
  finds it and adds no row (a new size or cap only updates its detail), and the size is left out of
  the key on purpose, since a download that stops at the cap never learns it. When a later pull gets
  the item's bytes within the cap (the cap raised, or a smaller version at the same path), the item
  gets its own receipt, keyed by its hash, and is stored and published as usual; the `too_large`
  receipt is then `superseded`, never left open. If the item is over the cap again later, the same
  receipt reopens as `too_large` (still one row), unless it is seen at the size it came in at: a cap
  lowered after the item was delivered leaves the marker superseded, since it is most likely the
  same item. Both moves filter on the surrogate. A payload hash equals it only for bytes built as its
  exact preimage, and then the pull just carries the marker's row through as that item's receipt,
  its key by then really the payload's hash: harmless. `GET /documents` and `documents status` show
  a marker's `payload_sha256` as null ("-"), since it is not the hash of any bytes; the surrogate is
  what the row is stored under in the SQLite stand-in ("What is temporary", below).
  [Decision record 0002](decisions/0002-too-large-receipts-for-pulls.md) (Proposed) records the
  marker, its key and the `superseded` status.
- **The bridge's own check**, on a payload in hand that is over its cap (a pushed upload would meet
  it; a pull stops such a file first, since the connector's cap is never larger than the bridge's).
  It is keyed by the payload's hash, and once the cap is raised the same bytes go through under the
  same ingest id.

Neither a `rejected` nor a `too_large` receipt keeps the bytes. Keeping them in a quarantine class,
for inspection and replay with `raw.dlq`, as the architecture asks for edge rejects and oversize
payloads, is nickel-connection issue #4; a pull's `too_large` receipt is what such a replay hangs on.

Only the open statuses move: a late or stale update cannot undo a `published` or a `rejected`. The
receipts file is shared by the job worker, the API threads and the CLI (SQLite in WAL mode with a busy
timeout), and every read names the tenant it is for: the API and the CLI read as the deployment's
tenant, and a read without one is refused, so another tenant's rows in the same file never show.

Two more columns carry delivery. `attempts` counts the publish attempts so far (counted before each
attempt, so a crash mid-publish counts too), and `envelope` keeps the envelope as first built, written
in the same insert or update that records `received` or `held`, and never replaced after that (a
later write leaves it as it is: `COALESCE(envelope, ?)`). A retry, a held item promoted later, or the
republish relay publishes that stored envelope, stamped with the attempt it is and the run that makes
it, so every attempt is the same message; `received_at` stays the time the item was first received,
never the retry's clock. (A stored envelope that no longer reads is set aside by the relay; a pull of
the item, which has the bytes, publishes an envelope it builds again instead.) `rejected` and
`too_large` receipts store no envelope, because nothing was stored to point at. A file from before
these columns gets them at open.

### The envelope

`raw_data` carries one envelope per message (`src/models/envelope.py`, v0.3; `python -m src.cli
schema envelope` prints the reader's JSON Schema). v0.3 adds the agreed envelope's three nullable
fields `member_id`, `device_id` and `fhir_release`, takes the contract's ingestion modes (`pull`,
`stream`, `listener`, `upload`, `mobile`; no `push`: Phase 1 has no inbound webhooks), and names the
payload's sha256 `payload_hash`, as the contract does (v0.2 said `payload_sha256`).

Versions: a reader takes any `0.x` (`^0\.\d+$`) and ignores the fields it does not know, so an
additive minor version is not rejected by the readers before it; another major version is refused.
The service builds its envelopes with a strict producer model (`ProducedEnvelope`: unknown fields
forbidden, so a misspelled one is an error, not a field silently dropped; the current version
only). A message goes out in the current version: the relay reads a stored v0.2 envelope through
the reader, which still takes `payload_sha256` as `payload_hash`, and publishes it as v0.3 (the same
values, the new fields null). That is the one older version it upgrades. A stored envelope newer
than the version this service writes (written before a rollback) is never relabelled, which would
drop the fields this reader does not know: the relay sets it aside (`stored envelope is newer
(0.4)`), and a pull of the item publishes an envelope it builds itself. The service that consumes
`raw_data` (nickel-transform) reads 0.x tolerantly too. What the document connector puts in each
field:

| Field | From |
|---|---|
| `envelope_version` | `0.3` |
| `ingest_id` | the receipt's id (UUIDv7), minted when the item was first received; the message's id on `raw_data`, the same on every attempt |
| `tenant_id` | the deployment's tenant (`NICKEL_TENANT_ID`) |
| `member_id` | null: the Nickel member, when a producer knows it (the mobile edge knows its signed-in member); identity resolution (Layer 3) decides otherwise |
| `source` | the manifest id |
| `fetch` | the manifest's `fetch`: how the item was fetched (`kaggle`, `huggingface`, `http`, `local`). What v0.1 called `route` |
| `route` | the manifest's `route`: the lab route (L1 to L9) the fidelity ranking reads. Every shipped document manifest says `L7`, the lowest tier |
| `mode` | the manifest's `mode` (`pull`; the envelope also takes `stream`, `listener`, `upload`, `mobile`) |
| `device_id` | null: the device a reading came from (a wearable, a CGM, a phone), for device data |
| `fhir_release` | null: the FHIR release (R4, R4B, R5, ...) of a FHIR payload |
| `connector`, `connector_version` | `documents` and its version |
| `message_id` | the item's stable id at the source: the URL for web links, `<fetch>:<locator>/<path>` otherwise |
| `idempotency_key` | sha256 over tenant, source, message id and payload hash, NUL-separated: the dedup key (D-00), the same on every attempt of one receipt. Tenant-first because keys are; the payload hash because new bytes under the same path are a new version, not a repeat. Held as it is until [decision record 0001](decisions/0001-idempotency-key.md) (Proposed) is decided: it recommends D-00's formula with the document's version in its message id instead |
| `trace_id` | the run that published the message: the job's id (as hex) for a job run, a fresh id for a command-line run (printed with the summary) |
| `attempt` | the receipt's publish attempts, this one included |
| `content_type` | `application/pdf` (the sniffed format) |
| `received_at` | when the receipt was first written; a retry keeps it |
| `retention_class` | `documents` |
| `schema_id`, `consent_ref` | null: a file by pointer has no registered schema, and public datasets carry no consent |
| `quality.flags` | `[]`: this layer flags nothing |
| `payload_hash`, `payload_bytes`, `payload_ref` | the original's sha256, size and File Store key (`payload`, the inline form, is unused for documents) |
| `source_meta` | the item's locator, path, URL and licence |

`connection_id`, `event_time`, `source_patient_id`, `member_id`, `device_id` and `fhir_release` are
always null from the document connector: public datasets have no Nango connection, no patient id and
no event time, and a PDF has no member, device or FHIR release. A repeat of a message carries the
same `ingest_id` and `idempotency_key` and a higher `attempt`, so a consumer can drop it.

### Republishing open receipts

An item the source stops listing must still get out. `documents republish` (a job,
`POST /jobs/documents/republish` in `docs/jobs-api.md`, which the scheduler queues every 5 minutes
and the service once whenever its job worker starts; from the command line,
`python -m src.cli documents republish [--source ID] [--limit N]`) takes the tenant's open receipts
by status (`received`, then `publish_failed`, then `held`), oldest first within each, and publishes
the envelope each one stores: the same ingest id and dedup key, the next attempt number, the relay
run's `trace_id`, and no source needed.

- Every open receipt obeys the rules a pull obeys, whatever its status: its source's manifest is
  found (and is that source's: a file whose `id` differs is not), cleared for this environment and
  out of shadow mode, the "enable" step of the Connections flow. Otherwise it waits, counted as
  `held_back`: a source whose manifest was removed, set back to `shadow: true`, or not cleared for
  this environment (a `dev` source in production) sends nothing. A manifest that cannot be read
  holds back its own source only; the log names the source and what is wrong, content-free (the
  manifest's error, or an OS error's type).
- `received` (a crash between the File Store write and the publish) and `publish_failed` (the broker
  was down) go out again, and `held` (shadow mode) goes out once its source leaves shadow mode.
- A stored envelope that does not validate (a corrupt row, a shape this reader does not take) is set
  aside, not raised: it is counted as `bad_envelope`, the receipt keeps its status and records why
  (`stored envelope set aside:` and pydantic's error types with the fields they are on, never a
  value), and the relay goes on to the next receipt. The write is the detail alone, made only while
  the receipt is still in the status the relay read (a run that moved it meanwhile wins) and only
  when the reason is new, so a run that meets the same bad envelope again writes and logs nothing.
  The next pull of the item publishes an envelope it builds again; the stored one stays as it was.
  Before, one such row raised out of every run and everything behind it stayed stuck.
- `too_large` is not the relay's business: no original was stored, so it waits for the source to list
  the item again once the cap is raised.
- A receipt from before the envelope was kept on it is counted (`no_envelope`), not guessed at: the
  next pull of its source builds one.

The relay counts the open receipts by status and source in the database first, and reads each
source's manifest once; the receipts of a source that may not publish are counted, never loaded. The
others are read a page at a time: keyset pages of 256 on `(received_at, rowid)` over the index
`(tenant_id, status, received_at)`, with the enabled sources filtered in the query, so memory holds
one page however many receipts are open. A `limit` bounds every receipt a run takes up (published,
found published already, set aside, or without an envelope), so one run examines at most that many,
however many stay open. `open_receipts` in its counts is how many were open when it began; the other
counts say what became of them (a receipt another run moved on meanwhile is counted open, not taken
up).

A publish failure stops the relay as it stops a pull: the receipt says `publish_failed`, and the next
run picks it up. Nothing is republished across tenants.

### The manifest governs the run

One YAML manifest per source in `app/manifests/documents/` (`src/connectors/documents/manifest.py`).
The shape is a subset of the connector manifest contract (CN-01), with its nesting for the fields
that exist here: `auth: {type, vault_path}` and `contract: {baa, dpa}`, both keys always given (a
null is a statement, not an omission), and `schedule: {class}`. `endpoints`, `rate_limit`,
`expectations`, `schedule`'s other keys and `formats` in CN-01's shape are not here yet ("What is
temporary", below); a document source has a source block (`kaggle`, `huggingface`, `http` or
`local`) in place of endpoints. CI fails if a manifest is missing `contract`, `auth`, `phi`, `owner`
or `license`. The fields are read, not only validated:

- `fetch`: the transport, `kaggle`, `huggingface`, `http` or `local`. It decides the source block
  and the message ids. (`route` is a different thing: the lab route the fidelity ranking reads,
  which the envelope carries.)
- `auth.type` must fit `fetch`: `kaggle_token` for kaggle, `hf_token` for a gated Hugging Face repo
  and `none` for a public one, `none` for web links and folders. `auth.vault_path` is where the
  connector runner (CN-09) will read the credential; until then the SDKs read `KAGGLE_API_TOKEN` and
  `HF_TOKEN` from the environment themselves, and every shipped manifest says `vault_path: null`.
  A token from the environment is for dev sources only: a `kaggle_token` or `hf_token` manifest in
  any other phase must name its `auth.vault_path`, or it is refused at load.
- `phi` and `contract`: a manifest that says `phi: true` without a `contract.baa` is refused at
  load. The shipped manifests are public data: `phi: false`, no agreement claimed.
- `owner`: who answers for the source, for the registrar and monitoring (CN-05); not read here.
- `phase`: how far the source is cleared. A `dev` manifest runs in the `development` environment only,
  `qa` in development and qa, `prod` anywhere. The service's environment is `NICKEL_ENV` (production
  unless set); a manifest the environment is not cleared for is refused before its source is listed,
  and the job fails saying so. Every shipped manifest is `dev`. `fetch: kaggle` and
  `fetch: huggingface` are `dev` only (a manifest that clears one for qa or prod is refused at
  load): their SDKs download on their own, outside the egress rule (the host allow-list and the
  pinned address), until they go through it. They only run in an image built with `DEV_SOURCES=1`.
- `schedule`: CN-01's shape, `{class: F1..F5}` (F1 continuous, F2 daily summary, F3 episodic,
  F4 on demand, F5 backfill); every shipped manifest says `F4`, the default. The scheduler
  (NIK-2209-03) reads it; `interval`, `overlap` and `backfill` come with it. Deprecated: the
  string form from before, `schedule: on_demand`, still reads as `{class: F4}` in this release,
  with a warning in the log at every load; the next release refuses it. Any other string is
  refused now. Write `schedule: {class: F4}`.
- `shadow` (default `true`): the "sandbox test > shadow mode > enable" flow of the Connections UI. In
  shadow mode the run is real: the originals are stored and receipted as `held`, and nothing is
  published. Once the manifest says `shadow: false`, the next run publishes the held items under the
  ingest ids they were held under.
- `raw_retention`: `forever`, or a number of days such as `90d` (1 to 3650). The originals go to the
  `documents` object class either way (the class is in every object key and envelope). The value
  stays with the source: an object is shared by every receipt that names its bytes (two sources
  can deliver the same PDF), so it carries facts about the content only, and expiry keeps an
  object until the longest `raw_retention` among the receipts that point at it has passed. That
  is the File Store lifecycle rule's job, not this connector's: nickel-connection issue #6
  ("Expire stored originals by raw_retention (File Store lifecycle)").
- `max_file_bytes`: the manifest's size cap. The cap in force is the smaller of it and the service's
  `NICKEL_MAX_FILE_BYTES` ("The egress rule", below).

### The egress rule

Outbound only to each connector's listed vendor hosts. For web links (`fetch: http`) the connector
applies it to every download hop, redirects included: the hop must be https on port 443 (the
manifest's own `http.urls` are checked for that at load, and for carrying no `user:password@`: a
credential in a link would go out as Basic auth and, as the item's message id, onto `raw_data` and
every receipt; credentials belong in Vault, `auth.vault_path`), its host must be one the manifest's
`http.urls` name (the allow-list is exactly those hosts, compared in the form httpcore dials:
lower-case ASCII, an IDN as punycode), and every address the host resolves to
must be public: never private, loopback, link-local, site-local or multicast, and an IPv6 address that embeds an IPv4
(IPv4-mapped `::ffff:a.b.c.d`, IPv4-compatible `::a.b.c.d`, NAT64 `64:ff9b::/96` and
`64:ff9b:1::/48`, 6to4 `2002::/16`, Teredo `2001::/32`) is checked as that IPv4 as well. The
connection then goes to the address the check passed: the download's httpx client runs on a custom
httpcore network backend (`PinnedBackend` in `sources.py`) that dials the checked address for the
host name, so a resolver that answers a public address to the check and 127.0.0.1 a moment later
(DNS rebinding) changes nothing; SNI and the certificate check still use the name. The certificate is
checked against the CA bundle httpx ships, never one the environment names: the transport's TLS
context and the client take nothing from the environment (`trust_env=False`: no `SSL_CERT_FILE`,
`SSL_CERT_DIR` or proxy variables), and the transport builds one connection pool, the pinned one.
Anything refused
is refused before the host is contacted and counted as `refused`, apart from downloads that merely
failed (`skipped`). A download has a total deadline (300 s) on top of the per-read timeout, follows
at most 5 redirects, stops at the size cap, and is spooled to a temporary file chunk by chunk.

Every error and refusal names the hop as `scheme://host` and the HTTP status or the error's type
(`https://lab.example: HTTP 404`, `https://lab.example: ConnectTimeout`), never a path or a query
string: a redirect's signed URL carries its token, and the text reaches the job record. The Kaggle,
Hugging Face and folder sources keep the same rule (`kaggle <handle>: <type>`), and so does the log:
the library's own message, which quotes the resolve URL and the file's path, goes nowhere.

The size cap in force is the smaller of the manifest's `max_file_bytes` and the service's
`NICKEL_MAX_FILE_BYTES`. A file whose listed size is over it is not downloaded, a web download stops
at it, a file an SDK put on disk (Kaggle lists no sizes) is measured before it is read into memory,
and the ingest bridge checks once more. Each of these leaves a `too_large` receipt ("Receipts",
above), counted as `too_large` in the run, apart from the fetches that failed (`skipped`).

A vendor link that redirects to another host therefore needs that host in its manifest. Checked on
23 Sep 2026: the Labcorp sample report in `http-lab-sample-reports` is served straight from
`www.labcorp.com` (HTTP 200, no redirect; the host refuses HEAD requests, which is not a redirect),
and Quest's overview straight from `www.questdiagnostics.com`, so both manifests name every host they
need. Re-check this when a source's link changes.

### Delivery

At-least-once, with a stable message id. `publish` returns only once the message is durable (Kafka:
`acks=all`; the JSONL outbox: appended under a lock that is exclusive across processes, flushed and
fsynced, in binary so the bytes are the same on every OS), and the receipt records that afterwards.
The lock is taken without blocking (`flock` with `LOCK_EX | LOCK_NB` on POSIX, `msvcrt.locking` with
`LK_NBLCK` on Windows) and tried again for 30 seconds at most: a holder that stalls (a stopped
command-line run, an fsync stuck on a network volume) fails the publish as `publish_failed`
(`TimeoutError`), for the relay to try again, instead of hanging the one job worker.
The envelope's `ingest_id` is the message's id, and a retry reuses it: a crash between the publish and
the receipt update leaves a durable message whose receipt still says `received`; the next run of the
same item, or the relay, publishes it again under the same id and `idempotency_key` as the next
`attempt`, and the receipt catches up. Two runs retrying one open receipt at once can both append:
the one whose final update finds the receipt already published still counts the message it made
durable as `published` (not a duplicate), so a job's counts match what went onto `raw_data`. The
publisher does not look for repeats (a scan of the outbox before each append was a check-then-act
with no lock across processes, and it grew with the file): consumers dedupe on the key, as the
Transform layer's Dispatcher does. A message is never lost (nothing is recorded as published before
it is durable) and never published twice unnoticed. The publisher's `message_id` argument is the
message's own id (the `ingest_id`, the line's `id`), not the envelope's `idempotency_key`.

The local File Store writes the metadata sidecar first, then the object, each atomically (temporary file,
fsync, rename), so a crash never leaves an object without its metadata, and a repeated `put` of the
same bytes is a no-op. On POSIX the directory is fsynced after each rename and after each directory
`mkdir` created, so a power loss cannot undo a rename that `put` already returned from (a published
receipt never writes the object again, so a lost rename would leave `payload_ref` pointing at
nothing). Windows cannot open a directory for fsync and NTFS journals its metadata itself, so the
step is skipped there.

## Where parsing and extraction went

Reading a PDF's text, OCR of scanned pages and extraction of result rows through the local model read
the content, need the GPU and must scale on their own: Transform-layer work, not the connection
layer's. In the architecture that is the Document worker ("reads the PDF / scan from File Store · OCR →
LangExtract on local Qwen → rows into extraction_staging · nothing leaves the box"): it consumes
`raw_data`, reads the original from the File Store by the envelope's `payload_ref`, and writes staging
rows that wait for physician review. It is a separate service in its own repository
(nickel-transform), with its own entry point, image and deployment, and its decision record lives
there (this repository's decision records are in [`docs/decisions/`](decisions/)). The parsing and
extraction libraries, their settings, jobs, stores and routes, and the benchmark harness that were
in this service's first drafts moved there with it. This service publishes `raw_data` and nothing
else.

## File Store (NIK-2209-06)

Every original that passes the edge checks is stored before its receipt is written, under the key
`object_key()` builds (`<tenant>/<class>/<sha256[:2]>/<sha256>`), in the File Store `NICKEL_FILE_STORE`
names:

- `s3`, production's default and the only store production takes: an S3 API, through boto3
  (`src/db/file_store/s3.py`). SeaweedFS serves it in development (compose's `s3` profile,
  [docker.md](docker.md)). Production gets the object store the SeaweedFS-vs-RustFS bake-off picks
  (RustFS expected); nothing in the code is specific to either, so the move is a change of endpoint
  and credentials. nickel-transform reads the same originals with the same settings and key layout.
- `local`, the default in development and qa: a folder, `NICKEL_DATA_DIR/filestore`, with a
  `.meta.json` sidecar next to each object ("Delivery", above). Production refuses it, so a pod that
  sets nothing never keeps PHI on its own disk.

| Setting (with `s3`) | What | Default |
|---|---|---|
| `NICKEL_S3_ENDPOINT_URL` | the S3 API, `http(s)://host[:port]` | none: required |
| `NICKEL_S3_REGION` | part of each request's signature | `us-east-1` |
| `NICKEL_S3_ORIGINALS_BUCKET` | the bucket of the originals | `originals` |
| `NICKEL_S3_ACCESS_KEY_ID`, `NICKEL_S3_SECRET_ACCESS_KEY` | the credentials; or `NICKEL_S3_ACCESS_KEY_ID_FILE`, `NICKEL_S3_SECRET_ACCESS_KEY_FILE` naming a mounted secret (one of each pair; UTF-8, a byte order mark dropped) | none: required |
| `NICKEL_S3_CA_BUNDLE` | a PEM bundle, for an https endpoint behind a private CA | the public CAs |
| `NICKEL_S3_ALLOW_PUBLIC_ENDPOINT` | `true` lifts the address rule below, for an https endpoint only | `false` |
| `NICKEL_S3_CALL_DEADLINE_SECONDS` | the most one call to the store may take, retries included | `300` (a 100 MB original at 1 MB/s, with room for retries; raise it with `NICKEL_MAX_FILE_BYTES`) |

**Checked at start-up, and again for every job.** With `s3`, a wrong setting stops the service (and the
command line, with exit code 2) before anything is contacted: a missing credential, both a value and a
`_FILE`, a secret file that is not UTF-8, a bucket or region name S3 would refuse, a CA bundle that is
not a file, a host name the S3 client refuses (such as `seaweed_fs`), or an endpoint outside the
platform. Every address the endpoint's host resolves to must be loopback, private (RFC 1918, IPv6
unique local) or on the tailnet (`100.64.0.0/10`), whatever the name ends in
(`src/utility/endpoints.py`, the mirror image of the egress rule). Outside development (qa and
production), plain http is taken to loopback only: an in-cluster endpoint is https, with its
private CA in `NICKEL_S3_CA_BUNDLE`, and there is no override for that. TLS is what binds the
connection to the name that was checked: the check resolves the name, and the client resolves it
again for every new connection, so over plain http a name that has come to resolve elsewhere
would get the originals. That leaves a development stack alone (compose's `http://seaweedfs:8333`)
with that window, and the deployment closes the rest with a NetworkPolicy egress rule that lets
the pod reach only the store (infra). Outside development the credentials come from the `_FILE`
variants only (a Vault or CSI mount); the variables themselves are refused there. Only `NICKEL_S3_ALLOW_PUBLIC_ENDPOINT=true` lets a public endpoint
through, over https, and every store opened then says so in the log. Each job checks the endpoint
again before its first call to the store, so a name that has come to resolve elsewhere fails that job
before anything is sent. The errors name the setting and `scheme://host[:port]`, never a credential,
and an endpoint that carries a user or password is refused without being quoted.

**Nothing from the environment.** The client takes its credentials, endpoint, region and CA bundle
from the settings. Its botocore session has constants where botocore would read a profile, the config
and credentials files, an `AWS_*` setting (the S3 options such as `AWS_S3_US_EAST_1_REGIONAL_ENDPOINT`
included) or a bearer token (`AWS_BEARER_TOKEN_S3`); it never asks the instance metadata service and
never goes through `HTTP(S)_PROXY`, so a stray variable can neither send the originals or the
credentials elsewhere nor break the client. (botocore still names `AWS_EXECUTION_ENV` in its
User-Agent.) The process builds one client per settings and every job shares it (boto3 clients are
thread-safe; building one takes a third of a second). A job opens the store only when it uses it: the
republish relay, which needs no original, never does. The client addresses buckets path-style
(`http://<endpoint>/<bucket>/<key>`), signs with SigV4, and gives each attempt a 5 s connect and a 30 s
read timeout, with up to 3 retries in botocore's standard mode (4 attempts in all).

**A deadline on every call.** Timeouts bound each read, not a call: a store that drips its answer a
byte at a time stays inside any read timeout. So every call (put, get, metadata) runs against
`NICKEL_S3_CALL_DEADLINE_SECONDS`, retries included; past it the call is abandoned on its daemon thread
and the job fails with `file store: <operation>: past the 300 s deadline`, instead of holding the job
worker, and with it every pull and republish behind it. An abandoned call does not run on: a `put`'s
body refuses to hand out another chunk once the deadline has passed, so the upload stops itself,
and `get` reads in chunks and stops between them. While four abandoned calls are still open, a new
call fails at once (`file store: <operation>: 4 earlier calls are still open past their
deadline`) instead of piling another thread, socket and object on a store that has stopped
answering, and each abandoned call is logged with the count. `get` also takes `max_bytes`: an
object larger than that is refused before its body is read, and the body goes into one buffer,
never held twice.

**How it behaves.**

- Write-once, by the store: `put` is one PutObject with `If-None-Match: *`, which creates the key only
  if it is not there, atomically; a 412 means it is (the key names the bytes, so they are stored
  already) and `put` returns False. SeaweedFS 4.47, RustFS and MinIO all take it. The bytes, the
  content type and the metadata go in that one PUT, so there is no sidecar, and `put` refuses bytes
  their key does not name. Two jobs racing on one key may get 409 (`ConditionalRequestConflict`,
  a conditional write in progress): `put` tries once more, and a 412 then means the other one
  stored it. Whether a store honours `If-None-Match` at all is tested live, not assumed:
  `tests/test_s3_conformance.py`, run by CI's `s3-conformance` job against compose's SeaweedFS and
  by the PL-06 bake-off against each candidate. It also asks that the service's identity cannot
  delete an original: SeaweedFS's "Write" can, so there that check is an expected failure, and the
  store the bake-off picks must pass it (object lock, or a policy that denies DeleteObject).
- `get` checks the bytes against the sha256 their key names, whatever the object's metadata says: an
  object replaced behind the S3 API (SeaweedFS's filer takes writes of its own) is refused, not read.
- Metadata holds facts about the content only: its `sha256`, with the content type and size in the
  object's own headers. The object is shared by every receipt that names its bytes, so who sent it,
  under which message id (a path at the source, which can carry a patient's name) and for how long
  it is kept stay with the receipts. S3 carries metadata in HTTP headers: ASCII only, and at most 2 KB in
  all, which `put` enforces before anything is sent. A value that is not plain printable ASCII, or that
  holds `=?`, is stored as an RFC 2047 encoded word, `=?UTF-8?B?<base64>?=`, the form AWS S3 itself
  uses; `metadata()` decodes it (also when S3 hands it back split into several words or Q-encoded) and
  lower-cases the names (SeaweedFS answers `X-Amz-Meta-Sha256`), so every value comes back exactly as it
  was put, and a value it cannot decode is a `FileStoreError`. A reader in another service decodes it
  the same way.
- Errors are content-free: a `FileStoreError` says the operation and the HTTP status, the S3 error code
  when it looks like one (letters, digits and dots; any other `<Code>` is the server's own text and is
  dropped), or botocore's error type (`file store: put: HTTP 403 AccessDenied`, `file store: put:
  cannot reach the store (EndpointConnectionError)`); never the key, a URL, the response body or a
  credential. A missing object is a `MissingObjectError` (`file store: get: no such object`); a missing
  bucket is not (`HTTP 404 NoSuchBucket`). A HEAD's 404 has no body to tell the two apart, so
  `metadata()` then asks HeadBucket.

**Failure ordering.** The ingest bridge stores the original before it writes the receipt, so a PUT that
fails raises before there is a receipt: nothing claims the original, no envelope points at it, and the
next run takes the item again (a receipt that was already open stays as it was). The job fails with
the store's content-free text as its error; the command line prints it and exits with 1.

## Where the code lives

The connector moved from the first draft's `app/nickel_connection` package into the service layout
(NIK-2209-08, the same structure as Rockwell). What went where:

| Was (`app/nickel_connection/`) | Now (`app/src/`) |
|---|---|
| `ids.py`, `keys.py` | `utility/ids.py`, `utility/keys.py` |
| `envelope.py` | `models/envelope.py` |
| `store/base.py`, `store/local.py` | `db/file_store/base.py`, `db/file_store/local.py` |
| `receipts.py`, `outbox.py` | `db/receipts.py`, `db/outbox.py` (+ `db/layout.py`: the folders under `NICKEL_DATA_DIR`) |
| `ingest.py` (`IngestBridge`) | `handler/ingest.py` |
| `connectors/documents/*` | `connectors/documents/*` |
| `config.py` (`Settings.from_env`) | `config/__init__.py`, sections `[CONNECTOR]` and `[STORAGE]` of `conf/application.conf` |
| `cli.py`, `__main__.py` | `cli.py` (`python -m src.cli ...`) |
| `manifests/documents/*.yaml` | unchanged: `app/manifests/documents/` (in the image) |

New with the jobs API: `db/jobs.py` (job records), `handler/documents.py` (pull, republish, receipts),
`handler/jobs.py` (the job worker), the routers `services/jobs_service.py` and
`services/documents_service.py`, `services/auth.py` (the service token) and the `[JOBS]` section of
`conf/application.conf`.

Under `NICKEL_DATA_DIR` (`.data` by default, `/data` in the image):

```
filestore/<tenant>/documents/<xx>/<sha256>   the originals (+ .meta.json sidecars), with the local File Store
outbox/raw_data.jsonl                        the envelopes, one JSON line each (+ raw_data.jsonl.lock)
receipts.sqlite                              ingest_receipts (with each receipt's envelope)
jobs.sqlite                                  job records (docs/jobs-api.md)
```

## Sources

One YAML manifest per source in `app/manifests/documents/`. CI fails if a manifest is missing its
governance fields (`phi`, `owner`, `contract`, `auth`, `license`).

| Manifest | What | Why |
|---|---|---|
| `hf-synthetic-au-medical-docs` | 100 synthetic medical PDFs on Hugging Face: 50 documents (pathology reports and requests, prescriptions, referrals, medication charts), each clean and scanned. Revision pinned. | Real document layouts with no real patients, plus ground truth and field bounding boxes for scoring the Document worker downstream. Licence CC BY-NC 4.0: evaluation only. |
| `http-lab-sample-reports` | Labcorp's public sample patient report; Quest's Metabolic Risk Panel overview. | Real US reference-lab layouts. Publisher copyright; internal testing only. |
| `kaggle-breast-cancer-reports` | 1,000 generated breast-cancer report PDFs (benign and malignant), randomised names, train/test split by class. Version pinned. | A consistent layout at volume, with a label per report. Apache-2.0. |
| `kaggle-hospital-tariff-pdfs` | Hospital tariff, package-rate and health-insurance plan PDFs from India. Version pinned. | Table-heavy documents with no patient data: a test set for the Document worker downstream. CC0. |

### How the Kaggle datasets were chosen

About 185 Kaggle datasets came back from 20 searches (medical, lab, pathology, discharge, prescription
and insurance terms, with and without "pdf"). Most medical-report datasets are CSV tables or images.
Kaggle's own file listings confirmed which ones really hold PDF files:

| Dataset | PDFs | Used? |
|---|---|---|
| omenkj/breast-cancer-medical-reports | 1,000 generated reports | Yes |
| salonishukla/hospital-tariff-documents-pdfs | tariff and insurance documents | Yes |
| infobayai/healthcare-discharge-summary, pathology, usg-reports | report samples | **No**: described as real-world patient records. Public or not, real patient documents need PHI handling and a legal review first. |
| infobayai/medical-ehr | a handful of files | No: the description is a general text corpus, not health records |
| fernandosr85/parteirabr-signalgrounding | Brazilian Ministry of Health guides (Portuguese, 5-20 MB each) | Not yet: long public-health guides, a candidate for long-document tests |
| mdhaarishussain/synthetic-medical-records-for-100-patients, divyanshuchaubey/medical-prescription-pdf | one PDF each | No: a single file adds little |

A real Kaggle pull needs a Kaggle token where it runs (`~/.kaggle` for `uv run`, `KAGGLE_API_TOKEN` in
Docker; see `docs/docker.md`). The token is never a setting and never logged.

Looked at and not used elsewhere: Walk-In Lab's sample reports (HTTP 403 to automated clients) and a
clinic's copy of a Cleveland HeartLab sample report (HTTP 418) refuse automated downloads.

## Run it

Through the service, the way the scheduler does it (`docs/jobs-api.md`; every call carries the
service token):

```bash
cp .env.example .env            # once; set NICKEL_SERVICE_TOKEN (NICKEL_TENANT_ID is set); .env is git-ignored
uv run --env-file .env uvicorn main:app --app-dir app --reload
export NICKEL_SERVICE_TOKEN=$(grep ^NICKEL_SERVICE_TOKEN= .env | cut -d= -f2-)
curl -s -X POST localhost:8000/jobs/documents/pull -H "Authorization: Bearer $NICKEL_SERVICE_TOKEN" \
  -H 'Content-Type: application/json' -d '{"manifest": "hf-synthetic-au-medical-docs"}'
curl -s -H "Authorization: Bearer $NICKEL_SERVICE_TOKEN" localhost:8000/jobs/<job_id>
curl -s -H "Authorization: Bearer $NICKEL_SERVICE_TOKEN" 'localhost:8000/documents?status=published&limit=5'
```

In the foreground, from the command line (the same settings, no token; `app/` must be the working
directory or on `PYTHONPATH`):

```bash
export NICKEL_TENANT_ID=0192a3b4-0000-7000-8000-000000000001   # see .env.example
export NICKEL_DATA_DIR=/data/nickel/connection-dev
cd app
uv run python -m src.cli documents pull manifests/documents/hf-synthetic-au-medical-docs.yaml
uv run python -m src.cli documents republish   # open receipts out again, from their stored envelopes
uv run python -m src.cli documents status
uv run python -m src.cli schema envelope        # the envelope's JSON Schema
```

Exit codes: 0 done; 1 the run had a problem (source unreachable, run aborted); 2 a setting is wrong
or `NICKEL_TENANT_ID` is not set, a manifest that does not load included. A manifest's error names
the field and pydantic's error type (and the message of the manifest's own checks), never the
value (`manifest does not validate: license string_too_short`), and a YAML error its line and
column; the job's record and the relay's log say the same. In Docker:
`docker compose run --rm nickel-connection python -m src.cli documents pull manifests/documents/<id>.yaml`
(`docs/docker.md`).

## Results on dev-server-divij, 22 Sep 2026

| Run | Listed | Matched | Published | Duplicates | Rejected | Time |
|---|---|---|---|---|---|---|
| Hugging Face, clean PDFs only (first manifest) | 108 | 50 | 50 | 0 | 0 | 39 s |
| Web links | 2 | 2 | 2 | 0 | 0 | 2 s |
| Both again | | | 0 | 52 | 0 | |
| Hugging Face with the scanned copies | 108 | 100 | 50 | 50 | 0 | 34 s |
| Both sources in Docker (fresh volume) | | 102 | 102 | 0 | 0 | |
| Kaggle breast-cancer reports (token from the environment) | | 1,000 | 1,000 | 0 | 0 | |
| Kaggle hospital tariff PDFs | | 9 | 9 | 0 | 0 | |
| Both Kaggle sources again | | | 0 | 1,009 | 0 | |

102 receipts and 102 envelopes from the first two sources, but 86 stored objects: 16 files in the
dataset's `pdfs_scanned/` folder are byte-identical copies of the clean PDFs (only 34 documents have a
real scan), so content addressing keeps one copy of each. With the two Kaggle sources the development
data on the server holds 1,111 receipts. In Docker the connector runs as user 999 on a read-only root
filesystem.

## Code quality

Scanned with the free SonarQube setup ([sonarqube.md](sonarqube.md)) on dev-server-divij, 22 Sep 2026.
Both scans were of the first draft (`app/nickel_connection`, before the move into the service
layout); the service layout has not been re-scanned yet, and its row is what `uv run pytest` and the
pre-commit hooks report locally, not a SonarQube result. Re-scan once the review-round changes are in.

| Scan | Quality gate | Bugs | Vulnerabilities | Hotspots | Code smells | Coverage | Duplication |
|---|---|---|---|---|---|---|---|
| First draft, first scan | Passed | 0 | 0 | 0 | 26 | 99.9% | 0.0% |
| First draft, after the clean-up commit | Passed | 0 | 0 | 0 | 0 | 100% | 0.0% |
| Service layout (this PR), local checks only | not scanned yet | | | | black, isort, flake8, bandit clean | 100% (line and branch) | |

## What is temporary

| Piece | Now | Replaced by |
|---|---|---|
| File Store | local disk (`LocalFileStore`, `NICKEL_DATA_DIR/filestore`) in development and qa unless `NICKEL_FILE_STORE=s3`; the S3 File Store in production, which refuses local disk | the S3 File Store everywhere but a laptop (NIK-2209-06); the local one stays for the tests |
| Object store | SeaweedFS in development, over the S3 API (compose's `s3` profile) | RustFS, pending the SeaweedFS-vs-RustFS bake-off: the same S3 client, another endpoint and credentials. The bake-off also settles immutability on the store's side: the client's `If-None-Match` keeps a PUT from replacing an original, but only a store that refuses the service's identity deletes (a deny-Delete policy) or holds objects under object lock keeps one from being removed |
| `raw_data` | a JSONL outbox: one append per message under a cross-process lock, fsynced | Kafka with `acks=all` once it is deployed; the same at-least-once contract, message id and dedup key |
| Receipts and job records | SQLite files under `NICKEL_DATA_DIR` | the Postgres ingest tables (schema migrations in `app/migrations` then) |
| Service-to-service auth | the bearer token (`NICKEL_SERVICE_TOKEN`), checked before routing | mTLS between services, a K3s NetworkPolicy that lets only the scheduler and the API layer reach the pod, and the token itself issued from Vault (infra-repo work); the token stays as the application-level check underneath |
| Tenant | `NICKEL_TENANT_ID` from the environment | the platform's answer for jobs with no logged-in user |
| Envelope | v0.3 in `models/envelope.py` (a reader and a strict producer model) | the agreed CN-01 contract in nickel-schemas |
| Job worker | one thread in the service process | the scheduler's own runner, once it exists |
| Manifest fields | a CN-01 subset: no `endpoints`, `rate_limit` or `expectations`; `schedule` has its `class` only; `formats` is the list of formats the edge check sniffs | CN-01's full shape: `endpoints` (which generate the OPA egress allow-list), `rate_limit`, `expectations: {freshness_slo}`, `schedule: {class, interval, overlap, backfill}` and `formats: {schema_id, validate}` |
| `schedule: on_demand` | the old string form, read as `{class: F4}` with a deprecation warning, for the manifests still on disk | refused in the next release, like any other string |
| Source credentials | `KAGGLE_API_TOKEN` and `HF_TOKEN` from the environment, read by the SDKs themselves; dev sources only (a token beyond `dev` needs `auth.vault_path`, and the SDK sources are `dev` only) | the connector runner (CN-09) reading `auth.vault_path` from Vault |
| `raw_retention` expiry | kept with each receipt's source (its manifest), never on the shared object; nothing expires yet | the File Store lifecycle rule: an object goes once the longest `raw_retention` among the receipts that point at it has passed (nickel-connection issue #6) |
| The `too_large` marker's key | a surrogate (`too_large_key`) in `payload_sha256`, since the SQLite table's unique key needs a value there; the API shows null | the Postgres ingest tables: a nullable `payload_sha256` (NULL for a marker) and a partial unique index on (tenant_id, source, message_id) where it is NULL; [decision record 0002](decisions/0002-too-large-receipts-for-pulls.md) |
| Quarantine | an edge reject or an over-cap file keeps a receipt and not its bytes (`rejected`: not stored; `too_large`: not fetched) | a quarantine object class that keeps them for inspection and replay, with `raw.dlq` and the replay tool: nickel-connection issue #4 (a pull's `too_large` receipt is what a replay of an over-cap file hangs on) |
| `idempotency_key` formula | sha256 over tenant, source, message id and payload hash | what [decision record 0001](decisions/0001-idempotency-key.md) decides (Proposed: D-00's formula, the document's version in its message id) |
