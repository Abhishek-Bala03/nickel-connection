# Scheduler (NIK-2209-03)

The scheduler owns timing and execution history. The connection service owns fetching,
raw-file storage and publishing. The scheduler drives two job kinds there: document pulls
(`documents`) and Nango connector pulls (`nango`, docs/demo-nango-wearables.md).

## Small set of responsibilities

- `models.py`: validated interval/daily-UTC settings, versions and execution records.
- `store.py`: SQLite columns/constraints, atomic version checks, history and migration.
- `registry.py`: deployment-approved connections and connector minimum intervals.
- `runner.py`: async adapter interface, demo adapter and idempotent jobs-API adapter.
- `engine.py`: APScheduler 3.11.2 triggers, deadlines, result recording and retention.
- `service.py`: mark abandoned running records interrupted at startup.
- `api.py`: shared service authentication, audit identity and FastAPI lifespan.

Schedules live in the application DB, never in connector manifests. The memory job
store is reconstructed at startup. UUID job IDs replace existing jobs; coalescing
and `max_instances=1` prevent catch-up bursts. The database also prevents two
active runs for one tenant/connection, including manual runs. Next-run times are
read from APScheduler, not persisted. Interval schedules restart their interval
on service restart; missed downtime executions are not replayed.

A run submits `POST /jobs/documents/pull` with `Idempotency-Key: <run UUID>` and
polls `/jobs/{id}`. Transient request retries reuse that key. Per-request HTTP timeouts
and one overall deadline bound submission, backoff and polling. A failed remote
job is not submitted again as a retry. Shutdown cancels async requests, then closes
storage. A timeout/interruption stops **polling**, not an already accepted connection
job: the jobs API has no cancel endpoint. Consult the connection job history before
manually resubmitting a timed-out job. Later scheduled runs are new logical runs.

`GET /runs` defaults to 50, permits at most 200 and accepts `schedule_id`.
A five-minute sweep deletes at most 1,000 terminal rows older than 30 days; running
rows are never pruned. SQLite uses WAL and a five-second busy timeout.

## Authentication and deployment

The image still starts `main:app`, the shared connection service. The additional
`compose.scheduler.yaml` overrides the command only for the scheduler. Use exactly
**one replica and `--workers 1`**, including during rolling deployment. Stop the old
scheduler before starting a replacement against the same DB. APScheduler 3.x is
not a distributed scheduler. There is no POSIX-only lock or second scheduler CLI.

The UI is behind OAuth2 Proxy with Keycloak OIDC and a required admin group. The
browser receives an HttpOnly session cookie, never the connection service token.
Nginx protects both HTML and `/api`, overwrites any browser-supplied actor/token,
and rejects mutations without the custom CSRF header. Cross-origin requests are
not enabled. The backend reuses `src.services.auth`, requires the gateway actor
on mutations, stores `changed_by` on schedule writes and logs actor/run ID on manual
runs. Only trusted services may reach the scheduler network; do not publish its
port. `/health` remains an unauthenticated probe; API docs exist only in development.

Create a confidential Keycloak client with Standard Flow and PKCE S256. Register
exactly `<public-origin>/oauth2/callback`, configure a group-membership mapper named
`groups`, and grant scheduler administrators the configured group. The issuer must
be reachable with the **same URL** from the browser and OAuth2 Proxy. Production
requires TLS at the ingress and secure cookies. No IdP secrets are checked in.

Use the OAuth2 Proxy [Keycloak provider](https://oauth2-proxy.github.io/oauth2-proxy/configuration/providers/keycloak_oidc/)
and [nginx integration](https://oauth2-proxy.github.io/oauth2-proxy/configuration/integrations/nginx/)
for the corresponding provider configuration.

Set these deployment variables through your secret/configuration manager or an
ignored `.env` file. Do not put credentials in UI source or `VITE_*` variables:

| Variable | Value |
| --- | --- |
| `NICKEL_TENANT_ID` | Same tenant as the connection service |
| `NICKEL_SERVICE_TOKEN` | Shared random URL-safe token, at least 32 characters |
| `NICKEL_ENV` | `development` for sample manifests/demo; otherwise `production` |
| `SCHEDULER_PUBLIC_ORIGIN` | Public origin, no trailing slash, e.g. `https://scheduler.example.org` |
| `KEYCLOAK_ISSUER_URL` | `https://identity.example.org/realms/nickel` |
| `KEYCLOAK_CLIENT_ID` / `KEYCLOAK_CLIENT_SECRET` | Confidential client credentials |
| `SCHEDULER_ADMIN_GROUP` | Exact group claim, e.g. `/scheduler-admins` |
| `SCHEDULER_COOKIE_SECRET` | Random base64-encoded 32-byte secret |
| `SCHEDULER_COOKIE_SECURE` | Default `true`; `false` only for local HTTP development |
| `SCHEDULER_CONNECTIONS_FILE` | Approved connection JSON; example supplied |
| `SCHEDULER_UI_CONTEXT` | UI checkout path; default `../nickel-connections-ui` |

Production connection service storage settings still apply; see `docker.md` and
`.env.example`. The scheduler has a **separate** data volume.

```sh
docker compose -f compose.yaml -f compose.scheduler.yaml config --quiet
docker compose -f compose.yaml -f compose.scheduler.yaml up -d --build
```

The UI binds loopback port 8080 by default. Put your TLS ingress in front of it or
use an SSH tunnel for development. Do not weaken authentication for a demo.

**Nango connections do not come from this file.** They are the enabled connectors an
administrator added through the Connections UI, which is behind the same authenticated gateway;
the scheduler reads them from the connection service's `GET /connectors` at start-up, before each
create or update of a `nango` schedule, and when listing connections. If the connection service
is down at start-up, `nango` schedules are kept and checked again when their run starts. A
`nango` entry in the file is refused. Each run records the connection job it queued (`job_id`,
schema v2), so the UI can show what the run stored.

Approved connections are a JSON list of `id`, `name`, `connector_type` and (for
`documents`) `manifest`. The manifest must exist and be cleared for this environment
by the connection service. The registry file grants access, **not** a schedule.
Restart after changing it. Document pulls have a five-minute minimum. The daily
UI/API option accepts `minute hour * * *` in UTC; arbitrary cron expressions are
not exposed yet. Demo connections require development mode and a 30-second minimum:

```json
[{"id":"demo-connection","name":"Demo","connector_type":"demo"}]
```

For a local UI walkthrough, point `NICKEL_SCHEDULER_CONNECTIONS` at
`scheduler-demo-connections.json` and restart the scheduler. This allows create,
pause, resume and manual-run tests without a running connection service. The UI's
Vite proxy reads `NICKEL_DEV_SERVICE_TOKEN` from its own terminal and passes that
token plus a local demo identity to the scheduler. Set it to the same value as
`NICKEL_SERVICE_TOKEN`. Vite only listens on loopback; use an SSH port forward
when the browser runs on another computer. Use the Docker/Keycloak gateway for
authentication testing.

## Migration and editing

Stop the old instance and back up its DB (including WAL via SQLite's backup API)
before the first upgrade. Legacy JSON rows migrate transactionally to schema v1;
invalid data rolls back the whole migration. Do not run old code against the migrated
DB. Existing connections and intervals must satisfy the deployment registry.

Creates return `version=1`. PUT requires the version read by the editor; stale
versions and connection changes return 409. Reload the form to resolve a conflict.
Pausing while editing reloads the saved form/version, so a later Save cannot
silently resume it. Settings and next-run state remain separate.

## Validation and manual UI check

```sh
uv run pytest
uv run pre-commit run --all-files
```

SonarQube runs in CI when its host/token are configured and now waits for its quality
gate. Passing pre-commit is not a SonarQube server result. Live S3 conformance tests
remain optional and require the storage environment.

After configuring Keycloak, sign in as an administrator and check: create an
interval schedule, inspect its history, run manually, pause while editing then Save,
resume, choose a daily UTC time, and edit the same schedule in two browser tabs to
observe a 409 instead of an overwrite. Sign out: HTML must redirect to login and
API mutations must fail. Confirm `changed_by` uses the authenticated Keycloak subject.
