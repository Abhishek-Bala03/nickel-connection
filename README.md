# nickel-connection

Layer 1 of the Nickel platform, the connection layer. Connectors fetch or receive every raw payload
(Nango-managed sources, the scheduler's pulls, document sources, listeners), keep the original
unchanged in the File Store, and publish an envelope on Kafka `raw_data` for the Transform layer.
The connection layer ends there: it does not read, parse, OCR, extract, judge or serve the content.
That work is the Document worker's, a separate service
([docs/document-ingestion.md](docs/document-ingestion.md), "Where parsing and extraction went").

## Layout

The same structure as Rockwell's `agentbuilder-be`, with all code under `app/` (the only folder that
goes into the image):

```
app/
  main.py                  FastAPI app: middlewares, error handlers, routers, the job worker (Rockwell: app.py)
  conf/application.conf    settings; ${VAR} values come from the environment
  manifests/documents/     one YAML manifest per document source (in the image)
  src/
    cli.py                 command line: python -m src.cli documents pull|republish|status, schema
    config/                service settings        load_settings(), passed on as an argument
    logging/               the service logger      from src.logging import logger
    error/                 error types and their JSON responses
    middlewares/           request id, request timing, request body cap (413), security headers
    services/              API routes, one router per module, collected in base_router; the service token
    handler/               business logic the routes call: ingest bridge, documents, jobs
    models/                request and response models, the raw_data envelope
    db/                    File Store (local disk or S3), the raw_data outbox, receipts, job records
    connectors/            source connectors (documents; Nango sources planned)
    utility/               small shared helpers (UUIDv7 ids, object keys, the internal-endpoint rule)
tests/                     unit tests (pytest), outside the image
docs/                      design notes and how-tos, outside the image
Dockerfile, compose.yaml   image and local run
pyproject.toml, uv.lock    dependencies
.pre-commit-config.yaml, sonar-project.properties, .github/    standards and CI
```

Where new code goes: a route in `src/services/<area>_service.py` (and `base_router`), its logic in
`src/handler/`, its request and response shapes in `src/models/`, and expected failures raised as one of
the errors in `src/error` (they become JSON with the right status and the request id).

## Docs

- [Jobs API](docs/jobs-api.md): what the scheduler calls to queue a pull or a republish, read a job and list the receipts; the service token, `Idempotency-Key`, the 429 and `Retry-After`.
- [Document ingestion](docs/document-ingestion.md): the PDF connector, its manifests (a CN-01 subset: `fetch`, `auth`, `contract`, `phase`, `shadow`, `raw_retention`), the receipts, the egress rule, delivery, the File Store (local or S3), what is temporary, and where parsing and extraction went.
- [Docker](docs/docker.md): run the service and the command line in Docker; `NICKEL_SERVICE_TOKEN`, `NICKEL_ENV`, `DEV_SOURCES`; SeaweedFS for the S3 File Store.
- [Decision records](docs/decisions/): [0001](docs/decisions/0001-idempotency-key.md), the idempotency key and D-00 (Proposed); [0002](docs/decisions/0002-too-large-receipts-for-pulls.md), a too_large receipt for an item a pull never fetched (Proposed).
- [Demo: wearables through Nango](docs/demo-nango-wearables.md): Nango and Postgres in Docker, simulated Oura and Dexcom, connectors, schedules and the pulled JSON in the Connections UI.
- [SonarQube](docs/sonarqube.md): the free SonarQube setup with Docker.

## Branches and pull requests

- `main` (production), `qa` and `dev` are protected: no direct pushes, no force-pushes, no deletion.
- Branch from `dev` as `feature/<ticket>` (for example `feature/NIK-2209-05`) and open a pull request into `dev`.
- A pull request needs one approval and all conversations resolved. Promotion is by pull request: `dev` → `qa` → `main`.

## Set up

```bash
uv sync                      # Python 3.11 and dependencies
uv run pre-commit install    # hooks run on every commit
uv run pytest                # unit tests; coverage must stay at or above 95%
```

## Configuration

Settings come from `app/conf/application.conf`, whose `${VAR}` values come from the environment
(`.env.example` lists them all):

- `NICKEL_SERVICE_TOKEN`, or `NICKEL_SERVICE_TOKEN_FILE` (a mounted secret; one of the two): the token
  every route but `/health` and `/ready` needs as `Authorization: Bearer <token>`. At least 32
  characters; the service refuses to start without it. Generate one with
  `python -c "import secrets; print(secrets.token_urlsafe(32))"`.
- `NICKEL_ENV`: `development`, `qa` or `production`. `production` unless set. The API docs (`/docs`,
  `/redoc`, `/openapi.json`) take no token, so they are on in `development` only; set that on a laptop.
  Production also needs the S3 File Store (below).
- `NICKEL_TENANT_ID`: the tenant (a UUID) everything is ingested for. Without it the service starts,
  but pulls are refused.
- `NICKEL_FILE_STORE`: where the originals go, `s3` or `local` (under `NICKEL_DATA_DIR`). Production
  defaults to `s3` and refuses `local`; development and qa default to `local`. With `s3`:
  `NICKEL_S3_ENDPOINT_URL` (loopback, private or the tailnet, whatever the host's name; https off
  loopback outside development), `NICKEL_S3_REGION`, `NICKEL_S3_ORIGINALS_BUCKET`,
  `NICKEL_S3_ACCESS_KEY_ID` and `NICKEL_S3_SECRET_ACCESS_KEY` (their `_FILE` variants only, outside
  development), `NICKEL_S3_CA_BUNDLE`,
  `NICKEL_S3_ALLOW_PUBLIC_ENDPOINT` and `NICKEL_S3_CALL_DEADLINE_SECONDS`, all checked at start-up
  ([docs/document-ingestion.md](docs/document-ingestion.md), "File Store"). SeaweedFS runs it in
  compose's `s3` profile ([docs/docker.md](docs/docker.md)).
- `DEV_SOURCES=1`, a build argument of the `Dockerfile`: adds the Kaggle and Hugging Face SDKs for the
  development manifests. `compose.yaml` sets it; the production image is built without.

## Run it

```bash
cp .env.example .env                                              # once; .env is git-ignored
python -c "import secrets; print(secrets.token_urlsafe(32))"     # paste into .env as NICKEL_SERVICE_TOKEN
uv run --env-file .env uvicorn main:app --app-dir app --reload    # http://localhost:8000/health, /docs
docker compose up --build                                         # the same, in the image
export NICKEL_SERVICE_TOKEN=$(grep ^NICKEL_SERVICE_TOKEN= .env | cut -d= -f2-)
curl -s -X POST localhost:8000/jobs/documents/pull -H "Authorization: Bearer $NICKEL_SERVICE_TOKEN" \
  -H 'Content-Type: application/json' -d '{"manifest": "hf-synthetic-au-medical-docs"}'
curl -s -H "Authorization: Bearer $NICKEL_SERVICE_TOKEN" localhost:8000/jobs
cd app && uv run --env-file ../.env python -m src.cli documents status   # the command line, no token
```

## Standards

- Python 3.11. Black (88 columns), isort (black profile), flake8, bandit, pyupgrade `--py311-plus` and Trivy, via pre-commit.
- SonarQube: in CI once a server is configured, and free with Docker until then ([docs/sonarqube.md](docs/sonarqube.md)).
- Unit tests with coverage at or above 95%.
- Docker: slim base image, non-root user, only `app/` in the image, no secrets in the image or the repository.
- No secrets, PHI or real patient data in code, tests, fixtures or logs.

## Scheduler

See [scheduler deployment and API behavior](docs/scheduler.md) for the authenticated
configuration UI, APScheduler process and the connection jobs API adapter.
