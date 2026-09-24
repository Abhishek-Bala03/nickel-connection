# Run the service in Docker

Prepared by Divij N.

Everything here runs on a laptop or a dev server with Docker.

```bash
cp .env.example .env                                            # sets NICKEL_TENANT_ID; .env is git-ignored
python -c "import secrets; print(secrets.token_urlsafe(32))"   # paste into .env as NICKEL_SERVICE_TOKEN
docker compose up --build                                       # the API on http://localhost:8000
export NICKEL_SERVICE_TOKEN=$(grep ^NICKEL_SERVICE_TOKEN= .env | cut -d= -f2-)
curl -s http://localhost:8000/health                                                 # a probe: no token
curl -s -H "Authorization: Bearer $NICKEL_SERVICE_TOKEN" http://localhost:8000/jobs  # everything else
```

- **`NICKEL_SERVICE_TOKEN`**, or `NICKEL_SERVICE_TOKEN_FILE` (a mounted secret; one of the two, never
  both): the token every caller but the probes must present (`docs/jobs-api.md`). At least 32
  characters; the service refuses to start without one, or with a shorter one, so a forgotten token can
  never mean no wall at all. `compose.yaml` refuses to start without it in `.env` or the shell, too.
- **`NICKEL_ENV`**: `production` unless set, and then the API docs (`/docs`, `/redoc`,
  `/openapi.json`) are off. `compose.yaml` sets `development` for a local run, which turns them on (as
  does `qa`); a deployment that sets nothing gets them off.
- **`DEV_SOURCES=1`**, a build argument: adds the Kaggle and Hugging Face SDKs (`kagglehub`,
  `huggingface-hub`, the `dev-sources` group of `pyproject.toml`), so a local image can pull the
  development manifests. `compose.yaml` builds with it; a plain `docker build` is the production image,
  without them.
- The image holds only `app/` (code, `conf/application.conf`, `manifests/documents/*.yaml`), runs as
  the non-root user 999, and `compose.yaml` adds a read-only root filesystem, no Linux capabilities
  and no privilege escalation. The base image is pinned by digest (`Dockerfile`).
- Files, receipts, envelopes, job records and download caches live in the named volume
  `connector-data` (`/data` in the container, `NICKEL_DATA_DIR`). `docker compose down -v` deletes it.
  With the S3 File Store on (below), the originals go to SeaweedFS instead.
- The job worker runs inside the service process (`docs/jobs-api.md`), so one container is the whole
  connection layer for now. Parsing and extraction are not in this image: they are the Document
  worker's, a separate service with an image of its own (`docs/document-ingestion.md`).

## One-off commands in the container

The same image runs the command line against the same volume; override the command:

```bash
docker compose run --rm nickel-connection python -m src.cli documents pull manifests/documents/hf-synthetic-au-medical-docs.yaml
docker compose run --rm nickel-connection python -m src.cli documents pull manifests/documents/http-lab-sample-reports.yaml
docker compose run --rm nickel-connection python -m src.cli documents status
docker compose run --rm nickel-connection python -m src.cli schema envelope
```

Manifest paths are relative to `/app` (the working directory), so `manifests/documents/<id>.yaml`.
In scripts, add `-T` to `docker compose run` and give it `< /dev/null`; otherwise the container reads the
script's own input.

## Kaggle and Hugging Face credentials

Kaggle manifests need a Kaggle token. Pass it through the environment of the command; it never goes into
a file in the repository or the image, and the service never reads or logs it (kagglehub does):

```bash
export KAGGLE_API_TOKEN=...       # Kaggle > Settings > API > Create New Token
docker compose run --rm nickel-connection python -m src.cli documents pull manifests/documents/kaggle-breast-cancer-reports.yaml
```

Public Hugging Face datasets need no token; `HF_TOKEN` only raises the rate limits. Outside Docker,
`uv run` picks the Kaggle token up from `~/.kaggle` by itself.

## The S3 File Store (SeaweedFS)

SeaweedFS stands in for the production object store in development (`docs/document-ingestion.md`, "File
Store"). The `seaweedfs` service runs `weed server -s3` (image `chrislusf/seaweedfs:4.47`, pinned by
digest) with its S3 API on port 8333, and the one-shot `seaweedfs-init` makes the originals bucket.
Both are on the `s3` profile, so a plain `docker compose up` starts neither. To turn it on, set in
`.env`:

```bash
NICKEL_FILE_STORE=s3
COMPOSE_PROFILES=s3
NICKEL_S3_SECRET_ACCESS_KEY=...   # python -c "import secrets; print(secrets.token_urlsafe(32))"
```

and run `docker compose up --build` as above.

- compose writes SeaweedFS's identities file (`/etc/seaweedfs/s3.json`) from `.env`: one identity,
  `nickel-connection`, with the access key (`NICKEL_S3_ACCESS_KEY_ID`, default `nickel-connection`) and
  the secret, allowed to read and write the originals bucket and nothing else. No committed file holds
  the secret, and `seaweedfs` refuses to start without one.
- The S3 identity guards the S3 API only. SeaweedFS's master (9333), volume server (8080) and filer
  (8888) take no credentials at all, and its "Write" action also lets the identity delete, so the
  master, volume server and filer listen on the container's loopback only (`-ip.bind=127.0.0.1`) and
  only the S3 API is on the compose network (`-s3.ip.bind=0.0.0.0`). SeaweedFS opens its IAM gRPC
  with the S3 API (port 18333); a JWT key made at each start, which no one outside the container
  knows (`WEED_JWT_FILER_SIGNING_KEY`), keeps it shut. Its Iceberg and Lance listeners are off, and so
  is its telemetry.
- `seaweedfs-init` creates the bucket (`NICKEL_S3_ORIGINALS_BUCKET`, default `originals`) through
  SeaweedFS's own shell, owned by that identity, which therefore needs no admin rights. It runs in the
  `seaweedfs` container's network namespace (`network_mode: service:seaweedfs`) to reach the master on
  loopback. Running it again changes nothing. The connection service starts once it has finished.
- The connection service reaches SeaweedFS at `http://seaweedfs:8333`, a private address on the compose
  network, as the endpoint rule wants; compose runs it as development, which takes plain http there
  (production takes https off loopback, and refuses the local File Store). Outside Docker, `uv run --env-file .env` reaches it at
  `http://127.0.0.1:8333` (`NICKEL_S3_ENDPOINT_URL` in `.env.example`; the host port is
  `NICKEL_S3_PORT`, on this machine only).
- Its data is in the `seaweedfs-data` volume, which `docker compose down -v` deletes with the rest.

SonarQube with Docker is part of the repository setup: see [sonarqube.md](sonarqube.md).
