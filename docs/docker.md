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

SonarQube with Docker is part of the repository setup: see [sonarqube.md](sonarqube.md).
