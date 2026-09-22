# nickel-connection

Layer 1 of the Nickel platform, the connection layer. Connectors fetch or receive every raw payload
(Nango-managed sources, the scheduler's pulls, document sources, listeners), keep the original
unchanged in the File Store, and publish an envelope on Kafka `raw_data` for the Transform layer.
This layer does not read, change or judge the content.

## Layout

The same structure as Rockwell's `agentbuilder-be`, with all code under `app/` (the only folder that
goes into the image):

```
app/
  main.py                  FastAPI app: middlewares, error handlers, routers (Rockwell: app.py)
  conf/application.conf    settings; ${VAR} values come from the environment
  src/
    config/                Service settings        from src.config import Service
    logging/               the service logger      from src.logging import logger
    error/                 error types and their JSON responses
    middlewares/           request id, request timing
    services/              API routes, one router per module, collected in base_router
    handler/               business logic the routes call
    models/                request and response models
    db/                    database access (Postgres)
    connectors/            source connectors (Nango sources, documents)
    scheduler/             scheduled pulls
    utility/               small shared helpers
tests/                     unit tests (pytest), outside the image
docs/                      design notes and how-tos, outside the image
Dockerfile, compose.yaml   image and local run
pyproject.toml, uv.lock    dependencies
.pre-commit-config.yaml, sonar-project.properties, .github/    standards and CI
```

Where new code goes: a route in `src/services/<area>_service.py` (and `base_router`), its logic in
`src/handler/`, its request and response shapes in `src/models/`, and expected failures raised as one of
the errors in `src/error` (they become JSON with the right status and the request id).

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

## Run it

```bash
cp .env.example .env                                              # once; .env is git-ignored
uv run --env-file .env uvicorn main:app --app-dir app --reload    # http://localhost:8000/health, /docs
docker compose up --build                                         # the same, in the image
```

## Standards

- Python 3.11. Black (88 columns), isort (black profile), flake8, bandit, pyupgrade `--py311-plus` and Trivy, via pre-commit.
- SonarQube: in CI once a server is configured, and free with Docker until then ([docs/sonarqube.md](docs/sonarqube.md)).
- Unit tests with coverage at or above 95%.
- Docker: slim base image, non-root user, only `app/` in the image, no secrets in the image or the repository.
- No secrets, PHI or real patient data in code, tests, fixtures or logs.
