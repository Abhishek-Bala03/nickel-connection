# nickel-connection

Layer 1 of the Nickel platform, the connection layer. Connectors fetch or receive every raw payload
(Nango-managed sources, the scheduler's pulls, document sources, listeners), keep the original
unchanged in the File Store, and publish an envelope on Kafka `raw_data` for the Transform layer.
This layer does not read, change or judge the content.

## Layout

| Path | What it holds |
|---|---|
| `app/` | All service code. The only folder that goes into the image. |
| `tests/` | Unit tests (pytest). Outside the image. |
| `docs/` | Design notes. Outside the image. |
| `Dockerfile`, `pyproject.toml`, `uv.lock` | Build and dependencies. |
| `.pre-commit-config.yaml`, `sonar-project.properties`, `.github/` | Standards and CI. |

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

## Standards

- Python 3.11. Black (88 columns), isort (black profile), flake8, bandit, pyupgrade `--py311-plus` and Trivy, via pre-commit.
- SonarQube on every pull request (`sonar-project.properties`; the server and token come from CI).
- Unit tests with coverage at or above 95%.
- Docker: slim base image, non-root user, only `app/` in the image, no secrets in the image or the repository.
- No secrets, PHI or real patient data in code, tests, fixtures or logs.
