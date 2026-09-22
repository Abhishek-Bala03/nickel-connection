# Only app/ goes into the image; the Dockerfile, docs and tests stay outside it (team standard).
#   docker build --build-arg APP_VERSION="$(git describe --tags --always)" -t nickel-connection .
FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# System packages, then the non-root runtime user (uid 999, as in the team's service images).
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates; \
    apt-get clean; \
    rm -rf /var/lib/apt/lists/*; \
    useradd --system --no-create-home --uid 999 --shell /bin/false nickel

# Build identity baked into the image, as in Rockwell; /version reports it and an env var overrides it.
ARG APP_VERSION=unknown
ENV APP_VERSION=${APP_VERSION}

WORKDIR /app
# The app root is importable in every process, so `src` and `conf` always resolve (as in Rockwell).
ENV PYTHONPATH=/app

# Dependencies first, so they stay cached between code changes. uv is pinned and installed with pip
# (not by piping an install script into a shell); the lock file's hashes are verified on install.
COPY pyproject.toml uv.lock ./
RUN pip install --no-cache-dir uv==0.12.0 \
    && uv export --frozen --no-dev --no-emit-project -o /tmp/requirements.txt \
    && uv pip install --system --no-cache -r /tmp/requirements.txt \
    && pip uninstall -y uv \
    && rm -f /tmp/requirements.txt pyproject.toml uv.lock

COPY --chown=nickel:nickel app/ .

USER 999
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=3)"]
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
