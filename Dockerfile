# app/ and the reviewed contract artifacts go into the image; docs and tests stay outside it.
#   docker build --build-arg APP_VERSION="$(git describe --tags --always)" -t nickel-connection .
# The base image is python:3.11-slim, pinned by the digest of its multi-arch index on 2026-09-23, so a
# build gets the bytes that were reviewed, not whatever the tag points at that day (digest only, not
# tag@digest: SonarQube's docker:S8431). To move it to the tag's current image:
#   docker buildx imagetools inspect python:3.11-slim   (or the registry API's Docker-Content-Digest)
FROM python@sha256:da047cb8f9d1d98e5c070f5300ba9f7274e33b8fc0e5be5ed88740aed1b95ba9

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# System packages, then the non-root runtime user (uid 999, as in the team's service images).
# /data is the only writable place the service needs (File Store, the raw_data outbox, receipts,
# job records, download caches); a named volume mounted there inherits this ownership.
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates; \
    apt-get clean; \
    rm -rf /var/lib/apt/lists/*; \
    useradd --system --no-create-home --uid 999 --shell /bin/false nickel; \
    mkdir -p /data; \
    chown nickel:nickel /data

WORKDIR /app
# `src` is importable from any working directory, e.g. for `python -m src.cli` or a scheduler process
# (uvicorn started from /app would find it anyway).
ENV PYTHONPATH=/app
# Runtime data and the source libraries' download caches all live under /data.
ENV NICKEL_DATA_DIR=/data \
    HF_HOME=/data/.cache/huggingface \
    KAGGLEHUB_CACHE=/data/.cache/kagglehub

# Dependencies first, so they stay cached between code changes. uv is pinned and installed with pip
# (not by piping an install script into a shell); the lock file's hashes are verified on install.
# The development sample-data sources (kagglehub, huggingface-hub: the dev-sources group in
# pyproject.toml) stay out of the production image; compose.yaml builds with DEV_SOURCES=1 so a local
# image can pull the Kaggle and Hugging Face manifests.
ARG DEV_SOURCES=
COPY pyproject.toml uv.lock ./
RUN pip install --no-cache-dir uv==0.12.0 \
    && uv export --frozen --no-dev --no-emit-project ${DEV_SOURCES:+--group dev-sources} -o /tmp/requirements.txt \
    && uv pip install --system --no-cache -r /tmp/requirements.txt \
    && pip uninstall -y uv \
    && rm -f /tmp/requirements.txt pyproject.toml uv.lock

# Build identity, as in Rockwell: /version reports it and an env var overrides it. It comes after the
# dependencies, because a changed build argument invalidates the cache of every step below it.
ARG APP_VERSION=unknown
ENV APP_VERSION=${APP_VERSION}

# Owned by root (the runtime user can read, not write), so the service cannot rewrite its own code.
# app/ holds the code, conf/application.conf and manifests/documents/*.yaml.
COPY app/ .
COPY schemas/ ./contracts/schemas/
COPY connectors/ ./contracts/examples/

USER 999
EXPOSE 8000
# No proxy for the local probe, even when HTTP(S)_PROXY is set for vendor calls.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD ["python", "-c", "import urllib.request as u; u.build_opener(u.ProxyHandler({})).open('http://127.0.0.1:8000/health', timeout=3)"]
# One process: the job worker runs inside it (docs/jobs-api.md). Override the command for a one-off
# CLI run, e.g. `python -m src.cli documents status` (docs/docker.md). No `Server: uvicorn` header:
# a response names no software for a scanner to look up.
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--no-server-header"]
