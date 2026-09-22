# Only app/ goes into the image; the Dockerfile, docs and tests stay outside it (team standard).
#   docker build -t nickel-connection .
FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates; \
    apt-get clean; \
    rm -rf /var/lib/apt/lists/*

# Non-root runtime user (uid 999), as in the team's service images.
RUN useradd --system --no-create-home --uid 999 --shell /bin/false nickel

WORKDIR /app

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

ENTRYPOINT ["python", "-m", "nickel_connection"]
