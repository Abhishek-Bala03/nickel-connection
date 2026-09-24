FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

RUN groupadd --system --gid 999 nickel \
    && useradd --system --uid 999 --gid nickel \
       --no-create-home --shell /bin/false nickel \
    && mkdir -p /data \
    && chown nickel:nickel /data

COPY pyproject.toml uv.lock ./

RUN pip install --no-cache-dir uv==0.12.0 \
    && uv export --frozen --no-dev --no-emit-project \
       -o /tmp/requirements.txt \
    && uv pip install --system --no-cache --require-hashes \
       -r /tmp/requirements.txt \
    && pip uninstall -y uv \
    && rm -f /tmp/requirements.txt pyproject.toml uv.lock

COPY --chown=nickel:nickel app/ ./

USER 999:999

ENV NICKEL_SCHEDULER_DB=/data/scheduler.db

EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=2).close()"]

ENTRYPOINT ["python", "-m", "uvicorn"]

CMD ["nickel_connection.scheduler.api:create_app", \
     "--factory", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
