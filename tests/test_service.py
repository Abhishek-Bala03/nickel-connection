"""The service as a caller sees it: routes, request ids, timing, errors, CORS, API docs."""

import asyncio
import dataclasses
import logging
import re
import threading

import anyio
import httpx
import pytest
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.testclient import TestClient

import main
from src.config import Service
from src.error import BadRequestError, ConflictError, NotFoundError, UpstreamError
from src.logging import logger
from src.middlewares import request_timing

UI = "http://localhost:3000"


@pytest.fixture
def records():
    """Log records from the service logger (it does not propagate, so caplog cannot see them)."""
    captured: list[logging.LogRecord] = []
    handler = logging.Handler()
    handler.emit = captured.append
    logger.addHandler(handler)
    yield captured
    logger.removeHandler(handler)


def _errors(records: list[logging.LogRecord]) -> list[logging.LogRecord]:
    return [r for r in records if r.levelno >= logging.ERROR]


def _service(monkeypatch=None, **settings) -> FastAPI:
    """The real app, with every middleware and handler; settings override the service settings."""
    if settings:
        monkeypatch.setattr(main, "Service", dataclasses.replace(Service, **settings))
    return main.create_app()


def _raising(error: Exception, monkeypatch=None, **settings) -> TestClient:
    app = _service(monkeypatch, **settings)

    @app.get("/boom")
    def boom():
        raise error

    return TestClient(app, raise_server_exceptions=False)


def test_health(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_ready(client):
    assert client.get("/ready").json() == {"status": "ready", "checks": {}}


def test_version_reports_the_service(client):
    assert client.get("/version").json() == {
        "name": Service.name,
        "version": Service.version,
        "environment": Service.environment,
    }


def test_health_answers_while_blocking_routes_hold_every_worker_thread():
    app = _service()
    release = threading.Event()

    @app.get("/blocking")
    def blocking():  # e.g. a connector waiting on a vendor
        release.wait(10)
        return {}

    async def scenario():
        limiter = anyio.to_thread.current_default_thread_limiter()
        limiter.total_tokens = 2
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as http:
            blockers = [asyncio.create_task(http.get("/blocking")) for _ in range(2)]
            try:
                with anyio.fail_after(5):
                    while limiter.borrowed_tokens < 2:
                        await asyncio.sleep(0.01)
                with anyio.fail_after(1):  # a sync probe would wait for a thread here
                    return [
                        (await http.get(p)).status_code for p in ("/health", "/ready")
                    ]
            finally:
                release.set()
                await asyncio.gather(*blockers)

    assert asyncio.run(scenario()) == [200, 200]


def test_every_response_gets_a_request_id_and_its_time(client):
    response = client.get("/health")
    assert re.fullmatch(r"[0-9a-f]{32}", response.headers["X-Request-ID"])
    assert re.fullmatch(r"\d+\.\dms", response.headers["X-Response-Time"])


def test_a_plain_caller_request_id_is_kept(client):
    for request_id in ("ui-7f3a.1", "a" * 64):
        response = client.get("/health", headers={"X-Request-ID": request_id})
        assert response.headers["X-Request-ID"] == request_id


def test_an_odd_or_long_caller_request_id_is_replaced(client):
    for request_id in ("bad id\twith tabs", "a" * 65):
        response = client.get("/health", headers={"X-Request-ID": request_id})
        assert re.fullmatch(r"[0-9a-f]{32}", response.headers["X-Request-ID"])


def test_slow_requests_are_logged(client, monkeypatch, records):
    monkeypatch.setattr(request_timing, "SLOW_REQUEST_MS", 0.0)
    client.get("/health", headers={"X-Request-ID": "r-slow"})
    [record] = [r for r in records if r.levelno == logging.WARNING]
    assert re.fullmatch(
        r"\[SLOW_REQUEST\] GET /health took \d+ms \(request r-slow\)",
        record.getMessage(),
    )


@pytest.mark.parametrize(
    ("error", "status", "code"),
    [
        (BadRequestError("bad input"), 400, "bad_request"),
        (NotFoundError("no such connection"), 404, "not_found"),
        (ConflictError("already exists"), 409, "conflict"),
        (UpstreamError("vendor down"), 502, "upstream_error"),
    ],
)
def test_service_errors_become_json_with_their_status(error, status, code):
    response = _raising(error).get("/boom", headers={"X-Request-ID": "r-1"})
    assert response.status_code == status
    assert response.json() == {
        "error": {"code": code, "message": error.message},
        "request_id": "r-1",
    }


def test_only_5xx_service_errors_are_logged(records):
    _raising(BadRequestError("bad input")).get("/boom")
    assert _errors(records) == []
    _raising(UpstreamError("vendor down")).get("/boom", headers={"X-Request-ID": "r-1"})
    [record] = _errors(records)
    assert (
        record.getMessage() == "upstream_error on GET /boom (request r-1): vendor down"
    )


def test_an_unexpected_error_is_a_plain_500_with_the_request_id():
    response = _raising(RuntimeError("secret internals")).get(
        "/boom", headers={"X-Request-ID": "r-500"}
    )
    assert response.status_code == 500
    assert response.json() == {
        "error": {"code": "internal_error", "message": "Internal server error"},
        "request_id": "r-500",
    }
    assert "secret internals" not in response.text
    # The middlewares still wrap it: the caller can quote the id, and the time is measured.
    assert response.headers["X-Request-ID"] == "r-500"
    assert response.headers["X-Response-Time"].endswith("ms")


def test_an_unexpected_error_is_logged_once_with_its_traceback(records):
    _raising(RuntimeError("secret internals")).get(
        "/boom", headers={"X-Request-ID": "r-500"}
    )
    [record] = _errors(records)
    assert record.getMessage() == "unhandled error on GET /boom (request r-500)"
    assert record.exc_info[0] is RuntimeError
    assert "secret internals" in logging.Formatter().formatException(record.exc_info)


def test_an_error_after_the_response_started_is_not_answered_twice():
    app = _service()

    def broken_stream():
        yield b"partial"
        raise RuntimeError("stream broke")

    @app.get("/stream")
    def stream():
        return StreamingResponse(broken_stream())

    client = TestClient(app)
    # The status line is already out, so the error goes up to the server instead of a second reply.
    with pytest.raises(RuntimeError, match="stream broke"):
        client.get("/stream")


def test_an_unexpected_error_keeps_the_cors_headers(monkeypatch):
    client = _raising(RuntimeError("x"), monkeypatch, cors_origins=(UI,))
    response = client.get("/boom", headers={"Origin": UI})
    assert response.status_code == 500
    assert response.headers["access-control-allow-origin"] == UI


def test_an_unknown_route_uses_the_error_shape(client):
    response = client.get("/nope", headers={"X-Request-ID": "r-2"})
    assert response.status_code == 404
    assert response.json() == {
        "error": {"code": "not_found", "message": "Not Found"},
        "request_id": "r-2",
    }


def test_a_wrong_method_uses_the_error_shape_and_says_what_is_allowed(client):
    response = client.post("/health")
    assert response.status_code == 405
    assert response.json()["error"]["code"] == "method_not_allowed"
    assert "GET" in response.headers["Allow"]


@pytest.mark.parametrize(
    ("status", "headers", "code"),
    [
        (401, {"WWW-Authenticate": "Bearer"}, "unauthorized"),
        (429, {"Retry-After": "30"}, "too_many_requests"),
    ],
)
def test_http_errors_keep_their_headers(status, headers, code):
    error = HTTPException(status_code=status, detail="try later", headers=headers)
    response = _raising(error).get("/boom")
    assert response.status_code == status
    assert response.json()["error"] == {"code": code, "message": "try later"}
    for name, value in headers.items():
        assert response.headers[name] == value


def test_a_5xx_http_error_hides_its_detail_and_logs_it(records):
    detail = 'psycopg.errors.UndefinedTable: relation "patients" does not exist'
    error = HTTPException(status_code=503, detail=detail)
    response = _raising(error).get("/boom", headers={"X-Request-ID": "r-503"})
    assert response.status_code == 503
    assert response.json()["error"] == {
        "code": "unavailable",
        "message": "Service Unavailable",
    }
    assert "patients" not in response.text
    [record] = _errors(records)
    assert record.getMessage() == f"503 on GET /boom (request r-503): {detail}"


def test_a_structured_http_error_detail_stays_json():
    error = HTTPException(status_code=400, detail={"field": "since", "why": "future"})
    response = _raising(error).get("/boom")
    assert response.json()["error"] == {
        "code": "bad_request",
        "message": "Bad Request",
        "details": {"field": "since", "why": "future"},
    }


def test_other_http_errors_get_a_generic_code():
    response = _raising(HTTPException(status_code=418, detail="teapot")).get("/boom")
    assert response.status_code == 418
    assert response.json()["error"] == {"code": "http_error", "message": "teapot"}
    response = _raising(HTTPException(status_code=599, detail="internals")).get("/boom")
    assert response.status_code == 599
    assert response.json()["error"] == {"code": "http_error", "message": "Error"}


def test_a_not_modified_error_has_no_body():
    response = _raising(HTTPException(status_code=304)).get("/boom")
    assert response.status_code == 304
    assert response.content == b""


def test_an_invalid_request_uses_the_error_shape_and_does_not_echo_the_input():
    app = _service()

    @app.get("/items/{item_id}")
    def item(item_id: int):
        return {"item_id": item_id}

    response = TestClient(app).get("/items/not-a-number-SECRET")
    assert response.status_code == 422
    error = response.json()["error"]
    assert error["code"] == "validation_error"
    assert error["details"] == [
        {"field": "path.item_id", "problem": error["details"][0]["problem"]}
    ]
    assert "SECRET" not in response.text


def test_cors_is_off_unless_origins_are_configured(client):
    response = client.options(
        "/health", headers={"Origin": UI, "Access-Control-Request-Method": "GET"}
    )
    assert response.status_code == 405
    assert "access-control-allow-origin" not in response.headers


def test_cors_allows_the_configured_origins_and_shows_them_the_request_id(monkeypatch):
    client = TestClient(_service(monkeypatch, cors_origins=(UI,)))
    preflight = client.options(
        "/health", headers={"Origin": UI, "Access-Control-Request-Method": "GET"}
    )
    assert preflight.headers["access-control-allow-origin"] == UI
    response = client.get("/health", headers={"Origin": UI})
    exposed = response.headers["access-control-expose-headers"]
    assert "X-Request-ID" in exposed
    assert "X-Response-Time" in exposed
    other = client.get("/health", headers={"Origin": "http://evil.example"})
    assert "access-control-allow-origin" not in other.headers


@pytest.mark.parametrize(
    ("environment", "status"), [("development", 200), ("qa", 200), ("production", 404)]
)
def test_api_docs_are_off_in_production(monkeypatch, environment, status):
    client = TestClient(_service(monkeypatch, environment=environment))
    for path in ("/docs", "/redoc", "/openapi.json"):
        assert client.get(path).status_code == status
