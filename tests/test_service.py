"""The service as a caller sees it: routes, request ids, timing, errors, CORS."""

import dataclasses
import re
from unittest import mock

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

import main
from src.config import Service
from src.error import (
    BadRequestError,
    ConflictError,
    NotFoundError,
    UpstreamError,
    register_error_handlers,
)
from src.middlewares import RequestIdMiddleware, request_timing


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


def test_every_response_gets_a_request_id_and_its_time(client):
    response = client.get("/health")
    assert re.fullmatch(r"[0-9a-f]{32}", response.headers["X-Request-ID"])
    assert response.headers["X-Response-Time"].endswith("ms")


def test_a_plain_caller_request_id_is_kept(client):
    response = client.get("/health", headers={"X-Request-ID": "ui-7f3a.1"})
    assert response.headers["X-Request-ID"] == "ui-7f3a.1"


def test_an_odd_caller_request_id_is_replaced(client):
    response = client.get("/health", headers={"X-Request-ID": "bad id\twith tabs"})
    assert re.fullmatch(r"[0-9a-f]{32}", response.headers["X-Request-ID"])


def test_slow_requests_are_logged(client, monkeypatch):
    monkeypatch.setattr(request_timing, "SLOW_REQUEST_MS", 0.0)
    with mock.patch.object(request_timing.logger, "warning") as warning:
        client.get("/health")
    warning.assert_called_once()
    assert "[SLOW_REQUEST]" in warning.call_args.args[0]


def _app_raising(error: Exception) -> TestClient:
    app = FastAPI()
    app.add_middleware(RequestIdMiddleware)
    register_error_handlers(app)

    @app.get("/boom")
    def boom():
        raise error

    return TestClient(app, raise_server_exceptions=False)


def test_service_errors_become_json_with_their_status():
    for error, status, code in [
        (BadRequestError("bad input"), 400, "bad_request"),
        (NotFoundError("no such connection"), 404, "not_found"),
        (ConflictError("already exists"), 409, "conflict"),
        (UpstreamError("vendor down"), 502, "upstream_error"),
    ]:
        response = _app_raising(error).get("/boom", headers={"X-Request-ID": "r-1"})
        assert response.status_code == status
        assert response.json() == {
            "error": {"code": code, "message": error.message},
            "request_id": "r-1",
        }


def test_unexpected_errors_become_a_plain_500_and_are_logged():
    with mock.patch("src.error.logger.exception") as logged:
        response = _app_raising(RuntimeError("secret internals")).get("/boom")
    assert response.status_code == 500
    assert response.json()["error"] == {
        "code": "internal_error",
        "message": "Internal server error",
    }
    assert "secret internals" not in response.text
    logged.assert_called_once()


def test_an_unknown_route_uses_the_error_shape(client):
    response = client.get("/nope", headers={"X-Request-ID": "r-2"})
    assert response.status_code == 404
    assert response.json() == {
        "error": {"code": "not_found", "message": "Not Found"},
        "request_id": "r-2",
    }


def test_a_wrong_method_uses_the_error_shape(client):
    response = client.post("/health")
    assert response.status_code == 405
    assert response.json()["error"]["code"] == "method_not_allowed"


def test_other_http_errors_get_a_generic_code():
    response = _app_raising(HTTPException(status_code=418, detail="teapot")).get(
        "/boom"
    )
    assert response.status_code == 418
    assert response.json()["error"] == {"code": "http_error", "message": "teapot"}


def test_an_invalid_request_uses_the_error_shape():
    app = FastAPI()
    register_error_handlers(app)

    @app.get("/items/{item_id}")
    def item(item_id: int):
        return {"item_id": item_id}

    response = TestClient(app).get("/items/not-a-number")
    assert response.status_code == 422
    error = response.json()["error"]
    assert error["code"] == "validation_error"
    assert error["details"][0]["field"] == "path.item_id"


def test_cors_is_off_unless_origins_are_configured(client):
    response = client.get("/health", headers={"Origin": "http://evil.example"})
    assert "access-control-allow-origin" not in response.headers


def test_cors_allows_the_configured_origins(monkeypatch):
    with_ui = dataclasses.replace(Service, cors_origins=("http://localhost:3000",))
    monkeypatch.setattr(main, "Service", with_ui)
    client = TestClient(main.create_app())
    response = client.options(
        "/health",
        headers={
            "Origin": "http://localhost:3000",
            "Access-Control-Request-Method": "GET",
        },
    )
    assert response.headers["access-control-allow-origin"] == "http://localhost:3000"
