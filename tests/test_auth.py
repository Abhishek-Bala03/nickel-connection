"""Service-to-service authentication (src.services.auth): every route but the probes needs the token."""

import dataclasses
import logging
import os

import pytest
from fastapi.testclient import TestClient

import main
from src.services.auth import MIN_TOKEN_CHARS, check_token

# The token tests/conftest.py starts the app on.
SERVICE_TOKEN = os.environ["NICKEL_SERVICE_TOKEN"]
PROBES = {("GET", "/health"), ("GET", "/ready")}


@pytest.fixture
def app(settings):
    return main.create_app(settings, republish_at_start=False)


@pytest.fixture
def anonymous(app):
    """A client that sends no Authorization header at all."""
    with TestClient(app) as client:
        yield client


def _example_path(path: str) -> str:
    return path.replace("{job_id}", "nope")


def test_every_route_but_the_probes_refuses_a_request_without_a_token(app, anonymous):
    # Every path and method the API documents (the schema exists even where /docs is off).
    answered = {}
    for path, operations in app.openapi()["paths"].items():
        for method in operations:
            response = anonymous.request(method, _example_path(path))
            answered[(method.upper(), path)] = response.status_code
    assert len(answered) >= 7, answered  # the whole API, not a subset
    assert {key for key, status in answered.items() if status != 401} == PROBES


def test_a_missing_token_is_a_401_in_the_error_shape_with_the_challenge(anonymous):
    response = anonymous.get("/jobs", headers={"X-Request-ID": "r-401"})
    assert response.status_code == 401
    assert response.headers["WWW-Authenticate"] == "Bearer"
    assert response.json() == {
        "error": {
            "code": "unauthorized",
            "message": "A valid service token is required",
        },
        "request_id": "r-401",
    }


@pytest.mark.parametrize(
    "authorization",
    [
        "Bearer wrong-token-0123456789abcdef0123456789abcdef",  # same length, wrong value
        f"Bearer {SERVICE_TOKEN}x",
        f"Bearer {SERVICE_TOKEN[:-1]}",
        f"Basic {SERVICE_TOKEN}",  # the right secret under the wrong scheme
        "Bearer",
        "Bearer ",
        "",
    ],
)
def test_a_wrong_token_or_scheme_is_a_401(anonymous, authorization):
    response = anonymous.get("/version", headers={"Authorization": authorization})
    assert response.status_code == 401
    assert response.headers["WWW-Authenticate"] == "Bearer"


@pytest.mark.parametrize("scheme", ["Bearer", "bearer", "BEARER"])
def test_the_token_opens_every_route_whatever_the_case_of_the_scheme(anonymous, scheme):
    headers = {"Authorization": f"{scheme} {SERVICE_TOKEN}"}
    assert anonymous.get("/version", headers=headers).status_code == 200
    assert anonymous.get("/jobs", headers=headers).json() == {"jobs": []}
    assert anonymous.get("/documents", headers=headers).json() == {"documents": []}


def test_the_probes_answer_without_a_token(anonymous):
    assert anonymous.get("/health").json() == {"status": "ok"}
    assert anonymous.get("/ready").status_code == 200


def test_the_token_is_checked_before_the_request_body(anonymous):
    # A 401 says nothing about what the route would have accepted; and the token check does
    # not depend on the caller sending a valid body first.
    response = anonymous.post("/jobs/documents/pull", json={"surprise": 1})
    assert response.status_code == 401
    assert "surprise" not in response.text


def test_a_presented_token_is_never_logged_or_echoed(anonymous, records):
    presented = "leaked-token-0123456789abcdef0123456789abcdef"
    response = anonymous.get(
        "/jobs", headers={"Authorization": f"Bearer {presented}", "X-Request-ID": "r-1"}
    )
    assert response.status_code == 401
    assert presented not in response.text
    assert all(presented not in record.getMessage() for record in records)
    assert [
        r for r in records if r.levelno >= logging.ERROR
    ] == []  # a 401 is not an error


def test_the_service_does_not_start_without_a_token(settings):
    with pytest.raises(ValueError, match="NICKEL_SERVICE_TOKEN .*is not set"):
        main.create_app(dataclasses.replace(settings, service_token=""))


def test_the_service_does_not_start_with_a_short_token(settings):
    short = "x" * (MIN_TOKEN_CHARS - 1)
    with pytest.raises(ValueError, match=f"shorter than {MIN_TOKEN_CHARS} characters"):
        main.create_app(dataclasses.replace(settings, service_token=short))
    check_token("x" * MIN_TOKEN_CHARS)  # the shortest token that passes


def test_a_malformed_body_without_a_token_is_a_401_not_a_422(anonymous):
    # Before the token check ran ahead of routing, FastAPI parsed the body first and a caller with no
    # token got a 422 describing the request's shape.
    response = anonymous.post(
        "/jobs/documents/pull",
        content=b'{"manifest": ',
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 401
    assert response.json()["error"]["code"] == "unauthorized"


def test_an_unknown_path_without_a_token_is_a_401_not_a_404(anonymous):
    response = anonymous.get("/no-such-route")
    assert response.status_code == 401
    assert response.headers["WWW-Authenticate"] == "Bearer"


def test_a_token_still_reaches_validation_and_routing(app):
    headers = {
        "Authorization": f"Bearer {SERVICE_TOKEN}",
        "Content-Type": "application/json",
    }
    with TestClient(app) as client:
        assert (
            client.post(
                "/jobs/documents/pull", content=b'{"manifest": ', headers=headers
            ).status_code
            == 422
        )
        assert client.get("/no-such-route", headers=headers).status_code == 404


@pytest.mark.parametrize(
    ("environment", "status"),
    [("development", 200), ("qa", 401), ("production", 401)],
)
def test_the_docs_pages_are_open_only_where_docs_are_on(settings, environment, status):
    with TestClient(
        main.create_app(dataclasses.replace(settings, environment=environment))
    ) as client:
        assert client.get("/docs").status_code == status


def test_the_route_dependency_still_refuses_without_the_middleware(settings):
    # Defence in depth: were the middleware ever left out of an app, every router still checks.
    from fastapi import FastAPI

    from src.error import register_error_handlers
    from src.services.auth import ServiceToken

    bare = FastAPI()
    bare.state.settings = settings
    register_error_handlers(bare)

    @bare.get("/guarded", dependencies=[ServiceToken])
    def guarded():
        return {}

    with TestClient(bare) as client:
        assert client.get("/guarded").status_code == 401
        ok = client.get(
            "/guarded", headers={"Authorization": f"Bearer {SERVICE_TOKEN}"}
        )
        assert ok.status_code == 200


@pytest.mark.parametrize("path", ["/health", "/jobs"])
def test_every_answer_carries_the_security_headers(anonymous, path):
    # /health answers 200 and /jobs a 401 from the token check: both carry them.
    response = anonymous.get(path)
    assert response.headers["X-Content-Type-Options"] == "nosniff"
    assert response.headers["X-Frame-Options"] == "DENY"
    assert response.headers["Cache-Control"] == "no-store"
    assert response.headers["Referrer-Policy"] == "no-referrer"


def test_a_header_the_route_set_itself_is_kept():
    import asyncio

    from src.middlewares import SecurityHeadersMiddleware

    async def app(scope, receive, send):
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"cache-control", b"max-age=60")],
            }
        )
        await send({"type": "http.response.body", "body": b""})

    sent = []

    async def send(message):
        sent.append(message)

    async def receive():  # pragma: no cover - the app never reads
        return {"type": "http.request"}

    asyncio.run(SecurityHeadersMiddleware(app)({"type": "http"}, receive, send))
    headers = dict(sent[0]["headers"])
    assert headers[b"cache-control"] == b"max-age=60"
    assert headers[b"x-content-type-options"] == b"nosniff"
    lifespan = []
    asyncio.run(
        SecurityHeadersMiddleware(lambda s, r, snd: _record(lifespan, s))(
            {"type": "lifespan"}, receive, send
        )
    )
    assert lifespan == ["lifespan"]


async def _record(seen, scope):
    seen.append(scope["type"])
