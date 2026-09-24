"""A request body has a cap (src.middlewares.request_size): over it, a 413 before the body is read."""

import asyncio
import os

import httpx

from src.middlewares.request_size import MAX_REQUEST_BYTES

AUTH = {"Authorization": f"Bearer {os.environ['NICKEL_SERVICE_TOKEN']}"}
JSON = {"Content-Type": "application/json"}
CHUNK = 1024
MESSAGE = f"The request body is over {MAX_REQUEST_BYTES} bytes"


def test_a_body_declared_over_the_cap_is_a_413_in_the_error_shape(client):
    response = client.post(
        "/jobs/documents/pull",
        content=b"x" * (MAX_REQUEST_BYTES + 1),
        headers={**JSON, "X-Request-ID": "r-413"},
    )
    assert response.status_code == 413
    assert response.json() == {
        "error": {"code": "too_large", "message": MESSAGE},
        "request_id": "r-413",
    }


def test_a_body_at_the_cap_still_reaches_the_route(client):
    # Junk of exactly the cap: read in full, and refused by validation, not by the cap.
    response = client.post(
        "/jobs/documents/pull", content=b"x" * MAX_REQUEST_BYTES, headers=JSON
    )
    assert response.status_code == 422


def test_a_streamed_body_over_the_cap_is_a_413_and_stops_being_read(client):
    # No Content-Length (chunked), from a client that keeps sending well past the cap: the bytes
    # are counted as they arrive, and the app stops taking them at the cap instead of buffering
    # them all.
    pulled = 0

    async def endless():
        nonlocal pulled
        for _ in range(4 * MAX_REQUEST_BYTES // CHUNK):
            pulled += 1
            yield b"x" * CHUNK

    async def scenario():
        transport = httpx.ASGITransport(app=client.app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://t", headers=AUTH
        ) as http:
            return await http.post(
                "/jobs/documents/pull", content=endless(), headers=JSON
            )

    response = asyncio.run(scenario())
    assert response.status_code == 413
    assert response.json()["error"] == {"code": "too_large", "message": MESSAGE}
    assert pulled <= MAX_REQUEST_BYTES // CHUNK + 1


def test_an_unauthenticated_request_over_the_cap_is_still_a_401(client):
    # The cap sits inside the token check: the size of a request says nothing to a caller
    # without the token.
    response = client.post(
        "/jobs/documents/pull",
        content=b"x" * (MAX_REQUEST_BYTES + 1),
        headers={**JSON, "Authorization": ""},
    )
    assert response.status_code == 401


def _drive(chunks):
    """Run the middleware on a raw ASGI request with no Content-Length whose body arrives in
    ``chunks``, with an app that reads every chunk; returns the bytes the app got."""
    from src.middlewares.request_size import RequestSizeLimitMiddleware

    queue = [
        {"type": "http.request", "body": chunk, "more_body": i < len(chunks) - 1}
        for i, chunk in enumerate(chunks)
    ]
    got = []

    async def receive():
        return queue.pop(0)

    async def app(scope, receive, send):
        while True:
            message = await receive()
            got.append(message["body"])
            if not message["more_body"]:
                return

    async def send(message):  # pragma: no cover - the app sends nothing
        raise AssertionError(message)

    scope = {"type": "http", "headers": []}
    asyncio.run(RequestSizeLimitMiddleware(app)(scope, receive, send))
    return b"".join(got)


def test_a_streamed_body_under_the_cap_reaches_the_app_whole():
    # Driven directly, so the count holds whatever length header a test client adds.
    body = [b"x" * (MAX_REQUEST_BYTES // 2), b"y" * (MAX_REQUEST_BYTES // 2)]
    assert _drive(body) == b"".join(body)


def test_a_streamed_body_stops_at_the_chunk_that_crosses_the_cap():
    import pytest
    from starlette.exceptions import HTTPException

    with pytest.raises(HTTPException) as refused:
        _drive([b"x" * MAX_REQUEST_BYTES, b"y", b"never read"])
    assert refused.value.status_code == 413
