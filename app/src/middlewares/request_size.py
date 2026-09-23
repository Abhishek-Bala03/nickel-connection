"""Cap the request body (``MAX_REQUEST_BYTES``): a request over it is a 413 before the body is read.

The routes take small JSON bodies (a pull request is a manifest id and a limit), and uvicorn puts
no limit on a body, so without this a caller with the token could send gigabytes and FastAPI would
hold every byte in memory to parse it. The cap is checked against ``Content-Length`` first, so a body
that says it is too big is refused before a byte of it is read, and then on the bytes as they
arrive, so a chunked or lying body stops at the cap instead of being buffered. It runs inside the
token check (``main.create_app``): an unauthenticated request stays a 401 whatever its size.
"""

from starlette.exceptions import HTTPException
from starlette.requests import Request

from src.error import handle_http_error

MAX_REQUEST_BYTES = 64 * 1024


def _too_large() -> HTTPException:
    # Starlette's HTTPException, not a NickelConnectionError: raised from the body read, FastAPI
    # re-raises an HTTPException as it is and turns any other exception into a 400 "error parsing
    # the body". src.error shapes it like every other error (413, code too_large).
    return HTTPException(
        status_code=413, detail=f"The request body is over {MAX_REQUEST_BYTES} bytes"
    )


def _declared_length(scope) -> int | None:
    """The ``Content-Length`` of an ASGI scope, or None (the server has checked it is a number)."""
    for name, value in scope.get("headers", ()):
        if name == b"content-length":
            return int(value)
    return None


class RequestSizeLimitMiddleware:
    def __init__(self, app, max_bytes: int = MAX_REQUEST_BYTES) -> None:
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        declared = _declared_length(scope)
        if declared is not None and declared > self.max_bytes:
            response = handle_http_error(Request(scope), _too_large())
            await response(scope, receive, send)
            return
        received = 0

        async def limited_receive():
            nonlocal received
            message = await receive()  # only http.request carries a body
            received += len(message.get("body", b""))
            if received > self.max_bytes:
                raise _too_large()
            return message

        await self.app(scope, limited_receive, send)
