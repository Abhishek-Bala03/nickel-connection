"""Errors the service raises on purpose, and the handlers that turn every error into the same JSON shape.

Raise one of these from a handler; the client gets ``{"error": {"code", "message"}, "request_id"}``
with the matching status, and the headers the status calls for (``WWW-Authenticate`` on a 401).
Framework errors (unknown route, wrong method, invalid request) use the same shape and keep their
headers (``Allow``, ``WWW-Authenticate``, ``Retry-After``). Anything unexpected, and any 5xx detail,
becomes a generic message: stack traces and internals never reach the client, they go to the log with
the request id.
"""

from __future__ import annotations

from collections.abc import Mapping
from http import HTTPStatus

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from starlette.exceptions import HTTPException

from src.logging import logger

_HTTP_CODES = {
    400: "bad_request",
    401: "unauthorized",
    403: "forbidden",
    404: "not_found",
    405: "method_not_allowed",
    409: "conflict",
    413: "too_large",
    415: "unsupported_media_type",
    429: "too_many_requests",
    500: "internal_error",
    502: "upstream_error",
    503: "unavailable",
    504: "upstream_timeout",
}
_NO_BODY = (204, 304)


class NickelConnectionError(Exception):
    status_code = 500
    code = "internal_error"
    # Sent with the response: WWW-Authenticate on a 401, Retry-After on a 429.
    headers: Mapping[str, str] | None = None

    def __init__(self, message: str = "Something went wrong") -> None:
        super().__init__(message)
        self.message = message


class BadRequestError(NickelConnectionError):
    status_code = 400
    code = "bad_request"


class UnauthorizedError(NickelConnectionError):
    """The request carries no valid service token (``src.services.auth``). One message for a missing
    and for a wrong token: the response says nothing about which, or about the token itself.
    """

    status_code = 401
    code = "unauthorized"
    headers = {"WWW-Authenticate": "Bearer"}

    def __init__(self, message: str = "A valid service token is required") -> None:
        super().__init__(message)


class NotFoundError(NickelConnectionError):
    status_code = 404
    code = "not_found"


class ConflictError(NickelConnectionError):
    status_code = 409
    code = "conflict"


class TooManyRequestsError(NickelConnectionError):
    """The caller must wait: the job queue is full. ``Retry-After`` says for how long, in seconds."""

    status_code = 429
    code = "too_many_requests"

    def __init__(self, message: str, retry_after_s: int) -> None:
        super().__init__(message)
        self.headers = {"Retry-After": str(retry_after_s)}


class UpstreamError(NickelConnectionError):
    """A source system or vendor API failed."""

    status_code = 502
    code = "upstream_error"


class UnavailableError(NickelConnectionError):
    """Something the operation needs is not there, such as a setting (``NICKEL_TENANT_ID``). The
    request is fine; the deployment is not ready for it.
    """

    status_code = 503
    code = "unavailable"


def _request_id(request: Request) -> str | None:
    return getattr(request.state, "request_id", None)


def _phrase(status: int) -> str:
    try:
        return HTTPStatus(status).phrase
    except ValueError:
        return "Error"


def _response(
    request: Request,
    status: int,
    code: str,
    message: str,
    headers: Mapping[str, str] | None = None,
    **extra,
) -> JSONResponse:
    error = {"code": code, "message": message, **extra}
    body = {"error": error, "request_id": _request_id(request)}
    return JSONResponse(status_code=status, content=body, headers=headers)


def handle_service_error(request: Request, exc: NickelConnectionError) -> JSONResponse:
    if exc.status_code >= 500:
        logger.error(
            "%s on %s %s (request %s): %s",
            exc.code,
            request.method,
            request.url.path,
            _request_id(request),
            exc.message,
        )
    return _response(request, exc.status_code, exc.code, exc.message, exc.headers)


def handle_http_error(request: Request, exc: HTTPException) -> Response:
    headers = getattr(exc, "headers", None)
    if exc.status_code in _NO_BODY:
        return Response(status_code=exc.status_code, headers=headers)
    code = _HTTP_CODES.get(exc.status_code, "http_error")
    extra = {}
    if exc.status_code >= 500:
        # The detail may carry internals (SQL, paths, vendor replies): log it, send the phrase.
        logger.error(
            "%s on %s %s (request %s): %s",
            exc.status_code,
            request.method,
            request.url.path,
            _request_id(request),
            exc.detail,
        )
        message = _phrase(exc.status_code)
    elif isinstance(exc.detail, str):
        message = exc.detail
    else:
        message, extra = _phrase(exc.status_code), {"details": exc.detail}
    return _response(request, exc.status_code, code, message, headers, **extra)


def handle_validation_error(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    # Field and problem only: FastAPI's default also echoes the rejected input back.
    details = [
        {"field": ".".join(str(part) for part in e["loc"]), "problem": e["msg"]}
        for e in exc.errors()
    ]
    return _response(
        request, 422, "validation_error", "The request is not valid", details=details
    )


def handle_unexpected_error(request: Request, exc: Exception) -> JSONResponse:
    # exc_info=exc, not logger.exception: this may run where sys.exc_info() is already empty.
    logger.error(
        "unhandled error on %s %s (request %s)",
        request.method,
        request.url.path,
        _request_id(request),
        exc_info=exc,
    )
    return _response(request, 500, "internal_error", "Internal server error")


class UnexpectedErrorMiddleware:
    """Turn an unhandled exception into the JSON 500 inside the other middlewares.

    Starlette's own ``Exception`` handler runs outside every middleware, so its 500 would go out
    without ``X-Request-ID``, ``X-Response-Time`` or the CORS headers. Add this middleware first,
    so it runs innermost; the ``Exception`` handler stays as the fallback for errors raised in a
    middleware itself.
    """

    def __init__(self, app) -> None:
        self.app = app

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        started = False

        async def send_and_track(message) -> None:
            nonlocal started
            if message["type"] == "http.response.start":
                started = True
            await send(message)

        try:
            await self.app(scope, receive, send_and_track)
        except Exception as exc:
            if started:  # part of the response is already out; nothing clean can follow
                raise
            response = handle_unexpected_error(Request(scope), exc)
            await response(scope, receive, send)


def register_error_handlers(app: FastAPI) -> None:
    app.add_exception_handler(NickelConnectionError, handle_service_error)
    app.add_exception_handler(HTTPException, handle_http_error)
    app.add_exception_handler(RequestValidationError, handle_validation_error)
    app.add_exception_handler(Exception, handle_unexpected_error)
