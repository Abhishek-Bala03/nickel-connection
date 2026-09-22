"""Errors the service raises on purpose, and the handlers that turn every error into the same JSON shape.

Raise one of these from a handler; the client gets ``{"error": {"code", "message"}, "request_id"}``
with the matching status. Framework errors (unknown route, wrong method, invalid request) use the same
shape. Anything unexpected becomes a plain 500: stack traces and internals never reach the client, they
go to the log with the request id.
"""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException

from src.logging import logger

_HTTP_CODES = {
    400: "bad_request",
    401: "unauthorized",
    403: "forbidden",
    404: "not_found",
    405: "method_not_allowed",
}


class NickelConnectionError(Exception):
    status_code = 500
    code = "internal_error"

    def __init__(self, message: str = "Something went wrong") -> None:
        super().__init__(message)
        self.message = message


class BadRequestError(NickelConnectionError):
    status_code = 400
    code = "bad_request"


class NotFoundError(NickelConnectionError):
    status_code = 404
    code = "not_found"


class ConflictError(NickelConnectionError):
    status_code = 409
    code = "conflict"


class UpstreamError(NickelConnectionError):
    """A source system or vendor API failed."""

    status_code = 502
    code = "upstream_error"


def _response(
    request: Request, status: int, code: str, message: str, **extra
) -> JSONResponse:
    error = {"code": code, "message": message, **extra}
    body = {"error": error, "request_id": getattr(request.state, "request_id", None)}
    return JSONResponse(status_code=status, content=body)


def handle_service_error(request: Request, exc: NickelConnectionError) -> JSONResponse:
    return _response(request, exc.status_code, exc.code, exc.message)


def handle_http_error(request: Request, exc: HTTPException) -> JSONResponse:
    code = _HTTP_CODES.get(exc.status_code, "http_error")
    return _response(request, exc.status_code, code, str(exc.detail))


def handle_validation_error(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    details = [
        {"field": ".".join(str(part) for part in e["loc"]), "problem": e["msg"]}
        for e in exc.errors()
    ]
    return _response(
        request, 422, "validation_error", "The request is not valid", details=details
    )


def handle_unexpected_error(request: Request, exc: Exception) -> JSONResponse:
    logger.exception(
        "unhandled error on %s %s (request %s)",
        request.method,
        request.url.path,
        getattr(request.state, "request_id", None),
    )
    return _response(request, 500, "internal_error", "Internal server error")


def register_error_handlers(app: FastAPI) -> None:
    app.add_exception_handler(NickelConnectionError, handle_service_error)
    app.add_exception_handler(HTTPException, handle_http_error)
    app.add_exception_handler(RequestValidationError, handle_validation_error)
    app.add_exception_handler(Exception, handle_unexpected_error)
