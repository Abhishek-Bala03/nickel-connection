"""Errors the service raises on purpose, and the handlers that turn them into JSON responses.

Raise one of these from a handler; the client gets ``{"error": {"code", "message"}, "request_id"}``
with the matching status. Anything unexpected becomes a plain 500: stack traces and internals never
reach the client, they go to the log with the request id.
"""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from src.logging import logger


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


def _body(request: Request, code: str, message: str) -> dict:
    return {
        "error": {"code": code, "message": message},
        "request_id": getattr(request.state, "request_id", None),
    }


async def handle_service_error(
    request: Request, exc: NickelConnectionError
) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code, content=_body(request, exc.code, exc.message)
    )


async def handle_unexpected_error(request: Request, exc: Exception) -> JSONResponse:
    logger.exception(
        "unhandled error on %s %s (request %s)",
        request.method,
        request.url.path,
        getattr(request.state, "request_id", None),
    )
    return JSONResponse(
        status_code=500,
        content=_body(request, "internal_error", "Internal server error"),
    )


def register_error_handlers(app: FastAPI) -> None:
    app.add_exception_handler(NickelConnectionError, handle_service_error)
    app.add_exception_handler(Exception, handle_unexpected_error)
