"""Request timing, as in Rockwell: every response gets ``X-Response-Time``, and requests slower than
``SLOW_REQUEST_MS`` are logged as warnings, so latency is measured per endpoint."""

import time

from starlette.middleware.base import BaseHTTPMiddleware

from src.logging import logger

SLOW_REQUEST_MS = 300.0


class RequestTimingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        started = time.perf_counter()
        response = await call_next(request)
        elapsed_ms = (time.perf_counter() - started) * 1000
        response.headers["X-Response-Time"] = f"{elapsed_ms:.1f}ms"
        if elapsed_ms >= SLOW_REQUEST_MS:
            logger.warning(
                "[SLOW_REQUEST] %s %s took %.0fms (request %s)",
                request.method,
                request.url.path,
                elapsed_ms,
                getattr(request.state, "request_id", None),
            )
        return response
