"""nickel-connection service entry point, as ``app.py`` is in Rockwell.

Run with ``uvicorn main:app`` from ``app/`` (the Docker image does this).
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

from src.config import ServiceSettings, load_settings
from src.error import UnexpectedErrorMiddleware, register_error_handlers
from src.handler.jobs import build_runner
from src.logging import configure_logging, logger
from src.middlewares import (
    RequestIdMiddleware,
    RequestSizeLimitMiddleware,
    RequestTimingMiddleware,
    SecurityHeadersMiddleware,
)
from src.services import base_router
from src.services.auth import ServiceTokenMiddleware, check_token


def create_app(
    settings: ServiceSettings, *, republish_at_start: bool = True
) -> FastAPI:
    """The service: the connection layer, up to ``raw_data`` and no further.

    ``settings`` are an argument, never a module global: the module-level ``app`` below is built
    from ``load_settings()``, tests and embedders pass their own, and the routes read them from
    ``app.state.settings``. ``republish_at_start``: the job worker queues one republish of the open
    receipts when it starts (``src.handler.jobs.build_runner``); tests that count jobs turn it off.
    """
    # Fail closed: no token, no service (src.services.auth). Raised here, at import, so uvicorn exits.
    check_token(settings.service_token)
    configure_logging(settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        logger.info(
            "%s %s starting (%s)", settings.name, settings.version, settings.environment
        )
        # The job worker: one thread, started with the process and stopped with it.
        app.state.jobs = build_runner(
            app.state.settings, republish_at_start=republish_at_start
        )
        app.state.jobs.start()
        try:
            yield
        finally:
            app.state.jobs.stop()
            logger.info("%s stopped", settings.name)

    # The API docs describe every route and take no token (Swagger UI fetches the schema with a
    # plain GET): on in development only, never on a deployment others can reach.
    docs = settings.environment == "development"
    app = FastAPI(
        title=settings.name,
        version=settings.version,
        lifespan=lifespan,
        docs_url="/docs" if docs else None,
        redoc_url="/redoc" if docs else None,
        openapi_url="/openapi.json" if docs else None,
    )
    app.state.settings = settings
    # Added last runs first: CORS, then the request id, then timing, then the catch for
    # unexpected errors, so even a 500 carries the request id, its time and the CORS headers.
    # The token check runs after the request id is set and before routing reads a body; the body
    # cap runs inside it, so an unauthenticated request is a 401 whatever its size, and an
    # authenticated one over the cap is a 413 before the body is read.
    app.add_middleware(RequestSizeLimitMiddleware)
    app.add_middleware(ServiceTokenMiddleware)
    app.add_middleware(UnexpectedErrorMiddleware)
    app.add_middleware(RequestTimingMiddleware)
    app.add_middleware(RequestIdMiddleware)
    # Outside everything but CORS, so every answer (a 401, a 413, a 500) carries the headers.
    app.add_middleware(SecurityHeadersMiddleware)
    if settings.cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=list(settings.cors_origins),
            allow_methods=["*"],
            allow_headers=["*"],
            # Let the UI read these, e.g. to show the request id with an error.
            expose_headers=["X-Request-ID", "X-Response-Time", "Retry-After"],
        )
    register_error_handlers(app)
    app.include_router(base_router)
    return app


# What uvicorn serves (``uvicorn main:app``): the settings are read once, here, at start-up.
app = create_app(load_settings())
