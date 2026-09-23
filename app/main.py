"""nickel-connection service entry point, as ``app.py`` is in Rockwell.

Run with ``uvicorn main:app`` from ``app/`` (the Docker image does this).
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

from src.config import Service, ServiceSettings
from src.error import UnexpectedErrorMiddleware, register_error_handlers
from src.handler.jobs import build_runner
from src.logging import logger
from src.middlewares import RequestIdMiddleware, RequestTimingMiddleware
from src.services import base_router


def create_app(settings: ServiceSettings | None = None) -> FastAPI:
    """The service: the connection layer, up to ``raw_data`` and no further.

    ``settings`` default to the ones read from the environment at start-up (``src.config.Service``);
    tests and embedders pass their own, and the routes read them from ``app.state.settings``.
    """
    settings = Service if settings is None else settings

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        logger.info(
            "%s %s starting (%s)", settings.name, settings.version, settings.environment
        )
        # The job worker: one thread, started with the process and stopped with it.
        app.state.jobs = build_runner(app.state.settings)
        app.state.jobs.start()
        try:
            yield
        finally:
            app.state.jobs.stop()
            logger.info("%s stopped", settings.name)

    # The API docs describe every route: useful in development and QA, not handed out in production.
    docs = settings.environment != "production"
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
    app.add_middleware(UnexpectedErrorMiddleware)
    app.add_middleware(RequestTimingMiddleware)
    app.add_middleware(RequestIdMiddleware)
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


app = create_app()
