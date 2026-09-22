"""nickel-connection service entry point, as ``app.py`` is in Rockwell.

Run with ``uvicorn main:app`` from ``app/`` (the Docker image does this).
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

from src.config import Service
from src.error import UnexpectedErrorMiddleware, register_error_handlers
from src.logging import logger
from src.middlewares import RequestIdMiddleware, RequestTimingMiddleware
from src.services import base_router


@asynccontextmanager
async def lifespan(_app: FastAPI):
    logger.info(
        "%s %s starting (%s)", Service.name, Service.version, Service.environment
    )
    yield
    logger.info("%s stopped", Service.name)


def create_app() -> FastAPI:
    # The API docs describe every route: useful in development and QA, not handed out in production.
    docs = Service.environment != "production"
    app = FastAPI(
        title=Service.name,
        version=Service.version,
        lifespan=lifespan,
        docs_url="/docs" if docs else None,
        redoc_url="/redoc" if docs else None,
        openapi_url="/openapi.json" if docs else None,
    )
    # Added last runs first: CORS, then the request id, then timing, then the catch for
    # unexpected errors, so even a 500 carries the request id, its time and the CORS headers.
    app.add_middleware(UnexpectedErrorMiddleware)
    app.add_middleware(RequestTimingMiddleware)
    app.add_middleware(RequestIdMiddleware)
    if Service.cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=list(Service.cors_origins),
            allow_methods=["*"],
            allow_headers=["*"],
            # Let the UI read these, e.g. to show the request id with an error.
            expose_headers=["X-Request-ID", "X-Response-Time", "Retry-After"],
        )
    register_error_handlers(app)
    app.include_router(base_router)
    return app


app = create_app()
