"""nickel-connection service entry point, as ``app.py`` is in Rockwell.

Run with ``uvicorn main:app`` from ``app/`` (the Docker image does this).
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

from src.config import Service
from src.error import register_error_handlers
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
    app = FastAPI(title=Service.name, version=Service.version, lifespan=lifespan)
    # Added last runs first: the request id is set before timing and handlers run.
    app.add_middleware(RequestTimingMiddleware)
    app.add_middleware(RequestIdMiddleware)
    if Service.cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=list(Service.cors_origins),
            allow_methods=["*"],
            allow_headers=["*"],
        )
    register_error_handlers(app)
    app.include_router(base_router)
    return app


app = create_app()
