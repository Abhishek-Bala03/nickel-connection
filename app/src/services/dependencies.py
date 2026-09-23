"""What the routes take from the app: the settings it was created with and the job runner the lifespan
started (``main.create_app``)."""

from typing import Annotated

from fastapi import Depends, Request

from src.config import ServiceSettings
from src.handler.jobs import JobRunner


def _settings(request: Request) -> ServiceSettings:
    return request.app.state.settings


def _jobs(request: Request) -> JobRunner:
    return request.app.state.jobs


Settings = Annotated[ServiceSettings, Depends(_settings)]
Jobs = Annotated[JobRunner, Depends(_jobs)]
