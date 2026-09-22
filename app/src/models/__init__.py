"""Request and response models (Pydantic). One module per area once they grow."""

from typing import Literal

from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: Literal["ok"]


class ReadyResponse(BaseModel):
    status: Literal["ready", "not_ready"]
    checks: dict[str, str]


class VersionResponse(BaseModel):
    name: str
    version: str
    environment: str
