"""Request and response models (Pydantic). One module per area: ``envelope`` (the raw_data envelope),
``jobs`` (the jobs API), ``documents`` (the receipts); the health models are here."""

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
