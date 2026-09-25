"""The connectors API: what the Connections UI sends and gets back (docs/demo-nango-wearables.md)."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from src.db.connectors import Connector

_ID = r"^[A-Za-z0-9._-]+$"


class ConnectorEdit(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    name: str = Field(min_length=1, max_length=120)
    description: str = Field(default="", max_length=500)
    streams: list[str] = Field(min_length=1, max_length=20)
    enabled: bool = True


class ConnectorCreate(ConnectorEdit):
    source: Literal["nango", "nickel_internal"] = "nango"
    provider: str = Field(pattern=r"^[a-z0-9-]{2,40}$")
    nango_connection_id: str = Field(min_length=1, max_length=100, pattern=_ID)
    patient_id: str | None = Field(default=None, max_length=64, pattern=_ID)


class ConnectorUpdate(ConnectorEdit):
    version: int = Field(ge=1, strict=True)


class ConnectorResponse(BaseModel):
    id: str
    name: str
    description: str
    source: str
    provider: str
    nango_integration: str
    nango_connection_id: str
    patient_id: str | None
    streams: list[str]
    enabled: bool
    version: int
    created_at: str
    updated_at: str
    changed_by: str | None

    @classmethod
    def of(cls, connector: Connector) -> ConnectorResponse:
        values = vars(connector).copy()
        values.pop("tenant_id")
        return cls(**values)


class ProviderInfo(BaseModel):
    id: str
    name: str
    streams: list[str]
    simulated: bool


class NangoPullRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    connector_id: str = Field(pattern=r"^[0-9a-f-]{36}$")


class PullResponse(BaseModel):
    stream: str
    outcome: str
    ingest_id: str
    bytes: int
    records: int
    fetched_at: str
    viewable: bool
