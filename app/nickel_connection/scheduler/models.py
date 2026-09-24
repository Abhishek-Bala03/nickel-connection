"""Scheduler configuration and execution records."""

from typing import Literal
from uuid import UUID, uuid4

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field


class ScheduleSettings(BaseModel):
    """Settings an administrator can configure."""

    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
        validate_assignment=True,
    )

    name: str = Field(min_length=1, max_length=120)
    connector_type: str = Field(min_length=1, max_length=80)
    connection_id: str = Field(min_length=1, max_length=200)
    interval_seconds: int = Field(strict=True, ge=1)
    max_retries: int = Field(default=0, strict=True, ge=0, le=5)
    retry_delay_seconds: int = Field(default=5, strict=True, ge=1, le=3600)
    timeout_seconds: int = Field(default=60, strict=True, ge=1, le=3600)
    enabled: bool = Field(default=True, strict=True)


class Schedule(ScheduleSettings):
    """Stored configuration for one recurring job."""

    id: UUID = Field(default_factory=uuid4)
    tenant_id: UUID
    next_run_at: AwareDatetime


class RunRecord(BaseModel):
    """Persisted history of one connector execution."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    id: UUID = Field(default_factory=uuid4)
    schedule_id: UUID
    tenant_id: UUID
    connection_id: str
    status: Literal["running", "succeeded", "failed", "interrupted"] = "running"
    started_at: AwareDatetime
    finished_at: AwareDatetime | None = None
    items_processed: int = Field(default=0, strict=True, ge=0)
    error_code: str | None = None
