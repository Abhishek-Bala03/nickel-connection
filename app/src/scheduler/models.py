"""Scheduler configuration and execution records."""

from datetime import UTC, datetime
from typing import Any, Literal
from uuid import UUID, uuid4

from apscheduler.triggers.cron import CronTrigger
from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)


class ScheduleSettings(BaseModel):
    """Administrator settings; identity and audit fields are server-owned."""

    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
        validate_assignment=True,
    )

    name: str = Field(min_length=1, max_length=120)
    connector_type: str = Field(min_length=1, max_length=80)
    connection_id: str = Field(min_length=1, max_length=200)
    trigger_type: Literal["interval", "cron"] = "interval"
    interval_seconds: int | None = Field(default=None, strict=True, ge=1, le=31_536_000)
    cron_expression: str | None = Field(default=None, min_length=1, max_length=100)
    timezone: Literal["UTC"] = "UTC"
    max_retries: int = Field(default=0, strict=True, ge=0, le=5)
    retry_delay_seconds: int = Field(default=5, strict=True, ge=1, le=3600)
    timeout_seconds: int = Field(default=60, strict=True, ge=1, le=3600)
    enabled: bool = Field(default=True, strict=True)

    @model_validator(mode="before")
    @classmethod
    def validate_trigger_settings(cls, data: Any) -> Any:
        """Require exactly the settings for the selected trigger."""
        if not isinstance(data, dict):
            return data

        trigger_type = data.get("trigger_type", "interval")
        interval = data.get("interval_seconds")
        cron = data.get("cron_expression")

        if trigger_type == "interval":
            if interval is None:
                raise ValueError("Interval schedules require interval_seconds")
            if cron is not None:
                raise ValueError("Interval schedules cannot include cron_expression")
        elif trigger_type == "cron":
            if cron is None:
                raise ValueError("Cron schedules require cron_expression")
            if interval is not None:
                raise ValueError("Cron schedules cannot include interval_seconds")

        return data

    @field_validator("cron_expression")
    @classmethod
    def validate_cron_expression(cls, value: str | None) -> str | None:
        """Use APScheduler's five-field format; weekday zero means Monday."""
        if value is not None:
            try:
                CronTrigger.from_crontab(value, timezone=UTC)
            except ValueError as error:
                raise ValueError("Invalid five-field cron expression") from error
        return value


class ScheduleUpdate(ScheduleSettings):
    """Full replacement settings with the version the administrator edited."""

    version: int = Field(strict=True, ge=1)


class Schedule(ScheduleSettings):
    """Application-owned schedule with audit metadata and runtime next run."""

    id: UUID = Field(default_factory=uuid4)
    tenant_id: UUID
    created_at: AwareDatetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: AwareDatetime = Field(default_factory=lambda: datetime.now(UTC))
    # None identifies legacy records whose actor was never recorded.
    changed_by: str | None = Field(default=None, min_length=1, max_length=255)
    version: int = Field(default=1, strict=True, ge=1)
    # Supplied by APScheduler at runtime, not persisted in the new schema.
    next_run_at: AwareDatetime | None = None


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
    # The connection service's job for this run, when one was accepted (schema v2).
    job_id: str | None = Field(default=None, max_length=64)
