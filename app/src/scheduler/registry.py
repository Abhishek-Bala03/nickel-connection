"""Deployment-approved connections; schedules remain in the scheduler database."""

import re

from pydantic import BaseModel, ConfigDict, Field

from .models import ScheduleSettings


class Connection(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    id: str = Field(min_length=1, max_length=200)
    name: str = Field(min_length=1, max_length=120)
    connector_type: str
    manifest: str | None = Field(default=None, pattern=r"^[a-z0-9-]{3,63}$")


class ConnectorInfo(BaseModel):
    id: str
    name: str
    minimum_interval_seconds: int


class ConnectorRegistry:
    def __init__(self, connections: list[Connection], *, demo: bool = False):
        self.connectors = [
            ConnectorInfo(
                id="documents", name="Document jobs API", minimum_interval_seconds=300
            )
        ]
        if demo:
            self.connectors.append(
                ConnectorInfo(
                    id="demo", name="Demo — simulated", minimum_interval_seconds=30
                )
            )
        self.connections = list(connections)
        if len({item.id for item in connections}) != len(connections):
            raise ValueError("Duplicate connection IDs")
        for item in connections:
            if item.connector_type not in {c.id for c in self.connectors}:
                raise ValueError("Unregistered connector")
            if item.connector_type == "documents" and item.manifest is None:
                raise ValueError("Document connections require a manifest ID")

    def validate(self, settings: ScheduleSettings) -> Connection:
        connection = next(
            (c for c in self.connections if c.id == settings.connection_id), None
        )
        if connection is None or connection.connector_type != settings.connector_type:
            raise ValueError("Select an available connection for this connector")
        connector = next(
            c for c in self.connectors if c.id == connection.connector_type
        )
        if settings.trigger_type == "interval":
            if settings.interval_seconds < connector.minimum_interval_seconds:
                raise ValueError(
                    f"Minimum interval is {connector.minimum_interval_seconds} seconds"
                )
        elif not re.fullmatch(
            r"(?:[0-9]|[1-5][0-9]) (?:[0-9]|1[0-9]|2[0-3]) \* \* \*",
            settings.cron_expression,
        ):
            raise ValueError("Use a daily UTC cron: minute hour * * *")
        return connection
