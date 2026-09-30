"""Plugin definitions contain request configuration, never account credentials."""

import json
from typing import Literal
from urllib.parse import unquote, urlsplit

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    SecretStr,
    field_validator,
    model_validator,
)


def resource_path(value: str) -> str:
    decoded = value
    for _ in range(5):
        new = unquote(decoded)
        if new == decoded:
            break
        decoded = new
    parsed = urlsplit(decoded)
    if (
        not decoded.startswith("/")
        or decoded.startswith("//")
        or parsed.scheme
        or parsed.netloc
        or parsed.query
        or parsed.fragment
        or "\\" in decoded
        or "%" in decoded
        or any(ord(c) < 32 for c in decoded)
        or any(p in {".", ".."} for p in decoded.split("/"))
    ):
        raise ValueError(
            "Use a relative resource path without a hostname, query, or fragment"
        )
    return value


class Pagination(BaseModel):
    model_config = ConfigDict(extra="forbid")
    mode: Literal["none", "fhir", "cursor", "offset"] = "none"
    cursor_path: str = Field(default="next_cursor", max_length=200)
    cursor_param: str = Field(default="cursor", pattern=r"^[A-Za-z0-9_.$-]{1,80}$")
    items_path: str = Field(default="data", max_length=200)
    offset_param: str = Field(default="offset", pattern=r"^[A-Za-z0-9_.$-]{1,80}$")
    limit_param: str = Field(default="limit", pattern=r"^[A-Za-z0-9_.$-]{1,80}$")
    page_size: int = Field(default=100, ge=1, le=1000)
    max_pages: int = Field(default=50, ge=1, le=200)


class PluginResource(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    name: str = Field(pattern=r"^[A-Za-z0-9_]{1,40}$")
    method: Literal["GET", "POST"] = "GET"
    path: str = Field(min_length=1, max_length=1000)
    query: dict[str, str] = Field(default_factory=dict, max_length=30)
    body: dict[str, JsonValue] | None = None
    pagination: Pagination = Field(default_factory=Pagination)

    _path = field_validator("path")(resource_path)

    @model_validator(mode="after")
    def request_shape(self) -> "PluginResource":
        if self.method == "GET" and self.body is not None:
            raise ValueError("GET resources cannot have a request body")
        if self.method == "POST" and not self.path.endswith("/_search"):
            raise ValueError("POST is allowed only for /_search endpoints")
        if self.method == "POST" and self.pagination.mode != "none":
            raise ValueError("Automatic pagination is supported for GET resources only")
        if len(json.dumps({"query": self.query, "body": self.body})) > 32000:
            raise ValueError("Resource request configuration is too large")
        return self


class PluginCreate(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    id: str = Field(min_length=2, max_length=40, pattern=r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
    name: str = Field(min_length=1, max_length=120)
    nango_provider: str = Field(
        min_length=1, max_length=100, pattern=r"^[A-Za-z0-9._-]+$"
    )
    resources: list[PluginResource] = Field(min_length=1, max_length=20)

    @model_validator(mode="after")
    def unique_resources(self) -> "PluginCreate":
        names = [resource.name for resource in self.resources]
        if len(names) != len(set(names)):
            raise ValueError("Resource names must be unique")
        return self


class PluginProvision(BaseModel):
    model_config = ConfigDict(extra="forbid")
    # These are forwarded once to Nango, never saved in Nickel or returned to the UI.
    client_id: SecretStr | None = Field(default=None, min_length=1, max_length=8192)
    client_secret: SecretStr | None = Field(
        default=None, min_length=1, max_length=16384
    )
    scopes: str | None = Field(default=None, max_length=4000)
    integration_config: dict[str, JsonValue] | None = None

    @model_validator(mode="after")
    def credential_pair(self) -> "PluginProvision":
        if (self.client_id is None) != (self.client_secret is None):
            raise ValueError("Provide both OAuth client ID and client secret")
        return self

    def nango_payload(self) -> dict:
        result = {}
        if self.client_id is not None:
            result["credentials"] = {
                "client_id": self.client_id.get_secret_value(),
                "client_secret": self.client_secret.get_secret_value(),
            }
        if self.scopes is not None:
            result.setdefault("credentials", {})["scopes"] = self.scopes
        if self.integration_config is not None:
            result["integration_config"] = self.integration_config
        return result
