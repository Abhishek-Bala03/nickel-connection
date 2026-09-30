"""Contract artifacts and draft preparation, separate from runtime provisioning."""

from fastapi import APIRouter
from pydantic import BaseModel, ConfigDict, Field

from src.contracts import EXAMPLE, inspect_manifest, schema
from src.db.layout import DataLayout
from src.db.manifest_drafts import ManifestDraftStore
from src.error import BadRequestError
from src.handler.documents import require_tenant
from src.services.connectors_service import Actor
from src.services.dependencies import Settings

contracts_router = APIRouter(tags=["contracts"])


class ManifestInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    manifest: str = Field(min_length=1, max_length=65536)


class ManifestSave(ManifestInput):
    version: int = Field(default=0, ge=0, strict=True)
    draft_id: str | None = Field(default=None, max_length=64)


def drafts(settings):
    return ManifestDraftStore(DataLayout(settings.data_dir).connectors_db)


@contracts_router.get("/contracts")
def contracts():
    return {
        "manifest_schema": schema("manifest"),
        "envelope_schema": schema("envelope"),
        "example": EXAMPLE.read_text(),
        "registration": "pending",
        "runtime_plugins": "held",
    }


@contracts_router.post("/plugin-manifests/validate")
def validate_manifest(body: ManifestInput):
    _, errors = inspect_manifest(body.manifest)
    return {"valid": not errors, "errors": errors}


@contracts_router.get("/plugin-manifests")
def list_drafts(settings: Settings):
    return drafts(settings).list(str(require_tenant(settings)))


@contracts_router.post("/plugin-manifests")
def save_draft(body: ManifestSave, settings: Settings, actor: Actor):
    data, errors = inspect_manifest(body.manifest)
    if errors:
        raise BadRequestError(
            "Manifest validation failed. Validate the draft to see field errors."
        )
    if (body.version > 0 and body.draft_id != data["id"]) or (
        body.version == 0 and body.draft_id is not None
    ):
        raise BadRequestError(
            "Draft identity cannot change; use New from template for another connector"
        )
    if not actor or not actor.strip():
        raise BadRequestError("An authenticated actor is required to save a draft")
    return drafts(settings).save(
        str(require_tenant(settings)), data["id"], body.manifest, body.version, actor
    )
