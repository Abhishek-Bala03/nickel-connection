"""Validate and save plugin drafts before Nango provisioning."""

from datetime import UTC, datetime

from src.config import ServiceSettings
from src.connectors.nango import PROVIDERS
from src.db.layout import DataLayout
from src.db.plugins import PluginStore
from src.error import BadRequestError
from src.handler import nango
from src.handler.documents import require_tenant
from src.models.plugins import PluginCreate, PluginProvision


def store(settings: ServiceSettings) -> PluginStore:
    return PluginStore(DataLayout(settings.data_dir).connectors_db)


def list_plugins(settings: ServiceSettings) -> list[dict]:
    tenant_id = str(require_tenant(settings))
    return store(settings).list(tenant_id)


def create_plugin(
    settings: ServiceSettings,
    body: PluginCreate,
    *,
    changed_by: str | None,
) -> dict:
    tenant_id = str(require_tenant(settings))

    # Reserve the built-in provider IDs.
    if body.id in PROVIDERS:
        raise BadRequestError("This plugin ID belongs to a built-in provider")

    remote = nango.client(settings)
    try:
        provider = next(
            (item for item in remote.providers() if item["id"] == body.nango_provider),
            None,
        )
    finally:
        remote.close()

    if provider is None:
        raise BadRequestError(
            "The selected provider is unavailable in this Nango deployment"
        )

    plugin = {
        **body.model_dump(mode="json"),
        "auth_mode": provider["auth_mode"],
        "status": "draft",
        "nango_integration": None,
        "created_at": datetime.now(UTC).isoformat(),
        "changed_by": changed_by,
    }

    store(settings).add(tenant_id, plugin)
    return plugin


def get_plugin(settings: ServiceSettings, plugin_id: str) -> dict:
    return store(settings).get(str(require_tenant(settings)), plugin_id)


def provision_plugin(
    settings: ServiceSettings, plugin_id: str, body: PluginProvision
) -> dict:
    tenant = str(require_tenant(settings))
    plugin = store(settings).get(tenant, plugin_id)
    # Deterministic and tenant-scoped: a timed-out create can be retried without duplication.
    integration = f"nickel-{tenant.replace('-', '')}-{plugin_id}"
    remote = nango.client(settings)
    try:
        remote.ensure_integration(
            integration, plugin["nango_provider"], plugin["name"], body.nango_payload()
        )
    finally:
        remote.close()
    return store(settings).activate(tenant, plugin_id, integration)
