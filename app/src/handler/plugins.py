"""Validate and save plugin drafts before Nango provisioning."""

from src.config import ServiceSettings
from src.db.layout import DataLayout
from src.db.plugins import PluginStore
from src.error import BadRequestError
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
    raise BadRequestError("Runtime plugins are on hold pending reviewed manifests")


def get_plugin(settings: ServiceSettings, plugin_id: str) -> dict:
    return store(settings).get(str(require_tenant(settings)), plugin_id)


def provision_plugin(
    settings: ServiceSettings, plugin_id: str, body: PluginProvision
) -> dict:
    raise BadRequestError(
        "Runtime plugins are on hold pending reviewed manifests; provisioning is disabled"
    )
