"""Resolve built-in adapters and provisioned plugins within the current tenant."""

from src.connectors import nango
from src.db.layout import DataLayout
from src.db.plugins import PluginStore
from src.error import BadRequestError
from src.handler.documents import require_tenant


def definitions(settings):
    return PluginStore(DataLayout(settings.data_dir).connectors_db).list(
        str(require_tenant(settings))
    )


def plugin(settings, key):
    found = PluginStore(DataLayout(settings.data_dir).connectors_db).get(
        str(require_tenant(settings)), key
    )
    if found["status"] != "ready":
        raise BadRequestError(
            "Complete plugin setup in Nango before adding a connection"
        )
    return found


def provider(settings, key):
    if key in nango.PROVIDERS:
        return nango.provider(key)
    definition = plugin(settings, key)
    return nango.Provider(
        name=definition["name"],
        integration=definition["nango_integration"],
        streams={r["name"]: r["path"] for r in definition["resources"]},
    )


def check_integration(settings, key, integration):
    if (
        key not in nango.PROVIDERS
        and provider(settings, key).integration != integration
    ):
        raise BadRequestError(
            "Select a connection belonging to this plugin's Nango integration"
        )
