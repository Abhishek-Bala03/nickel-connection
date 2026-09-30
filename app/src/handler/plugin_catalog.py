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
    raise BadRequestError("Runtime plugins are on hold pending reviewed manifests")


def check_integration(settings, key, integration):
    if provider(settings, key).integration != integration:
        raise BadRequestError(
            "Select a connection belonging to this plugin's Nango integration"
        )


def allowed_integration(settings, integration):
    prefix = f"nickel-{str(require_tenant(settings)).replace('-', '')}-"
    return integration in {
        p.integration for p in nango.PROVIDERS.values()
    } or integration.startswith(prefix)


def require_integration(settings, integration):
    if not allowed_integration(settings, integration):
        raise BadRequestError("Nango integration is not available to this tenant")
