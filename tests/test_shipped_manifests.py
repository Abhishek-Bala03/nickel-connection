"""Every manifest that ships in app/manifests must validate (CN-01: CI rejects a manifest missing
contract baa/dpa, auth vault_path, phi or owner), and none of them is a production source."""

import pytest

from src.config import CONF_FILE, DEFAULT_MAX_FILE_BYTES, MANIFEST_DIR, load_settings
from src.connectors.documents import DocumentConnector, DocumentManifest

MANIFESTS = sorted(MANIFEST_DIR.glob("*.yaml"))


class NoSource:
    locator = "none"

    def files(self):
        raise AssertionError("a refused manifest must not list its source")


def test_manifests_are_shipped():
    assert MANIFESTS
    # Where the jobs API looks unless NICKEL_MANIFEST_DIR says otherwise.
    assert load_settings(CONF_FILE, environ={}).manifest_dir == MANIFEST_DIR


@pytest.mark.parametrize("path", MANIFESTS, ids=lambda p: p.stem)
def test_shipped_manifest_is_valid_and_named_after_its_id(path):
    manifest = DocumentManifest.load(path)
    assert manifest.id == path.stem
    # Nothing with real patient data is pulled from public sources, and no agreement is claimed.
    assert manifest.phi is False
    assert (manifest.contract.baa, manifest.contract.dpa) == (None, None)
    # Development sources: the SDKs read their token from the environment, nothing is in Vault yet.
    assert manifest.auth.vault_path is None
    assert manifest.route == "L7"  # specialty-lab PDFs: the lowest fidelity tier
    assert manifest.schedule.frequency_class == "F4"  # CN-01's shape: on demand
    assert getattr(manifest, manifest.fetch) is not None  # the block the fetch needs


@pytest.mark.parametrize("path", MANIFESTS, ids=lambda p: p.stem)
def test_shipped_manifests_are_development_sources_refused_in_production(path, bridge):
    manifest = DocumentManifest.load(path)
    assert manifest.phase == "dev"
    connector = DocumentConnector(
        manifest,
        bridge,
        NoSource(),
        environment="production",
        max_file_bytes=DEFAULT_MAX_FILE_BYTES,
    )
    summary = connector.run()
    assert summary.aborted == (
        "manifest phase 'dev' is not cleared for the production environment"
    )
    assert (summary.listed, summary.published) == (0, 0)
