import pytest
import yaml
from pydantic import ValidationError

from src.connectors.documents import DocumentManifest
from src.utility.keys import ObjectClass

VALID = {
    "id": "hf-lab-reports",
    "description": "Sample lab report PDFs",
    "stream": "documents",
    "mode": "pull",
    "route": "L7",
    "fetch": "huggingface",
    "auth": {"type": "hf_token", "vault_path": None},
    "phi": False,
    "owner": "divij",
    "phase": "dev",
    "contract": {"baa": None, "dpa": None},
    "license": "CC BY 4.0",
    "huggingface": {"repo_id": "org/lab-reports"},
}


def test_a_valid_manifest_loads_from_yaml(tmp_path):
    path = tmp_path / "hf-lab-reports.yaml"
    path.write_text(yaml.safe_dump(VALID), encoding="utf-8")
    manifest = DocumentManifest.load(path)
    assert manifest.fetch == "huggingface"
    assert manifest.formats == ["pdf"]
    assert manifest.include == ["*.pdf"]
    assert manifest.shadow is True  # the safe default: store, receipt, publish nothing
    assert manifest.raw_retention == "forever"
    assert manifest.retention_class is ObjectClass.DOCUMENTS
    assert manifest.huggingface.repo_type == "dataset"
    assert (manifest.auth.type, manifest.auth.vault_path) == ("hf_token", None)
    assert (manifest.contract.baa, manifest.contract.dpa) == (None, None)


@pytest.mark.parametrize(
    "field",
    ["phi", "owner", "contract", "auth", "license", "phase", "fetch", "route"],
)
def test_governance_fields_are_required(field):
    data = {k: v for k, v in VALID.items() if k != field}
    with pytest.raises(ValidationError, match=field):
        DocumentManifest.model_validate(data)


@pytest.mark.parametrize(
    "block, missing",
    [
        ("auth", "vault_path"),
        ("auth", "type"),
        ("contract", "baa"),
        ("contract", "dpa"),
    ],
)
def test_both_keys_of_auth_and_of_contract_are_given_even_when_null(block, missing):
    """CN-01 nesting: the keys are always there, so a null is a statement, not an omission."""
    partial = {k: v for k, v in VALID[block].items() if k != missing}
    with pytest.raises(ValidationError, match=missing):
        DocumentManifest.model_validate({**VALID, block: partial})


@pytest.mark.parametrize(
    "fetch, block, fits, wrong",
    [
        ("kaggle", {"kaggle": {"handle": "owner/dataset"}}, "kaggle_token", "none"),
        (
            "huggingface",
            {"huggingface": {"repo_id": "org/ds"}},
            "hf_token",
            "kaggle_token",
        ),
        ("huggingface", {"huggingface": {"repo_id": "org/ds"}}, "none", "kaggle_token"),
        ("http", {"http": {"urls": ["https://lab.example/a.pdf"]}}, "none", "hf_token"),
        ("local", {"local": {"path": "drops"}}, "none", "kaggle_token"),
    ],
)
def test_the_auth_type_must_fit_the_fetch(fetch, block, fits, wrong):
    """Kaggle always needs its token; a public Hugging Face repo needs none and a gated one the
    hub token; web links and folders take no credential here."""
    base = {k: v for k, v in VALID.items() if k != "huggingface"}
    data = {**base, "fetch": fetch, **block, "auth": {"type": fits, "vault_path": None}}
    assert DocumentManifest.model_validate(data).auth.type == fits
    with pytest.raises(ValidationError, match=f"fetch '{fetch}' takes auth.type"):
        DocumentManifest.model_validate(
            {**data, "auth": {"type": wrong, "vault_path": None}}
        )


def test_a_phi_source_needs_a_baa():
    with pytest.raises(ValidationError, match="business associate agreement"):
        DocumentManifest.model_validate({**VALID, "phi": True})
    with_baa = {**VALID, "phi": True, "contract": {"baa": "BAA-2026-014", "dpa": None}}
    assert DocumentManifest.model_validate(with_baa).contract.baa == "BAA-2026-014"


@pytest.mark.parametrize("retention", ["1d", "90d", "3650d"])
def test_raw_retention_accepts_a_number_of_days(retention):
    manifest = DocumentManifest.model_validate({**VALID, "raw_retention": retention})
    assert manifest.raw_retention == retention
    # The object class does not change with the retention: the sidecar records the value.
    assert manifest.retention_class is ObjectClass.DOCUMENTS


@pytest.mark.parametrize(
    "url", ["http://lab.example/a.pdf", "https://lab.example:8443/a.pdf"]
)
def test_http_urls_must_be_https_on_443(url):
    base = {k: v for k, v in VALID.items() if k != "huggingface"}
    data = {**base, "fetch": "http", "auth": {"type": "none", "vault_path": None}}
    with pytest.raises(ValidationError, match="must be https on port 443"):
        DocumentManifest.model_validate({**data, "http": {"urls": [url]}})
    explicit = {**data, "http": {"urls": ["https://lab.example:443/a.pdf"]}}
    assert str(DocumentManifest.model_validate(explicit).http.urls[0]).startswith(
        "https://"
    )


@pytest.mark.parametrize(
    "url",
    [
        "https://user:secret@lab.example/a.pdf",
        "https://user@lab.example/a.pdf",
        "https://:secret@lab.example/a.pdf",
    ],
)
def test_http_urls_carry_no_credential(url):
    """A ``user:password@`` in a link would go out as Basic auth and, as the item's message id,
    onto raw_data and every receipt; a credential belongs in Vault. The refusal names the host.
    """
    base = {k: v for k, v in VALID.items() if k != "huggingface"}
    data = {**base, "fetch": "http", "auth": {"type": "none", "vault_path": None}}
    with pytest.raises(
        ValidationError, match="must carry no user or password"
    ) as caught:
        DocumentManifest.model_validate({**data, "http": {"urls": [url]}})
    [error] = caught.value.errors()
    assert error["msg"] == (
        "Value error, https://lab.example: http.urls must carry no user or password "
        "(a credential belongs in Vault: auth.vault_path)"
    )


@pytest.mark.parametrize("route", ["huggingface", "L0", "L10", "l7", ""])
def test_route_is_the_lab_route_not_the_transport(route):
    """``route`` is the lab route (L1-L9) the fidelity ranking reads. v0.1 manifests used the name
    for the transport, which is ``fetch`` now; the old spelling no longer fills ``fetch`` in.
    """
    with pytest.raises(ValidationError, match="route"):
        DocumentManifest.model_validate({**VALID, "route": route})
    old = {**{k: v for k, v in VALID.items() if k != "fetch"}, "route": route}
    with pytest.raises(ValidationError, match="fetch"):
        DocumentManifest.model_validate(old)


def test_every_lab_route_is_accepted():
    for tier in range(1, 10):
        assert DocumentManifest.model_validate({**VALID, "route": f"L{tier}"}).route


def test_the_fetch_block_must_match_the_fetch():
    wrong = {**VALID, "fetch": "kaggle"}
    with pytest.raises(ValidationError, match="exactly the 'kaggle' block"):
        DocumentManifest.model_validate(wrong)
    both = {**VALID, "kaggle": {"handle": "owner/dataset"}}
    with pytest.raises(ValidationError, match="found"):
        DocumentManifest.model_validate(both)


@pytest.mark.parametrize(
    "change",
    [
        {"id": "Bad ID"},
        {"fetch": "kaggle", "huggingface": None, "kaggle": {"handle": "no-slash"}},
        {"unexpected": 1},
        {"formats": ["docx"]},
        {"phase": 1},
        {"phase": "production"},
        {"raw_retention": "0d"},
        {"raw_retention": "3651d"},
        {"raw_retention": "90"},
        {"raw_retention": "never"},
        {"auth": "none"},  # the flat form of the first draft
        {"contract": "n/a"},
    ],
)
def test_malformed_manifests_are_rejected(change):
    with pytest.raises(ValidationError):
        DocumentManifest.model_validate({**VALID, **change})


@pytest.mark.parametrize(
    "phase, cleared",
    [
        ("dev", ["development"]),
        ("qa", ["development", "qa"]),
        ("prod", ["development", "qa", "production"]),
    ],
)
def test_a_phase_clears_a_manifest_for_the_environments_up_to_it(phase, cleared):
    manifest = DocumentManifest.model_validate({**VALID, "phase": phase})
    for environment in ("development", "qa", "production"):
        assert manifest.cleared_for(environment) is (environment in cleared)
    assert not manifest.cleared_for("somewhere-else")


def test_include_and_exclude_globs_are_case_insensitive():
    manifest = DocumentManifest.model_validate({**VALID, "exclude": ["*draft*"]})
    assert manifest.wants("reports/2026/Report.PDF")
    assert not manifest.wants("notes.txt")
    assert not manifest.wants("drafts/a.pdf")
