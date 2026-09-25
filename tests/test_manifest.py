import logging
import traceback

import pytest
import yaml
from pydantic import ValidationError

from src.connectors.documents import DocumentManifest, ManifestError
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


# A web-link source, which may run in any phase (the SDK sources are dev only).
HTTP = {
    **{k: v for k, v in VALID.items() if k != "huggingface"},
    "fetch": "http",
    "auth": {"type": "none", "vault_path": None},
    "http": {"urls": ["https://lab.example/a.pdf"]},
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
    assert (
        manifest.schedule.frequency_class == "F4"
    )  # on demand, unless it says otherwise


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
    manifest = DocumentManifest.model_validate({**HTTP, "phase": phase})
    for environment in ("development", "qa", "production"):
        assert manifest.cleared_for(environment) is (environment in cleared)
    assert not manifest.cleared_for("somewhere-else")


def test_include_and_exclude_globs_are_case_insensitive():
    manifest = DocumentManifest.model_validate({**VALID, "exclude": ["*draft*"]})
    assert manifest.wants("reports/2026/Report.PDF")
    assert not manifest.wants("notes.txt")
    assert not manifest.wants("drafts/a.pdf")


# ── The review's manifest thread: schedule, tokens beyond dev, SDK sources ─────
@pytest.mark.parametrize("frequency", ["F1", "F2", "F3", "F4", "F5"])
def test_schedule_is_cn01s_shape_with_a_frequency_class(tmp_path, frequency):
    path = tmp_path / "hf-lab-reports.yaml"
    path.write_text(
        yaml.safe_dump({**VALID, "schedule": {"class": frequency}}), encoding="utf-8"
    )
    assert DocumentManifest.load(path).schedule.frequency_class == frequency


@pytest.mark.parametrize(
    "schedule",
    [
        "hourly",  # a string, as the manifests said before: on_demand alone still reads
        {"class": "F6"},
        {"class": "f4"},
        {"class": "F4", "interval": "1h"},  # not here yet: comes with the scheduler
        {"frequency_class": "F4"},  # the model's own name is not CN-01's key
    ],
)
def test_a_schedule_outside_cn01s_shape_is_refused(schedule):
    with pytest.raises(ValidationError, match="schedule"):
        DocumentManifest.model_validate({**VALID, "schedule": schedule})


@pytest.mark.parametrize("phase", ["qa", "prod"])
@pytest.mark.parametrize(
    "fetch, block, auth",
    [
        ("kaggle", {"kaggle": {"handle": "owner/dataset"}}, "kaggle_token"),
        ("huggingface", {"huggingface": {"repo_id": "org/ds"}}, "hf_token"),
    ],
)
def test_a_token_beyond_the_dev_phase_needs_a_vault_path(fetch, block, auth, phase):
    """Environment tokens are for dev sources only: beyond dev, the manifest names where the
    runner reads the credential. (The source is refused beyond dev anyway, below; this is the
    first thing it is told.)"""
    base = {k: v for k, v in VALID.items() if k != "huggingface"}
    data = {**base, "fetch": fetch, **block, "phase": phase}
    with pytest.raises(ValidationError, match="needs auth.vault_path"):
        DocumentManifest.model_validate(
            {**data, "auth": {"type": auth, "vault_path": None}}
        )
    dev = {**data, "phase": "dev", "auth": {"type": auth, "vault_path": None}}
    assert DocumentManifest.model_validate(dev).auth.vault_path is None


@pytest.mark.parametrize("phase", ["qa", "prod"])
@pytest.mark.parametrize(
    "fetch, block, auth",
    [
        ("kaggle", {"kaggle": {"handle": "owner/dataset"}}, "kaggle_token"),
        ("huggingface", {"huggingface": {"repo_id": "org/ds"}}, "hf_token"),
        ("huggingface", {"huggingface": {"repo_id": "org/ds"}}, "none"),
    ],
)
def test_an_sdk_source_is_refused_beyond_the_dev_phase(fetch, block, auth, phase):
    """The Kaggle and Hugging Face SDKs download on their own, outside the egress rule: a
    manifest that clears one for qa or production is refused, even with its token in Vault.
    """
    base = {k: v for k, v in VALID.items() if k != "huggingface"}
    data = {
        **base,
        "fetch": fetch,
        **block,
        "phase": phase,
        "auth": {"type": auth, "vault_path": "secret/nickel/sources/token"},
    }
    with pytest.raises(ValidationError, match="dev phase only"):
        DocumentManifest.model_validate(data)


def test_the_old_schedule_string_on_demand_still_reads_as_f4_for_one_release(
    tmp_path, records
):
    """The re-review's probe: the dev deployment's manifests on disk still say
    ``schedule: on_demand``, the string before CN-01's shape, and every pull of one failed with a
    bare ValidationError. It reads as {class: F4} for this release, with a warning at each load.
    """
    path = tmp_path / "hf-lab-reports.yaml"
    path.write_text(
        yaml.safe_dump({**VALID, "schedule": "on_demand"}), encoding="utf-8"
    )
    assert DocumentManifest.load(path).schedule.frequency_class == "F4"
    warnings = [r.getMessage() for r in records if r.levelno == logging.WARNING]
    assert warnings == [
        "manifest hf-lab-reports: schedule: on_demand is deprecated and read as "
        "{class: F4} for this release only; write schedule: {class: F4}"
    ]


_SCHEDULE_IS_A_MAPPING = (
    "schedule value_error (schedule is a mapping, {class: F1..F5}; of the old strings only "
    "on_demand is still read, as {class: F4})"
)
_NO_CREDENTIAL = (
    "http value_error (https://lab.example: http.urls must carry no user or password (a "
    "credential belongs in Vault: auth.vault_path))"
)


@pytest.mark.parametrize(
    "content, problem",
    [
        ({**VALID, "schedule": "Jane_Doe_weekly"}, _SCHEDULE_IS_A_MAPPING),
        ({**VALID, "license": ""}, "license string_too_short"),
        ({**VALID, "id": "Jane Doe"}, "id string_pattern_mismatch"),
        (
            {**HTTP, "http": {"urls": ["ftp://user:secret@lab.example/Jane_Doe.pdf"]}},
            "http url_scheme",
        ),
        (
            {
                **HTTP,
                "http": {"urls": ["https://user:secret@lab.example/Jane_Doe.pdf"]},
            },
            _NO_CREDENTIAL,
        ),
        (
            {**VALID, "phi": True},
            "value_error (a PHI source needs a business associate agreement: set "
            "contract.baa)",
        ),
        # A key the model does not know is the file's own text: its error type alone.
        ({**VALID, "Jane_Doe_MRN12345.pdf": "x"}, "extra_forbidden"),
        ({**VALID, "schedule": {"Jane_Doe": 1}}, "schedule extra_forbidden"),
        (["Jane_Doe"], "model_type"),
        (
            {"id": "Jane Doe"},
            "auth missing, contract missing, description missing, fetch missing, "
            "id string_pattern_mismatch and 7 more",
        ),
    ],
)
def test_a_manifest_that_does_not_validate_says_where_and_why_without_quoting_it(
    tmp_path, content, problem
):
    """The re-review: a manifest that did not validate failed a job with a bare
    "ValidationError", and the command line printed pydantic's message, which quotes the file (a
    folder's path, a link with its credential). The error names the field and pydantic's error
    type now, and this module's own message, which names fields and hosts only; a traceback of it
    carries nothing more."""
    path = tmp_path / "m.yaml"
    path.write_text(yaml.safe_dump(content), encoding="utf-8")
    with pytest.raises(ManifestError) as caught:
        DocumentManifest.load(path)
    assert str(caught.value) == f"manifest does not validate: {problem}"
    shown = "".join(traceback.format_exception(caught.value))
    assert "Jane" not in shown
    assert "secret" not in shown


@pytest.mark.parametrize(
    "content, problem",
    [
        (b"id: [Jane_Doe\n", "ParserError at line 2, column 1"),
        (b"id: Jane_Doe \xff\n", "ReaderError"),  # not UTF-8
    ],
)
def test_a_manifest_that_is_not_yaml_says_where_without_quoting_it(
    tmp_path, content, problem
):
    """The YAML parser's message quotes the line it stopped at: the error is its type, and the
    line and column when the parser knows them."""
    path = tmp_path / "m.yaml"
    path.write_bytes(content)
    with pytest.raises(ManifestError) as caught:
        DocumentManifest.load(path)
    assert str(caught.value) == f"manifest is not YAML: {problem}"
    assert "Jane" not in "".join(traceback.format_exception(caught.value))


def test_a_manifest_that_cannot_be_read_is_an_os_error_for_the_caller(tmp_path):
    """Not a ManifestError: the caller has the path, and says what it can (the command line
    exits 2, the relay logs the error's type)."""
    with pytest.raises(FileNotFoundError):
        DocumentManifest.load(tmp_path / "gone.yaml")
