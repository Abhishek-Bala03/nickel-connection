"""The command line as an operator uses it: ``python -m src.cli ...``."""

import io
import json
import logging
import re
import subprocess
import sys

import pytest

from src import __version__, cli
from src.cli import main
from src.config import APP_DIR, load_settings
from src.db.outbox import JsonlOutbox
from src.db.receipts import Receipt, ReceiptStatus, ReceiptStore
from src.logging import logger

TENANT = "0192a3b4-0000-7000-8000-000000000001"


def _run(argv, env=None):
    out = io.StringIO()
    code = main(argv, env=env, out=out)
    return code, out.getvalue()


def _env(tmp_path):
    return {"NICKEL_TENANT_ID": TENANT, "NICKEL_DATA_DIR": str(tmp_path / "data")}


def test_version_is_set():
    assert __version__ == "0.1.0"


def test_the_module_runs_as_a_program():
    result = subprocess.run(
        [sys.executable, "-m", "src.cli", "schema", "envelope"],
        cwd=APP_DIR,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    assert "payload_sha256" in json.loads(result.stdout)["properties"]


def test_schema_prints_json_or_writes_a_file(tmp_path):
    code, text = _run(["schema", "envelope"])
    assert code == 0
    assert "payload_sha256" in json.loads(text)["properties"]
    target = tmp_path / "manifest.schema.json"
    code, _ = _run(["-v", "schema", "manifest", "--out", str(target)])
    assert code == 0
    assert "contract" in json.loads(target.read_text(encoding="utf-8"))["properties"]


def test_schema_envelope_is_v0_2_with_the_lab_route():
    code, text = _run(["schema", "envelope"])
    schema = json.loads(text)
    properties = schema["properties"]
    assert properties["envelope_version"]["default"] == "0.2"
    assert {
        "fetch",
        "route",
        "mode",
        "idempotency_key",
        "trace_id",
        "attempt",
        "schema_id",
        "consent_ref",
        "quality",
    } <= set(properties)
    assert properties["route"]["pattern"] == "^L[1-9]$"
    assert properties["quality"]["$ref"].endswith("/Quality")


def test_a_pull_prints_the_trace_id_its_envelopes_carry(
    tmp_path, pdf_folder, manifest_writer
):
    code, text = _run(
        ["documents", "pull", str(manifest_writer(pdf_folder))], _env(tmp_path)
    )
    assert code == 0
    [line] = [line for line in text.splitlines() if line.startswith("trace_id")]
    trace_id = line.split()[1]
    assert re.fullmatch(r"[0-9a-f]{32}", trace_id)
    [message] = JsonlOutbox(tmp_path / "data" / "outbox").read("raw_data")
    assert message["value"]["trace_id"] == trace_id


def test_pull_then_status(tmp_path, pdf_folder, manifest_writer):
    env = _env(tmp_path)
    code, text = _run(["documents", "pull", str(manifest_writer(pdf_folder))], env)
    assert code == 0
    assert "published       1" in text
    # Another tenant's receipt in the same file (written as another deployment would write it):
    # the status is this tenant's, and it names items by ingest id and hash, never by path.
    with ReceiptStore(tmp_path / "data" / "receipts.sqlite") as receipts:
        [own] = receipts.recent(tenant_id=TENANT)
        receipts.add(
            Receipt(
                ingest_id="0192a3b4-0000-7000-8000-0000000000bb",
                tenant_id="0192a3b4-0000-7000-8000-000000000002",
                source="local-docs",
                message_id="local:theirs/Jane-Roe-labs.pdf",
                payload_sha256="ab" * 32,
                status=ReceiptStatus.REJECTED,
                received_at=own.received_at,
                updated_at=own.updated_at,
            )
        )
    code, text = _run(["documents", "status", "--source", "local-docs"], env)
    assert code == 0
    assert "receipts by status: published 1\n" in text
    assert (
        f"published       local-docs  {own.ingest_id}  {own.payload_sha256[:12]}"
        in text
    )
    assert "a.pdf" not in text
    assert "Jane-Roe" not in text


def test_status_with_no_receipts(tmp_path):
    assert _run(["documents", "status"], _env(tmp_path)) == (
        0,
        "receipts by status: none\n",
    )


def test_a_missing_tenant_exits_2(capsys):
    assert _run(["documents", "status"], env={})[0] == 2
    assert "NICKEL_TENANT_ID" in capsys.readouterr().err


def test_a_bad_setting_exits_2(capsys):
    env = {"NICKEL_TENANT_ID": TENANT, "NICKEL_MAX_FILE_BYTES": "lots"}
    assert _run(["documents", "status"], env)[0] == 2
    assert "max_file_bytes" in capsys.readouterr().err


def test_the_service_settings_are_the_default(monkeypatch, tmp_path):
    monkeypatch.setattr(cli, "Service", load_settings(environ=_env(tmp_path)))
    assert _run(["documents", "status"]) == (0, "receipts by status: none\n")


def test_an_unreachable_source_exits_1(tmp_path, capsys, manifest_writer):
    manifest = manifest_writer(tmp_path / "missing")
    assert _run(["documents", "pull", str(manifest)], _env(tmp_path))[0] == 1
    err = capsys.readouterr().err
    assert "local missing: not a folder" in err
    assert str(tmp_path) not in err  # the folder's path stays out of it


def test_an_aborted_run_exits_1(tmp_path, pdf_folder, manifest_writer):
    data = tmp_path / "data"
    data.mkdir()
    (data / "outbox").write_text("a file where the outbox folder should be")
    code, text = _run(
        ["documents", "pull", str(manifest_writer(pdf_folder))], _env(tmp_path)
    )
    assert code == 1
    assert "ABORTED" in text


def test_verbose_logs_each_file(tmp_path, pdf_folder, manifest_writer, records):
    before = logger.level
    _run(["-v", "documents", "pull", str(manifest_writer(pdf_folder))], _env(tmp_path))
    assert any(
        r.levelno == logging.DEBUG and r.getMessage() == "local-docs a.pdf -> published"
        for r in records
    )
    assert logger.level == before


@pytest.mark.parametrize("action", [["pull", "m.yaml"], ["republish"]])
@pytest.mark.parametrize("limit", ["0", "-3", "many"])
def test_a_limit_is_a_positive_count_as_in_the_api(action, limit, capsys):
    """The jobs API refuses a limit under 1 (422); the command line refuses it too, instead of
    running a pull or a relay that does nothing (or everything)."""
    with pytest.raises(SystemExit) as exc:
        main(["documents", *action, "--limit", limit], out=io.StringIO())
    assert exc.value.code == 2
    assert "--limit" in capsys.readouterr().err
