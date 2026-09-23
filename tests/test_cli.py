"""The command line as an operator uses it: ``python -m src.cli ...``."""

import io
import json
import logging
import re
import subprocess
import sys

import pytest

from src import __version__
from src.cli import main
from src.config import APP_DIR
from src.db.file_store import LocalFileStore
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
    assert "payload_hash" in json.loads(result.stdout)["properties"]


def test_schema_prints_json_or_writes_a_file(tmp_path):
    code, text = _run(["schema", "envelope"])
    assert code == 0
    assert "payload_hash" in json.loads(text)["properties"]
    target = tmp_path / "manifest.schema.json"
    code, _ = _run(["-v", "schema", "manifest", "--out", str(target)])
    assert code == 0
    assert "contract" in json.loads(target.read_text(encoding="utf-8"))["properties"]


def test_schema_envelope_is_v0_3_the_readers_contract():
    code, text = _run(["schema", "envelope"])
    schema = json.loads(text)
    properties = schema["properties"]
    # The reader's contract: any 0.x version reads, this service writes 0.3.
    assert properties["envelope_version"]["default"] == "0.3"
    assert properties["envelope_version"]["pattern"] == "^0\\.\\d+$"
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
        "member_id",
        "device_id",
        "fhir_release",
        "payload_hash",
    } <= set(properties)
    assert "payload_sha256" not in properties
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


def test_the_settings_are_read_from_the_process_environment_for_the_run(
    monkeypatch, tmp_path
):
    """No settings global: a run reads the environment it runs in, when it runs."""
    for name, value in _env(tmp_path).items():
        monkeypatch.setenv(name, value)
    assert _run(["documents", "status"]) == (0, "receipts by status: none\n")
    assert (tmp_path / "data" / "receipts.sqlite").is_file()  # this run's data dir


def test_an_unreachable_source_exits_1(tmp_path, capsys, manifest_writer):
    manifest = manifest_writer(tmp_path / "missing")
    assert _run(["documents", "pull", str(manifest)], _env(tmp_path))[0] == 1
    err = capsys.readouterr().err
    assert "local missing: not a folder" in err
    assert str(tmp_path) not in err  # the folder's path stays out of it


def test_an_aborted_run_exits_1(tmp_path, pdf_folder, manifest_writer):
    (pdf_folder / "a.pdf").rename(pdf_folder / "Jane_Doe.pdf")
    data = tmp_path / "data"
    data.mkdir()
    (data / "outbox").write_text("a file where the outbox folder should be")
    manifest = str(manifest_writer(pdf_folder))
    code, text = _run(["documents", "pull", manifest], _env(tmp_path))
    assert code == 1
    assert "ABORTED         publish failed; the pull stopped" in text  # no file named
    assert "Jane" not in text
    code, text = _run(["-v", "documents", "pull", manifest], _env(tmp_path))
    assert "ABORTED         publish failed at Jane_Doe.pdf" in text


def test_verbose_logs_each_item_by_its_ingest_id(
    tmp_path, pdf_folder, manifest_writer, records
):
    before = logger.level
    _run(["-v", "documents", "pull", str(manifest_writer(pdf_folder))], _env(tmp_path))
    with ReceiptStore(tmp_path / "data" / "receipts.sqlite") as receipts:
        [receipt] = receipts.recent(tenant_id=TENANT)
    debug = [r.getMessage() for r in records if r.levelno == logging.DEBUG]
    assert debug == [f"local-docs {receipt.ingest_id} -> published"]  # not the path
    assert logger.level == before


def test_a_pull_prints_the_paths_of_its_problems_only_with_verbose(
    tmp_path, pdf_folder, manifest_writer
):
    """In a container, what a command prints is the log stream: a clinic's file names stay out of
    it unless the operator asks for them."""
    (pdf_folder / "Jane_Doe_MRN12345.pdf").write_bytes(b"not a pdf")
    manifest = str(manifest_writer(pdf_folder))
    code, text = _run(["documents", "pull", manifest], _env(tmp_path))
    assert code == 0
    assert "  - content does not match the declared formats ['pdf']" in text
    assert "Jane" not in text
    code, text = _run(["-v", "documents", "pull", manifest], _env(tmp_path))
    assert "  - Jane_Doe_MRN12345.pdf: content does not match" in text


@pytest.mark.parametrize("action", [["pull", "m.yaml"], ["republish"]])
@pytest.mark.parametrize("limit", ["0", "-3", "many"])
def test_a_limit_is_a_positive_count_as_in_the_api(action, limit, capsys):
    """The jobs API refuses a limit under 1 (422); the command line refuses it too, instead of
    running a pull or a relay that does nothing (or everything)."""
    with pytest.raises(SystemExit) as exc:
        main(["documents", *action, "--limit", limit], out=io.StringIO())
    assert exc.value.code == 2
    assert "--limit" in capsys.readouterr().err


def test_an_os_error_that_stops_a_pull_is_reported_by_its_type_not_a_traceback(
    tmp_path, pdf_folder, manifest_writer, monkeypatch, capsys
):
    """The File Store cannot write (a full disk, say): the OS's text quotes the object's path, and
    a traceback would too. The command line says the pull stopped and the error's type, exit 1;
    -v adds the OS's text, for an operator who asks."""

    def full_disk(self, key, data, **kwargs):
        raise OSError(
            28, "No space left on device", f"/data/filestore/{key}/Jane_Doe.pdf"
        )

    monkeypatch.setattr(LocalFileStore, "put", full_disk)
    manifest = str(manifest_writer(pdf_folder))
    assert _run(["documents", "pull", manifest], _env(tmp_path))[0] == 1
    err = capsys.readouterr().err
    assert err == "error: the pull stopped: OSError\n"
    assert _run(["-v", "documents", "pull", manifest], _env(tmp_path))[0] == 1
    assert "No space left on device" in capsys.readouterr().err


def test_a_manifest_that_does_not_load_exits_2_and_says_why_without_quoting_it(
    tmp_path, pdf_folder, manifest_writer, capsys
):
    """The re-review's probe: an old manifest on disk (a schedule string) printed pydantic's
    traceback, which quotes the file, and exited 1. A manifest that does not load is a wrong
    setting: exit 2, content-free, no traceback."""
    manifest = manifest_writer(pdf_folder, schedule="Jane_Doe_weekly")
    assert _run(["documents", "pull", str(manifest)], _env(tmp_path)) == (2, "")
    err = capsys.readouterr().err
    assert err == (
        "error: manifest does not validate: schedule value_error (schedule is a mapping, "
        "{class: F1..F5}; of the old strings only on_demand is still read, as {class: F4})\n"
    )
    assert not (tmp_path / "data" / "receipts.sqlite").exists()  # nothing ran


def test_a_manifest_that_cannot_be_read_exits_2_with_the_error_type(tmp_path, capsys):
    """The OS's text quotes the path: the type only, and the text with -v."""
    missing = str(tmp_path / "Jane_Doe" / "local-docs.yaml")
    assert _run(["documents", "pull", missing], _env(tmp_path)) == (2, "")
    assert capsys.readouterr().err == (
        "error: the manifest could not be read: FileNotFoundError\n"
    )
    assert _run(["-v", "documents", "pull", missing], _env(tmp_path)) == (2, "")
    assert "No such file or directory" in capsys.readouterr().err
