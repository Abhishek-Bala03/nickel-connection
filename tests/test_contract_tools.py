"""CI command and registry publication without relying on a deployed registry."""

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[1]


def load(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_contract_gate_and_schema_drift(tmp_path):
    tool = load("contracts")
    assert tool.check() >= 1
    import shutil

    shutil.copytree(ROOT / "schemas", tmp_path / "schemas")
    shutil.copytree(ROOT / "connectors", tmp_path / "connectors")
    p = tmp_path / "connectors/public-documents/manifest.yaml"
    p.write_text(p.read_text().replace("owner: connection-team\n", ""))
    with pytest.raises(ValueError, match="owner"):
        tool.check(tmp_path)
    p.write_text(
        (ROOT / "connectors/public-documents/manifest.yaml")
        .read_text()
        .replace("id: public-documents", "id: different-source")
    )
    with pytest.raises(ValueError, match="directory"):
        tool.check(tmp_path)
    p = tmp_path / "schemas/raw-data-envelope.schema.json"
    p.write_text("{}")
    with pytest.raises(ValueError, match="stale"):
        tool.check(tmp_path)


@pytest.mark.parametrize("exists", [False, True])
def test_registry_compatibility_registration_and_readback(exists):
    tool = load("register_envelope")
    body = tool.payload()
    seen = []

    def respond(req):
        seen.append(req.url.path)
        if req.method == "GET":
            return (
                httpx.Response(200, json={"id": 1})
                if exists
                else httpx.Response(404, json={"error_code": 40401})
            )
        assert json.loads(req.content) == body
        if req.url.path.startswith("/compatibility"):
            return httpx.Response(200, json={"is_compatible": True})
        return httpx.Response(
            200, json={"id": 2, "version": 1, "schema": body["schema"]}
        )

    with httpx.Client(
        base_url="https://registry.test", transport=httpx.MockTransport(respond)
    ) as client:
        assert tool.register(client, "raw_data-value", body) == {
            "id": 2,
            "version": 1,
            "subject": "raw_data-value",
        }
    assert any(p.startswith("/compatibility") for p in seen) == exists


@pytest.mark.parametrize("case", ["incompatible", "unauthorized", "mismatch", "no-id"])
def test_registry_failure_never_claims_success(case):
    tool = load("register_envelope")
    seen = []

    def respond(req):
        seen.append(req.url.path)
        if req.method == "GET":
            return httpx.Response(
                401 if case == "unauthorized" else 200, json={"id": 1}
            )
        if req.url.path.startswith("/compatibility"):
            return httpx.Response(200, json={"is_compatible": case != "incompatible"})
        return httpx.Response(
            200, json={} if case == "no-id" else {"id": 2, "version": 1, "schema": "{}"}
        )

    with httpx.Client(
        base_url="https://registry.test", transport=httpx.MockTransport(respond)
    ) as client:
        with pytest.raises((ValueError, httpx.HTTPError)):
            tool.register(client, "raw_data-value", tool.payload())
    if case in {"incompatible", "unauthorized"}:
        assert "/subjects/raw_data-value/versions" not in seen


def test_registration_default_is_offline():
    result = subprocess.run(
        [sys.executable, "-m", "scripts.register_envelope"],
        cwd=ROOT,
        env={"PYTHONPATH": str(ROOT / "app")},
        capture_output=True,
        text=True,
        check=True,
    )
    assert json.loads(result.stdout)["schemaType"] == "JSON"
