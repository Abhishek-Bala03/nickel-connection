"""What a reviewer signs off on and a build must keep: the base image by digest, and the development
sample-data sources out of the production image (Dockerfile, pyproject.toml, uv.lock, compose.yaml)."""

import re
import tomllib
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
DEV_SOURCES = {"kagglehub", "huggingface-hub"}


def _read(name: str) -> str:
    return (ROOT / name).read_text(encoding="utf-8")


def _names(requirements) -> set[str]:
    """Package names of requirement strings ("uvicorn[standard]>=0.53" is uvicorn)."""
    return {
        re.split(r"[\s\[<>=~!;]", requirement, 1)[0].lower()
        for requirement in requirements
        if isinstance(requirement, str)
    }


def test_the_base_image_is_pinned_by_digest():
    dockerfile = _read("Dockerfile")
    [base] = re.findall(r"^FROM\s+(\S+)", dockerfile, flags=re.MULTILINE)
    assert re.fullmatch(r"python@sha256:[0-9a-f]{64}", base), base
    assert (
        "python:3.11-slim" in dockerfile
    )  # the tag the digest stands for, for the reader


def test_the_dev_sources_are_a_group_of_their_own_inside_dev():
    project = tomllib.loads(_read("pyproject.toml"))
    assert _names(project["project"]["dependencies"]).isdisjoint(DEV_SOURCES)
    groups = project["dependency-groups"]
    assert _names(groups["dev-sources"]) == DEV_SOURCES
    # uv sync and the tests (tests/test_sources.py imports them) have them through dev.
    assert {"include-group": "dev-sources"} in groups["dev"]


def test_the_lock_keeps_the_dev_sources_out_of_the_image():
    lock = tomllib.loads(_read("uv.lock"))
    [project] = [p for p in lock["package"] if p["name"] == "nickel-connection"]
    assert {d["name"] for d in project["dependencies"]}.isdisjoint(DEV_SOURCES)
    groups = project["dev-dependencies"]
    assert {d["name"] for d in groups["dev-sources"]} == DEV_SOURCES


def test_the_server_names_no_software_in_its_responses():
    # uvicorn puts "server: uvicorn" on every response unless told not to: a name for a scanner to
    # look up. compose.yaml runs the image's CMD, so a local run inherits the flag.
    dockerfile = _read("Dockerfile")
    [command] = re.findall(r"^CMD \[(.*)\]$", dockerfile, flags=re.MULTILINE)
    assert '"uvicorn"' in command
    assert '"--no-server-header"' in command


def test_the_image_installs_the_dev_sources_only_when_asked():
    dockerfile = _read("Dockerfile")
    assert "\nARG DEV_SOURCES=\n" in dockerfile  # empty, so a plain build is production
    assert (
        "uv export --frozen --no-dev --no-emit-project "
        "${DEV_SOURCES:+--group dev-sources}" in dockerfile
    )
    assert 'DEV_SOURCES: "1"' in _read("compose.yaml")  # the local image


def test_boto3_is_in_the_image_and_moto_only_in_the_tests():
    project = tomllib.loads(_read("pyproject.toml"))
    runtime = _names(project["project"]["dependencies"])
    assert "boto3" in runtime
    assert "moto" not in runtime
    assert "moto" in _names(project["dependency-groups"]["dev"])
    lock = tomllib.loads(_read("uv.lock"))
    [locked] = [p for p in lock["package"] if p["name"] == "nickel-connection"]
    # What `uv export --no-dev` installs in the image.
    image = {d["name"] for d in locked["dependencies"]}
    assert "boto3" in image
    assert "moto" not in image
    assert "moto" in {d["name"] for d in locked["dev-dependencies"]["dev"]}


def test_the_development_object_store_is_pinned_off_by_default_and_commits_no_secret():
    compose = yaml.safe_load(_read("compose.yaml"))
    services = compose["services"]
    for name in ("seaweedfs", "seaweedfs-init"):
        assert services[name]["profiles"] == ["s3"]  # only with COMPOSE_PROFILES=s3
        # A release by its digest, never a tag that moves (latest, dev) or a tag alone.
        assert re.fullmatch(
            r"chrislusf/seaweedfs:\d+\.\d+@sha256:[0-9a-f]{64}", services[name]["image"]
        )
    assert services["seaweedfs"]["image"] == services["seaweedfs-init"]["image"]
    # Its master, volume server and filer take no credentials: loopback only. The S3 API, which
    # checks its identities, is the one listener on the network.
    command = services["seaweedfs"]["command"]
    assert {"-ip.bind=127.0.0.1", "-s3.ip.bind=0.0.0.0"} <= set(command)
    # The IAM gRPC that listens with the S3 API takes a token signed with a key made at start.
    assert "export WEED_JWT_FILER_SIGNING_KEY" in services["seaweedfs"]["entrypoint"][2]
    assert services["seaweedfs-init"]["network_mode"] == "service:seaweedfs"
    # The identities are written at start from compose's secret (the committed file names the
    # variable, never a value, and `docker compose config` prints no config that holds it), into
    # a tmpfs; SeaweedFS keeps no capabilities and cannot gain privileges.
    assert "configs" not in compose
    assert compose["secrets"] == {
        "s3-secret-access-key": {"environment": "NICKEL_S3_SECRET_ACCESS_KEY"}
    }
    seaweedfs = services["seaweedfs"]
    assert seaweedfs["secrets"] == ["s3-secret-access-key"]
    assert "cat /run/secrets/s3-secret-access-key" in seaweedfs["entrypoint"][2]
    assert "/etc/seaweedfs" in seaweedfs["tmpfs"]
    assert seaweedfs["cap_drop"] == ["ALL"]
    # only what the image needs to drop to its own user
    assert seaweedfs["cap_add"] == ["CHOWN", "SETUID", "SETGID"]
    assert seaweedfs["security_opt"] == ["no-new-privileges:true"]
    environment = services["nickel-connection"]["environment"]
    assert environment["NICKEL_FILE_STORE"] == "${NICKEL_FILE_STORE:-local}"
