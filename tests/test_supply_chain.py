"""What a reviewer signs off on and a build must keep: the base image by digest, and the development
sample-data sources out of the production image (Dockerfile, pyproject.toml, uv.lock, compose.yaml)."""

import re
import tomllib
from pathlib import Path

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
