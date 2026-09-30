"""CN-01 wire contracts and bounded, content-free manifest validation."""

import json
from pathlib import Path

import yaml
from jsonschema import Draft202012Validator, FormatChecker

from src.models.envelope import ProducedEnvelope

PACKAGE = Path(__file__).resolve().parents[2] / "contracts"
REPOSITORY = Path(__file__).resolve().parents[3]
SCHEMA_DIR = PACKAGE / "schemas" if PACKAGE.exists() else REPOSITORY / "schemas"
EXAMPLE = (
    PACKAGE / "examples/public-documents/manifest.yaml"
    if PACKAGE.exists()
    else REPOSITORY / "connectors/public-documents/manifest.yaml"
)
FILES = {
    "manifest": "connector-manifest.schema.json",
    "envelope": "raw-data-envelope.schema.json",
}
MAX_MANIFEST_BYTES = 65536


def schema(name):
    return json.loads((SCHEMA_DIR / FILES[name]).read_text())


def envelope_schema():
    result = ProducedEnvelope.model_json_schema(mode="serialization")
    result.update(
        {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": "urn:nickel:schemas:raw-data-envelope:0.3",
            "title": "Nickel raw data envelope v0.3",
            "oneOf": [
                {
                    "required": ["payload_ref"],
                    "properties": {
                        "payload_ref": {"type": "string"},
                        "payload": {"type": "null"},
                    },
                },
                {
                    "required": ["payload"],
                    "properties": {
                        "payload": {"type": "object"},
                        "payload_ref": {"type": "null"},
                    },
                },
            ],
        }
    )
    result["required"] = sorted(set(result["required"]) | {"envelope_version"})
    return result


def validator(contract):
    Draft202012Validator.check_schema(contract)
    return Draft202012Validator(contract, format_checker=FormatChecker())


class ManifestLoader(yaml.SafeLoader):
    """No YAML aliases, implicit timestamps, or duplicate mapping keys."""

    def compose_node(self, parent, index):
        if self.check_event(yaml.AliasEvent):
            raise ValueError("YAML aliases are not supported")
        return super().compose_node(parent, index)

    def construct_mapping(self, node, deep=False):
        keys = [self.construct_object(key, deep=deep) for key, _ in node.value]
        if any(not isinstance(key, str) for key in keys) or len(keys) != len(set(keys)):
            raise ValueError("Use unique string mapping keys")
        return super().construct_mapping(node, deep=deep)


def inspect_manifest(text, contract=None):
    """Return data and field/rule diagnostics without echoing submitted values."""
    if len(text.encode()) > MAX_MANIFEST_BYTES:
        return None, [{"path": "$", "rule": "Manifest exceeds 64 KiB"}]
    try:
        # ManifestLoader inherits SafeLoader; its only changes reject aliases and duplicate keys.
        data = yaml.load(text, Loader=ManifestLoader)  # nosec B506

        # Require ordinary JSON types and bound depth before schema traversal.
        def visit(value, depth=0):
            if depth > 20:
                raise ValueError("Manifest nesting exceeds 20 levels")
            if isinstance(value, dict):
                for child in value.values():
                    visit(child, depth + 1)
            elif isinstance(value, list):
                for child in value:
                    visit(child, depth + 1)
            elif not isinstance(value, (str, int, float, bool, type(None))):
                raise ValueError("Use JSON-compatible YAML values")

        visit(data)
        json.dumps(data, allow_nan=False)
    except (ValueError, yaml.YAMLError, RecursionError):
        return None, [
            {
                "path": "$",
                "rule": "Invalid YAML: use unique keys, no aliases, and JSON-compatible values",
            }
        ]
    problems = []
    for error in validator(
        contract if contract is not None else schema("manifest")
    ).iter_errors(data):
        # Schema paths are trusted. Never include unknown user-supplied field names or values.
        path = "$" + "".join(
            f".{p}"
            for p in error.absolute_path
            if isinstance(p, str)
            and p
            in {
                "id",
                "schema_version",
                "adapter",
                "version",
                "stream",
                "mode",
                "route",
                "auth",
                "type",
                "vault_path",
                "schedule",
                "class",
                "interval",
                "overlap",
                "backfill",
                "endpoints",
                "rate_limit",
                "requests",
                "window_seconds",
                "formats",
                "schema_id",
                "validate",
                "expectations",
                "freshness_slo",
                "contract",
                "baa",
                "dpa",
                "phi",
                "owner",
                "phase",
                "shadow",
                "raw_retention",
            }
        )
        rule = str(error.validator)
        if rule == "required":
            missing = [k for k in error.validator_value if k not in error.instance]
            rule = "required: " + ", ".join(missing)
        problems.append({"path": path, "rule": rule})
        if len(problems) == 30:
            break
    return data, problems
