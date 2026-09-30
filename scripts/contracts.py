"""Check CN-01 artifacts: PYTHONPATH=app python -m scripts.contracts."""

import argparse
import json
from pathlib import Path

from src.contracts import envelope_schema, inspect_manifest, validator

ROOT = Path(__file__).resolve().parents[1]


def check(root=ROOT):
    envelope = json.loads((root / "schemas/raw-data-envelope.schema.json").read_text())
    if envelope != envelope_schema():
        raise ValueError("Envelope schema is stale; run --write-envelope")
    validator(envelope)
    manifest = json.loads((root / "schemas/connector-manifest.schema.json").read_text())
    validator(manifest)
    paths = sorted((root / "connectors").glob("*/manifest.yaml"))
    if not paths:
        raise ValueError("No connectors/<id>/manifest.yaml files found")
    for path in paths:
        data, errors = inspect_manifest(path.read_text(), manifest)
        if errors:
            raise ValueError(f"{path.relative_to(root)}: {errors}")
        if data["id"] != path.parent.name:
            raise ValueError(f"{path.relative_to(root)}: id must match directory")
    return len(paths)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write-envelope", action="store_true")
    args = parser.parse_args()
    if args.write_envelope:
        (ROOT / "schemas/raw-data-envelope.schema.json").write_text(
            json.dumps(envelope_schema(), indent=2) + "\n"
        )
    try:
        count = check()
    except (ValueError, OSError) as exc:
        parser.exit(1, f"Contract check failed: {exc}\n")
    print(f"Both contracts valid; {count} manifests checked.")


if __name__ == "__main__":
    main()
