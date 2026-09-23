"""Command line, for one-off runs on a dev box or in the container (``python -m src.cli ...``).

    python -m src.cli documents pull <manifest.yaml> [--limit N]      pull a document source
    python -m src.cli documents republish [--source ID] [--limit N]   publish open receipts again
    python -m src.cli documents status [--source ID] [--recent N]     what the receipts say
    python -m src.cli schema envelope|manifest [--out FILE]           JSON Schema of a contract

The same settings as the service (conf/application.conf and the environment). Exit codes: 0 done,
1 the run had a problem (source unreachable, run aborted), 2 a setting is wrong.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import TextIO

from src.config import Service, ServiceSettings, load_settings
from src.connectors.documents import DocumentManifest
from src.connectors.documents.sources import FetchError
from src.error import UnavailableError
from src.handler import documents
from src.logging import logger
from src.models.envelope import Envelope
from src.utility.ids import uuid7

SCHEMAS = {"envelope": Envelope, "manifest": DocumentManifest}


def _fail(message: str, code: int) -> int:
    print(f"error: {message}", file=sys.stderr)
    return code


def _pull(args: argparse.Namespace, settings: ServiceSettings, out: TextIO) -> int:
    manifest = DocumentManifest.load(args.manifest)
    try:
        # A command-line run has no job id, so it mints its trace id (printed with the summary).
        summary = documents.pull_documents(
            settings, manifest, limit=args.limit, trace_id=uuid7().hex
        )
    except FetchError as exc:
        return _fail(str(exc), 1)
    print("\n".join(summary.lines()), file=out)
    return 1 if summary.aborted else 0


def _republish(args: argparse.Namespace, settings: ServiceSettings, out: TextIO) -> int:
    summary = documents.republish_documents(
        settings, source=args.source, limit=args.limit, trace_id=uuid7().hex
    )
    print("\n".join(summary.lines()), file=out)
    return 1 if summary.error else 0


def _status(args: argparse.Namespace, settings: ServiceSettings, out: TextIO) -> int:
    counts = documents.receipt_counts(settings, args.source)
    receipts = documents.list_documents(settings, source=args.source, limit=args.recent)
    print(
        "receipts by status: "
        + (", ".join(f"{k} {v}" for k, v in counts.items()) or "none"),
        file=out,
    )
    # The ingest id and the hash name the item; the message id (a path at the source, which can
    # carry a patient's name) stays in the receipt.
    for receipt in receipts:
        print(
            f"  {receipt.received_at}  {receipt.status:<15} {receipt.source}  "
            f"{receipt.ingest_id}  {receipt.payload_sha256[:12]}",
            file=out,
        )
    return 0


def _schema(args: argparse.Namespace, out: TextIO) -> int:
    text = json.dumps(SCHEMAS[args.name].model_json_schema(), indent=2)
    if args.out:
        Path(args.out).write_text(text + "\n", encoding="utf-8")
    else:
        print(text, file=out)
    return 0


def _positive_int(text: str) -> int:
    """A count of at least 1, as the jobs API takes it (argparse turns the error into exit 2)."""
    try:
        value = int(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"{text!r} is not a whole number") from None
    if value < 1:
        raise argparse.ArgumentTypeError(f"{value} is less than 1")
    return value


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m src.cli", description="Nickel connection layer"
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="log every file")
    commands = parser.add_subparsers(dest="command", required=True)

    docs = commands.add_parser("documents", help="document sources (PDFs)")
    doc_commands = docs.add_subparsers(dest="action", required=True)
    pull = doc_commands.add_parser(
        "pull", help="pull a document source described by a manifest"
    )
    pull.add_argument("manifest", type=Path)
    pull.add_argument(
        "--limit",
        type=_positive_int,
        default=None,
        help="stop after this many matching files",
    )
    republish = doc_commands.add_parser(
        "republish",
        help="publish the open receipts again from the envelopes they store",
    )
    republish.add_argument("--source", default=None, help="one source (a manifest id)")
    republish.add_argument(
        "--limit",
        type=_positive_int,
        default=None,
        help="publish at most this many receipts",
    )
    status = doc_commands.add_parser("status", help="summarise the ingest receipts")
    status.add_argument("--source", default=None)
    status.add_argument("--recent", type=int, default=10)

    schema = commands.add_parser("schema", help="print the JSON Schema of a contract")
    schema.add_argument("name", choices=sorted(SCHEMAS))
    schema.add_argument("--out", type=Path, default=None)
    return parser


_ACTIONS = {"pull": _pull, "republish": _republish, "status": _status}


def _documents(
    args: argparse.Namespace, env: Mapping[str, str] | None, out: TextIO
) -> int:
    try:
        settings = Service if env is None else load_settings(environ=env)
        documents.require_tenant(settings)
    except (ValueError, UnavailableError) as exc:
        return _fail(str(exc), 2)
    return _ACTIONS[args.action](args, settings, out)


def main(
    argv: list[str] | None = None,
    env: Mapping[str, str] | None = None,
    out: TextIO | None = None,
) -> int:
    """``env`` replaces the process environment (tests); ``out`` replaces stdout."""
    out = out or sys.stdout
    args = _parser().parse_args(argv)
    level = logger.level
    logger.setLevel(logging.DEBUG if args.verbose else logging.WARNING)
    try:
        if args.command == "schema":
            return _schema(args, out)
        return _documents(args, env, out)
    finally:
        logger.setLevel(level)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
