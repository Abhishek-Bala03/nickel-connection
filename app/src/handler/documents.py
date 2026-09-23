"""Document sources: pull one into the connection layer, publish open receipts again, and read back
what the receipts say.

Used by the jobs API, the CLI and the ``documents.pull`` and ``documents.republish`` jobs. The pieces
(File Store, outbox, receipts) are opened here from the settings, so callers only pass settings and a
manifest. What comes back out is receipt metadata: the connection layer never reads, serves or judges
a document's content. The original waits in the File Store for the Transform layer (the Document
worker), which finds it by the envelope's ``payload_ref``.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

from src.config import ServiceSettings
from src.connectors.documents import DocumentConnector, DocumentManifest, RunSummary
from src.db.file_store import LocalFileStore
from src.db.layout import DataLayout
from src.db.outbox import JsonlOutbox, PublishError
from src.db.receipts import Receipt, ReceiptStatus, ReceiptStore
from src.error import NotFoundError, UnavailableError
from src.handler.ingest import IngestBridge, Outcome
from src.utility.ids import uuid7

# What the relay takes up, in this order: received (a crash before the publish), publish_failed (a
# broker that was down), and held (shadow mode), the last only once the source is enabled.
# too_large receipts have no stored original (the edge check stops them before the File Store), so
# they wait for the source to list them again.
_REPUBLISHABLE = (
    ReceiptStatus.RECEIVED,
    ReceiptStatus.PUBLISH_FAILED,
    ReceiptStatus.HELD,
)


def require_tenant(settings: ServiceSettings) -> UUID:
    if settings.tenant_id is None:
        raise UnavailableError("NICKEL_TENANT_ID is not set (a UUID; see .env.example)")
    return settings.tenant_id


def manifest_path(settings: ServiceSettings, manifest_id: str) -> Path:
    """The manifest file for an id (the file stem); ids are validated by the caller's model."""
    path = settings.manifest_dir / f"{manifest_id}.yaml"
    if not path.is_file():
        raise NotFoundError(f"no document manifest {manifest_id!r}")
    return path


def load_manifest(settings: ServiceSettings, manifest_id: str) -> DocumentManifest:
    return DocumentManifest.load(manifest_path(settings, manifest_id))


@contextmanager
def ingest_bridge(settings: ServiceSettings) -> Iterator[IngestBridge]:
    """The bridge on this deployment's stores; the receipts connection closes with the block."""
    tenant = require_tenant(settings)
    layout = DataLayout(settings.data_dir)
    with ReceiptStore(layout.receipts_db) as receipts:
        yield IngestBridge(
            tenant_id=tenant,
            store=LocalFileStore(layout.filestore),
            publisher=JsonlOutbox(layout.outbox),
            receipts=receipts,
            max_bytes=settings.max_file_bytes,
        )


def pull_documents(
    settings: ServiceSettings,
    manifest: DocumentManifest,
    *,
    limit: int | None = None,
    trace_id: str | None = None,
) -> RunSummary:
    """Run the document connector for one manifest. Raises FetchError if the source is unreachable.
    ``trace_id`` is the run's id on every envelope it publishes: the job's, or a fresh one.
    """
    with ingest_bridge(settings) as bridge:
        return DocumentConnector(
            manifest,
            bridge,
            environment=settings.environment,
            max_file_bytes=settings.max_file_bytes,
            trace_id=trace_id,
        ).run(limit=limit)


@dataclass
class RepublishSummary:
    """What the relay did: numbers and the run's id, nothing of the content."""

    source: str | None
    trace_id: str
    open_receipts: int = 0  # received, publish_failed and held receipts looked at
    published: int = 0
    duplicates: int = 0  # published by another run meanwhile
    # Held receipts whose source is still in shadow mode, not cleared for this environment, or
    # has no manifest any more: they wait.
    held_back: int = 0
    # Receipts from before the envelope was kept on them: they wait for the source to list the
    # item again, which rebuilds the envelope. Counted, not guessed at.
    no_envelope: int = 0
    # Content-free (what a job record keeps): why the run did not reach the end.
    error: str | None = None

    def counts(self) -> dict[str, int]:
        return {
            "open_receipts": self.open_receipts,
            "published": self.published,
            "duplicates": self.duplicates,
            "held_back": self.held_back,
            "no_envelope": self.no_envelope,
        }

    def lines(self) -> list[str]:
        out = [
            f"source          {self.source or 'all'}",
            f"trace_id        {self.trace_id}",
            *(f"{name:<15} {value}" for name, value in self.counts().items()),
        ]
        if self.error:
            out.append(f"ABORTED         {self.error}")
        return out


def republish_documents(
    settings: ServiceSettings,
    *,
    source: str | None = None,
    limit: int | None = None,
    trace_id: str | None = None,
) -> RepublishSummary:
    """Publish the tenant's open receipts again from the envelopes they store, without their
    sources: ``received`` and ``publish_failed`` ones, and ``held`` ones once their manifest is
    enabled (found, cleared for this environment and no longer in shadow mode). Oldest first, at
    most ``limit`` of them; a publish failure stops the run, as it stops a pull.
    """
    tenant = str(require_tenant(settings))
    summary = RepublishSummary(source=source, trace_id=trace_id or uuid7().hex)
    enabled: dict[str, bool] = {}  # source -> whether its held items may go out
    with ingest_bridge(settings) as bridge:
        for status in _REPUBLISHABLE:
            for receipt in bridge.receipts.with_status(
                status, source, tenant_id=tenant
            ):
                if (
                    limit is not None
                    and summary.published + summary.duplicates >= limit
                ):
                    return summary
                summary.open_receipts += 1
                if status is ReceiptStatus.HELD and not _source_enabled(
                    settings, receipt.source, enabled
                ):
                    summary.held_back += 1
                elif receipt.envelope is None:
                    summary.no_envelope += 1
                elif not _republish_one(bridge, receipt, summary):
                    return summary
    return summary


def _republish_one(
    bridge: IngestBridge, receipt: Receipt, summary: RepublishSummary
) -> bool:
    """Republish one receipt and count the outcome. False stops the run: a publish failed, and
    nothing after it would be durable (the receipt says publish_failed, for the next run).
    """
    try:
        result = bridge.republish(receipt, summary.trace_id)
    except PublishError:
        summary.error = "publish failed; the republish stopped"
        return False
    if result.outcome is Outcome.PUBLISHED:
        summary.published += 1
    else:
        summary.duplicates += 1
    return True


def _source_enabled(
    settings: ServiceSettings, source: str, cache: dict[str, bool]
) -> bool:
    """Whether a source's held items may go out: its manifest is there, cleared for this
    environment and no longer in shadow mode (the "sandbox test > shadow > enable" flow).
    """
    if source not in cache:
        try:
            manifest = load_manifest(settings, source)
        except NotFoundError:
            cache[source] = False
        else:
            cache[source] = (
                manifest.cleared_for(settings.environment) and not manifest.shadow
            )
    return cache[source]


def receipt_counts(
    settings: ServiceSettings, source: str | None = None
) -> dict[str, int]:
    """This deployment's tenant's receipts by status; 503 (UnavailableError) without a tenant."""
    tenant = str(require_tenant(settings))
    with ReceiptStore(DataLayout(settings.data_dir).receipts_db) as receipts:
        return receipts.counts(source, tenant_id=tenant)


def list_documents(
    settings: ServiceSettings,
    *,
    source: str | None = None,
    status: ReceiptStatus | None = None,
    limit: int = 20,
) -> list[Receipt]:
    """This tenant's newest receipts first: what arrived and how far it got, never what it says."""
    tenant = str(require_tenant(settings))
    with ReceiptStore(DataLayout(settings.data_dir).receipts_db) as receipts:
        return receipts.recent(source, limit, status=status, tenant_id=tenant)
