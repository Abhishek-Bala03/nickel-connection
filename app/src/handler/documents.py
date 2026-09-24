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
from src.connectors.documents import (
    DocumentConnector,
    DocumentManifest,
    ManifestError,
    RunSummary,
)
from src.db.file_store import open_file_store
from src.db.layout import DataLayout
from src.db.outbox import JsonlOutbox, PublishError
from src.db.receipts import Receipt, ReceiptStatus, ReceiptStore
from src.error import NotFoundError, UnavailableError
from src.handler.ingest import IngestBridge, Outcome
from src.logging import logger
from src.utility.ids import uuid7

# What the relay takes up, in this order: received (a crash before the publish), publish_failed (a
# broker that was down) and held (shadow mode). Each of them only while its source is enabled, the
# rules a pull obeys: its manifest is found, cleared for this environment and out of shadow mode.
# too_large receipts have no stored original (the size check stops them before the File Store), so
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
            store=open_file_store(settings),
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
    # The received, publish_failed and held receipts open when the run began (for its source, if it
    # names one). The counts below say what became of them; with a limit or a publish failure the
    # rest were not reached, and a receipt another run moved on meanwhile (a pull that published
    # it) is counted open but not taken up.
    open_receipts: int = 0
    published: int = 0
    duplicates: int = 0  # published by another run meanwhile
    # Open receipts of a source that is not enabled: no manifest any more, not cleared for this
    # environment, back in shadow mode, or a manifest that cannot be read. Counted in the
    # database, never loaded, never sent: they wait.
    held_back: int = 0
    # Receipts whose stored envelope cannot go out (it does not validate, or is from a newer
    # version than this service writes): set aside, the receipt says why, and the relay goes on.
    # The next pull of the item publishes an envelope it builds again; the stored one stays.
    bad_envelope: int = 0
    # Receipts from before the envelope was kept on them: they wait for the source to list the
    # item again, whose pull builds one. Counted, not guessed at.
    no_envelope: int = 0
    # Content-free (what a job record keeps): why the run did not reach the end.
    error: str | None = None

    @property
    def taken(self) -> int:
        """The receipts this run took up: what a limit bounds, whatever became of them."""
        return self.published + self.duplicates + self.bad_envelope + self.no_envelope

    def counts(self) -> dict[str, int]:
        return {
            "open_receipts": self.open_receipts,
            "published": self.published,
            "duplicates": self.duplicates,
            "held_back": self.held_back,
            "bad_envelope": self.bad_envelope,
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
    sources: ``received``, ``publish_failed`` and ``held`` ones, each only while its source is
    enabled (its manifest found, cleared for this environment and out of shadow mode, as a pull
    requires). Oldest first within each status, at most ``limit`` of them taken up (published,
    found published already, set aside or without an envelope); a publish failure stops the run,
    as it stops a pull.

    The open receipts are counted by source in the database first, and each source's manifest is
    read once. The ones of a source that is not enabled are counted as held back without loading
    a row; the others are read a page at a time (``ReceiptStore.in_status``), their source
    filtered in the query, so memory holds one page however many are open. A stored envelope that
    does not validate is set aside and counted, and the run goes on.
    """
    tenant = str(require_tenant(settings))
    summary = RepublishSummary(source=source, trace_id=trace_id or uuid7().hex)
    with ingest_bridge(settings) as bridge:
        waiting = bridge.receipts.counts_by_source(
            _REPUBLISHABLE, source, tenant_id=tenant
        )
        enabled = _enabled_sources(settings, waiting, summary)
        for status in _REPUBLISHABLE:
            # Nothing of an enabled source in this status: no query at all.
            if any(waiting.get((status, name)) for name in enabled) and not _relay(
                bridge, status, enabled=enabled, limit=limit, summary=summary
            ):
                break
    return summary


def _enabled_sources(
    settings: ServiceSettings,
    waiting: dict[tuple[ReceiptStatus, str], int],
    summary: RepublishSummary,
) -> set[str]:
    """The sources whose open receipts may go out (each manifest read once). Every open receipt
    is counted, and those of the other sources are counted as held back, none of them loaded.
    """
    sources = {name for _status, name in waiting.keys()}
    enabled = {name for name in sources if _source_enabled(settings, name)}
    for (_, name), count in waiting.items():
        summary.open_receipts += count
        if name not in enabled:
            summary.held_back += count
    return enabled


def _relay(
    bridge: IngestBridge,
    status: ReceiptStatus,
    *,
    enabled: set[str],
    limit: int | None,
    summary: RepublishSummary,
) -> bool:
    """Republish the enabled sources' receipts in one status, oldest first, a page at a time.
    False when the run stops here: the limit is reached, or a publish failed. The limit counts
    every receipt taken up, set aside or without an envelope included, so one run examines at
    most ``limit`` of them, however many stay open."""
    tenant = str(bridge.tenant_id)
    for receipt in bridge.receipts.in_status(status, enabled, tenant_id=tenant):
        if limit is not None and summary.taken >= limit:
            return False
        if not _republish_one(bridge, receipt, summary):
            return False
    return True


def _republish_one(
    bridge: IngestBridge, receipt: Receipt, summary: RepublishSummary
) -> bool:
    """Republish one receipt and count the outcome. False stops the run: a publish failed, and
    nothing after it would be durable (the receipt says publish_failed, for the next run).
    """
    if receipt.envelope is None:
        summary.no_envelope += 1
        return True
    try:
        result = bridge.republish(receipt, summary.trace_id)
    except PublishError:
        summary.error = "publish failed; the republish stopped"
        return False
    if result.outcome is Outcome.PUBLISHED:
        summary.published += 1
    elif result.outcome is Outcome.BAD_ENVELOPE:
        summary.bad_envelope += 1  # the bridge logged it, once, when its reason was new
    else:
        summary.duplicates += 1
    return True


def _source_enabled(settings: ServiceSettings, source: str) -> bool:
    """Whether a source's open receipts may go out: its manifest is found (and is that source's:
    a file whose ``id`` differs is not), cleared for this environment and out of shadow mode,
    the "sandbox test > shadow > enable" flow a pull obeys. A manifest that cannot be read counts
    as not enabled: its receipts wait, and the relay goes on with the other sources.
    """
    try:
        manifest = load_manifest(settings, source)
    except NotFoundError:
        return False
    except ManifestError as exc:
        # The source is a manifest id, and a ManifestError's message is content-free.
        logger.warning("republish: %s: %s; its receipts wait", source, exc)
        return False
    except OSError as exc:
        # The type only: the OS's text quotes the path.
        logger.warning(
            "republish: %s: the manifest could not be read (%s); its receipts wait",
            source,
            type(exc).__name__,
        )
        return False
    return (
        manifest.id == source
        and manifest.cleared_for(settings.environment)
        and not manifest.shadow
    )


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
