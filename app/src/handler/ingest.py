"""Ingest bridge (CN-02): the one path every raw payload takes into the platform.

1. Edge checks only: not empty, under the size cap, and the bytes really are a declared format.
   Anything deeper (schema, codes, patient) is the Transform layer's job. An empty or wrong-format
   payload is rejected for good (the same bytes can never pass); one over the cap is receipted as
   too_large and goes through once NICKEL_MAX_FILE_BYTES is raised. A file a pull skips for size
   before it has the bytes gets a too_large receipt too (``too_large``: metadata only, keyed by a
   surrogate, one per item), which the item's own receipt supersedes once it comes in.
2. The original goes to the File Store under a tenant-first, content-addressed key.
3. A receipt records it, with the envelope as built, then the envelope is published for the
   Transform layer. Under shadow mode (the manifest's ``shadow: true``) the original and the receipt
   are kept, the receipt says the item is held, and nothing is published; once the source is
   enabled, the next run publishes the held items under the ingest ids they were held under.
4. The work counts as done only after the publish succeeded (the acks=all rule). A failed publish is
   recorded on the receipt and raised; re-running the same item retries it under the same ingest id.
   A retry (or a held item promoted later) goes out with the receipt's ``received_at``, not the
   retry's clock, and says which attempt it is and which run made it; everything else is the stored
   envelope unchanged.

Delivery is at-least-once with a stable message id (``src.db.outbox``). The steps are not one
transaction: a crash after the publish and before the receipt update leaves a durable message and a
receipt that still says received (or publish_failed, when the line reached the disk but the publish
raised). The next run of the same item (or the republish relay) publishes it again under the same
ingest id and dedup key, as the next attempt, and the receipt catches up; the consumer drops the
repeat. A crash before the publish leaves the original stored and the receipt pending: the next run
publishes it. The published receipt short-circuits a re-pull to a duplicate before anything is
published: that is transport retry suppression, not dedupe, which stays the next layer's job.

Two pulls of one item at once both miss the receipt lookup; the receipt's unique index decides, and the
pull whose insert loses counts the item as a duplicate under the winner's ingest id (the winner
publishes it, or records why it could not).
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from uuid import UUID

from pydantic import ValidationError

from src.config import DEFAULT_MAX_FILE_BYTES
from src.db.file_store import FileStore
from src.db.outbox import RAW_DATA_TOPIC, Publisher, PublishError
from src.db.receipts import Receipt, ReceiptStatus, ReceiptStore, too_large_key
from src.logging import logger
from src.models.envelope import (
    ENVELOPE_VERSION,
    Envelope,
    ProducedEnvelope,
    idempotency_key_of,
    newer_than_written,
)
from src.utility.ids import uuid7
from src.utility.keys import ObjectClass, object_key, parse_tenant_id

# format -> (content type, sniffer). A PDF header must appear within the first 1024 bytes (ISO 32000-1, 7.5.2).
FORMATS: dict[str, tuple[str, Callable[[bytes], bool]]] = {
    "pdf": ("application/pdf", lambda data: b"%PDF-" in data[:1024]),
    # A vendor API response (the Nango connector): a JSON object or array, nothing else.
    "json": ("application/json", lambda data: _is_json(data)),
}


def _is_json(data: bytes) -> bool:
    try:
        return isinstance(json.loads(data), dict | list)
    except ValueError:
        return False


_PROBLEMS_SHOWN = 5  # a set-aside receipt's reason names at most this many problems


def _envelope_problem(exc: ValidationError) -> str:
    """Why a stored envelope was set aside, content-free: each problem is pydantic's error type,
    after the top-level field it is on (one of the model's own names), never pydantic's message,
    which quotes the input: a stored envelope holds the item's path at the source."""
    problems = sorted(
        {
            f"{error['loc'][0]} {error['type']}" if error["loc"] else error["type"]
            for error in exc.errors()
        }
    )
    shown = ", ".join(problems[:_PROBLEMS_SHOWN])
    if len(problems) > _PROBLEMS_SHOWN:
        shown += f" and {len(problems) - _PROBLEMS_SHOWN} more"
    return f"stored envelope set aside: {shown}"


@dataclass(frozen=True)
class SourceContext:
    """Everything about where a batch of items comes from, and the run that brings it; fixed for
    a connector run. The envelope fields of the same names come from here."""

    source: str
    fetch: str  # how the items are fetched (the manifest's fetch): kaggle, http, local, ...
    route: str  # the lab route (the manifest's route, L1-L9) the fidelity ranking reads
    mode: str  # the manifest's mode: pull, stream, listener, upload or mobile
    connector: str
    connector_version: str
    retention_class: ObjectClass
    formats: tuple[str, ...]
    trace_id: str  # the run's id: every envelope the run publishes carries it
    connection_id: str | None = None
    shadow: bool = False  # store and receipt, publish nothing (the manifest's shadow)


@dataclass(frozen=True)
class RawItem:
    message_id: str
    data: bytes
    source_meta: dict[str, str | int | None] = field(default_factory=dict)
    event_time: datetime | None = None
    source_patient_id: str | None = None


class Outcome(StrEnum):
    PUBLISHED = "published"
    HELD = "held"  # stored and receipted under shadow mode, not published
    DUPLICATE = "duplicate"
    TOO_LARGE = "too_large"  # over the cap for now: through once NICKEL_MAX_FILE_BYTES is raised
    REJECTED = "rejected"  # final: the same bytes can never pass
    # The relay only: the receipt's stored envelope does not validate, or is newer than this
    # service writes, so nothing was published; the receipt says why, and the next pull of its item
    # publishes an envelope it builds again (the stored one stays as it was).
    BAD_ENVELOPE = "bad_envelope"


@dataclass(frozen=True)
class IngestResult:
    outcome: Outcome
    ingest_id: str
    payload_sha256: str
    payload_ref: str | None = None
    new_object: bool = False
    reason: str | None = None


@dataclass(frozen=True)
class _Pending:
    """An item stored and receipted whose envelope is still to be published."""

    receipt: Receipt
    envelope: Envelope
    new_object: bool


def _first_built(existing: Receipt | None, built: ProducedEnvelope) -> Envelope:
    """What a publish sends: the envelope the receipt keeps, first built when the item was first
    received, so that every attempt (a retry, a promotion, the relay) is the same message. The
    receipt never has it replaced (``COALESCE(envelope, ?)``), so ``built`` is what it keeps only
    when it had none. One that no longer reads, or that a newer version of this service wrote
    (which going out again as this version would strip), is not published: this run's ``built``
    is.
    """
    if existing is None or existing.envelope is None:
        return built
    try:
        stored = Envelope.model_validate_json(existing.envelope)
    except ValidationError:
        return built
    return built if newer_than_written(stored.envelope_version) else stored


def _over_cap(size: int | None, cap: int) -> str:
    """A too_large receipt's detail, content-free: the size seen, if any, and the cap."""
    if size is None:
        return f"over the {cap}-byte cap; the download stopped there"
    return f"{size} bytes; the cap is {cap}"


class IngestBridge:
    def __init__(
        self,
        *,
        tenant_id: str | UUID,
        store: FileStore,
        publisher: Publisher,
        receipts: ReceiptStore,
        max_bytes: int = DEFAULT_MAX_FILE_BYTES,
        topic: str = RAW_DATA_TOPIC,
        clock: Callable[[], datetime] | None = None,
        new_id: Callable[[], UUID] = uuid7,
    ) -> None:
        self.tenant_id = parse_tenant_id(tenant_id)
        self.store = store
        self.publisher = publisher
        self.receipts = receipts
        self.max_bytes = max_bytes
        self.topic = topic
        self._clock = clock or (lambda: datetime.now(UTC))
        self._new_id = new_id

    def ingest(self, ctx: SourceContext, item: RawItem) -> IngestResult:
        tenant = str(self.tenant_id)
        sha = hashlib.sha256(item.data).hexdigest()
        received = self._receive(tenant, ctx, item, sha)
        # The item's bytes have a receipt now (this run's, or the one another run wrote first). A
        # too_large receipt an earlier pull left for the item, when it came over the cap and was
        # never fetched whole, has done its job: it is superseded by that receipt.
        self.receipts.supersede_too_large(
            tenant,
            ctx.source,
            item.message_id,
            detail=f"superseded by the receipt of payload {sha}",
        )
        if isinstance(received, IngestResult):
            return received
        return self._publish(
            received.receipt, received.envelope, ctx.trace_id, received.new_object
        )

    def too_large(
        self, ctx: SourceContext, message_id: str, *, size: int | None, cap: int
    ) -> IngestResult:
        """Receipt an item a pull skipped for size without its bytes: listed over the cap, a
        download that passed it, or an SDK file over it on disk. Metadata only: nothing is stored
        or published, the receipt has no payload ref and no envelope, and its detail gives the size
        seen and the cap. It is keyed by ``too_large_key`` (``src.db.receipts``), one per item: a
        repeated pull finds it and adds no row, only bringing its detail up to date. If the item
        came in within the cap meanwhile (the marker is superseded) and is over the cap in force
        again, the marker reopens, unless the size seen is the one the item came in at: most
        likely the same item, already delivered, so nothing is left open for it.
        """
        tenant = str(self.tenant_id)
        key = too_large_key(tenant, ctx.source, message_id)
        detail = _over_cap(size, cap)
        marker = self.receipts.find(tenant, ctx.source, message_id, key)
        if marker is None:
            stamp = self._clock().isoformat()
            marker = Receipt(
                ingest_id=str(self._new_id()),
                tenant_id=tenant,
                source=ctx.source,
                message_id=message_id,
                payload_sha256=key,
                status=ReceiptStatus.TOO_LARGE,
                received_at=stamp,
                updated_at=stamp,
                detail=detail,
            )
            if not self.receipts.add(marker):
                # Another pull of the item wrote it first: that receipt is the item's.
                marker = self.receipts.find(tenant, ctx.source, message_id, key)
        elif (marker.status, marker.detail) != (
            ReceiptStatus.TOO_LARGE,
            detail,
        ) and not self._delivered_at(marker, size):
            self.receipts.mark_too_large(marker, detail=detail)
        return IngestResult(Outcome.TOO_LARGE, marker.ingest_id, key, reason=detail)

    def _delivered_at(self, marker: Receipt, size: int | None) -> bool:
        """Whether a superseded marker's item is seen again at the size it came in at (its
        published receipt's stored envelope says so): then it is not reopened. An unknown size,
        or none published to compare with, reopens it."""
        return (
            marker.status is ReceiptStatus.SUPERSEDED
            and size is not None
            and size
            == self.receipts.published_size(
                marker.tenant_id, marker.source, marker.message_id
            )
        )

    def _receive(
        self, tenant: str, ctx: SourceContext, item: RawItem, sha: str
    ) -> IngestResult | _Pending:
        """Everything up to the publish: the edge checks, the File Store, the receipt. What is
        decided already comes back as a result; an item still to publish, as ``_Pending``.
        """
        existing = self.receipts.find(tenant, ctx.source, item.message_id, sha)
        settled = self._settled(existing, ctx, sha)
        if settled is not None:
            return settled

        now = self._clock()
        stamp = now.isoformat()
        ingest_id = existing.ingest_id if existing is not None else str(self._new_id())
        verdict, reason, content_type = self._edge_check(ctx, item.data)
        if verdict is not None:
            receipt = Receipt(
                ingest_id=ingest_id,
                tenant_id=tenant,
                source=ctx.source,
                message_id=item.message_id,
                payload_sha256=sha,
                status=verdict,
                received_at=stamp,
                updated_at=stamp,
                detail=reason,
            )
            if not self._record(existing, receipt):
                return self._lost_race(tenant, ctx, item, sha)
            outcome = (
                Outcome.TOO_LARGE
                if verdict is ReceiptStatus.TOO_LARGE
                else Outcome.REJECTED
            )
            return IngestResult(outcome, ingest_id, sha, reason=reason)

        key = object_key(self.tenant_id, ctx.retention_class, sha)
        new_object = self.store.put(
            key,
            item.data,
            content_type=content_type,
            # Content facts only: the object is shared by every receipt that names these bytes, so
            # who sent it, under which message id and for how long it is kept stay on the receipts
            # (expiry keeps it for the longest raw_retention among them: issue #6).
            metadata={"sha256": sha},
        )
        status, detail = (
            (ReceiptStatus.HELD, "held for shadow mode")
            if ctx.shadow
            else (ReceiptStatus.RECEIVED, None)
        )
        # An item seen before keeps the time it was first received: a retry or a promotion is not
        # a new arrival. The envelope goes onto the receipt in the same write.
        received_at = (
            datetime.fromisoformat(existing.received_at)
            if existing is not None
            else now
        )
        envelope = self._envelope(
            ctx, item, ingest_id, sha, content_type, key, received_at
        )
        receipt = Receipt(
            ingest_id=ingest_id,
            tenant_id=tenant,
            source=ctx.source,
            message_id=item.message_id,
            payload_sha256=sha,
            status=status,
            received_at=received_at.isoformat(),
            updated_at=stamp,
            payload_ref=key,
            detail=detail,
            envelope=envelope.model_dump_json(),
        )
        if not self._record(existing, receipt):
            return self._lost_race(tenant, ctx, item, sha)
        if ctx.shadow:
            return IngestResult(Outcome.HELD, ingest_id, sha, key, new_object)
        return _Pending(receipt, _first_built(existing, envelope), new_object)

    def _envelope(
        self,
        ctx: SourceContext,
        item: RawItem,
        ingest_id: str,
        sha: str,
        content_type: str,
        key: str,
        received_at: datetime,
    ) -> ProducedEnvelope:
        """The envelope as first built, with this run's id and attempt 1: what the receipt keeps.
        Every publish stamps the attempt it is and the run that makes it (``_publish``). Built
        with the strict producer model: a misspelled field here is an error, not a field dropped.
        The document connector knows no member, device or FHIR release: those stay null.
        """
        return ProducedEnvelope(
            ingest_id=ingest_id,
            tenant_id=self.tenant_id,
            source=ctx.source,
            fetch=ctx.fetch,
            route=ctx.route,
            mode=ctx.mode,
            connector=ctx.connector,
            connector_version=ctx.connector_version,
            connection_id=ctx.connection_id,
            source_patient_id=item.source_patient_id,
            message_id=item.message_id,
            idempotency_key=idempotency_key_of(
                self.tenant_id, ctx.source, item.message_id, sha
            ),
            trace_id=ctx.trace_id,
            attempt=1,
            content_type=content_type,
            received_at=received_at,
            event_time=item.event_time,
            retention_class=ctx.retention_class.value,
            payload_hash=sha,
            payload_bytes=len(item.data),
            payload_ref=key,
            source_meta=item.source_meta,
        )

    @staticmethod
    def _settled(
        existing: Receipt | None, ctx: SourceContext, sha: str
    ) -> IngestResult | None:
        """What an existing receipt decides without a new run: a published item is a duplicate, a
        rejected one stays rejected (final), and a held one stays held while the source is still in
        shadow mode. None when the item has to go through (again)."""
        if existing is None:
            return None
        if existing.status is ReceiptStatus.PUBLISHED:
            return IngestResult(
                Outcome.DUPLICATE, existing.ingest_id, sha, existing.payload_ref
            )
        if existing.status is ReceiptStatus.REJECTED:
            return IngestResult(
                Outcome.REJECTED, existing.ingest_id, sha, reason=existing.detail
            )
        if existing.status is ReceiptStatus.HELD and ctx.shadow:
            return IngestResult(
                Outcome.HELD, existing.ingest_id, sha, existing.payload_ref
            )
        return None

    def _record(self, existing: Receipt | None, receipt: Receipt) -> bool:
        """Write the receipt as this run sees it: a new row, or the open row moved on (its payload
        ref filled in once the original is stored). False when another run got there first.
        """
        if existing is None:
            return self.receipts.add(receipt)
        return self.receipts.set_status(
            receipt.ingest_id,
            receipt.status,
            tenant_id=receipt.tenant_id,
            detail=receipt.detail,
            payload_ref=receipt.payload_ref,
            envelope=receipt.envelope,
        )

    def republish(self, receipt: Receipt, trace_id: str) -> IngestResult:
        """Publish an open receipt's stored envelope again, without its source (the republish
        relay, ``src.handler.documents``): the same ingest id and dedup key, this run's id and the
        next attempt number. Published, or a duplicate if another run got there first; a
        PublishError is recorded on the receipt and raised, as in ``ingest``.

        A stored envelope that does not validate (a corrupt row, or a shape this reader does not
        take) is set aside, not raised: nothing is published, the receipt keeps its status and
        records why (content-free), and the result says BAD_ENVELOPE, so the relay goes on to the
        next receipt instead of stopping at this one on every run.
        """
        try:
            envelope = Envelope.model_validate_json(receipt.envelope)
        except ValidationError as exc:
            return self._set_aside(receipt, _envelope_problem(exc))
        if newer_than_written(envelope.envelope_version):
            # Written by a newer version of this service (before a rollback): going out again as
            # this version would drop the fields this reader does not know. It waits for that
            # version, or for a pull of the item, which builds its own.
            return self._set_aside(
                receipt, f"stored envelope is newer ({envelope.envelope_version})"
            )
        return self._publish(receipt, envelope, trace_id, new_object=False)

    def _set_aside(self, receipt: Receipt, reason: str) -> IngestResult:
        """The receipt's stored envelope is not published: its detail says why (a detail-only
        write, while the receipt is still in the status it was read in), logged once, when the
        reason is new. A run that meets the same bad envelope again writes and logs nothing.
        """
        if self.receipts.set_aside(receipt, detail=reason):
            logger.warning("republish: receipt %s: %s", receipt.ingest_id, reason)
        return IngestResult(
            Outcome.BAD_ENVELOPE,
            receipt.ingest_id,
            receipt.payload_sha256,
            receipt.payload_ref,
            reason=reason,
        )

    def _publish(
        self, receipt: Receipt, envelope: Envelope, trace_id: str, new_object: bool
    ) -> IngestResult:
        """Publish the receipt's envelope, stamped with the attempt this is and the run making it,
        and record the outcome on the receipt."""
        ingest_id, tenant, sha, key = (
            receipt.ingest_id,
            receipt.tenant_id,
            receipt.payload_sha256,
            receipt.payload_ref,
        )
        attempt = self.receipts.begin_attempt(ingest_id, tenant_id=tenant)
        if attempt is None:
            # Another run retried the same open receipt and published it first.
            return IngestResult(Outcome.DUPLICATE, ingest_id, sha, key)
        # The current version goes out: this one, or a stored v0.2 one, read through the reader's
        # alias, which is the same values in v0.3's shape (a newer one never gets here).
        stamped = envelope.model_copy(
            update={
                "envelope_version": ENVELOPE_VERSION,
                "attempt": attempt,
                "trace_id": trace_id,
            }
        )
        # Kafka key: the patient, so one patient's messages stay in order. A document with no patient yet
        # (a public dataset, an unassigned upload) has no ordering need, so its hash spreads the load.
        partition_key = stamped.source_patient_id or sha
        try:
            self.publisher.publish(
                self.topic,
                partition_key,
                stamped.model_dump(mode="json"),
                message_id=ingest_id,
            )
        except PublishError as exc:
            self.receipts.set_status(
                ingest_id,
                ReceiptStatus.PUBLISH_FAILED,
                tenant_id=tenant,
                detail=str(exc),
            )
            raise
        # This run's message is durable: the item is published, for this run, whatever the update
        # below says. It says False when another run retried the same open receipt and recorded
        # its own publish first: both appended (the id is on raw_data twice, at-least-once, and the
        # consumer drops the repeat), and each run counts the message it made durable.
        self.receipts.set_status(ingest_id, ReceiptStatus.PUBLISHED, tenant_id=tenant)
        return IngestResult(Outcome.PUBLISHED, ingest_id, sha, key, new_object)

    def _lost_race(
        self, tenant: str, ctx: SourceContext, item: RawItem, sha: str
    ) -> IngestResult:
        """Another pull receipted this item between this pull's ``find`` and its ``add``. That pull
        owns the item (it publishes it, or records why not); for this one it is a duplicate.
        """
        other = self.receipts.find(tenant, ctx.source, item.message_id, sha)
        return IngestResult(Outcome.DUPLICATE, other.ingest_id, sha, other.payload_ref)

    def _edge_check(
        self, ctx: SourceContext, data: bytes
    ) -> tuple[ReceiptStatus | None, str | None, str]:
        """(the receipt status a failed check gives, or None; its reason; the content type).

        ``rejected`` is final: empty bytes, or bytes that are not a declared format, never pass.
        ``too_large`` is not: the same bytes pass once the cap is raised, so the receipt keeps them
        open for the next run.
        """
        if not data:
            return ReceiptStatus.REJECTED, "empty payload", ""
        if len(data) > self.max_bytes:
            reason = f"payload is {len(data)} bytes; the cap is {self.max_bytes}"
            return ReceiptStatus.TOO_LARGE, reason, ""
        for fmt in ctx.formats:
            content_type, sniff = FORMATS[fmt]
            if sniff(data):
                return None, None, content_type
        reason = f"content does not match the declared formats {list(ctx.formats)}"
        return ReceiptStatus.REJECTED, reason, ""
