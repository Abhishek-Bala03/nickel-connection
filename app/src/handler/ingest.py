"""Ingest bridge (CN-02): the one path every raw payload takes into the platform.

1. Edge checks only: not empty, under the size cap, and the bytes really are a declared format.
   Anything deeper (schema, codes, patient) is the Transform layer's job. An empty or wrong-format
   payload is rejected for good (the same bytes can never pass); one over the cap is receipted as
   too_large and goes through once NICKEL_MAX_FILE_BYTES is raised.
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
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from uuid import UUID

from src.config import DEFAULT_MAX_FILE_BYTES
from src.db.file_store import FileStore
from src.db.outbox import RAW_DATA_TOPIC, Publisher, PublishError
from src.db.receipts import Receipt, ReceiptStatus, ReceiptStore
from src.models.envelope import Envelope, idempotency_key_of
from src.utility.ids import uuid7
from src.utility.keys import ObjectClass, object_key, parse_tenant_id

# format -> (content type, sniffer). A PDF header must appear within the first 1024 bytes (ISO 32000-1, 7.5.2).
FORMATS: dict[str, tuple[str, Callable[[bytes], bool]]] = {
    "pdf": ("application/pdf", lambda data: b"%PDF-" in data[:1024]),
}


@dataclass(frozen=True)
class SourceContext:
    """Everything about where a batch of items comes from, and the run that brings it; fixed for
    a connector run. The envelope fields of the same names come from here."""

    source: str
    fetch: str  # how the items are fetched (the manifest's fetch): kaggle, http, local, ...
    route: str  # the lab route (the manifest's route, L1-L9) the fidelity ranking reads
    mode: str  # the manifest's mode: pull, stream or push
    connector: str
    connector_version: str
    retention_class: ObjectClass
    formats: tuple[str, ...]
    trace_id: str  # the run's id: every envelope the run publishes carries it
    connection_id: str | None = None
    shadow: bool = False  # store and receipt, publish nothing (the manifest's shadow)
    raw_retention: str = "forever"  # the manifest's raw_retention, kept on the object


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


@dataclass(frozen=True)
class IngestResult:
    outcome: Outcome
    ingest_id: str
    payload_sha256: str
    payload_ref: str | None = None
    new_object: bool = False
    reason: str | None = None


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
            metadata={
                "sha256": sha,
                "source": ctx.source,
                "message_id": item.message_id,
                "raw_retention": ctx.raw_retention,
            },
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
        return self._publish(receipt, envelope, ctx.trace_id, new_object)

    def _envelope(
        self,
        ctx: SourceContext,
        item: RawItem,
        ingest_id: str,
        sha: str,
        content_type: str,
        key: str,
        received_at: datetime,
    ) -> Envelope:
        """The envelope as first built, with this run's id and attempt 1: what the receipt keeps.
        Every publish stamps the attempt it is and the run that makes it (``_publish``).
        """
        return Envelope(
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
            payload_sha256=sha,
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
            detail=receipt.detail,
            payload_ref=receipt.payload_ref,
            envelope=receipt.envelope,
        )

    def republish(self, receipt: Receipt, trace_id: str) -> IngestResult:
        """Publish an open receipt's stored envelope again, without its source (the republish
        relay, ``src.handler.documents``): the same ingest id and dedup key, this run's id and the
        next attempt number. Published, or a duplicate if another run got there first; a
        PublishError is recorded on the receipt and raised, as in ``ingest``.
        """
        envelope = Envelope.model_validate_json(receipt.envelope)
        return self._publish(receipt, envelope, trace_id, new_object=False)

    def _publish(
        self, receipt: Receipt, envelope: Envelope, trace_id: str, new_object: bool
    ) -> IngestResult:
        """Publish the receipt's envelope, stamped with the attempt this is and the run making it,
        and record the outcome on the receipt."""
        ingest_id, sha, key = (
            receipt.ingest_id,
            receipt.payload_sha256,
            receipt.payload_ref,
        )
        attempt = self.receipts.begin_attempt(ingest_id)
        if attempt is None:
            # Another run retried the same open receipt and published it first.
            return IngestResult(Outcome.DUPLICATE, ingest_id, sha, key)
        stamped = envelope.model_copy(update={"attempt": attempt, "trace_id": trace_id})
        # Kafka key: the patient, so one patient's messages stay in order. A document with no patient yet
        # (a public dataset, an unassigned upload) has no ordering need, so its hash spreads the load.
        partition_key = stamped.source_patient_id or sha
        try:
            self.publisher.publish(
                self.topic,
                partition_key,
                stamped.model_dump(mode="json"),
                idempotency_key=ingest_id,
            )
        except PublishError as exc:
            self.receipts.set_status(
                ingest_id, ReceiptStatus.PUBLISH_FAILED, detail=str(exc)
            )
            raise
        if not self.receipts.set_status(ingest_id, ReceiptStatus.PUBLISHED):
            # Another run retried the same pending receipt and finished first (the outbox appended
            # the message once): for this run the item is a duplicate.
            return IngestResult(Outcome.DUPLICATE, ingest_id, sha, key)
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
