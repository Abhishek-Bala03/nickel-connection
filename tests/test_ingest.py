import dataclasses
import hashlib
import json
import threading
import uuid
from datetime import UTC, datetime

import pytest

from src.db.file_store import LocalFileStore
from src.db.outbox import JsonlOutbox, PublishError
from src.db.receipts import ReceiptStatus, ReceiptStore
from src.handler.ingest import IngestBridge, Outcome, RawItem
from src.models.envelope import Envelope, idempotency_key_of

TENANT = "0192a3b4-0000-7000-8000-000000000001"


class FlakyPublisher:
    """Fails the first ``failures`` publishes, then records messages."""

    def __init__(self, failures=1):
        self.failures = failures
        self.messages = []

    def publish(self, topic, key, value, **kw):
        if self.failures:
            self.failures -= 1
            raise PublishError("broker unavailable")
        self.messages.append((topic, key, value))


def _item(data, message_id="local:docs/a.pdf", **kw):
    return RawItem(
        message_id=message_id, data=data, source_meta={"path": "a.pdf"}, **kw
    )


def test_a_new_pdf_is_stored_received_and_published(bridge, ctx, pdf_bytes):
    result = bridge.ingest(ctx, _item(pdf_bytes))
    sha = hashlib.sha256(pdf_bytes).hexdigest()
    assert result.outcome is Outcome.PUBLISHED
    assert result.new_object
    assert result.payload_ref == f"{TENANT}/documents/{sha[:2]}/{sha}"
    assert bridge.store.get(result.payload_ref) == pdf_bytes
    [message] = bridge.publisher.read("raw_data")
    envelope = message["value"]
    assert message["key"] == sha  # no patient yet, so the hash spreads the load
    assert envelope["ingest_id"] == result.ingest_id
    assert envelope["tenant_id"] == TENANT
    assert envelope["payload_ref"] == result.payload_ref
    assert envelope["content_type"] == "application/pdf"
    assert envelope["source_meta"] == {"path": "a.pdf"}
    assert (
        bridge.receipts.get(result.ingest_id, tenant_id=TENANT).status
        is ReceiptStatus.PUBLISHED
    )


def test_the_envelope_names_the_run_the_fetch_the_route_and_the_dedup_key(
    bridge, ctx, pdf_bytes
):
    result = bridge.ingest(ctx, _item(pdf_bytes))
    sha = hashlib.sha256(pdf_bytes).hexdigest()
    [message] = bridge.publisher.read("raw_data")
    envelope = message["value"]
    assert envelope["envelope_version"] == "0.3"
    assert (envelope["fetch"], envelope["route"], envelope["mode"]) == (
        "local",
        "L7",
        "pull",
    )
    assert (envelope["trace_id"], envelope["attempt"]) == (ctx.trace_id, 1)
    assert envelope["idempotency_key"] == idempotency_key_of(
        TENANT, "test-docs", "local:docs/a.pdf", sha
    )
    assert envelope["quality"] == {"flags": []}
    assert (envelope["schema_id"], envelope["consent_ref"]) == (None, None)
    # Always null from this connector: no Nango connection, no patient id, no event time, and
    # no member, device or FHIR release.
    assert (
        envelope["connection_id"],
        envelope["source_patient_id"],
        envelope["event_time"],
        envelope["member_id"],
        envelope["device_id"],
        envelope["fhir_release"],
    ) == (None, None, None, None, None, None)
    assert envelope["payload_hash"] == sha
    assert "payload_sha256" not in envelope
    assert bridge.receipts.get(result.ingest_id, tenant_id=TENANT).attempts == 1


def test_a_retry_counts_the_attempt_on_the_receipt_and_in_the_envelope(
    tmp_path, receipts, ctx, pdf_bytes
):
    publisher = FlakyPublisher(failures=1)
    bridge = IngestBridge(
        tenant_id=TENANT,
        store=LocalFileStore(tmp_path / "fs"),
        publisher=publisher,
        receipts=receipts,
    )
    with pytest.raises(PublishError):
        bridge.ingest(ctx, _item(pdf_bytes))
    sha = hashlib.sha256(pdf_bytes).hexdigest()
    failed = receipts.find(TENANT, ctx.source, "local:docs/a.pdf", sha)
    # Counted before the attempt is made, so a crash mid-publish counts too.
    assert failed.attempts == 1
    other_run = dataclasses.replace(ctx, trace_id="0192a3b40000700080000000000000cd")
    retried = bridge.ingest(other_run, _item(pdf_bytes))
    assert retried.outcome is Outcome.PUBLISHED
    [(_topic, _key, envelope)] = publisher.messages
    # The retry says which attempt it is and which run made it; the dedup key is the same.
    assert (envelope["attempt"], envelope["trace_id"]) == (2, other_run.trace_id)
    assert envelope["idempotency_key"] == idempotency_key_of(
        TENANT, ctx.source, "local:docs/a.pdf", sha
    )
    assert receipts.get(retried.ingest_id, tenant_id=TENANT).attempts == 2


def test_the_envelope_is_kept_on_the_receipt_and_a_retry_keeps_received_at(
    tmp_path, receipts, ctx, pdf_bytes
):
    """The receipt stores the envelope in the write that records the item, so a retry can publish
    without the source; and the retry goes out with the time the item was received, not its own.
    """
    first_time = datetime(2026, 9, 22, 12, 0, tzinfo=UTC)
    clock = [first_time]
    publisher = FlakyPublisher(failures=1)
    bridge = IngestBridge(
        tenant_id=TENANT,
        store=LocalFileStore(tmp_path / "fs"),
        publisher=publisher,
        receipts=receipts,
        clock=lambda: clock[0],
    )
    with pytest.raises(PublishError):
        bridge.ingest(ctx, _item(pdf_bytes))
    [failed] = receipts.recent(tenant_id=TENANT)
    stored = Envelope.model_validate_json(failed.envelope)
    assert (str(stored.ingest_id), stored.received_at) == (failed.ingest_id, first_time)
    assert (stored.attempt, stored.trace_id) == (1, ctx.trace_id)  # as first built

    clock[0] = datetime(2026, 9, 23, 8, 0, tzinfo=UTC)  # the retry, the next morning
    retried = bridge.ingest(ctx, _item(pdf_bytes))
    [(_topic, _key, envelope)] = publisher.messages
    assert envelope["received_at"] == "2026-09-22T12:00:00Z"
    assert envelope["attempt"] == 2
    assert (
        receipts.get(retried.ingest_id, tenant_id=TENANT).received_at
        == first_time.isoformat()
    )


def test_republish_sends_the_stored_envelope_under_the_next_attempt(
    tmp_path, receipts, ctx, pdf_bytes
):
    class CrashOnce:
        def __init__(self, inner):
            self.inner, self.crashed = inner, False

        def publish(self, topic, key, value, **kw):
            if not self.crashed:
                self.crashed = True
                raise RuntimeError("power cut")
            self.inner.publish(topic, key, value, **kw)

    outbox = JsonlOutbox(tmp_path / "out")
    bridge = IngestBridge(
        tenant_id=TENANT,
        store=LocalFileStore(tmp_path / "fs"),
        publisher=CrashOnce(outbox),
        receipts=receipts,
    )
    with pytest.raises(RuntimeError):
        bridge.ingest(ctx, _item(pdf_bytes))
    [pending] = receipts.recent(tenant_id=TENANT)
    assert (pending.status, pending.attempts) == (ReceiptStatus.RECEIVED, 1)

    result = bridge.republish(pending, "0192a3b40000700080000000000000ee")
    assert (result.outcome, result.ingest_id, result.new_object) == (
        Outcome.PUBLISHED,
        pending.ingest_id,
        False,
    )
    [message] = outbox.read("raw_data")
    envelope = message["value"]
    stored = Envelope.model_validate_json(pending.envelope)
    assert (envelope["attempt"], envelope["trace_id"]) == (
        2,
        "0192a3b40000700080000000000000ee",
    )
    # Everything else is the stored envelope, unchanged.
    assert (
        Envelope.model_validate(envelope).model_copy(
            update={"attempt": 1, "trace_id": ctx.trace_id}
        )
        == stored
    )
    assert (
        receipts.get(pending.ingest_id, tenant_id=TENANT).status
        is ReceiptStatus.PUBLISHED
    )


def test_a_republish_that_fails_is_recorded_and_raised(
    tmp_path, receipts, ctx, pdf_bytes
):
    bridge = IngestBridge(
        tenant_id=TENANT,
        store=LocalFileStore(tmp_path / "fs"),
        publisher=FlakyPublisher(failures=2),
        receipts=receipts,
    )
    with pytest.raises(PublishError):
        bridge.ingest(ctx, _item(pdf_bytes))
    [failed] = receipts.recent(tenant_id=TENANT)
    with pytest.raises(PublishError, match="broker unavailable"):
        bridge.republish(failed, ctx.trace_id)
    again = receipts.get(failed.ingest_id, tenant_id=TENANT)
    assert (again.status, again.attempts) == (ReceiptStatus.PUBLISH_FAILED, 2)
    assert bridge.republish(again, ctx.trace_id).outcome is Outcome.PUBLISHED


def test_a_held_item_promoted_later_keeps_the_time_it_was_received(
    tmp_path, receipts, ctx, pdf_bytes
):
    clock = [datetime(2026, 9, 22, 12, 0, tzinfo=UTC)]
    outbox = JsonlOutbox(tmp_path / "out")
    bridge = IngestBridge(
        tenant_id=TENANT,
        store=LocalFileStore(tmp_path / "fs"),
        publisher=outbox,
        receipts=receipts,
        clock=lambda: clock[0],
    )
    held = bridge.ingest(dataclasses.replace(ctx, shadow=True), _item(pdf_bytes))
    stored = Envelope.model_validate_json(
        receipts.get(held.ingest_id, tenant_id=TENANT).envelope
    )
    # Kept while held: publishable later without the source.
    assert stored.received_at == clock[0]
    clock[0] = datetime(2026, 9, 30, 9, 0, tzinfo=UTC)  # enabled a week later
    assert bridge.ingest(ctx, _item(pdf_bytes)).outcome is Outcome.PUBLISHED
    [message] = outbox.read("raw_data")
    assert message["value"]["received_at"] == "2026-09-22T12:00:00Z"


def test_the_same_item_twice_is_one_object_one_message_one_receipt(
    bridge, ctx, pdf_bytes
):
    first = bridge.ingest(ctx, _item(pdf_bytes))
    second = bridge.ingest(ctx, _item(pdf_bytes))
    assert second.outcome is Outcome.DUPLICATE
    assert second.ingest_id == first.ingest_id
    assert len(bridge.publisher.read("raw_data")) == 1
    assert bridge.receipts.counts(tenant_id=TENANT) == {"published": 1}


def test_identical_bytes_under_two_message_ids_share_one_object(bridge, ctx, pdf_bytes):
    first = bridge.ingest(ctx, _item(pdf_bytes, "local:docs/a.pdf"))
    second = bridge.ingest(ctx, _item(pdf_bytes, "local:docs/copy-of-a.pdf"))
    assert second.outcome is Outcome.PUBLISHED
    assert not second.new_object
    assert second.payload_ref == first.payload_ref
    assert len(bridge.publisher.read("raw_data")) == 2


def test_the_patient_is_the_partition_key_when_known(bridge, ctx, pdf_bytes):
    bridge.ingest(ctx, _item(pdf_bytes, source_patient_id="patient-1"))
    [message] = bridge.publisher.read("raw_data")
    assert message["key"] == "patient-1"
    assert message["value"]["source_patient_id"] == "patient-1"


@pytest.mark.parametrize(
    "data, reason",
    [(b"", "empty payload"), (b"hello, not a pdf", "declared formats")],
)
def test_rejects_are_receipted_and_never_stored_or_published(bridge, ctx, data, reason):
    result = bridge.ingest(ctx, _item(data))
    assert result.outcome is Outcome.REJECTED
    assert reason in result.reason
    assert bridge.publisher.read("raw_data") == []
    rejected = bridge.receipts.get(result.ingest_id, tenant_id=TENANT)
    assert (rejected.status, rejected.envelope) == (ReceiptStatus.REJECTED, None)
    again = bridge.ingest(ctx, _item(data))
    assert (again.outcome, again.ingest_id) == (Outcome.REJECTED, result.ingest_id)
    assert bridge.receipts.counts(tenant_id=TENANT) == {"rejected": 1}


def test_payloads_over_the_cap_are_too_large_not_rejected(
    tmp_path, receipts, ctx, pdf_bytes
):
    small = IngestBridge(
        tenant_id=TENANT,
        store=LocalFileStore(tmp_path / "fs"),
        publisher=JsonlOutbox(tmp_path / "out"),
        receipts=receipts,
        max_bytes=10,
    )
    result = small.ingest(ctx, _item(pdf_bytes))
    assert (
        result.outcome is Outcome.TOO_LARGE
    )  # not final: a raised cap lets it through
    assert "cap is 10" in result.reason


def test_a_failed_publish_is_recorded_then_retried_under_the_same_id(
    tmp_path, receipts, ctx, pdf_bytes
):
    publisher = FlakyPublisher(failures=1)
    bridge = IngestBridge(
        tenant_id=TENANT,
        store=LocalFileStore(tmp_path / "fs"),
        publisher=publisher,
        receipts=receipts,
    )
    item = _item(pdf_bytes)
    with pytest.raises(PublishError):
        bridge.ingest(ctx, item)
    sha = hashlib.sha256(pdf_bytes).hexdigest()
    failed = receipts.find(TENANT, ctx.source, "local:docs/a.pdf", sha)
    assert failed.status is ReceiptStatus.PUBLISH_FAILED
    assert "broker unavailable" in failed.detail

    retried = bridge.ingest(ctx, _item(pdf_bytes))
    assert retried.outcome is Outcome.PUBLISHED
    assert retried.ingest_id == failed.ingest_id
    assert not retried.new_object  # the original was already stored on the first try
    assert len(publisher.messages) == 1
    assert receipts.counts(tenant_id=TENANT) == {"published": 1}


def test_a_pending_item_that_now_fails_the_edge_check_moves_to_that_status(
    tmp_path, receipts, ctx, pdf_bytes
):
    kwargs = {
        "tenant_id": TENANT,
        "store": LocalFileStore(tmp_path / "fs"),
        "receipts": receipts,
    }
    failing = IngestBridge(publisher=FlakyPublisher(), **kwargs)
    item = _item(pdf_bytes)
    with pytest.raises(PublishError):
        failing.ingest(ctx, item)
    stricter = IngestBridge(publisher=FlakyPublisher(failures=0), max_bytes=5, **kwargs)
    result = stricter.ingest(ctx, _item(pdf_bytes))
    assert result.outcome is Outcome.TOO_LARGE
    assert (
        receipts.get(result.ingest_id, tenant_id=TENANT).status
        is ReceiptStatus.TOO_LARGE
    )


def test_clock_and_ids_can_be_pinned(tmp_path, receipts, ctx, pdf_bytes):
    fixed_id = uuid.UUID("0192a3b4-0000-7000-8000-0000000000ff")
    fixed_time = datetime(2026, 9, 22, 12, 0, tzinfo=UTC)
    publisher = FlakyPublisher(failures=0)
    bridge = IngestBridge(
        tenant_id=TENANT,
        store=LocalFileStore(tmp_path / "fs"),
        publisher=publisher,
        receipts=receipts,
        clock=lambda: fixed_time,
        new_id=lambda: fixed_id,
    )
    result = bridge.ingest(ctx, _item(pdf_bytes))
    [(topic, _key, envelope)] = publisher.messages
    assert topic == "raw_data"
    assert result.ingest_id == envelope["ingest_id"] == str(fixed_id)
    assert envelope["received_at"] == "2026-09-22T12:00:00Z"


class Interleaving:
    """A receipt store whose first ``add`` lets ``rival`` run to completion first: the moment between one
    pull's ``find`` and its ``add`` in which another pull of the same item slips in whole.
    """

    def __init__(self, inner, rival):
        self._inner = inner
        self._rival = rival

    def __getattr__(self, name):
        return getattr(self._inner, name)

    def add(self, receipt):
        if self._rival is not None:
            rival, self._rival = self._rival, None
            rival()
        return self._inner.add(receipt)


def test_two_pulls_of_one_item_at_once_make_one_receipt_and_one_message(
    tmp_path, receipts, ctx, pdf_bytes
):
    store = LocalFileStore(tmp_path / "fs")
    outbox = JsonlOutbox(tmp_path / "out")
    first = IngestBridge(
        tenant_id=TENANT, store=store, publisher=outbox, receipts=receipts
    )
    won = []
    second = IngestBridge(
        tenant_id=TENANT,
        store=store,
        publisher=outbox,
        receipts=Interleaving(
            receipts, lambda: won.append(first.ingest(ctx, _item(pdf_bytes)))
        ),
    )
    # Both pulls miss ``find``; the first one's receipt lands while the second is between its
    # ``find`` and its ``add``. The second must not die on the unique index: it lost, so the item
    # is a duplicate for it, under the first pull's ingest id.
    result = second.ingest(ctx, _item(pdf_bytes))
    [winner] = won
    assert winner.outcome is Outcome.PUBLISHED
    assert (result.outcome, result.ingest_id, result.payload_ref) == (
        Outcome.DUPLICATE,
        winner.ingest_id,
        winner.payload_ref,
    )
    assert len(outbox.read("raw_data")) == 1
    assert receipts.counts(tenant_id=TENANT) == {"published": 1}


def test_two_pulls_rejecting_one_item_at_once_share_its_receipt(
    tmp_path, receipts, ctx
):
    store = LocalFileStore(tmp_path / "fs")
    outbox = JsonlOutbox(tmp_path / "out")
    first = IngestBridge(
        tenant_id=TENANT, store=store, publisher=outbox, receipts=receipts
    )
    won = []
    second = IngestBridge(
        tenant_id=TENANT,
        store=store,
        publisher=outbox,
        receipts=Interleaving(
            receipts, lambda: won.append(first.ingest(ctx, _item(b"not a pdf")))
        ),
    )
    result = second.ingest(ctx, _item(b"not a pdf"))
    [winner] = won
    assert winner.outcome is Outcome.REJECTED
    assert (result.outcome, result.ingest_id) == (Outcome.DUPLICATE, winner.ingest_id)
    assert receipts.counts(tenant_id=TENANT) == {"rejected": 1}


class CrashingReceipts:
    """A receipt store that crashes (power cut, not an error the bridge handles) on the first status
    update to ``at``: the message is out, the receipt was never told."""

    def __init__(self, inner, at):
        self._inner = inner
        self._at = at

    def __getattr__(self, name):
        return getattr(self._inner, name)

    def set_status(self, ingest_id, status, **kw):
        if status is self._at:
            self._at = None
            raise RuntimeError("power cut")
        return self._inner.set_status(ingest_id, status, **kw)


def test_a_crash_after_the_publish_is_repaired_by_a_repeat_that_says_so(
    tmp_path, receipts, ctx, pdf_bytes
):
    outbox = JsonlOutbox(tmp_path / "out")
    bridge = IngestBridge(
        tenant_id=TENANT,
        store=LocalFileStore(tmp_path / "fs"),
        publisher=outbox,
        receipts=CrashingReceipts(receipts, at=ReceiptStatus.PUBLISHED),
    )
    with pytest.raises(RuntimeError, match="power cut"):
        bridge.ingest(ctx, _item(pdf_bytes))
    sha = hashlib.sha256(pdf_bytes).hexdigest()
    pending = receipts.find(TENANT, ctx.source, "local:docs/a.pdf", sha)
    # The message is out, the receipt lags.
    assert pending.status is ReceiptStatus.RECEIVED
    assert len(outbox.read("raw_data")) == 1

    # The next run repairs the receipt and publishes again (at-least-once): the repeat carries the
    # same id and dedup key and says it is attempt 2, so the consumer drops it.
    again = bridge.ingest(ctx, _item(pdf_bytes))
    assert (again.outcome, again.ingest_id) == (Outcome.PUBLISHED, pending.ingest_id)
    first, repeat = outbox.read("raw_data")
    assert first["id"] == repeat["id"] == pending.ingest_id
    assert first["value"]["idempotency_key"] == repeat["value"]["idempotency_key"]
    assert (first["value"]["attempt"], repeat["value"]["attempt"]) == (1, 2)
    assert receipts.counts(tenant_id=TENANT) == {"published": 1}


def test_a_torn_publish_is_repeated_under_the_same_id(
    tmp_path, receipts, ctx, pdf_bytes
):
    class TornOutbox(JsonlOutbox):
        """The line reaches the disk, then the publish fails (say, the fsync): once."""

        torn = True

        def publish(self, topic, key, value, **kw):
            super().publish(topic, key, value, **kw)
            if self.torn:
                self.torn = False
                raise PublishError("fsync failed")

    outbox = TornOutbox(tmp_path / "out")
    bridge = IngestBridge(
        tenant_id=TENANT,
        store=LocalFileStore(tmp_path / "fs"),
        publisher=outbox,
        receipts=receipts,
    )
    with pytest.raises(PublishError):
        bridge.ingest(ctx, _item(pdf_bytes))
    sha = hashlib.sha256(pdf_bytes).hexdigest()
    failed = receipts.find(TENANT, ctx.source, "local:docs/a.pdf", sha)
    assert failed.status is ReceiptStatus.PUBLISH_FAILED
    assert bridge.ingest(ctx, _item(pdf_bytes)).outcome is Outcome.PUBLISHED
    assert [m["id"] for m in outbox.read("raw_data")] == [failed.ingest_id] * 2
    assert receipts.counts(tenant_id=TENANT) == {"published": 1}


def test_two_threads_retrying_one_open_receipt_both_append_and_both_count_it_published(
    tmp_path, ctx, pdf_bytes
):
    """Two runs retry the same open receipt at once, each on its own receipts connection and its
    own outbox instance (as two jobs or two processes would). The barrier sits after each run has
    counted its attempt, so both are past the point where one could see the other's publish: the
    overlap is forced, not left to timing. Both append (the id is on raw_data twice, attempts 2
    and 3: at-least-once, never lost), the receipt ends published, and each run counts the message
    it made durable as published, not as a duplicate it never sent.
    """

    class CrashOnce:
        def __init__(self, inner):
            self.inner, self.crashed = inner, False

        def publish(self, topic, key, value, **kw):
            if not self.crashed:
                self.crashed = True
                raise RuntimeError("power cut")
            self.inner.publish(topic, key, value, **kw)

    store = LocalFileStore(tmp_path / "fs")
    db = tmp_path / "receipts.sqlite"
    with ReceiptStore(db) as receipts:
        first = IngestBridge(
            tenant_id=TENANT,
            store=store,
            publisher=CrashOnce(JsonlOutbox(tmp_path / "out")),
            receipts=receipts,
        )
        with pytest.raises(RuntimeError):
            first.ingest(ctx, _item(pdf_bytes))
        [pending] = receipts.recent(tenant_id=TENANT)
    assert (pending.status, pending.attempts) == (ReceiptStatus.RECEIVED, 1)

    barrier = threading.Barrier(2, timeout=10)

    class AfterTheAttempt:
        """Each run waits for the other right after counting its attempt."""

        def __init__(self, inner):
            self._inner = inner

        def __getattr__(self, name):
            return getattr(self._inner, name)

        def begin_attempt(self, ingest_id, **kw):
            attempt = self._inner.begin_attempt(ingest_id, **kw)
            barrier.wait()
            return attempt

    outcomes = []

    def retry():
        with ReceiptStore(db) as own:  # a SQLite connection belongs to one thread
            bridge = IngestBridge(
                tenant_id=TENANT,
                store=store,
                publisher=JsonlOutbox(tmp_path / "out"),
                receipts=AfterTheAttempt(own),
            )
            outcomes.append(bridge.ingest(ctx, _item(pdf_bytes)).outcome)

    threads = [threading.Thread(target=retry) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(20)
    assert outcomes == [Outcome.PUBLISHED, Outcome.PUBLISHED]
    with ReceiptStore(db) as receipts:
        final = receipts.get(pending.ingest_id, tenant_id=TENANT)
    assert (final.status, final.attempts) == (ReceiptStatus.PUBLISHED, 3)
    records = JsonlOutbox(tmp_path / "out").read("raw_data")
    assert [record["id"] for record in records] == [pending.ingest_id] * 2
    assert sorted(record["value"]["attempt"] for record in records) == [2, 3]


def test_a_crash_before_the_publish_loses_nothing(tmp_path, receipts, ctx, pdf_bytes):
    class CrashOnce:
        def __init__(self, inner):
            self.inner, self.crashed = inner, False

        def publish(self, topic, key, value, **kw):
            if not self.crashed:
                self.crashed = True
                raise RuntimeError("power cut")
            self.inner.publish(topic, key, value, **kw)

    outbox = JsonlOutbox(tmp_path / "out")
    bridge = IngestBridge(
        tenant_id=TENANT,
        store=LocalFileStore(tmp_path / "fs"),
        publisher=CrashOnce(outbox),
        receipts=receipts,
    )
    with pytest.raises(RuntimeError):
        bridge.ingest(ctx, _item(pdf_bytes))
    assert outbox.read("raw_data") == []
    assert receipts.counts(tenant_id=TENANT) == {
        "received": 1
    }  # the original is stored, the receipt pending
    again = bridge.ingest(ctx, _item(pdf_bytes))
    assert (again.outcome, again.new_object) == (Outcome.PUBLISHED, False)
    assert len(outbox.read("raw_data")) == 1
    assert receipts.counts(tenant_id=TENANT) == {"published": 1}


def test_a_size_reject_is_retried_once_the_cap_is_raised(
    tmp_path, receipts, ctx, pdf_bytes
):
    kwargs = {
        "tenant_id": TENANT,
        "store": LocalFileStore(tmp_path / "fs"),
        "publisher": JsonlOutbox(tmp_path / "out"),
        "receipts": receipts,
    }
    small = IngestBridge(max_bytes=10, **kwargs)
    result = small.ingest(ctx, _item(pdf_bytes))
    assert (result.outcome, result.payload_ref) == (Outcome.TOO_LARGE, None)
    too_large = receipts.get(result.ingest_id, tenant_id=TENANT)
    # Nothing stored, so nothing to publish later: no object, no envelope on the receipt.
    assert (too_large.status, too_large.envelope) == (ReceiptStatus.TOO_LARGE, None)
    assert small.ingest(ctx, _item(pdf_bytes)).outcome is Outcome.TOO_LARGE
    assert receipts.counts(tenant_id=TENANT) == {"too_large": 1}

    # NICKEL_MAX_FILE_BYTES raised: the same item goes through under the same ingest id.
    roomy = IngestBridge(**kwargs)
    retried = roomy.ingest(ctx, _item(pdf_bytes))
    assert (retried.outcome, retried.ingest_id) == (Outcome.PUBLISHED, result.ingest_id)
    assert retried.new_object
    published = receipts.get(result.ingest_id, tenant_id=TENANT)
    assert published.payload_ref == retried.payload_ref
    assert (
        Envelope.model_validate_json(published.envelope).payload_ref
        == retried.payload_ref
    )
    assert receipts.counts(tenant_id=TENANT) == {"published": 1}


def test_a_format_reject_is_final(tmp_path, receipts, ctx):
    kwargs = {
        "tenant_id": TENANT,
        "store": LocalFileStore(tmp_path / "fs"),
        "publisher": JsonlOutbox(tmp_path / "out"),
        "receipts": receipts,
    }
    first = IngestBridge(max_bytes=10, **kwargs).ingest(ctx, _item(b"no pdf"))
    assert first.outcome is Outcome.REJECTED
    again = IngestBridge(**kwargs).ingest(ctx, _item(b"no pdf"))
    assert (again.outcome, again.ingest_id) == (Outcome.REJECTED, first.ingest_id)
    assert receipts.counts(tenant_id=TENANT) == {"rejected": 1}


def test_a_run_whose_status_update_loses_after_its_append_counts_it_published(
    tmp_path, receipts, ctx, pdf_bytes
):
    """The review's probe: an overlapping republish and pull put 2 lines on raw_data and reported
    `duplicates: 1`. A run whose final update finds the receipt already published by another run
    did append its own message, so it counts it as published: the job counts match raw_data.
    """
    outbox = JsonlOutbox(tmp_path / "out")
    store = LocalFileStore(tmp_path / "fs")
    kwargs = {"tenant_id": TENANT, "store": store, "publisher": outbox}
    with pytest.raises(RuntimeError):
        IngestBridge(
            receipts=CrashingReceipts(receipts, at=ReceiptStatus.PUBLISHED), **kwargs
        ).ingest(ctx, _item(pdf_bytes))
    pending = receipts.recent(tenant_id=TENANT)[0]
    assert pending.status is ReceiptStatus.RECEIVED

    class Meanwhile:
        """Before this run's final update, the other run's update lands."""

        def __getattr__(self, name):
            return getattr(receipts, name)

        def set_status(self, ingest_id, status, **kw):
            if status is ReceiptStatus.PUBLISHED:
                receipts.set_status(
                    ingest_id, ReceiptStatus.PUBLISHED, tenant_id=TENANT
                )
            return receipts.set_status(ingest_id, status, **kw)

    result = IngestBridge(receipts=Meanwhile(), **kwargs).ingest(ctx, _item(pdf_bytes))
    assert (result.outcome, result.ingest_id) == (Outcome.PUBLISHED, pending.ingest_id)
    assert [m["id"] for m in outbox.read("raw_data")] == [pending.ingest_id] * 2
    assert receipts.counts(tenant_id=TENANT) == {"published": 1}


def test_a_receipt_published_before_the_attempt_is_counted_is_a_duplicate(
    tmp_path, receipts, ctx, pdf_bytes
):
    """Two runs retry one pending receipt; the other one's publish lands between this run's
    receipt write and its attempt count. Nothing goes out twice from here: a duplicate.
    """
    outbox = JsonlOutbox(tmp_path / "out")
    kwargs = {"tenant_id": TENANT, "store": LocalFileStore(tmp_path / "fs")}
    with pytest.raises(RuntimeError):
        IngestBridge(
            publisher=outbox,
            receipts=CrashingReceipts(receipts, at=ReceiptStatus.PUBLISHED),
            **kwargs,
        ).ingest(ctx, _item(pdf_bytes))
    [pending] = receipts.recent(tenant_id=TENANT)
    assert pending.status is ReceiptStatus.RECEIVED

    class OtherRunFirst:
        def __getattr__(self, name):
            return getattr(receipts, name)

        def begin_attempt(self, ingest_id, **kw):
            # the other run's update
            receipts.set_status(ingest_id, ReceiptStatus.PUBLISHED, tenant_id=TENANT)
            return receipts.begin_attempt(ingest_id, **kw)

    class Unreachable:
        def publish(self, *args, **kwargs):
            raise AssertionError("a published receipt must not be published again")

    result = IngestBridge(
        publisher=Unreachable(), receipts=OtherRunFirst(), **kwargs
    ).ingest(ctx, _item(pdf_bytes))
    assert (result.outcome, result.ingest_id) == (Outcome.DUPLICATE, pending.ingest_id)
    assert len(outbox.read("raw_data")) == 1
    assert receipts.get(pending.ingest_id, tenant_id=TENANT).attempts == 1


def test_shadow_mode_holds_the_item_until_the_source_is_enabled(bridge, ctx, pdf_bytes):
    shadow = dataclasses.replace(ctx, shadow=True)
    held = bridge.ingest(shadow, _item(pdf_bytes))
    assert (held.outcome, held.new_object) == (Outcome.HELD, True)
    receipt = bridge.receipts.get(held.ingest_id, tenant_id=TENANT)
    assert (receipt.status, receipt.detail, receipt.payload_ref) == (
        ReceiptStatus.HELD,
        "held for shadow mode",
        held.payload_ref,
    )
    assert bridge.store.get(held.payload_ref) == pdf_bytes
    assert bridge.publisher.read("raw_data") == []

    again = bridge.ingest(shadow, _item(pdf_bytes))
    assert (again.outcome, again.ingest_id, again.new_object) == (
        Outcome.HELD,
        held.ingest_id,
        False,
    )
    assert (
        bridge.receipts.get(held.ingest_id, tenant_id=TENANT) == receipt
    )  # nothing written

    enabled = bridge.ingest(ctx, _item(pdf_bytes))
    assert (enabled.outcome, enabled.ingest_id) == (Outcome.PUBLISHED, held.ingest_id)
    [message] = bridge.publisher.read("raw_data")
    assert message["value"]["ingest_id"] == held.ingest_id
    assert bridge.receipts.counts(tenant_id=TENANT) == {"published": 1}


def test_the_bridge_builds_every_envelope_with_the_strict_producer_model(
    bridge, ctx, pdf_bytes, monkeypatch
):
    """The reader ignores unknown fields; the bridge must not: it builds with the producer model,
    which forbids them, so a misspelled field is an error at the first item instead of a field
    silently missing from every message."""
    from src.handler import ingest as ingest_module

    built = []

    class Spy(ingest_module.ProducedEnvelope):
        def __init__(self, **fields):
            built.append(sorted(fields))
            super().__init__(**fields)

    monkeypatch.setattr(ingest_module, "ProducedEnvelope", Spy)
    result = bridge.ingest(ctx, _item(pdf_bytes))
    assert result.outcome is Outcome.PUBLISHED
    [fields] = built
    assert "payload_hash" in fields and "payload_sha256" not in fields
    stored = bridge.receipts.get(result.ingest_id, tenant_id=TENANT).envelope
    assert ingest_module.ProducedEnvelope.model_validate_json(stored).payload_hash


def test_a_retry_publishes_the_envelope_first_built_and_the_receipt_keeps_it(
    tmp_path, receipts, ctx, pdf_bytes
):
    """The review's probe: a re-pull overwrote the stored envelope, with another source_meta and
    connector_version, although the receipt keeps the envelope "as first built". It is never
    replaced now, and a retry publishes it, as the relay does: every attempt is the same message.
    """
    publisher = FlakyPublisher(failures=1)
    bridge = IngestBridge(
        tenant_id=TENANT,
        store=LocalFileStore(tmp_path / "fs"),
        publisher=publisher,
        receipts=receipts,
    )
    with pytest.raises(PublishError):
        bridge.ingest(ctx, _item(pdf_bytes))
    [failed] = receipts.recent(tenant_id=TENANT)
    newer = dataclasses.replace(ctx, connector_version="0.2.0")
    moved = RawItem(
        message_id="local:docs/a.pdf",
        data=pdf_bytes,
        source_meta={"path": "a.pdf", "license": "changed since"},
    )
    assert bridge.ingest(newer, moved).outcome is Outcome.PUBLISHED
    [(_topic, _key, envelope)] = publisher.messages
    assert (envelope["connector_version"], envelope["source_meta"]) == (
        "0.1.0",
        {"path": "a.pdf"},
    )
    assert envelope["attempt"] == 2
    assert receipts.get(failed.ingest_id, tenant_id=TENANT).envelope == failed.envelope


def test_a_pull_publishes_its_own_build_when_the_stored_envelope_does_not_read(
    tmp_path, receipts, ctx, pdf_bytes
):
    """A stored envelope the relay sets aside (it does not validate) is not published by a pull
    either: the pull has the bytes, builds the envelope again and publishes that one."""
    publisher = FlakyPublisher(failures=1)
    bridge = IngestBridge(
        tenant_id=TENANT,
        store=LocalFileStore(tmp_path / "fs"),
        publisher=publisher,
        receipts=receipts,
    )
    with pytest.raises(PublishError):
        bridge.ingest(ctx, _item(pdf_bytes))
    [failed] = receipts.recent(tenant_id=TENANT)
    receipts._db.execute(
        "UPDATE ingest_receipts SET envelope = ? WHERE ingest_id = ?",
        ("{not an envelope", failed.ingest_id),
    )
    receipts._db.commit()
    assert bridge.ingest(ctx, _item(pdf_bytes)).outcome is Outcome.PUBLISHED
    [(_topic, _key, envelope)] = publisher.messages
    assert (envelope["ingest_id"], envelope["attempt"]) == (failed.ingest_id, 2)
    assert envelope["payload_hash"] == failed.payload_sha256


def test_a_pull_publishes_its_own_build_when_the_stored_envelope_is_newer(
    tmp_path, receipts, ctx, pdf_bytes
):
    """A stored envelope a newer version of the service wrote (before a rollback) is not sent
    again as this version, which would strip what this reader does not know: a pull has the
    bytes, so it builds the envelope again and publishes that one; the stored one stays.
    """
    publisher = FlakyPublisher(failures=1)
    bridge = IngestBridge(
        tenant_id=TENANT,
        store=LocalFileStore(tmp_path / "fs"),
        publisher=publisher,
        receipts=receipts,
    )
    with pytest.raises(PublishError):
        bridge.ingest(ctx, _item(pdf_bytes))
    [failed] = receipts.recent(tenant_id=TENANT)
    newer = json.loads(failed.envelope)
    newer.update(envelope_version="0.4", connector_version="9.9.9")
    receipts._db.execute(
        "UPDATE ingest_receipts SET envelope = ? WHERE ingest_id = ?",
        (json.dumps(newer), failed.ingest_id),
    )
    receipts._db.commit()
    assert bridge.ingest(ctx, _item(pdf_bytes)).outcome is Outcome.PUBLISHED
    [(_topic, _key, envelope)] = publisher.messages
    assert (envelope["envelope_version"], envelope["connector_version"]) == (
        "0.3",
        "0.1.0",
    )
    assert (
        json.loads(receipts.get(failed.ingest_id, tenant_id=TENANT).envelope) == newer
    )
