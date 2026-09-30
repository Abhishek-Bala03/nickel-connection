"""Acknowledged Kafka transport for the local JSON demo; receipts own durable retries."""

import hashlib
import json

from confluent_kafka import KafkaException, Producer

from src.db.file_store.base import FileStoreError
from src.db.outbox import PublishError
from src.models.envelope import ProducedEnvelope

MAX_MESSAGE_BYTES = 900 * 1024


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


class KafkaPublisher:
    def __init__(self, bootstrap_servers, store, *, producer_factory=Producer):
        self.store = store
        self.factory = producer_factory
        self.producer = None
        self.config = {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "nickel-connection",
            "enable.idempotence": True,
            "acks": "all",
            "delivery.timeout.ms": 10000,
            "request.timeout.ms": 5000,
            "message.max.bytes": 1000000,
            "allow.auto.create.topics": False,
            "log_level": 0,
        }

    def _serialize(self, value):
        envelope = dict(value)
        if envelope.get("payload_ref") is not None:
            ref = envelope["payload_ref"]
            if not ref.startswith(envelope["tenant_id"] + "/"):
                raise ValueError("payload tenant mismatch")
            if envelope["content_type"] != "application/json":
                raise ValueError("demo Kafka requires JSON payloads")
            raw = self.store.get(ref, max_bytes=MAX_MESSAGE_BYTES)
            if len(raw) != envelope["payload_bytes"] or hashlib.sha256(raw).hexdigest() != envelope["payload_hash"]:
                raise ValueError("payload integrity mismatch")
            envelope["payload"] = json.loads(raw, object_pairs_hook=_object)
            envelope["payload_ref"] = None
        ProducedEnvelope.model_validate(envelope)
        encoded = json.dumps(envelope, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")
        if len(encoded) > MAX_MESSAGE_BYTES:
            raise ValueError("demo envelope exceeds 900 KiB")
        return encoded

    def publish(self, topic, key, value, *, message_id=None):
        try:
            encoded = self._serialize(value)
        except (ValueError, TypeError, KeyError, RecursionError, FileStoreError):
            raise PublishError("Kafka payload unavailable, invalid, or over the 900 KiB demo limit") from None
        delivered = []

        def acknowledged(error, message):
            delivered.append(error is None)

        try:
            if self.producer is None:
                self.producer = self.factory(self.config)
            partition_key = f"{value['tenant_id']}:{value.get('connection_id') or key}"
            self.producer.produce(topic, key=partition_key.encode(), value=encoded, on_delivery=acknowledged)
            pending = self.producer.flush(11)
            if pending or delivered != [True]:
                raise PublishError("Kafka delivery not acknowledged; receipt retained for retry")
        except (KafkaException, BufferError):
            raise PublishError("Kafka unavailable; receipt retained for retry") from None

    def close(self):
        if self.producer is not None:
            self.producer.purge()
            self.producer.flush(1)
            self.producer = None
