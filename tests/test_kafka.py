"""Broker acknowledgements, bounded inline payloads, and durable retry behavior."""

import dataclasses
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from confluent_kafka import KafkaException

from src.config import load_settings
from src.db.kafka import MAX_MESSAGE_BYTES, KafkaPublisher
from src.db.outbox import PublishError
from src.db.receipts import ReceiptStatus
from src.handler.documents import ingest_bridge
from src.handler.ingest import Outcome, RawItem
from src.handler.nango import _replay_pending


class Producer:
    def __init__(self, config):
        self.config = config
        self.error = None
        self.pending = 0
        self.calls = []
        self.callback = None

    def produce(self, topic, **kwargs):
        if isinstance(self.error, Exception):
            raise self.error
        self.calls.append((topic, kwargs))
        self.callback = kwargs['on_delivery']

    def flush(self, timeout):
        if self.callback and not self.pending:
            callback, self.callback = self.callback, None
            callback(self.error, None)
        return self.pending

    def purge(self):
        self.pending = 0
        self.error = 'purged'


@pytest.fixture
def value():
    return json.loads(Path('docs/examples/kafka/dexcom.envelope.json').read_text())


@pytest.fixture
def publisher(bridge):
    return KafkaPublisher('kafka:19092', bridge.store, producer_factory=Producer)


def test_acknowledged_plain_json_and_scoped_key(publisher, value):
    publisher.publish('raw_data', 'patient', value)
    assert publisher.producer.config['acks'] == 'all'
    assert publisher.producer.config['enable.idempotence'] is True
    topic, args = publisher.producer.calls[0]
    assert topic == 'raw_data'
    assert args['key'].decode() == value['tenant_id'] + ':' + value['connection_id']
    assert json.loads(args['value']) == value
    publisher.close()
    assert publisher.producer is None
    publisher.close()


@pytest.mark.parametrize('failure', ['delivery', 'timeout', 'missing_callback', 'queue', 'kafka'])
def test_no_ack_is_not_success(publisher, value, failure):
    producer = Producer({})
    publisher.producer = producer
    if failure == 'delivery':
        producer.error = 'broker rejected'
    elif failure == 'timeout':
        producer.pending = 1
    elif failure == 'missing_callback':
        producer.flush = lambda timeout: 0
    else:
        producer.error = BufferError() if failure == 'queue' else KafkaException()
    with pytest.raises(PublishError):
        publisher.publish('raw_data', 'key', value)
    publisher.close()


@pytest.mark.parametrize('raw', [b'[]', b'{"a":1,"a":2}', b'{"a":NaN}', b'broken'])
def test_invalid_raw_json_never_reaches_broker(publisher, value, raw):
    value.update(payload=None, payload_ref=value['tenant_id'] + '/test',
                 payload_hash=hashlib.sha256(raw).hexdigest(), payload_bytes=len(raw))
    publisher.store = SimpleNamespace(get=lambda *a, **kw: raw)
    with pytest.raises(PublishError):
        publisher.publish('raw_data', 'key', value)
    assert publisher.producer is None


@pytest.mark.parametrize('change', [
    {'payload_ref': 'another-tenant/object', 'payload': None},
    {'payload_ref': '11111111-1111-4111-8111-111111111111/test', 'payload': None, 'content_type': 'application/pdf'},
    {'payload': {'data': 'a' * MAX_MESSAGE_BYTES}},
    {'envelope_version': '999'},
])
def test_invalid_or_large_envelope_is_retained(publisher, value, change):
    value.update(change)
    with pytest.raises(PublishError):
        publisher.publish('raw_data', 'key', value)
    assert publisher.producer is None


def test_missing_and_corrupt_original(publisher, value):
    value.update(payload=None, payload_ref=value['tenant_id'] + '/stream/aa/' + 'a'*64)
    with pytest.raises(PublishError):
        publisher.publish('raw_data', 'key', value)
    publisher.store = SimpleNamespace(get=lambda *a, **kw: b'{}')
    with pytest.raises(PublishError):
        publisher.publish('raw_data', 'key', value)


def test_failure_survives_and_replays_original_without_fetching(bridge, ctx, publisher):
    bridge.publisher = publisher
    context = dataclasses.replace(ctx, source='connection', connection_id='connection',
                                  fetch='nango', connector='nango', formats=('json',))
    item = RawItem(message_id='connection/egvs', data=b'{"records": [1]}\n',
                   source_patient_id='patient', source_meta={'provider':'dexcom','stream':'egvs','page':1})
    producer = Producer({})
    producer.error = 'broker down'
    publisher.producer = producer
    with pytest.raises(PublishError):
        bridge.ingest(context, item)
    tenant = str(bridge.tenant_id)
    receipt = next(bridge.receipts.in_status(ReceiptStatus.PUBLISH_FAILED, {'connection'}, tenant_id=tenant))
    assert bridge.store.get(receipt.payload_ref) == item.data
    saved = json.loads(receipt.envelope)
    connector = SimpleNamespace(tenant_id=tenant, nango_connection_id='connection', patient_id='patient', provider='dexcom', streams=['egvs'])
    # A different patient cannot replay the page.
    connector.patient_id = 'other'
    assert _replay_pending(bridge, connector, ctx.trace_id) == 0
    connector.patient_id = 'patient'
    producer.error = None
    assert _replay_pending(bridge, connector, ctx.trace_id) == 1
    assert bridge.receipts.get(receipt.ingest_id, tenant_id=tenant).status is ReceiptStatus.PUBLISHED
    delivered = json.loads(producer.calls[-1][1]['value'])
    assert delivered['payload'] == {'records': [1]}
    assert delivered['payload_ref'] is None
    assert delivered['idempotency_key'] == saved['idempotency_key']
    assert delivered['ingest_id'] == receipt.ingest_id
    assert delivered['attempt'] == 2
    assert json.loads(receipt.envelope)['payload_ref'] == receipt.payload_ref
    assert bridge.ingest(context, item).outcome is Outcome.DUPLICATE


def test_settings_and_factory(settings):
    configured = load_settings(environ={'NICKEL_ENV':'development','NICKEL_KAFKA_BOOTSTRAP_SERVERS':'localhost:9092'})
    assert configured.kafka_bootstrap_servers == 'localhost:9092'
    with pytest.raises(ValueError, match='development'):
        load_settings(environ={'NICKEL_ENV':'qa','NICKEL_KAFKA_BOOTSTRAP_SERVERS':'localhost:9092'})
    with ingest_bridge(dataclasses.replace(settings, kafka_bootstrap_servers='localhost:9092')) as bridge:
        assert isinstance(bridge.publisher, KafkaPublisher)
