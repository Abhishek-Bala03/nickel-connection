# Connection → Transform demo contract

Status: proposed handoff for Nikhil's agreement; not yet a live Kafka publisher.
The existing v0.3 schema is unchanged. Schema Registry registration remains pending.

## Proposed wire format

- Topic: `raw_data`; value: UTF-8 JSON envelope v0.3, without a Schema Registry
  binary prefix. Validate against [the schema](../schemas/raw-data-envelope.schema.json).
- Proposed Kafka key: UTF-8 `tenant_id:connection_id`, for per-connection partitioning.
- One message per vendor response page, containing multiple readings, not one message
  per reading. Preserve the vendor JSON shape; Transform owns normalization.
- For this demo, propose inline `payload` and null `payload_ref`. Limit the entire
  serialized envelope to 900 KiB; larger messages must fail visibly and remain
  retryable until shared storage is agreed. Never silently truncate a response.
  Inline publication and the limit still need implementation in tasks 3–4.
- `payload_hash` and `payload_bytes` describe original response bytes, not a
  consumer's re-serialized JSON. JSON parsing can change whitespace/key ordering;
  do not recompute this hash from the inline object. The sample raw files preserve
  the exact bytes used for the example hashes.

## Field mapping

| Transform needs | Envelope field / meaning |
| --- | --- |
| Tenant | `tenant_id`; include it in storage and dedup scope |
| Patient | `source_patient_id`; currently the patient ID bound to the connector and Nango end user, not necessarily a vendor-issued ID |
| Provider | `source_meta.provider`: `dexcom` or `oura` |
| Stream | `source_meta.stream`: initially `egvs` or `daily_sleep` |
| Page | `source_meta.page`, starting at 1 within a pull |
| Connection | `connection_id`; `source` currently has the same value, not the provider name |
| Message ID | `message_id`: `<connection_id>/<stream>`; repeats across pages and pulls |
| Deduplication | `idempotency_key`; includes tenant, source, message ID and original payload hash |
| Run / receipt | `trace_id` / `ingest_id` |
| Payload | Dexcom `payload.records[]`; Oura `payload.data[]` |
| Time | `received_at` is ingestion time; each reading retains vendor timestamps |

`member_id` and envelope `event_time` can be null. Do not substitute ingestion
time for measurement time. `route=L7` is the existing wearable placeholder;
route these messages by provider/stream rather than interpreting them as lab data.

Delivery is proposed as at-least-once: retries may repeat an envelope. Deduplicate
by `idempotency_key`, not `message_id` alone (which would drop later pages).
Overlapping pull windows can repeat individual readings in different envelopes;
Transform also needs vendor record-level deduplication, scoped by tenant,
patient, provider and stream. Commit Kafka offsets only after durable processing;
failed processing must not be reported as successful.

## Review samples

These are synthetic one-record extracts of the checked-in vendor simulator
fixtures, with invented connection/tenant IDs, not live API responses:

- [Dexcom envelope](examples/kafka/dexcom.envelope.json) and [original payload bytes](examples/kafka/dexcom.payload.json).
- [Oura envelope](examples/kafka/oura.envelope.json) and [original payload bytes](examples/kafka/oura.payload.json).

Both envelopes are validated against the producer model and JSON Schema. Their
hashes, byte counts and idempotency keys match the accompanying payload files.

## Send Nikhil for agreement

> For the demo, can your Transform consumer accept plain JSON envelope v0.3 on
> `raw_data`, with inline vendor JSON (`records` for Dexcom egvs, `data` for Oura
> daily_sleep)? Patient is `source_patient_id`; provider and stream are in
> `source_meta`. Please deduplicate by `idempotency_key` and handle repeated
> readings across pull windows. Attached are the schema and two synthetic
> examples. Please confirm this mapping and payload format before we wire it up.

Task 2 is agreed only after Nikhil confirms or requests changes. No message has
been sent to him automatically.
