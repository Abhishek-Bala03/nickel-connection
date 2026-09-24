# 0001: The idempotency key and D-00

Prepared by Divij N.

**Status: Proposed** (24 Sep 2026), for the architecture decision the round-3 review of
nickel-connection PR #3 asked for. Until it is decided, the formula the code uses stays as it is.

## Context

D-00, the Layer 3 idempotency gate (step 0), keys a message by sha256(source ‖ message id): a message
that comes again is a repeat, and a replay is a no-op.

The `raw_data` envelope (v0.3, `app/src/models/envelope.py`) carries
`idempotency_key = sha256(tenant ‖ source ‖ message_id ‖ payload_hash)`, NUL-separated. It adds two
things to D-00:

- **The tenant.** Keys are tenant-first (the multi-tenancy rule). This only narrows the key, and is
  not in question.
- **The payload hash.** A document source can serve new bytes under the same path: a lab re-issues a
  report at the same URL, a folder gets a corrected file under the same name. The new version must
  not be dropped as a repeat, and with D-00's formula it would be, since its path, the message id,
  is the same.

The payload hash is right for documents, but not for every source. An HL7 v2 or FHIR source resends
a message under the same message id (the MSH-10 control id; a resource's id and version) with bytes
that differ in ways that do not make it a new message: a new MSH-7 timestamp, reordered segments,
re-serialised JSON. With the payload hash in the key, a genuine resend gets a new key, passes step 0
and falls through to clinical matching, where a duplicate costs the most.

## Options

**A. Keep D-00's formula, and let each connector's message id carry what makes a new version.**
`idempotency_key = sha256(tenant ‖ source ‖ message_id)`. The document connector's message id carries
the version: `<path>@<sha256>` (`local:clinic-drop/a.pdf@9f86d081…`, and the URL plus `@<sha256>` for
a web link). New bytes at the same path are a new message id, so a new key; the same bytes are the
same key. An HL7 connector's message id is its control id, so a resend with the same control id
stays a repeat, whatever its bytes.

- One formula for every connector, the one D-00 names (with the tenant).
- Each connector says what a new version of a message is, where it knows that best: its message id.
- The receipt's identity (tenant, source, message id, payload hash) already holds the hash, so it
  loses nothing.

**B. Amend D-00 per connector.** Keep `sha256(tenant ‖ source ‖ message_id ‖ payload_hash)` for
document sources and D-00's `sha256(tenant ‖ source ‖ message_id)` for message sources (HL7, FHIR), in
a decision record that names which connector uses which.

- No message id changes.
- Two formulas: whatever recomputes or reasons about the key must know which one a source uses, and
  every connector added later has to pick one. The choice lives in a table, not in the message.

## Recommendation

**A.** One formula, and each connector defines what a message version is, in its message id. HL7
resends with the same control id stay repeats; a document's new bytes at the same path are a new
message.

## Consequences if A is accepted

- `idempotency_key_of` drops the payload hash: sha256 over tenant, source and message id.
- The document connector's message id becomes `<fetch>:<locator>/<path>@<sha256>` (a web link's URL
  plus `@<sha256>`). `source_meta` keeps the path and the URL as they are.
- Receipts already written keep the old message id. They are migrated in place (the payload hash is
  on each receipt, so the new id can be computed), or a re-pull would not find them and would
  receive, store (a no-op: content-addressed) and publish every item again under new ingest ids.
- Open receipts' stored envelopes carry the old key. They migrate together with their message ids
  (the key recomputed under the new formula, the version stamped 0.4), or the relay rebuilds them
  under the new formula before it publishes: either way no old key goes out under the new version,
  and no new key under the old one.
- A `too_large` receipt a pull leaves for an item it never fetched has no hash to put in the message
  id: it keeps its surrogate key (`src.db.receipts.too_large_key`) over the path-only message id, and
  the item's own receipt, once it comes in, is keyed as above.
- Envelopes already on `raw_data` keep their keys. The key's meaning changes but not the envelope's
  shape; the version moves to 0.4 so that a consumer can tell the two apart.
- nickel-transform's Dispatcher dedupes on the key either way; under A it needs no per-source rule.
