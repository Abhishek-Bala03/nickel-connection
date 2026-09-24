# 0002: A too_large receipt for an item a pull never fetched

Prepared by Divij N.

**Status: Proposed** (24 Sep 2026). It records what nickel-connection PR #3 does since round 3 and
asks for the receipt contract (architecture §6) to name it.

## Context

The architecture's receipt contract (§6) has `received → (held) → published`, with `publish_failed`,
`too_large` and `rejected` as side states, and asks that edge rejects and oversize payloads be kept
rather than dropped. A pull stops a file over the size cap in force before it has the bytes: it is
listed over the cap (never downloaded), a download passes the cap while it is spooled (stopped
there), or a file an SDK put on disk is over it (measured, never read). Before round 3 such a file
left no receipt at all, only a `skipped` count shared with failed fetches, so an oversize lab PDF
never reached the record. The review asked for a metadata-only `too_large` receipt for it: the size
seen and the cap, no bytes.

Two things make that receipt unlike the others:

- **It has no content hash.** A receipt is unique per (tenant, source, message id, payload hash),
  and the pull never read the bytes.
- **The item can still arrive.** Once the cap is raised, or a smaller version appears at the same
  path, the next pull gets the bytes and the item gets a receipt of its own, keyed by its hash. The
  too_large receipt must not stay open beside it for ever.

## Decision (proposed)

1. **A marker per item at the source.** Its `payload_sha256` column holds a surrogate key, sha256
   over `too_large`, tenant, source and message id, NUL-separated (`src.db.receipts.too_large_key`).
   The size seen is left out of the key: a download stopped at the cap never learns it, and a key
   that changed with the size would add a row for every size. A repeated pull finds the marker and
   adds no row; a new size or cap only updates its detail. The API and the command line show the
   marker's `payload_sha256` as null (`Receipt.is_marker`): it is not the hash of any bytes.
2. **A new final status, `superseded`.** When a pull gets the item's bytes within the cap, whatever
   becomes of them (published, held, rejected), the marker moves `too_large → superseded`, and its
   detail names the payload hash of the receipt that took over. The relay never takes it up.
3. **Guarded moves, on the marker only.** Both moves filter on the surrogate key and the status they
   come from: `supersede_too_large` moves `too_large → superseded`; `mark_too_large` moves
   `superseded → too_large` (the item is over the cap again) or refreshes a `too_large` marker's
   detail. `set_status` never moves a `superseded` receipt. A payload hash equals the surrogate only
   for bytes built as its exact preimage; the pull then finds the marker as that item's receipt and
   carries it through, its key by then really the payload's hash: harmless.
4. **No reopening for the same bytes.** A superseded marker whose item is seen again at the size its
   published receipt came in at (a cap lowered after the item was delivered) stays superseded: most
   likely it is the same item. Another size, or an unknown one, reopens it.

The bridge's own check of a payload in hand (an upload would meet it) keeps its `too_large` receipt
keyed by the payload's hash: it goes through under the same ingest id once the cap is raised, and is
never superseded.

## Why a new status and not the others

- `rejected` means the same bytes can never pass; a raised cap lets this item through.
- Deleting the marker would lose the record that the item was once held back, which the
  architecture asks to keep (and which issue #4's replay would hang on).
- Taking the marker's row over as the item's receipt (rewriting its key to the real hash) works
  only when no receipt exists for those bytes yet; when one does (an earlier version came back) the
  marker still needs closing, so a closing status is needed either way.

## What the Postgres move changes

The marker lives in the SQLite stand-in. In the Postgres ingest tables, `payload_sha256` becomes
nullable: a marker stores NULL instead of the surrogate, and a partial unique index on
(tenant_id, source, message_id) `WHERE payload_sha256 IS NULL` keeps it one per item, beside the
existing unique key on (tenant_id, source, message_id, payload_sha256) for receipts with bytes.
`too_large_key` and `Receipt.is_marker` go then.

## Open

- Whether `superseded` joins architecture §6's receipt statuses, or the marker moves to a table of
  its own.
- Issue #4 (quarantine, `raw.dlq`, replay): whether it covers the pull side, where this marker is
  what a replay of an over-cap file hangs on.
