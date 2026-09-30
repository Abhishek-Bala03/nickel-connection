# Connector manifests and Kafka envelopes (CN-01)

The canonical JSON Schemas are `schemas/connector-manifest.schema.json` (v1.0)
and `schemas/raw-data-envelope.schema.json` (producer envelope v0.3), using
JSON Schema Draft 2020-12. Connector definitions live at
`connectors/<id>/manifest.yaml`; the directory name must match `id`.
These contracts are stored here until the team's `nickel-schemas` repository is available.

## Prepare a plugin in the UI

Open **Connectors → Plugin**. Edit the YAML template, select **Validate**, then
**Save draft** or **Download manifest**. Validation errors identify the field and rule.
Saved drafts are tenant-scoped in `/data/connectors.sqlite`, have optimistic versions,
and record the authenticated actor and time on every save. Use **Reload drafts** after
a conflict; use **New from template** to create a different connector ID.
The envelope schema and manifest schema can also be inspected in the drawer.

Saving a draft does not approve or activate it, provision Nango, read a credential,
or contact an endpoint. Runtime plugin provisioning remains held as requested in PR #12.
A reviewed adapter, vendor/BAA gate, fixtures and conformance, egress controls, shadow
execution and version pinning remain prerequisites for an executable plugin.
The schema's endpoint declarations are not themselves a network egress control.
Existing approved provider account authorization stays under **Add new**.

## Manifest field conventions

The initial field names come from CN-01. The following nested shapes and units are
explicit v1.0 implementation choices for review; the spreadsheet did not specify them.

| Field | Contract |
| --- | --- |
| `schema_version` | Exactly `"1.0"` |
| `adapter` | `{id, version}`; exact `major.minor.patch`, never `latest` or a range |
| `id`, `stream`, `mode`, `route` | Source identifier, stream name, ingestion mode, L1–L9 route |
| `auth` | `{type, vault_path}`; references, never API keys or tokens |
| `schedule` | `{class, interval, overlap, backfill}`; durations in seconds, interval nullable for event/on-demand sources |
| `endpoints` | Unique HTTPS endpoint URLs without embedded credentials, query or fragment |
| `rate_limit` | `{requests, window_seconds}`, positive integers |
| `formats` | `{schema_id, validate}`; validation requires a schema reference |
| `expectations` | `{freshness_slo}` in seconds |
| `contract` | `{baa, dpa}` agreement references; both keys required |
| `phi`, `owner`, `phase`, `shadow` | Required PHI boolean, nonblank owner, dev/qa/prod phase and shadow boolean |
| `raw_retention` | `forever` or 1–3650 days, e.g. `30d` |

`contract.baa`, `contract.dpa`, `auth.vault_path`, `phi` and `owner` cannot be omitted.
Public development examples may explicitly use null agreement/credential references.
PHI requires a BAA reference; authenticated qa/prod sources require a Vault reference.
References are declarations, not evidence that legal or operational approval occurred.
Unknown fields and duplicate YAML keys are rejected. The editor accepts up to 64 KiB,
no aliases and at most 20 nested levels. It never echoes invalid values into API errors.

The existing `app/manifests/documents` format is a separate runtime subset. It is still
validated by the document connector and its tests; these new manifests do not silently
replace its fetch configuration or change how current document jobs execute.

## Validate and export

From the backend repository:

```sh
PYTHONPATH=app uv run python -m scripts.contracts
PYTHONPATH=app uv run python -m src.cli schema contract-manifest
PYTHONPATH=app uv run python -m src.cli schema contract-envelope
```

CI runs the contract check for every PR base. It rejects missing governance fields,
invalid manifests, ID/directory mismatches, invalid schema files and envelope model/schema
drift. Existing `schema manifest` and `schema envelope` CLI aliases retain the legacy
runtime subset and tolerant reader for compatibility; use `contract-*` for CN-01.

After an intentional producer model change, regenerate and review the artifact:

```sh
PYTHONPATH=app uv run python -m scripts.contracts --write-envelope
```

The envelope artifact derives from `ProducedEnvelope`, requires version 0.3 on the wire,
rejects unknown top-level fields and the old `payload_sha256` key, and expresses exactly
one non-null payload pointer or inline object. Consumers must enable JSON Schema format
checks for UUIDs and timezone-qualified date-times, as this repository's validator does.
Payload hashes cover the original bytes; semantic checks such as recomputing a hash
belong to ingestion and cannot be verified from an envelope-only JSON Schema.
Kafka transport itself remains PL-05 work; the local JSONL outbox is unchanged.

## Register the envelope when PL-05 is available

Live registration is pending: no registry type, URL or credentials have been supplied.
The script supports the [Confluent-compatible JSON Schema API](https://docs.confluent.io/platform/current/schema-registry/develop/api.html).
Confirm that PL-05 provides that API before using `--register`; adapt the registrar if it does not.
Preview the exact registration body without network access:

```sh
PYTHONPATH=app uv run python -m scripts.register_envelope
```

Set `NICKEL_SCHEMA_REGISTRY_URL` and `NICKEL_SCHEMA_REGISTRY_SUBJECT` in the local
environment (e.g. `raw_data-value` only if that is PL-05's agreed subject).
For basic authentication also set `NICKEL_SCHEMA_REGISTRY_USERNAME` and
`NICKEL_SCHEMA_REGISTRY_PASSWORD`; do not put secrets in the manifest or repository.

```sh
PYTHONPATH=app uv run python -m scripts.register_envelope --register
```

The registrar checks compatibility with the latest registered version when the subject
exists, preserves the registry's configured compatibility policy, registers `schemaType=JSON`,
and reads back the exact schema before reporting its ID and version. A missing subject
is created; other lookup errors fail closed. HTTPS is required except on loopback for a
local registry. Successful registration is not claimed by the UI until platform integration
is implemented; retain the command's receipt with PL-05's deployment evidence.
