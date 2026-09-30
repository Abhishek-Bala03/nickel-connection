> PR review update (30 September 2026): runtime plugin creation and provisioning are
> disabled pending the reviewed-manifest/SDK decision. Existing plugin records are retained,
> but omitted from the catalog and refused by create, test and pull. The plugin workflow
> described below is historical design context, not an enabled feature.
>
> Available built-ins enforce their exact configured integration. A patient ID is required;
> Connect sessions set `end_user.id` from that patient and `organization.id` from the server's
> tenant. Discovery filters by those identities, rechecks returned ownership, and refuses
> ambiguous accounts. Unbound legacy accounts must be reauthorized; no automatic reassignment.
> Provider, patient and connection are immutable after creation. User base URL overrides
> and arbitrary POSTs are refused; only `/_search` POSTs remain in the held SDK helper.
>
> Successful built-in pulls persist per-tenant/connector/stream UTC watermarks. The initial
> window is one day, with a one-day Oura overlap and five-minute Dexcom EGVS overlap.
> Oura pagination is consumed one page at a time, each with its own raw receipt, hash and
> payload viewer entry. The watermark advances only after the whole stream succeeds.
> Metadata endpoints have no date filter. Failed/rejected streams do not advance. GETs retry
> 429 twice, respecting Retry-After up to 30 seconds; longer waits fail for a later retry.
> This does not implement a FHIR incremental adapter: that remains part of the held SDK work.
> The reusable resource helper now yields original pages separately without a synthetic wrapper;
> it is not connected to runtime plugins. HAPI base-path next links (`/fhir?_getpages=...`)
> remain unsupported and fail explicitly. SDK integration must ingest each yielded page before
> requesting the next, with its own receipt and hash.
>
> Provisioning (including rotation of live integrations) cannot run, so no OAuth secret can
> be patched through these routes. An actor audit and explicit rotation flow are prerequisites
> if reviewed plugin provisioning is restored.
>
> CI now runs for all PR bases. Retarget to dev only after the dependent base branches land.

# Healthcare API plugins and account connections

The Plugins drawer now saves resource definitions, creates/configures a Nango integration,
and opens the connection workflow. Oura and Dexcom's existing adapters still work.
A plugin is reusable: authorize another account under its integration to create another
Nickel connector and schedule.

## Add a healthcare source

1. Open **Connectors → Plugin**. Search and select a provider from the running Nango catalog.
2. Enter the source name and a unique plugin ID. Leave the base URL blank to use Nango's
   provider template, or supply the authorized API's public HTTPS base URL.
3. Add named resources with relative paths, such as `/Patient` (use the actual vendor's
   documented path). GET and POST are supported; POST is for read/search endpoints.
4. Expand resource options for JSON query parameters, an optional POST body, and pagination.
   Query values must be strings. Credentials belong in Nango, not resource configuration.
5. Supply OAuth application credentials/scopes if the selected provider requires them.
   Advanced integration settings are forwarded as Nango's `integration_config` object.
   Application credentials and integration configuration are never persisted by Nickel.
6. Choose **Save and configure in Nango**. The definition is saved first. On failure, select
   the saved draft and retry **Configure in Nango**. Creation retries reuse a deterministic,
   tenant-scoped integration ID. Existing integration credentials are kept when fields are blank.
7. Choose **Add account**. Enter an account reference, then **Authorize another account**.
   Open the authorization link and enter account credentials/consent in Nango's UI.
8. Return to Nickel, choose **Refresh connections**, select the authorized connection and
   save the connector. Test it, then create its schedule under **Nango — API sources**.

Configured means the integration exists, not that an account has authorized successfully.
Nickel verifies the connection exists when saving. Multiple accounts receive separate
connector IDs and can have separate streams and schedules. Cancelling authorization saves
no connector; a successful authorization remains available if the local connector save fails.

Saved plugin definitions are immutable in this version; use a new ID/name for a changed
resource definition. Nango application settings can be reconfigured on an existing plugin.
The saved-plugin selector also resumes drafts made through the API before this UI was added.

## Provider compatibility

Provider selection determines authentication. API keys, OAuth and provider-specific flows
are handled by the selected Nango template and Connect UI, including required account/domain
configuration. A provider missing from this deployment must be added/configured in Nango
first; a URL alone cannot implement a bespoke healthcare authentication protocol.
The deployment's Connect UI must be enabled and browser-reachable. The API key stays on the
backend. No production account authorization is performed by the automated tests.

Base URL overrides are administrator configuration for the API that owns the credentials.
Nango's outbound URL policy must enforce deployment network restrictions (including DNS and
redirects); Nickel rejects non-HTTPS, embedded credentials and literal private addresses.
The local demo's relaxed policy is only for the vendor simulator.

## Resources and pagination

- `none`: one JSON response. GET and POST are supported; POST bodies are JSON objects.
- `fhir`: follows a Bundle's `link` entry with `relation: next`. Only its query is forwarded
  to the original resource; next links cannot change Nango's host or the configured path.
- `cursor`: dot-separated `cursor_path` (default `next_cursor`) is sent through `cursor_param`
  (default `cursor`) until it is absent or empty.
- `offset`: `items_path` (default `data`, empty for a root array), `offset_param` (`offset`),
  `limit_param` (`limit`), and `page_size` (100). Stops on a short page.

Automatic pagination is supported for GET only. `max_pages` defaults to 50, up to 200.
Loops, page-limit exhaustion, non-JSON replies and oversized responses fail explicitly rather
than silently reporting a complete pull. The total response limit is the smaller of Nickel's
configured file limit and 20 MiB. Multiple pages are retained in a JSON `pages` wrapper,
with each original page's bytes preserved inside it. Payloads above the existing 5 MiB
viewer limit are retained but are not displayed inline in Executions.

## Backend endpoints

- `GET /nango/providers`: safe provider metadata.
- `GET /plugins`, `POST /plugins`: tenant-owned definitions; POST creates a draft.
- `POST /plugins/{id}/provision`: create/configure the Nango integration and mark it ready.
- `GET /connectors/catalog`: built-ins plus ready plugins.
- `POST /nango/connect-session`: short-lived authorization link for the selected integration.
- Existing `/connectors`, test, `/jobs/nango/pull`, scheduler and Executions APIs support plugins.

The browser uses `/api/connection` before these paths. The existing gateway supplies the
service token and CSRF protection. This is an administrator UI for the configured tenant's
Nango environment, not a public end-user integration explorer.

## Run locally

Rebuild with the same overlay used by the active deployment. For the local demo:

```bash
docker compose -f compose.yaml -f compose.nango.yaml up -d --build nickel-connection scheduler
```

For a remote Nango deployment, set `NICKEL_NANGO_URL` and `NICKEL_NANGO_SECRET_KEY` in the
backend environment and use `compose.nango.remote.yaml` instead. Keep the existing data volumes.
The Vite UI continues to proxy the backend on port 8000 and scheduler on port 8001.

References: [Nango providers](https://nango.dev/docs/reference/backend/http-api/providers/list),
[create integration](https://nango.dev/docs/reference/backend/http-api/integration/create),
[update integration](https://nango.dev/docs/reference/backend/http-api/integration/update),
[Connect sessions](https://nango.dev/docs/reference/backend/http-api/connect/sessions/create).
