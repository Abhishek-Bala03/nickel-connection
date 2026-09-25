# Demo: wearables through Nango (Oura Ring, Dexcom CGM)

A local, end-to-end run of the connection layer: add a connector in the Connections UI, schedule
it, and read the JSON each run brought in. Nango holds the credentials and proxies the vendor call
(auth + proxy only, decision 2); the Nickel scheduler decides when; the connection service pulls,
stores the original and publishes the envelope to `raw_data`.

```
Connections UI ──/api──────────────► scheduler ──POST /jobs/nango/pull──► nickel-connection
   (Vite, :5173)  └─/api/connection/─────────────────────────────────────►   │  Nango proxy
                                                                               ▼
                                             vendor-sim (Oura v2 / Dexcom v3) ◄── nango-server ── nango-db (Postgres 16)
```

The vendors are simulated: `vendor-sim` serves one synthetic patient per vendor from
`dev/vendor-sim/data` (made by the Nickel data simulator; no real data). Nango's proxy is pointed at
it with `Base-Url-Override`, set only by `NICKEL_NANGO_BASE_URL_OVERRIDES`, which the service
refuses outside development.

## Run it

1. `.env` (git-ignored) from `.env.example`: set `NICKEL_SERVICE_TOKEN`, and the four `NANGO_*`
   values the example explains how to generate. `NANGO_SECRET_KEY_DEV` must be a UUID v4.
2. The backend: Nango, its Postgres, the vendor simulator, the connection service and the scheduler.

   ```sh
   docker compose -f compose.yaml -f compose.nango.yaml up -d --build
   ```

   Ports, all on loopback: the connection service on 8000, the scheduler on 8001, and the Nango
   dashboard on 3003 (basic auth: `admin` / `NANGO_DASHBOARD_PASSWORD`).
3. The UI, from a `nickel-connections-ui` checkout next to this one:

   ```sh
   cd ../nickel-connections-ui/app
   NICKEL_DEV_SERVICE_TOKEN=<NICKEL_SERVICE_TOKEN from .env> npm run dev
   ```

   Open http://127.0.0.1:5173.

## The walkthrough

1. **Connectors → Add new.** Name `Oura Ring – 12489275`, source Nango, provider Oura Ring, Nango
   connection id `oura-12489275`, patient id `12489275`, all five streams. Save: for a simulated
   provider the service creates the Nango integration and connection itself. Press **Test**: one
   call through Nango, e.g. "daily_sleep returned 82 records".
2. The same for **Dexcom CGM**: connection id `dexcom-32021994`, patient `32021994`, streams `egvs`,
   `devices`, `dataRange`.
3. **Scheduler.** New schedule, connector *Nango — wearables*, pick the connection, timing *Every
   minute* (the Nango minimum is 60 s). Save, then **Run now** for an immediate run.
4. **Executions.** Each run shows its status, duration and record count (Oura: 410 = 5 streams ×
   82 days; Dexcom: 1,404 glucose readings + device + data range). Select a run: one tab per stream,
   and the stored original as a JSON tree, with Copy and Download.

A second run of unchanged data stores nothing new: its streams are `duplicate` (the receipt already
exists) and still viewable, and the record count is the same.

## What is where

| Piece | Where |
|---|---|
| Provider catalog (streams → vendor paths) | `app/src/connectors/nango/__init__.py` |
| Connectors and each pull's objects | `connectors.sqlite` (`app/src/db/connectors.py`) |
| API | `GET/POST/PUT /connectors`, `POST /connectors/{id}/test`, `POST /jobs/nango/pull`, `GET /jobs/{id}/pulls`, `GET /jobs/{id}/pulls/{stream}/payload` |
| Scheduler side | connector type `nango` (minimum 60 s); connections come from `GET /connectors`; each run keeps its `job_id` (schema v2) |

## Limits

- The simulator ignores date and page parameters: every pull returns the whole fixture.
- Each simulated vendor has one patient; the base-URL override is per provider.
- The envelope requires a lab route (L1–L9) and wearables have none: they carry L7, the lowest
  fidelity tier, until the envelope takes a null route for non-lab sources.
- Real Oura or Dexcom: create the vendor's OAuth integration in Nango, connect the patient through
  Nango's Connect flow, and leave `NICKEL_NANGO_BASE_URL_OVERRIDES` unset.
- Reset the demo: `docker compose -f compose.yaml -f compose.nango.yaml down -v`.
