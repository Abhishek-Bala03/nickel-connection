"""Data access. Development stores under ``NICKEL_DATA_DIR`` (see ``layout``): the File Store
(``file_store``), the JSONL outbox (``outbox``), the ingest receipts (``receipts``) and the job
records (``jobs``). Receipts and jobs move to Postgres with the other ingest tables; schema
migrations will live in ``app/migrations`` then."""
