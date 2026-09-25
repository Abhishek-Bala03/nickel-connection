"""Business logic, called by the routes in ``src.services``, the CLI and the jobs (as in Rockwell's
src/handler).

``ingest``: the ingest bridge every raw payload goes through. ``documents``: pull a document source,
read the receipts. ``jobs``: the job runner.
"""
