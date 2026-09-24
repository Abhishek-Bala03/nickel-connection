"""Where the development data lives under ``NICKEL_DATA_DIR`` (``[STORAGE] data_dir``).

One place knows the folder names, so the CLI, the jobs and the API all open the same files:

    <data_dir>/filestore/          the File Store (content-addressed originals)
    <data_dir>/outbox/<topic>.jsonl the JSONL outbox (raw_data)
    <data_dir>/receipts.sqlite     ingest receipts
    <data_dir>/jobs.sqlite         job records for the jobs API
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DataLayout:
    root: Path

    @property
    def filestore(self) -> Path:
        return self.root / "filestore"

    @property
    def outbox(self) -> Path:
        return self.root / "outbox"

    @property
    def receipts_db(self) -> Path:
        return self.root / "receipts.sqlite"

    @property
    def jobs_db(self) -> Path:
        return self.root / "jobs.sqlite"
