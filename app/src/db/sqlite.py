"""One connection policy for the service's SQLite files: the receipts and the job records.

Both files are shared by the job worker, the API's request threads and the command line, each on a
connection of its own: WAL mode (readers never block the writer, nor the writer the readers) and a
busy timeout (a writer waits for the other one instead of failing at once with "database is
locked"). The policy is written once, here, so the two stores cannot drift apart again.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

BUSY_TIMEOUT_MS = 5000


def connect(path: Path) -> sqlite3.Connection:
    """A connection to the file (created, with its folder, if need be) under the policy above."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path)
    db.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
    db.execute("PRAGMA journal_mode = WAL")
    return db
