"""Tenant-scoped plugin drafts stored alongside connector records."""

import json
import sqlite3
from contextlib import closing
from pathlib import Path

from src.db.sqlite import connect
from src.error import ConflictError, NotFoundError

_SCHEMA = """
CREATE TABLE IF NOT EXISTS plugins (
    tenant_id TEXT NOT NULL,
    id TEXT NOT NULL,
    name TEXT NOT NULL,
    definition TEXT NOT NULL,
    PRIMARY KEY (tenant_id, id),
    UNIQUE (tenant_id, name)
);
"""


class PluginStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        with closing(connect(self.path)) as db:
            with db:
                db.executescript(_SCHEMA)

    def add(self, tenant_id: str, plugin: dict) -> None:
        try:
            with closing(connect(self.path)) as db:
                with db:
                    db.execute(
                        """
                        INSERT INTO plugins (
                            tenant_id, id, name, definition
                        ) VALUES (?, ?, ?, ?)
                        """,
                        (
                            tenant_id,
                            plugin["id"],
                            plugin["name"],
                            json.dumps(plugin),
                        ),
                    )
        except sqlite3.IntegrityError:
            raise ConflictError(
                "A plugin with this ID or name already exists"
            ) from None

    def list(self, tenant_id: str) -> list[dict]:
        with closing(connect(self.path)) as db:
            rows = db.execute(
                """
                SELECT definition
                FROM plugins
                WHERE tenant_id = ?
                ORDER BY name
                """,
                (tenant_id,),
            ).fetchall()

        return [json.loads(row[0]) for row in rows]

    def get(self, tenant_id: str, plugin_id: str) -> dict:
        with closing(connect(self.path)) as db:
            row = db.execute(
                "SELECT definition FROM plugins WHERE tenant_id = ? AND id = ?",
                (tenant_id, plugin_id),
            ).fetchone()
        if row is None:
            raise NotFoundError("Plugin not found")
        return json.loads(row[0])

    def activate(self, tenant_id: str, plugin_id: str, integration: str) -> dict:
        # Read/modify/write inside one transaction; preserve the stored definition.
        with closing(connect(self.path)) as db:
            with db:
                db.execute("BEGIN IMMEDIATE")
                row = db.execute(
                    "SELECT definition FROM plugins WHERE tenant_id = ? AND id = ?",
                    (tenant_id, plugin_id),
                ).fetchone()
                if row is None:
                    raise NotFoundError("Plugin not found")
                plugin = json.loads(row[0])
                plugin.update(status="ready", nango_integration=integration)
                db.execute(
                    "UPDATE plugins SET definition = ? WHERE tenant_id = ? AND id = ?",
                    (json.dumps(plugin), tenant_id, plugin_id),
                )
        return plugin
