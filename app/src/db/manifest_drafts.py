"""Tenant-scoped, audited manifest drafts. Saving never enables a connector."""

from contextlib import closing
from datetime import UTC, datetime

from src.db.sqlite import connect
from src.error import ConflictError

SCHEMA = """
CREATE TABLE IF NOT EXISTS manifest_drafts (
 tenant_id TEXT NOT NULL,
 id TEXT NOT NULL,
 manifest TEXT NOT NULL,
 version INTEGER NOT NULL,
 updated_at TEXT NOT NULL,
 changed_by TEXT NOT NULL,
 PRIMARY KEY (tenant_id, id)
);
CREATE TABLE IF NOT EXISTS manifest_draft_audit (
 tenant_id TEXT NOT NULL,
 id TEXT NOT NULL,
 version INTEGER NOT NULL,
 updated_at TEXT NOT NULL,
 changed_by TEXT NOT NULL,
 PRIMARY KEY (tenant_id, id, version)
);
"""


class ManifestDraftStore:
    def __init__(self, path):
        self.path = path
        with closing(connect(path)) as db:
            db.executescript(SCHEMA)

    def list(self, tenant):
        with closing(connect(self.path)) as db:
            rows = db.execute(
                "SELECT id, manifest, version, updated_at, changed_by FROM manifest_drafts WHERE tenant_id=? ORDER BY id",
                (tenant,),
            ).fetchall()
        return [
            dict(
                zip(("id", "manifest", "version", "updated_at", "changed_by"), row),
                status="draft",
            )
            for row in rows
        ]

    def save(self, tenant, id, manifest, expected_version, actor):
        with closing(connect(self.path)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT version FROM manifest_drafts WHERE tenant_id=? AND id=?",
                (tenant, id),
            ).fetchone()
            if (row[0] if row else 0) != expected_version:
                raise ConflictError("The draft changed. Reload it before saving.")
            version = expected_version + 1
            stamp = datetime.now(UTC).isoformat()
            db.execute(
                "INSERT INTO manifest_drafts VALUES (?,?,?,?,?,?) ON CONFLICT(tenant_id,id) DO UPDATE SET manifest=excluded.manifest,version=excluded.version,updated_at=excluded.updated_at,changed_by=excluded.changed_by",
                (tenant, id, manifest, version, stamp, actor),
            )
            db.execute(
                "INSERT INTO manifest_draft_audit VALUES (?,?,?,?,?)",
                (tenant, id, version, stamp, actor),
            )
        return {
            "id": id,
            "manifest": manifest,
            "version": version,
            "updated_at": stamp,
            "changed_by": actor,
            "status": "draft",
        }
