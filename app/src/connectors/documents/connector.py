"""The document connector run: list the source, keep the files the manifest wants, fetch each one, and
hand it to the ingest bridge. A publish failure stops the run (nothing after it would be durable).

The manifest governs the run: its ``phase`` must clear it for the service's environment or nothing is
listed; ``shadow`` holds every item (stored and receipted, not published); ``raw_retention`` decides the
object class of the originals. The size cap in force is the smaller of the manifest's
``max_file_bytes`` and the service's (``NICKEL_MAX_FILE_BYTES``): a file listed over it is skipped
before the download, the source hands back nothing over it, and the bridge checks it once more.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from src.config import Service
from src.db.outbox import PublishError
from src.handler.ingest import (
    IngestBridge,
    IngestResult,
    Outcome,
    RawItem,
    SourceContext,
)
from src.logging import logger
from src.utility.ids import uuid7

from .manifest import DocumentManifest
from .sources import (
    FetchError,
    HttpSource,
    HuggingFaceSource,
    KaggleSource,
    LocalDirSource,
    RefusedError,
    Source,
    SourceFile,
)

CONNECTOR = "documents"
CONNECTOR_VERSION = "0.1.0"
_KEEP_EXAMPLES = 10


@dataclass
class RunSummary:
    source: str
    trace_id: str | None = None  # the run's id, on every envelope it published
    listed: int = 0
    matched: int = 0
    published: int = 0
    held: int = 0  # stored and receipted under shadow mode, not published
    duplicates: int = 0
    rejected: int = 0
    skipped: int = 0
    refused: int = 0  # downloads the egress rule stopped (docs: HttpSource)
    new_objects: int = 0
    new_bytes: int = 0
    # The line for a person: may name the file a publish failed at.
    aborted: str | None = None
    # Why the run did not reach the end, content-free (what a job record keeps, with the counts): a
    # clinic's file names can carry patient names, so the publish failure names no file here.
    error: str | None = None
    problems: list[str] = field(default_factory=list)  # first few "path: reason"

    def note(self, rel_path: str, reason: str) -> None:
        if len(self.problems) < _KEEP_EXAMPLES:
            self.problems.append(f"{rel_path}: {reason}")

    def skip(self, rel_path: str, reason: str) -> None:
        self.skipped += 1
        self.note(rel_path, reason)

    def refuse(self, rel_path: str, reason: str) -> None:
        self.refused += 1
        self.note(rel_path, reason)

    def record(self, result: IngestResult, rel_path: str, size: int) -> None:
        if result.outcome in (Outcome.PUBLISHED, Outcome.HELD):
            if result.outcome is Outcome.PUBLISHED:
                self.published += 1
            else:
                self.held += 1
            if result.new_object:
                self.new_objects += 1
                self.new_bytes += size
        elif result.outcome is Outcome.DUPLICATE:
            self.duplicates += 1
        elif result.outcome is Outcome.TOO_LARGE:
            # Held back by the service's cap, as by the manifest's: not final, so not rejected.
            self.skip(rel_path, result.reason or "over the cap")
        else:
            self.rejected += 1
            self.note(rel_path, result.reason or "rejected")

    def counts(self) -> dict[str, int]:
        """The numbers only (what a job record keeps): no paths, no reasons."""
        return {
            "listed": self.listed,
            "matched": self.matched,
            "published": self.published,
            "held": self.held,
            "duplicates": self.duplicates,
            "rejected": self.rejected,
            "skipped": self.skipped,
            "refused": self.refused,
            "new_objects": self.new_objects,
            "new_bytes": self.new_bytes,
        }

    def lines(self) -> list[str]:
        out = [
            f"source          {self.source}",
            f"trace_id        {self.trace_id or '-'}",
            f"listed          {self.listed}",
            f"matched         {self.matched}",
            f"published       {self.published}  "
            f"(new objects {self.new_objects}, {self.new_bytes:,} bytes)",
            f"held            {self.held}",
            f"duplicates      {self.duplicates}",
            f"rejected        {self.rejected}",
            f"skipped         {self.skipped}",
            f"refused         {self.refused}",
        ]
        if self.aborted:
            out.append(f"ABORTED         {self.aborted}")
        out.extend(f"  - {problem}" for problem in self.problems)
        return out


def build_source(manifest: DocumentManifest, *, max_bytes: int) -> Source:
    """The manifest's source, which hands back no file over ``max_bytes`` (the cap in force: see
    ``DocumentConnector``, which passes the smaller of the manifest's and the service's).
    """
    if manifest.kaggle is not None:
        return KaggleSource(
            manifest.kaggle.handle, manifest.kaggle.files, max_bytes=max_bytes
        )
    if manifest.huggingface is not None:
        spec = manifest.huggingface
        return HuggingFaceSource(
            spec.repo_id, spec.revision, spec.repo_type, max_bytes=max_bytes
        )
    if manifest.http is not None:
        urls = [str(url) for url in manifest.http.urls]
        return HttpSource(urls, max_bytes=max_bytes)
    if manifest.local is not None:
        return LocalDirSource(manifest.local.path, max_bytes=max_bytes)
    # The manifest model already guarantees exactly one source block.
    raise ValueError(f"manifest {manifest.id!r} has no source block")


class DocumentConnector:
    def __init__(
        self,
        manifest: DocumentManifest,
        bridge: IngestBridge,
        source: Source | None = None,
        *,
        environment: str | None = None,
        max_file_bytes: int | None = None,
        trace_id: str | None = None,
    ) -> None:
        self.manifest = manifest
        self.bridge = bridge
        # The size cap in force: the smaller of the manifest's and the service's
        # (NICKEL_MAX_FILE_BYTES, unless the caller says otherwise). The manifest's alone has no
        # upper bound, and the bridge's check comes only once the whole file is in memory.
        service_cap = (
            Service.max_file_bytes if max_file_bytes is None else max_file_bytes
        )
        self.max_file_bytes = min(manifest.max_file_bytes, service_cap)
        self.source = (
            source
            if source is not None
            else build_source(manifest, max_bytes=self.max_file_bytes)
        )
        # The service's environment (conf/application.conf), unless the caller says otherwise.
        self.environment = Service.environment if environment is None else environment
        # The run's id, on every envelope it publishes: a job's id, or a fresh one.
        self.trace_id = trace_id or uuid7().hex
        self.context = SourceContext(
            source=manifest.id,
            fetch=manifest.fetch,
            route=manifest.route,
            mode=manifest.mode,
            connector=CONNECTOR,
            connector_version=CONNECTOR_VERSION,
            retention_class=manifest.retention_class,
            formats=tuple(manifest.formats),
            trace_id=self.trace_id,
            shadow=manifest.shadow,
            raw_retention=manifest.raw_retention,
        )

    def run(self, limit: int | None = None) -> RunSummary:
        cap = self._cap(limit)
        summary = RunSummary(source=self.manifest.id, trace_id=self.trace_id)
        if not self.manifest.cleared_for(self.environment):
            summary.aborted = summary.error = (
                f"manifest phase {self.manifest.phase!r} is not cleared for the "
                f"{self.environment} environment"
            )
            return summary
        for item in self.source.files():
            summary.listed += 1
            if not self.manifest.wants(item.rel_path):
                continue
            if cap is not None and summary.matched >= cap:
                break
            summary.matched += 1
            if not self._ingest_one(item, summary):
                break
        return summary

    def _cap(self, limit: int | None) -> int | None:
        caps = [cap for cap in (limit, self.manifest.max_files) if cap is not None]
        return min(caps) if caps else None

    def _ingest_one(self, item: SourceFile, summary: RunSummary) -> bool:
        """Fetch and ingest one file. False means stop the run (a publish failed)."""
        data = self._fetch(item, summary)
        if data is None:
            return True
        raw = RawItem(
            message_id=self._message_id(item),
            data=data,
            source_meta=self._meta(item),
        )
        try:
            result = self.bridge.ingest(self.context, raw)
        except PublishError as exc:
            summary.aborted = f"publish failed at {item.rel_path}: {exc}"
            summary.error = "publish failed; the pull stopped"
            return False
        logger.debug("%s %s -> %s", self.manifest.id, item.rel_path, result.outcome)
        summary.record(result, item.rel_path, len(data))
        return True

    def _fetch(self, item: SourceFile, summary: RunSummary) -> bytes | None:
        """The file's bytes, or None when it is skipped (over the cap, or unreachable) or refused
        (the egress rule: a host the manifest does not name, not https, a private address).
        """
        cap = self.max_file_bytes
        if item.size is not None and item.size > cap:
            summary.skip(item.rel_path, f"{item.size:,} bytes is over the {cap:,} cap")
            return None
        try:
            return item.fetch()
        except RefusedError as exc:
            summary.refuse(item.rel_path, str(exc))
            return None
        except (FetchError, OSError) as exc:
            summary.skip(item.rel_path, f"fetch failed: {exc}")
            return None

    def _message_id(self, item: SourceFile) -> str:
        if self.manifest.fetch == "http" and item.url:
            return item.url
        return f"{self.manifest.fetch}:{self.source.locator}/{item.rel_path}"

    def _meta(self, item: SourceFile) -> dict[str, str | int | None]:
        return {
            "locator": self.source.locator,
            "path": item.rel_path,
            "url": item.url,
            "license": self.manifest.license,
        }
