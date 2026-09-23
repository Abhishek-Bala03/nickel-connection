import re

import pytest

from src.config import DEFAULT_MAX_FILE_BYTES
from src.connectors.documents import (
    DocumentConnector,
    DocumentManifest,
    RunSummary,
    build_source,
)
from src.connectors.documents.sources import (
    FetchError,
    HttpSource,
    HuggingFaceSource,
    KaggleSource,
    LocalDirSource,
    RefusedError,
    SourceFile,
)
from src.db.file_store import LocalFileStore
from src.db.outbox import JsonlOutbox, PublishError
from src.db.receipts import ReceiptStatus
from src.handler.ingest import IngestBridge

TENANT = "0192a3b4-0000-7000-8000-000000000001"
AUTH_TYPES = {"kaggle": "kaggle_token", "huggingface": "hf_token"}


# The settings the handler passes (ServiceSettings.environment and .max_file_bytes): a development
# deployment with the default cap, unless a test says otherwise.
DEPLOYMENT = {"environment": "development", "max_file_bytes": DEFAULT_MAX_FILE_BYTES}


def _connector(manifest, bridge, source=None, **kw):
    return DocumentConnector(manifest, bridge, source, **{**DEPLOYMENT, **kw})


def _manifest(fetch="local", **over):
    blocks = {
        "local": {"local": {"path": "unused"}},
        "kaggle": {"kaggle": {"handle": "owner/dataset"}},
        "huggingface": {"huggingface": {"repo_id": "org/ds"}},
        "http": {"http": {"urls": ["https://lab.example/a.pdf"]}},
    }
    data = {
        "id": "test-docs",
        "description": "test",
        "stream": "documents",
        "mode": "pull",
        "route": "L7",
        "fetch": fetch,
        "auth": {"type": AUTH_TYPES.get(fetch, "none"), "vault_path": None},
        "phi": False,
        "owner": "divij",
        # A clinic's own source; the phase tests set theirs. The SDK sources are dev only.
        "phase": "dev" if fetch in ("kaggle", "huggingface") else "prod",
        "shadow": False,
        "contract": {"baa": None, "dpa": None},
        "license": "test only",
        **blocks[fetch],
        **over,
    }
    return DocumentManifest.model_validate(data)


class FakeSource:
    def __init__(self, items, locator="fake"):
        self.items = items
        self.locator = locator

    def files(self):
        yield from self.items


def _file(name, data=None, size=None, url=None, error=None):
    def fetch():
        if error:
            raise error
        return data

    return SourceFile(rel_path=name, fetch=fetch, size=size, url=url)


def test_end_to_end_from_a_local_folder(tmp_path, bridge, pdf_bytes):
    folder = tmp_path / "src"
    folder.mkdir()
    (folder / "a.pdf").write_bytes(pdf_bytes)
    (folder / "b.pdf").write_bytes(pdf_bytes + b"% another report\n")
    (folder / "copy-of-a.pdf").write_bytes(pdf_bytes)  # same bytes, another name
    (folder / "notes.txt").write_bytes(b"not wanted")
    (folder / "fake.pdf").write_bytes(b"<html>a login page</html>")
    manifest = _manifest(local={"path": str(folder)})

    summary = _connector(manifest, bridge).run()
    assert (summary.listed, summary.matched) == (5, 4)
    # three published, but the copy shares a's stored object
    assert (summary.published, summary.new_objects, summary.rejected) == (3, 2, 1)
    assert summary.problems == [
        ("fake.pdf", "content does not match the declared formats ['pdf']")
    ]
    messages = bridge.publisher.read("raw_data")
    assert {m["value"]["message_id"] for m in messages} == {
        "local:src/a.pdf",
        "local:src/b.pdf",
        "local:src/copy-of-a.pdf",
    }

    again = _connector(manifest, bridge).run()
    assert (again.published, again.duplicates, again.rejected) == (0, 3, 1)
    assert any(line.startswith("duplicates      3") for line in again.lines())


def test_a_file_name_the_file_system_could_not_decode_does_not_stop_the_run(
    tmp_path, bridge, pdf_bytes
):
    """Linux hands a non-UTF-8 name back as lone surrogates. The envelope is JSON, so building
    its text raised inside the bridge, after the File Store write and before the receipt, and the
    run died at that file on every pull: nothing sorted after it was ever ingested. The path is
    spelt with escapes instead, the same every time, and the file goes through like any other.
    """
    folder = tmp_path / "src"
    folder.mkdir()
    try:
        (folder / "report\udcff.pdf").write_bytes(pdf_bytes)
    except OSError as exc:  # a file system that refuses the name cannot meet the bug
        pytest.skip(f"the file system refuses such a name: {exc}")
    (folder / "z.pdf").write_bytes(pdf_bytes + b"% after it in the listing\n")
    summary = _connector(_manifest(local={"path": str(folder)}), bridge).run()
    assert (summary.published, summary.rejected, summary.skipped) == (2, 0, 0)
    ids = sorted(m["value"]["message_id"] for m in bridge.publisher.read("raw_data"))
    assert ids == ["local:src/report\\udcff.pdf", "local:src/z.pdf"]
    # Stable: the same name spells the same id on the next pull.
    assert (
        _connector(_manifest(local={"path": str(folder)}), bridge).run().duplicates == 2
    )


def test_the_counts_are_the_numbers_only(tmp_path, bridge, pdf_bytes):
    folder = tmp_path / "src"
    folder.mkdir()
    (folder / "a.pdf").write_bytes(pdf_bytes)
    (folder / "fake.pdf").write_bytes(b"not a pdf")
    summary = _connector(_manifest(local={"path": str(folder)}), bridge).run()
    assert summary.counts() == {
        "listed": 2,
        "matched": 2,
        "published": 1,
        "held": 0,
        "duplicates": 0,
        "rejected": 1,
        "too_large": 0,
        "skipped": 0,
        "refused": 0,
        "new_objects": 1,
        "new_bytes": len(pdf_bytes),
    }
    assert "fake.pdf" not in str(summary.counts())


def test_limit_and_max_files_cap_the_run(bridge, pdf_bytes):
    items = [_file(f"{n}.pdf", pdf_bytes + bytes([n])) for n in range(5)]
    assert _connector(_manifest(), bridge, FakeSource(items)).run(limit=2).matched == 2
    capped = _manifest(max_files=1)
    assert _connector(capped, bridge, FakeSource(items)).run(limit=3).matched == 1


def test_a_file_listed_over_the_cap_is_not_downloaded_and_leaves_a_receipt(bridge):
    items = [_file("big.pdf", size=10, error=AssertionError("must not fetch"))]
    summary = _connector(_manifest(max_file_bytes=5), bridge, FakeSource(items)).run()
    assert (summary.too_large, summary.skipped, summary.published) == (1, 0, 0)
    assert summary.problems == [("big.pdf", "10 bytes; the cap is 5")]
    [receipt] = bridge.receipts.recent(tenant_id=TENANT)
    assert (receipt.status, receipt.payload_ref, receipt.envelope) == (
        ReceiptStatus.TOO_LARGE,
        None,
        None,
    )
    assert bridge.publisher.read("raw_data") == []


def test_a_file_over_the_bridges_cap_is_too_large_not_rejected(
    tmp_path, receipts, pdf_bytes
):
    """NICKEL_MAX_FILE_BYTES holds it back for now: the receipt says too_large and the file goes
    through once the cap is raised. The counts say too_large, as for a file over the cap at the
    source, not rejected, which is final (empty, or not the declared format), nor skipped.
    """
    bridge = IngestBridge(
        tenant_id=TENANT,
        store=LocalFileStore(tmp_path / "fs"),
        publisher=JsonlOutbox(tmp_path / "out"),
        receipts=receipts,
        max_bytes=10,
    )
    items = [_file("big.pdf", pdf_bytes)]  # no size known up front: the bridge decides
    summary = _connector(_manifest(), bridge, FakeSource(items)).run()
    assert (summary.too_large, summary.skipped, summary.rejected) == (1, 0, 0)
    assert summary.problems == [
        ("big.pdf", f"payload is {len(pdf_bytes)} bytes; the cap is 10")
    ]
    [receipt] = receipts.recent(tenant_id=TENANT)
    assert receipt.status is ReceiptStatus.TOO_LARGE


def test_one_failed_download_does_not_stop_the_run(bridge, pdf_bytes):
    items = [
        _file("broken.pdf", error=FetchError("timeout")),
        _file("ok.pdf", pdf_bytes),
    ]
    summary = _connector(_manifest(), bridge, FakeSource(items)).run()
    assert (summary.skipped, summary.published) == (1, 1)
    assert summary.problems == [("broken.pdf", "fetch failed: timeout")]


def test_a_publish_failure_aborts_the_run(tmp_path, receipts, pdf_bytes):
    class DownPublisher:
        def publish(self, topic, key, value, **kw):
            raise PublishError("broker down")

    bridge = IngestBridge(
        tenant_id=TENANT,
        store=LocalFileStore(tmp_path / "fs"),
        publisher=DownPublisher(),
        receipts=receipts,
    )
    items = [_file("a.pdf", pdf_bytes), _file("b.pdf", pdf_bytes + b"x")]
    summary = _connector(_manifest(), bridge, FakeSource(items)).run()
    assert summary.aborted.startswith("publish failed at a.pdf")
    assert summary.error == "publish failed; the pull stopped"  # no file name
    assert summary.matched == 1
    assert any(line.startswith("ABORTED") for line in summary.lines())


def test_http_items_are_identified_by_their_url(bridge, pdf_bytes):
    url = "https://lab.example/reports/cbc.pdf"
    items = [_file("cbc.pdf", pdf_bytes, url=url)]
    _connector(_manifest("http"), bridge, FakeSource(items, "http")).run()
    [message] = bridge.publisher.read("raw_data")
    assert message["value"]["message_id"] == url
    assert message["value"]["source_meta"]["url"] == url


def test_the_envelope_carries_the_manifest_fetch_route_and_mode_and_the_run_id(
    bridge, pdf_bytes
):
    manifest = _manifest("http", route="L3")
    items = [_file("cbc.pdf", pdf_bytes, url="https://lab.example/a.pdf")]
    trace = "0192a3b40000700080000000000000ef"
    summary = _connector(
        manifest, bridge, FakeSource(items, "http"), trace_id=trace
    ).run()
    [message] = bridge.publisher.read("raw_data")
    envelope = message["value"]
    # fetch is the transport (what v0.1 called route); route is the lab route the manifest gives.
    assert (envelope["fetch"], envelope["route"], envelope["mode"]) == (
        "http",
        "L3",
        "pull",
    )
    assert envelope["trace_id"] == trace == summary.trace_id
    assert f"trace_id        {trace}" in summary.lines()
    # A run with no job to name it mints its own id.
    fresh = _connector(manifest, bridge, FakeSource(items, "http"))
    assert re.fullmatch(r"[0-9a-f]{32}", fresh.trace_id)
    assert fresh.trace_id != trace


@pytest.mark.parametrize(
    "fetch, kind",
    [
        ("local", LocalDirSource),
        ("kaggle", KaggleSource),
        ("huggingface", HuggingFaceSource),
        ("http", HttpSource),
    ],
)
def test_each_fetch_builds_its_source_with_the_cap(fetch, kind):
    source = build_source(_manifest(fetch), max_bytes=7)
    assert isinstance(source, kind)
    assert source.max_bytes == 7


def test_a_manifest_without_a_source_block_cannot_build_a_source():
    broken = _manifest().model_copy(update={"local": None})
    with pytest.raises(ValueError, match="no source block"):
        build_source(broken, max_bytes=7)


def test_the_cap_in_force_is_the_smaller_of_the_manifest_and_the_service(bridge):
    """The manifest's max_file_bytes has no upper bound, and the bridge's NICKEL_MAX_FILE_BYTES
    check comes only once the whole file is in memory: the smaller of the two applies before the
    download (a listed size) and at the source."""
    items = [_file("big.pdf", size=20, error=AssertionError("must not fetch"))]
    manifest = _manifest("http", max_file_bytes=50)
    tighter = _connector(manifest, bridge, FakeSource(items), max_file_bytes=10)
    assert tighter.max_file_bytes == 10
    summary = tighter.run()
    assert (summary.too_large, summary.published) == (1, 0)
    assert summary.problems == [("big.pdf", "20 bytes; the cap is 10")]
    assert _connector(manifest, bridge, max_file_bytes=10).source.max_bytes == 10
    looser = _connector(manifest, bridge, max_file_bytes=1000)
    assert looser.max_file_bytes == looser.source.max_bytes == 50


def test_the_environment_and_the_cap_are_required_arguments(bridge):
    """The environment a manifest's phase must clear and the service's size cap come from the
    caller (the handler passes the service's settings): there is no settings global to fall back
    on (tests/test_config_and_logging.py), so a caller that forgets one fails at once instead of
    running on import-time values.
    """
    with pytest.raises(TypeError, match="environment"):
        DocumentConnector(_manifest(), bridge, FakeSource([]), max_file_bytes=7)
    with pytest.raises(TypeError, match="max_file_bytes"):
        DocumentConnector(
            _manifest(), bridge, FakeSource([]), environment="development"
        )


def test_problem_notes_are_capped():
    summary = RunSummary(source="s")
    for n in range(15):
        summary.note(f"{n}.pdf", "bad")
    assert len(summary.problems) == 10


def test_a_pull_racing_another_pull_of_the_same_source_goes_on(
    tmp_path, receipts, pdf_bytes
):
    class Interleaving:
        """The first ``add`` first lets a rival pull of the same item run to completion."""

        def __init__(self, inner, rival):
            self._inner, self._rival = inner, rival

        def __getattr__(self, name):
            return getattr(self._inner, name)

        def add(self, receipt):
            if self._rival is not None:
                rival, self._rival = self._rival, None
                rival()
            return self._inner.add(receipt)

    items = [_file("a.pdf", pdf_bytes), _file("b.pdf", pdf_bytes + b"x")]
    store = LocalFileStore(tmp_path / "fs")
    outbox = JsonlOutbox(tmp_path / "out")
    rival = _connector(
        _manifest(),
        IngestBridge(
            tenant_id=TENANT, store=store, publisher=outbox, receipts=receipts
        ),
        FakeSource(items),
    )
    racing = IngestBridge(
        tenant_id=TENANT,
        store=store,
        publisher=outbox,
        receipts=Interleaving(receipts, lambda: rival.run(limit=1)),
    )
    summary = _connector(_manifest(), racing, FakeSource(items)).run()
    assert summary.aborted is None
    assert (summary.published, summary.duplicates) == (1, 1)
    assert len(outbox.read("raw_data")) == 2


def test_a_refused_download_is_counted_apart_from_a_failed_one(bridge, pdf_bytes):
    items = [
        _file("evil.pdf", error=RefusedError("refused https://cdn.other: not a host")),
        _file("broken.pdf", error=FetchError("timeout")),
        _file("ok.pdf", pdf_bytes),
    ]
    summary = _connector(_manifest(), bridge, FakeSource(items)).run()
    assert (summary.refused, summary.skipped, summary.published) == (1, 1, 1)
    assert summary.counts()["refused"] == 1
    assert summary.problems[0] == (
        "evil.pdf",
        "refused https://cdn.other: not a host",
    )
    assert any(line.startswith("refused         1") for line in summary.lines())


def test_shadow_mode_stores_and_receipts_but_publishes_nothing(bridge, pdf_bytes):
    items = [_file("a.pdf", pdf_bytes), _file("b.pdf", pdf_bytes + b"x")]
    shadow = _connector(_manifest(shadow=True), bridge, FakeSource(items)).run()
    assert (shadow.held, shadow.published, shadow.new_objects) == (2, 0, 2)
    assert shadow.counts()["held"] == 2
    assert bridge.publisher.read("raw_data") == []
    held = list(
        bridge.receipts.in_status(ReceiptStatus.HELD, {"test-docs"}, tenant_id=TENANT)
    )
    assert [r.detail for r in held] == ["held for shadow mode"] * 2
    assert all(bridge.store.get(r.payload_ref).startswith(b"%PDF-") for r in held)
    assert any(line.startswith("held            2") for line in shadow.lines())
    # Still in shadow: the same items are held again, nothing is stored or published.
    again = _connector(_manifest(shadow=True), bridge, FakeSource(items)).run()
    assert (again.held, again.new_objects) == (2, 0)
    # Enabled: the held items go out under the ingest ids they were held under.
    enabled = _connector(_manifest(shadow=False), bridge, FakeSource(items)).run()
    assert (enabled.published, enabled.held, enabled.new_objects) == (2, 0, 0)
    published = {m["value"]["ingest_id"] for m in bridge.publisher.read("raw_data")}
    assert published == {r.ingest_id for r in held}
    assert bridge.receipts.counts(tenant_id=TENANT) == {"published": 2}


def test_a_dev_manifest_is_refused_in_production(bridge, pdf_bytes):
    items = [_file("a.pdf", pdf_bytes)]
    refused = _connector(
        _manifest(phase="dev"), bridge, FakeSource(items), environment="production"
    ).run()
    assert refused.aborted == (
        "manifest phase 'dev' is not cleared for the production environment"
    )
    assert refused.error == refused.aborted  # content-free: a job record keeps it
    assert (refused.listed, refused.published) == (0, 0)
    assert bridge.publisher.read("raw_data") == []
    cleared = _connector(
        _manifest(phase="prod"), bridge, FakeSource(items), environment="production"
    ).run()
    assert cleared.published == 1


@pytest.mark.parametrize("retention", ["forever", "90d"])
def test_retention_comes_from_the_manifest(bridge, pdf_bytes, retention):
    """The object class stays documents whatever the retention; the value itself is recorded on
    the stored object's sidecar, where the File Store lifecycle rule reads it."""
    manifest = _manifest(raw_retention=retention)
    connector = _connector(manifest, bridge, FakeSource([_file("a.pdf", pdf_bytes)]))
    assert connector.context.retention_class is manifest.retention_class
    assert connector.context.raw_retention == manifest.raw_retention == retention
    connector.run()
    [message] = bridge.publisher.read("raw_data")
    assert message["value"]["retention_class"] == "documents"
    [receipt] = bridge.receipts.recent(tenant_id=TENANT)
    assert bridge.store.metadata(receipt.payload_ref)["raw_retention"] == retention
