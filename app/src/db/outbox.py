"""Publisher: where envelopes go. Kafka once PL-05 lands; a local JSONL outbox per topic until then.

One topic: ``raw_data``, every original the connection layer received. The connection layer ends at
Kafka, so this service publishes nothing else; what a consumer makes of a message (the Document
worker's staging rows, for one) goes on the Transform layer's own topics and tables.

Delivery guarantee: at-least-once, with a stable message id. ``publish`` returns only once the message
is durable, and the caller records that on its receipt afterwards; a crash between the two leaves a
durable message whose receipt still says pending, so the next run publishes it again. A repeat
carries the same id (the envelope's ``ingest_id``), the same ``idempotency_key`` and its attempt
number, and consumers dedupe on it (the Transform layer's Dispatcher drops repeats), which is what
Kafka needs anyway. The publisher does not look for repeats itself: a scan of the file before each
append was a check-then-act with no lock across processes, and it read the whole file, which grows
for ever. A message is never lost (nothing is recorded as published before it is durable) and never
published twice unnoticed (a repeat says what it is).
"""

from __future__ import annotations

import errno
import json
import os
import re
import time
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Protocol

from src.logging import logger

RAW_DATA_TOPIC = "raw_data"
_TOPIC = re.compile(r"^[A-Za-z0-9._-]{1,200}$")
# How long a publish waits for the outbox lock before it fails: a PublishError, so the receipt says
# publish_failed and the relay tries again later. A holder that stalls (a stopped command-line run,
# an fsync stuck on a network volume) must not hang the one job worker while the API fills to 429.
LOCK_TIMEOUT_S = 30.0
# A busy lock is polled at this interval: both OS locks are taken without blocking and tried again.
_LOCK_RETRY_S = 0.005
# The errnos msvcrt.locking gives for a locking violation, i.e. another process holds the byte
# (EACCES for LK_NBLCK; EDEADLOCK for the blocking modes). Only those are worth waiting out.
_LOCKED_ERRNOS = frozenset({errno.EACCES, errno.EDEADLOCK})


class PublishError(RuntimeError):
    """The message could not be durably published, so nothing downstream will see it."""


class Publisher(Protocol):
    def publish(
        self,
        topic: str,
        key: str,
        value: Mapping[str, Any],
        *,
        message_id: str | None = None,
    ) -> None:
        """Return only once the message is durable (Kafka: acks=all). Raise PublishError otherwise.

        ``message_id`` is the message's own id (the envelope's ingest_id, the same on a retry). It
        travels with the message, so a consumer can drop a repeat; the publisher appends it again.
        It is not the envelope's ``idempotency_key``, the sha256 dedup key inside ``value``.
        """
        ...


def _lock_functions() -> tuple[Callable[[int], bool], Callable[[int], None]]:
    """(try_lock, unlock) for an exclusive lock across processes on a file descriptor.
    ``try_lock`` takes the lock and returns True, or returns False at once while another holder
    has it; anything else (a bad descriptor, a file system without locks) never clears, so it is
    raised and the publish fails as a PublishError at once. ``fcntl.flock`` with
    ``LOCK_EX | LOCK_NB`` on POSIX, ``msvcrt.locking`` of the first byte with ``LK_NBLCK`` on
    Windows: neither blocks, so the wait has a deadline on both (``_acquire``). Chosen when the
    module loads; the tests inject either module, so both sides run on any OS.
    """
    try:
        import fcntl
    except ImportError:  # Windows
        import msvcrt

        def try_lock(fd: int) -> bool:
            try:
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            except OSError as exc:
                if exc.errno not in _LOCKED_ERRNOS:
                    raise
                return False  # another process holds the byte
            return True

        def unlock(fd: int) -> None:
            msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)

        return try_lock, unlock

    def try_lock(fd: int) -> bool:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:  # EWOULDBLOCK: another holder has it
            return False
        return True

    def unlock(fd: int) -> None:
        fcntl.flock(fd, fcntl.LOCK_UN)

    return try_lock, unlock


_TRY_LOCK, _UNLOCK = _lock_functions()


def _acquire(fd: int) -> None:
    """Take the lock, waiting for another holder ``LOCK_TIMEOUT_S`` at most. Past that, a
    TimeoutError (an OSError, so the publish fails as a content-free PublishError)."""
    deadline = time.monotonic() + LOCK_TIMEOUT_S
    while not _TRY_LOCK(fd):
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"the outbox lock was not free within {LOCK_TIMEOUT_S:g} seconds"
            )
        time.sleep(_LOCK_RETRY_S)


@contextmanager
def _exclusive(lock_path: Path) -> Iterator[None]:
    """Hold the lock file exclusively across processes for the block; waits for another holder
    ``LOCK_TIMEOUT_S`` at most. The file stays empty: only its lock matters, and closing it
    releases the lock too.
    """
    with lock_path.open("ab") as handle:
        _acquire(handle.fileno())
        try:
            yield
        finally:
            _UNLOCK(handle.fileno())


def _torn_tail(path: Path) -> bool:
    """Whether the file ends mid-line: a writer crashed, or the disk filled, with a line half out.
    An empty or missing file has no tail."""
    try:
        with path.open("rb") as handle:
            size = handle.seek(0, os.SEEK_END)
            handle.seek(max(size - 1, 0))
            return handle.read(1) not in (b"", b"\n")
    except FileNotFoundError:
        return False


def _parse(raw: bytes) -> dict[str, Any] | None:
    """The record on one line; None for a blank line, or for a torn one. A torn line is what a
    crashed writer left, sealed onto a line of its own by the next append: its publish never
    returned, so its message was never acknowledged, its receipt is still open, and the next run
    publishes it again under the same id. Nothing acknowledged is skipped.
    """
    if not raw.strip():
        return None
    try:
        record = json.loads(raw)
    except ValueError:
        record = None
    if not isinstance(record, dict):
        logger.warning("the outbox holds a torn line of %d bytes; skipped", len(raw))
        return None
    return record


class JsonlOutbox:
    """One JSON line per message per topic, appended under the topic's lock (``<topic>.jsonl.lock``,
    exclusive across processes, waited for ``LOCK_TIMEOUT_S`` at most), flushed and fsynced before
    ``publish`` returns. A repeat of a
    message is appended again, under the same ``id`` (at-least-once). A line a crashed writer left
    half out is sealed by the next append (a newline first), so it stays a fragment readers skip
    instead of becoming the head of the next record.
    """

    def __init__(self, directory: Path) -> None:
        self.directory = Path(directory)

    def _path(self, topic: str) -> Path:
        if not _TOPIC.match(topic):
            raise ValueError(f"invalid topic name: {topic!r}")
        return self.directory / f"{topic}.jsonl"

    def publish(
        self,
        topic: str,
        key: str,
        value: Mapping[str, Any],
        *,
        message_id: str | None = None,
    ) -> None:
        path = self._path(topic)
        record: dict[str, Any] = {"topic": topic, "key": key}
        if message_id is not None:
            record["id"] = message_id
        record["value"] = dict(value)
        line = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
        try:
            self.directory.mkdir(parents=True, exist_ok=True)
            # One writer at a time: the torn-tail check, the append and the fsync happen under the
            # lock, so two processes cannot interleave their bytes or both seal the same tail.
            with _exclusive(path.with_name(path.name + ".lock")):
                # A torn line left by a crashed writer is sealed first (see ``_parse``): without the
                # newline this record would be glued to it, unreadable, while its receipt says published.
                prefix = b"\n" if _torn_tail(path) else b""
                with path.open("ab") as handle:  # binary: the same bytes on every OS
                    handle.write(prefix + line.encode("utf-8") + b"\n")
                    handle.flush()
                    os.fsync(handle.fileno())
        except OSError as exc:
            # The OS error's text names the file (``str(exc)`` quotes the path): the log gets its
            # type and the OS's reason (``strerror``: "Permission denied", "No space left on
            # device"), never a path. The error's text is what the receipt keeps as its detail and
            # GET /documents shows: the topic and the type.
            logger.error(
                "could not append to the %s outbox: %s (%s)",
                topic,
                type(exc).__name__,
                exc.strerror or "no OS reason",
            )
            raise PublishError(
                f"could not append to the {topic} outbox: {type(exc).__name__}"
            ) from exc

    def read(self, topic: str) -> list[dict[str, Any]]:
        path = self._path(topic)
        if not path.is_file():
            return []
        with path.open("rb") as handle:
            records = [_parse(raw) for raw in handle]
        return [record for record in records if record is not None]
