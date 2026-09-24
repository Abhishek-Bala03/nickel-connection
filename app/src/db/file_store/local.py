"""Content-addressed, write-once File Store on local disk, for development and tests.

``S3FileStore`` is the same interface over the S3 API (``NICKEL_FILE_STORE=s3``), and this store
applies its contract: ``put`` refuses bytes the key does not name and metadata the S3 store would
refuse (``base.encode_metadata``), ``get`` checks the bytes against the key's sha256 and takes
``max_bytes``, and a missing object is a MissingObjectError that names no path. Metadata sits next
to each object in a ``.meta.json`` sidecar, the way S3 keeps it on the object.

Both files are written atomically (a temp file next to the target, fsync, then rename), sidecar first
and object second. A crash between the two leaves a sidecar with no object, which the next ``put``
simply writes again; the other order stranded an object whose ``put`` returned False for ever and
whose ``metadata()`` raised.

The directory entries are made durable too, on POSIX: the directory is fsynced after each rename
and after each directory ``mkdir`` created (its entry lives in its parent). Without that, a power
loss after ``put`` returned can undo the rename, and because a published receipt short-circuits a
re-pull to DUPLICATE before ``put``, the object would never be written again and ``payload_ref``
would point at nothing. Windows cannot open a directory for fsync, and NTFS journals its metadata
itself, so it is skipped there.
"""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .base import FileStoreError, MissingObjectError, encode_metadata, key_parts

META_SUFFIX = ".meta.json"
# Whether directory entries need an explicit fsync here (POSIX). The tests set both values.
_FSYNC_DIRECTORIES = os.name == "posix"


def _fsync_directory(path: Path) -> None:
    """Make the directory's entries durable: after a rename into it, or once a sub-directory was
    created in it."""
    if not _FSYNC_DIRECTORIES:
        return
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _make_directory(path: Path) -> None:
    """``mkdir -p``, then fsync the parent of every directory it created, top-down: each new
    entry is durable before the one inside it."""
    created = []
    probe = path
    while not probe.is_dir():
        created.append(probe)
        probe = probe.parent
    path.mkdir(parents=True, exist_ok=True)
    for directory in reversed(created):
        _fsync_directory(directory.parent)


def _write_atomically(path: Path, data: bytes) -> None:
    """The whole file or none of it: readers never see a partial file, and the bytes are on disk
    before the name is (a crash cannot leave an empty or half-written file under the final name).
    The rename itself is then made durable (``_fsync_directory``).
    """
    # A short random suffix: Windows paths are capped at 260 characters.
    tmp = path.with_name(f"{path.name}.{uuid.uuid4().hex[:12]}.tmp")
    done = False
    try:
        with tmp.open("wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
        done = True
    finally:
        if not done:
            tmp.unlink(missing_ok=True)
    _fsync_directory(path.parent)


class LocalFileStore:
    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    def _path(self, key: str) -> Path:
        return self.root.joinpath(*key_parts(key))

    def put(
        self, key: str, data: bytes, *, content_type: str, metadata: Mapping[str, str]
    ) -> bool:
        path = self._path(key)
        if hashlib.sha256(data).hexdigest() != key_parts(key)[-1]:
            raise ValueError("the bytes are not the ones their key names")
        encode_metadata(metadata)  # what S3 would refuse, refused here too
        if path.is_file():
            return False
        _make_directory(path.parent)
        meta = {"content_type": content_type, "bytes": len(data), **dict(metadata)}
        # Sidecar first, object second: see the module docstring.
        _write_atomically(
            path.with_name(path.name + META_SUFFIX),
            json.dumps(meta, indent=2, sort_keys=True).encode("utf-8"),
        )
        _write_atomically(path, data)
        return True

    def get(self, key: str, *, max_bytes: int | None = None) -> bytes:
        path = self._path(key)
        try:
            if max_bytes is not None and path.stat().st_size > max_bytes:
                raise FileStoreError(
                    f"file store: get: over the {max_bytes} bytes asked"
                )
            data = path.read_bytes()
        except FileNotFoundError:  # its message names the path
            raise MissingObjectError("file store: get: no such object") from None
        if hashlib.sha256(data).hexdigest() != key_parts(key)[-1]:
            raise FileStoreError(
                "file store: get: the bytes are not the ones their key names"
            )
        return data

    def metadata(self, key: str) -> dict[str, Any]:
        path = self._path(key)
        try:
            text = path.with_name(path.name + META_SUFFIX).read_text(encoding="utf-8")
        except FileNotFoundError:  # its message names the path
            raise MissingObjectError("file store: metadata: no such object") from None
        return json.loads(text)
