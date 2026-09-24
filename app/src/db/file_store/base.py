"""The File Store interface, the errors a store raises, and the key rule every store applies.

``LocalFileStore`` keeps the originals on local disk (development and tests). ``S3FileStore`` keeps
them behind an S3 API: SeaweedFS in development, and in production the object store the
SeaweedFS-vs-RustFS bake-off picks, through the same code (an endpoint and credentials apart).
``open_file_store`` picks one from the settings (``NICKEL_FILE_STORE``).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol


class FileStoreError(RuntimeError):
    """The File Store could not do what was asked: the store cannot be reached, refused the request,
    or holds no such object. The text is content-free by contract, as a FetchError's is: the
    operation and the error's type, HTTP status or S3 error code; never the key, a path, a URL or a
    credential. A job record keeps it.
    """


class MissingObjectError(FileStoreError):
    """No object under the key: a lost original, not a store that is down."""


def key_parts(key: str) -> list[str]:
    """The key's segments: relative and ``/``-separated, none of them empty, ``.`` or ``..``, and no
    backslash. Keys come from ``src.utility.keys.object_key``; the check keeps any other string from
    climbing out of the store's root (a folder, or a bucket's directory on SeaweedFS).
    """
    parts = key.split("/")
    if (
        not key
        or key.startswith("/")
        or "\\" in key
        or any(part in ("", ".", "..") for part in parts)
    ):
        raise ValueError("invalid object key: relative segments, none empty, . or ..")
    return parts


class FileStore(Protocol):
    def put(
        self, key: str, data: bytes, *, content_type: str, metadata: Mapping[str, str]
    ) -> bool:
        """Write the object if it is not there yet and return True; return False if it already was.

        Objects are immutable: an existing key is never overwritten. Keys are content-addressed, so an
        existing key already holds these exact bytes.
        """
        ...

    def get(self, key: str) -> bytes:
        """The object's bytes: what the Document worker reads by the envelope's ``payload_ref``."""
        ...

    def metadata(self, key: str) -> dict[str, Any]:
        """What ``put`` stored with the object: its ``content_type``, its size in ``bytes`` and
        the metadata it was given, each value exactly as given."""
        ...
