"""The File Store interface, the errors a store raises, and the rules every store applies: keys, and
object metadata.

``LocalFileStore`` keeps the originals on local disk (development and tests). ``S3FileStore`` keeps
them behind an S3 API: SeaweedFS in development, and in production the object store the
SeaweedFS-vs-RustFS bake-off picks, through the same code (an endpoint and credentials apart).
``open_file_store`` picks one from the settings (``NICKEL_FILE_STORE``). Both apply the same rules,
so metadata the S3 store would refuse is refused on a laptop too, not first in production.

An object is content-addressed and shared by every receipt that names its bytes, so its metadata
holds facts about the content only (its sha256; the store adds its type and size). Who sent it,
under which message id and for how long it is kept belong to each receipt: expiry keeps an object
for the longest ``raw_retention`` among the receipts that point at it (issue #6).
"""

from __future__ import annotations

import base64
import re
from collections.abc import Mapping
from typing import Any, Protocol

# The user metadata S3 takes: names and values, at most 2 KB in all.
METADATA_LIMIT = 2048
# What S3 keeps of a metadata name: it lower-cases names, so only lower-case ones come back the same.
_NAME = re.compile(r"[a-z0-9][a-z0-9_-]*")
# Printable ASCII with single spaces inside and none at either end: a request signature's canonical
# form trims and collapses the spaces of a header value, so any other value is encoded.
_PLAIN = re.compile(r"[!-~]+(?: [!-~]+)*")


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


def _encode(value: str) -> str:
    """A metadata value as S3 carries it: as it is when plain printable ASCII without ``=?``,
    otherwise an RFC 2047 encoded word, ``=?UTF-8?B?<base64>?=`` (the form AWS S3 itself uses).
    """
    if _PLAIN.fullmatch(value) and "=?" not in value:
        return value
    encoded = base64.b64encode(value.encode("utf-8")).decode("ascii")
    return f"=?UTF-8?B?{encoded}?="


def encode_metadata(metadata: Mapping[str, str]) -> dict[str, str]:
    """The metadata as S3 carries it (``_encode``), after the rules every store applies: names S3
    keeps as they are (a ValueError before anything is sent), and at most ``METADATA_LIMIT`` bytes
    once encoded (a FileStoreError: a store that enforces S3's limit would refuse the object).
    """
    for name in metadata:
        if not _NAME.fullmatch(name):
            raise ValueError(
                f"metadata name {name!r} is not lower-case ASCII letters, digits, _ and -"
            )
    encoded = {name: _encode(value) for name, value in metadata.items()}
    size = sum(len(name) + len(value) for name, value in encoded.items())
    if size > METADATA_LIMIT:
        raise FileStoreError(
            f"file store: put: {size} bytes of metadata, over the {METADATA_LIMIT} S3 takes"
        )
    return encoded


class FileStore(Protocol):
    def put(
        self, key: str, data: bytes, *, content_type: str, metadata: Mapping[str, str]
    ) -> bool:
        """Write the object if it is not there yet and return True; return False if it already was.

        Objects are immutable: an existing key is never overwritten. Keys are content-addressed, so an
        existing key already holds these exact bytes, and ``put`` refuses bytes the key does not
        name. ``metadata`` holds facts about the content only (the module docstring).
        """
        ...

    def get(self, key: str, *, max_bytes: int | None = None) -> bytes:
        """The object's bytes, checked against the sha256 the key names: what the Document worker
        reads by the envelope's ``payload_ref``. An object over ``max_bytes`` is refused.
        """
        ...

    def metadata(self, key: str) -> dict[str, Any]:
        """What ``put`` stored with the object: its ``content_type``, its size in ``bytes`` and
        the metadata it was given, each value exactly as given."""
        ...
