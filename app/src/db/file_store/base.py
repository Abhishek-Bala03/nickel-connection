"""The File Store interface. LocalFileStore implements it for development; RustFS (S3) comes later behind
the same interface."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol


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
