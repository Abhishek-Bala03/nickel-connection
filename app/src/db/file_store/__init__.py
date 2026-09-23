"""File Store: every original, byte for byte, under a content-addressed key (see ``src.utility.keys``)."""

from .base import FileStore
from .local import LocalFileStore

__all__ = ["FileStore", "LocalFileStore"]
