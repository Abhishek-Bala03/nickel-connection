"""File Store: every original, byte for byte, under a content-addressed key (see ``src.utility.keys``),
on local disk or behind an S3 API; ``open_file_store`` opens the one the settings name."""

from src.config import ServiceSettings
from src.db.layout import DataLayout
from src.logging import logger

from .base import FileStore, FileStoreError, MissingObjectError
from .local import LocalFileStore
from .s3 import S3FileStore

__all__ = [
    "FileStore",
    "FileStoreError",
    "LocalFileStore",
    "MissingObjectError",
    "S3FileStore",
    "open_file_store",
]


def open_file_store(settings: ServiceSettings) -> FileStore:
    """The File Store the settings name (``NICKEL_FILE_STORE``): the S3 API for ``s3``, local disk
    under ``NICKEL_DATA_DIR`` otherwise. The S3 store contacts nothing, and checks its endpoint
    again, only when the job first uses it."""
    if settings.s3 is None:
        return LocalFileStore(DataLayout(settings.data_dir).filestore)
    if settings.s3.allow_public_endpoint:
        logger.warning(
            "file store: NICKEL_S3_ALLOW_PUBLIC_ENDPOINT is on, so the S3 endpoint %s was not "
            "held to loopback, private or tailnet addresses",
            settings.s3.endpoint_url,
        )
    return S3FileStore(settings.s3)
