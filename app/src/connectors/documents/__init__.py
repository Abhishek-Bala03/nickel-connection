"""Document connector (NIK-2209-05): bring PDFs from Kaggle, Hugging Face, web links or a folder into the
connection layer: original to the File Store, receipt, envelope to ``raw_data`` for the document worker."""

from .connector import (
    CONNECTOR,
    CONNECTOR_VERSION,
    DocumentConnector,
    RunSummary,
    build_source,
)
from .manifest import MANIFEST_ID_PATTERN, DocumentManifest

__all__ = [
    "CONNECTOR",
    "CONNECTOR_VERSION",
    "MANIFEST_ID_PATTERN",
    "DocumentConnector",
    "DocumentManifest",
    "RunSummary",
    "build_source",
]
