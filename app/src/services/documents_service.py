"""Documents: the ingest receipts, metadata only.

The connection layer never serves a document's content, and nothing derived from the content exists
here to serve: the original stays in the File Store for the Transform layer (the Document worker) to
read by the envelope's ``payload_ref``.
"""

from fastapi import APIRouter, Query

from src.db.receipts import ReceiptStatus
from src.handler import documents
from src.models.documents import DocumentList, DocumentResponse
from src.services.dependencies import Settings

documents_router = APIRouter(tags=["documents"])


@documents_router.get("/documents")
def list_documents(
    settings: Settings,
    source: str | None = None,
    status: ReceiptStatus | None = None,
    limit: int = Query(20, ge=1, le=500),
) -> DocumentList:
    """The newest receipts first."""
    receipts = documents.list_documents(
        settings, source=source, status=status, limit=limit
    )
    return DocumentList(documents=[DocumentResponse.of(r) for r in receipts])
