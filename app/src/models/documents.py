"""Documents as the API shows them: the ingest receipt, and nothing of the content.

Not the receipt's ``message_id``: for a file source it holds the path at the source, and a clinic's
file names can carry patient names. The ingest id and the payload hash identify the item. A pull's
too_large marker has no payload hash (the bytes were never read): its ``payload_sha256`` is null,
not the surrogate key the receipt is stored under (``Receipt.is_marker``).
"""

from __future__ import annotations

from pydantic import BaseModel

from src.db.receipts import Receipt, ReceiptStatus


class DocumentResponse(BaseModel):
    ingest_id: str
    tenant_id: str
    source: str
    payload_sha256: str | None
    status: ReceiptStatus
    received_at: str
    updated_at: str
    payload_ref: str | None
    detail: str | None

    @classmethod
    def of(cls, receipt: Receipt) -> DocumentResponse:
        return cls(
            ingest_id=receipt.ingest_id,
            tenant_id=receipt.tenant_id,
            source=receipt.source,
            payload_sha256=None if receipt.is_marker else receipt.payload_sha256,
            status=receipt.status,
            received_at=receipt.received_at,
            updated_at=receipt.updated_at,
            payload_ref=receipt.payload_ref,
            detail=receipt.detail,
        )


class DocumentList(BaseModel):
    documents: list[DocumentResponse]
