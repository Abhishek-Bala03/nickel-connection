"""File Store object keys.

Multi-tenancy rule (Jack, 18 Sep 2026): the tenant is the first path segment, and every writer calls
``object_key()``. Nothing builds an object key by hand. Originals are content-addressed, so the same
bytes land on the same key within a tenant and are stored once; across tenants they never share a key.
"""

from __future__ import annotations

import re
import uuid
from enum import StrEnum

_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class ObjectClass(StrEnum):
    """What an original is; decides its retention (see the File Store design)."""

    DOCUMENTS = "documents"  # PDFs, scans, C-CDA: kept forever
    CLINICAL = "clinical"  # HL7 v2, FHIR bundles: kept forever
    STREAM = "stream"  # wearable and CGM events: 30-90 days, then the TimescaleDB row is the record
    IMAGING = "imaging"  # DICOM: kept forever, tiered after 12 months


def parse_tenant_id(value: str | uuid.UUID) -> uuid.UUID:
    """A tenant id is a UUID. Never a clinic name or a sequence number, and never the nil UUID."""
    if isinstance(value, uuid.UUID):
        tenant = value
    else:
        try:
            tenant = uuid.UUID(str(value).strip())
        except ValueError as exc:
            raise ValueError(
                "tenant_id must be a UUID (never a clinic name or a sequence number)"
            ) from exc
    if tenant.int == 0:
        raise ValueError("the nil UUID is not a tenant")
    return tenant


def object_key(
    tenant_id: str | uuid.UUID, object_class: ObjectClass | str, sha256: str
) -> str:
    """``<tenant>/<class>/<first two hex>/<sha256>``: tenant first, content-addressed, sharded for listing."""
    tenant = parse_tenant_id(tenant_id)
    klass = ObjectClass(object_class)
    if not _SHA256.match(sha256):
        raise ValueError("sha256 must be 64 lowercase hex characters")
    return f"{tenant}/{klass.value}/{sha256[:2]}/{sha256}"
