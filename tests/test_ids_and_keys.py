import uuid

import pytest

from src.utility.ids import uuid7
from src.utility.keys import ObjectClass, object_key, parse_tenant_id

TENANT = "0192a3b4-0000-7000-8000-000000000001"
SHA = "ab" * 32


def test_uuid7_has_version_7_and_the_rfc_variant():
    value = uuid7()
    assert value.version == 7
    assert value.variant == uuid.RFC_4122


def test_uuid7_leads_with_the_millisecond_timestamp():
    value = uuid7(now_ms=0x0123456789AB, rand=bytes(10))
    assert value.int >> 80 == 0x0123456789AB
    assert str(value) == "01234567-89ab-7000-8000-000000000000"


def test_uuid7_orders_by_time():
    assert uuid7(now_ms=1_000, rand=b"\xff" * 10) < uuid7(now_ms=1_001, rand=bytes(10))


def test_object_key_is_tenant_first_and_content_addressed():
    assert (
        object_key(TENANT, ObjectClass.DOCUMENTS, SHA) == f"{TENANT}/documents/ab/{SHA}"
    )


def test_object_key_accepts_uuid_objects_and_class_names():
    assert object_key(uuid.UUID(TENANT), "clinical", SHA).startswith(
        f"{TENANT}/clinical/"
    )


@pytest.mark.parametrize("bad", ["clinic-one", "42", "", "0"])
def test_tenant_must_be_a_uuid(bad):
    with pytest.raises(ValueError, match="UUID"):
        parse_tenant_id(bad)


def test_nil_uuid_is_not_a_tenant():
    with pytest.raises(ValueError, match="nil"):
        parse_tenant_id(uuid.UUID(int=0))


def test_parse_tenant_id_keeps_a_uuid_instance():
    value = uuid.UUID(TENANT)
    assert parse_tenant_id(value) is value


@pytest.mark.parametrize("bad", ["AB" * 32, "ab" * 31, "zz" * 32])
def test_sha256_must_be_64_lowercase_hex(bad):
    with pytest.raises(ValueError, match="sha256"):
        object_key(TENANT, ObjectClass.DOCUMENTS, bad)


def test_unknown_object_class_is_rejected():
    with pytest.raises(ValueError):
        object_key(TENANT, "photos", SHA)
