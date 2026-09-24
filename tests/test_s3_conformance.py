"""Live conformance of a real S3 store (a PL-06 bake-off gate): the File Store's write-once rests on
the store honouring ``If-None-Match: *``, and immutability also needs DeleteObject denied (or object
lock) for the service's own identity. moto and the fakes cannot prove either, so these run against a
live store only: CI's ``s3-conformance`` job against compose's SeaweedFS, and the bake-off against
each candidate. Skipped unless ``S3_CONFORMANCE_ENDPOINT`` is set (not a NICKEL_* name: the test
settings clear those).

    S3_CONFORMANCE_ENDPOINT          http://127.0.0.1:8333
    S3_CONFORMANCE_ACCESS_KEY_ID     the service's identity (not an admin one)
    S3_CONFORMANCE_SECRET_ACCESS_KEY
    S3_CONFORMANCE_BUCKET            default originals
    S3_CONFORMANCE_EXPECT_DELETE_DENIED  1 to require it (the bake-off); SeaweedFS in development
                                          lets "Write" delete, so there it is an expected failure
"""

import hashlib
import os
import time
import uuid

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from src.config import S3Settings
from src.db.file_store import S3FileStore

ENDPOINT = os.environ.get("S3_CONFORMANCE_ENDPOINT", "")
pytestmark = pytest.mark.skipif(
    not ENDPOINT, reason="a live S3 store only (S3_CONFORMANCE_ENDPOINT)"
)
TENANT = "0192a3b4-0000-7000-8000-00000000c0f0"


@pytest.fixture
def live():
    settings = S3Settings(
        endpoint_url=ENDPOINT,
        region="us-east-1",
        originals_bucket=os.environ.get("S3_CONFORMANCE_BUCKET", "originals"),
        access_key_id=os.environ["S3_CONFORMANCE_ACCESS_KEY_ID"],
        secret_access_key=os.environ["S3_CONFORMANCE_SECRET_ACCESS_KEY"],
    )
    raw = boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        region_name="us-east-1",
        aws_access_key_id=settings.access_key_id,
        aws_secret_access_key=settings.secret_access_key,
        config=Config(s3={"addressing_style": "path"}, proxies={}),
    )
    # A store just started may still be electing its master: wait until the bucket answers.
    for _ in range(60):
        try:
            raw.head_bucket(Bucket=settings.originals_bucket)
            break
        except (ClientError, BotoCoreError):
            time.sleep(1)
    return S3FileStore(settings), raw, settings.originals_bucket


def _fresh():
    data = f"conformance {uuid.uuid4()}".encode()
    sha = hashlib.sha256(data).hexdigest()
    return f"{TENANT}/documents/{sha[:2]}/{sha}", data


def test_the_store_refuses_a_second_create_of_one_key(live):
    """If-None-Match: * is honoured: the first create lands, the second gets 412 and the bytes and
    metadata of the first stay."""
    store, raw, bucket = live
    key, data = _fresh()
    assert store.put(
        key, data, content_type="application/pdf", metadata={"sha256": "a"}
    )
    assert not store.put(
        key, data, content_type="application/pdf", metadata={"sha256": "b"}
    )
    with pytest.raises(ClientError) as caught:
        raw.put_object(Bucket=bucket, Key=key, Body=b"other", IfNoneMatch="*")
    assert caught.value.response["ResponseMetadata"]["HTTPStatusCode"] == 412
    assert store.get(key) == data
    assert store.metadata(key)["sha256"] == "a"


@pytest.mark.xfail(
    os.environ.get("S3_CONFORMANCE_EXPECT_DELETE_DENIED") != "1",
    reason="SeaweedFS's Write action also deletes: the PL-06 store must deny it (object lock or "
    "a deny policy), and the bake-off runs this with S3_CONFORMANCE_EXPECT_DELETE_DENIED=1",
    strict=False,
)
def test_the_services_identity_cannot_delete_an_original(live):
    store, raw, bucket = live
    key, data = _fresh()
    store.put(key, data, content_type="application/pdf", metadata={})
    try:
        raw.delete_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        assert exc.response["ResponseMetadata"]["HTTPStatusCode"] == 403
    assert store.get(key) == data  # denied, or kept by object lock
