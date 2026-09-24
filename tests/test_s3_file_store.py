"""The S3 File Store (NIK-2209-06): write-once conditional puts, bytes checked against their key,
metadata that comes back exactly as it was put and stays under 2 KB, content-free errors, a deadline
on every call, a client that takes nothing from the environment, and the failure ordering at the
ingest bridge.

S3 is moto's (``mock_aws``) wherever S3's answers matter. Where the wire matters (which host is
contacted, through what, how often, how fast, with which credentials, CA and body encoding), a small
S3 endpoint on a loopback port answers from a script and records what it was asked (``FakeS3``).
"""

import base64
import dataclasses
import hashlib
import http.server
import io
import ipaddress
import json
import logging
import re
import socket
import ssl
import threading
import time
import zlib
from datetime import UTC, datetime, timedelta
from typing import NamedTuple

import boto3
import botocore.retries.standard
import pytest
from botocore.config import Config
from moto import mock_aws
from moto.core import enable_iam_authentication

from src.cli import main
from src.config import S3Settings
from src.db.file_store import FileStoreError, MissingObjectError, S3FileStore
from src.db.file_store import s3 as s3_module
from src.db.outbox import JsonlOutbox
from src.db.receipts import ReceiptStore
from src.handler.ingest import IngestBridge, Outcome, RawItem

TENANT = "0192a3b4-0000-7000-8000-000000000001"
# moto answers for this endpoint (MOTO_S3_CUSTOM_ENDPOINTS); nothing listens on the port.
ENDPOINT = "http://127.0.0.1:8333"
BUCKET = "originals"
ACCESS_KEY = "AKIANICKELSETTINGS01"
SECRET = "nickel-settings-secret-0123456789abcdef"
PDF = b"%PDF-1.4\n1 0 obj<</Type/Catalog>>endobj\ntrailer<</Root 1 0 R>>\n%%EOF\n"


def _sha(data):
    return hashlib.sha256(data).hexdigest()


def _key(data=PDF):
    sha = _sha(data)
    return f"{TENANT}/documents/{sha[:2]}/{sha}"


def _put(store, key=None, data=PDF, **metadata):
    return store.put(
        _key(data) if key is None else key,
        data,
        content_type="application/pdf",
        metadata=metadata or {"sha256": _sha(data)},
    )


@pytest.fixture(autouse=True)
def fresh_clients():
    """Each test builds its own clients: the process keeps one per settings (``s3_client``)."""
    s3_module.s3_client.cache_clear()
    yield
    s3_module.s3_client.cache_clear()


@pytest.fixture
def s3_settings():
    return S3Settings(
        endpoint_url=ENDPOINT,
        region="us-east-1",
        originals_bucket=BUCKET,
        access_key_id=ACCESS_KEY,
        secret_access_key=SECRET,
    )


@pytest.fixture
def aws(monkeypatch):
    """moto's S3 at ENDPOINT with the originals bucket made; yields a plain client to look inside."""
    monkeypatch.setenv("MOTO_S3_CUSTOM_ENDPOINTS", ENDPOINT)
    with mock_aws():
        raw = boto3.client(
            "s3",
            endpoint_url=ENDPOINT,
            region_name="us-east-1",
            aws_access_key_id="moto",
            aws_secret_access_key="moto",
            config=Config(s3={"addressing_style": "path"}),
        )
        raw.create_bucket(Bucket=BUCKET)
        yield raw


@pytest.fixture
def store(aws, s3_settings):
    return S3FileStore(s3_settings)


def _iam_user(*actions):
    """An IAM user whose policy allows ``actions`` on the originals bucket, and a function that
    replaces them; its credentials as S3Settings fields. Only checked under enable_iam_authentication.
    """
    iam = boto3.client("iam", region_name="us-east-1")
    iam.create_user(UserName="nickel-connection")
    key = iam.create_access_key(UserName="nickel-connection")["AccessKey"]

    def allow(*allowed):
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": list(allowed),
                    "Resource": [f"arn:aws:s3:::{BUCKET}", f"arn:aws:s3:::{BUCKET}/*"],
                }
            ],
        }
        iam.put_user_policy(
            UserName="nickel-connection",
            PolicyName="originals",
            PolicyDocument=json.dumps(policy),
        )

    allow(*actions)
    credentials = {
        "access_key_id": key["AccessKeyId"],
        "secret_access_key": key["SecretAccessKey"],
    }
    return credentials, allow


READ_ONLY = ("s3:GetObject", "s3:ListBucket")


@pytest.fixture
def no_backoff(monkeypatch):
    """The client's retries without their sleeps, so a test of four attempts takes no seconds."""
    monkeypatch.setattr(
        botocore.retries.standard.ExponentialBackoff,
        "delay_amount",
        lambda self, context: 0,
    )


# ── A scripted S3 endpoint ───────────────────────────────────────────────────
class Request(NamedTuple):
    method: str
    path: str
    headers: dict  # lower-cased names
    body: bytes  # as S3 would store it: HTTP and aws-chunked encodings undone
    trailers: dict  # the aws-chunked trailers (the CRC32 of the body)


def _aws_unchunk(raw):
    """aws-chunked: ``<hex size>\\r\\n<data>\\r\\n`` ... ``0\\r\\n<trailer>:<value>\\r\\n\\r\\n``."""
    stream = io.BytesIO(raw)
    data = b""
    while size := int(stream.readline().split(b";")[0], 16):
        data += stream.read(size)
        stream.readline()
    trailers = {}
    for line in stream.read().split(b"\r\n"):
        if line:
            name, _, value = line.partition(b":")
            trailers[name.decode().lower()] = value.decode().strip()
    return data, trailers


class FakeS3:
    """An S3 endpoint on a loopback port that answers from a script and records every request.

    - PUT: answers each of ``puts`` in turn (the last one again after that), and 412 to
      ``If-None-Match: *`` when ``exists``. The body is read as sent (Content-Length, or chunked
      transfer with aws-chunked content, which is how botocore puts over https) and decoded.
    - HEAD of an object: ``head``, without a body as S3 answers (``head_headers`` on a 200);
      HEAD of the bucket: ``bucket``. GET: 200 with ``body``.
    - ``drip``: the answer's ``"headers"`` or ``"body"`` come a byte at a time, until closed.
    - ``delay``: every answer waits that long first. ``bad_header``: the PUT's answer carries a
      header line urllib3 cannot parse. ``tls``: a certificate and its key, for https.
    """

    def __init__(
        self,
        *,
        puts=(200,),
        exists=False,
        head=404,
        head_headers=None,
        bucket=200,
        body=PDF,
        drip=None,
        delay=0.0,
        bad_header=False,
        code="SlowDown",
        tls=None,
    ):
        self.requests = []
        self.closed = threading.Event()
        fake = self

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"  # answers Expect: 100-continue, as S3 does

            def _record(self, body=b"", trailers=None):
                headers = {k.lower(): v for k, v in self.headers.items()}
                fake.requests.append(
                    Request(self.command, self.path, headers, body, trailers or {})
                )

            def _read_body(self):
                if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
                    raw = b""
                    while size := int(self.rfile.readline().split(b";")[0], 16):
                        raw += self.rfile.read(size)
                        self.rfile.readline()
                    while self.rfile.readline() not in (b"\r\n", b""):
                        pass
                else:
                    raw = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                if "aws-chunked" in self.headers.get("Content-Encoding", ""):
                    return _aws_unchunk(raw)
                return raw, {}

            def _drip(self, data):
                for i in range(len(data)):
                    if fake.closed.wait(0.05):
                        break
                    self.wfile.write(data[i : i + 1])
                self.close_connection = True  # the client sees the end at once

            def _answer(self, status, headers=None, body=b""):
                time.sleep(delay)
                if drip == "headers":
                    self.wfile.write(f"HTTP/1.1 {status} X\r\n".encode())
                    self._drip(b"X-Pad: " + b"a" * 10_000)
                    return
                self.send_response(status)
                length = 10_000_000 if drip == "body" else len(body)
                self.send_header("Content-Length", str(length))
                for name, value in (headers or {}).items():
                    self.send_header(name, value)
                self.end_headers()
                if drip == "body":
                    self._drip(b"b" * length)
                elif self.command != "HEAD":
                    self.wfile.write(body)

            def _error(self, status, error_code):
                document = (
                    f'<?xml version="1.0" encoding="UTF-8"?><Error><Code>{error_code}</Code>'
                    "<Message>m</Message></Error>"
                ).encode()
                self._answer(status, {"Content-Type": "application/xml"}, document)

            def do_PUT(self):
                self._record(*self._read_body())
                if exists and self.headers.get("If-None-Match") == "*":
                    self._error(412, "PreconditionFailed")
                    return
                status = fake._next_put()
                if status == 200:
                    headers = {"ETag": '"etag"'}
                    if bad_header:  # a second header line, and it has no colon
                        headers["X-Ok"] = "1\r\nNoColonHere"
                    self._answer(200, headers)
                else:
                    self._error(status, code)

            def do_HEAD(self):
                self._record()
                if self.path.count("/") == 1:  # /<bucket>
                    self._answer(bucket)
                else:
                    self._answer(head, head_headers if head == 200 else None)

            def do_GET(self):
                self._record()
                self._answer(200, {"Content-Type": "application/pdf"}, body)

            do_POST = do_DELETE = do_CONNECT = do_GET

            def log_message(self, *args):  # quiet
                pass

        self._puts = list(puts)
        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.server.block_on_close = False
        # Quiet about a client that refuses the TLS handshake (a test wants that).
        self.server.handle_error = lambda request, address: None
        scheme = "http"
        if tls is not None:
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.load_cert_chain(*tls)
            self.server.socket = context.wrap_socket(
                self.server.socket, server_side=True
            )
            scheme = "https"
        self.url = f"{scheme}://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(
            target=self.server.serve_forever,
            kwargs={"poll_interval": 0.05},
            daemon=True,
        ).start()

    def _next_put(self):
        return self._puts.pop(0) if len(self._puts) > 1 else self._puts[0]

    def methods(self):
        return [request.method for request in self.requests]

    def close(self):
        self.closed.set()
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture
def fake_s3():
    made = []

    def make(**kw):
        made.append(FakeS3(**kw))
        return made[-1]

    yield make
    for server in made:
        server.close()


def _store_at(s3_settings, url, **changes):
    return S3FileStore(dataclasses.replace(s3_settings, endpoint_url=url, **changes))


# ── put, get, metadata ───────────────────────────────────────────────────────
def test_put_then_get_and_metadata_return_what_was_stored(store):
    assert _put(store, source="test-docs", sha256=_sha(PDF))
    assert store.get(_key()) == PDF
    assert store.metadata(_key()) == {
        "content_type": "application/pdf",
        "bytes": len(PDF),
        "sha256": _sha(PDF),
        "source": "test-docs",
    }


def test_the_object_is_stored_under_the_key_as_given(store, aws):
    _put(store)
    [listed] = aws.list_objects_v2(Bucket=BUCKET)["Contents"]
    assert listed["Key"] == _key()  # object_key(), unchanged: tenant first, sha256 last


def test_put_is_write_once(store):
    assert _put(store, source="first", sha256=_sha(PDF))
    # A second put of the key keeps the first object and its metadata, whatever it brings.
    assert not _put(store, source="second")
    assert store.get(_key()) == PDF
    assert store.metadata(_key())["source"] == "first"


def test_a_new_key_is_one_conditional_put_path_style(fake_s3, s3_settings):
    endpoint = fake_s3()
    assert _put(_store_at(s3_settings, endpoint.url))
    [request] = endpoint.requests
    assert (request.method, request.path) == ("PUT", f"/{BUCKET}/{_key()}")
    # Created only if the key is not there: atomic write-once, no HEAD first.
    assert request.headers["if-none-match"] == "*"
    assert request.headers["content-type"] == "application/pdf"
    assert request.headers["x-amz-meta-sha256"] == _sha(PDF)
    assert request.body == PDF


def test_a_key_that_is_there_answers_412_and_put_says_so(fake_s3, s3_settings):
    endpoint = fake_s3(exists=True)
    assert not _put(_store_at(s3_settings, endpoint.url))
    assert endpoint.methods() == ["PUT"]


def test_runs_that_put_one_new_key_at_once_all_succeed_and_keep_the_bytes(
    aws, s3_settings
):
    """Every run gets True or False, never an error, and the key holds the bytes; which run's
    metadata is kept is the store's to decide (a real store creates the key exactly once).
    """
    runs = 8
    start = threading.Barrier(runs)
    results = [None] * runs

    def run(i):
        store = S3FileStore(s3_settings)
        start.wait()
        results[i] = _put(store, source=f"run-{i}", sha256=_sha(PDF))

    threads = [threading.Thread(target=run, args=(i,)) for i in range(runs)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert True in results  # at least one run wrote it
    assert set(results) <= {True, False}
    store = S3FileStore(s3_settings)
    assert store.get(_key()) == PDF
    assert store.metadata(_key())["source"] in {f"run-{i}" for i in range(runs)}


def test_put_refuses_bytes_their_key_does_not_name(fake_s3, s3_settings):
    endpoint = fake_s3()
    with pytest.raises(ValueError, match="not the ones their key names"):
        _put(_store_at(s3_settings, endpoint.url), key=_key(), data=b"other bytes")
    assert endpoint.requests == []


def test_get_refuses_bytes_their_key_does_not_name(store, aws):
    """An object replaced behind the S3 API (SeaweedFS's filer takes writes of its own), with its
    metadata or without: the key is the sha256 of the original, and decides."""
    for metadata in ({"sha256": _sha(PDF)}, {}):
        aws.put_object(
            Bucket=BUCKET, Key=_key(), Body=b"not the original", Metadata=metadata
        )
        with pytest.raises(FileStoreError) as caught:
            store.get(_key())
        assert str(caught.value) == (
            "file store: get: the bytes are not the ones their key names"
        )


@pytest.mark.parametrize(
    "key", ["", "/abs", "a/../b", "a//b", "a\\b", "./a", "a/.", "t/documents/ab/abc"]
)
def test_a_key_that_is_not_content_addressed_is_refused_before_anything_is_sent(
    fake_s3, s3_settings, key
):
    endpoint = fake_s3()
    store = _store_at(s3_settings, endpoint.url)
    for call in (
        lambda: _put(store, key=key),
        lambda: store.get(key),
        lambda: store.metadata(key),
    ):
        with pytest.raises(ValueError, match="invalid object key"):
            call()
    assert endpoint.requests == []


# ── Metadata: ASCII, exact, and under 2 KB ──────────────────────────────────
@pytest.mark.parametrize(
    "value",
    [
        "local:docs/a.pdf",
        "kaggle:org/reports/Überweisung für Müller.pdf",
        "huggingface:org/ds@main/日本語/レポート.pdf",
        "https://lab.example/a b.pdf?x=1&y=é",
        "",
        " leading",
        "trailing ",
        "two  spaces",
        "tab\there",
        "line\nfeed",
        "=?UTF-8?B?aMOpbGxv?=",  # a value that looks encoded is encoded, not taken as one
        "plain =? inside",
        "x" * 1000,
    ],
)
def test_metadata_comes_back_exactly_as_it_was_put_and_is_ascii_in_s3(
    store, aws, value
):
    _put(store, message_id=value)
    assert store.metadata(_key())["message_id"] == value
    held = aws.head_object(Bucket=BUCKET, Key=_key())["Metadata"]["message_id"]
    assert held.isascii()
    assert held == value or held.startswith("=?UTF-8?B?")


def test_a_plain_value_is_stored_as_it_is(store, aws):
    _put(store, message_id_sha256=_sha(b"local:docs/a.pdf"), raw_retention="forever")
    held = aws.head_object(Bucket=BUCKET, Key=_key())["Metadata"]
    assert held == {
        "message_id_sha256": _sha(b"local:docs/a.pdf"),
        "raw_retention": "forever",
    }


def test_an_encoded_word_s3_split_or_q_encoded_on_the_way_back_is_decoded(store, aws):
    """AWS S3 decodes an encoded word it is given and encodes the value again on the way back,
    in words of its own choosing: split, even inside a character, or Q-encoded."""
    aws.put_object(
        Bucket=BUCKET,
        Key=_key(),
        Body=PDF,
        Metadata={
            "split": "=?UTF-8?B?aMM=?= =?UTF-8?B?qWxsbw==?=",
            "q": "=?utf-8?q?h=C3=A9llo_world?=",
        },
    )
    metadata = store.metadata(_key())
    assert (metadata["split"], metadata["q"]) == ("héllo", "héllo world")


@pytest.mark.parametrize(
    "value",
    [
        "=?UTF-8?B?not*base64?=",  # base64 that is not
        "=?x-unknown-charset?B?YQ==?=",  # a charset Python has not heard of
        "=?UTF-8?B?/w==?=",  # bytes that are not UTF-8
    ],
)
def test_metadata_that_cannot_be_decoded_is_a_file_store_error(store, aws, value):
    aws.put_object(Bucket=BUCKET, Key=_key(), Body=PDF, Metadata={"message_id": value})
    with pytest.raises(FileStoreError) as caught:
        store.metadata(_key())
    assert str(caught.value) == "file store: metadata: a value that cannot be decoded"
    assert store.get(_key()) == PDF  # get() reads the bytes, not the metadata


def test_metadata_names_come_back_lower_cased(fake_s3, s3_settings):
    """SeaweedFS answers with Go's canonical header names (X-Amz-Meta-Message_id)."""
    endpoint = fake_s3(
        head=200,
        head_headers={
            "Content-Type": "application/pdf",
            "X-Amz-Meta-Message_id_sha256": _sha(b"local:docs/a.pdf"),
            "X-Amz-Meta-Sha256": _sha(PDF),
        },
    )
    metadata = _store_at(s3_settings, endpoint.url).metadata(_key())
    assert metadata == {
        "content_type": "application/pdf",
        "bytes": 0,
        "message_id_sha256": _sha(b"local:docs/a.pdf"),
        "sha256": _sha(PDF),
    }


@pytest.mark.parametrize("name", ["Source", "message id", "sha256!", "_x", "grün", ""])
def test_a_metadata_name_s3_would_change_is_refused_before_anything_is_sent(
    fake_s3, s3_settings, name
):
    endpoint = fake_s3()
    with pytest.raises(ValueError, match="is not lower-case ASCII letters"):
        _put(_store_at(s3_settings, endpoint.url), **{name: "value"})
    assert endpoint.requests == []


@pytest.mark.parametrize(
    ("value", "size"),
    [
        ("u" * 2039, 2049),  # long ASCII: one byte over
        (
            "local:/scans/" + "日" * 600,
            2442,
        ),  # CJK grows under base64: 1813 bytes of UTF-8
    ],
)
def test_metadata_over_2_kb_is_refused_before_anything_is_sent(
    fake_s3, s3_settings, value, size
):
    """S3 takes 2 KB of user metadata (names and values); more is a store-side 400 on some
    stores and an object that cannot be read back on others, so it never leaves."""
    endpoint = fake_s3()
    with pytest.raises(FileStoreError) as caught:
        _put(_store_at(s3_settings, endpoint.url), message_id=value)
    assert str(caught.value) == (
        f"file store: put: {size} bytes of metadata, over the 2048 S3 takes"
    )
    assert endpoint.requests == []


def test_metadata_of_exactly_2_kb_goes(fake_s3, s3_settings):
    endpoint = fake_s3()
    assert _put(_store_at(s3_settings, endpoint.url), message_id="u" * 2038)
    assert len(endpoint.requests[0].headers["x-amz-meta-message_id"]) == 2038


# ── Errors: content-free ─────────────────────────────────────────────────────
def _content_free(error, *secrets):
    text = str(error)
    for secret in (_key(), _sha(PDF), TENANT, *secrets):
        assert secret not in text, secret


def test_a_missing_object_is_a_missing_object_error_that_names_no_key(store):
    for call, operation in ((store.get, "get"), (store.metadata, "metadata")):
        with pytest.raises(MissingObjectError) as caught:
            call(_key())
        assert str(caught.value) == f"file store: {operation}: no such object"
        assert isinstance(caught.value, FileStoreError)
        _content_free(caught.value)


def test_a_missing_bucket_is_not_a_missing_object(aws, s3_settings):
    store = S3FileStore(dataclasses.replace(s3_settings, originals_bucket="never-made"))
    for call, operation in (
        (lambda: _put(store), "put"),
        (lambda: store.get(_key()), "get"),
        (lambda: store.metadata(_key()), "metadata"),
    ):
        with pytest.raises(FileStoreError) as caught:
            call()
        assert str(caught.value) == f"file store: {operation}: HTTP 404 NoSuchBucket"
        assert not isinstance(caught.value, MissingObjectError)


@pytest.mark.parametrize(
    ("bucket", "error"),
    [
        (404, "file store: metadata: HTTP 404 NoSuchBucket"),
        (200, "file store: metadata: no such object"),
        (
            403,
            "file store: metadata: no such object",
        ),  # only a 404 says the bucket is not there
    ],
)
def test_a_head_404_without_a_body_asks_whether_the_bucket_is_there(
    fake_s3, s3_settings, bucket, error
):
    """A HEAD answer has no body to tell a missing key from a missing bucket (moto's has one;
    SeaweedFS's, RustFS's and MinIO's have not): a HeadBucket does."""
    endpoint = fake_s3(head=404, bucket=bucket)
    with pytest.raises(FileStoreError) as caught:
        _store_at(s3_settings, endpoint.url).metadata(_key())
    assert str(caught.value) == error
    assert isinstance(caught.value, MissingObjectError) is (bucket != 404)
    assert [(r.method, r.path) for r in endpoint.requests] == [
        ("HEAD", f"/{BUCKET}/{_key()}"),
        ("HEAD", f"/{BUCKET}"),
    ]


def test_bad_credentials_are_refused_and_never_quoted(aws, s3_settings):
    credentials, _ = _iam_user("s3:*")
    good = dataclasses.replace(s3_settings, **credentials)
    unknown = dataclasses.replace(
        s3_settings,
        access_key_id="AKIAUNKNOWNKEY000000",
        secret_access_key="unknown-secret-0123456789",
    )
    wrong = dataclasses.replace(good, secret_access_key="not-the-secret-0123456789")
    with enable_iam_authentication():
        assert _put(S3FileStore(good))  # the settings' own do work
        with pytest.raises(FileStoreError) as unknown_key:
            _put(S3FileStore(unknown), key=_key(b"other"), data=b"other")
        with pytest.raises(FileStoreError) as wrong_secret:
            S3FileStore(wrong).get(_key())
    assert str(unknown_key.value) == "file store: put: HTTP 403 InvalidAccessKeyId"
    assert str(wrong_secret.value) == "file store: get: HTTP 403 SignatureDoesNotMatch"
    for caught in (unknown_key, wrong_secret):
        _content_free(
            caught.value,
            unknown.access_key_id,
            unknown.secret_access_key,
            good.access_key_id,
            wrong.secret_access_key,
            _key(b"other"),
        )


def test_a_refusal_without_a_body_is_named_by_its_status(fake_s3, s3_settings):
    """A HEAD answer has no body; botocore names its error by the status alone."""
    endpoint = fake_s3(head=403)
    with pytest.raises(FileStoreError) as caught:
        _store_at(s3_settings, endpoint.url).metadata(_key())
    assert str(caught.value) == "file store: metadata: HTTP 403"


def test_an_error_code_that_is_not_an_s3_code_is_dropped(fake_s3, s3_settings):
    """The <Code> is the server's text: kept when it looks like an S3 code, never as free text
    (a forged code could carry a name, or a fake log line, into GET /jobs and the log).
    """
    forged = (
        "AccessDenied for Jane Roe\n2026-09-24 INFO [nickel-connection] job 0 succeeded"
    )
    endpoint = fake_s3(puts=(400,), code=forged)
    with pytest.raises(FileStoreError) as caught:
        _put(_store_at(s3_settings, endpoint.url))
    assert str(caught.value) == "file store: put: HTTP 400"


def _closed_port():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def test_a_store_that_cannot_be_reached_is_a_content_free_error(
    monkeypatch, s3_settings
):
    # One quick attempt: a refused loopback connection takes Windows two seconds per attempt.
    fast = Config(
        connect_timeout=0.5, retries={"total_max_attempts": 1, "mode": "standard"}
    )
    monkeypatch.setattr(s3_module, "CLIENT_CONFIG", s3_module.CLIENT_CONFIG.merge(fast))
    url = f"http://127.0.0.1:{_closed_port()}"
    with pytest.raises(FileStoreError) as caught:
        _put(_store_at(s3_settings, url))
    assert re.fullmatch(
        r"file store: put: cannot reach the store "
        r"\((EndpointConnectionError|ConnectTimeoutError)\)",
        str(caught.value),
    )
    _content_free(caught.value, url)


def test_a_failing_store_is_retried_three_times_and_no_more(
    fake_s3, s3_settings, no_backoff
):
    endpoint = fake_s3(puts=(503,))
    with pytest.raises(FileStoreError) as caught:
        _put(_store_at(s3_settings, endpoint.url))
    assert str(caught.value) == "file store: put: HTTP 503 SlowDown"
    assert endpoint.methods() == ["PUT"] * 4  # the attempt and 3 retries


def test_a_store_that_answers_after_a_retry_is_a_success(
    fake_s3, s3_settings, no_backoff
):
    endpoint = fake_s3(puts=(503, 200))
    assert _put(_store_at(s3_settings, endpoint.url))
    assert endpoint.methods() == ["PUT", "PUT"]


def test_a_store_that_hangs_times_out_instead_of_holding_the_worker(
    monkeypatch, fake_s3, s3_settings, no_backoff
):
    monkeypatch.setattr(
        s3_module,
        "CLIENT_CONFIG",
        s3_module.CLIENT_CONFIG.merge(Config(read_timeout=0.2)),
    )
    endpoint = fake_s3(delay=3.0)
    started = time.monotonic()
    with pytest.raises(FileStoreError) as caught:
        _put(_store_at(s3_settings, endpoint.url))
    assert str(caught.value) == "file store: put: ReadTimeoutError"
    assert endpoint.methods() == ["PUT"] * 4
    assert time.monotonic() - started < 3.0


# ── The deadline: a store that drips its answer ─────────────────────────────
@pytest.mark.parametrize(("drip", "operation"), [("headers", "put"), ("body", "get")])
def test_a_store_that_drips_its_answer_fails_the_call_at_the_deadline(
    fake_s3, s3_settings, drip, operation
):
    """A byte every 50 ms never trips the 30 s read timeout; the call's own deadline does."""
    endpoint = fake_s3(drip=drip)
    store = _store_at(s3_settings, endpoint.url, call_deadline_s=0.5)
    call = {"put": lambda: _put(store), "get": lambda: store.get(_key())}[operation]
    started = time.monotonic()
    with pytest.raises(FileStoreError) as caught:
        call()
    assert str(caught.value) == f"file store: {operation}: past the 0.5 s deadline"
    assert time.monotonic() - started < 2.0


def test_get_stops_reading_between_chunks_once_past_the_deadline():
    """An abandoned get stops reading at the next chunk instead of draining the object."""

    class Body:
        def __init__(self):
            self.chunks, self.closed = [b"a" * 10, b"b" * 10, b"c" * 10], False

        def read(self, size):
            return self.chunks.pop(0) if self.chunks else b""

        def close(self):
            self.closed = True

    ticks = iter([1.0, 5.0])  # the first chunk in time, the second past it
    body = Body()
    with pytest.raises(
        FileStoreError, match="^file store: get: past the 2 s deadline$"
    ):
        s3_module._read_within(body, 2.0, 2, clock=lambda: next(ticks))
    assert (len(body.chunks), body.closed) == (1, True)
    whole = Body()
    assert s3_module._read_within(whole, 2.0, 2, clock=lambda: 0.0) == (
        b"a" * 10 + b"b" * 10 + b"c" * 10
    )


# ── The client: one per process, nothing from the environment ───────────────
def test_the_client_is_path_style_signed_bounded_and_proxy_free(s3_settings):
    client = s3_module.s3_client(s3_settings)
    config = client.meta.config
    assert (client.meta.endpoint_url, client.meta.region_name) == (
        ENDPOINT,
        "us-east-1",
    )
    assert config.s3["addressing_style"] == "path"
    assert config.signature_version == "s3v4"
    # retries={"max_attempts": 3}: botocore counts the retries, so 4 attempts in all.
    assert config.retries == {"mode": "standard", "total_max_attempts": 4}
    assert (config.connect_timeout, config.read_timeout) == (5, 30)
    assert config.proxies == {}


def test_the_process_builds_one_client_per_settings(fake_s3, s3_settings):
    """A job opens a store of its own; they all share the client (boto3 clients are thread-safe,
    and building one takes a third of a second)."""
    endpoint = fake_s3()
    for _ in range(3):
        assert _put(_store_at(s3_settings, endpoint.url), source="each job")
    info = s3_module.s3_client.cache_info()
    assert (info.misses, info.hits) == (1, 2)


def test_republishing_never_opens_the_s3_store(
    monkeypatch, settings, pdf_folder, manifest_writer, s3_settings
):
    """The relay publishes the envelopes its receipts keep and needs no original: its job must
    not fail for a store it never uses (here one that no client could reach)."""
    from src.handler import documents

    manifest_writer(pdf_folder, settings.manifest_dir)
    settings.data_dir.mkdir(parents=True)
    blocker = settings.data_dir / "outbox"
    blocker.write_text("a file where the outbox folder should be")
    pull = documents.pull_documents(
        settings, documents.load_manifest(settings, "local-docs")
    )
    assert pull.error == "publish failed; the pull stopped"
    blocker.unlink()

    def no_client(s3):
        raise AssertionError("the relay opened the store")

    monkeypatch.setattr(s3_module, "s3_client", no_client)
    unreachable = dataclasses.replace(s3_settings, endpoint_url="http://127.0.0.1:1")
    relay = documents.republish_documents(dataclasses.replace(settings, s3=unreachable))
    assert (relay.published, relay.error) == (1, None)


def test_the_endpoint_is_checked_again_when_a_job_first_uses_the_store(
    monkeypatch, fake_s3, s3_settings
):
    """A name that resolved to a private address at start-up and to a public one now fails the
    job before anything is sent."""
    from src.utility import endpoints

    endpoint = fake_s3()
    monkeypatch.setattr(endpoints, "resolve", lambda host: ["93.184.216.34"])
    with pytest.raises(FileStoreError) as caught:
        _put(_store_at(s3_settings, endpoint.url))
    assert str(caught.value).startswith(
        f"file store: NICKEL_S3_ENDPOINT_URL: {endpoint.url} is not a loopback, private "
        "or tailnet address"
    )
    assert endpoint.requests == []


def test_a_proxy_in_the_environment_is_never_used(monkeypatch, fake_s3, s3_settings):
    endpoint, proxy = fake_s3(), fake_s3()
    for name in ("NO_PROXY", "no_proxy"):
        monkeypatch.delenv(name, raising=False)
    for name in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"):
        monkeypatch.setenv(name, proxy.url)
    assert _put(_store_at(s3_settings, endpoint.url))
    assert proxy.requests == []
    assert endpoint.methods() == ["PUT"]


def test_nothing_in_the_environment_or_in_aws_files_steers_the_client(
    monkeypatch, tmp_path, fake_s3, s3_settings
):
    """Credentials, endpoint, region, CA bundle, retries, S3 options, the bearer token and the
    defaults mode all come from the settings or constants. ``auto`` defaults would ask the instance
    metadata service, which is the decoy here."""
    endpoint, decoy = fake_s3(), fake_s3()
    config_file = tmp_path / "config"
    config_file.write_text(
        f"[default]\nregion = eu-west-3\nendpoint_url = {decoy.url}\n"
        f"ca_bundle = {tmp_path / 'nope.pem'}\nretry_mode = legacy\nmax_attempts = 9\n"
        "defaults_mode = auto\ns3 =\n    addressing_style = virtual\n",
        encoding="utf-8",
    )
    credentials_file = tmp_path / "credentials"
    credentials_file.write_text(
        "[default]\naws_access_key_id = AKIAFROMTHEFILE00000\n"
        "aws_secret_access_key = from-the-file\n",
        encoding="utf-8",
    )
    hostile = {
        "AWS_CONFIG_FILE": str(config_file),
        "AWS_SHARED_CREDENTIALS_FILE": str(credentials_file),
        "AWS_PROFILE": "no-such-profile",
        "AWS_ACCESS_KEY_ID": "AKIAFROMTHEENV000000",
        "AWS_SECRET_ACCESS_KEY": "from-the-environment",
        "AWS_SESSION_TOKEN": "from-the-environment",
        "AWS_BEARER_TOKEN_S3": "bearer-from-the-environment",
        "AWS_REGION": "eu-central-1",
        "AWS_DEFAULT_REGION": "eu-central-1",
        "AWS_ENDPOINT_URL": decoy.url,
        "AWS_ENDPOINT_URL_S3": decoy.url,
        "AWS_CA_BUNDLE": str(tmp_path / "nope.pem"),
        "AWS_DEFAULTS_MODE": "auto",
        "AWS_EC2_METADATA_SERVICE_ENDPOINT": decoy.url,
        "AWS_MAX_ATTEMPTS": "9",
        "AWS_RETRY_MODE": "legacy",
        # botocore refuses the client outright on this one: every job would fail.
        "AWS_S3_US_EAST_1_REGIONAL_ENDPOINT": "bogus",
        "AWS_S3_USE_ARN_REGION": "bogus",
        "AWS_S3_DISABLE_MULTIREGION_ACCESS_POINTS": "bogus",
    }
    for name, value in hostile.items():
        monkeypatch.setenv(name, value)
    assert _put(_store_at(s3_settings, endpoint.url))
    assert decoy.requests == []
    assert endpoint.methods() == ["PUT"]
    [request] = endpoint.requests
    assert request.path == f"/{BUCKET}/{_key()}"
    assert request.headers["authorization"].startswith(
        f"AWS4-HMAC-SHA256 Credential={ACCESS_KEY}/"
    )
    assert "/us-east-1/s3/aws4_request" in request.headers["authorization"]
    assert "x-amz-security-token" not in request.headers


def test_the_session_holds_no_bearer_token_from_the_environment(monkeypatch):
    """SigV4 is set in code, so botocore would not send one today; the session holds none either."""
    monkeypatch.setenv("AWS_BEARER_TOKEN_S3", "bearer-from-the-environment")
    assert s3_module._isolated_session().get_auth_token(signing_name="s3") is None


def test_the_client_libraries_log_nothing_a_root_handler_could_keep(
    fake_s3, s3_settings
):
    """With the root logger at DEBUG (JSON logs in K3s), botocore would log the request with its
    key, and urllib3 warns with the URL when an answer carries a header line it cannot parse.
    """
    records = []
    handler = logging.Handler(logging.DEBUG)
    handler.emit = records.append
    root = logging.getLogger()
    level = root.level
    root.addHandler(handler)
    root.setLevel(logging.DEBUG)
    try:
        endpoint = fake_s3(bad_header=True)
        assert _put(_store_at(s3_settings, endpoint.url))
    finally:
        root.removeHandler(handler)
        root.setLevel(level)
    libraries = ("botocore", "boto3", "s3transfer", "urllib3")
    assert [r.name for r in records if r.name.split(".")[0] in libraries] == []


def _certificate(tmp_path):
    """A self-signed certificate for 127.0.0.1, standing in for a private CA, and its key."""
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "nickel test CA")])
    now = datetime.now(UTC)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=5))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(
            x509.SubjectAlternativeName(
                [x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]
            ),
            critical=False,
        )
        .add_extension(
            x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False
        )
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(key.public_key()), critical=False
        )
        .sign(key, hashes.SHA256())
    )
    cert_file, key_file = tmp_path / "ca.pem", tmp_path / "key.pem"
    cert_file.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    key_file.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    return cert_file, key_file


def test_tls_trusts_the_ca_bundle_of_the_settings_and_only_that(
    monkeypatch, tmp_path, fake_s3, s3_settings, no_backoff
):
    cert_file, key_file = _certificate(tmp_path)
    endpoint = fake_s3(tls=(cert_file, key_file))
    assert _put(_store_at(s3_settings, endpoint.url, ca_bundle=cert_file))
    # The same CA from the environment is not the settings' CA: the private endpoint is refused.
    monkeypatch.setenv("AWS_CA_BUNDLE", str(cert_file))
    monkeypatch.setenv("REQUESTS_CA_BUNDLE", str(cert_file))
    with pytest.raises(FileStoreError) as caught:
        _put(_store_at(s3_settings, endpoint.url), key=_key(b"other"), data=b"other")
    assert str(caught.value) == "file store: put: cannot reach the store (SSLError)"


def test_over_https_the_put_arrives_whole_as_aws_chunked_with_its_crc32(
    tmp_path, fake_s3, s3_settings
):
    """How botocore puts over https (production's wire): chunked transfer, aws-chunked content,
    an unsigned payload with a CRC32 trailer. The store gets the bytes, and can check them.
    """
    cert_file, key_file = _certificate(tmp_path)
    endpoint = fake_s3(tls=(cert_file, key_file))
    assert _put(_store_at(s3_settings, endpoint.url, ca_bundle=cert_file))
    [request] = endpoint.requests
    assert request.headers["transfer-encoding"] == "chunked"
    assert "aws-chunked" in request.headers["content-encoding"]
    assert (
        request.headers["x-amz-content-sha256"] == "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
    )
    assert request.headers["x-amz-decoded-content-length"] == str(len(PDF))
    assert request.body == PDF
    crc32 = base64.b64encode(zlib.crc32(PDF).to_bytes(4, "big")).decode()
    assert request.trailers == {"x-amz-checksum-crc32": crc32}


# ── Failure ordering: no receipt claims an original a PUT did not store ──────
def _bridge(store, tmp_path, receipts):
    return IngestBridge(
        tenant_id=TENANT,
        store=store,
        publisher=JsonlOutbox(tmp_path / "outbox"),
        receipts=receipts,
    )


def test_a_put_that_fails_leaves_no_receipt_and_the_next_run_stores_it(
    aws, s3_settings, tmp_path, receipts, ctx, pdf_bytes
):
    credentials, allow = _iam_user(*READ_ONLY)  # it may look, not write
    store = S3FileStore(dataclasses.replace(s3_settings, **credentials))
    bridge = _bridge(store, tmp_path, receipts)
    item = RawItem(message_id="local:docs/a.pdf", data=pdf_bytes)
    with enable_iam_authentication():
        with pytest.raises(FileStoreError) as caught:
            bridge.ingest(ctx, item)
    assert str(caught.value) == "file store: put: HTTP 403 AccessDenied"
    # Nothing claims the original: no receipt (so no published one to short-circuit the next run),
    # and no envelope pointing at an object that is not there.
    assert receipts.counts(tenant_id=TENANT) == {}
    assert bridge.publisher.read("raw_data") == []
    assert "Contents" not in aws.list_objects_v2(Bucket=BUCKET)

    allow(*READ_ONLY, "s3:PutObject")  # the store takes writes again
    with enable_iam_authentication():
        again = bridge.ingest(ctx, item)
    assert (again.outcome, again.new_object) == (Outcome.PUBLISHED, True)
    assert receipts.counts(tenant_id=TENANT) == {"published": 1}
    assert store.get(again.payload_ref) == pdf_bytes
    # The object carries facts about the content only: nothing of the message (a path at the
    # source), nor the source or its retention (they stay with the receipts).
    metadata = store.metadata(again.payload_ref)
    assert metadata == {
        "sha256": _sha(pdf_bytes),
        "content_type": "application/pdf",
        "bytes": len(pdf_bytes),
    }
    assert "local:docs/a.pdf" not in json.dumps(metadata)


def test_a_pull_job_whose_store_refuses_fails_with_a_content_free_error(
    aws,
    s3_settings,
    service,
    settings,
    pdf_folder,
    manifest_writer,
    wait_for,
    records,
):
    (pdf_folder / "a.pdf").rename(pdf_folder / "Jane-Roe-labs.pdf")
    manifest_writer(pdf_folder, settings.manifest_dir)
    credentials, _ = _iam_user(*READ_ONLY)
    s3 = dataclasses.replace(s3_settings, **credentials)
    with enable_iam_authentication(), service(s3=s3) as client:
        job_id = client.post(
            "/jobs/documents/pull", json={"manifest": "local-docs"}
        ).json()["job_id"]
        job = wait_for(client, job_id)
        documents = client.get("/documents").json()["documents"]
    assert job["status"] == "failed"
    assert job["error"] == "file store: put: HTTP 403 AccessDenied"
    assert documents == []
    assert all("Jane-Roe" not in r.getMessage() for r in records)
    assert all(credentials["secret_access_key"] not in r.getMessage() for r in records)


def test_the_command_line_says_the_store_refused_and_exits_1(
    aws, tmp_path, pdf_folder, manifest_writer, capsys
):
    credentials, _ = _iam_user(*READ_ONLY)
    manifest = manifest_writer(pdf_folder)
    env = {
        "NICKEL_ENV": "development",  # the credentials as variables: development only
        "NICKEL_TENANT_ID": TENANT,
        "NICKEL_DATA_DIR": str(tmp_path / "data"),
        "NICKEL_FILE_STORE": "s3",
        "NICKEL_S3_ENDPOINT_URL": ENDPOINT,
        "NICKEL_S3_ACCESS_KEY_ID": credentials["access_key_id"],
        "NICKEL_S3_SECRET_ACCESS_KEY": credentials["secret_access_key"],
    }
    with enable_iam_authentication():
        code = main(["documents", "pull", str(manifest)], env=env, out=io.StringIO())
    assert code == 1
    assert capsys.readouterr().err == "error: file store: put: HTTP 403 AccessDenied\n"
    with ReceiptStore(tmp_path / "data" / "receipts.sqlite") as receipts:
        assert receipts.counts(tenant_id=TENANT) == {}


# ── Bounds: abandoned calls, the upload's deadline, max_bytes, a 409 (review of #7) ──
@pytest.fixture(autouse=True)
def no_abandoned_call_outlives_its_test():
    """Calls a test abandons end with it (its fake store closes): the next test starts at zero."""
    yield
    for _ in range(200):
        if s3_module._ABANDONED.open == 0:
            return
        time.sleep(0.05)
    raise AssertionError(f"{s3_module._ABANDONED.open} abandoned calls still open")


def test_abandoned_calls_are_bounded_and_a_new_one_fails_at_once(records):
    """A store that stopped answering must not pile threads, sockets and objects: past four
    abandoned calls a new one fails straight away, and once they end, calls run again.
    """
    release = threading.Event()
    for _ in range(s3_module._ABANDONED_LIMIT):
        with pytest.raises(FileStoreError, match="^file store: put: past the 0.05 s"):
            s3_module._within(0.05, "put", release.wait)
    assert s3_module._ABANDONED.open == s3_module._ABANDONED_LIMIT
    started = time.monotonic()
    with pytest.raises(
        FileStoreError,
        match="^file store: get: 4 earlier calls are still open past their deadline$",
    ):
        s3_module._within(30, "get", lambda: b"never called")
    assert time.monotonic() - started < 1
    warnings = [r.getMessage() for r in records if r.levelno == logging.WARNING]
    assert warnings[-1] == "file store: put abandoned past its 0.05 s deadline (4 open)"
    release.set()
    for _ in range(100):
        if s3_module._ABANDONED.open == 0:
            break
        time.sleep(0.02)
    assert s3_module._within(5, "get", lambda: b"ok") == b"ok"


def test_a_call_that_ends_as_its_deadline_passes_is_not_counted_abandoned(monkeypatch):
    class LateEvent(threading.Event):
        def wait(self, timeout=None):
            super().wait(5)  # the call ends...
            return False  # ...but the wait reports the deadline passed

    monkeypatch.setattr(s3_module.threading, "Event", LateEvent)
    assert s3_module._within(0.01, "get", lambda: b"ok") == b"ok"
    assert s3_module._ABANDONED.open == 0


def test_an_upload_past_its_deadline_stops_itself():
    """botocore reads the body a chunk at a time: once the deadline has passed, the next read
    raises, so an abandoned upload stops instead of trickling on to a slow store."""
    ticks = iter([0.0, 1.0, 3.0])
    upload = s3_module._Upload(b"abcdef", 2.0, 2, clock=lambda: next(ticks))
    assert upload.read(3) == b"abc"
    assert upload.read(3) == b"def"
    with pytest.raises(
        FileStoreError, match="^file store: put: past the 2 s deadline$"
    ):
        upload.read(3)


def test_a_409_is_tried_once_more_and_a_412_then_means_stored(
    fake_s3, s3_settings, monkeypatch
):
    """Two jobs racing on one key: S3 may answer the second 409 (a conditional write in progress).
    One more try settles it: 412 when the other one landed, 200 when it did not."""
    monkeypatch.setattr(s3_module, "_CONFLICT_PAUSE_S", 0)
    landed = fake_s3(puts=(409, 412), code="ConditionalRequestConflict")
    assert not _put(_store_at(s3_settings, landed.url))
    assert landed.methods() == ["PUT", "PUT"]
    failed = fake_s3(puts=(409, 200), code="ConditionalRequestConflict")
    assert _put(_store_at(s3_settings, failed.url))
    assert failed.methods() == ["PUT", "PUT"]
    stuck = fake_s3(puts=(409, 409), code="ConditionalRequestConflict")
    with pytest.raises(
        FileStoreError, match="^file store: put: HTTP 409 ConditionalRequestConflict$"
    ):
        _put(_store_at(s3_settings, stuck.url))
    assert stuck.methods() == ["PUT", "PUT"]


def test_get_refuses_an_object_over_max_bytes_before_reading_it(store, monkeypatch):
    _put(store)
    read = s3_module._read_within
    reads = []
    monkeypatch.setattr(
        s3_module, "_read_within", lambda *a, **k: reads.append(1) or read(*a, **k)
    )
    assert store.get(_key(), max_bytes=len(PDF)) == PDF
    with pytest.raises(
        FileStoreError, match=f"^file store: get: over the {len(PDF) - 1} bytes asked$"
    ):
        store.get(_key(), max_bytes=len(PDF) - 1)
    assert reads == [1]  # the object that fitted was read; the larger one never was


def test_get_stops_reading_once_past_max_bytes():
    """A store that gives no length, or the wrong one, is capped while reading."""
    body = io.BytesIO(b"x" * (3 * s3_module._CHUNK))
    with pytest.raises(
        FileStoreError, match="^file store: get: over the 10 bytes asked$"
    ):
        s3_module._read_within(body, 1e9, 60, max_bytes=10)


def test_metadata_without_a_length_is_a_file_store_error(s3_settings, monkeypatch):
    class NoLength:
        def head_object(self, **_):
            return {"ContentType": "application/pdf", "Metadata": {}}

    monkeypatch.setattr(s3_module, "s3_client", lambda settings: NoLength())
    with pytest.raises(
        FileStoreError, match="^file store: metadata: the store gave no length$"
    ):
        S3FileStore(s3_settings).metadata(_key())
