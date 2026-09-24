"""Content-addressed, write-once File Store over the S3 API, through boto3 (NIK-2209-06).

SeaweedFS serves it in development (``compose.yaml``); production gets the object store the
SeaweedFS-vs-RustFS bake-off picks. Nothing here is specific to either: moving is a change of
endpoint and credentials (``[FILE_STORE]`` in ``conf/application.conf``). The keys are
``object_key()``'s, unchanged: ``<tenant>/<class>/<sha256[:2]>/<sha256>``, path-style under the
bucket (``http://<endpoint>/<bucket>/<key>``).

- ``put`` is one PutObject with ``If-None-Match: *``: the store creates the key only if it is not
  there, atomically, and answers 412 when it is, which ``put`` reports as False (write-once: a
  content-addressed key that exists holds these bytes already). SeaweedFS 4.47, RustFS and MinIO
  all take it. The bytes, the content type and the metadata go in that one PUT, so readers see the
  whole object with its metadata or nothing. ``put`` refuses bytes the key does not name.
- ``get`` checks the bytes against the sha256 the key names, so an object replaced behind the S3
  API (SeaweedFS's filer takes writes of its own) is refused, not returned.
- S3 metadata travels as HTTP headers: ASCII only, and at most 2 KB in all. A value that is not
  plain printable ASCII, or that holds ``=?``, goes as an RFC 2047 encoded word,
  ``=?UTF-8?B?<base64>?=``, the form AWS S3 itself uses; ``metadata()`` decodes it. Metadata over
  2 KB is refused before anything is sent.
- Every call runs against a wall-clock deadline (``NICKEL_S3_CALL_DEADLINE_SECONDS``), retries
  included: within it the client retries up to 3 times (standard mode: 4 attempts in all), each
  attempt with its connect and read timeouts; past it the call is abandoned on its daemon thread and
  the job fails, however slowly the store drips its answer. An abandoned call does not run on: a
  ``put``'s body stops the upload itself once the deadline has passed (``_Upload``), and ``get``
  reads in chunks and stops between them. While ``_ABANDONED_LIMIT`` abandoned calls are still open,
  a new call fails at once instead of adding a thread, a socket and an object's bytes to them.
- ``get`` takes ``max_bytes``: an object larger than that is refused before its body is read, and
  the body is read into one buffer, never held twice.
- Two jobs racing to create one key may get 409 (ConditionalRequestConflict, a conditional write in
  progress): ``put`` tries once more, and a 412 then means the other one stored it.
- Every failure is a FileStoreError whose text names the operation and the HTTP status and S3 error
  code (only when it looks like one), or botocore's error type: never the key, the endpoint's URL
  (botocore's own messages quote the URL they failed on, the key in it), the response body or a
  credential. A missing object is a MissingObjectError, a missing bucket is not: when a HEAD's 404
  has no body to tell them apart, a HeadBucket does.

The client takes everything from the settings: its credentials, endpoint, region and CA bundle are
passed in, its botocore session has constants where botocore would read a profile, ``~/.aws``, a
bearer token or an ``AWS_*`` setting (``_isolated_session``), and it never goes through an
``HTTP(S)_PROXY``. The process builds it once per settings (boto3 clients are thread-safe) and each
job checks the endpoint again before its first call (``src.config.check_s3_endpoint``).
"""

from __future__ import annotations

import functools
import hashlib
import io
import os
import re
import threading
import time
from collections.abc import Callable, Mapping
from contextlib import closing
from email.errors import HeaderParseError
from email.header import decode_header, make_header
from typing import Any, TypeVar

import boto3
import botocore.session
from botocore.config import Config
from botocore.configprovider import ConstantProvider
from botocore.exceptions import BotoCoreError, ClientError
from botocore.exceptions import ConnectionError as BotoConnectionError
from botocore.tokens import TokenProviderChain

from src.config import S3Settings, check_s3_endpoint
from src.logging import logger

from .base import FileStoreError, MissingObjectError, encode_metadata, key_parts

T = TypeVar("T")

CLIENT_CONFIG = Config(
    # http://<endpoint>/<bucket>/<key>: no bucket subdomain for the store's DNS to answer.
    s3={"addressing_style": "path"},
    signature_version="s3v4",
    # botocore counts the retries here: 4 attempts in all, within the call's deadline.
    retries={"max_attempts": 3, "mode": "standard"},
    connect_timeout=5,
    read_timeout=30,
    # Straight to the endpoint, never through a proxy from HTTP(S)_PROXY: the originals could leave.
    proxies={},
)
# get() reads the body this much at a time, the deadline checked in between.
_CHUNK = 256 * 1024
# An S3 error code worth quoting; any other <Code> is the server's own text, and is dropped.
_CODE = re.compile(r"[A-Za-z0-9.]{1,64}")
_SHA256 = re.compile(r"[0-9a-f]{64}")
# Calls past their deadline whose threads are still open; while this many are, a new call fails at
# once rather than pile another thread, socket and object on a store that has stopped answering.
_ABANDONED_LIMIT = 4
# How long put waits before its one retry of a 409 (a conditional write to the key in progress).
_CONFLICT_PAUSE_S = 0.2


class _Abandoned:
    """The calls abandoned at their deadline whose threads have not ended yet (module docstring)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.open = 0

    def refuse_if_full(self, operation: str) -> None:
        with self._lock:
            if self.open >= _ABANDONED_LIMIT:
                raise FileStoreError(
                    f"file store: {operation}: {self.open} earlier calls are still open past "
                    "their deadline"
                )

    def add(self) -> int:
        with self._lock:
            self.open += 1
            return self.open

    def remove(self) -> None:
        with self._lock:
            self.open -= 1


_ABANDONED = _Abandoned()


class _Upload(io.BytesIO):
    """A ``put``'s body: the object's bytes, read by botocore a chunk at a time, that refuses to hand
    out another chunk once the call's deadline has passed. An upload its caller abandoned therefore
    stops on its own, and its thread lets go of the socket and the bytes, instead of trickling on
    to a slow store after the job has failed."""

    def __init__(
        self,
        data: bytes,
        deadline_at: float,
        seconds: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        super().__init__(data)
        self._deadline_at = deadline_at
        self._seconds = seconds
        self._clock = clock

    def read(self, size: int | None = -1) -> bytes:
        if self._clock() > self._deadline_at:
            raise _deadline("put", self._seconds)
        return super().read(size)


def _isolated_session() -> botocore.session.Session:
    """A botocore session that takes no setting from the environment or ``~/.aws``: every session
    variable loses its ``AWS_*`` variable and its config-file key, the config and credentials files
    are none, the S3 and proxy sections (``AWS_S3_US_EAST_1_REGIONAL_ENDPOINT`` and the like) are
    empty constants, and there is no bearer token (``AWS_BEARER_TOKEN_S3``). So no profile,
    credentials, CA bundle (``AWS_CA_BUNDLE``), retry setting, S3 option or defaults mode (``auto``
    asks the instance metadata service) comes from outside; the endpoint passed in wins over
    ``AWS_ENDPOINT_URL*``. (botocore still names ``AWS_EXECUTION_ENV`` in its User-Agent.)
    """
    defaults = botocore.session.Session.SESSION_VARIABLES
    variables = {
        name: (None, None, default, conversion)
        for name, (_, _, default, conversion) in defaults.items()
    }
    variables["config_file"] = (None, None, os.devnull, None)
    variables["credentials_file"] = (None, None, os.devnull, None)
    session = botocore.session.Session(session_vars=variables)
    store = session.get_component("config_store")
    store.set_config_provider("s3", ConstantProvider({}))
    store.set_config_provider("proxies_config", ConstantProvider({}))
    session.register_component("token_provider", TokenProviderChain(providers=[]))
    return session


@functools.lru_cache(maxsize=8)
def s3_client(settings: S3Settings) -> Any:
    """The process's boto3 S3 client for ``settings`` (module docstring): built once, since boto3
    clients are thread-safe and building one takes a third of a second."""
    session = boto3.session.Session(botocore_session=_isolated_session())
    return session.client(
        "s3",
        endpoint_url=settings.endpoint_url,
        region_name=settings.region,
        aws_access_key_id=settings.access_key_id,
        aws_secret_access_key=settings.secret_access_key,
        verify=str(settings.ca_bundle) if settings.ca_bundle else True,
        config=CLIENT_CONFIG,
    )


def _deadline(operation: str, seconds: float) -> FileStoreError:
    return FileStoreError(f"file store: {operation}: past the {seconds:g} s deadline")


def _within(seconds: float, operation: str, call: Callable[[], T]) -> T:
    """``call()`` on a daemon thread, waited for at most ``seconds``. Past that the call is
    abandoned (its thread ends when the store answers, its socket times out, or its upload stops at
    the deadline, and never holds the process open) and the caller gets a FileStoreError. While
    ``_ABANDONED_LIMIT`` abandoned calls are still open, a new one fails at once."""
    _ABANDONED.refuse_if_full(operation)
    outcome: dict[str, Any] = {}
    finished = threading.Event()
    state = threading.Lock()
    abandoned = False

    def run() -> None:
        try:
            outcome["value"] = call()
        except Exception as exc:  # handed to the caller below
            outcome["error"] = exc
        finally:
            with state:
                finished.set()
                if abandoned:
                    _ABANDONED.remove()

    threading.Thread(target=run, name=f"file-store-{operation}", daemon=True).start()
    if not finished.wait(seconds):
        with state:
            if not finished.is_set():
                abandoned = True
                still_open = _ABANDONED.add()
                logger.warning(
                    "file store: %s abandoned past its %g s deadline (%d open)",
                    operation,
                    seconds,
                    still_open,
                )
                raise _deadline(operation, seconds)
    if "error" in outcome:
        raise outcome["error"]
    return outcome["value"]


def _read_within(
    body: Any,
    deadline_at: float,
    seconds: float,
    max_bytes: int | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> bytes:
    """The whole body, a chunk at a time into one buffer, stopping between chunks once the deadline
    has passed (a get abandoned by its caller stops reading instead of draining the rest of the
    object) or once it holds more than ``max_bytes``."""
    buffer = io.BytesIO()
    with closing(body):
        while chunk := body.read(_CHUNK):
            buffer.write(chunk)
            if max_bytes is not None and buffer.tell() > max_bytes:
                raise FileStoreError(
                    f"file store: get: over the {max_bytes} bytes asked"
                )
            if clock() > deadline_at:
                raise _deadline("get", seconds)
    return buffer.getvalue()


def _decode(value: str) -> str:
    """A value as it was put: an encoded word decoded (ours, or one S3 split into several words or
    Q-encoded on the way back); any other value unchanged."""
    if "=?" not in value:
        return value
    return str(make_header(decode_header(value)))


def _decode_metadata(headers: Mapping[str, str]) -> dict[str, str]:
    # Lower-cased: SeaweedFS answers with Go's canonical header names (X-Amz-Meta-Sha256).
    return {name.lower(): _decode(value) for name, value in headers.items()}


def _content_sha(key: str) -> str:
    """The sha256 a content-addressed key names: its last segment."""
    sha = key_parts(key)[-1]
    if not _SHA256.fullmatch(sha):
        raise ValueError("invalid object key: the last segment is not a sha256")
    return sha


def _status(exc: ClientError) -> Any:
    return exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")


def _code(exc: ClientError) -> str:
    return str(exc.response.get("Error", {}).get("Code", ""))


def _bucket_exists(client: Any, bucket: str) -> bool:
    """Whether the bucket is there, when a body-less 404 left it open. Only a 404 says no."""
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError as exc:
        return _status(exc) != 404
    return True


def _failure(operation: str, exc: BotoCoreError | ClientError) -> FileStoreError:
    """The content-free error for a failed call (module docstring)."""
    if isinstance(exc, ClientError):
        status, code = _status(exc), _code(exc)
        if status == 404 and code != "NoSuchBucket":
            return MissingObjectError(f"file store: {operation}: no such object")
        # A HEAD answer has no body, so botocore names its error by the status alone; any other
        # code is quoted only when it looks like an S3 code, never as the server's free text.
        quoted = _CODE.fullmatch(code) and code != str(status)
        detail = f"HTTP {status} {code}" if quoted else f"HTTP {status}"
        return FileStoreError(f"file store: {operation}: {detail}")
    if isinstance(exc, BotoConnectionError):
        return FileStoreError(
            f"file store: {operation}: cannot reach the store ({type(exc).__name__})"
        )
    return FileStoreError(f"file store: {operation}: {type(exc).__name__}")


class S3FileStore:
    """One job's view of the S3 File Store. Nothing is contacted, and no client is looked up,
    until its first call: the republish relay opens one and never uses it."""

    def __init__(self, settings: S3Settings) -> None:
        self.settings = settings
        self.bucket = settings.originals_bucket
        self._client: Any = None

    def _open(self) -> Any:
        """The client, once per store: the endpoint checked again first (its name may resolve
        elsewhere by now), then the process's client for these settings."""
        if self._client is None:
            try:
                check_s3_endpoint(self.settings)
            except ValueError as exc:
                raise FileStoreError(f"file store: {exc}") from None
            self._client = s3_client(self.settings)
        return self._client

    def _call(self, operation: str, call: Callable[[Any], T]) -> T:
        """``call(client)`` within the call deadline; botocore's errors become FileStoreErrors."""
        try:
            return _within(
                self.settings.call_deadline_s, operation, lambda: call(self._open())
            )
        except (BotoCoreError, ClientError) as exc:
            # from None: a ClientError's text carries the server's <Message>.
            raise _failure(operation, exc) from None

    def put(
        self, key: str, data: bytes, *, content_type: str, metadata: Mapping[str, str]
    ) -> bool:
        if hashlib.sha256(data).hexdigest() != _content_sha(key):
            raise ValueError("the bytes are not the ones their key names")
        headers = encode_metadata(metadata)
        seconds = self.settings.call_deadline_s
        deadline_at = time.monotonic() + seconds

        def attempt(client: Any) -> bool:
            client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=_Upload(data, deadline_at, seconds),
                ContentType=content_type,
                Metadata=headers,
                IfNoneMatch="*",
            )
            return True

        def create(client: Any) -> bool:
            for tries_left in (1, 0):
                try:
                    return attempt(client)
                except ClientError as exc:
                    if (
                        _status(exc) == 412
                    ):  # PreconditionFailed: the key is there already
                        return False
                    # 409: another conditional write to the key is in progress (its bytes are
                    # these bytes): once more, when a 412 says whether it landed.
                    if _status(exc) != 409 or not tries_left:
                        raise
                    time.sleep(_CONFLICT_PAUSE_S)
            raise AssertionError("unreachable")  # pragma: no cover

        return self._call("put", create)

    def get(self, key: str, *, max_bytes: int | None = None) -> bytes:
        expected = _content_sha(key)
        seconds = self.settings.call_deadline_s
        deadline_at = time.monotonic() + seconds

        def read(client: Any) -> bytes:
            response = client.get_object(Bucket=self.bucket, Key=key)
            length = response.get("ContentLength")
            if max_bytes is not None and length is not None and length > max_bytes:
                response["Body"].close()
                raise FileStoreError(
                    f"file store: get: over the {max_bytes} bytes asked"
                )
            return _read_within(response["Body"], deadline_at, seconds, max_bytes)

        data = self._call("get", read)
        if hashlib.sha256(data).hexdigest() != expected:
            raise FileStoreError(
                "file store: get: the bytes are not the ones their key names"
            )
        return data

    def metadata(self, key: str) -> dict[str, Any]:
        _content_sha(key)

        def head(client: Any) -> dict[str, Any]:
            try:
                return client.head_object(Bucket=self.bucket, Key=key)
            except ClientError as exc:
                if (
                    _status(exc) == 404
                    and _code(exc) == "404"
                    and not _bucket_exists(client, self.bucket)
                ):
                    raise FileStoreError(
                        "file store: metadata: HTTP 404 NoSuchBucket"
                    ) from None
                raise

        found = self._call("metadata", head)
        try:
            decoded = _decode_metadata(found.get("Metadata", {}))
        except (ValueError, LookupError, HeaderParseError):
            raise FileStoreError(
                "file store: metadata: a value that cannot be decoded"
            ) from None
        if found.get("ContentLength") is None:
            raise FileStoreError("file store: metadata: the store gave no length")
        return {
            "content_type": found.get("ContentType"),
            "bytes": found["ContentLength"],
            **decoded,
        }
