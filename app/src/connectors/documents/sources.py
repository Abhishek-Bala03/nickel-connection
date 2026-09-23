"""Where the document connector reads files from: Kaggle, Hugging Face, web links, or a local folder.

A source lists ``SourceFile`` items lazily; bytes are fetched only when the connector asks, so a limit
of 20 files downloads 20 files. Any failure to fetch one file is a ``FetchError`` for that file alone;
a download the egress rule stops before it starts is a ``RefusedError`` (a ``FetchError`` too).
Every source takes the size cap in force (``DocumentConnector``: the smaller of the manifest's and
the service's) and hands back no file over it: a web download stops at the cap, and a file an SDK
put on disk is measured before it is read into memory.

The text of a ``FetchError`` is content-free by contract: a job record keeps it and ``GET /jobs``
returns it. It names the source's locator (the manifest's handle, repo or host) and the failure's
type or HTTP status, never a file's path, a redirect's query string or a library's own message: a
clinic's file names can carry patient names, and a signed URL carries its token. The SDK sources
log the library's message (a path is a debugging detail, not a secret); httpx's is dropped.

Credentials never come through here: kagglehub reads ``KAGGLE_API_TOKEN`` (or ``~/.kaggle``) and
huggingface_hub reads ``HF_TOKEN`` from the environment themselves.
"""

from __future__ import annotations

import ipaddress
import socket
import tempfile
import time
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, replace
from functools import partial
from pathlib import Path, PurePosixPath
from typing import Any, Protocol
from urllib.parse import urlparse

import httpcore
import httpx

from src.logging import logger

USER_AGENT = "nickel-connection/0.1 (document connector)"
MAX_REDIRECTS = 5
HTTPS_PORT = 443  # the only port a hop may use: the vendor's https service
DEFAULT_DEADLINE_S = 300.0


class FetchError(RuntimeError):
    """One file could not be fetched from its source."""


class RefusedError(FetchError):
    """The egress rule stopped a download before anything was fetched: a hop that is not https, a
    host the manifest does not name, or a private or local address."""


@dataclass(frozen=True)
class SourceFile:
    rel_path: str  # POSIX path inside the source
    fetch: Callable[[], bytes]
    size: int | None = None
    url: str | None = None


class Source(Protocol):
    locator: str  # stable identity of the source; part of every message id

    def files(self) -> Iterator[SourceFile]: ...


def _read_capped(path: Path, max_bytes: int, where: str) -> bytes:
    """The file's bytes, once its size on disk is known to be within the cap: nothing over the cap
    is read into memory, whatever the source listed for it, or did not (Kaggle lists no sizes).
    """
    if path.stat().st_size > max_bytes:
        raise FetchError(f"{where}: larger than the {max_bytes}-byte cap")
    return path.read_bytes()


def _posix_path(path: Path, root: Path) -> str:
    """The path inside the source as text JSON can carry. A name the file system could not
    decode comes back with lone surrogates (Linux: ``surrogateescape``), which no UTF-8 output
    takes: the envelope, and the receipt that stores it, could not be written, and the run died
    at that file on every pull. Such a name is spelt with escapes instead, the same every time,
    so the message id built on it is stable; the bytes are still read from the real name.
    """
    text = path.relative_to(root).as_posix()
    return text.encode("utf-8", "backslashreplace").decode("utf-8")


def _folder_files(root: Path, max_bytes: int, where: str) -> Iterator[SourceFile]:
    for path in sorted(p for p in root.rglob("*") if p.is_file()):
        yield SourceFile(
            rel_path=_posix_path(path, root),
            fetch=partial(_read_capped, path, max_bytes, where),
            size=path.stat().st_size,
        )


class LocalDirSource(Source):
    """A folder on disk: manual drops and tests."""

    def __init__(self, root: Path, *, max_bytes: int) -> None:
        self.root = Path(root)
        self.locator = self.root.name
        self.max_bytes = max_bytes

    def files(self) -> Iterator[SourceFile]:
        if not self.root.is_dir():
            logger.warning(
                "local source %s: %s is not a folder", self.locator, self.root
            )
            raise FetchError(f"local {self.locator}: not a folder")
        yield from _folder_files(self.root, self.max_bytes, f"local {self.locator}")


class KaggleSource(Source):
    """A Kaggle dataset through kagglehub, which finds the Kaggle token in the environment by itself."""

    def __init__(
        self,
        handle: str,
        files: list[str] | None = None,
        *,
        max_bytes: int,
        download: Callable[..., str] | None = None,
    ) -> None:
        self.handle = handle
        self.locator = handle
        self.only = files
        self.max_bytes = max_bytes
        self._download = download

    def _downloader(self) -> Callable[..., str]:
        if self._download is None:
            import kagglehub

            self._download = kagglehub.dataset_download
        return self._download

    def _page(self) -> str:
        return f"https://www.kaggle.com/datasets/{self.handle.split('/versions/')[0]}"

    def _failure(self, exc: Exception) -> FetchError:
        """Content-free (module docstring): kagglehub's message can name the file it choked on."""
        logger.warning("kaggle %s: %s: %s", self.handle, type(exc).__name__, exc)
        return FetchError(f"kaggle {self.handle}: {type(exc).__name__}")

    def _fetch_one(self, rel_path: str) -> bytes:
        # kagglehub raises its own HTTP errors; one file's failure is only that file's. It lists no
        # sizes, so the file lands in its cache first; the cap is checked before the bytes are read.
        try:
            local = Path(self._downloader()(self.handle, path=rel_path))
        except Exception as exc:
            raise self._failure(exc) from exc
        return _read_capped(local, self.max_bytes, f"kaggle {self.handle}")

    def files(self) -> Iterator[SourceFile]:
        if self.only:
            for rel_path in self.only:
                yield SourceFile(
                    rel_path=rel_path,
                    fetch=partial(self._fetch_one, rel_path),
                    url=self._page(),
                )
            return
        try:
            root = Path(self._downloader()(self.handle))
        except Exception as exc:
            raise self._failure(exc) from exc
        for item in _folder_files(root, self.max_bytes, f"kaggle {self.handle}"):
            yield replace(item, url=self._page())


class HuggingFaceSource(Source):
    """A Hugging Face repo (datasets by default) through huggingface_hub; public repos need no token."""

    def __init__(
        self,
        repo_id: str,
        revision: str | None = None,
        repo_type: str = "dataset",
        *,
        max_bytes: int,
        api: Any = None,
        download: Callable[..., str] | None = None,
    ) -> None:
        self.repo_id = repo_id
        self.revision = revision
        self.repo_type = repo_type
        self.locator = f"{repo_id}@{revision or 'main'}"
        self.max_bytes = max_bytes
        self._api = api
        self._download = download

    def _hub(self) -> tuple[Any, Callable[..., str]]:
        if self._api is None or self._download is None:
            import huggingface_hub

            self._api = self._api or huggingface_hub.HfApi()
            self._download = self._download or huggingface_hub.hf_hub_download
        return self._api, self._download

    def _url(self, path: str) -> str:
        prefix = {"dataset": "datasets/", "space": "spaces/"}.get(self.repo_type, "")
        return f"https://huggingface.co/{prefix}{self.repo_id}/resolve/{self.revision or 'main'}/{path}"

    def _failure(self, exc: Exception) -> FetchError:
        """Content-free (module docstring): the hub's message can name the file it choked on."""
        logger.warning("huggingface %s: %s: %s", self.locator, type(exc).__name__, exc)
        return FetchError(f"huggingface {self.repo_id}: {type(exc).__name__}")

    def _fetch_one(self, path: str) -> bytes:
        _, download = self._hub()
        try:
            local = download(
                repo_id=self.repo_id,
                filename=path,
                repo_type=self.repo_type,
                revision=self.revision,
            )
        except Exception as exc:
            raise self._failure(exc) from exc
        # The listing's size skipped the file before the download; the size on disk decides here.
        return _read_capped(Path(local), self.max_bytes, f"huggingface {self.repo_id}")

    def files(self) -> Iterator[SourceFile]:
        api, _ = self._hub()
        try:  # the listing is lazy: sorting it inside the try wraps network errors too
            entries = sorted(
                api.list_repo_tree(
                    self.repo_id,
                    repo_type=self.repo_type,
                    revision=self.revision,
                    recursive=True,
                ),
                key=lambda e: e.path,
            )
        except Exception as exc:
            raise self._failure(exc) from exc
        for entry in entries:
            size = getattr(entry, "size", None)
            if size is None:  # a folder
                continue
            yield SourceFile(
                rel_path=entry.path,
                fetch=partial(self._fetch_one, entry.path),
                size=size,
                url=self._url(entry.path),
            )


def resolve(host: str) -> list[str]:
    """Every address a host name resolves to; a literal address is its own answer."""
    return sorted({info[4][0] for info in socket.getaddrinfo(host, None)})


# IPv6 forms that carry an IPv4 address in their low 32 bits. ``ipaddress`` counts ``::127.0.0.1``
# and ``64:ff9b::127.0.0.1`` as global (the outer prefix is), so the embedded address is checked too.
_IPV4_IN_LOW_32 = (
    ipaddress.ip_network("::/96"),  # IPv4-compatible (deprecated, still parsed)
    ipaddress.ip_network("64:ff9b::/96"),  # NAT64 well-known prefix (RFC 6052)
    ipaddress.ip_network("64:ff9b:1::/48"),  # NAT64 local-use prefix (RFC 8215)
)


def _embedded_ipv4s(ip: ipaddress.IPv6Address) -> list[ipaddress.IPv4Address]:
    """The IPv4 addresses an IPv6 address leads to, if it is a transition form."""
    if ip.sixtofour is not None:  # 2002::/16
        return [ip.sixtofour]
    if ip.teredo is not None:  # 2001::/32: (server, client)
        return list(ip.teredo)
    if any(ip in network for network in _IPV4_IN_LOW_32):
        return [ipaddress.IPv4Address(ip.packed[12:])]
    return []


def is_public(address: str) -> bool:
    """Global unicast, and so is every IPv4 address the address embeds. An IPv4-mapped address
    (``::ffff:a.b.c.d``, how a dual-stack resolver may hand back an IPv4 answer) is its IPv4.
    ``ipaddress`` counts multicast (``224.0.0.0/4``, ``ff00::/8``) and IPv6 site-local
    (``fec0::/10``, deprecated, still what a private network may answer) as global; neither is
    a public host.
    """
    ip = ipaddress.ip_address(address)
    if ip.is_multicast:
        return False
    if isinstance(ip, ipaddress.IPv4Address):
        return ip.is_global
    if ip.is_site_local:
        return False
    if ip.ipv4_mapped is not None:
        return ip.ipv4_mapped.is_global
    embedded = _embedded_ipv4s(ip)
    return ip.is_global and all(v4.is_global for v4 in embedded)


class PinnedBackend(httpcore.NetworkBackend):
    """Dials the address the egress check passed for a host, never a fresh answer from DNS.

    httpcore resolves a host name again when it connects, so a resolver that answers a public
    address to the check and 127.0.0.1 a moment later (DNS rebinding: a record with a zero TTL)
    would get the connection to loopback. The pool asks this backend to connect to the host *name*;
    it connects to the checked address instead. TLS is unchanged: SNI and the certificate check use
    the name, so a certificate for the name is still required at the other end.
    """

    def __init__(
        self,
        pinned: Mapping[str, list[str]],
        inner: httpcore.NetworkBackend | None = None,
    ) -> None:
        self._pinned = (
            pinned  # host -> the addresses the check passed; shared with the checker
        )
        self.inner = inner if inner is not None else httpcore.SyncBackend()

    def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: Any = None,
    ) -> httpcore.NetworkStream:
        addresses = self._pinned.get(host.lower())
        if not addresses:
            raise httpcore.ConnectError(f"{host}: no checked address to connect to")
        *others, last = addresses
        for address in others:
            try:
                return self._dial(address, port, timeout, local_address, socket_options)
            except httpcore.ConnectError:
                continue  # the next checked address may answer
        return self._dial(last, port, timeout, local_address, socket_options)

    def _dial(
        self,
        address: str,
        port: int,
        timeout: float | None,
        local_address: str | None,
        socket_options: Any,
    ) -> httpcore.NetworkStream:
        return self.inner.connect_tcp(
            address,
            port,
            timeout=timeout,
            local_address=local_address,
            socket_options=socket_options,
        )


class _PinnedTransport(httpx.HTTPTransport):
    """httpx's own transport, over a connection pool that dials through a PinnedBackend."""

    def __init__(self, backend: httpcore.NetworkBackend) -> None:
        super().__init__()
        self._pool = httpcore.ConnectionPool(
            ssl_context=httpx.create_ssl_context(), network_backend=backend
        )


class HttpSource(Source):
    """Plain web links, e.g. sample reports a lab publishes.

    The egress rule (outbound only to the connector's listed vendor hosts) is applied to every hop,
    redirects included: https only, a host one of the manifest's URLs names, and never a private or
    local address; anything else is refused before the host is contacted. The connection then goes
    to the address the check passed (``PinnedBackend``), not to whatever the name resolves to a
    moment later. A download has a total deadline on top of the per-read timeout, stops at the size
    cap, and is spooled to an anonymous temporary file chunk by chunk, so memory holds one copy of
    it at most.
    """

    def __init__(
        self,
        urls: list[str],
        *,
        max_bytes: int,
        client: httpx.Client | None = None,
        timeout: float = 60.0,
        deadline: float = DEFAULT_DEADLINE_S,
        resolver: Callable[[str], list[str]] = resolve,
        network: httpcore.NetworkBackend | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.urls = list(urls)
        self.locator = "http"
        self.max_bytes = max_bytes
        self.timeout = timeout
        self.deadline = deadline
        # The allow-list: the hosts the manifest's own URLs name, nothing else.
        self.hosts = frozenset(
            host for url in self.urls if (host := urlparse(url).hostname) is not None
        )
        self._client = client  # a ready client (tests): no pinned transport under it
        self._resolve = resolver
        # What the pinned backend dials with (None: real sockets), and the addresses the check
        # passed for each host, which the backend reads.
        self._network = network
        self._pinned: dict[str, list[str]] = {}
        self._clock = clock

    def files(self) -> Iterator[SourceFile]:
        for url in self.urls:
            name = PurePosixPath(urlparse(url).path).name or "download"
            yield SourceFile(rel_path=name, fetch=partial(self._get, url), url=url)

    def _get(self, url: str) -> bytes:
        if self._client is not None:
            return self._download(self._client, url)
        with httpx.Client(
            transport=_PinnedTransport(PinnedBackend(self._pinned, self._network)),
            timeout=self.timeout,
            headers={"User-Agent": USER_AGENT},
        ) as client:
            return self._download(client, url)

    def _download(self, client: httpx.Client, url: str) -> bytes:
        """Every error names the hop as ``scheme://host`` and the status or the error's type:
        httpx's own messages quote the URL they failed on, a redirect's signed query included.
        """
        started = self._clock()
        request = client.build_request("GET", url)
        for _hop in range(MAX_REDIRECTS + 1):
            where = self._check_hop(request.url)
            try:
                response = client.send(request, stream=True, follow_redirects=False)
            except (httpx.HTTPError, UnicodeError) as exc:
                # UnicodeError: idna's IDNAError, which httpx lets out of ``send`` when a redirect
                # names a malformed punycode label (building the next request reads ``url.host``,
                # which decodes it): this file fails, not the run.
                raise self._failure(where, exc) from exc
            try:
                if response.next_request is None:
                    if not response.is_success:
                        raise FetchError(f"{where}: HTTP {response.status_code}")
                    return self._spool(response, where, started)
                request = response.next_request
            finally:
                response.close()
            self._check_deadline(where, started)
        raise RefusedError(f"{where}: more than {MAX_REDIRECTS} redirects")

    @staticmethod
    def _failure(where: str, exc: Exception) -> FetchError:
        # Not logged either: httpx's message would put a signed redirect URL into the log.
        return FetchError(f"{where}: {type(exc).__name__}")

    def _check_hop(self, url: httpx.URL) -> str:
        """The egress rule for one hop; returns the hop as ``scheme://host``, all any error may quote
        (a redirect's path or query may carry a token). The addresses that pass are pinned.

        The host is taken as httpcore dials it and as the manifest's URLs hold it: ``raw_host``,
        lower-case ASCII with an IDN as punycode. ``url.host`` decodes punycode instead, which
        spells an IDN host differently from the allow-list and the pin, and raises idna's error
        out of the run when a redirect names a malformed label.
        """
        host = url.raw_host.decode("ascii")
        where = f"{url.scheme}://{host}"
        if url.scheme != "https":
            raise RefusedError(f"refused {where}: not https")
        if url.port not in (None, HTTPS_PORT):  # None: the scheme's default port
            raise RefusedError(f"refused {where}: not port {HTTPS_PORT}")
        if host not in self.hosts:
            raise RefusedError(f"refused {where}: not a host the manifest names")
        try:
            addresses = self._resolve(host)
        except OSError as exc:
            raise FetchError(
                f"{where}: cannot resolve the host ({type(exc).__name__})"
            ) from exc
        if not addresses:
            raise FetchError(f"{where}: the host has no address")
        if not all(is_public(address) for address in addresses):
            raise RefusedError(
                f"refused {where}: resolves to a private or local address"
            )
        self._pinned[host] = addresses
        return where

    def _check_deadline(self, where: str, started: float) -> None:
        if self._clock() - started > self.deadline:
            raise FetchError(
                f"{where}: past the {self.deadline:g}-second download deadline"
            )

    def _spool(self, response: httpx.Response, where: str, started: float) -> bytes:
        """Chunks go to an anonymous temporary file as they arrive, the cap and the deadline checked
        on each; the bytes come back in one read, so nothing ever holds two copies."""
        size = 0
        with tempfile.TemporaryFile() as spool:
            try:
                for chunk in response.iter_bytes():
                    size += len(chunk)
                    if size > self.max_bytes:
                        raise FetchError(
                            f"{where}: larger than the {self.max_bytes}-byte cap"
                        )
                    self._check_deadline(where, started)
                    spool.write(chunk)
            except httpx.HTTPError as exc:  # the body stopped coming, or was garbled
                raise self._failure(where, exc) from exc
            spool.seek(0)
            return spool.read()
