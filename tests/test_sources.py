from types import SimpleNamespace

import httpcore
import httpx
import pytest

from src.connectors.documents import sources
from src.connectors.documents.sources import (
    MAX_REDIRECTS,
    FetchError,
    HttpSource,
    HuggingFaceSource,
    KaggleSource,
    LocalDirSource,
    RefusedError,
)

CAP = 100  # the size cap every source takes (the connector passes the cap in force)


def _local(root):
    return LocalDirSource(root, max_bytes=CAP)


def _kaggle(handle, files=None, **kw):
    return KaggleSource(handle, files, max_bytes=CAP, **kw)


def _hf(repo_id, **kw):
    return HuggingFaceSource(repo_id, max_bytes=CAP, **kw)


def _tree(root):
    (root / "sub").mkdir(parents=True)
    (root / "a.pdf").write_bytes(b"A")
    (root / "sub" / "b.pdf").write_bytes(b"BB")
    return root


# ── Local folder ─────────────────────────────────────────────────────────────
def test_local_folder_lists_sorted_posix_paths_lazily(tmp_path):
    source = _local(_tree(tmp_path / "docs"))
    files = list(source.files())
    assert [(f.rel_path, f.size) for f in files] == [("a.pdf", 1), ("sub/b.pdf", 2)]
    assert files[1].fetch() == b"BB"
    assert source.locator == "docs"


def test_a_missing_folder_is_a_fetch_error_that_names_no_path(tmp_path, records):
    # files() is a generator, so nothing runs until the first next().
    files = _local(tmp_path / "missing").files()
    with pytest.raises(FetchError) as caught:
        next(files)
    # A job record keeps this text: the locator, never the path (a clinic's folder names).
    assert str(caught.value) == "local missing: not a folder"
    assert [str(tmp_path / "missing") in r.getMessage() for r in records] == [True]


# ── Kaggle ───────────────────────────────────────────────────────────────────
def test_kaggle_whole_dataset(tmp_path):
    root = _tree(tmp_path / "cache")
    source = _kaggle("owner/dataset", download=lambda handle, path=None: str(root))
    files = list(source.files())
    assert [f.rel_path for f in files] == ["a.pdf", "sub/b.pdf"]
    assert files[0].url == "https://www.kaggle.com/datasets/owner/dataset"


def test_kaggle_selected_files_download_one_at_a_time(tmp_path):
    (tmp_path / "x.pdf").write_bytes(b"X")
    calls = []

    def download(handle, path=None):
        calls.append((handle, path))
        return str(tmp_path / path)

    source = _kaggle("owner/dataset/versions/3", files=["x.pdf"], download=download)
    [item] = list(source.files())
    assert calls == []  # nothing downloaded until fetch
    assert item.fetch() == b"X"
    assert calls == [("owner/dataset/versions/3", "x.pdf")]
    assert item.url == "https://www.kaggle.com/datasets/owner/dataset"


def test_kaggle_failures_become_content_free_fetch_errors(records):
    def broken(handle, path=None):
        raise RuntimeError("403 Forbidden for /cache/Jane-Doe-biopsy.pdf")

    [item] = list(_kaggle("o/d", files=["x.pdf"], download=broken).files())
    with pytest.raises(FetchError) as caught:
        item.fetch()
    assert str(caught.value) == "kaggle o/d: RuntimeError"
    whole_dataset = _kaggle("o/d", download=broken).files()
    with pytest.raises(FetchError) as caught:
        next(whole_dataset)
    assert str(caught.value) == "kaggle o/d: RuntimeError"
    # The library's message, a file name and all, is in the log and nowhere else.
    assert [r.getMessage() for r in records] == [
        "kaggle o/d: RuntimeError: 403 Forbidden for /cache/Jane-Doe-biopsy.pdf"
    ] * 2


def test_kaggle_uses_kagglehub_by_default(monkeypatch, tmp_path):
    import kagglehub

    root = _tree(tmp_path / "cache")
    monkeypatch.setattr(
        kagglehub, "dataset_download", lambda handle, path=None: str(root)
    )
    assert len(list(_kaggle("o/d").files())) == 2


# ── Hugging Face ─────────────────────────────────────────────────────────────
class FakeApi:
    def __init__(self, entries=None, error=None):
        self.entries = entries or []
        self.error = error

    def list_repo_tree(self, repo_id, repo_type, revision, recursive):
        if self.error:
            raise self.error
        return iter(self.entries)


def test_huggingface_lists_files_and_skips_folders(tmp_path):
    (tmp_path / "a.pdf").write_bytes(b"A")
    entries = [
        SimpleNamespace(path="b.pdf", size=5),
        SimpleNamespace(path="reports"),
        SimpleNamespace(path="a.pdf", size=1),
    ]

    def download(repo_id, filename, repo_type, revision):
        return str(tmp_path / filename)

    source = _hf("org/ds", api=FakeApi(entries), download=download)
    files = list(source.files())
    assert [(f.rel_path, f.size) for f in files] == [("a.pdf", 1), ("b.pdf", 5)]
    assert files[0].url == "https://huggingface.co/datasets/org/ds/resolve/main/a.pdf"
    assert files[0].fetch() == b"A"
    assert source.locator == "org/ds@main"


def test_huggingface_model_repos_and_pinned_revisions():
    source = _hf(
        "org/model", revision="abc123", repo_type="model", api=FakeApi(), download=str
    )
    assert (
        source._url("w.pdf") == "https://huggingface.co/org/model/resolve/abc123/w.pdf"
    )
    assert source.locator == "org/model@abc123"


def test_huggingface_failures_become_content_free_fetch_errors(records):
    listing = _hf("o/d", api=FakeApi(error=RuntimeError("gone")), download=str).files()
    with pytest.raises(FetchError) as caught:
        next(listing)
    assert str(caught.value) == "huggingface o/d: RuntimeError"

    def broken(**kw):
        raise RuntimeError("rate limited fetching Jane-Doe-biopsy.pdf")

    entries = [SimpleNamespace(path="a.pdf", size=1)]
    [item] = list(_hf("o/d", api=FakeApi(entries), download=broken).files())
    with pytest.raises(FetchError) as caught:
        item.fetch()
    assert str(caught.value) == "huggingface o/d: RuntimeError"
    assert [r.getMessage() for r in records] == [
        "huggingface o/d@main: RuntimeError: gone",
        "huggingface o/d@main: RuntimeError: rate limited fetching Jane-Doe-biopsy.pdf",
    ]


def test_a_file_over_the_cap_is_never_read_into_memory(tmp_path):
    """Kaggle lists no sizes and a hub listing can understate one: the file on disk decides, before
    any bytes are read. A local folder's listing says the size, and the fetch refuses as well.
    """
    (tmp_path / "big.pdf").write_bytes(b"x" * 5)
    kaggle = KaggleSource(
        "o/d",
        files=["big.pdf"],
        max_bytes=4,
        download=lambda handle, path=None: str(tmp_path / path),
    )
    [item] = list(kaggle.files())
    with pytest.raises(FetchError, match="^kaggle o/d: larger than the 4-byte cap$"):
        item.fetch()
    hub = HuggingFaceSource(
        "o/d",
        max_bytes=4,
        api=FakeApi([SimpleNamespace(path="big.pdf", size=1)]),  # understated
        download=lambda **kw: str(tmp_path / kw["filename"]),
    )
    [item] = list(hub.files())
    with pytest.raises(
        FetchError, match="^huggingface o/d: larger than the 4-byte cap$"
    ):
        item.fetch()
    [item] = list(LocalDirSource(tmp_path, max_bytes=4).files())
    assert item.size == 5
    with pytest.raises(FetchError, match=f"^local {tmp_path.name}: larger than the 4"):
        item.fetch()


def test_huggingface_uses_the_hub_client_by_default(monkeypatch):
    import huggingface_hub

    monkeypatch.setattr(huggingface_hub, "HfApi", lambda: FakeApi())
    monkeypatch.setattr(huggingface_hub, "hf_hub_download", lambda **kw: "unused")
    assert list(_hf("o/d").files()) == []


# ── HTTP ─────────────────────────────────────────────────────────────────────
def _client(handler):
    return httpx.Client(transport=httpx.MockTransport(handler))


def test_http_streams_each_file_and_names_it_from_the_url():
    client = _client(lambda request: httpx.Response(200, content=b"%PDF-1.4 x"))
    source = HttpSource(
        ["https://lab.example/reports/cbc.pdf"],
        max_bytes=100,
        client=client,
        resolver=lambda host: ["93.184.216.34"],
    )
    [item] = list(source.files())
    assert (item.rel_path, item.url) == (
        "cbc.pdf",
        "https://lab.example/reports/cbc.pdf",
    )
    assert item.fetch() == b"%PDF-1.4 x"


def test_http_stops_at_the_size_cap():
    client = _client(lambda request: httpx.Response(200, content=b"x" * 50))
    [item] = list(
        HttpSource(
            ["https://a.example/big.pdf"],
            max_bytes=10,
            client=client,
            resolver=lambda host: ["93.184.216.34"],
        ).files()
    )
    with pytest.raises(
        FetchError, match="^https://a.example: larger than the 10-byte cap$"
    ):
        item.fetch()


def test_http_errors_become_fetch_errors():
    client = _client(lambda request: httpx.Response(404))
    [item] = list(
        HttpSource(
            ["https://a.example/gone.pdf"],
            max_bytes=10,
            client=client,
            resolver=lambda host: ["93.184.216.34"],
        ).files()
    )
    with pytest.raises(FetchError, match="^https://a.example: HTTP 404$"):
        item.fetch()


SIGNED = "https://lab.example/x.pdf?sig=SECRET-TOKEN"


def _signed_redirect_then(outcome):
    """A handler that redirects the manifest's link to a signed URL, which then meets ``outcome``:
    a response, or an exception to raise."""

    def handler(request):
        if not request.url.query:
            return httpx.Response(302, headers={"Location": SIGNED})
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    return handler


@pytest.mark.parametrize(
    "outcome, text",
    [
        (httpx.Response(404), "https://lab.example: HTTP 404"),
        (httpx.Response(304), "https://lab.example: HTTP 304"),  # not a success
        (
            httpx.ConnectError(f"boom for url '{SIGNED}'"),
            "https://lab.example: ConnectError",
        ),
        (
            httpx.ReadTimeout(f"timed out reading '{SIGNED}'"),
            "https://lab.example: ReadTimeout",
        ),
    ],
)
def test_http_errors_quote_the_host_and_the_status_or_type_never_the_url(
    outcome, text, records
):
    """httpx's own messages quote the URL they failed on, a redirect's signed query included, and
    a FetchError's text reaches the job record. Only scheme://host and the status or type do.
    """
    source = HttpSource(
        ["https://lab.example/x.pdf"],
        max_bytes=10,
        client=_client(_signed_redirect_then(outcome)),
        resolver=lambda host: ["93.184.216.34"],
    )
    with pytest.raises(FetchError) as caught:
        _only(source).fetch()
    assert str(caught.value) == text
    assert all("SECRET" not in r.getMessage() for r in records)  # nor in the log


def test_a_body_that_stops_coming_is_a_fetch_error_naming_the_host():
    def body():
        yield b"%PD"
        raise httpx.ReadError(f"connection reset reading '{SIGNED}'")

    source = HttpSource(
        ["https://lab.example/x.pdf"],
        max_bytes=10,
        client=_client(lambda request: httpx.Response(200, content=body())),
        resolver=lambda host: ["93.184.216.34"],
    )
    with pytest.raises(FetchError, match="^https://lab.example: ReadError$"):
        _only(source).fetch()


OK_PDF = b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\n%PDF"


def _redirect(location):
    return f"HTTP/1.1 302 Found\r\nLocation: {location}\r\nContent-Length: 0\r\n\r\n".encode()


class RecordingStream(httpcore.MockStream):
    def __init__(self, buffer, backend):
        super().__init__(buffer)
        self._backend = backend

    def start_tls(self, ssl_context, server_hostname=None, timeout=None):
        self._backend.sni.append(server_hostname)
        return self


class RecordingBackend(httpcore.MockBackend):
    """httpcore's canned-response backend, plus a record of every address it was asked to dial
    (never a socket) and of the name TLS was told to verify."""

    def __init__(self, *responses, refuse=()):
        super().__init__(list(responses))
        self.dials = []
        self.sni = []
        self.refuse = set(refuse)

    def connect_tcp(
        self, host, port, timeout=None, local_address=None, socket_options=None
    ):
        self.dials.append((host, port))
        if host in self.refuse:
            raise httpcore.ConnectError("connection refused")
        return RecordingStream(list(self._buffer), self)


def test_http_without_a_client_connects_to_the_checked_address_by_name(monkeypatch):
    """The source opens its own client: the pool is asked for the name, the socket is opened to the
    address the egress check passed, and TLS still verifies the certificate against the name.
    """
    network = RecordingBackend(OK_PDF)
    [item] = list(
        HttpSource(
            ["https://a.example/"],
            max_bytes=10,
            resolver=lambda host: ["93.184.216.34"],
            network=network,
        ).files()
    )
    assert item.rel_path == "download"
    assert item.fetch() == b"%PDF"
    assert network.dials == [("93.184.216.34", 443)]
    assert network.sni == ["a.example"]


def test_dns_rebinding_between_the_check_and_the_connect_cannot_reach_loopback():
    """A resolver that answers a public address once and 127.0.0.1 from then on (a zero-TTL record):
    the connection goes to the address that was checked, and the next hop's answer is checked afresh
    and refused. Nothing ever dials loopback."""
    answers = iter([["93.184.216.34"], ["127.0.0.1"], ["127.0.0.1"]])
    network = RecordingBackend(_redirect("https://lab.example/y.pdf"), OK_PDF)
    source = HttpSource(
        ["https://lab.example/x.pdf"],
        max_bytes=10,
        resolver=lambda host: next(answers),
        network=network,
    )
    with pytest.raises(RefusedError, match="private or local address"):
        _only(source).fetch()
    assert network.dials == [("93.184.216.34", 443)]


def test_the_next_checked_address_is_tried_when_one_refuses():
    v6, v4 = "2606:2800:220:1:248:1893:25c8:1946", "93.184.216.34"
    network = RecordingBackend(OK_PDF, refuse={v6})
    source = HttpSource(
        ["https://a.example/x.pdf"],
        max_bytes=10,
        resolver=lambda host: [v6, v4],
        network=network,
    )
    assert _only(source).fetch() == b"%PDF"
    assert network.dials == [(v6, 443), (v4, 443)]


def test_a_host_with_no_address_is_a_fetch_error():
    source = HttpSource(
        ["https://a.example/x.pdf"], max_bytes=10, resolver=lambda host: []
    )
    with pytest.raises(FetchError, match="no address"):
        _only(source).fetch()


def test_the_pinned_backend_never_dials_a_host_that_was_not_checked():
    network = RecordingBackend(OK_PDF)
    backend = sources.PinnedBackend({"checked.example": ["93.184.216.34"]}, network)
    with pytest.raises(httpcore.ConnectError, match="no checked address"):
        backend.connect_tcp("other.example", 443)
    assert network.dials == []
    backend.connect_tcp("Checked.example", 443)  # names are case-insensitive
    assert network.dials == [("93.184.216.34", 443)]


def test_the_pinned_backend_dials_real_sockets_by_default():
    assert isinstance(sources.PinnedBackend({}).inner, httpcore.SyncBackend)


# ── HTTP: the egress rule, the deadline, the spool ───────────────────────────
PUBLIC = "93.184.216.34"


def _public(host):
    return [PUBLIC]


def _source(urls, handler, *, resolver=_public, max_bytes=100, **kw):
    return HttpSource(
        urls, max_bytes=max_bytes, client=_client(handler), resolver=resolver, **kw
    )


def _only(source):
    [item] = list(source.files())
    return item


def test_a_redirect_to_a_host_the_manifest_does_not_name_is_refused():
    contacted = []

    def handler(request):
        contacted.append(request.url.host)
        if request.url.host == "lab.example":
            return httpx.Response(302, headers={"Location": "https://cdn.other/x.pdf"})
        return httpx.Response(200, content=b"%PDF")

    item = _only(_source(["https://lab.example/x.pdf"], handler))
    with pytest.raises(RefusedError, match="cdn.other.*not a host the manifest names"):
        item.fetch()
    assert contacted == ["lab.example"]  # the other host was never asked


def test_a_redirect_to_a_host_the_manifest_names_is_followed():
    contacted = []

    def handler(request):
        contacted.append(request.url.host)
        if request.url.host == "lab.example":
            return httpx.Response(
                302, headers={"Location": "https://files.lab.example/x.pdf?t=1"}
            )
        return httpx.Response(200, content=b"%PDF")

    urls = ["https://lab.example/x.pdf", "https://files.lab.example/other.pdf"]
    first, _other = list(_source(urls, handler).files())
    assert first.fetch() == b"%PDF"
    assert contacted == ["lab.example", "files.lab.example"]


def test_an_idn_host_is_checked_pinned_and_dialled_in_its_punycode_form():
    """The manifest holds the punycode form (pydantic writes it so) and httpcore dials that form;
    the check compares and pins the same form, so an IDN source works and its text stays ASCII.
    """
    network = RecordingBackend(OK_PDF)
    source = HttpSource(
        ["https://xn--fiqs8s.example/x.pdf"],
        max_bytes=10,
        resolver=lambda host: [PUBLIC],
        network=network,
    )
    assert _only(source).fetch() == b"%PDF"
    assert network.dials == [(PUBLIC, 443)]
    assert network.sni == ["xn--fiqs8s.example"]
    assert source._pinned == {"xn--fiqs8s.example": [PUBLIC]}


def test_a_redirect_to_a_malformed_punycode_label_fails_that_file_only():
    """idna refuses to decode the label, and httpx lets its error (a UnicodeError) out of ``send``
    while it builds the next request (that reads ``url.host``): the file fails with the hop and
    the type, no path or query, and the run goes on to the next file."""
    handler = lambda request: httpx.Response(  # noqa: E731
        302, headers={"Location": "https://xn--zzz-.example/x.pdf?sig=ABC"}
    )
    with pytest.raises(FetchError) as caught:
        _only(_source(["https://lab.example/x.pdf"], handler)).fetch()
    assert str(caught.value) == "https://lab.example: IDNAError"


@pytest.mark.parametrize(
    "url, location",
    [
        ("http://lab.example/x.pdf", None),  # the manifest's own link is not https
        ("https://lab.example/x.pdf", "http://lab.example/x.pdf"),  # a downgrade
    ],
)
def test_a_hop_that_is_not_https_is_refused(url, location):
    def handler(request):
        if location and request.url.scheme == "https":
            return httpx.Response(302, headers={"Location": location})
        return httpx.Response(200, content=b"%PDF")

    with pytest.raises(RefusedError, match="http://lab.example.*not https"):
        _only(_source([url], handler)).fetch()


@pytest.mark.parametrize(
    "url, location",
    [
        ("https://lab.example:6443/x.pdf", None),  # the manifest's own link
        ("https://lab.example/x.pdf", "https://lab.example:8443/x.pdf"),  # a redirect
    ],
)
def test_a_hop_on_a_port_other_than_443_is_refused(url, location):
    contacted = []

    def handler(request):
        contacted.append(request.url.port)
        if location and request.url.port is None:
            return httpx.Response(302, headers={"Location": location})
        return httpx.Response(200, content=b"%PDF")

    with pytest.raises(RefusedError, match="https://lab.example: not port 443"):
        _only(_source([url], handler)).fetch()
    assert contacted == ([] if location is None else [None])


def test_an_explicit_443_is_the_default_port():
    handler = lambda request: httpx.Response(200, content=b"%PDF")  # noqa: E731
    assert _only(_source(["https://lab.example:443/x.pdf"], handler)).fetch() == b"%PDF"


@pytest.mark.parametrize(
    "address",
    [
        "10.0.0.1",
        "127.0.0.1",
        "169.254.169.254",
        "::1",
        "::ffff:10.0.0.1",
        "fe80::1",
        # Not private to ``ipaddress``, not a public host either.
        "224.0.0.1",  # IPv4 multicast
        "ff02::1",  # IPv6 multicast
        "fec0::1",  # IPv6 site-local (deprecated; a private network may still answer it)
        # IPv6 forms that lead to an IPv4 address: the embedded address decides.
        "::ffff:127.0.0.1",  # IPv4-mapped
        "::127.0.0.1",  # IPv4-compatible
        "64:ff9b::10.0.0.1",  # NAT64, well-known prefix
        "64:ff9b:1::a00:1",  # NAT64, local-use prefix (10.0.0.1)
        "2002:a00:1::",  # 6to4 of 10.0.0.1
        "2001:0:a00:1:0:0:f5ff:fffe",  # Teredo: server and client 10.0.0.1
    ],
)
def test_a_host_at_a_private_or_local_address_is_refused(address):
    contacted = []

    def handler(request):
        contacted.append(request.url.host)
        return httpx.Response(200, content=b"%PDF")

    source = _source(
        ["https://lab.example/x.pdf"], handler, resolver=lambda h: [address]
    )
    with pytest.raises(RefusedError, match="private or local address"):
        _only(source).fetch()
    assert contacted == []


@pytest.mark.parametrize(
    "address",
    [
        "93.184.216.34",
        "2606:2800:220:1:248:1893:25c8:1946",
        "::ffff:93.184.216.34",  # IPv4-mapped: how a dual-stack resolver answers for IPv4
        "::93.184.216.34",  # IPv4-compatible
        "64:ff9b::93.184.216.34",  # NAT64 of a public address
    ],
)
def test_a_public_address_or_a_public_embedded_address_is_allowed(address):
    network = RecordingBackend(OK_PDF)
    source = HttpSource(
        ["https://lab.example/x.pdf"],
        max_bytes=10,
        resolver=lambda host: [address],
        network=network,
    )
    assert _only(source).fetch() == b"%PDF"
    assert network.dials == [(address, 443)]  # the checked form is what is dialled


def test_is_public_checks_the_embedded_address_of_every_transition_form():
    assert (
        sources.is_public("2002:5db8:d822::") is False
    )  # 6to4: the prefix is not global
    assert sources.is_public("2001:0:5db8:d822::a247:27cd") is False  # Teredo: likewise
    assert sources.is_public("64:ff9b::7f00:1") is False  # NAT64 of 127.0.0.1
    assert sources.is_public("::") is False
    assert sources.is_public("100::1") is False  # discard-only, no embedded address


@pytest.mark.parametrize("host", ["localhost", "127.0.0.1", "[::1]"])
def test_the_real_resolver_refuses_local_names_and_literal_addresses(host):
    """The default resolver, without a fake: a local name and literal addresses, no DNS needed."""
    contacted = []

    def handler(request):
        contacted.append(request.url.host)
        return httpx.Response(200, content=b"%PDF")

    source = HttpSource(
        [f"https://{host}/x.pdf"], max_bytes=10, client=_client(handler)
    )
    with pytest.raises(RefusedError, match="private or local address"):
        _only(source).fetch()
    assert contacted == []


def test_an_unresolvable_host_is_a_fetch_error_not_a_refusal():
    def no_dns(host):
        raise OSError("no such host")

    source = _source(
        ["https://lab.example/x.pdf"],
        lambda request: httpx.Response(200),
        resolver=no_dns,
    )
    with pytest.raises(FetchError, match="cannot resolve the host") as caught:
        _only(source).fetch()
    assert not isinstance(caught.value, RefusedError)
    assert str(caught.value) == "https://lab.example: cannot resolve the host (OSError)"


def test_too_many_redirects_are_refused():
    hops = []

    def handler(request):
        hops.append(str(request.url))
        return httpx.Response(302, headers={"Location": "https://lab.example/again"})

    with pytest.raises(
        RefusedError, match="^https://lab.example: more than 5 redirects$"
    ):
        _only(_source(["https://lab.example/x.pdf"], handler)).fetch()
    assert len(hops) == MAX_REDIRECTS + 1


def test_a_download_past_its_deadline_is_abandoned():
    ticks = iter([0.0, 1.0, 301.0])

    def handler(request):
        return httpx.Response(200, content=iter([b"a", b"b", b"c"]))

    source = _source(
        ["https://lab.example/slow.pdf"],
        handler,
        deadline=300.0,
        clock=lambda: next(ticks),
    )
    with pytest.raises(FetchError, match="^https://lab.example: past the 300-second"):
        _only(source).fetch()


def test_a_download_is_spooled_to_a_temporary_file_and_hashed(monkeypatch):
    spools = []
    real_temporary_file = sources.tempfile.TemporaryFile

    def spy(*args, **kw):
        spool = real_temporary_file(*args, **kw)
        spools.append(spool)
        return spool

    monkeypatch.setattr(sources.tempfile, "TemporaryFile", spy)
    content = b"%PDF-1.4 " + b"x" * 50

    def handler(request):
        return httpx.Response(200, content=iter([content[:20], content[20:]]))

    item = _only(_source(["https://lab.example/x.pdf"], handler))
    assert item.fetch() == content
    assert len(spools) == 1 and spools[0].closed  # gone once the bytes are handed on
