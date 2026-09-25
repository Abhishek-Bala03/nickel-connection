"""Internal endpoints: the rule for a service URL the connection layer sends originals and credentials
to, such as the File Store's S3 API (``NICKEL_S3_ENDPOINT_URL``).

Such a service lives inside the platform, so every address its host resolves to must be a loopback
address, a private one (RFC 1918 ``10.0.0.0/8``, ``172.16.0.0/12`` and ``192.168.0.0/16``; IPv6
unique local ``fc00::/7``) or one in the shared address space the tailnet uses (RFC 6598
``100.64.0.0/10``). A literal address is its own answer; a name is resolved, whatever it ends in
(``rustfs.nickel.svc.cluster.local`` or ``seaweedfs`` on a compose network alike). With
``https_off_loopback`` (production), plain http is taken to loopback only: the originals never cross
a network in clear.

Anything else is refused before the service is contacted, unless the deployment sets the explicit
override; a public endpoint must then be https, and the service logs that the rule is off. This
mirrors the vendor egress rule (``src.connectors.documents.sources``), which requires the opposite:
a vendor host must resolve to public addresses only. The endpoint is the operator's setting, not a
vendor's or a redirect's, so it is not pinned: it is checked at start-up and again each time a job
opens the store, so a name that has come to resolve elsewhere fails that job.

No error quotes the URL as given: a URL can carry a user and password. They name the setting and,
once the URL is known to carry neither, ``scheme://host[:port]``.
"""

from __future__ import annotations

import ipaddress
import socket
from urllib.parse import SplitResult, urlsplit

# The ranges of the rule (module docstring), not hosts to connect to.
INTERNAL_NETWORKS = tuple(
    ipaddress.ip_network(network)
    for network in (
        "127.0.0.0/8",
        "10.0.0.0/8",  # NOSONAR(S1313)
        "172.16.0.0/12",  # NOSONAR(S1313)
        "192.168.0.0/16",  # NOSONAR(S1313)
        "100.64.0.0/10",  # NOSONAR(S1313)
        "::1/128",
        "fc00::/7",
    )
)


def resolve(host: str) -> list[str]:
    """Every address a host name resolves to; a literal address is its own answer."""
    return sorted({info[4][0] for info in socket.getaddrinfo(host, None)})


def _address(address: str) -> ipaddress.IPv4Address | ipaddress.IPv6Address:
    """The address, an IPv4-mapped IPv6 one (``::ffff:a.b.c.d``) as its IPv4. Every other IPv6
    form of an IPv4 address (6to4, Teredo, NAT64) leads through a relay or a gateway, so it stays
    the IPv6 address it is and matches no internal range."""
    ip = ipaddress.ip_address(address)
    if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped is not None:
        return ip.ipv4_mapped
    return ip


def is_internal(address: str) -> bool:
    """Loopback, private or tailnet (``INTERNAL_NETWORKS``)."""
    ip = _address(address)
    return any(ip in network for network in INTERNAL_NETWORKS)


def _parts(url: str, setting: str) -> SplitResult:
    """The URL's parts, once it is ``http(s)://host[:port]``: no user or password, and nothing
    after the port."""
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise ValueError(f"{setting} must be an http or https URL with a host")
    if parts.username is not None or parts.password is not None:
        raise ValueError(
            f"{setting} must not carry a user or password: the credentials are settings of "
            "their own"
        )
    if parts.path not in ("", "/") or parts.query or parts.fragment:
        raise ValueError(
            f"{setting} must be scheme://host[:port], with nothing after it"
        )
    try:
        valid_port = parts.port is None or parts.port > 0
    except ValueError:  # not a number, or over 65535
        valid_port = False
    if not valid_port:
        raise ValueError(f"{setting} has a port that is not valid")
    return parts


def check_internal_endpoint(
    url: str,
    *,
    setting: str,
    override: str,
    allow_public: bool,
    https_off_loopback: bool,
) -> None:
    """Refuse ``url`` (the value of ``setting``) unless it passes the rule in the module docstring.
    ``allow_public`` is the value of ``override``, the explicit setting that lifts the address
    rule; ``https_off_loopback`` refuses plain http to anything but loopback."""
    parts = _parts(url, setting)
    where = f"{parts.scheme}://{parts.netloc}"
    if allow_public:
        if parts.scheme != "https":
            raise ValueError(
                f"{setting}: {where} is not https, and {override} lets only an https "
                "endpoint be public"
            )
        return
    try:
        addresses = resolve(parts.hostname.rstrip("."))
    except OSError as exc:
        raise ValueError(
            f"{setting}: cannot resolve {where} ({type(exc).__name__})"
        ) from exc
    if not addresses or not all(is_internal(address) for address in addresses):
        raise ValueError(
            f"{setting}: {where} is not a loopback, private or tailnet address, so it is "
            f"outside the platform ({override}=true, with https, allows a public endpoint)"
        )
    if (
        https_off_loopback
        and parts.scheme != "https"
        and not all(_address(address).is_loopback for address in addresses)
    ):
        raise ValueError(
            f"{setting}: {where} is plain http to a host that is not loopback, which "
            "production refuses: use https"
        )
