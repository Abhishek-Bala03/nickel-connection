"""The File Store's settings (NIK-2209-06): the backend switch (production takes S3 only), and every
S3 setting checked at start-up, the endpoint by the internal-endpoint rule (src.utility.endpoints) and
by what the S3 client takes."""

import dataclasses
import logging
import os
from uuid import UUID

import pytest

from src.config import CONF_FILE, S3Settings, load_settings
from src.db.file_store import LocalFileStore, S3FileStore, open_file_store
from src.handler import documents
from src.utility import endpoints

ACCESS_KEY = "settings-test-access-key"
SECRET = "settings-test-secret-0123456789abcdef"
TENANT = "0192a3b4-0000-7000-8000-000000000001"


def _s3(**variables):
    """Settings for the S3 backend in development: a loopback endpoint and both credentials, then
    ``variables`` (an empty value unsets one). Outside development the credentials come from
    files, as production takes them (conftest's mounted secrets), unless a test names them.
    """
    environ = {
        "NICKEL_ENV": "development",
        "NICKEL_FILE_STORE": "s3",
        "NICKEL_S3_ENDPOINT_URL": "http://127.0.0.1:8333",
        "NICKEL_S3_ACCESS_KEY_ID": ACCESS_KEY,
        "NICKEL_S3_SECRET_ACCESS_KEY": SECRET,
    }
    environ.update(variables)
    if (environ.get("NICKEL_ENV") or "production").lower() != "development":
        for name in ("NICKEL_S3_ACCESS_KEY_ID", "NICKEL_S3_SECRET_ACCESS_KEY"):
            if name not in variables and f"{name}_FILE" not in variables:
                environ[name] = ""
                environ[f"{name}_FILE"] = os.environ[f"{name}_FILE"]
    return load_settings(CONF_FILE, environ={k: v for k, v in environ.items() if v})


# ── The backend switch ───────────────────────────────────────────────────────
@pytest.mark.parametrize("environment", ["development", "qa"])
@pytest.mark.parametrize("backend", ["", "local", "LOCAL"])
def test_outside_production_the_file_store_is_local_unless_s3_is_asked_for(
    environment, backend
):
    environ = {
        "NICKEL_ENV": environment,
        "NICKEL_FILE_STORE": backend,
        "NICKEL_S3_ENDPOINT_URL": "not even a URL",
    }
    # The S3 settings are neither read nor checked for the local backend.
    assert load_settings(CONF_FILE, environ=environ).s3 is None


def test_production_keeps_the_originals_in_s3_by_default_and_refuses_local():
    """A K3s pod that sets no NICKEL_FILE_STORE must not write PHI to its own disk."""
    # Production, the default: the S3 File Store, and none given.
    with pytest.raises(ValueError, match="NICKEL_S3_ENDPOINT_URL is not set"):
        load_settings(CONF_FILE, environ={})
    in_production = _s3(NICKEL_ENV="production", NICKEL_FILE_STORE="")
    assert in_production.s3.require_https is True
    for environ in ({"NICKEL_FILE_STORE": "local"}, {"NICKEL_ENV": "PRODUCTION"}):
        with pytest.raises(
            ValueError, match="NICKEL_FILE_STORE=local is for development"
        ):
            load_settings(CONF_FILE, environ={"NICKEL_FILE_STORE": "local", **environ})


def test_an_unknown_backend_is_refused_by_name():
    with pytest.raises(ValueError, match="file store 'minio' is not one of local, s3"):
        load_settings(CONF_FILE, environ={"NICKEL_FILE_STORE": "minio"})


def test_the_s3_defaults():
    assert _s3().s3 == S3Settings(
        endpoint_url="http://127.0.0.1:8333",
        region="us-east-1",
        originals_bucket="originals",
        access_key_id=ACCESS_KEY,
        secret_access_key=SECRET,
        ca_bundle=None,
        allow_public_endpoint=False,
        require_https=False,
        call_deadline_s=300,
    )


def test_open_file_store_follows_the_settings(tmp_path):
    local = dataclasses.replace(_s3(), s3=None, data_dir=tmp_path)
    store = open_file_store(local)
    assert isinstance(store, LocalFileStore)
    assert store.root == tmp_path / "filestore"
    s3 = open_file_store(_s3(NICKEL_S3_ORIGINALS_BUCKET="nickel-originals"))
    assert isinstance(s3, S3FileStore)
    assert s3.bucket == "nickel-originals"


def test_the_ingest_bridge_stores_where_the_settings_say(tmp_path):
    settings = dataclasses.replace(_s3(), data_dir=tmp_path, tenant_id=UUID(TENANT))
    with documents.ingest_bridge(settings) as bridge:
        assert isinstance(bridge.store, S3FileStore)
    with documents.ingest_bridge(dataclasses.replace(settings, s3=None)) as bridge:
        assert isinstance(bridge.store, LocalFileStore)


# ── The endpoint ─────────────────────────────────────────────────────────────
def test_an_s3_store_without_an_endpoint_is_refused():
    with pytest.raises(ValueError, match="NICKEL_S3_ENDPOINT_URL is not set"):
        _s3(NICKEL_S3_ENDPOINT_URL="")


@pytest.mark.parametrize(
    ("url", "problem"),
    [
        ("seaweedfs:8333", "must be an http or https URL with a host"),
        ("ftp://10.0.0.5", "must be an http or https URL with a host"),
        ("http://:8333", "must be an http or https URL with a host"),
        ("http://10.0.0.5:8333/originals", "nothing after it"),
        ("http://10.0.0.5:8333?x=1", "nothing after it"),
        ("http://10.0.0.5:8333#x", "nothing after it"),
        ("http://10.0.0.5:99999", "port that is not valid"),
        ("http://10.0.0.5:0", "port that is not valid"),
        ("http://10.0.0.5:s3", "port that is not valid"),
    ],
)
def test_an_endpoint_that_is_not_scheme_host_port_is_refused(url, problem):
    with pytest.raises(ValueError, match=f"^NICKEL_S3_ENDPOINT_URL .*{problem}"):
        _s3(NICKEL_S3_ENDPOINT_URL=url)


def test_an_endpoint_that_carries_a_user_and_password_is_refused_without_quoting_it():
    with pytest.raises(ValueError, match="must not carry a user or password") as caught:
        _s3(NICKEL_S3_ENDPOINT_URL="http://admin:hunter2-secret@10.0.0.5:8333")
    assert "hunter2" not in str(caught.value)
    assert "admin" not in str(caught.value)


@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1:8333",
        "http://127.8.9.10:8333",
        "http://10.43.0.12:9000",
        "http://172.16.0.1:8333",
        "http://172.31.255.254:8333",
        "http://192.168.1.20:8333",
        "http://100.64.0.1:8333",  # the tailnet
        "http://100.127.255.254:8333",
        "http://[::1]:8333",
        "http://[fd00::1]:8333",  # IPv6 unique local
        "http://[::ffff:10.0.0.5]:8333",  # IPv4-mapped: its IPv4 decides
        "https://10.0.0.5",
        "http://10.0.0.5:8333/",  # a trailing slash is not a path
    ],
)
def test_a_loopback_private_or_tailnet_endpoint_is_accepted(url):
    assert _s3(NICKEL_S3_ENDPOINT_URL=url).s3.endpoint_url == url


@pytest.mark.parametrize(
    "address",
    [
        "93.184.216.34",  # public
        "[2606:2800:220:1:248:1893:25c8:1946]",  # public IPv6
        "169.254.169.254",  # link-local: the cloud's instance metadata service
        "[fe80::1]",  # IPv6 link-local
        "172.32.0.1",  # just past 172.16.0.0/12
        "100.128.0.1",  # just past 100.64.0.0/10
        # Unspecified: a test value that must be refused, not an address bound to.
        "0.0.0.0",  # nosec B104
        "224.0.0.1",  # multicast
        "[::ffff:8.8.8.8]",  # IPv4-mapped public
        "[64:ff9b::a00:5]",  # NAT64 of 10.0.0.5: out through a gateway
        "[2002:a00:5::]",  # 6to4 of 10.0.0.5: out through a relay
        "[fec0::1]",  # IPv6 site-local (deprecated)
    ],
)
def test_any_other_address_is_refused(address):
    with pytest.raises(
        ValueError,
        match="is not a loopback, private or tailnet address.*"
        "NICKEL_S3_ALLOW_PUBLIC_ENDPOINT=true, with https",
    ):
        _s3(NICKEL_S3_ENDPOINT_URL=f"http://{address}:8333")


def test_a_name_is_checked_by_every_address_it_resolves_to(monkeypatch):
    answers = {
        "seaweedfs": ["172.18.0.2"],  # a compose network
        "dual": ["172.18.0.3", "fd00::3"],
        # One public answer is enough to refuse.
        "split": ["172.18.0.4", "93.184.216.34"],
        "nothing": [],
    }
    asked = []

    def fake_resolve(host):
        asked.append(host)
        return answers[host]

    monkeypatch.setattr(endpoints, "resolve", fake_resolve)
    assert _s3(NICKEL_S3_ENDPOINT_URL="http://seaweedfs:8333").s3
    # The name as DNS spells it: lower case, no trailing dot.
    assert _s3(NICKEL_S3_ENDPOINT_URL="http://Dual.:8333").s3
    for host in ("split", "nothing"):
        with pytest.raises(ValueError, match=f"http://{host}:8333 is not a loopback"):
            _s3(NICKEL_S3_ENDPOINT_URL=f"http://{host}:8333")
    assert asked == ["seaweedfs", "dual", "split", "nothing"]


def test_the_real_resolver_takes_localhost_to_loopback():
    """No fake: localhost resolves without DNS, to 127.0.0.1 and maybe ::1, both loopback."""
    assert set(endpoints.resolve("localhost")) <= {"127.0.0.1", "::1"}
    assert _s3(NICKEL_S3_ENDPOINT_URL="http://localhost:8333").s3


def test_a_cluster_name_is_held_to_the_same_rule(monkeypatch):
    """A name under .local or .internal is a name like any other: what it resolves to decides."""
    answers = {
        "rustfs.nickel.svc.cluster.local": ["10.43.0.12"],
        "seaweedfs.internal": ["172.18.0.2"],
        "s3.amazonaws.com.local": ["52.216.0.1"],
    }
    monkeypatch.setattr(endpoints, "resolve", lambda host: answers[host])
    for host in ("rustfs.nickel.svc.cluster.local", "seaweedfs.internal"):
        assert _s3(NICKEL_S3_ENDPOINT_URL=f"http://{host}:9000").s3
    with pytest.raises(ValueError, match="is not a loopback, private or tailnet"):
        _s3(NICKEL_S3_ENDPOINT_URL="http://s3.amazonaws.com.local:9000")


@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1:8333",
        "http://localhost:8333",  # a name, resolved: loopback only
        "http://[::1]:8333",
        "https://10.43.0.12:9000",
        "https://rustfs.nickel.svc.cluster.local:9000",
    ],
)
def test_production_takes_https_or_loopback(monkeypatch, url):
    # Addresses and localhost need no DNS; the cluster name is faked.
    real = endpoints.resolve
    monkeypatch.setattr(
        endpoints,
        "resolve",
        lambda host: ["10.43.0.12"] if host.endswith(".local") else real(host),
    )
    assert _s3(NICKEL_ENV="production", NICKEL_S3_ENDPOINT_URL=url).s3


@pytest.mark.parametrize(
    "url",
    [
        "http://192.168.1.20:8333",
        "http://10.43.0.12:9000",
        "http://100.101.102.103:8333",
        "http://rustfs.nickel.svc.cluster.local:9000",
    ],
)
def test_only_development_takes_plain_http_off_loopback(monkeypatch, url):
    """The originals, and the credentials' signatures, would cross the network in clear, and a
    checked name that resolves elsewhere later would get them: TLS (the name's certificate) is
    what stops that. There is no override for it: a private CA comes in through
    NICKEL_S3_CA_BUNDLE."""
    monkeypatch.setattr(endpoints, "resolve", lambda host: ["10.43.0.12"])
    for environment in ("production", "qa"):
        with pytest.raises(
            ValueError, match="plain http to a host that is not loopback"
        ):
            _s3(NICKEL_ENV=environment, NICKEL_S3_ENDPOINT_URL=url)
    # A development stack alone takes it (compose's http://seaweedfs:8333).
    assert _s3(NICKEL_ENV="development", NICKEL_S3_ENDPOINT_URL=url).s3


def test_a_name_that_resolves_to_loopback_and_further_is_not_loopback(monkeypatch):
    monkeypatch.setattr(endpoints, "resolve", lambda host: ["127.0.0.1", "10.0.0.5"])
    with pytest.raises(ValueError, match="plain http to a host that is not loopback"):
        _s3(NICKEL_ENV="production", NICKEL_S3_ENDPOINT_URL="http://store:8333")


def test_an_endpoint_the_s3_client_refuses_is_refused_at_start_up(monkeypatch):
    """botocore refuses a host name with an underscore (a compose service called seaweed_fs):
    at start-up, not at every job."""
    monkeypatch.setattr(endpoints, "resolve", lambda host: ["172.18.0.9"])
    with pytest.raises(ValueError, match="has a host name the S3 client refuses"):
        _s3(NICKEL_S3_ENDPOINT_URL="http://seaweed_fs:8333")


def test_a_name_that_does_not_resolve_is_refused_with_the_error_type(monkeypatch):
    def no_such_host(host):
        raise OSError("[Errno 11001] getaddrinfo failed")

    monkeypatch.setattr(endpoints, "resolve", no_such_host)
    with pytest.raises(ValueError) as caught:
        _s3(NICKEL_S3_ENDPOINT_URL="http://seaweedfs:8333")
    assert str(caught.value) == (
        "NICKEL_S3_ENDPOINT_URL: cannot resolve http://seaweedfs:8333 (OSError)"
    )


def test_the_override_allows_a_public_https_endpoint_and_is_logged(records):
    settings = _s3(
        NICKEL_S3_ENDPOINT_URL="https://s3.us-east-1.amazonaws.com",
        NICKEL_S3_ALLOW_PUBLIC_ENDPOINT="TRUE",
    )
    assert settings.s3.allow_public_endpoint is True
    assert isinstance(open_file_store(settings), S3FileStore)
    [warning] = [r for r in records if r.levelno == logging.WARNING]
    assert warning.getMessage() == (
        "file store: NICKEL_S3_ALLOW_PUBLIC_ENDPOINT is on, so the S3 endpoint "
        "https://s3.us-east-1.amazonaws.com was not held to loopback, private or tailnet "
        "addresses"
    )


def test_an_endpoint_under_the_rule_is_not_logged(records):
    open_file_store(_s3())
    assert records == []


def test_the_override_still_refuses_plain_http():
    with pytest.raises(ValueError, match="is not https, and NICKEL_S3_ALLOW_PUBLIC"):
        _s3(
            NICKEL_S3_ENDPOINT_URL="http://93.184.216.34:8333",
            NICKEL_S3_ALLOW_PUBLIC_ENDPOINT="true",
        )


def test_the_override_takes_true_or_false_only():
    assert (
        _s3(NICKEL_S3_ALLOW_PUBLIC_ENDPOINT="false").s3.allow_public_endpoint is False
    )
    with pytest.raises(ValueError, match="must be true or false, not 'yes'"):
        _s3(NICKEL_S3_ALLOW_PUBLIC_ENDPOINT="yes")


# ── Bucket, region and CA bundle ─────────────────────────────────────────────
@pytest.mark.parametrize(
    "bucket",
    ["Originals", "ab", "a" * 64, "-originals", "originals-", "orig..inals", "a_b"],
)
def test_a_bucket_name_s3_would_refuse_is_refused_at_start_up(bucket):
    with pytest.raises(ValueError, match="is not a bucket name"):
        _s3(NICKEL_S3_ORIGINALS_BUCKET=bucket)


@pytest.mark.parametrize("region", ["US East", "us_east_1", "us-east-1/", "-us"])
def test_a_region_that_is_not_a_region_name_is_refused(region):
    with pytest.raises(ValueError, match="is not a region name"):
        _s3(NICKEL_S3_REGION=region)


def test_the_call_deadline_is_a_whole_number_of_seconds():
    assert _s3(NICKEL_S3_CALL_DEADLINE_SECONDS="45").s3.call_deadline_s == 45
    for raw, problem in (("soon", "a whole number"), ("0", "a positive number")):
        with pytest.raises(
            ValueError, match=f"NICKEL_S3_CALL_DEADLINE_SECONDS must be {problem}"
        ):
            _s3(NICKEL_S3_CALL_DEADLINE_SECONDS=raw)


def test_the_ca_bundle_must_be_a_file(tmp_path):
    bundle = tmp_path / "ca.pem"
    bundle.write_text("-----BEGIN CERTIFICATE-----\n", encoding="utf-8")
    assert _s3(NICKEL_S3_CA_BUNDLE=str(bundle)).s3.ca_bundle == bundle
    with pytest.raises(ValueError, match="NICKEL_S3_CA_BUNDLE .* is not a file"):
        _s3(NICKEL_S3_CA_BUNDLE=str(tmp_path))


# ── Credentials ──────────────────────────────────────────────────────────────
@pytest.mark.parametrize(
    ("missing", "named"),
    [
        (
            {"NICKEL_S3_ACCESS_KEY_ID": "", "NICKEL_S3_SECRET_ACCESS_KEY": ""},
            "NICKEL_S3_ACCESS_KEY_ID and NICKEL_S3_SECRET_ACCESS_KEY not set",
        ),
        ({"NICKEL_S3_ACCESS_KEY_ID": ""}, "^NICKEL_S3_ACCESS_KEY_ID not set"),
        ({"NICKEL_S3_SECRET_ACCESS_KEY": ""}, "^NICKEL_S3_SECRET_ACCESS_KEY not set"),
    ],
)
def test_the_credentials_come_from_the_settings_or_nowhere(missing, named):
    with pytest.raises(ValueError, match=named):
        _s3(**missing)


def test_the_credentials_can_come_from_mounted_secret_files(tmp_path):
    key_file, secret_file = tmp_path / "key", tmp_path / "secret"
    key_file.write_text(f"{ACCESS_KEY}-from-vault\n", encoding="utf-8")
    secret_file.write_text(f"  {SECRET}-from-vault\n", encoding="utf-8")
    settings = _s3(
        NICKEL_S3_ACCESS_KEY_ID="",
        NICKEL_S3_ACCESS_KEY_ID_FILE=str(key_file),
        NICKEL_S3_SECRET_ACCESS_KEY="",
        NICKEL_S3_SECRET_ACCESS_KEY_FILE=str(secret_file),
    ).s3
    assert settings.access_key_id == f"{ACCESS_KEY}-from-vault"
    assert settings.secret_access_key == f"{SECRET}-from-vault"


def test_a_credential_in_the_environment_and_in_a_file_is_refused(tmp_path):
    secret_file = tmp_path / "secret"
    secret_file.write_text("other", encoding="utf-8")
    with pytest.raises(
        ValueError,
        match="set NICKEL_S3_SECRET_ACCESS_KEY or NICKEL_S3_SECRET_ACCESS_KEY_FILE, not both",
    ) as caught:
        _s3(NICKEL_S3_SECRET_ACCESS_KEY_FILE=str(secret_file))
    assert SECRET not in str(caught.value)


def test_a_secret_file_written_with_a_byte_order_mark_is_read_without_it(tmp_path):
    """PowerShell 5.1's Set-Content -Encoding UTF8 writes one: kept, it would be part of the key
    (a 403 at every call) or of the service token (a 401 at every request)."""
    key_file, token_file = tmp_path / "key", tmp_path / "token"
    key_file.write_bytes(b"\xef\xbb\xbf" + ACCESS_KEY.encode() + b"\r\n")
    token = "a-token-0123456789abcdef0123456789abcdef"
    token_file.write_bytes(b"\xef\xbb\xbf" + token.encode())
    settings = _s3(
        NICKEL_S3_ACCESS_KEY_ID="",
        NICKEL_S3_ACCESS_KEY_ID_FILE=str(key_file),
        NICKEL_SERVICE_TOKEN_FILE=str(token_file),
    )
    assert settings.s3.access_key_id == ACCESS_KEY
    assert settings.service_token == token


def test_a_secret_file_that_is_not_utf_8_is_refused_by_name_without_its_bytes(tmp_path):
    key_file = tmp_path / "key"
    key_file.write_bytes(b"AKIA\xa3PROBE")
    with pytest.raises(ValueError) as caught:
        _s3(NICKEL_S3_ACCESS_KEY_ID="", NICKEL_S3_ACCESS_KEY_ID_FILE=str(key_file))
    assert str(caught.value) == "NICKEL_S3_ACCESS_KEY_ID_FILE is not UTF-8 text"
    assert caught.value.__cause__ is None


def test_a_secret_file_that_cannot_be_read_is_refused_by_name(tmp_path):
    with pytest.raises(
        ValueError, match="^NICKEL_S3_ACCESS_KEY_ID_FILE cannot be read"
    ):
        _s3(
            NICKEL_S3_ACCESS_KEY_ID="",
            NICKEL_S3_ACCESS_KEY_ID_FILE=str(tmp_path / "nope"),
        )


def test_the_settings_never_show_the_s3_credentials():
    settings = _s3()
    for text in (repr(settings), str(settings), repr(settings.s3), str(settings.s3)):
        assert ACCESS_KEY not in text
        assert SECRET not in text


@pytest.mark.parametrize("environment", ["qa", "production"])
@pytest.mark.parametrize(
    "name", ["NICKEL_S3_ACCESS_KEY_ID", "NICKEL_S3_SECRET_ACCESS_KEY"]
)
def test_outside_development_the_s3_credentials_come_from_files(environment, name):
    """Env credentials are for a development stack only: a deployment mounts them (Vault, a CSI
    secret) and names the file. The error names the variable, never the value."""
    with pytest.raises(ValueError) as caught:
        _s3(
            NICKEL_ENV=environment, **{name: "from-the-environment", f"{name}_FILE": ""}
        )
    assert str(caught.value).startswith(f"{name} is for development: in {environment}")
    assert "from-the-environment" not in str(caught.value)
    # the same from a file is taken, and in development the variable itself is too
    assert _s3(NICKEL_ENV=environment).s3.access_key_id
    assert _s3(**{name: "from-the-environment"}).s3
