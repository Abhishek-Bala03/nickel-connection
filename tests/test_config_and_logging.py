"""Settings come from the shipped conf/application.conf and the environment; the logger writes once."""

import dataclasses
import logging
import re
from pathlib import Path
from uuid import UUID

import pytest

import main
from src import __version__
from src.config import (
    CONF_FILE,
    DEFAULT_MAX_FILE_BYTES,
    MANIFEST_DIR,
    S3Settings,
    load_settings,
)
from src.logging import LIBRARY_LEVELS, LOG_FORMAT, LOGGER_NAME, build_logger, logger

TENANT = "0192a3b4-0000-7000-8000-000000000001"
TOKEN = "config-test-token-0123456789abcdef0123456789abcdef"
EVERY_VARIABLE = {
    "NICKEL_ENV": "qa",
    "NICKEL_LOG_LEVEL": "debug",
    "NICKEL_CORS_ORIGINS": "http://a.example, http://b.example ,",
    "NICKEL_SERVICE_TOKEN": TOKEN,
    "APP_VERSION": "1.4.0",
    "NICKEL_TENANT_ID": TENANT,
    "NICKEL_MAX_FILE_BYTES": "123",
    "NICKEL_MANIFEST_DIR": "somewhere/manifests",
    "NICKEL_DATA_DIR": "somewhere",
    "NICKEL_JOBS_MAX_QUEUED": "5",
    "NICKEL_FILE_STORE": "S3",
    "NICKEL_S3_ENDPOINT_URL": "http://10.1.2.3:8333",
    "NICKEL_S3_ALLOW_PUBLIC_ENDPOINT": "false",
    "NICKEL_S3_REGION": "eu-west-2",
    "NICKEL_S3_ORIGINALS_BUCKET": "nickel-originals",
    "NICKEL_S3_ACCESS_KEY_ID": "config-test-access-key",
    "NICKEL_S3_SECRET_ACCESS_KEY": "config-test-secret-0123456789abcdef",
    "NICKEL_S3_CA_BUNDLE": str(CONF_FILE),  # any file: the path is checked, not parsed
    "NICKEL_S3_CALL_DEADLINE_SECONDS": "120",
}
# Read from a mounted secret file instead of the variable; tested on their own.
SECRET_FILES = {
    "NICKEL_SERVICE_TOKEN_FILE",
    "NICKEL_S3_ACCESS_KEY_ID_FILE",
    "NICKEL_S3_SECRET_ACCESS_KEY_FILE",
}
# What production, the default, needs besides: the S3 File Store (http on loopback only).
PRODUCTION_S3 = {
    "NICKEL_S3_ENDPOINT_URL": "http://127.0.0.1:8333",
    "NICKEL_S3_ACCESS_KEY_ID": "config-test-access-key",
    "NICKEL_S3_SECRET_ACCESS_KEY": "config-test-secret-0123456789abcdef",
}
# The settings tests below that are not about the environment run as development.
DEVELOPMENT = {"NICKEL_ENV": "development"}


def test_the_shipped_configuration_falls_back_to_the_defaults():
    settings = load_settings(CONF_FILE, environ=PRODUCTION_S3)
    assert settings.name == "nickel-connection"
    # production unless set: a deployment that forgets NICKEL_ENV gets the API docs off.
    assert settings.environment == "production"
    assert settings.version == __version__
    assert settings.log_level == "INFO"
    assert settings.cors_origins == ()
    assert settings.service_token == ""  # main.create_app refuses to start on it
    assert settings.tenant_id is None
    assert settings.max_file_bytes == DEFAULT_MAX_FILE_BYTES == 100 * 1024 * 1024
    assert settings.manifest_dir == MANIFEST_DIR
    assert settings.data_dir == Path(".data")
    assert settings.max_queued_jobs == 100
    # Production keeps the originals in the S3 File Store.
    assert settings.s3 == S3Settings(
        endpoint_url="http://127.0.0.1:8333",
        region="us-east-1",
        originals_bucket="originals",
        access_key_id="config-test-access-key",
        secret_access_key="config-test-secret-0123456789abcdef",
        ca_bundle=None,
        allow_public_endpoint=False,
        require_https=True,
        call_deadline_s=300,
    )


def test_the_shipped_configuration_reads_every_variable():
    settings = load_settings(CONF_FILE, environ=EVERY_VARIABLE)
    assert settings.environment == "qa"
    assert settings.log_level == "DEBUG"
    assert settings.cors_origins == ("http://a.example", "http://b.example")
    assert settings.service_token == TOKEN
    assert settings.version == "1.4.0"
    assert settings.tenant_id == UUID(TENANT)
    assert settings.max_file_bytes == 123
    assert settings.manifest_dir == Path("somewhere/manifests")
    assert settings.data_dir == Path("somewhere")
    assert settings.max_queued_jobs == 5
    assert settings.s3 == S3Settings(
        endpoint_url="http://10.1.2.3:8333",
        region="eu-west-2",
        originals_bucket="nickel-originals",
        access_key_id="config-test-access-key",
        secret_access_key="config-test-secret-0123456789abcdef",
        ca_bundle=CONF_FILE,
        allow_public_endpoint=False,
        require_https=False,  # qa: plain http to a private host is taken
        call_deadline_s=120,
    )


def test_the_shipped_configuration_names_no_other_variables():
    # Every ${VAR} the file reads is set above (the secret files apart, tested on their own), so a
    # setting added without a test, or one left behind by a removed feature, shows up here.
    text = CONF_FILE.read_text(encoding="utf-8")
    named = set(re.findall(r"^\w+ = \$\{(\w+)\}$", text, flags=re.MULTILINE))
    expected = (set(EVERY_VARIABLE) - {"APP_VERSION"}) | SECRET_FILES
    assert named == expected


def test_the_service_token_can_come_from_a_mounted_secret_file(tmp_path):
    secret = tmp_path / "nickel-service-token"
    secret.write_text(f"  {TOKEN}\n", encoding="utf-8")  # a trailing newline is usual
    environ = {**DEVELOPMENT, "NICKEL_SERVICE_TOKEN_FILE": str(secret)}
    assert load_settings(CONF_FILE, environ=environ).service_token == TOKEN


def test_a_missing_token_file_is_refused_by_name(tmp_path):
    environ = {"NICKEL_SERVICE_TOKEN_FILE": str(tmp_path / "nope")}
    with pytest.raises(ValueError, match="NICKEL_SERVICE_TOKEN_FILE"):
        load_settings(CONF_FILE, environ=environ)


def test_a_token_both_in_the_environment_and_in_a_file_is_refused(tmp_path):
    secret = tmp_path / "token"
    secret.write_text("other", encoding="utf-8")
    environ = {"NICKEL_SERVICE_TOKEN": TOKEN, "NICKEL_SERVICE_TOKEN_FILE": str(secret)}
    with pytest.raises(ValueError, match="not both"):
        load_settings(CONF_FILE, environ=environ)


def test_the_settings_never_show_the_token():
    settings = load_settings(
        CONF_FILE, environ={**DEVELOPMENT, "NICKEL_SERVICE_TOKEN": TOKEN}
    )
    assert TOKEN not in repr(settings)
    assert TOKEN not in str(settings)


def test_values_in_the_file_win_over_the_defaults(tmp_path):
    conf = tmp_path / "application.conf"
    conf.write_text(
        "[SERVICE]\nname = other-service\nenvironment = qa\n", encoding="utf-8"
    )
    settings = load_settings(conf, environ={})
    assert (settings.name, settings.environment) == ("other-service", "qa")


def test_environment_values_are_taken_literally():
    origins = "http://a.example/%41,http://b.example/${NICKEL_ENV}"
    settings = load_settings(
        CONF_FILE, environ={**DEVELOPMENT, "NICKEL_CORS_ORIGINS": origins}
    )
    assert settings.cors_origins == tuple(origins.split(","))


def test_the_environment_name_is_case_insensitive():
    assert load_settings(CONF_FILE, environ={"NICKEL_ENV": "QA"}).environment == "qa"


@pytest.mark.parametrize(
    ("variable", "value", "message"),
    [
        ("NICKEL_LOG_LEVEL", "LOUD", "log level 'LOUD'"),
        ("NICKEL_ENV", "staging", "environment 'staging'"),
        ("NICKEL_TENANT_ID", "clinic-one", "tenant_id: .*UUID"),
        ("NICKEL_TENANT_ID", "00000000-0000-0000-0000-000000000000", "nil UUID"),
        ("NICKEL_MAX_FILE_BYTES", "lots", "max_file_bytes must be a whole number"),
        ("NICKEL_MAX_FILE_BYTES", "0", "max_file_bytes must be a positive"),
        ("NICKEL_JOBS_MAX_QUEUED", "0", "jobs max_queued must be a positive"),
        ("NICKEL_JOBS_MAX_QUEUED", "many", "jobs max_queued must be a whole number"),
    ],
)
def test_an_unknown_value_is_refused_by_name(variable, value, message):
    with pytest.raises(ValueError, match=message):
        load_settings(CONF_FILE, environ={variable: value})


def test_a_missing_configuration_file_is_refused(tmp_path):
    missing = tmp_path / "nope.conf"
    with pytest.raises(FileNotFoundError, match="service configuration"):
        load_settings(missing, environ={})


def test_source_tokens_are_not_settings():
    text = CONF_FILE.read_text(encoding="utf-8")
    assert "${KAGGLE_API_TOKEN}" not in text
    assert "${HF_TOKEN}" not in text


def test_the_logger_writes_to_stdout_once(capsys):
    log = build_logger("nickel-test", "INFO")
    again = build_logger("nickel-test", "WARNING")
    assert again is log
    assert len(log.handlers) == 1
    assert log.level == logging.WARNING
    assert log.propagate is False
    assert log.handlers[0].formatter._fmt == LOG_FORMAT
    log.warning("hello")
    assert "WARNING [nickel-test] hello" in capsys.readouterr().out


def test_there_is_no_settings_global():
    """Importing src.config reads nothing: ``load_settings()`` reads the settings, and whoever
    starts something passes them on (main's app, the command line's run, the handlers and the
    connector: a connector without them is a TypeError, tests/test_connector.py)."""
    import src.config

    assert not hasattr(src.config, "Service")


def test_the_app_sets_the_log_level_from_the_settings_it_is_given(settings):
    """The logger is created at import under the service's name and reads no settings there;
    the app's settings decide its level when the app is created."""
    assert logger.name == LOGGER_NAME == "nickel-connection"
    before = logger.level
    try:
        main.create_app(dataclasses.replace(settings, log_level="DEBUG"))
        assert logger.level == logging.DEBUG
        main.create_app(dataclasses.replace(settings, log_level="WARNING"))
        assert logger.level == logging.WARNING
    finally:
        logger.setLevel(before)


def test_the_s3_client_libraries_are_pinned_whatever_the_root_logger_says():
    """botocore and boto3 log requests (keys, headers) at DEBUG and urllib3 warns with the URL it
    choked on: a root handler at DEBUG (JSON logs in K3s) must not get them."""
    for library in LIBRARY_LEVELS:
        logging.getLogger(library).setLevel(logging.NOTSET)
    build_logger("nickel-test", "INFO")
    assert {
        library: logging.getLogger(library).level for library in LIBRARY_LEVELS
    } == {
        "botocore": logging.WARNING,
        "boto3": logging.WARNING,
        "s3transfer": logging.WARNING,
        "urllib3": logging.ERROR,
    }
