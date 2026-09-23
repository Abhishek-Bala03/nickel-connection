"""Settings come from the shipped conf/application.conf and the environment; the logger writes once."""

import logging
import re
from pathlib import Path
from uuid import UUID

import pytest

from src import __version__
from src.config import CONF_FILE, DEFAULT_MAX_FILE_BYTES, MANIFEST_DIR, load_settings
from src.logging import LOG_FORMAT, build_logger

TENANT = "0192a3b4-0000-7000-8000-000000000001"
EVERY_VARIABLE = {
    "NICKEL_ENV": "qa",
    "NICKEL_LOG_LEVEL": "debug",
    "NICKEL_CORS_ORIGINS": "http://a.example, http://b.example ,",
    "APP_VERSION": "1.4.0",
    "NICKEL_TENANT_ID": TENANT,
    "NICKEL_MAX_FILE_BYTES": "123",
    "NICKEL_MANIFEST_DIR": "somewhere/manifests",
    "NICKEL_DATA_DIR": "somewhere",
}


def test_the_shipped_configuration_falls_back_to_the_defaults():
    settings = load_settings(CONF_FILE, environ={})
    assert settings.name == "nickel-connection"
    assert settings.environment == "development"
    assert settings.version == __version__
    assert settings.log_level == "INFO"
    assert settings.cors_origins == ()
    assert settings.tenant_id is None
    assert settings.max_file_bytes == DEFAULT_MAX_FILE_BYTES == 100 * 1024 * 1024
    assert settings.manifest_dir == MANIFEST_DIR
    assert settings.data_dir == Path(".data")


def test_the_shipped_configuration_reads_every_variable():
    settings = load_settings(CONF_FILE, environ=EVERY_VARIABLE)
    assert settings.environment == "qa"
    assert settings.log_level == "DEBUG"
    assert settings.cors_origins == ("http://a.example", "http://b.example")
    assert settings.version == "1.4.0"
    assert settings.tenant_id == UUID(TENANT)
    assert settings.max_file_bytes == 123
    assert settings.manifest_dir == Path("somewhere/manifests")
    assert settings.data_dir == Path("somewhere")


def test_the_shipped_configuration_names_no_other_variables():
    # Every ${VAR} the file reads is set above, so a setting added without a test, or one left
    # behind by a removed feature, shows up here.
    text = CONF_FILE.read_text(encoding="utf-8")
    named = set(re.findall(r"^\w+ = \$\{(\w+)\}$", text, flags=re.MULTILINE))
    assert named == set(EVERY_VARIABLE) - {"APP_VERSION"}


def test_values_in_the_file_win_over_the_defaults(tmp_path):
    conf = tmp_path / "application.conf"
    conf.write_text("[SERVICE]\nname = other-service\n", encoding="utf-8")
    assert load_settings(conf, environ={}).name == "other-service"


def test_environment_values_are_taken_literally():
    origins = "http://a.example/%41,http://b.example/${NICKEL_ENV}"
    settings = load_settings(CONF_FILE, environ={"NICKEL_CORS_ORIGINS": origins})
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
