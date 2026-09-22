import logging

import pytest

from src import __version__
from src.config import CONF_FILE, load_settings
from src.logging import LOG_FORMAT, build_logger

CONF = """
[SERVICE]
name = nickel-connection
environment = ${NICKEL_ENV}
cors_origins = ${NICKEL_CORS_ORIGINS}

[LOGGING]
level = ${NICKEL_LOG_LEVEL}
"""


@pytest.fixture
def conf(tmp_path):
    path = tmp_path / "application.conf"
    path.write_text(CONF, encoding="utf-8")
    return path


def test_defaults_apply_when_variables_are_not_set(conf):
    settings = load_settings(conf, environ={})
    assert settings.name == "nickel-connection"
    assert settings.environment == "development"
    assert settings.version == __version__
    assert settings.log_level == "INFO"
    assert settings.cors_origins == ()


def test_values_come_from_the_environment(conf):
    settings = load_settings(
        conf,
        environ={
            "NICKEL_ENV": "qa",
            "NICKEL_LOG_LEVEL": "debug",
            "NICKEL_CORS_ORIGINS": "http://a.example, http://b.example ,",
            "APP_VERSION": "1.4.0",
        },
    )
    assert settings.environment == "qa"
    assert settings.log_level == "DEBUG"
    assert settings.cors_origins == ("http://a.example", "http://b.example")
    assert settings.version == "1.4.0"


def test_an_unknown_log_level_is_refused(conf):
    with pytest.raises(ValueError, match="log level"):
        load_settings(conf, environ={"NICKEL_LOG_LEVEL": "LOUD"})


def test_a_missing_configuration_file_is_refused(tmp_path):
    missing = tmp_path / "nope.conf"
    with pytest.raises(FileNotFoundError, match="service configuration"):
        load_settings(missing, environ={})


def test_the_shipped_configuration_loads():
    assert load_settings(CONF_FILE, environ={}).name == "nickel-connection"


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
