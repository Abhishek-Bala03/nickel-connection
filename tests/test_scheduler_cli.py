"""Tests for scheduler startup arguments and shutdown signals."""

import signal
from uuid import UUID

import pytest

from nickel_connection.scheduler import __main__ as cli
from nickel_connection.scheduler.runner import ConnectorRunner

TENANT_ID = UUID("00000000-0000-4000-8000-000000000001")


@pytest.fixture
def arguments(tmp_path):
    return [
        "--database",
        str(tmp_path / "scheduler.db"),
        "--tenant-id",
        str(TENANT_ID),
    ]


def test_cli_passes_configuration_to_service(arguments, tmp_path, monkeypatch, capsys):
    captured = {}

    def fake_serve(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(cli, "serve", fake_serve)

    result = cli.main(arguments + ["--poll-seconds", "0.5"])

    assert result == 0
    assert captured["database"] == tmp_path / "scheduler.db"
    assert captured["tenant_id"] == TENANT_ID
    assert captured["poll_seconds"] == 0.5
    assert isinstance(captured["runner"], ConnectorRunner)
    assert captured["stop"].is_set() is False

    output = capsys.readouterr().out
    assert "simulated demo connector" in output
    assert "Scheduler stopped." in output


def test_cli_uses_default_poll_interval(arguments, monkeypatch):
    captured = {}

    def fake_serve(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(cli, "serve", fake_serve)

    assert cli.main(arguments) == 0
    assert captured["poll_seconds"] == 1.0


@pytest.mark.parametrize("stop_signal", [signal.SIGINT, signal.SIGTERM])
def test_signal_requests_shutdown_and_handlers_are_restored(
    arguments, monkeypatch, stop_signal
):
    previous = {
        item: signal.getsignal(item) for item in (signal.SIGINT, signal.SIGTERM)
    }

    def fake_serve(**kwargs):
        handler = signal.getsignal(stop_signal)
        handler(stop_signal, None)
        assert kwargs["stop"].is_set() is True

    monkeypatch.setattr(cli, "serve", fake_serve)

    assert cli.main(arguments) == 0

    for item, handler in previous.items():
        assert signal.getsignal(item) == handler


def test_service_failure_restores_signal_handlers(arguments, monkeypatch):
    previous = {
        item: signal.getsignal(item) for item in (signal.SIGINT, signal.SIGTERM)
    }

    def fake_serve(**kwargs):
        raise RuntimeError("Service failed")

    monkeypatch.setattr(cli, "serve", fake_serve)

    with pytest.raises(RuntimeError, match="Service failed"):
        cli.main(arguments)

    for item, handler in previous.items():
        assert signal.getsignal(item) == handler


@pytest.mark.parametrize("value", ["0", "-1", "inf", "nan"])
def test_cli_rejects_invalid_poll_interval(arguments, value):
    with pytest.raises(SystemExit) as error:
        cli.main(arguments + ["--poll-seconds", value])

    assert error.value.code == 2


def test_cli_requires_configuration():
    with pytest.raises(SystemExit) as error:
        cli.main([])

    assert error.value.code == 2


def test_cli_rejects_invalid_tenant(arguments):
    arguments[-1] = "invalid-tenant"

    with pytest.raises(SystemExit) as error:
        cli.main(arguments)

    assert error.value.code == 2


def test_cli_help(capsys):
    with pytest.raises(SystemExit) as error:
        cli.main(["--help"])

    assert error.value.code == 0
    assert "--database" in capsys.readouterr().out
