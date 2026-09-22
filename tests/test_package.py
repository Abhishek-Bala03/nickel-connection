from nickel_connection import __version__
from nickel_connection.__main__ import main


def test_version_is_set():
    assert __version__ == "0.1.0"


def test_main_prints_the_version(capsys):
    assert main() == 0
    assert "nickel-connection 0.1.0" in capsys.readouterr().out
