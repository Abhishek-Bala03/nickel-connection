"""Entry point for ``python -m nickel_connection``. Commands arrive with the connectors."""

from nickel_connection import __version__


def main() -> int:
    print(f"nickel-connection {__version__}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
