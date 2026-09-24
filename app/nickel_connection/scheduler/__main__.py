"""Command-line entry point for the scheduler service."""

import argparse
import math
import signal
from pathlib import Path
from threading import Event
from uuid import UUID

from .runner import ConnectorRunner, DemoConnector
from .service import serve


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run the Nickel scheduler with the demo connector."
    )
    parser.add_argument(
        "--database",
        type=Path,
        required=True,
        help="Path to the persistent SQLite database.",
    )
    parser.add_argument(
        "--tenant-id",
        type=UUID,
        required=True,
        help="Tenant UUID supplied by the deployment.",
    )
    parser.add_argument(
        "--poll-seconds",
        type=float,
        default=1.0,
        help="How often to check for due schedules; default: 1 second.",
    )
    args = parser.parse_args(argv)

    if not math.isfinite(args.poll_seconds) or args.poll_seconds <= 0:
        parser.error("--poll-seconds must be finite and positive")

    stop = Event()

    def request_stop(signum, frame):
        stop.set()

    previous_handlers = {}

    try:
        for stop_signal in (signal.SIGINT, signal.SIGTERM):
            previous_handlers[stop_signal] = signal.signal(stop_signal, request_stop)

        runner = ConnectorRunner({"demo": DemoConnector()})

        print(
            "Starting scheduler with the simulated demo connector. "
            "Press Ctrl+C to stop.",
            flush=True,
        )

        serve(
            database=args.database,
            tenant_id=args.tenant_id,
            runner=runner,
            stop=stop,
            poll_seconds=args.poll_seconds,
        )
    finally:
        for stop_signal, handler in previous_handlers.items():
            signal.signal(stop_signal, handler)

    print("Scheduler stopped.", flush=True)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
