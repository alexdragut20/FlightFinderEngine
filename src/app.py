#!/usr/bin/env python3
"""Backward-compatible app exports for `server.py` imports."""

from __future__ import annotations

import argparse
from collections.abc import Sequence

from . import config as _config
from . import utils as _utils
from .data.airports import AirportCoordinates
from .engine import SplitTripOptimizer, _estimate_candidates_for_destination
from .exceptions import ProviderNoResultError
from .providers import (
    AmadeusClient,
    GoogleFlightsLocalClient,
    KayakScrapeClient,
    KiwiClient,
    MomondoScrapeClient,
    MultiProviderClient,
    SerpApiGoogleFlightsClient,
    SkyscannerScrapeClient,
)
from .services.http_server import AppHandler, run_server
from .utils.logging import capture_provider_response as _capture_provider_response
from .utils.logging import log_event

# Re-export config + utils symbols for backward compatibility with the legacy
# monolithic `server.py` import surface.
for _module in (_config, _utils):
    for _name, _value in _module.__dict__.items():
        if _name.startswith("__"):
            continue
        globals().setdefault(_name, _value)
del _module, _name, _value


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line parser for the local server entrypoint.

    Returns:
        argparse.ArgumentParser: Parser for the local server command-line interface.
    """
    parser = argparse.ArgumentParser(
        prog="flightfinder-engine",
        description="Start the local FlightFinder Engine web server.",
    )
    parser.add_argument(
        "--host",
        default=None,
        help="Host interface to bind. Defaults to HOST env var or 127.0.0.1.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="TCP port to bind. Defaults to PORT env var or 8000.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the FlightFinder Engine command-line entrypoint.

    Args:
        argv: Optional command-line arguments. When omitted, arguments are read from `sys.argv`.

    Returns:
        int: Process exit code for the command-line invocation.
    """
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    run_server(host=args.host, port=args.port)
    return 0


__all__ = [
    "AirportCoordinates",
    "SplitTripOptimizer",
    "_estimate_candidates_for_destination",
    "ProviderNoResultError",
    "AppHandler",
    "run_server",
    "_capture_provider_response",
    "log_event",
    "AmadeusClient",
    "GoogleFlightsLocalClient",
    "KayakScrapeClient",
    "KiwiClient",
    "MomondoScrapeClient",
    "MultiProviderClient",
    "SerpApiGoogleFlightsClient",
    "SkyscannerScrapeClient",
    "build_parser",
    "main",
]


if __name__ == "__main__":
    raise SystemExit(main())
