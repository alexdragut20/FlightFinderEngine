#!/usr/bin/env python3
"""Backward-compatible app exports for `server.py` imports."""

from __future__ import annotations

from . import config as _config
from . import utils as _utils
from .airports import AirportCoordinates
from .engine import SplitTripOptimizer, _estimate_candidates_for_destination
from .exceptions import ProviderNoResultError
from .http_server import AppHandler, run_server
from .logging_utils import capture_provider_response as _capture_provider_response
from .logging_utils import log_event
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

# Re-export config + utils symbols for backward compatibility with the legacy
# monolithic `server.py` import surface.
for _module in (_config, _utils):
    for _name, _value in _module.__dict__.items():
        if _name.startswith("__"):
            continue
        globals().setdefault(_name, _value)
del _module, _name, _value

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
]


if __name__ == "__main__":
    run_server()
