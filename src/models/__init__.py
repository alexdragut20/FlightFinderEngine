from __future__ import annotations

import datetime as dt
from dataclasses import dataclass


@dataclass(frozen=True)
class PassengerConfig:
    """Passenger baggage and traveler-count configuration."""

    adults: int = 1
    hand_bags: int = 0
    hold_bags: int = 0


@dataclass(frozen=True)
class SearchConfig:
    """Search settings used by the optimizer and service layer."""

    origins: tuple[str, ...]
    destinations: tuple[str, ...]
    hub_candidates: tuple[str, ...]
    auto_hubs_per_direction: int
    exhaustive_hub_scan: bool
    period_start: dt.date
    period_end: dt.date
    min_stay_days: int
    max_stay_days: int
    min_stopover_days: int
    max_stopover_days: int
    max_transfers_per_direction: int
    max_stops_per_leg: int
    max_layovers_per_direction: int
    max_connection_layover_hours: int | None
    currency: str
    objective: str
    provider_ids: tuple[str, ...]
    market_compare_fares: bool
    validate_top_per_destination: int
    top_results: int
    estimated_pool_multiplier: int
    calendar_hubs_prefetch: int | None
    max_validate_oneway_keys_per_destination: int | None
    max_validate_return_keys_per_destination: int | None
    max_total_provider_calls: int | None
    max_calls_kiwi: int | None
    max_calls_amadeus: int | None
    max_calls_serpapi: int | None
    serpapi_probe_oneway_keys: int
    serpapi_probe_return_keys: int
    io_workers: int
    cpu_workers: int
    cpu_workers_auto: bool
    search_timeout_seconds: int
    passengers: PassengerConfig
