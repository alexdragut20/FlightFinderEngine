from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import heapq
import logging
import math
import os
import time
import uuid
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import replace
from functools import partial
from typing import Any

from ..config import (
    _FREE_PROVIDER_IDS,
    AUTO_HUB_CANDIDATES,
    DEFAULT_CALENDAR_HUBS_PREFETCH,
    DEFAULT_CPU_WORKERS,
    DEFAULT_DESTINATIONS,
    DEFAULT_IO_WORKERS,
    DEFAULT_MAX_CALLS_AMADEUS,
    DEFAULT_MAX_CALLS_KIWI,
    DEFAULT_MAX_CALLS_SERPAPI,
    DEFAULT_MAX_TOTAL_PROVIDER_CALLS,
    DEFAULT_MAX_VALIDATE_ONEWAY_KEYS,
    DEFAULT_MAX_VALIDATE_RETURN_KEYS,
    DEFAULT_SEARCH_TIMEOUT_SECONDS,
    DEFAULT_SERPAPI_PROBE_ONEWAY_KEYS,
    DEFAULT_SERPAPI_PROBE_RETURN_KEYS,
    DESTINATION_NOTES,
    MIN_SPLIT_CONNECTION_CROSS_AIRPORT_SECONDS,
    MIN_SPLIT_CONNECTION_SAME_AIRPORT_SECONDS,
    SUPPORTED_PROVIDER_IDS,
)
from ..data.airports import AirportCoordinates
from ..models import PassengerConfig, SearchConfig
from ..providers import (
    AmadeusClient,
    GoogleFlightsLocalClient,
    KayakScrapeClient,
    KiwiClient,
    MomondoScrapeClient,
    MultiProviderClient,
    SerpApiGoogleFlightsClient,
    SkyscannerScrapeClient,
    TravelpayoutsDataClient,
)
from ..services.progress import SearchProgressTracker
from ..services.route_graph import RouteConnectivityGraph
from ..utils import (
    boundary_transfer_events,
    bounded_io_concurrency,
    build_comparison_links,
    clamp_int,
    clamp_optional_int,
    connection_gap_seconds,
    date_range,
    haversine_km,
    kiwi_oneway_url,
    kiwi_return_url,
    leg_endpoints_from_segments,
    max_segment_layover_seconds,
    minimum_split_boundary_connection_seconds,
    normalize_codes,
    normalize_provider_ids,
    to_bool,
    to_date,
)
from ..utils.constants import (
    BEST_OBJECTIVE_PRICE_PER_HOUR_WEIGHT,
    CANDIDATE_CHAIN_PAIR_LIMIT_MAX,
    CANDIDATE_CHAIN_PAIR_LIMIT_MIN,
    CANDIDATE_CHAIN_PAIR_PRIORITY_MULTIPLIER,
    CANDIDATE_CHUNK_CAP_PER_DESTINATION,
    CANDIDATE_CHUNK_WORKER_MULTIPLIER,
    CANDIDATE_PRUNING_PRICE_MARGIN,
    CANDIDATE_PRUNING_SCORE_MARGIN_RATIO,
    COVERAGE_AUDIT_CHAIN_PAIR_MULTIPLIER,
    COVERAGE_AUDIT_DATE_RADIUS_DAYS,
    COVERAGE_AUDIT_DESTINATION_LIMIT,
    COVERAGE_AUDIT_DIRECT_CANDIDATE_MULTIPLIER,
    COVERAGE_AUDIT_DISCOVERY_IO_CAP,
    COVERAGE_AUDIT_MAX_DATES_PER_ROUTE,
    COVERAGE_AUDIT_MAX_ROUTES,
    COVERAGE_AUDIT_PRUNING_PRICE_MARGIN,
    COVERAGE_AUDIT_PRUNING_SCORE_MARGIN_RATIO,
    COVERAGE_AUDIT_SPLIT_CANDIDATE_MULTIPLIER,
    COVERAGE_AUDIT_TOP_CANDIDATES,
    COVERAGE_AUDIT_VALIDATION_MULTIPLIER,
    FASTEST_OBJECTIVE_PRICE_MULTIPLIER,
    FREE_PROVIDER_DISCOVERY_IO_CAP,
    FREE_PROVIDER_DISCOVERY_MAX_DATES_PER_ROUTE,
    FREE_PROVIDER_DISCOVERY_MAX_ROUTES_PER_DESTINATION,
    INNER_RETURN_BUNDLE_DISCOUNT_FACTOR,
    MAX_EXHAUSTIVE_DIRECT_CANDIDATES_PER_DESTINATION,
    MAX_EXHAUSTIVE_SPLIT_CANDIDATES_PER_DESTINATION,
    MAX_NON_EXHAUSTIVE_DIRECT_CANDIDATES_PER_DESTINATION,
    MAX_NON_EXHAUSTIVE_SPLIT_CANDIDATES_PER_DESTINATION,
    OUTBOUND_TIME_PROXY_BASE_SECONDS,
    OUTBOUND_TIME_PROXY_TRANSFER_PENALTY_SECONDS,
    PRICE_SENTINEL,
    PRICE_TIME_SCORE_PRICE_WEIGHT,
    PRICE_TIME_SCORE_TIME_WEIGHT,
    SEARCH_EVENT_REQUEST_ACCEPTED,
    SEARCH_EVENT_STARTED,
    SECONDS_PER_DAY,
    SECONDS_PER_HOUR,
)
from ..utils.logging import log_event

_CANDIDATE_WORKER_TASKS: dict[str, dict[str, Any]] = {}


def _min_calendar_price(prices: dict[str, int] | None) -> int | None:
    """Return the minimum price from a calendar mapping.

    Args:
        prices: Mapping of prices.

    Returns:
        int | None: The minimum price from a calendar mapping.
    """
    if not prices:
        return None
    try:
        return int(min(prices.values()))
    except Exception:
        return None


def _estimate_inner_return_bundle_price(
    outbound_price: int | None,
    inbound_price: int | None,
) -> int | None:
    """Estimate a bundled inner round-trip price.

    Args:
        outbound_price: Price for the outbound segment.
        inbound_price: Price for the inbound segment.

    Returns:
        int | None: Estimated bundled inner round-trip price.
    """
    known_prices: list[int] = []
    for value in (outbound_price, inbound_price):
        try:
            price = int(value) if value is not None else 0
        except (TypeError, ValueError):
            price = 0
        if price > 0:
            known_prices.append(price)

    if not known_prices:
        return None
    if len(known_prices) == 1:
        return known_prices[0] * 2

    cheaper = min(known_prices)
    pricier = max(known_prices)
    summed = cheaper + pricier

    # Return bundles are often closer to "two cheap directions" than to
    # the full sum of two separate one-ways, and symmetric markets usually
    # land near a discounted share of the total two-way spend.
    directional_proxy = cheaper * 2
    discounted_sum_proxy = max(
        cheaper,
        int(round(float(summed) * INNER_RETURN_BUNDLE_DISCOUNT_FACTOR)),
    )
    return max(cheaper, min(summed, directional_proxy, discounted_sum_proxy))


def _apply_inner_return_bundle_estimate(
    *,
    base_total: int,
    outbound_market_price: int | None,
    inbound_market_price: int | None,
) -> tuple[int, int | None]:
    """Apply an inner round-trip estimate to the current total.

    Args:
        base_total: Running trip total before additional pricing adjustments.
        outbound_market_price: Price for outbound market.
        inbound_market_price: Price for inbound market.

    Returns:
        tuple[int, int | None]: An inner round-trip estimate to the current total.
    """
    bundle_price = _estimate_inner_return_bundle_price(
        outbound_market_price,
        inbound_market_price,
    )
    if bundle_price is None:
        return base_total, None

    outbound_component = int(outbound_market_price or 0)
    inbound_component = int(inbound_market_price or 0)
    original_market_total = outbound_component + inbound_component
    if original_market_total <= 0 or bundle_price >= original_market_total:
        return base_total, None
    return base_total - original_market_total + bundle_price, bundle_price


def _apply_price_time_score(
    items: list[dict[str, Any]],
    *,
    price_key: str,
    time_key: str,
    score_key: str,
) -> None:
    """Apply the weighted price-time score.

    Args:
        items: Items for the current operation.
        price_key: Dictionary key used for price.
        time_key: Dictionary key used for time.
        score_key: Dictionary key used for score.
    """
    if not items:
        return

    prices = [int(item[price_key]) for item in items]
    times = [int(item[time_key]) for item in items if item.get(time_key) is not None]

    min_price = min(prices)
    max_price = max(prices)
    min_time = min(times) if times else 0
    max_time = max(times) if times else 0

    def normalize(value: int, low: int, high: int) -> float:
        if high <= low:
            return 0.0
        return (value - low) / (high - low)

    for item in items:
        price_norm = normalize(int(item[price_key]), min_price, max_price)
        outbound_time = item.get(time_key)
        if outbound_time is None:
            time_norm = 1.0
        else:
            time_norm = normalize(int(outbound_time), min_time, max_time)
        item[score_key] = round(
            (PRICE_TIME_SCORE_PRICE_WEIGHT * price_norm)
            + (PRICE_TIME_SCORE_TIME_WEIGHT * time_norm),
            6,
        )


def _estimated_outbound_time_proxy_seconds(
    *,
    depart_origin_date: dt.date,
    depart_destination_date: dt.date,
    outbound_transfer_count: int,
) -> int:
    """Estimate outbound travel time for scoring.

    Args:
        depart_origin_date: Date for depart origin.
        depart_destination_date: Date for depart destination.
        outbound_transfer_count: Number of outbound transfer.

    Returns:
        int: Estimated outbound travel time for scoring.
    """
    day_delta = max(0, (depart_destination_date - depart_origin_date).days)
    transfer_penalty_seconds = (
        max(0, int(outbound_transfer_count)) * OUTBOUND_TIME_PROXY_TRANSFER_PENALTY_SECONDS
    )
    return (
        OUTBOUND_TIME_PROXY_BASE_SECONDS + (day_delta * SECONDS_PER_DAY) + transfer_penalty_seconds
    )


def _estimate_objective_score(
    *,
    objective: str,
    estimated_total: int,
    distance_basis_km: float | None,
    outbound_time_proxy_seconds: int | None,
) -> float:
    """Estimate the objective score for a candidate route.

    Args:
        objective: Ranking objective for the search.
        estimated_total: Estimated total fare value for the candidate.
        distance_basis_km: Distance basis in kilometers used for scoring.
        outbound_time_proxy_seconds: Duration in seconds for outbound time proxy.

    Returns:
        float: Estimated objective score for a candidate route.
    """
    if objective == "fastest":
        if outbound_time_proxy_seconds is None:
            return float(estimated_total) * FASTEST_OBJECTIVE_PRICE_MULTIPLIER
        return float(outbound_time_proxy_seconds) + (
            float(estimated_total) * FASTEST_OBJECTIVE_PRICE_MULTIPLIER
        )
    if objective == "best":
        if outbound_time_proxy_seconds is None:
            return float(estimated_total)
        outbound_hours = float(outbound_time_proxy_seconds) / float(SECONDS_PER_HOUR)
        return float(estimated_total) + (outbound_hours * BEST_OBJECTIVE_PRICE_PER_HOUR_WEIGHT)
    if not distance_basis_km:
        return float(estimated_total)
    return (estimated_total / distance_basis_km) * 1000.0


def _coerce_optional_price(value: Any) -> int | None:
    """Coerce an arbitrary value into an optional integer price.

    Args:
        value: Raw value to normalize.

    Returns:
        int | None: Normalized integer price when available.
    """
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _min_series_price(
    series: tuple[int | None, ...] | list[int | None] | dict[Any, Any] | None,
) -> int | None:
    """Return the minimum non-null price from a compact calendar series.

    Args:
        series: Calendar prices indexed by day offset.

    Returns:
        int | None: The minimum non-null price from a compact calendar series.
    """
    if not series:
        return None
    raw_values = series.values() if isinstance(series, dict) else series
    values = [int(price) for price in raw_values if price is not None]
    if not values:
        return None
    return min(values)


def _normalize_route_key(raw_key: Any) -> tuple[str, str]:
    """Normalize a compact or legacy route key into a tuple key.

    Args:
        raw_key: Route key from a legacy string map or compact tuple map.

    Returns:
        tuple[str, str]: Tuple route key with source and destination airport codes.
    """
    if isinstance(raw_key, tuple) and len(raw_key) == 2:
        return str(raw_key[0]), str(raw_key[1])
    parts = str(raw_key).split("|", 1)
    if len(parts) == 2:
        return parts[0], parts[1]
    return str(raw_key), ""


def _calendar_series_from_prices(
    date_keys: tuple[str, ...],
    prices: Any,
) -> tuple[int | None, ...]:
    """Build a dense per-day series from a sparse calendar map.

    Args:
        date_keys: Ordered ISO dates for the full search period.
        prices: Sparse mapping or already-compact series of prices.

    Returns:
        tuple[int | None, ...]: Dense price series aligned with `date_keys`.
    """
    if isinstance(prices, tuple) and len(prices) == len(date_keys):
        return tuple(_coerce_optional_price(value) for value in prices)
    if isinstance(prices, list) and len(prices) == len(date_keys):
        return tuple(_coerce_optional_price(value) for value in prices)
    if not isinstance(prices, dict):
        return tuple(None for _ in date_keys)
    return tuple(_coerce_optional_price(prices.get(date_key)) for date_key in date_keys)


def _compact_candidate_task(task: dict[str, Any]) -> dict[str, Any]:
    """Normalize an estimator task into a compact, chunk-friendly structure.

    Args:
        task: Legacy or compact estimator task payload.

    Returns:
        dict[str, Any]: Compact estimator task payload ready for chunk processing.
    """
    if "date_keys" in task:
        date_keys = tuple(str(value) for value in task["date_keys"])
    else:
        period_start = dt.date.fromisoformat(str(task["period_start"]))
        period_end = dt.date.fromisoformat(str(task["period_end"]))
        date_keys = tuple(day.isoformat() for day in date_range(period_start, period_end))

    destination = str(task["destination"])
    compact_task: dict[str, Any] = {
        "destination": destination,
        "origins": tuple(str(origin) for origin in task["origins"]),
        "outbound_hubs": tuple(str(hub) for hub in task["outbound_hubs"]),
        "inbound_hubs": tuple(str(hub) for hub in task["inbound_hubs"]),
        "date_keys": date_keys,
        "min_stay_days": int(task["min_stay_days"]),
        "max_stay_days": int(task["max_stay_days"]),
        "min_stopover_days": int(task["min_stopover_days"]),
        "max_stopover_days": int(task["max_stopover_days"]),
        "objective": str(task["objective"]),
        "max_candidates": int(task["max_candidates"]),
        "max_direct_candidates": int(task.get("max_direct_candidates") or task["max_candidates"]),
        "max_transfers_per_direction": int(task.get("max_transfers_per_direction") or 2),
        "chunk_start_index": max(0, int(task.get("chunk_start_index") or 0)),
        "chunk_end_index": min(
            len(date_keys),
            int(task.get("chunk_end_index") or len(date_keys)),
        ),
        "prune_score_margin_ratio": float(
            task.get("prune_score_margin_ratio", CANDIDATE_PRUNING_SCORE_MARGIN_RATIO)
        ),
        "prune_price_margin": int(task.get("prune_price_margin", CANDIDATE_PRUNING_PRICE_MARGIN)),
        "chain_pair_limit_multiplier": max(
            1,
            int(task.get("chain_pair_limit_multiplier") or 1),
        ),
        "audit_mode": bool(task.get("audit_mode")),
    }

    compact_task["origin_to_hub"] = {
        _normalize_route_key(raw_key): _calendar_series_from_prices(date_keys, prices)
        for raw_key, prices in dict(task["origin_to_hub"]).items()
    }
    compact_task["hub_to_origin"] = {
        _normalize_route_key(raw_key): _calendar_series_from_prices(date_keys, prices)
        for raw_key, prices in dict(task["hub_to_origin"]).items()
    }
    compact_task["hub_to_destination"] = {
        str(raw_key): _calendar_series_from_prices(date_keys, prices)
        for raw_key, prices in dict(task["hub_to_destination"]).items()
    }
    compact_task["destination_to_hub"] = {
        str(raw_key): _calendar_series_from_prices(date_keys, prices)
        for raw_key, prices in dict(task["destination_to_hub"]).items()
    }
    compact_task["hub_to_hub"] = {
        _normalize_route_key(raw_key): _calendar_series_from_prices(date_keys, prices)
        for raw_key, prices in dict(task.get("hub_to_hub", {})).items()
    }
    compact_task["origin_to_destination"] = {
        _normalize_route_key(raw_key): _calendar_series_from_prices(date_keys, prices)
        for raw_key, prices in dict(task.get("origin_to_destination", {})).items()
    }
    compact_task["destination_to_origin"] = {
        _normalize_route_key(raw_key): _calendar_series_from_prices(date_keys, prices)
        for raw_key, prices in dict(task.get("destination_to_origin", {})).items()
    }
    compact_task["destination_distance_map"] = {
        _normalize_route_key(raw_key): value
        for raw_key, value in dict(task["destination_distance_map"]).items()
    }
    return compact_task


def _candidate_worker_init(task_map: dict[str, dict[str, Any]]) -> None:
    """Initialize worker-local candidate task state.

    Args:
        task_map: Compact estimator tasks keyed by task identifier.
    """
    global _CANDIDATE_WORKER_TASKS
    _CANDIDATE_WORKER_TASKS = task_map


def _lookup_route_prices(
    mapping: dict[Any, Any],
    source: str,
    destination: str,
) -> Any:
    """Look up route pricing from compact or legacy keyed mappings.

    Args:
        mapping: Price mapping keyed by tuples or legacy `A|B` strings.
        source: Source airport code for the route.
        destination: Destination airport code for the route.

    Returns:
        Any: Matching pricing payload, if present.
    """
    return mapping.get((source, destination), mapping.get(f"{source}|{destination}"))


def _rank_chain_pairs(
    *,
    origins: list[str],
    first_hubs: list[str],
    second_hubs: list[str],
    leg_a_map: dict[tuple[str, str], tuple[int | None, ...]],
    leg_b_map: dict[tuple[str, str], tuple[int | None, ...]],
    leg_c_map: dict[str, tuple[int | None, ...]],
    reverse_leg_c_map: dict[str, tuple[int | None, ...]] | None = None,
    pair_limit: int,
) -> list[tuple[str, str, str]]:
    """Rank outbound chain pairs before validation.

    Args:
        origins: Origins for the operation.
        first_hubs: Collection of first hubs.
        second_hubs: Collection of second hubs.
        leg_a_map: Mapping of leg a.
        leg_b_map: Mapping of leg b.
        leg_c_map: Mapping of leg c.
        reverse_leg_c_map: Mapping of reverse leg c.
        pair_limit: Maximum number of hub pairs to evaluate.

    Returns:
        list[tuple[str, str, str]]: Ranked outbound chain pairs before validation.
    """
    scored: list[tuple[int, str, str, str]] = []
    for origin in origins:
        for first_hub in first_hubs:
            min_a = _min_series_price(_lookup_route_prices(leg_a_map, origin, first_hub))
            if min_a is None:
                continue
            for second_hub in second_hubs:
                if second_hub == first_hub:
                    continue
                min_b = _min_series_price(_lookup_route_prices(leg_b_map, first_hub, second_hub))
                if min_b is None:
                    continue
                min_c = _min_series_price(leg_c_map.get(second_hub))
                if min_c is None:
                    continue
                market_c = _estimate_inner_return_bundle_price(
                    min_c,
                    _min_series_price((reverse_leg_c_map or {}).get(second_hub)),
                )
                scored.append(
                    (
                        min_a + min_b + int(market_c or min_c),
                        origin,
                        first_hub,
                        second_hub,
                    )
                )
    scored.sort()
    out: list[tuple[str, str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for _, origin, first_hub, second_hub in scored:
        key = (origin, first_hub, second_hub)
        if key in seen:
            continue
        seen.add(key)
        out.append(key)
        if len(out) >= pair_limit:
            break
    return out


def _rank_inbound_chain_pairs(
    *,
    origins: list[str],
    first_hubs: list[str],
    second_hubs: list[str],
    destination_to_hub: dict[str, tuple[int | None, ...]],
    hub_to_destination: dict[str, tuple[int | None, ...]] | None,
    hub_to_hub: dict[tuple[str, str], tuple[int | None, ...]],
    hub_to_origin: dict[tuple[str, str], tuple[int | None, ...]],
    pair_limit: int,
) -> list[tuple[str, str, str]]:
    """Rank inbound chain pairs before validation.

    Args:
        origins: Origins for the operation.
        first_hubs: Collection of first hubs.
        second_hubs: Collection of second hubs.
        destination_to_hub: Mapping of destination to hub.
        hub_to_destination: Mapping of hub to destination.
        hub_to_hub: Mapping of hub to hub.
        hub_to_origin: Mapping of hub to origin.
        pair_limit: Maximum number of hub pairs to evaluate.

    Returns:
        list[tuple[str, str, str]]: Ranked inbound chain pairs before validation.
    """
    scored: list[tuple[int, str, str, str]] = []
    for arrival_origin in origins:
        for first_hub in first_hubs:
            min_a = _min_series_price(destination_to_hub.get(first_hub))
            if min_a is None:
                continue
            for second_hub in second_hubs:
                if second_hub == first_hub:
                    continue
                min_b = _min_series_price(_lookup_route_prices(hub_to_hub, first_hub, second_hub))
                if min_b is None:
                    continue
                min_c = _min_series_price(
                    _lookup_route_prices(hub_to_origin, second_hub, arrival_origin)
                )
                if min_c is None:
                    continue
                market_a = _estimate_inner_return_bundle_price(
                    _min_series_price((hub_to_destination or {}).get(first_hub)),
                    min_a,
                )
                scored.append(
                    (
                        int(market_a or min_a) + min_b + min_c,
                        arrival_origin,
                        first_hub,
                        second_hub,
                    )
                )
    scored.sort()
    out: list[tuple[str, str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for _, arrival_origin, first_hub, second_hub in scored:
        key = (arrival_origin, first_hub, second_hub)
        if key in seen:
            continue
        seen.add(key)
        out.append(key)
        if len(out) >= pair_limit:
            break
    return out


def _finalize_estimated_candidates(
    candidates: list[dict[str, Any]],
    *,
    objective: str,
    max_candidates: int,
    max_direct_candidates: int,
) -> list[dict[str, Any]]:
    """Sort and trim estimated candidates after chunk or destination scoring.

    Args:
        candidates: Estimated candidates to finalize.
        objective: Ranking objective for the search.
        max_candidates: Maximum split candidates to keep.
        max_direct_candidates: Maximum direct round-trip candidates to keep.

    Returns:
        list[dict[str, Any]]: Finalized estimated candidates ordered by objective.
    """
    split_candidates = [
        candidate
        for candidate in candidates
        if candidate.get("candidate_type") != "direct_roundtrip"
    ]
    direct_candidates = [
        candidate
        for candidate in candidates
        if candidate.get("candidate_type") == "direct_roundtrip"
    ]

    if objective == "best":
        _apply_price_time_score(
            split_candidates + direct_candidates,
            price_key="estimated_total",
            time_key="estimated_outbound_time_to_destination_seconds",
            score_key="estimated_best_value_score",
        )

    def candidate_sort_key(candidate: dict[str, Any]) -> tuple[float, int, int]:
        best_value_score = candidate.get("estimated_best_value_score")
        outbound_time = candidate.get("estimated_outbound_time_to_destination_seconds")
        return (
            float(
                best_value_score
                if objective == "best" and best_value_score is not None
                else candidate["estimated_score"]
            ),
            int(candidate["estimated_total"]),
            int(outbound_time) if outbound_time is not None else PRICE_SENTINEL,
        )

    split_candidates.sort(key=candidate_sort_key)
    direct_candidates.sort(key=candidate_sort_key)

    if max_direct_candidates > 0:
        direct_candidates = direct_candidates[:max_direct_candidates]
    if max_candidates > 0:
        split_candidates = split_candidates[:max_candidates]
    else:
        split_candidates = []

    out = split_candidates + direct_candidates
    out.sort(key=candidate_sort_key)
    return out


def _filter_finalized_estimated_candidates(
    candidates: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Keep only estimator outputs that already include ranking metrics.

    Args:
        candidates: Candidate records to filter.

    Returns:
        list[dict[str, Any]]: Candidates ready for estimate-based ranking.
    """
    return [
        candidate
        for candidate in candidates
        if "estimated_total" in candidate and "estimated_score" in candidate
    ]


def _build_candidate_chunk_specs(
    tasks: list[dict[str, Any]],
    cpu_workers: int,
) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    """Build compact candidate tasks and chunk descriptors for parallel scoring.

    Args:
        tasks: Candidate task payloads for each destination.
        cpu_workers: Configured CPU worker count.

    Returns:
        tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]: Compact task map and chunk specs.
    """
    if not tasks:
        return {}, []

    compact_tasks: dict[str, dict[str, Any]] = {}
    chunk_specs: list[dict[str, Any]] = []
    destinations_count = max(1, len(tasks))
    target_chunks_per_destination = max(
        1,
        min(
            CANDIDATE_CHUNK_CAP_PER_DESTINATION,
            math.ceil(
                (max(1, cpu_workers) * CANDIDATE_CHUNK_WORKER_MULTIPLIER) / destinations_count
            ),
        ),
    )

    for task in tasks:
        compact_task = _compact_candidate_task(task)
        task_id = str(compact_task["destination"])
        compact_tasks[task_id] = compact_task
        date_keys = tuple(compact_task["date_keys"])
        date_count = len(date_keys)
        if date_count <= 1:
            chunks_per_destination = 1
        else:
            chunks_per_destination = min(date_count, target_chunks_per_destination)
        chunk_size = max(1, math.ceil(date_count / max(1, chunks_per_destination)))
        for chunk_start_index in range(0, date_count, chunk_size):
            chunk_end_index = min(date_count, chunk_start_index + chunk_size)
            chunk_specs.append(
                {
                    "task_id": task_id,
                    "destination": task_id,
                    "chunk_start_index": chunk_start_index,
                    "chunk_end_index": chunk_end_index,
                    "chunk_label": f"{date_keys[chunk_start_index]}..{date_keys[chunk_end_index - 1]}",
                    "base_task": compact_task,
                }
            )
    return compact_tasks, chunk_specs


def _estimate_candidates_for_chunk(chunk: dict[str, Any]) -> tuple[str, list[dict[str, Any]]]:
    """Estimate candidates for one chunk of destination dates.

    Args:
        chunk: Chunk descriptor for the estimator worker.

    Returns:
        tuple[str, list[dict[str, Any]]]: Destination code and estimated candidates for the chunk.
    """
    task_id = str(chunk["task_id"])
    base_task = _CANDIDATE_WORKER_TASKS.get(task_id) or dict(chunk["base_task"])
    chunk_task = dict(base_task)
    chunk_task["chunk_start_index"] = int(chunk["chunk_start_index"])
    chunk_task["chunk_end_index"] = int(chunk["chunk_end_index"])
    destination = str(chunk_task["destination"])
    return destination, _estimate_candidates_for_destination(chunk_task)


def _estimate_candidates_for_destination(task: dict[str, Any]) -> list[dict[str, Any]]:
    """Estimate candidate routes for a destination.

    Args:
        task: Mapping of task.

    Returns:
        list[dict[str, Any]]: Estimated candidate routes for a destination.
    """
    return _estimate_candidates_for_destination_compact(task)


def _estimate_candidates_for_destination_compact(task: dict[str, Any]) -> list[dict[str, Any]]:
    """Estimate candidate routes for a compact destination task.

    Args:
        task: Compact estimator task payload.

    Returns:
        list[dict[str, Any]]: Estimated candidate routes for the provided chunk.
    """
    task = _compact_candidate_task(task)
    destination = str(task["destination"])
    origins = list(task["origins"])
    outbound_hubs = list(task["outbound_hubs"])
    inbound_hubs = list(task["inbound_hubs"])
    date_keys = list(task["date_keys"])
    date_count = len(date_keys)
    chunk_start_index = max(0, int(task.get("chunk_start_index") or 0))
    chunk_end_index = min(date_count, int(task.get("chunk_end_index") or date_count))

    min_stay_days = int(task["min_stay_days"])
    max_stay_days = int(task["max_stay_days"])
    min_stopover_days = int(task["min_stopover_days"])
    max_stopover_days = int(task["max_stopover_days"])
    objective = str(task["objective"])
    max_candidates = int(task["max_candidates"])
    max_direct_candidates = int(task.get("max_direct_candidates") or max_candidates)
    max_transfers_per_direction = int(task.get("max_transfers_per_direction") or 2)
    prune_score_margin_ratio = max(
        0.0,
        float(task.get("prune_score_margin_ratio", CANDIDATE_PRUNING_SCORE_MARGIN_RATIO)),
    )
    prune_price_margin = max(
        0,
        int(task.get("prune_price_margin", CANDIDATE_PRUNING_PRICE_MARGIN)),
    )
    chain_pair_limit_multiplier = max(
        1,
        int(task.get("chain_pair_limit_multiplier") or 1),
    )

    origin_to_hub: dict[tuple[str, str], tuple[int | None, ...]] = dict(task["origin_to_hub"])
    hub_to_origin: dict[tuple[str, str], tuple[int | None, ...]] = dict(task["hub_to_origin"])
    hub_to_destination: dict[str, tuple[int | None, ...]] = dict(task["hub_to_destination"])
    destination_to_hub: dict[str, tuple[int | None, ...]] = dict(task["destination_to_hub"])
    hub_to_hub: dict[tuple[str, str], tuple[int | None, ...]] = dict(task.get("hub_to_hub", {}))
    origin_to_destination: dict[tuple[str, str], tuple[int | None, ...]] = dict(
        task.get("origin_to_destination", {})
    )
    destination_to_origin: dict[tuple[str, str], tuple[int | None, ...]] = dict(
        task.get("destination_to_origin", {})
    )
    destination_distance_map: dict[tuple[str, str], float | None] = dict(
        task["destination_distance_map"]
    )

    min_hub_to_origin = {key: _min_series_price(series) for key, series in hub_to_origin.items()}
    min_hub_to_any_origin: dict[str, int | None] = {}
    for hub in inbound_hubs:
        values = [
            price
            for (route_hub, _arrival_origin), price in min_hub_to_origin.items()
            if route_hub == hub and price is not None
        ]
        min_hub_to_any_origin[hub] = min(values) if values else None
    min_hub_to_hub = {key: _min_series_price(series) for key, series in hub_to_hub.items()}

    counter = 0
    heap: list[tuple[float, int, int, dict[str, Any]]] = []
    direct_candidates: list[dict[str, Any]] = []

    def push_candidate(
        candidate: dict[str, Any],
        estimated_score: float,
        estimated_total: int,
    ) -> None:
        nonlocal counter
        marker = (-estimated_score, -estimated_total, counter, candidate)
        counter += 1

        if len(heap) < max_candidates:
            heapq.heappush(heap, marker)
            return

        worst_score = -heap[0][0]
        worst_total = -heap[0][1]
        if estimated_score < worst_score or (
            math.isclose(estimated_score, worst_score) and estimated_total < worst_total
        ):
            heapq.heapreplace(heap, marker)

    def candidate_bound_is_competitive(
        *,
        lower_total: int,
        distance_basis_km: float | None,
        outbound_time_proxy_seconds: int | None,
    ) -> bool:
        if max_candidates <= 0 or len(heap) < max_candidates:
            return True
        worst_score = -heap[0][0]
        worst_total = -heap[0][1]
        lower_score = _estimate_objective_score(
            objective=objective,
            estimated_total=lower_total,
            distance_basis_km=distance_basis_km,
            outbound_time_proxy_seconds=outbound_time_proxy_seconds,
        )
        score_margin_limit = worst_score * (1.0 + prune_score_margin_ratio)
        total_margin_limit = worst_total + prune_price_margin
        return (
            lower_score <= score_margin_limit
            or lower_total <= total_margin_limit
            or (math.isclose(lower_score, worst_score) and lower_total < worst_total)
        )

    for depart_index in range(chunk_start_index, chunk_end_index):
        depart_origin_key = date_keys[depart_index]

        for origin in origins:
            distance_basis_km = destination_distance_map.get((origin, destination))
            for outbound_hub in outbound_hubs:
                first_leg_prices = origin_to_hub.get((origin, outbound_hub), ())
                first_leg_price = (
                    first_leg_prices[depart_index] if depart_index < len(first_leg_prices) else None
                )
                if first_leg_price is None:
                    continue

                for outbound_stopover_days in range(min_stopover_days, max_stopover_days + 1):
                    depart_destination_index = depart_index + outbound_stopover_days
                    if depart_destination_index >= date_count:
                        continue
                    depart_destination_key = date_keys[depart_destination_index]

                    second_leg_prices = hub_to_destination.get(outbound_hub, ())
                    second_leg_price = (
                        second_leg_prices[depart_destination_index]
                        if depart_destination_index < len(second_leg_prices)
                        else None
                    )
                    if second_leg_price is None:
                        continue

                    estimated_outbound_time_seconds = _estimated_outbound_time_proxy_seconds(
                        depart_origin_date=dt.date.fromisoformat(depart_origin_key),
                        depart_destination_date=dt.date.fromisoformat(depart_destination_key),
                        outbound_transfer_count=1,
                    )

                    for main_stay_days in range(min_stay_days, max_stay_days + 1):
                        leave_destination_index = depart_destination_index + main_stay_days
                        if leave_destination_index >= date_count:
                            continue
                        leave_destination_key = date_keys[leave_destination_index]

                        for inbound_hub in inbound_hubs:
                            third_leg_prices = destination_to_hub.get(inbound_hub, ())
                            third_leg_price = (
                                third_leg_prices[leave_destination_index]
                                if leave_destination_index < len(third_leg_prices)
                                else None
                            )
                            if third_leg_price is None:
                                continue

                            base_three_leg_total = (
                                first_leg_price + second_leg_price + third_leg_price
                            )
                            global_min_fourth_leg = min_hub_to_any_origin.get(inbound_hub)
                            if global_min_fourth_leg is not None:
                                global_lower_bound_total = (
                                    base_three_leg_total + global_min_fourth_leg
                                )
                                if outbound_hub == inbound_hub:
                                    bundled_origin_return = min_hub_to_origin.get(
                                        (inbound_hub, origin)
                                    )
                                    if bundled_origin_return is not None:
                                        bundled_lower_bound_total, _ = (
                                            _apply_inner_return_bundle_estimate(
                                                base_total=base_three_leg_total
                                                + bundled_origin_return,
                                                outbound_market_price=second_leg_price,
                                                inbound_market_price=third_leg_price,
                                            )
                                        )
                                        global_lower_bound_total = min(
                                            global_lower_bound_total,
                                            bundled_lower_bound_total,
                                        )
                                if not candidate_bound_is_competitive(
                                    lower_total=global_lower_bound_total,
                                    distance_basis_km=distance_basis_km,
                                    outbound_time_proxy_seconds=estimated_outbound_time_seconds,
                                ):
                                    continue

                            for arrival_origin in origins:
                                min_fourth_leg = min_hub_to_origin.get(
                                    (inbound_hub, arrival_origin)
                                )
                                if min_fourth_leg is None:
                                    continue

                                lower_bound_total = base_three_leg_total + min_fourth_leg
                                if origin == arrival_origin and outbound_hub == inbound_hub:
                                    lower_bound_total, _ = _apply_inner_return_bundle_estimate(
                                        base_total=lower_bound_total,
                                        outbound_market_price=second_leg_price,
                                        inbound_market_price=third_leg_price,
                                    )
                                if not candidate_bound_is_competitive(
                                    lower_total=lower_bound_total,
                                    distance_basis_km=distance_basis_km,
                                    outbound_time_proxy_seconds=estimated_outbound_time_seconds,
                                ):
                                    continue

                                fourth_leg_prices = hub_to_origin.get(
                                    (inbound_hub, arrival_origin), ()
                                )
                                for inbound_stopover_days in range(
                                    min_stopover_days,
                                    max_stopover_days + 1,
                                ):
                                    return_origin_index = (
                                        leave_destination_index + inbound_stopover_days
                                    )
                                    if return_origin_index >= date_count:
                                        continue
                                    return_origin_key = date_keys[return_origin_index]

                                    fourth_leg_price = (
                                        fourth_leg_prices[return_origin_index]
                                        if return_origin_index < len(fourth_leg_prices)
                                        else None
                                    )
                                    if fourth_leg_price is None:
                                        continue

                                    estimated_total = (
                                        first_leg_price
                                        + second_leg_price
                                        + third_leg_price
                                        + fourth_leg_price
                                    )
                                    estimated_pricing_strategy = "separate_oneways"
                                    if origin == arrival_origin and outbound_hub == inbound_hub:
                                        bundle_total, bundle_price = (
                                            _apply_inner_return_bundle_estimate(
                                                base_total=estimated_total,
                                                outbound_market_price=second_leg_price,
                                                inbound_market_price=third_leg_price,
                                            )
                                        )
                                        if bundle_price is not None:
                                            estimated_total = bundle_total
                                            estimated_pricing_strategy = "inner_return_bundle_proxy"

                                    estimated_score = _estimate_objective_score(
                                        objective=objective,
                                        estimated_total=estimated_total,
                                        distance_basis_km=distance_basis_km,
                                        outbound_time_proxy_seconds=estimated_outbound_time_seconds,
                                    )

                                    push_candidate(
                                        {
                                            "candidate_type": "split_stopover",
                                            "destination": destination,
                                            "origin": origin,
                                            "arrival_origin": arrival_origin,
                                            "outbound_hub": outbound_hub,
                                            "inbound_hub": inbound_hub,
                                            "depart_origin_date": depart_origin_key,
                                            "depart_destination_date": depart_destination_key,
                                            "leave_destination_date": leave_destination_key,
                                            "return_origin_date": return_origin_key,
                                            "outbound_stopover_days": outbound_stopover_days,
                                            "inbound_stopover_days": inbound_stopover_days,
                                            "main_stay_days": main_stay_days,
                                            "estimated_total": estimated_total,
                                            "distance_basis_km": distance_basis_km,
                                            "estimated_score": estimated_score,
                                            "estimated_outbound_time_to_destination_seconds": (
                                                estimated_outbound_time_seconds
                                            ),
                                            "estimated_pricing_strategy": estimated_pricing_strategy,
                                        },
                                        estimated_score=estimated_score,
                                        estimated_total=estimated_total,
                                    )

    if max_transfers_per_direction >= 2 and hub_to_hub:
        pair_limit = max(CANDIDATE_CHAIN_PAIR_LIMIT_MIN, int(math.sqrt(max_candidates)))
        if objective in {"best", "cheapest"}:
            pair_limit *= CANDIDATE_CHAIN_PAIR_PRIORITY_MULTIPLIER
        if max_transfers_per_direction >= 3:
            pair_limit = int(math.ceil(pair_limit * 1.25))
        pair_limit *= chain_pair_limit_multiplier
        pair_limit = min(CANDIDATE_CHAIN_PAIR_LIMIT_MAX, pair_limit)
        outbound_pairs = _rank_chain_pairs(
            origins=origins,
            first_hubs=outbound_hubs,
            second_hubs=outbound_hubs,
            leg_a_map=origin_to_hub,
            leg_b_map=hub_to_hub,
            leg_c_map=hub_to_destination,
            reverse_leg_c_map=destination_to_hub,
            pair_limit=pair_limit,
        )
        inbound_pairs = _rank_inbound_chain_pairs(
            origins=origins,
            first_hubs=inbound_hubs,
            second_hubs=inbound_hubs,
            destination_to_hub=destination_to_hub,
            hub_to_destination=hub_to_destination,
            hub_to_hub=hub_to_hub,
            hub_to_origin=hub_to_origin,
            pair_limit=pair_limit,
        )

        for depart_index in range(chunk_start_index, chunk_end_index):
            depart_origin_key = date_keys[depart_index]
            for origin, out_hub_a, out_hub_b in outbound_pairs:
                distance_basis_km = destination_distance_map.get((origin, destination))
                first_leg_prices = origin_to_hub.get((origin, out_hub_a), ())
                first_leg_price = (
                    first_leg_prices[depart_index] if depart_index < len(first_leg_prices) else None
                )
                if first_leg_price is None:
                    continue

                for out_stop_days_a in range(min_stopover_days, max_stopover_days + 1):
                    date_leg_2_index = depart_index + out_stop_days_a
                    if date_leg_2_index >= date_count:
                        continue
                    date_leg_2_key = date_keys[date_leg_2_index]
                    second_leg_prices = hub_to_hub.get((out_hub_a, out_hub_b), ())
                    second_leg_price = (
                        second_leg_prices[date_leg_2_index]
                        if date_leg_2_index < len(second_leg_prices)
                        else None
                    )
                    if second_leg_price is None:
                        continue

                    for out_stop_days_b in range(min_stopover_days, max_stopover_days + 1):
                        date_leg_3_index = date_leg_2_index + out_stop_days_b
                        if date_leg_3_index >= date_count:
                            continue
                        date_leg_3_key = date_keys[date_leg_3_index]
                        third_leg_prices = hub_to_destination.get(out_hub_b, ())
                        third_leg_price = (
                            third_leg_prices[date_leg_3_index]
                            if date_leg_3_index < len(third_leg_prices)
                            else None
                        )
                        if third_leg_price is None:
                            continue

                        estimated_outbound_time_seconds = _estimated_outbound_time_proxy_seconds(
                            depart_origin_date=dt.date.fromisoformat(depart_origin_key),
                            depart_destination_date=dt.date.fromisoformat(date_leg_3_key),
                            outbound_transfer_count=2,
                        )

                        for main_stay_days in range(min_stay_days, max_stay_days + 1):
                            leave_destination_index = date_leg_3_index + main_stay_days
                            if leave_destination_index >= date_count:
                                continue
                            leave_destination_key = date_keys[leave_destination_index]

                            for arrival_origin, in_hub_a, in_hub_b in inbound_pairs:
                                fourth_leg_prices = destination_to_hub.get(in_hub_a, ())
                                fourth_leg_price = (
                                    fourth_leg_prices[leave_destination_index]
                                    if leave_destination_index < len(fourth_leg_prices)
                                    else None
                                )
                                if fourth_leg_price is None:
                                    continue

                                min_fifth_leg = min_hub_to_hub.get((in_hub_a, in_hub_b))
                                min_sixth_leg = min_hub_to_origin.get((in_hub_b, arrival_origin))
                                if min_fifth_leg is None or min_sixth_leg is None:
                                    continue

                                lower_bound_total = (
                                    first_leg_price
                                    + second_leg_price
                                    + third_leg_price
                                    + fourth_leg_price
                                    + min_fifth_leg
                                    + min_sixth_leg
                                )
                                if origin == arrival_origin and out_hub_b == in_hub_a:
                                    lower_bound_total, _ = _apply_inner_return_bundle_estimate(
                                        base_total=lower_bound_total,
                                        outbound_market_price=third_leg_price,
                                        inbound_market_price=fourth_leg_price,
                                    )
                                if not candidate_bound_is_competitive(
                                    lower_total=lower_bound_total,
                                    distance_basis_km=distance_basis_km,
                                    outbound_time_proxy_seconds=estimated_outbound_time_seconds,
                                ):
                                    continue

                                fifth_leg_prices = hub_to_hub.get((in_hub_a, in_hub_b), ())
                                sixth_leg_prices = hub_to_origin.get((in_hub_b, arrival_origin), ())
                                for in_stop_days_a in range(
                                    min_stopover_days, max_stopover_days + 1
                                ):
                                    date_leg_5_index = leave_destination_index + in_stop_days_a
                                    if date_leg_5_index >= date_count:
                                        continue
                                    date_leg_5_key = date_keys[date_leg_5_index]
                                    fifth_leg_price = (
                                        fifth_leg_prices[date_leg_5_index]
                                        if date_leg_5_index < len(fifth_leg_prices)
                                        else None
                                    )
                                    if fifth_leg_price is None:
                                        continue

                                    for in_stop_days_b in range(
                                        min_stopover_days,
                                        max_stopover_days + 1,
                                    ):
                                        return_origin_index = date_leg_5_index + in_stop_days_b
                                        if return_origin_index >= date_count:
                                            continue
                                        return_origin_key = date_keys[return_origin_index]
                                        sixth_leg_price = (
                                            sixth_leg_prices[return_origin_index]
                                            if return_origin_index < len(sixth_leg_prices)
                                            else None
                                        )
                                        if sixth_leg_price is None:
                                            continue

                                        estimated_total = (
                                            first_leg_price
                                            + second_leg_price
                                            + third_leg_price
                                            + fourth_leg_price
                                            + fifth_leg_price
                                            + sixth_leg_price
                                        )
                                        estimated_pricing_strategy = "separate_oneways"
                                        if origin == arrival_origin and out_hub_b == in_hub_a:
                                            bundle_total, bundle_price = (
                                                _apply_inner_return_bundle_estimate(
                                                    base_total=estimated_total,
                                                    outbound_market_price=third_leg_price,
                                                    inbound_market_price=fourth_leg_price,
                                                )
                                            )
                                            if bundle_price is not None:
                                                estimated_total = bundle_total
                                                estimated_pricing_strategy = (
                                                    "inner_return_bundle_proxy"
                                                )

                                        estimated_score = _estimate_objective_score(
                                            objective=objective,
                                            estimated_total=estimated_total,
                                            distance_basis_km=distance_basis_km,
                                            outbound_time_proxy_seconds=estimated_outbound_time_seconds,
                                        )

                                        push_candidate(
                                            {
                                                "candidate_type": "split_chain",
                                                "destination": destination,
                                                "origin": origin,
                                                "arrival_origin": arrival_origin,
                                                "outbound_hub": f"{out_hub_a}/{out_hub_b}",
                                                "inbound_hub": f"{in_hub_a}/{in_hub_b}",
                                                "depart_origin_date": depart_origin_key,
                                                "depart_destination_date": date_leg_3_key,
                                                "leave_destination_date": leave_destination_key,
                                                "return_origin_date": return_origin_key,
                                                "outbound_stopover_days": out_stop_days_a
                                                + out_stop_days_b,
                                                "inbound_stopover_days": in_stop_days_a
                                                + in_stop_days_b,
                                                "outbound_boundary_stopover_days": [
                                                    out_stop_days_a,
                                                    out_stop_days_b,
                                                ],
                                                "inbound_boundary_stopover_days": [
                                                    in_stop_days_a,
                                                    in_stop_days_b,
                                                ],
                                                "main_stay_days": main_stay_days,
                                                "estimated_total": estimated_total,
                                                "distance_basis_km": distance_basis_km,
                                                "estimated_score": estimated_score,
                                                "estimated_outbound_time_to_destination_seconds": (
                                                    estimated_outbound_time_seconds
                                                ),
                                                "estimated_pricing_strategy": estimated_pricing_strategy,
                                                "outbound_legs": [
                                                    {
                                                        "source": origin,
                                                        "destination": out_hub_a,
                                                        "date": depart_origin_key,
                                                    },
                                                    {
                                                        "source": out_hub_a,
                                                        "destination": out_hub_b,
                                                        "date": date_leg_2_key,
                                                    },
                                                    {
                                                        "source": out_hub_b,
                                                        "destination": destination,
                                                        "date": date_leg_3_key,
                                                    },
                                                ],
                                                "inbound_legs": [
                                                    {
                                                        "source": destination,
                                                        "destination": in_hub_a,
                                                        "date": leave_destination_key,
                                                    },
                                                    {
                                                        "source": in_hub_a,
                                                        "destination": in_hub_b,
                                                        "date": date_leg_5_key,
                                                    },
                                                    {
                                                        "source": in_hub_b,
                                                        "destination": arrival_origin,
                                                        "date": return_origin_key,
                                                    },
                                                ],
                                            },
                                            estimated_score=estimated_score,
                                            estimated_total=estimated_total,
                                        )

    for depart_index in range(chunk_start_index, chunk_end_index):
        depart_origin_key = date_keys[depart_index]
        for origin in origins:
            distance_basis_km = destination_distance_map.get((origin, destination))
            outbound_prices = origin_to_destination.get((origin, destination), ())
            inbound_prices = destination_to_origin.get((destination, origin), ())

            fallback_outbound = _min_series_price(outbound_prices) or 999_999
            fallback_inbound = _min_series_price(inbound_prices) or 999_999

            first_leg_price = (
                outbound_prices[depart_index] if depart_index < len(outbound_prices) else None
            )
            first_leg_missing = first_leg_price is None
            if first_leg_missing:
                first_leg_price = fallback_outbound

            for main_stay_days in range(min_stay_days, max_stay_days + 1):
                return_origin_index = depart_index + main_stay_days
                if return_origin_index >= date_count:
                    continue
                return_origin_key = date_keys[return_origin_index]

                second_leg_price = (
                    inbound_prices[return_origin_index]
                    if return_origin_index < len(inbound_prices)
                    else None
                )
                second_leg_missing = second_leg_price is None
                if second_leg_missing:
                    second_leg_price = fallback_inbound

                availability_penalty = 0
                if first_leg_missing:
                    availability_penalty += 250
                if second_leg_missing:
                    availability_penalty += 250

                estimated_total = first_leg_price + second_leg_price + availability_penalty
                estimated_outbound_time_seconds = _estimated_outbound_time_proxy_seconds(
                    depart_origin_date=dt.date.fromisoformat(depart_origin_key),
                    depart_destination_date=dt.date.fromisoformat(depart_origin_key),
                    outbound_transfer_count=0,
                )
                estimated_score = _estimate_objective_score(
                    objective=objective,
                    estimated_total=estimated_total,
                    distance_basis_km=distance_basis_km,
                    outbound_time_proxy_seconds=estimated_outbound_time_seconds,
                )

                direct_candidates.append(
                    {
                        "candidate_type": "direct_roundtrip",
                        "destination": destination,
                        "origin": origin,
                        "arrival_origin": origin,
                        "depart_origin_date": depart_origin_key,
                        "return_origin_date": return_origin_key,
                        "main_stay_days": main_stay_days,
                        "estimated_total": estimated_total,
                        "calendar_outbound_missing": first_leg_missing,
                        "calendar_inbound_missing": second_leg_missing,
                        "distance_basis_km": distance_basis_km,
                        "estimated_score": estimated_score,
                        "estimated_outbound_time_to_destination_seconds": (
                            estimated_outbound_time_seconds
                        ),
                    }
                )

    split_candidates = [item[3] for item in heap]
    return _finalize_estimated_candidates(
        split_candidates + direct_candidates,
        objective=objective,
        max_candidates=max_candidates,
        max_direct_candidates=max_direct_candidates,
    )


class SplitTripOptimizer:
    """Layover-first optimizer for direct, stopover, and split-ticket searches."""

    def __init__(
        self, client: KiwiClient | dict[str, Any], coordinates: AirportCoordinates
    ) -> None:
        """Initialize the SplitTripOptimizer.

        Args:
            client: Provider client used for the request.
            coordinates: Airport coordinates to evaluate.
        """
        self.runtime_provider_secrets: dict[str, str] = {}
        self.providers: dict[str, Any] = {}
        self._set_provider_instances(client)
        self.coords = coordinates
        self.route_graph = RouteConnectivityGraph()

    def _set_provider_instances(self, client: KiwiClient | dict[str, Any]) -> None:
        """Configure the active provider instances for the optimizer.

        Args:
            client: Provider client used for the request.
        """
        if isinstance(client, dict):
            providers = {str(k).lower(): v for k, v in client.items()}
        else:
            providers = {"kiwi": client}

        if "kiwi" not in providers:
            providers["kiwi"] = KiwiClient()

        providers["kayak"] = KayakScrapeClient()
        providers["momondo"] = MomondoScrapeClient()
        providers["googleflights"] = GoogleFlightsLocalClient()
        providers["skyscanner"] = SkyscannerScrapeClient()
        travelpayouts_token = (
            self.runtime_provider_secrets.get("travelpayouts_api_token") or ""
        ).strip()
        travelpayouts_market = (
            self.runtime_provider_secrets.get("travelpayouts_market") or ""
        ).strip()
        providers["travelpayouts"] = TravelpayoutsDataClient(
            api_token=travelpayouts_token if travelpayouts_token else None,
            market=travelpayouts_market if travelpayouts_market else None,
        )

        amadeus_id = (self.runtime_provider_secrets.get("amadeus_client_id") or "").strip()
        amadeus_secret = (self.runtime_provider_secrets.get("amadeus_client_secret") or "").strip()
        amadeus_base_url = (self.runtime_provider_secrets.get("amadeus_base_url") or "").strip()
        providers["amadeus"] = AmadeusClient(
            client_id=amadeus_id if amadeus_id else None,
            client_secret=amadeus_secret if amadeus_secret else None,
            base_url=amadeus_base_url if amadeus_base_url else None,
        )

        serpapi_key = (self.runtime_provider_secrets.get("serpapi_api_key") or "").strip()
        serpapi_search_url = (self.runtime_provider_secrets.get("serpapi_search_url") or "").strip()
        serpapi_scan_limit_raw = (
            self.runtime_provider_secrets.get("serpapi_return_option_scan_limit") or ""
        ).strip()
        try:
            serpapi_scan_limit = int(serpapi_scan_limit_raw) if serpapi_scan_limit_raw else None
        except ValueError:
            serpapi_scan_limit = None
        providers["serpapi"] = SerpApiGoogleFlightsClient(
            api_key=serpapi_key if serpapi_key else None,
            search_url=serpapi_search_url if serpapi_search_url else None,
            return_option_scan_limit=serpapi_scan_limit,
        )

        self.providers = providers

    def update_runtime_provider_secrets(self, payload: dict[str, Any]) -> None:
        """Update runtime provider credentials and related settings.

        Args:
            payload: JSON-serializable payload for the operation.
        """
        mapping = {
            "travelpayouts_api_token": "travelpayouts_api_token",
            "travelpayouts_market": "travelpayouts_market",
            "amadeus_client_id": "amadeus_client_id",
            "amadeus_client_secret": "amadeus_client_secret",
            "amadeus_base_url": "amadeus_base_url",
            "serpapi_api_key": "serpapi_api_key",
            "serpapi_search_url": "serpapi_search_url",
            "serpapi_return_option_scan_limit": "serpapi_return_option_scan_limit",
        }
        for incoming_key, internal_key in mapping.items():
            if incoming_key not in payload:
                continue
            raw = str(payload.get(incoming_key) or "").strip()
            if raw:
                self.runtime_provider_secrets[internal_key] = raw
            elif internal_key in self.runtime_provider_secrets:
                self.runtime_provider_secrets.pop(internal_key, None)

        self._set_provider_instances(self.providers.get("kiwi", KiwiClient()))

    def runtime_provider_config_status(self) -> dict[str, bool]:
        """Return runtime provider configuration status.

        Returns:
            dict[str, bool]: Runtime provider configuration status.
        """
        return {
            "travelpayouts_api_token_set": bool(
                self.runtime_provider_secrets.get("travelpayouts_api_token")
            ),
            "travelpayouts_market_set": bool(
                self.runtime_provider_secrets.get("travelpayouts_market")
            ),
            "amadeus_client_id_set": bool(self.runtime_provider_secrets.get("amadeus_client_id")),
            "amadeus_client_secret_set": bool(
                self.runtime_provider_secrets.get("amadeus_client_secret")
            ),
            "serpapi_api_key_set": bool(self.runtime_provider_secrets.get("serpapi_api_key")),
            "serpapi_return_option_scan_limit_set": bool(
                self.runtime_provider_secrets.get("serpapi_return_option_scan_limit")
            ),
        }

    @staticmethod
    def _available_cpu_workers() -> int:
        """Return the number of CPU workers available to the process.

        Returns:
            int: The number of CPU workers available to the process.
        """
        return max(1, os.cpu_count() or 1)

    def runtime_capabilities(self) -> dict[str, int]:
        """Return runtime capability information for the optimizer.

        Returns:
            dict[str, int]: Runtime capability information for the optimizer.
        """
        return {
            "cpu_workers_default": DEFAULT_CPU_WORKERS,
            "cpu_workers_max": self._available_cpu_workers(),
            "io_workers_default": DEFAULT_IO_WORKERS,
            "io_workers_max": 96,
        }

    def _distance_km(self, from_code: str, to_code: str) -> float | None:
        """Calculate the direct distance in kilometers between two airports.

        Args:
            from_code: Origin airport code for the segment.
            to_code: Destination airport code for the segment.

        Returns:
            float | None: Calculated direct distance in kilometers between two airports.
        """
        a = self.coords.get(from_code)
        b = self.coords.get(to_code)
        if not a or not b:
            return None
        return haversine_km(a, b)

    def _distance_for_route(self, airports: list[str]) -> float | None:
        """Calculate the direct distance for a route.

        Args:
            airports: Collection of airports.

        Returns:
            float | None: Calculated direct distance for a route.
        """
        total = 0.0
        for idx in range(len(airports) - 1):
            d = self._distance_km(airports[idx], airports[idx + 1])
            if d is None:
                return None
            total += d
        return total

    def _destination_display_name(self, code: str) -> str:
        """Return the human-readable destination label.

        Args:
            code: Airport or provider code to process.

        Returns:
            str: The human-readable destination label.
        """
        normalized = str(code or "").strip().upper()
        if not normalized:
            return str(code or "")
        notes_name = str((DESTINATION_NOTES.get(normalized) or {}).get("name") or "").strip()
        if notes_name:
            return notes_name
        city_name = self.coords.display_name(normalized)
        if city_name:
            return city_name
        return normalized

    def _expand_route_graph_hub_candidates(
        self,
        config: SearchConfig,
    ) -> tuple[SearchConfig, dict[str, Any], list[str]]:
        """Expand the hub pool with route-graph candidates.

        Args:
            config: Search configuration for the operation.

        Returns:
            tuple[SearchConfig, dict[str, Any], list[str]]: Expand the hub pool with route-graph candidates.
        """
        base_hubs = tuple(dict.fromkeys(config.hub_candidates))
        meta: dict[str, Any] = {
            "hub_candidates_input_count": len(base_hubs),
            "hub_candidates_graph_count": 0,
            "hub_candidates_effective_count": len(base_hubs),
            "hub_candidates_graph_applied": False,
            "hub_candidates_graph_available": False,
            "hub_candidates_graph_source": "openflights_routes",
        }
        warnings: list[str] = []
        max_split_hubs = min(2, max(0, int(config.max_transfers_per_direction)))
        if max_split_hubs <= 0:
            return config, meta, warnings

        graph_available = self.route_graph.available()
        meta["hub_candidates_graph_available"] = graph_available
        if not graph_available:
            return config, meta, warnings

        graph_scores = self.route_graph.score_path_hubs(
            origins=config.origins,
            destinations=config.destinations,
            max_split_hubs=max_split_hubs,
        )
        if not graph_scores:
            return config, meta, warnings

        base_order = {code: index for index, code in enumerate(base_hubs)}
        graph_hubs = tuple(
            sorted(
                graph_scores,
                key=lambda code: (
                    -int(graph_scores.get(code) or 0),
                    base_order.get(code, len(base_hubs)),
                    code,
                ),
            )
        )
        effective_hubs = tuple(dict.fromkeys([*graph_hubs, *base_hubs]))
        meta["hub_candidates_graph_count"] = len(graph_hubs)
        meta["hub_candidates_effective_count"] = len(effective_hubs)
        meta["hub_candidates_graph_applied"] = effective_hubs != base_hubs
        if meta["hub_candidates_graph_applied"]:
            warnings.append(
                "Route-graph auto discovery expanded the hub pool with "
                f"{len(graph_hubs)} connectivity-based airports."
            )
            config = replace(config, hub_candidates=effective_hubs)
        return config, meta, warnings

    def provider_catalog(
        self, requested_provider_ids: tuple[str, ...] | None = None
    ) -> list[dict[str, Any]]:
        """Handle provider catalog.

        Args:
            requested_provider_ids: Identifiers for requested provider.

        Returns:
            list[dict[str, Any]]: Handle provider catalog.
        """
        requested = set(requested_provider_ids or SUPPORTED_PROVIDER_IDS)
        catalog: list[dict[str, Any]] = []
        for provider_id in SUPPORTED_PROVIDER_IDS:
            provider = self.providers.get(provider_id)
            configured = bool(provider and provider.is_configured())
            requires_credentials = bool(
                provider and getattr(provider, "requires_credentials", False)
            )
            credential_env = list(getattr(provider, "credential_env", ())) if provider else []
            missing_env = [env for env in credential_env if not os.getenv(env)]
            selected_by_request = provider_id in requested
            active = bool(provider and selected_by_request and configured)
            default_enabled = (
                bool(getattr(provider, "default_enabled", True)) if provider else False
            )
            configuration_hint = None
            if provider:
                with contextlib.suppress(Exception):
                    hint_getter = getattr(provider, "configuration_hint", None)
                    if callable(hint_getter):
                        configuration_hint = hint_getter()
            catalog.append(
                {
                    "id": provider_id,
                    "name": (
                        getattr(provider, "display_name", provider_id.upper())
                        if provider
                        else provider_id
                    ),
                    "docs_url": getattr(provider, "docs_url", None) if provider else None,
                    "selected_by_request": selected_by_request,
                    "configured": configured,
                    "active": active,
                    "requires_credentials": requires_credentials,
                    "credential_env": credential_env,
                    "missing_env": missing_env,
                    "default_enabled": default_enabled,
                    "configuration_hint": configuration_hint,
                }
            )
        return catalog

    def _build_search_client(
        self,
        config: SearchConfig,
    ) -> tuple[MultiProviderClient, list[dict[str, Any]], list[str]]:
        """Build search client.

        Args:
            config: Search configuration for the operation.

        Returns:
            tuple[MultiProviderClient, list[dict[str, Any]], list[str]]: Search client.
        """
        provider_status = self.provider_catalog(config.provider_ids)
        active_providers = [
            self.providers[item["id"]] for item in provider_status if item.get("active")
        ]
        warnings: list[str] = []
        for item in provider_status:
            if item.get("selected_by_request") and not item.get("configured"):
                missing_env = item.get("missing_env") or []
                if missing_env:
                    warnings.append(
                        f"Provider {item['id']} skipped (missing credentials: {', '.join(missing_env)})."
                    )
                else:
                    hint = str(item.get("configuration_hint") or "").strip()
                    if hint:
                        warnings.append(f"Provider {item['id']} skipped ({hint}).")
                    else:
                        warnings.append(f"Provider {item['id']} skipped (not configured).")
        if not active_providers:
            active_providers = [self.providers["kiwi"]]
            warnings.append("No requested provider is configured. Falling back to Kiwi.")
            provider_status = self.provider_catalog(("kiwi",))
        provider_caps = {
            "kiwi": config.max_calls_kiwi,
            # Keep free scraper providers uncapped unless the global cap is used.
            # `max_calls_kiwi` should only apply to Kiwi itself.
            "kayak": None,
            "momondo": None,
            "googleflights": None,
            "skyscanner": None,
            "amadeus": config.max_calls_amadeus,
            "serpapi": config.max_calls_serpapi,
        }
        return (
            MultiProviderClient(
                active_providers,
                max_total_calls=config.max_total_provider_calls,
                max_calls_by_provider=provider_caps,
            ),
            provider_status,
            warnings,
        )

    def parse_search_config(self, payload: dict[str, Any]) -> SearchConfig:
        """Parse search config.

        Args:
            payload: JSON-serializable payload for the operation.

        Returns:
            SearchConfig: Parsed search config.
        """
        today = dt.date.today()
        default_period_start = today + dt.timedelta(days=45)
        default_period_end = default_period_start + dt.timedelta(days=60)

        period_start = to_date(
            payload.get("period_start") or payload.get("departure_start"),
            default_period_start,
        )
        period_end = to_date(
            payload.get("period_end") or payload.get("departure_end"),
            default_period_end,
        )

        if period_end < period_start:
            raise ValueError("period_end must be on or after period_start")

        min_stay_days = clamp_int(payload.get("min_stay_days"), 6, 1, 30)
        max_stay_days = clamp_int(payload.get("max_stay_days"), 8, min_stay_days, 45)

        min_stopover_days = clamp_int(payload.get("min_stopover_days"), 0, 0, 10)
        max_stopover_days = clamp_int(
            payload.get("max_stopover_days"),
            5,
            min_stopover_days,
            14,
        )

        transfers_raw = payload.get("max_transfers_per_direction")
        if transfers_raw in (None, ""):
            if payload.get("max_layovers_per_direction") not in (None, ""):
                transfers_raw = payload.get("max_layovers_per_direction")
            else:
                transfers_raw = payload.get("max_stops_per_leg")
        max_transfers_per_direction = clamp_int(
            transfers_raw,
            2,
            0,
            6,
        )
        # Provider APIs cap per-leg stops independently. Keep a high enough cap per queried leg,
        # while final itinerary filtering enforces the single direction transfer limit.
        max_stops_per_leg = min(3, max_transfers_per_direction)
        max_layovers_per_direction = max_transfers_per_direction
        max_connection_layover_hours_raw = clamp_int(
            payload.get("max_connection_layover_hours"),
            0,
            0,
            240,
        )
        max_connection_layover_hours = (
            max_connection_layover_hours_raw if max_connection_layover_hours_raw > 0 else None
        )

        top_results = clamp_int(payload.get("top_results"), 20, 1, 100)
        validate_top_per_destination = clamp_int(
            payload.get("validate_top_per_destination"),
            140,
            10,
            500,
        )

        estimated_pool_multiplier = clamp_int(
            payload.get("estimated_pool_multiplier"),
            8,
            2,
            50,
        )
        calendar_hubs_prefetch = clamp_optional_int(
            payload.get("calendar_hubs_prefetch"),
            DEFAULT_CALENDAR_HUBS_PREFETCH,
            4,
            96,
        )
        max_validate_oneway_keys_per_destination = clamp_optional_int(
            payload.get("max_validate_oneway_keys_per_destination"),
            DEFAULT_MAX_VALIDATE_ONEWAY_KEYS,
            20,
            2000,
        )
        max_validate_return_keys_per_destination = clamp_optional_int(
            payload.get("max_validate_return_keys_per_destination"),
            DEFAULT_MAX_VALIDATE_RETURN_KEYS,
            10,
            800,
        )
        max_total_provider_calls = clamp_optional_int(
            payload.get("max_total_provider_calls"),
            DEFAULT_MAX_TOTAL_PROVIDER_CALLS,
            50,
            50000,
        )
        max_calls_kiwi = clamp_optional_int(
            payload.get("max_calls_kiwi"),
            DEFAULT_MAX_CALLS_KIWI,
            20,
            50000,
        )
        max_calls_amadeus = clamp_optional_int(
            payload.get("max_calls_amadeus"),
            DEFAULT_MAX_CALLS_AMADEUS,
            20,
            50000,
        )
        max_calls_serpapi = clamp_optional_int(
            payload.get("max_calls_serpapi"),
            DEFAULT_MAX_CALLS_SERPAPI,
            10,
            50000,
        )
        serpapi_probe_oneway_keys = clamp_int(
            payload.get("serpapi_probe_oneway_keys"),
            DEFAULT_SERPAPI_PROBE_ONEWAY_KEYS,
            0,
            5000,
        )
        serpapi_probe_return_keys = clamp_int(
            payload.get("serpapi_probe_return_keys"),
            DEFAULT_SERPAPI_PROBE_RETURN_KEYS,
            0,
            1000,
        )

        auto_hubs_per_direction = clamp_int(
            payload.get("auto_hubs_per_direction"),
            10,
            1,
            96,
        )
        exhaustive_hub_scan = to_bool(payload.get("exhaustive_hub_scan"), False)

        io_workers = clamp_int(
            payload.get("io_workers"),
            DEFAULT_IO_WORKERS,
            4,
            96,
        )
        available_cpu_workers = self._available_cpu_workers()
        cpu_workers_raw = payload.get("cpu_workers")
        cpu_workers_auto = False
        cpu_workers_raw_text = (
            str("" if cpu_workers_raw is None else cpu_workers_raw).strip().lower()
        )
        if (
            cpu_workers_raw in (None, "")
            or to_bool(payload.get("cpu_workers_auto"), False)
            or cpu_workers_raw_text
            in {
                "0",
                "auto",
                "max",
            }
        ):
            cpu_workers_auto = True
            cpu_workers = available_cpu_workers
        else:
            cpu_workers = clamp_int(
                cpu_workers_raw,
                DEFAULT_CPU_WORKERS,
                1,
                available_cpu_workers,
            )
        search_timeout_raw = payload.get("search_timeout_seconds")
        search_timeout_text = (
            str("" if search_timeout_raw is None else search_timeout_raw).strip().lower()
        )
        if search_timeout_raw in (None, "") or search_timeout_text in {
            "0",
            "none",
            "off",
            "unlimited",
            "infinite",
            "inf",
        }:
            search_timeout_seconds = 0
        else:
            search_timeout_seconds = clamp_int(
                search_timeout_raw,
                DEFAULT_SEARCH_TIMEOUT_SECONDS,
                60,
                7200,
            )

        currency = str(payload.get("currency") or "RON").upper()
        objective_raw = str(payload.get("objective") or "best").strip().lower()
        objective_aliases = {
            "best": "best",
            "best_value": "best",
            "cheapest": "cheapest",
            "total_price": "cheapest",
            "fastest": "fastest",
            "price_per_km": "price_per_km",
        }
        objective = objective_aliases.get(objective_raw, "best")
        providers_raw = payload.get("providers")
        if providers_raw in (None, "", []):
            auto_provider_ids: list[str] = []
            for provider_id in SUPPORTED_PROVIDER_IDS:
                provider = self.providers.get(provider_id)
                if not provider:
                    continue
                if not bool(getattr(provider, "default_enabled", True)):
                    continue
                auto_provider_ids.append(provider_id)
            provider_ids = tuple(auto_provider_ids or ["kiwi"])
        else:
            provider_ids = normalize_provider_ids(providers_raw)
        # Default OFF: it doubles fare lookups (selected bags + base no-bag) and is only for comparison/debugging.
        market_compare_fares = to_bool(payload.get("market_compare_fares"), False)

        passengers_payload = payload.get("passengers") or {}
        passengers = PassengerConfig(
            adults=clamp_int(passengers_payload.get("adults"), 1, 1, 9),
            hand_bags=clamp_int(passengers_payload.get("hand_bags"), 1, 0, 3),
            hold_bags=clamp_int(passengers_payload.get("hold_bags"), 0, 0, 3),
        )

        destinations_fallback = (
            DEFAULT_DESTINATIONS
            if payload.get("use_beach_presets", True)
            else ["MLE", "SEZ", "PUJ"]
        )

        hub_candidates = normalize_codes(
            payload.get("hub_candidates") or payload.get("hubs"),
            AUTO_HUB_CANDIDATES,
        )
        if exhaustive_hub_scan:
            # Exhaustive hub scan means "consider all hubs in the calendar stage",
            # but candidate generation becomes O(hubs^2). Keep the auto-pick cap aligned
            # with the UI max (96) to avoid runaway CPU time on large hub pools.
            auto_hubs_per_direction = max(
                auto_hubs_per_direction,
                min(96, len(hub_candidates)),
            )

        return SearchConfig(
            origins=normalize_codes(payload.get("origins"), ["OTP"]),
            destinations=normalize_codes(payload.get("destinations"), destinations_fallback),
            hub_candidates=hub_candidates,
            auto_hubs_per_direction=auto_hubs_per_direction,
            exhaustive_hub_scan=exhaustive_hub_scan,
            period_start=period_start,
            period_end=period_end,
            min_stay_days=min_stay_days,
            max_stay_days=max_stay_days,
            min_stopover_days=min_stopover_days,
            max_stopover_days=max_stopover_days,
            max_transfers_per_direction=max_transfers_per_direction,
            max_stops_per_leg=max_stops_per_leg,
            max_layovers_per_direction=max_layovers_per_direction,
            max_connection_layover_hours=max_connection_layover_hours,
            currency=currency,
            objective=objective,
            provider_ids=provider_ids,
            market_compare_fares=market_compare_fares,
            validate_top_per_destination=validate_top_per_destination,
            top_results=top_results,
            estimated_pool_multiplier=estimated_pool_multiplier,
            calendar_hubs_prefetch=calendar_hubs_prefetch,
            max_validate_oneway_keys_per_destination=max_validate_oneway_keys_per_destination,
            max_validate_return_keys_per_destination=max_validate_return_keys_per_destination,
            max_total_provider_calls=max_total_provider_calls,
            max_calls_kiwi=max_calls_kiwi,
            max_calls_amadeus=max_calls_amadeus,
            max_calls_serpapi=max_calls_serpapi,
            serpapi_probe_oneway_keys=serpapi_probe_oneway_keys,
            serpapi_probe_return_keys=serpapi_probe_return_keys,
            io_workers=io_workers,
            cpu_workers=cpu_workers,
            cpu_workers_auto=cpu_workers_auto,
            search_timeout_seconds=search_timeout_seconds,
            passengers=passengers,
        )

    def _score_candidate(
        self,
        total_price: int,
        distance_basis_km: float | None,
        objective: str,
    ) -> float:
        """Score candidate.

        Args:
            total_price: Price for total.
            distance_basis_km: Distance basis in kilometers used for scoring.
            objective: Ranking objective for the search.

        Returns:
            float: Scored candidate.
        """
        if objective in {"best", "cheapest", "fastest"}:
            return float(total_price)
        if distance_basis_km and distance_basis_km > 0:
            return (total_price / distance_basis_km) * 1000.0
        return float("inf")

    @staticmethod
    def _transfer_airports(segments: list[dict[str, Any]]) -> list[str]:
        """Handle transfer airports.

        Args:
            segments: Mapping of segments.

        Returns:
            list[str]: Handle transfer airports.
        """
        out: list[str] = []
        for segment in segments[:-1]:
            code = str(segment.get("to") or "").upper().strip()
            if code and code not in out:
                out.append(code)
        return out

    @staticmethod
    def _compute_best_value_scores(results: list[dict[str, Any]]) -> None:
        """Handle compute best value scores.

        Args:
            results: Result records for the current operation.
        """
        _apply_price_time_score(
            results,
            price_key="total_price",
            time_key="outbound_time_to_destination_seconds",
            score_key="best_value_score",
        )

    @staticmethod
    def _as_int(value: Any) -> int | None:
        """Handle as int.

        Args:
            value: Input value to process.

        Returns:
            int | None: Handle as int.
        """
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _exceeds_connection_layover_limit(
        segments: list[dict[str, Any]],
        max_allowed_seconds: int | None,
    ) -> bool:
        """Handle exceeds connection layover limit.

        Args:
            segments: Mapping of segments.
            max_allowed_seconds: Duration in seconds for max allowed.

        Returns:
            bool: Handle exceeds connection layover limit.
        """
        if max_allowed_seconds is None:
            return False
        gap_seconds = max_segment_layover_seconds(segments)
        if gap_seconds is None:
            return False
        return gap_seconds > max_allowed_seconds

    @staticmethod
    def _candidate_split_plans(candidate: dict[str, Any]) -> dict[str, Any] | None:
        """Handle candidate split plans.

        Args:
            candidate: Mapping of candidate.

        Returns:
            dict[str, Any] | None: Handle candidate split plans.
        """
        candidate_type = str(candidate.get("candidate_type") or "split_stopover")
        if candidate_type == "direct_roundtrip":
            return None

        if candidate_type == "split_chain":
            outbound_plan = [
                (
                    str(leg.get("source") or "").upper(),
                    str(leg.get("destination") or "").upper(),
                    str(leg.get("date") or "")[:10],
                )
                for leg in (candidate.get("outbound_legs") or [])
                if str(leg.get("source") or "").strip()
                and str(leg.get("destination") or "").strip()
                and str(leg.get("date") or "").strip()
            ]
            inbound_plan = [
                (
                    str(leg.get("source") or "").upper(),
                    str(leg.get("destination") or "").upper(),
                    str(leg.get("date") or "")[:10],
                )
                for leg in (candidate.get("inbound_legs") or [])
                if str(leg.get("source") or "").strip()
                and str(leg.get("destination") or "").strip()
                and str(leg.get("date") or "").strip()
            ]
            outbound_boundary_days = [
                int(value or 0)
                for value in (candidate.get("outbound_boundary_stopover_days") or [])
            ]
            inbound_boundary_days = [
                int(value or 0) for value in (candidate.get("inbound_boundary_stopover_days") or [])
            ]
        else:
            outbound_plan = [
                (
                    str(candidate.get("origin") or "").upper(),
                    str(candidate.get("outbound_hub") or "").upper(),
                    str(candidate.get("depart_origin_date") or "")[:10],
                ),
                (
                    str(candidate.get("outbound_hub") or "").upper(),
                    str(candidate.get("destination") or "").upper(),
                    str(candidate.get("depart_destination_date") or "")[:10],
                ),
            ]
            inbound_plan = [
                (
                    str(candidate.get("destination") or "").upper(),
                    str(candidate.get("inbound_hub") or "").upper(),
                    str(candidate.get("leave_destination_date") or "")[:10],
                ),
                (
                    str(candidate.get("inbound_hub") or "").upper(),
                    str(candidate.get("arrival_origin") or "").upper(),
                    str(candidate.get("return_origin_date") or "")[:10],
                ),
            ]
            outbound_boundary_days = [int(candidate.get("outbound_stopover_days") or 0)]
            inbound_boundary_days = [int(candidate.get("inbound_stopover_days") or 0)]

        if not outbound_plan or not inbound_plan:
            return None

        while len(outbound_boundary_days) < max(0, len(outbound_plan) - 1):
            outbound_boundary_days.append(0)
        while len(inbound_boundary_days) < max(0, len(inbound_plan) - 1):
            inbound_boundary_days.append(0)

        return {
            "outbound_plan": outbound_plan,
            "inbound_plan": inbound_plan,
            "outbound_boundary_days": outbound_boundary_days,
            "inbound_boundary_days": inbound_boundary_days,
        }

    def _candidate_inner_return_plan(self, candidate: dict[str, Any]) -> dict[str, Any] | None:
        """Handle candidate inner return plan.

        Args:
            candidate: Mapping of candidate.

        Returns:
            dict[str, Any] | None: Handle candidate inner return plan.
        """
        plans = self._candidate_split_plans(candidate)
        if not plans:
            return None

        origin = str(candidate.get("origin") or "").upper()
        arrival_origin = str(candidate.get("arrival_origin") or "").upper()
        destination = str(candidate.get("destination") or "").upper()
        if not origin or not destination or origin != arrival_origin:
            return None

        outbound_plan: list[tuple[str, str, str]] = list(plans["outbound_plan"])
        inbound_plan: list[tuple[str, str, str]] = list(plans["inbound_plan"])
        outbound_boundary_days: list[int] = list(plans["outbound_boundary_days"])
        inbound_boundary_days: list[int] = list(plans["inbound_boundary_days"])

        if len(outbound_plan) < 2 or len(inbound_plan) < 2:
            return None

        gateway_code = str(outbound_plan[-1][0] or "").upper()
        if not gateway_code or gateway_code == destination:
            return None
        inbound_gateway_code = str(inbound_plan[0][1] or "").upper()
        if not inbound_gateway_code:
            return None

        return {
            "return_key": (
                gateway_code,
                destination,
                outbound_plan[-1][2],
                inbound_plan[0][2],
            ),
            "gateway_code": gateway_code,
            "expected_inbound_gateway_code": inbound_gateway_code,
            "outer_outbound_plan": outbound_plan[:-1],
            "outer_inbound_plan": inbound_plan[1:],
            "outer_outbound_boundary_days": outbound_boundary_days[:-1],
            "outer_inbound_boundary_days": inbound_boundary_days[1:],
            "outbound_bundle_boundary_days": int(outbound_boundary_days[-1] or 0),
            "inbound_bundle_boundary_days": int(inbound_boundary_days[0] or 0),
        }

    def _materialize_oneway_plan_entries(
        self,
        plan: list[tuple[str, str, str]],
        oneway_map: dict[tuple[str, str, str], dict[str, Any] | None],
        entry_cache: dict[tuple[str, str, str], dict[str, Any]] | None = None,
    ) -> list[dict[str, Any]] | None:
        """Handle materialize oneway plan entries.

        Args:
            plan: Collection of plan.
            oneway_map: Mapping of oneway.
            entry_cache: Cache of entry.

        Returns:
            list[dict[str, Any]] | None: Handle materialize oneway plan entries.
        """
        entries: list[dict[str, Any]] = []
        for source, destination, date_iso in plan:
            cached_entry = (entry_cache or {}).get((source, destination, date_iso))
            if cached_entry is not None:
                if not self._leg_matches_expected_route(
                    str(cached_entry.get("source") or ""),
                    str(cached_entry.get("destination") or ""),
                    source,
                    destination,
                ):
                    return None
                entries.append(cached_entry)
                continue

            fare = oneway_map.get((source, destination, date_iso))
            if not fare:
                return None
            segments = fare.get("segments") or []
            leg_source, leg_destination = leg_endpoints_from_segments(
                segments,
                source,
                destination,
            )
            if not self._leg_matches_expected_route(
                leg_source,
                leg_destination,
                source,
                destination,
            ):
                return None
            entries.append(
                {
                    "source": leg_source,
                    "destination": leg_destination,
                    "date": date_iso,
                    "fare": fare,
                    "segments": segments,
                    "duration_seconds": self._as_int(fare.get("duration_seconds")),
                    "stops": int(fare.get("stops") or 0),
                }
            )
        return entries

    def _validate_entry_boundaries(
        self,
        entries: list[dict[str, Any]],
        boundary_days: list[int],
        max_connection_layover_seconds: int | None,
    ) -> tuple[bool, int]:
        """Handle validate entry boundaries.

        Args:
            entries: Mapping of entries.
            boundary_days: Duration in days for boundary.
            max_connection_layover_seconds: Duration in seconds for max connection layover.

        Returns:
            tuple[bool, int]: Handle validate entry boundaries.
        """
        boundary_events = 0
        for idx in range(len(entries) - 1):
            current_entry = entries[idx]
            next_entry = entries[idx + 1]
            current_segments = current_entry.get("segments") or []
            next_segments = next_entry.get("segments") or []
            if not current_segments or not next_segments:
                return False, 0
            gap_seconds = connection_gap_seconds(
                current_segments[-1].get("arrive_local"),
                next_segments[0].get("depart_local"),
            )
            min_boundary = minimum_split_boundary_connection_seconds(
                current_entry.get("destination") or "",
                next_entry.get("source") or "",
            )
            if gap_seconds is None or gap_seconds < min_boundary:
                return False, 0
            stopover_days = int(boundary_days[idx] or 0) if idx < len(boundary_days) else 0
            if (
                max_connection_layover_seconds is not None
                and stopover_days <= 0
                and gap_seconds > max_connection_layover_seconds
            ):
                return False, 0
            boundary_events += boundary_transfer_events(
                current_entry.get("destination") or "",
                next_entry.get("source") or "",
            )
        return True, boundary_events

    @staticmethod
    def _leg_matches_expected_route(
        actual_source: str,
        actual_destination: str,
        expected_source: str,
        expected_destination: str,
    ) -> bool:
        """Handle leg matches expected route.

        Args:
            actual_source: Observed source airport code from provider data.
            actual_destination: Observed destination airport code from provider data.
            expected_source: Expected source airport code for validation.
            expected_destination: Expected destination airport code for validation.

        Returns:
            bool: Handle leg matches expected route.
        """
        return (
            str(actual_source or "").strip().upper() == str(expected_source or "").strip().upper()
            and str(actual_destination or "").strip().upper()
            == str(expected_destination or "").strip().upper()
        )

    @staticmethod
    def _oneway_entry_to_leg(
        entry: dict[str, Any],
        *,
        fallback_source: str,
        fallback_destination: str,
        fallback_date: str,
        max_stops_per_leg: int,
    ) -> dict[str, Any]:
        """Handle oneway entry to leg.

        Args:
            entry: Mapping of entry.
            fallback_source: Fallback source airport code to use when segments are missing.
            fallback_destination: Fallback destination airport code to use when segments are missing.
            fallback_date: Date for fallback.
            max_stops_per_leg: Max stops per leg.

        Returns:
            dict[str, Any]: Handle oneway entry to leg.
        """
        fare = entry.get("fare") or {}
        segments = entry.get("segments") or []
        return {
            "source": entry.get("source"),
            "destination": entry.get("destination"),
            "date": entry.get("date"),
            "price": fare.get("price"),
            "formatted_price": fare.get("formatted_price"),
            "stops": fare.get("stops"),
            "segments": segments,
            "duration_seconds": entry.get("duration_seconds"),
            "departure_local": segments[0].get("depart_local") if segments else None,
            "arrival_local": segments[-1].get("arrive_local") if segments else None,
            "fare_mode": fare.get("fare_mode", "selected_bags"),
            "provider": fare.get("provider", "kiwi"),
            "price_mode": fare.get("price_mode"),
            "booking_url": fare.get("booking_url")
            or kiwi_oneway_url(
                fallback_source,
                fallback_destination,
                fallback_date,
                max_stops_per_leg,
            ),
        }

    @staticmethod
    def _return_fare_to_ticket_leg(
        fare: dict[str, Any],
        *,
        source: str,
        destination: str,
        outbound_iso: str,
        inbound_iso: str,
        max_stops_per_leg: int,
    ) -> dict[str, Any]:
        """Handle return fare to ticket leg.

        Args:
            fare: Mapping of fare.
            source: Origin airport code for the request.
            destination: Destination airport code for the request.
            outbound_iso: Outbound travel date in ISO 8601 format.
            inbound_iso: Inbound travel date in ISO 8601 format.
            max_stops_per_leg: Max stops per leg.

        Returns:
            dict[str, Any]: Handle return fare to ticket leg.
        """
        outbound_segments = fare.get("outbound_segments") or []
        inbound_segments = fare.get("inbound_segments") or []
        return {
            "ticket_type": "roundtrip",
            "source": source,
            "destination": destination,
            "date": outbound_iso,
            "return_date": inbound_iso,
            "price": fare.get("price"),
            "formatted_price": fare.get("formatted_price"),
            "stops": (int(fare.get("outbound_stops") or 0) + int(fare.get("inbound_stops") or 0)),
            "outbound_stops": int(fare.get("outbound_stops") or 0),
            "inbound_stops": int(fare.get("inbound_stops") or 0),
            "segments": [],
            "outbound_segments": outbound_segments,
            "inbound_segments": inbound_segments,
            "duration_seconds": fare.get("duration_seconds"),
            "outbound_duration_seconds": fare.get("outbound_duration_seconds"),
            "inbound_duration_seconds": fare.get("inbound_duration_seconds"),
            "departure_local": (
                outbound_segments[0].get("depart_local") if outbound_segments else None
            ),
            "arrival_local": inbound_segments[-1].get("arrive_local") if inbound_segments else None,
            "fare_mode": fare.get("fare_mode", "selected_bags"),
            "provider": fare.get("provider", "kiwi"),
            "price_mode": fare.get("price_mode"),
            "booking_url": fare.get("booking_url")
            or kiwi_return_url(
                source,
                destination,
                outbound_iso,
                inbound_iso,
                max_stops_per_leg,
            ),
        }

    def _prepare_oneway_entry_cache(
        self,
        oneway_map: dict[tuple[str, str, str], dict[str, Any] | None],
    ) -> dict[tuple[str, str, str], dict[str, Any]]:
        """Handle prepare oneway entry cache.

        Args:
            oneway_map: Mapping of oneway.

        Returns:
            dict[tuple[str, str, str], dict[str, Any]]: Handle prepare oneway entry cache.
        """
        cache: dict[tuple[str, str, str], dict[str, Any]] = {}
        for (source, destination, date_iso), fare in oneway_map.items():
            if not fare:
                continue
            segments = fare.get("segments") or []
            leg_source, leg_destination = leg_endpoints_from_segments(
                segments,
                source,
                destination,
            )
            cache[(source, destination, date_iso)] = {
                "source": leg_source,
                "destination": leg_destination,
                "date": date_iso,
                "fare": fare,
                "segments": segments,
                "duration_seconds": self._as_int(fare.get("duration_seconds")),
                "stops": int(fare.get("stops") or 0),
            }
        return cache

    def _prepare_return_trip_cache(
        self,
        return_map: dict[tuple[str, str, str, str], dict[str, Any] | None],
    ) -> dict[tuple[str, str, str, str], dict[str, Any]]:
        """Handle prepare return trip cache.

        Args:
            return_map: Mapping of return.

        Returns:
            dict[tuple[str, str, str, str], dict[str, Any]]: Handle prepare return trip cache.
        """
        cache: dict[tuple[str, str, str, str], dict[str, Any]] = {}
        for (source, destination, outbound_iso, inbound_iso), fare in return_map.items():
            if not fare:
                continue
            outbound_segments = fare.get("outbound_segments") or []
            inbound_segments = fare.get("inbound_segments") or []
            outbound_source, outbound_destination = leg_endpoints_from_segments(
                outbound_segments,
                source,
                destination,
            )
            inbound_source, inbound_destination = leg_endpoints_from_segments(
                inbound_segments,
                destination,
                source,
            )
            cache[(source, destination, outbound_iso, inbound_iso)] = {
                "fare": fare,
                "source": source,
                "destination": destination,
                "outbound_iso": outbound_iso,
                "inbound_iso": inbound_iso,
                "outbound_segments": outbound_segments,
                "inbound_segments": inbound_segments,
                "outbound_source": outbound_source,
                "outbound_destination": outbound_destination,
                "inbound_source": inbound_source,
                "inbound_destination": inbound_destination,
                "outbound_duration_seconds": self._as_int(fare.get("outbound_duration_seconds")),
                "inbound_duration_seconds": self._as_int(fare.get("inbound_duration_seconds")),
            }
        return cache

    def _build_split_candidate_with_inner_return_bundle(
        self,
        *,
        candidate: dict[str, Any],
        inner_return_plan: dict[str, Any],
        oneway_map: dict[tuple[str, str, str], dict[str, Any] | None],
        return_map: dict[tuple[str, str, str, str], dict[str, Any] | None],
        config: SearchConfig,
        distance_basis_km: float | None,
        max_connection_layover_seconds: int | None,
        destination_name: str,
        notes: dict[str, str],
        oneway_entry_cache: dict[tuple[str, str, str], dict[str, Any]] | None = None,
        comparison_links: dict[str, str] | None = None,
    ) -> dict[str, Any] | None:
        """Build split candidate with inner return bundle.

        Args:
            candidate: Mapping of candidate.
            inner_return_plan: Mapping of inner return plan.
            oneway_map: Mapping of oneway.
            return_map: Mapping of return.
            config: Search configuration for the operation.
            distance_basis_km: Distance basis in kilometers used for scoring.
            max_connection_layover_seconds: Duration in seconds for max connection layover.
            destination_name: Destination name.
            notes: Mapping of notes.
            oneway_entry_cache: Cache of oneway entry.
            comparison_links: Mapping of comparison links.

        Returns:
            dict[str, Any] | None: Split candidate with inner return bundle.
        """
        inner_return = return_map.get(inner_return_plan["return_key"])
        if not inner_return:
            return None

        outer_outbound_entries = self._materialize_oneway_plan_entries(
            inner_return_plan["outer_outbound_plan"],
            oneway_map,
            oneway_entry_cache,
        )
        outer_inbound_entries = self._materialize_oneway_plan_entries(
            inner_return_plan["outer_inbound_plan"],
            oneway_map,
            oneway_entry_cache,
        )
        if outer_outbound_entries is None or outer_inbound_entries is None:
            return None

        inner_outbound_segments = inner_return.get("outbound_segments") or []
        inner_inbound_segments = inner_return.get("inbound_segments") or []
        if not inner_outbound_segments or not inner_inbound_segments:
            return None

        if max_connection_layover_seconds is not None:
            if any(
                self._exceeds_connection_layover_limit(
                    entry.get("segments") or [],
                    max_connection_layover_seconds,
                )
                for entry in [*outer_outbound_entries, *outer_inbound_entries]
            ):
                return None
            if self._exceeds_connection_layover_limit(
                inner_outbound_segments,
                max_connection_layover_seconds,
            ) or self._exceeds_connection_layover_limit(
                inner_inbound_segments,
                max_connection_layover_seconds,
            ):
                return None

        outbound_ok, outbound_outer_boundary_events = self._validate_entry_boundaries(
            outer_outbound_entries,
            list(inner_return_plan["outer_outbound_boundary_days"]),
            max_connection_layover_seconds,
        )
        inbound_ok, inbound_outer_boundary_events = self._validate_entry_boundaries(
            outer_inbound_entries,
            list(inner_return_plan["outer_inbound_boundary_days"]),
            max_connection_layover_seconds,
        )
        if not outbound_ok or not inbound_ok:
            return None

        bundle_out_source, bundle_out_destination = leg_endpoints_from_segments(
            inner_outbound_segments,
            inner_return_plan["gateway_code"],
            candidate["destination"],
        )
        bundle_in_source, bundle_in_destination = leg_endpoints_from_segments(
            inner_inbound_segments,
            candidate["destination"],
            inner_return_plan["gateway_code"],
        )
        if not self._leg_matches_expected_route(
            bundle_out_source,
            bundle_out_destination,
            inner_return_plan["gateway_code"],
            candidate["destination"],
        ):
            return None
        if not self._leg_matches_expected_route(
            bundle_in_source,
            bundle_in_destination,
            candidate["destination"],
            inner_return_plan["expected_inbound_gateway_code"],
        ):
            return None

        outbound_bundle_boundary_events = 0
        if outer_outbound_entries:
            previous_entry = outer_outbound_entries[-1]
            previous_segments = previous_entry.get("segments") or []
            if not previous_segments:
                return None
            outbound_gap = connection_gap_seconds(
                previous_segments[-1].get("arrive_local"),
                inner_outbound_segments[0].get("depart_local"),
            )
            min_outbound_boundary = minimum_split_boundary_connection_seconds(
                previous_entry.get("destination") or "",
                bundle_out_source,
            )
            if outbound_gap is None or outbound_gap < min_outbound_boundary:
                return None
            if (
                max_connection_layover_seconds is not None
                and int(inner_return_plan["outbound_bundle_boundary_days"] or 0) <= 0
                and outbound_gap > max_connection_layover_seconds
            ):
                return None
            outbound_bundle_boundary_events = boundary_transfer_events(
                previous_entry.get("destination") or "",
                bundle_out_source,
            )

        inbound_bundle_boundary_events = 0
        if outer_inbound_entries:
            next_entry = outer_inbound_entries[0]
            next_segments = next_entry.get("segments") or []
            if not next_segments:
                return None
            inbound_gap = connection_gap_seconds(
                inner_inbound_segments[-1].get("arrive_local"),
                next_segments[0].get("depart_local"),
            )
            min_inbound_boundary = minimum_split_boundary_connection_seconds(
                bundle_in_destination,
                next_entry.get("source") or "",
            )
            if inbound_gap is None or inbound_gap < min_inbound_boundary:
                return None
            if (
                max_connection_layover_seconds is not None
                and int(inner_return_plan["inbound_bundle_boundary_days"] or 0) <= 0
                and inbound_gap > max_connection_layover_seconds
            ):
                return None
            inbound_bundle_boundary_events = boundary_transfer_events(
                bundle_in_destination,
                next_entry.get("source") or "",
            )

        outbound_layovers = (
            sum(
                int(entry.get("fare", {}).get("transfer_events", entry.get("stops", 0)) or 0)
                for entry in outer_outbound_entries
            )
            + int(
                inner_return.get(
                    "outbound_transfer_events",
                    inner_return.get("outbound_stops", 0),
                )
                or 0
            )
            + outbound_outer_boundary_events
            + outbound_bundle_boundary_events
        )
        inbound_layovers = (
            int(
                inner_return.get(
                    "inbound_transfer_events",
                    inner_return.get("inbound_stops", 0),
                )
                or 0
            )
            + sum(
                int(entry.get("fare", {}).get("transfer_events", entry.get("stops", 0)) or 0)
                for entry in outer_inbound_entries
            )
            + inbound_outer_boundary_events
            + inbound_bundle_boundary_events
        )
        if (
            outbound_layovers > config.max_layovers_per_direction
            or inbound_layovers > config.max_layovers_per_direction
        ):
            return None

        total_price = int(inner_return.get("price") or 0) + sum(
            int((entry.get("fare") or {}).get("price") or 0)
            for entry in [*outer_outbound_entries, *outer_inbound_entries]
        )
        score = self._score_candidate(
            total_price,
            distance_basis_km,
            config.objective,
        )
        price_per_1000_km = (
            round((total_price / distance_basis_km) * 1000.0, 1)
            if distance_basis_km and distance_basis_km > 0
            else None
        )

        outbound_time_to_destination_seconds = None
        inbound_time_to_origin_seconds = None
        outer_outbound_durations = [
            entry.get("duration_seconds") for entry in outer_outbound_entries
        ]
        outer_inbound_durations = [entry.get("duration_seconds") for entry in outer_inbound_entries]
        inner_outbound_duration = self._as_int(inner_return.get("outbound_duration_seconds"))
        inner_inbound_duration = self._as_int(inner_return.get("inbound_duration_seconds"))
        if (
            all(value is not None for value in outer_outbound_durations)
            and inner_outbound_duration is not None
        ):
            outbound_time_to_destination_seconds = int(
                sum(int(value or 0) for value in outer_outbound_durations)
                + int(inner_outbound_duration)
                + (
                    int(inner_return_plan["outbound_bundle_boundary_days"] or 0)
                    + sum(
                        int(value or 0)
                        for value in inner_return_plan["outer_outbound_boundary_days"]
                    )
                )
                * SECONDS_PER_DAY
            )
        if (
            all(value is not None for value in outer_inbound_durations)
            and inner_inbound_duration is not None
        ):
            inbound_time_to_origin_seconds = int(
                int(inner_inbound_duration)
                + sum(int(value or 0) for value in outer_inbound_durations)
                + (
                    int(inner_return_plan["inbound_bundle_boundary_days"] or 0)
                    + sum(
                        int(value or 0)
                        for value in inner_return_plan["outer_inbound_boundary_days"]
                    )
                )
                * SECONDS_PER_DAY
            )

        price_modes = sorted(
            {
                str(mode).strip()
                for mode in (
                    [inner_return.get("price_mode")]
                    + [
                        (entry.get("fare") or {}).get("price_mode")
                        for entry in [*outer_outbound_entries, *outer_inbound_entries]
                    ]
                )
                if str(mode or "").strip()
            }
        )
        fare_mode = "selected_bags"
        if str(inner_return.get("fare_mode") or "") == "base_no_bags" or any(
            str((entry.get("fare") or {}).get("fare_mode") or "") == "base_no_bags"
            for entry in [*outer_outbound_entries, *outer_inbound_entries]
        ):
            fare_mode = "base_no_bags"

        legs = [
            *[
                self._oneway_entry_to_leg(
                    entry,
                    fallback_source=str(raw_source),
                    fallback_destination=str(raw_destination),
                    fallback_date=str(raw_date),
                    max_stops_per_leg=config.max_stops_per_leg,
                )
                for entry, (raw_source, raw_destination, raw_date) in zip(
                    outer_outbound_entries,
                    inner_return_plan["outer_outbound_plan"],
                    strict=False,
                )
            ],
            self._return_fare_to_ticket_leg(
                inner_return,
                source=inner_return_plan["gateway_code"],
                destination=str(candidate["destination"]),
                outbound_iso=str(candidate["depart_destination_date"]),
                inbound_iso=str(candidate["leave_destination_date"]),
                max_stops_per_leg=config.max_stops_per_leg,
            ),
            *[
                self._oneway_entry_to_leg(
                    entry,
                    fallback_source=str(raw_source),
                    fallback_destination=str(raw_destination),
                    fallback_date=str(raw_date),
                    max_stops_per_leg=config.max_stops_per_leg,
                )
                for entry, (raw_source, raw_destination, raw_date) in zip(
                    outer_inbound_entries,
                    inner_return_plan["outer_inbound_plan"],
                    strict=False,
                )
            ],
        ]

        primary_provider = next(
            (
                str((entry.get("fare") or {}).get("provider") or "")
                for entry in [*outer_outbound_entries, *outer_inbound_entries]
                if str((entry.get("fare") or {}).get("provider") or "").strip()
            ),
            str(inner_return.get("provider") or "kiwi"),
        )

        return {
            "result_id": (
                f"{candidate['destination']}|splitbundle|{candidate.get('origin')}|"
                f"{candidate.get('arrival_origin')}|{candidate.get('depart_origin_date')}|"
                f"{candidate.get('return_origin_date')}|{candidate.get('outbound_hub')}|"
                f"{candidate.get('inbound_hub')}|{inner_return_plan['gateway_code']}"
            ),
            "itinerary_type": "split_stopover",
            "pricing_strategy": "inner_return_bundle",
            "pricing_strategy_note": (
                "Middle gateway-to-destination segment priced as a bundled round-trip "
                f"({inner_return_plan['gateway_code']} <-> {candidate['destination']})."
            ),
            "destination_code": candidate["destination"],
            "destination_name": destination_name,
            "destination_note": notes.get("note"),
            "total_price": total_price,
            "passengers_adults": int(config.passengers.adults),
            "price_per_adult": round(total_price / max(1, int(config.passengers.adults)), 2),
            "price_modes": price_modes,
            "currency": config.currency,
            "price_per_1000_km": price_per_1000_km,
            "distance_km": round(distance_basis_km, 1) if distance_basis_km else None,
            "distance_basis": "direct_origin_to_destination",
            "score": score,
            "outbound_time_to_destination_seconds": outbound_time_to_destination_seconds,
            "inbound_time_to_origin_seconds": inbound_time_to_origin_seconds,
            "objective": config.objective,
            "provider": primary_provider,
            "outbound": {
                "origin": candidate["origin"],
                "hub": candidate.get("outbound_hub") or inner_return_plan["gateway_code"],
                "date_from_origin": candidate["depart_origin_date"],
                "date_to_destination": candidate["depart_destination_date"],
                "stopover_days": int(candidate.get("outbound_stopover_days") or 0),
                "layovers_count": outbound_layovers,
                "fare_mode": fare_mode,
                "provider": primary_provider,
            },
            "fare_mode": fare_mode,
            "main_destination_stay_days": candidate["main_stay_days"],
            "inbound": {
                "hub": candidate.get("inbound_hub") or bundle_in_destination,
                "arrival_origin": candidate["arrival_origin"],
                "date_from_destination": candidate["leave_destination_date"],
                "date_to_origin": candidate["return_origin_date"],
                "stopover_days": int(candidate.get("inbound_stopover_days") or 0),
                "layovers_count": inbound_layovers,
                "fare_mode": fare_mode,
                "provider": primary_provider,
            },
            "comparison_links": comparison_links
            or build_comparison_links(
                candidate["origin"],
                candidate["destination"],
                candidate["depart_origin_date"],
                candidate["leave_destination_date"],
                adults=config.passengers.adults,
                max_stops_per_leg=config.max_stops_per_leg,
                currency=config.currency,
            ),
            "legs": legs,
            "risk_notes": [
                "Middle gateway-to-destination segment priced as a bundled round-trip fare.",
                "Split tickets: self-transfer and missed-connection risk applies.",
                "Baggage and fare rules can differ per ticket and airline.",
            ],
        }

    @staticmethod
    def _strategy_anchor_key(item: dict[str, Any]) -> str | None:
        """Handle strategy anchor key.

        Args:
            item: Item for the current operation.

        Returns:
            str | None: Handle strategy anchor key.
        """
        outbound = item.get("outbound") or {}
        inbound = item.get("inbound") or {}
        if item.get("itinerary_type") == "split_stopover":
            outbound_stopover_days = int(outbound.get("stopover_days") or 0)
            inbound_stopover_days = int(inbound.get("stopover_days") or 0)
            if outbound_stopover_days > 0 or inbound_stopover_days > 0:
                return "has_long_stopover"

        outbound_layovers = int(outbound.get("layovers_count") or 0)

        if outbound_layovers <= 0:
            return "outbound_layovers_0"
        if outbound_layovers == 1:
            return "outbound_layovers_1"
        return "outbound_layovers_2plus"

    def _merge_strategy_anchors(
        self,
        ranked: list[dict[str, Any]],
        top_results: int,
    ) -> list[dict[str, Any]]:
        """Handle merge strategy anchors.

        Args:
            ranked: Mapping of ranked.
            top_results: Ranked results to keep for the current operation.

        Returns:
            list[dict[str, Any]]: Handle merge strategy anchors.
        """
        if top_results <= 0 or not ranked:
            return []

        anchors_needed = {
            "outbound_layovers_0",
            "outbound_layovers_1",
            "outbound_layovers_2plus",
            "has_long_stopover",
        }
        anchors: dict[str, dict[str, Any]] = {}
        extra_direct: list[dict[str, Any]] = []
        seen_direct_hubs: set[str] = set()
        for item in ranked:
            key = self._strategy_anchor_key(item)
            if not key or key not in anchors_needed or key in anchors:
                pass
            else:
                anchors[key] = item

            if item.get("itinerary_type") == "direct_roundtrip":
                outbound_hub = str((item.get("outbound") or {}).get("hub") or "DIRECT")
                inbound_hub = str((item.get("inbound") or {}).get("hub") or "DIRECT")
                direct_key = f"{outbound_hub}|{inbound_hub}"
                if direct_key not in seen_direct_hubs:
                    seen_direct_hubs.add(direct_key)
                    extra_direct.append(item)
                    if len(extra_direct) >= 3 and len(anchors) == len(anchors_needed):
                        break

        selected = ranked[:top_results]

        cheapest_item: dict[str, Any] | None = None
        cheapest_price = float("inf")
        fastest_item: dict[str, Any] | None = None
        fastest_time = float("inf")
        for item in ranked:
            try:
                price = float(item.get("total_price"))
            except (TypeError, ValueError):
                price = float("inf")
            if price < cheapest_price:
                cheapest_price = price
                cheapest_item = item

            outbound_time = item.get("outbound_time_to_destination_seconds")
            if outbound_time is None:
                continue
            try:
                outbound_time_value = float(outbound_time)
            except (TypeError, ValueError):
                continue
            if outbound_time_value < fastest_time:
                fastest_time = outbound_time_value
                fastest_item = item

        destination_anchors: dict[str, dict[str, Any]] = {}
        for item in ranked:
            destination_code = str(item.get("destination_code") or "").upper()
            if not destination_code or destination_code in destination_anchors:
                continue
            destination_anchors[destination_code] = item

        if (
            not anchors
            and not extra_direct
            and not destination_anchors
            and not cheapest_item
            and not fastest_item
        ):
            return selected

        rank_pos = {item.get("result_id"): idx for idx, item in enumerate(ranked)}
        selected_ids = {item.get("result_id") for item in selected}
        anchor_items_in_order: list[dict[str, Any]] = []
        destination_anchor_items = list(destination_anchors.values())
        if cheapest_item is not None:
            destination_anchor_items.append(cheapest_item)
        if fastest_item is not None:
            destination_anchor_items.append(fastest_item)
        anchor_items_in_order.extend(destination_anchor_items)
        for key in [
            "outbound_layovers_0",
            "outbound_layovers_1",
            "outbound_layovers_2plus",
            "has_long_stopover",
        ]:
            anchor = anchors.get(key)
            if not anchor:
                continue
            anchor_items_in_order.append(anchor)
        anchor_items_in_order.extend(extra_direct)

        destination_required: list[dict[str, Any]] = []
        destination_required_ids: set[Any] = set()
        for anchor in destination_anchor_items:
            anchor_id = anchor.get("result_id")
            if anchor_id in destination_required_ids:
                continue
            destination_required.append(anchor)
            destination_required_ids.add(anchor_id)

        selected = list(destination_required[:top_results])
        selected_ids = {item.get("result_id") for item in selected}

        if len(selected) < top_results:
            for anchor in anchor_items_in_order:
                anchor_id = anchor.get("result_id")
                if anchor_id in selected_ids:
                    continue
                selected.append(anchor)
                selected_ids.add(anchor_id)
                if len(selected) >= top_results:
                    break

        for item in ranked:
            item_id = item.get("result_id")
            if item_id in selected_ids:
                continue
            if len(selected) >= top_results:
                break
            selected.append(item)
            selected_ids.add(item_id)

        selected.sort(key=lambda item: rank_pos.get(item.get("result_id"), 10**9))
        return selected[:top_results]

    def _cap_results_per_destination(
        self,
        ranked: list[dict[str, Any]],
        top_results_per_destination: int,
        destination_order: list[str] | tuple[str, ...] | None = None,
        required_by_destination: dict[str, list[dict[str, Any]]] | None = None,
    ) -> tuple[list[dict[str, Any]], dict[str, int]]:
        """Handle cap results per destination.

        Args:
            ranked: Mapping of ranked.
            top_results_per_destination: Maximum number of ranked results to keep per destination.
            destination_order: Collection of destination order.
            required_by_destination: Mapping of required by destination.

        Returns:
            tuple[list[dict[str, Any]], dict[str, int]]: Handle cap results per destination.
        """
        if top_results_per_destination <= 0 or not ranked:
            return [], {}

        grouped: dict[str, list[dict[str, Any]]] = {}
        for item in ranked:
            code = str(item.get("destination_code") or "").upper()
            if not code:
                continue
            grouped.setdefault(code, []).append(item)
        if not grouped:
            return [], {}

        order: list[str] = []
        if destination_order:
            for code in destination_order:
                normalized = str(code or "").upper()
                if normalized in grouped and normalized not in order:
                    order.append(normalized)
        for code in grouped.keys():
            if code not in order:
                order.append(code)

        capped: list[dict[str, Any]] = []
        per_destination_counts: dict[str, int] = {}
        for code in order:
            bucket = grouped.get(code) or []
            if not bucket:
                continue
            selected = self._merge_strategy_anchors(bucket, top_results_per_destination)
            required = list((required_by_destination or {}).get(code) or [])
            if required:
                required_prefixed: list[dict[str, Any]] = []
                required_set: set[str] = set()
                for item in required:
                    item_id = str(item.get("result_id") or "")
                    if not item_id or item_id in required_set:
                        continue
                    required_prefixed.append(item)
                    required_set.add(item_id)
                merged = required_prefixed + selected
                deduped: list[dict[str, Any]] = []
                seen_ids: set[str] = set()
                for item in merged:
                    item_id = str(item.get("result_id") or "")
                    if not item_id or item_id in seen_ids:
                        continue
                    deduped.append(item)
                    seen_ids.add(item_id)
                selected = deduped[:top_results_per_destination]
            per_destination_counts[code] = len(selected)
            capped.extend(selected)
        return capped, per_destination_counts

    @staticmethod
    def _split_candidate_key(candidate: dict[str, Any]) -> tuple[Any, ...]:
        """Handle split candidate key.

        Args:
            candidate: Mapping of candidate.

        Returns:
            tuple[Any, ...]: Handle split candidate key.
        """
        candidate_type = str(candidate.get("candidate_type") or "split_stopover")
        base = (
            candidate_type,
            str(candidate.get("origin") or ""),
            str(candidate.get("arrival_origin") or ""),
            str(candidate.get("outbound_hub") or ""),
            str(candidate.get("inbound_hub") or ""),
            str(candidate.get("depart_origin_date") or ""),
            str(candidate.get("depart_destination_date") or ""),
            str(candidate.get("leave_destination_date") or ""),
            str(candidate.get("return_origin_date") or ""),
        )
        if candidate_type != "split_chain":
            return base

        outbound_plan = tuple(
            (
                str(leg.get("source") or ""),
                str(leg.get("destination") or ""),
                str(leg.get("date") or "")[:10],
            )
            for leg in (candidate.get("outbound_legs") or [])
        )
        inbound_plan = tuple(
            (
                str(leg.get("source") or ""),
                str(leg.get("destination") or ""),
                str(leg.get("date") or "")[:10],
            )
            for leg in (candidate.get("inbound_legs") or [])
        )
        return (
            *base,
            tuple(
                int(value or 0)
                for value in (candidate.get("outbound_boundary_stopover_days") or [])
            ),
            tuple(
                int(value or 0) for value in (candidate.get("inbound_boundary_stopover_days") or [])
            ),
            outbound_plan,
            inbound_plan,
        )

    def _select_split_candidates_with_diversity(
        self,
        split_candidates: list[dict[str, Any]],
        base_quota: int,
        extra_quota: int,
    ) -> list[dict[str, Any]]:
        """Select split candidates with diversity.

        Args:
            split_candidates: Mapping of split candidates.
            base_quota: Base validation quota per destination.
            extra_quota: Additional validation quota to distribute.

        Returns:
            list[dict[str, Any]]: Selected split candidates with diversity.
        """
        if not split_candidates or base_quota <= 0:
            return []

        selected: list[dict[str, Any]] = []
        seen_keys: set[tuple[Any, ...]] = set()
        seen_outbound_hubs: set[str] = set()
        seen_inbound_hubs: set[str] = set()
        seen_hub_pairs: set[tuple[str, str]] = set()

        target = min(
            len(split_candidates),
            max(base_quota, base_quota + max(0, extra_quota)),
        )

        def register(item: dict[str, Any]) -> None:
            selected.append(item)
            seen_keys.add(self._split_candidate_key(item))
            outbound_hub = str(item.get("outbound_hub") or "")
            inbound_hub = str(item.get("inbound_hub") or "")
            if outbound_hub:
                seen_outbound_hubs.add(outbound_hub)
            if inbound_hub:
                seen_inbound_hubs.add(inbound_hub)
            if outbound_hub or inbound_hub:
                seen_hub_pairs.add((outbound_hub, inbound_hub))

        for item in split_candidates:
            if len(selected) >= min(base_quota, target):
                break
            register(item)

        if len(selected) >= target:
            return selected[:target]

        for item in split_candidates:
            if len(selected) >= target:
                break
            key = self._split_candidate_key(item)
            if key in seen_keys:
                continue
            outbound_hub = str(item.get("outbound_hub") or "")
            if outbound_hub and outbound_hub not in seen_outbound_hubs:
                register(item)

        for item in split_candidates:
            if len(selected) >= target:
                break
            key = self._split_candidate_key(item)
            if key in seen_keys:
                continue
            inbound_hub = str(item.get("inbound_hub") or "")
            if inbound_hub and inbound_hub not in seen_inbound_hubs:
                register(item)

        for item in split_candidates:
            if len(selected) >= target:
                break
            key = self._split_candidate_key(item)
            if key in seen_keys:
                continue
            outbound_hub = str(item.get("outbound_hub") or "")
            inbound_hub = str(item.get("inbound_hub") or "")
            pair = (outbound_hub, inbound_hub)
            if pair not in seen_hub_pairs:
                register(item)

        for item in split_candidates:
            if len(selected) >= target:
                break
            key = self._split_candidate_key(item)
            if key in seen_keys:
                continue
            register(item)

        return selected[:target]

    def _prepare_destination_validation_context(
        self,
        *,
        destination: str,
        estimated_candidates: list[dict[str, Any]],
        config: SearchConfig,
        validation_target_per_destination: int,
        origin_rank: dict[str, int],
        core_provider_ids: tuple[str, ...],
        serpapi_active: bool,
        audit_destinations: set[str] | None = None,
        audit_metadata: dict[str, dict[str, Any]] | None = None,
    ) -> tuple[dict[str, Any], list[str]]:
        """Handle prepare destination validation context.

        Args:
            destination: Destination airport code for the request.
            estimated_candidates: Mapping of estimated candidates.
            config: Search configuration for the operation.
            validation_target_per_destination: Validation budget allocated to each destination.
            origin_rank: Mapping of origin rank.
            core_provider_ids: Identifiers for core provider.
            serpapi_active: Flag that controls whether serpapi active is used.
            audit_destinations: Destinations receiving an expanded audit pass.
            audit_metadata: Coverage-audit metadata keyed by destination.

        Returns:
            tuple[dict[str, Any], list[str]]: Handle prepare destination validation context.
        """
        warnings: list[str] = []
        destination_name = self._destination_display_name(destination)
        notes = DESTINATION_NOTES.get(destination, {})
        split_chain_estimated_count = sum(
            1
            for candidate in estimated_candidates
            if str(candidate.get("candidate_type") or "") == "split_chain"
        )
        if split_chain_estimated_count > 0:
            warnings.append(
                f"{destination}: generated {split_chain_estimated_count} destination-first split-chain candidates."
            )

        destination_validation_target = validation_target_per_destination
        if config.exhaustive_hub_scan:
            destination_validation_target = max(
                destination_validation_target,
                min(
                    len(estimated_candidates),
                    validation_target_per_destination * 2,
                ),
            )
        destination_audit_meta = dict((audit_metadata or {}).get(destination) or {})
        if destination in (audit_destinations or set()):
            destination_validation_target = min(
                len(estimated_candidates),
                max(
                    destination_validation_target,
                    validation_target_per_destination * COVERAGE_AUDIT_VALIDATION_MULTIPLIER,
                ),
            )
            warnings.append(
                f"{destination}: coverage audit expanded validation to {destination_validation_target} candidates."
            )

        def candidate_sort_key(item: dict[str, Any]) -> tuple[float, int, int]:
            best_value_score = item.get("estimated_best_value_score")
            estimated_outbound_time = item.get("estimated_outbound_time_to_destination_seconds")
            return (
                float(
                    best_value_score
                    if config.objective == "best" and best_value_score is not None
                    else item["estimated_score"]
                ),
                int(item["estimated_total"]),
                int(estimated_outbound_time)
                if estimated_outbound_time is not None
                else PRICE_SENTINEL,
            )

        split_candidates = sorted(
            (
                candidate
                for candidate in estimated_candidates
                if candidate.get("candidate_type") != "direct_roundtrip"
            ),
            key=candidate_sort_key,
        )
        direct_candidates = sorted(
            (
                candidate
                for candidate in estimated_candidates
                if candidate.get("candidate_type") == "direct_roundtrip"
            ),
            key=candidate_sort_key,
        )

        if not direct_candidates:
            split_base_quota = min(
                len(split_candidates),
                destination_validation_target,
            )
            split_extra_quota = min(
                len(split_candidates),
                max(
                    destination_validation_target,
                    destination_validation_target * (2 if config.exhaustive_hub_scan else 1),
                ),
            )
            limited_candidates = self._select_split_candidates_with_diversity(
                split_candidates,
                split_base_quota,
                split_extra_quota,
            )
        elif not split_candidates:
            direct_quota = min(
                len(direct_candidates),
                max(
                    destination_validation_target,
                    480,
                ),
            )
            if config.exhaustive_hub_scan:
                direct_quota = min(
                    len(direct_candidates),
                    max(
                        direct_quota,
                        destination_validation_target * 2,
                        config.validate_top_per_destination * 12,
                    ),
                )
            limited_candidates = direct_candidates[:direct_quota]
        else:
            split_base_quota = min(
                len(split_candidates),
                destination_validation_target,
            )
            split_extra_quota = min(
                len(split_candidates),
                max(
                    destination_validation_target,
                    destination_validation_target * (2 if config.exhaustive_hub_scan else 1),
                ),
            )
            diversified_split_candidates = self._select_split_candidates_with_diversity(
                split_candidates,
                split_base_quota,
                split_extra_quota,
            )
            direct_quota = min(
                len(direct_candidates),
                max(
                    destination_validation_target,
                    480,
                ),
            )
            if config.exhaustive_hub_scan:
                direct_quota = min(
                    len(direct_candidates),
                    max(
                        direct_quota,
                        destination_validation_target * 2,
                        config.validate_top_per_destination * 12,
                    ),
                )
            limited_candidates = diversified_split_candidates + direct_candidates[:direct_quota]
            limited_candidates.sort(key=candidate_sort_key)

        split_chain_candidates = [
            candidate
            for candidate in split_candidates
            if str(candidate.get("candidate_type") or "") == "split_chain"
        ]
        if split_chain_candidates:
            chain_probe_quota = min(
                len(split_chain_candidates),
                max(6, min(48, destination_validation_target // 2)),
            )
            limited_ids = {self._split_candidate_key(item) for item in limited_candidates}
            for chain_candidate in split_chain_candidates[:chain_probe_quota]:
                chain_key = self._split_candidate_key(chain_candidate)
                if chain_key in limited_ids:
                    continue
                limited_candidates.append(chain_candidate)
                limited_ids.add(chain_key)
            limited_candidates.sort(key=candidate_sort_key)

        def candidate_identity(item: dict[str, Any]) -> tuple[Any, ...]:
            candidate_type = str(item.get("candidate_type") or "split_stopover")
            if candidate_type == "direct_roundtrip":
                return (
                    candidate_type,
                    str(item.get("origin") or ""),
                    str(item.get("destination") or destination),
                    str(item.get("depart_origin_date") or ""),
                    str(item.get("return_origin_date") or ""),
                )
            return self._split_candidate_key(item)

        if config.objective != "cheapest" and estimated_candidates:
            price_floor_quota = min(
                len(estimated_candidates),
                max(2, min(8, int(math.ceil(destination_validation_target / 4)))),
            )
            price_floor_candidates = sorted(
                estimated_candidates,
                key=lambda item: (
                    int(item.get("estimated_total") or PRICE_SENTINEL),
                    candidate_sort_key(item),
                ),
            )[:price_floor_quota]
            selected_ids = {candidate_identity(item) for item in limited_candidates}
            preserved_price_floor = 0
            for price_floor_candidate in price_floor_candidates:
                candidate_id = candidate_identity(price_floor_candidate)
                if candidate_id in selected_ids:
                    continue
                limited_candidates.append(price_floor_candidate)
                selected_ids.add(candidate_id)
                preserved_price_floor += 1
            if preserved_price_floor > 0:
                limited_candidates.sort(key=candidate_sort_key)
                warnings.append(
                    f"{destination}: preserved {preserved_price_floor} price-floor candidate(s) for validation."
                )

        unique_leg_keys: set[tuple[str, str, str]] = set()
        unique_return_keys: set[tuple[str, str, str, str]] = set()
        leg_rank_score: dict[tuple[str, str, str], int] = {}
        return_rank_score: dict[tuple[str, str, str, str], int] = {}

        for candidate in limited_candidates:
            candidate_type = str(candidate.get("candidate_type") or "split_stopover")
            candidate["_candidate_type"] = candidate_type
            estimated_total = int(candidate.get("estimated_total") or PRICE_SENTINEL)
            if candidate_type == "direct_roundtrip":
                direct_return_key = (
                    candidate["origin"],
                    candidate["destination"],
                    candidate["depart_origin_date"],
                    candidate["return_origin_date"],
                )
                candidate["_direct_return_key"] = direct_return_key
                unique_return_keys.add(direct_return_key)
                prev_return_rank = return_rank_score.get(direct_return_key)
                if prev_return_rank is None or estimated_total < prev_return_rank:
                    return_rank_score[direct_return_key] = estimated_total
                continue

            split_plans = self._candidate_split_plans(candidate)
            if split_plans is not None:
                candidate["_split_plans"] = split_plans
            else:
                candidate.pop("_split_plans", None)

            inner_return_plan = self._candidate_inner_return_plan(candidate)
            if inner_return_plan is not None:
                candidate["_inner_return_plan"] = inner_return_plan
                inner_return_key = inner_return_plan["return_key"]
                unique_return_keys.add(inner_return_key)
                prev_inner_return_rank = return_rank_score.get(inner_return_key)
                if prev_inner_return_rank is None or estimated_total < prev_inner_return_rank:
                    return_rank_score[inner_return_key] = estimated_total
            else:
                candidate.pop("_inner_return_plan", None)

            leg_keys_for_candidate: tuple[tuple[str, str, str], ...] = ()
            if split_plans is not None:
                leg_keys_for_candidate = tuple(
                    [*split_plans["outbound_plan"], *split_plans["inbound_plan"]]
                )
            candidate["_leg_keys"] = leg_keys_for_candidate
            for leg_key in leg_keys_for_candidate:
                unique_leg_keys.add(leg_key)
                prev_leg_rank = leg_rank_score.get(leg_key)
                if prev_leg_rank is None or estimated_total < prev_leg_rank:
                    leg_rank_score[leg_key] = estimated_total

        def leg_sort_key(key: tuple[str, str, str]) -> tuple[int, int, tuple[str, str, str]]:
            return (
                leg_rank_score.get(key, PRICE_SENTINEL),
                origin_rank.get(key[0], len(origin_rank)),
                key,
            )

        def return_sort_key(
            key: tuple[str, str, str, str],
        ) -> tuple[int, int, tuple[str, str, str, str]]:
            return (
                return_rank_score.get(key, PRICE_SENTINEL),
                origin_rank.get(key[0], len(origin_rank)),
                key,
            )

        ordered_oneway_keys = sorted(unique_leg_keys, key=leg_sort_key)
        if (
            config.max_validate_oneway_keys_per_destination is not None
            and len(ordered_oneway_keys) > config.max_validate_oneway_keys_per_destination
        ):
            dropped = len(ordered_oneway_keys) - config.max_validate_oneway_keys_per_destination
            ordered_oneway_keys = ordered_oneway_keys[
                : config.max_validate_oneway_keys_per_destination
            ]
            warnings.append(
                f"{destination}: capped one-way key validations by budget ({dropped} dropped)."
            )

        ordered_return_keys = sorted(unique_return_keys, key=return_sort_key)
        if (
            config.max_validate_return_keys_per_destination is not None
            and len(ordered_return_keys) > config.max_validate_return_keys_per_destination
        ):
            dropped = len(ordered_return_keys) - config.max_validate_return_keys_per_destination
            ordered_return_keys = ordered_return_keys[
                : config.max_validate_return_keys_per_destination
            ]
            warnings.append(
                f"{destination}: capped return key validations by budget ({dropped} dropped)."
            )

        oneway_provider_map: dict[tuple[str, str, str], tuple[str, ...]] = {}
        return_provider_map: dict[tuple[str, str, str, str], tuple[str, ...]] = {}
        if serpapi_active and "serpapi" not in core_provider_ids and core_provider_ids:
            serpapi_probe_oneway_keys = max(0, config.serpapi_probe_oneway_keys)
            serpapi_probe_return_keys = max(0, config.serpapi_probe_return_keys)
            if serpapi_probe_oneway_keys == 0 and serpapi_probe_return_keys == 0:
                serpapi_probe_oneway_keys = min(4, len(ordered_oneway_keys))
                serpapi_probe_return_keys = min(1, len(ordered_return_keys))
                if serpapi_probe_oneway_keys > 0 or serpapi_probe_return_keys > 0:
                    warnings.append(
                        f"{destination}: SerpApi probes were 0; auto-enabled lightweight probes "
                        f"({serpapi_probe_oneway_keys} one-way, {serpapi_probe_return_keys} return)."
                    )
            provider_with_serpapi = tuple(dict.fromkeys((*core_provider_ids, "serpapi")))
            probe_leg_count = min(
                len(ordered_oneway_keys),
                serpapi_probe_oneway_keys,
            )
            probe_return_count = min(
                len(ordered_return_keys),
                serpapi_probe_return_keys,
            )
            top_leg_keys = set(ordered_oneway_keys[:probe_leg_count])
            top_return_keys = set(ordered_return_keys[:probe_return_count])
            for leg_key in ordered_oneway_keys:
                oneway_provider_map[leg_key] = (
                    provider_with_serpapi if leg_key in top_leg_keys else core_provider_ids
                )
            for return_key in ordered_return_keys:
                return_provider_map[return_key] = (
                    provider_with_serpapi if return_key in top_return_keys else core_provider_ids
                )
            warnings.append(
                f"{destination}: SerpApi probe scope {probe_leg_count} one-way keys and "
                f"{probe_return_count} return keys."
            )

        return (
            {
                "destination": destination,
                "destination_name": destination_name,
                "notes": notes,
                "estimated_candidates_count": len(estimated_candidates),
                "validation_target": destination_validation_target,
                "split_chain_estimated_count": split_chain_estimated_count,
                "limited_candidates": limited_candidates,
                "ordered_oneway_keys": ordered_oneway_keys,
                "ordered_return_keys": ordered_return_keys,
                "oneway_provider_map": oneway_provider_map,
                "return_provider_map": return_provider_map,
                "coverage_audit": destination_audit_meta,
            },
            warnings,
        )

    def _select_coverage_audit_destinations(
        self,
        estimated_by_destination: dict[str, list[dict[str, Any]]],
        *,
        objective: str,
    ) -> list[str]:
        """Pick the destinations that deserve an expanded coverage audit.

        Args:
            estimated_by_destination: Estimated candidates grouped by destination.
            objective: Ranking objective for the search.

        Returns:
            list[str]: Destination codes ordered by audit priority.
        """
        ranked: list[tuple[tuple[float, int, str], str]] = []
        for destination, items in estimated_by_destination.items():
            if not items:
                continue
            best_item = min(
                items,
                key=lambda item: (
                    float(
                        item.get("estimated_best_value_score")
                        if objective == "best"
                        and item.get("estimated_best_value_score") is not None
                        else item.get("estimated_score") or PRICE_SENTINEL
                    ),
                    int(item.get("estimated_total") or PRICE_SENTINEL),
                    str(item.get("destination") or destination),
                ),
            )
            ranked.append(
                (
                    (
                        float(
                            best_item.get("estimated_best_value_score")
                            if objective == "best"
                            and best_item.get("estimated_best_value_score") is not None
                            else best_item.get("estimated_score") or PRICE_SENTINEL
                        ),
                        int(best_item.get("estimated_total") or PRICE_SENTINEL),
                        destination,
                    ),
                    destination,
                )
            )
        ranked.sort(key=lambda entry: entry[0])
        return [destination for _, destination in ranked[:COVERAGE_AUDIT_DESTINATION_LIMIT]]

    @staticmethod
    def _sample_discovery_dates(
        date_keys: tuple[str, ...],
        *,
        max_dates: int,
    ) -> tuple[str, ...]:
        """Select evenly distributed date seeds for sparse provider discovery.

        Args:
            date_keys: Candidate date keys available for the task.
            max_dates: Maximum number of dates to retain.

        Returns:
            tuple[str, ...]: Sampled date seeds ordered chronologically.
        """
        if not date_keys:
            return ()
        if len(date_keys) <= max_dates:
            return tuple(date_keys)
        if max_dates <= 1:
            return (date_keys[0],)
        indexes = {
            round(index * (len(date_keys) - 1) / float(max_dates - 1)) for index in range(max_dates)
        }
        return tuple(date_keys[index] for index in sorted(indexes))

    def _build_initial_free_provider_discovery_seed_map(
        self,
        *,
        task: dict[str, Any],
    ) -> dict[tuple[str, str], tuple[str, ...]]:
        """Build sparse route/date seeds for baseline free-provider discovery.

        Args:
            task: Estimator task to augment before candidate scoring.

        Returns:
            dict[tuple[str, str], tuple[str, ...]]: Sparse route/date seeds.
        """
        sampled_dates = self._sample_discovery_dates(
            tuple(str(value) for value in task.get("date_keys") or ()),
            max_dates=FREE_PROVIDER_DISCOVERY_MAX_DATES_PER_ROUTE,
        )
        if not sampled_dates:
            return {}

        destination = str(task.get("destination") or "")
        route_scores: dict[tuple[str, str], int] = {}

        def register_route(
            route_key: tuple[str, str],
            series: tuple[int | None, ...] | None,
        ) -> None:
            if not route_key[0] or not route_key[1]:
                return
            min_price = _min_series_price(series)
            current = route_scores.get(route_key, PRICE_SENTINEL)
            route_scores[route_key] = min(current, int(min_price or PRICE_SENTINEL))

        for route_key, series in dict(task.get("origin_to_destination") or {}).items():
            register_route((str(route_key[0]), str(route_key[1])), tuple(series))
        for route_key, series in dict(task.get("destination_to_origin") or {}).items():
            register_route((str(route_key[0]), str(route_key[1])), tuple(series))
        for route_key, series in dict(task.get("origin_to_hub") or {}).items():
            register_route((str(route_key[0]), str(route_key[1])), tuple(series))
        for route_key, series in dict(task.get("hub_to_origin") or {}).items():
            register_route((str(route_key[0]), str(route_key[1])), tuple(series))
        for hub, series in dict(task.get("hub_to_destination") or {}).items():
            register_route((str(hub), destination), tuple(series))
        for hub, series in dict(task.get("destination_to_hub") or {}).items():
            register_route((destination, str(hub)), tuple(series))
        for route_key, series in dict(task.get("hub_to_hub") or {}).items():
            register_route((str(route_key[0]), str(route_key[1])), tuple(series))

        selected_routes = sorted(
            route_scores,
            key=lambda route_key: (
                route_scores.get(route_key, PRICE_SENTINEL),
                route_key[0],
                route_key[1],
            ),
        )[:FREE_PROVIDER_DISCOVERY_MAX_ROUTES_PER_DESTINATION]
        return {route_key: sampled_dates for route_key in selected_routes}

    def _build_free_provider_discovery_seed_map(
        self,
        *,
        destination: str,
        estimated_candidates: list[dict[str, Any]],
        config: SearchConfig,
    ) -> dict[tuple[str, str], tuple[str, ...]]:
        """Build route/date seeds for free-provider discovery probes.

        Args:
            destination: Destination airport code for the request.
            estimated_candidates: Estimated candidates for the destination.
            config: Search configuration for the operation.

        Returns:
            dict[tuple[str, str], tuple[str, ...]]: Route keys mapped to candidate date seeds.
        """
        route_dates: dict[tuple[str, str], set[str]] = {}
        route_rank: dict[tuple[str, str], int] = {}

        ranked_candidates = sorted(
            estimated_candidates,
            key=lambda item: (
                float(
                    item.get("estimated_best_value_score")
                    if config.objective == "best"
                    and item.get("estimated_best_value_score") is not None
                    else item.get("estimated_score") or PRICE_SENTINEL
                ),
                int(item.get("estimated_total") or PRICE_SENTINEL),
            ),
        )[:COVERAGE_AUDIT_TOP_CANDIDATES]

        for candidate in ranked_candidates:
            estimated_total = int(candidate.get("estimated_total") or PRICE_SENTINEL)
            candidate_type = str(candidate.get("candidate_type") or "split_stopover")
            seed_keys: list[tuple[str, str, str]] = []
            if candidate_type == "direct_roundtrip":
                seed_keys.extend(
                    [
                        (
                            str(candidate["origin"]),
                            destination,
                            str(candidate["depart_origin_date"]),
                        ),
                        (
                            destination,
                            str(candidate["arrival_origin"]),
                            str(candidate["return_origin_date"]),
                        ),
                    ]
                )
            else:
                split_plans = self._candidate_split_plans(candidate)
                if split_plans is not None:
                    seed_keys.extend([*split_plans["outbound_plan"], *split_plans["inbound_plan"]])
            for source, target, date_iso in seed_keys:
                route_key = (str(source), str(target))
                route_dates.setdefault(route_key, set()).add(str(date_iso))
                route_rank[route_key] = min(
                    route_rank.get(route_key, PRICE_SENTINEL), estimated_total
                )

        if not route_dates:
            return {}

        selected_routes = sorted(
            route_dates,
            key=lambda route_key: (
                route_rank.get(route_key, PRICE_SENTINEL),
                route_key[0],
                route_key[1],
            ),
        )[:COVERAGE_AUDIT_MAX_ROUTES]

        out: dict[tuple[str, str], tuple[str, ...]] = {}
        for route_key in selected_routes:
            expanded_dates: set[str] = set()
            for date_iso in route_dates[route_key]:
                base_date = dt.date.fromisoformat(str(date_iso))
                for delta in range(
                    -COVERAGE_AUDIT_DATE_RADIUS_DAYS,
                    COVERAGE_AUDIT_DATE_RADIUS_DAYS + 1,
                ):
                    candidate_date = base_date + dt.timedelta(days=delta)
                    if candidate_date < config.period_start or candidate_date > config.period_end:
                        continue
                    expanded_dates.add(candidate_date.isoformat())
            out[route_key] = tuple(sorted(expanded_dates)[:COVERAGE_AUDIT_MAX_DATES_PER_ROUTE])
        return out

    async def _probe_free_provider_discovery(
        self,
        *,
        search_client: MultiProviderClient,
        provider_ids: tuple[str, ...],
        route_dates: dict[tuple[str, str], tuple[str, ...]],
        config: SearchConfig,
        io_pool: ThreadPoolExecutor,
        io_cap: int | None = None,
    ) -> tuple[dict[tuple[str, str], dict[str, int]], list[str]]:
        """Probe free providers for sparse route/date discovery prices.

        Args:
            search_client: Client used to execute search requests.
            provider_ids: Free-provider identifiers used for discovery.
            route_dates: Route seeds mapped to outbound ISO dates.
            config: Search configuration for the operation.
            io_pool: Thread pool used for I/O-bound provider validation.
            io_cap: Maximum concurrency cap for the sparse discovery stage.

        Returns:
            tuple[dict[tuple[str, str], dict[str, int]], list[str]]: Sparse discovered prices and warnings.
        """
        if not provider_ids or not route_dates:
            return {}, []

        loop = asyncio.get_running_loop()
        sem = asyncio.Semaphore(
            max(
                1,
                min(
                    int(io_cap or COVERAGE_AUDIT_DISCOVERY_IO_CAP),
                    bounded_io_concurrency(config.io_workers),
                ),
            )
        )
        max_connection_layover_seconds = (
            int(config.max_connection_layover_hours * SECONDS_PER_HOUR)
            if config.max_connection_layover_hours
            else None
        )
        discovered: dict[tuple[str, str], dict[str, int]] = {}
        warnings: list[str] = []

        async def probe_date(source: str, target: str, date_iso: str) -> None:
            async with sem:
                try:
                    fn = partial(
                        search_client.get_best_oneway,
                        source=source,
                        destination=target,
                        departure_iso=date_iso,
                        currency=config.currency,
                        max_stops_per_leg=config.max_stops_per_leg,
                        adults=config.passengers.adults,
                        hand_bags=config.passengers.hand_bags,
                        hold_bags=config.passengers.hold_bags,
                        max_connection_layover_seconds=max_connection_layover_seconds,
                        provider_ids=provider_ids,
                    )
                    item = await loop.run_in_executor(io_pool, fn)
                except Exception as exc:
                    warnings.append(
                        f"Coverage audit discovery failed {source}->{target} {date_iso}: {exc}"
                    )
                    return
                if not item:
                    return
                price = int(item.get("price") or 0)
                if price <= 0:
                    return
                route_key = (source, target)
                route_prices = discovered.setdefault(route_key, {})
                current = route_prices.get(date_iso)
                if current is None or price < current:
                    route_prices[date_iso] = price

        await asyncio.gather(
            *(
                probe_date(source, target, date_iso)
                for (source, target), dates in route_dates.items()
                for date_iso in dates
            )
        )
        return discovered, warnings

    async def _run_initial_free_provider_discovery(
        self,
        *,
        search_client: MultiProviderClient,
        candidate_tasks: list[dict[str, Any]],
        config: SearchConfig,
        io_pool: ThreadPoolExecutor,
        progress: SearchProgressTracker | None = None,
    ) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]], list[str]]:
        """Augment baseline candidate tasks with sparse free-provider discoveries.

        Args:
            search_client: Client used to execute search requests.
            candidate_tasks: Estimator tasks used for the baseline run.
            config: Search configuration for the operation.
            io_pool: Thread pool used for I/O-bound provider validation.
            progress: Progress tracker for the search job.

        Returns:
            tuple[list[dict[str, Any]], dict[str, dict[str, Any]], list[str]]:
                Updated candidate tasks, discovery metadata, and warnings.
        """
        free_discovery_provider_ids = tuple(
            provider_id
            for provider_id in search_client.active_provider_ids
            if provider_id in _FREE_PROVIDER_IDS and provider_id != "kiwi"
        )
        if not free_discovery_provider_ids or not candidate_tasks:
            return candidate_tasks, {}, []

        if progress is not None:
            progress.log_message(
                "Free-provider discovery: probing non-Kiwi providers for extra candidate dates.",
                phase="setup",
            )

        updated_tasks: list[dict[str, Any]] = []
        discovery_metadata: dict[str, dict[str, Any]] = {}
        warnings: list[str] = []
        for task in candidate_tasks:
            destination = str(task.get("destination") or "")
            route_dates = self._build_initial_free_provider_discovery_seed_map(task=task)
            if not route_dates:
                updated_tasks.append(task)
                continue
            if progress is not None:
                progress.log_message(
                    f"{destination}: probing {len(route_dates)} route(s) on "
                    f"{'/'.join(free_discovery_provider_ids)} before candidate scoring.",
                    phase="setup",
                )
            discovered_prices, discovery_warnings = await self._probe_free_provider_discovery(
                search_client=search_client,
                provider_ids=free_discovery_provider_ids,
                route_dates=route_dates,
                config=config,
                io_pool=io_pool,
                io_cap=FREE_PROVIDER_DISCOVERY_IO_CAP,
            )
            warnings.extend(discovery_warnings[:12])
            updated_tasks.append(
                self._merge_discovery_prices_into_task(
                    task=task,
                    discovered_prices=discovered_prices,
                )
            )
            discovery_metadata[destination] = {
                "destination": destination,
                "provider_ids": list(free_discovery_provider_ids),
                "seed_routes": len(route_dates),
                "discovered_routes": len(discovered_prices),
                "discovered_price_points": sum(
                    len(prices) for prices in discovered_prices.values()
                ),
            }
        return updated_tasks, discovery_metadata, warnings

    def _merge_discovery_prices_into_task(
        self,
        *,
        task: dict[str, Any],
        discovered_prices: dict[tuple[str, str], dict[str, int]],
    ) -> dict[str, Any]:
        """Merge sparse discovery prices back into an estimator task.

        Args:
            task: Estimator task to augment.
            discovered_prices: Sparse prices keyed by route/date.

        Returns:
            dict[str, Any]: Augmented estimator task.
        """
        if not discovered_prices:
            return dict(task)

        date_keys = tuple(str(value) for value in task["date_keys"])
        destination = str(task["destination"])

        def merged_series(
            series: tuple[int | None, ...],
            route_key: tuple[str, str],
        ) -> tuple[int | None, ...]:
            route_prices = discovered_prices.get(route_key) or {}
            if not route_prices:
                return tuple(series)
            out = list(series)
            for index, date_iso in enumerate(date_keys):
                price = route_prices.get(date_iso)
                if price is None:
                    continue
                current = out[index]
                if current is None or int(price) < int(current):
                    out[index] = int(price)
            return tuple(out)

        merged_task = dict(task)
        merged_task["origin_to_hub"] = {
            key: merged_series(tuple(series), key)
            for key, series in dict(task["origin_to_hub"]).items()
        }
        merged_task["hub_to_origin"] = {
            key: merged_series(tuple(series), key)
            for key, series in dict(task["hub_to_origin"]).items()
        }
        merged_task["hub_to_destination"] = {
            hub: merged_series(tuple(series), (str(hub), destination))
            for hub, series in dict(task["hub_to_destination"]).items()
        }
        merged_task["destination_to_hub"] = {
            hub: merged_series(tuple(series), (destination, str(hub)))
            for hub, series in dict(task["destination_to_hub"]).items()
        }
        merged_task["hub_to_hub"] = {
            key: merged_series(tuple(series), key)
            for key, series in dict(task.get("hub_to_hub", {})).items()
        }
        merged_task["origin_to_destination"] = {
            key: merged_series(tuple(series), key)
            for key, series in dict(task.get("origin_to_destination", {})).items()
        }
        merged_task["destination_to_origin"] = {
            key: merged_series(tuple(series), key)
            for key, series in dict(task.get("destination_to_origin", {})).items()
        }
        return merged_task

    async def _run_coverage_audit(
        self,
        *,
        search_client: MultiProviderClient,
        candidate_tasks: list[dict[str, Any]],
        estimated_by_destination: dict[str, list[dict[str, Any]]],
        config: SearchConfig,
        io_pool: ThreadPoolExecutor,
        progress: SearchProgressTracker | None = None,
    ) -> tuple[dict[str, list[dict[str, Any]]], dict[str, dict[str, Any]], list[str]]:
        """Run a widened audit pass for the most promising destinations.

        Args:
            search_client: Client used to execute search requests.
            candidate_tasks: Estimator tasks used for the baseline run.
            estimated_by_destination: Baseline estimated candidates by destination.
            config: Search configuration for the operation.
            io_pool: Thread pool used for I/O-bound provider validation.
            progress: Progress tracker for the search job.

        Returns:
            tuple[dict[str, list[dict[str, Any]]], dict[str, dict[str, Any]], list[str]]:
                Audited estimates, audit metadata, and warnings.
        """
        task_by_destination = {
            str(task["destination"]): dict(task)
            for task in candidate_tasks
            if task.get("destination")
        }
        audit_destinations = self._select_coverage_audit_destinations(
            estimated_by_destination,
            objective=config.objective,
        )
        if not audit_destinations:
            return {}, {}, []

        free_discovery_provider_ids = tuple(
            provider_id
            for provider_id in search_client.active_provider_ids
            if provider_id in _FREE_PROVIDER_IDS and provider_id != "kiwi"
        )
        if progress is not None:
            progress.log_message(
                f"Coverage audit: widening search for {len(audit_destinations)} destination(s).",
                phase="candidates",
            )

        audit_tasks: list[dict[str, Any]] = []
        audit_metadata: dict[str, dict[str, Any]] = {}
        warnings: list[str] = []

        for destination in audit_destinations:
            base_task = task_by_destination.get(destination)
            baseline_candidates = list(estimated_by_destination.get(destination) or [])
            if not base_task or not baseline_candidates:
                continue
            route_dates = self._build_free_provider_discovery_seed_map(
                destination=destination,
                estimated_candidates=baseline_candidates,
                config=config,
            )
            discovered_prices: dict[tuple[str, str], dict[str, int]] = {}
            discovery_warnings: list[str] = []
            if free_discovery_provider_ids and route_dates:
                if progress is not None:
                    progress.log_message(
                        f"{destination}: probing {len(route_dates)} route(s) on "
                        f"{'/'.join(free_discovery_provider_ids)} for extra date discovery.",
                        phase="candidates",
                    )
                discovered_prices, discovery_warnings = await self._probe_free_provider_discovery(
                    search_client=search_client,
                    provider_ids=free_discovery_provider_ids,
                    route_dates=route_dates,
                    config=config,
                    io_pool=io_pool,
                )
                warnings.extend(discovery_warnings[:12])

            audited_task = self._merge_discovery_prices_into_task(
                task=base_task,
                discovered_prices=discovered_prices,
            )
            audited_task["audit_mode"] = True
            audited_task["prune_score_margin_ratio"] = COVERAGE_AUDIT_PRUNING_SCORE_MARGIN_RATIO
            audited_task["prune_price_margin"] = COVERAGE_AUDIT_PRUNING_PRICE_MARGIN
            audited_task["chain_pair_limit_multiplier"] = COVERAGE_AUDIT_CHAIN_PAIR_MULTIPLIER
            audited_task["max_candidates"] = min(
                MAX_EXHAUSTIVE_SPLIT_CANDIDATES_PER_DESTINATION,
                max(
                    int(base_task["max_candidates"]) * COVERAGE_AUDIT_SPLIT_CANDIDATE_MULTIPLIER,
                    len(baseline_candidates) * COVERAGE_AUDIT_SPLIT_CANDIDATE_MULTIPLIER,
                ),
            )
            audited_task["max_direct_candidates"] = min(
                MAX_EXHAUSTIVE_DIRECT_CANDIDATES_PER_DESTINATION,
                max(
                    int(base_task.get("max_direct_candidates") or base_task["max_candidates"])
                    * COVERAGE_AUDIT_DIRECT_CANDIDATE_MULTIPLIER,
                    len(baseline_candidates) * COVERAGE_AUDIT_DIRECT_CANDIDATE_MULTIPLIER,
                ),
            )
            audit_tasks.append(audited_task)
            discovery_route_count = len(discovered_prices)
            discovery_price_points = sum(len(prices) for prices in discovered_prices.values())
            audit_metadata[destination] = {
                "destination": destination,
                "provider_ids": list(free_discovery_provider_ids),
                "seed_routes": len(route_dates),
                "discovered_routes": discovery_route_count,
                "discovered_price_points": discovery_price_points,
                "baseline_candidates": len(baseline_candidates),
                "expanded_max_candidates": audited_task["max_candidates"],
                "expanded_direct_candidates": audited_task["max_direct_candidates"],
            }
            if progress is not None:
                progress.log_message(
                    f"{destination}: coverage audit prepared "
                    f"{discovery_price_points} discovered price point(s) across "
                    f"{discovery_route_count} route(s).",
                    phase="candidates",
                )

        if progress is not None:
            progress.set_runtime_data(
                "coverage_audit",
                {
                    "destinations": list(audit_metadata.values()),
                    "provider_ids": list(free_discovery_provider_ids),
                },
            )

        if not audit_tasks:
            return {}, audit_metadata, warnings

        audited_estimates = await self._estimate_candidates_parallel(
            audit_tasks,
            config,
            progress=None,
        )
        if progress is not None:
            progress.log_message(
                "Coverage audit complete: "
                f"{sum(len(items or []) for items in audited_estimates.values())} extra estimates reviewed.",
                phase="candidates",
            )
        return audited_estimates, audit_metadata, warnings

    @staticmethod
    def _prune_dominated_split_results(
        results: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], int]:
        """Handle prune dominated split results.

        Args:
            results: Result records for the current operation.

        Returns:
            tuple[list[dict[str, Any]], int]: Handle prune dominated split results.
        """
        if not results:
            return results, 0

        # Key direct alternatives by destination, origin, outbound date, target stay, and arrival origin.
        direct_best: dict[tuple[str, str, str, int, str], dict[str, Any]] = {}
        for item in results:
            if item.get("itinerary_type") != "direct_roundtrip":
                continue
            outbound = item.get("outbound") or {}
            inbound = item.get("inbound") or {}
            key = (
                str(item.get("destination_code") or ""),
                str(outbound.get("origin") or ""),
                str(outbound.get("date_from_origin") or ""),
                int(item.get("main_destination_stay_days") or 0),
                str(inbound.get("arrival_origin") or ""),
            )
            best = direct_best.get(key)
            if best is None:
                direct_best[key] = item
                continue

            best_price = int(best.get("total_price") or 10**9)
            current_price = int(item.get("total_price") or 10**9)
            if current_price < best_price:
                direct_best[key] = item
                continue
            if current_price > best_price:
                continue

            best_outbound = best.get("outbound_time_to_destination_seconds")
            current_outbound = item.get("outbound_time_to_destination_seconds")
            if best_outbound is None:
                continue
            if current_outbound is None or int(current_outbound) < int(best_outbound):
                direct_best[key] = item

        filtered: list[dict[str, Any]] = []
        removed = 0
        for item in results:
            if item.get("itinerary_type") != "split_stopover":
                filtered.append(item)
                continue

            outbound = item.get("outbound") or {}
            inbound = item.get("inbound") or {}
            key = (
                str(item.get("destination_code") or ""),
                str(outbound.get("origin") or ""),
                str(outbound.get("date_from_origin") or ""),
                int(item.get("main_destination_stay_days") or 0),
                str(inbound.get("arrival_origin") or ""),
            )
            direct = direct_best.get(key)
            if not direct:
                filtered.append(item)
                continue

            split_price = int(item.get("total_price") or 10**9)
            direct_price = int(direct.get("total_price") or 10**9)
            if direct_price > split_price:
                filtered.append(item)
                continue

            split_outbound = item.get("outbound_time_to_destination_seconds")
            direct_outbound = direct.get("outbound_time_to_destination_seconds")
            if split_outbound is None or direct_outbound is None:
                removed += 1
                continue

            if int(direct_outbound) <= int(split_outbound):
                removed += 1
                continue

            filtered.append(item)

        return filtered, removed

    def _pick_auto_hubs(
        self,
        destination: str,
        config: SearchConfig,
        calendars: dict[tuple[str, str], dict[str, int]],
    ) -> tuple[list[str], list[str]]:
        """Handle pick auto hubs.

        Args:
            destination: Destination airport code for the request.
            config: Search configuration for the operation.
            calendars: Mapping of calendars.

        Returns:
            tuple[list[str], list[str]]: Handle pick auto hubs.
        """

        def robust_low(values: list[int]) -> float | None:
            if not values:
                return None
            sorted_values = sorted(values)
            idx = max(0, int(len(sorted_values) * 0.2) - 1)
            return float(sorted_values[idx])

        outbound_score_by_hub: dict[str, float] = {}
        inbound_score_by_hub: dict[str, float] = {}
        origin_to_hub_best: dict[str, int | None] = {}
        hub_to_origin_best: dict[str, int | None] = {}
        hub_to_destination_best: dict[str, int | None] = {}
        destination_to_hub_best: dict[str, int | None] = {}
        bundle_market_best_by_hub: dict[str, int | None] = {}

        for hub in config.hub_candidates:
            origin_to_hub_samples: list[int] = []
            hub_to_origin_samples: list[int] = []
            for origin in config.origins:
                prices_a = calendars.get((origin, hub), {})
                prices_b = calendars.get((hub, origin), {})
                if prices_a:
                    origin_to_hub_samples.extend(prices_a.values())
                if prices_b:
                    hub_to_origin_samples.extend(prices_b.values())

            hub_to_dest = calendars.get((hub, destination), {})
            dest_to_hub = calendars.get((destination, hub), {})

            hub_to_dest_samples = list(hub_to_dest.values())
            dest_to_hub_samples = list(dest_to_hub.values())
            origin_to_hub_best[hub] = min(origin_to_hub_samples) if origin_to_hub_samples else None
            hub_to_origin_best[hub] = min(hub_to_origin_samples) if hub_to_origin_samples else None
            hub_to_destination_best[hub] = min(hub_to_dest_samples) if hub_to_dest_samples else None
            destination_to_hub_best[hub] = min(dest_to_hub_samples) if dest_to_hub_samples else None
            bundle_market_best = _estimate_inner_return_bundle_price(
                hub_to_destination_best[hub],
                destination_to_hub_best[hub],
            )
            bundle_market_best_by_hub[hub] = bundle_market_best
            bundle_market_robust = _estimate_inner_return_bundle_price(
                (
                    int(robust_low(hub_to_dest_samples))
                    if robust_low(hub_to_dest_samples) is not None
                    else hub_to_destination_best[hub]
                ),
                (
                    int(robust_low(dest_to_hub_samples))
                    if robust_low(dest_to_hub_samples) is not None
                    else destination_to_hub_best[hub]
                ),
            )

            if origin_to_hub_samples and hub_to_dest_samples:
                best_part = min(origin_to_hub_samples) + int(
                    bundle_market_best or min(hub_to_dest_samples)
                )
                robust_part = (robust_low(origin_to_hub_samples) or 0.0) + (
                    float(bundle_market_robust)
                    if bundle_market_robust is not None
                    else (robust_low(hub_to_dest_samples) or 0.0)
                )
                coverage_bonus = min(
                    160.0,
                    float(len(set(origin_to_hub_samples)) + len(set(hub_to_dest_samples))),
                )
                outbound_score_by_hub[hub] = (
                    (0.58 * best_part) + (0.42 * robust_part) - coverage_bonus
                )

            if hub_to_origin_samples and dest_to_hub_samples:
                best_part = int(bundle_market_best or min(dest_to_hub_samples)) + min(
                    hub_to_origin_samples
                )
                robust_part = (robust_low(dest_to_hub_samples) or 0.0) + (
                    robust_low(hub_to_origin_samples) or 0.0
                )
                if bundle_market_robust is not None:
                    robust_part = float(bundle_market_robust) + (
                        robust_low(hub_to_origin_samples) or 0.0
                    )
                coverage_bonus = min(
                    160.0,
                    float(len(set(dest_to_hub_samples)) + len(set(hub_to_origin_samples))),
                )
                inbound_score_by_hub[hub] = (
                    (0.58 * best_part) + (0.42 * robust_part) - coverage_bonus
                )

        if config.max_transfers_per_direction >= 2:
            for hub in config.hub_candidates:
                outbound_chain_scores: list[float] = []
                inbound_chain_scores: list[float] = []

                current_hub_to_destination = bundle_market_best_by_hub.get(
                    hub
                ) or hub_to_destination_best.get(hub)
                current_origin_to_hub = origin_to_hub_best.get(hub)
                current_destination_to_hub = bundle_market_best_by_hub.get(
                    hub
                ) or destination_to_hub_best.get(hub)
                current_hub_to_origin = hub_to_origin_best.get(hub)

                if current_hub_to_destination is not None:
                    for bridge_hub in config.hub_candidates:
                        if bridge_hub == hub:
                            continue
                        bridge_origin = origin_to_hub_best.get(bridge_hub)
                        bridge_to_hub = _min_calendar_price(calendars.get((bridge_hub, hub), {}))
                        if bridge_origin is None or bridge_to_hub is None:
                            continue
                        outbound_chain_scores.append(
                            float(bridge_origin + bridge_to_hub + current_hub_to_destination)
                        )

                if current_origin_to_hub is not None:
                    for bridge_hub in config.hub_candidates:
                        if bridge_hub == hub:
                            continue
                        hub_to_bridge = _min_calendar_price(calendars.get((hub, bridge_hub), {}))
                        bridge_to_destination = bundle_market_best_by_hub.get(
                            bridge_hub
                        ) or hub_to_destination_best.get(bridge_hub)
                        if hub_to_bridge is None or bridge_to_destination is None:
                            continue
                        outbound_chain_scores.append(
                            float(current_origin_to_hub + hub_to_bridge + bridge_to_destination)
                        )

                if current_hub_to_origin is not None:
                    for bridge_hub in config.hub_candidates:
                        if bridge_hub == hub:
                            continue
                        destination_to_bridge = bundle_market_best_by_hub.get(
                            bridge_hub
                        ) or destination_to_hub_best.get(bridge_hub)
                        bridge_to_hub = _min_calendar_price(calendars.get((bridge_hub, hub), {}))
                        if destination_to_bridge is None or bridge_to_hub is None:
                            continue
                        inbound_chain_scores.append(
                            float(destination_to_bridge + bridge_to_hub + current_hub_to_origin)
                        )

                if current_destination_to_hub is not None:
                    for bridge_hub in config.hub_candidates:
                        if bridge_hub == hub:
                            continue
                        hub_to_bridge = _min_calendar_price(calendars.get((hub, bridge_hub), {}))
                        bridge_to_origin = hub_to_origin_best.get(bridge_hub)
                        if hub_to_bridge is None or bridge_to_origin is None:
                            continue
                        inbound_chain_scores.append(
                            float(current_destination_to_hub + hub_to_bridge + bridge_to_origin)
                        )

                if outbound_chain_scores:
                    best_outbound = min(outbound_chain_scores)
                    existing = outbound_score_by_hub.get(hub)
                    if existing is None or best_outbound < existing:
                        outbound_score_by_hub[hub] = best_outbound

                if inbound_chain_scores:
                    best_inbound = min(inbound_chain_scores)
                    existing = inbound_score_by_hub.get(hub)
                    if existing is None or best_inbound < existing:
                        inbound_score_by_hub[hub] = best_inbound

        outbound_scores = sorted((score, hub) for hub, score in outbound_score_by_hub.items())
        inbound_scores = sorted((score, hub) for hub, score in inbound_score_by_hub.items())

        if config.exhaustive_hub_scan:
            outbound = [hub for _, hub in outbound_scores]
            inbound = [hub for _, hub in inbound_scores]
            if not outbound:
                outbound = list(config.hub_candidates)
            if not inbound:
                inbound = list(config.hub_candidates)
            return outbound, inbound

        adaptive_auto_hubs = config.auto_hubs_per_direction
        destination_distances = [
            self._distance_km(origin, destination) for origin in config.origins
        ]
        valid_distances = [d for d in destination_distances if d is not None]
        if valid_distances:
            nearest_distance_km = min(valid_distances)
            if nearest_distance_km >= 9000:
                adaptive_auto_hubs = max(adaptive_auto_hubs, 18)
            elif nearest_distance_km >= 6500:
                adaptive_auto_hubs = max(adaptive_auto_hubs, 14)
        if config.max_transfers_per_direction >= 3:
            adaptive_auto_hubs = max(adaptive_auto_hubs, min(24, len(config.hub_candidates)))
        elif config.max_transfers_per_direction >= 2:
            adaptive_auto_hubs = max(adaptive_auto_hubs, min(18, len(config.hub_candidates)))
        adaptive_auto_hubs = min(adaptive_auto_hubs, len(config.hub_candidates))

        outbound = [hub for _, hub in outbound_scores[:adaptive_auto_hubs]]
        inbound = [hub for _, hub in inbound_scores[:adaptive_auto_hubs]]

        if not outbound:
            outbound = list(config.hub_candidates[:adaptive_auto_hubs])
        if not inbound:
            inbound = list(config.hub_candidates[:adaptive_auto_hubs])

        return outbound, inbound

    async def _fetch_calendars_parallel(
        self,
        search_client: MultiProviderClient,
        routes: list[tuple[str, str]],
        config: SearchConfig,
        io_pool: ThreadPoolExecutor,
        calendar_provider_ids: tuple[str, ...] | None = None,
        progress: SearchProgressTracker | None = None,
    ) -> tuple[dict[tuple[str, str], dict[str, int]], list[str]]:
        """Fetch calendars parallel.

        Args:
            search_client: Client used to execute search requests.
            routes: Collection of routes.
            config: Search configuration for the operation.
            io_pool: Thread pool used for I/O-bound provider validation.
            calendar_provider_ids: Identifiers for calendar provider.
            progress: Progress ratio for the current phase.

        Returns:
            tuple[dict[tuple[str, str], dict[str, int]], list[str]]: Calendars parallel.
        """
        loop = asyncio.get_running_loop()
        warnings: list[str] = []
        sem = asyncio.Semaphore(bounded_io_concurrency(config.io_workers))

        common_kwargs = {
            "date_start_iso": config.period_start.isoformat(),
            "date_end_iso": config.period_end.isoformat(),
            "currency": config.currency,
            "max_stops_per_leg": config.max_stops_per_leg,
            "adults": config.passengers.adults,
            "hand_bags": config.passengers.hand_bags,
            "hold_bags": config.passengers.hold_bags,
        }

        out: dict[tuple[str, str], dict[str, int]] = {}
        completed_routes = 0
        total_routes = len(routes)

        async def fetch_route(source: str, destination: str) -> None:
            nonlocal completed_routes
            async with sem:
                try:
                    fn = partial(
                        search_client.get_calendar_prices,
                        source=source,
                        destination=destination,
                        provider_ids=calendar_provider_ids,
                        **common_kwargs,
                    )
                    prices = await loop.run_in_executor(io_pool, fn)
                    out[(source, destination)] = prices
                except Exception as exc:
                    warnings.append(f"Calendar fetch failed {source}->{destination}: {exc}")
                    out[(source, destination)] = {}
                finally:
                    completed_routes += 1
                    if progress is not None:
                        progress.advance_phase(
                            "calendar",
                            completed=completed_routes,
                            total=total_routes,
                            detail=(
                                f"Fetched {completed_routes}/{total_routes} route calendars "
                                f"(latest {source}->{destination})."
                            ),
                        )

        await asyncio.gather(*(fetch_route(s, d) for s, d in routes))
        return out, warnings

    @staticmethod
    def _merge_baggage_compared_fares(
        selected_item: dict[str, Any] | None,
        base_item: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        """Keep fare comparison metadata while surfacing the cheaper eligible fare."""

        def _price_value(raw_price: Any) -> float | None:
            try:
                return float(raw_price)
            except (TypeError, ValueError):
                return None

        selected = dict(selected_item) if selected_item else None
        if selected:
            selected["fare_mode"] = "selected_bags"

        base = dict(base_item) if base_item else None
        if base:
            base["fare_mode"] = "base_no_bags"

        if selected and base:
            selected["base_no_bags_price"] = base.get("price")
            selected["base_no_bags_formatted_price"] = base.get("formatted_price")
            selected["base_no_bags_provider"] = base.get("provider", "kiwi")
            selected["base_no_bags_booking_url"] = base.get("booking_url")

            base["selected_bags_price"] = selected.get("price")
            base["selected_bags_formatted_price"] = selected.get("formatted_price")
            base["selected_bags_provider"] = selected.get("provider")
            base["selected_bags_booking_url"] = selected.get("booking_url")

            selected_price = _price_value(selected.get("price"))
            base_price = _price_value(base.get("price"))
            if base_price is not None and (selected_price is None or base_price < selected_price):
                return base

        return selected or base

    async def _fetch_oneways_parallel(
        self,
        search_client: MultiProviderClient,
        leg_keys: list[tuple[str, str, str]],
        config: SearchConfig,
        io_pool: ThreadPoolExecutor,
        provider_map: dict[tuple[str, str, str], tuple[str, ...]] | None = None,
        base_provider_ids: tuple[str, ...] | None = None,
        progress: SearchProgressTracker | None = None,
        progress_completed_offset: int = 0,
        progress_total: int | None = None,
    ) -> tuple[dict[tuple[str, str, str], dict[str, Any] | None], list[str], int]:
        """Fetch oneways parallel.

        Args:
            search_client: Client used to execute search requests.
            leg_keys: Collection of leg keys.
            config: Search configuration for the operation.
            io_pool: Thread pool used for I/O-bound provider validation.
            provider_map: Mapping of provider.
            base_provider_ids: Identifiers for base provider.
            progress: Progress ratio for the current phase.
            progress_completed_offset: Completed-work offset applied to progress reporting.
            progress_total: Total work units represented by the progress sample.

        Returns:
            tuple[dict[tuple[str, str, str], dict[str, Any] | None], list[str], int]: Oneways parallel.
        """
        loop = asyncio.get_running_loop()
        warnings: list[str] = []
        sem = asyncio.Semaphore(bounded_io_concurrency(config.io_workers))
        compare_to_base = bool(config.market_compare_fares)
        compare_to_base = compare_to_base and (
            config.passengers.hand_bags > 0 or config.passengers.hold_bags > 0
        )
        max_connection_layover_seconds = (
            int(config.max_connection_layover_hours * SECONDS_PER_HOUR)
            if config.max_connection_layover_hours
            else None
        )
        # Count of legs where we also fetched a no-bag base fare for cheaper-fare comparison.
        base_fare_checked = 0

        out: dict[tuple[str, str, str], dict[str, Any] | None] = {}
        completed_legs = 0
        total_legs = len(leg_keys)

        async def fetch_leg(source: str, destination: str, date_iso: str) -> None:
            nonlocal base_fare_checked
            nonlocal completed_legs
            async with sem:
                try:
                    key = (source, destination, date_iso)
                    provider_ids = provider_map.get(key) if provider_map else None
                    fn = partial(
                        search_client.get_best_oneway,
                        source=source,
                        destination=destination,
                        departure_iso=date_iso,
                        currency=config.currency,
                        max_stops_per_leg=config.max_stops_per_leg,
                        adults=config.passengers.adults,
                        hand_bags=config.passengers.hand_bags,
                        hold_bags=config.passengers.hold_bags,
                        max_connection_layover_seconds=max_connection_layover_seconds,
                        provider_ids=provider_ids,
                    )
                    item = await loop.run_in_executor(io_pool, fn)
                    base_item: dict[str, Any] | None = None

                    if compare_to_base:
                        # Base no-bag fare is only meaningful on Kiwi (it prices bags explicitly and is free).
                        base_compare_provider_ids: tuple[str, ...] | None = None
                        if base_provider_ids and "kiwi" in base_provider_ids:
                            base_compare_provider_ids = ("kiwi",)
                        if base_compare_provider_ids:
                            base_fn = partial(
                                search_client.get_best_oneway,
                                source=source,
                                destination=destination,
                                departure_iso=date_iso,
                                currency=config.currency,
                                max_stops_per_leg=config.max_stops_per_leg,
                                adults=config.passengers.adults,
                                hand_bags=0,
                                hold_bags=0,
                                max_connection_layover_seconds=max_connection_layover_seconds,
                                provider_ids=base_compare_provider_ids,
                            )
                            base_item = await loop.run_in_executor(io_pool, base_fn)
                            if base_item:
                                base_fare_checked += 1
                    item = self._merge_baggage_compared_fares(item, base_item)

                    out[key] = item
                except Exception as exc:
                    warnings.append(
                        f"One-way fetch failed {source}->{destination} {date_iso}: {exc}"
                    )
                    out[(source, destination, date_iso)] = None
                finally:
                    completed_legs += 1
                    if progress is not None:
                        phase_total = progress_total if progress_total is not None else total_legs
                        progress.advance_phase(
                            "oneways",
                            completed=progress_completed_offset + completed_legs,
                            detail=(
                                f"Validated {progress_completed_offset + completed_legs}/"
                                f"{phase_total} one-way legs "
                                f"(latest {source}->{destination} {date_iso})."
                            ),
                        )

        await asyncio.gather(*(fetch_leg(s, d, day) for s, d, day in leg_keys))
        return out, warnings, base_fare_checked

    async def _fetch_returns_parallel(
        self,
        search_client: MultiProviderClient,
        trip_keys: list[tuple[str, str, str, str]],
        config: SearchConfig,
        io_pool: ThreadPoolExecutor,
        provider_map: dict[tuple[str, str, str, str], tuple[str, ...]] | None = None,
        base_provider_ids: tuple[str, ...] | None = None,
        progress: SearchProgressTracker | None = None,
        progress_completed_offset: int = 0,
        progress_total: int | None = None,
    ) -> tuple[dict[tuple[str, str, str, str], dict[str, Any] | None], list[str], int]:
        """Fetch returns parallel.

        Args:
            search_client: Client used to execute search requests.
            trip_keys: Collection of trip keys.
            config: Search configuration for the operation.
            io_pool: Thread pool used for I/O-bound provider validation.
            provider_map: Mapping of provider.
            base_provider_ids: Identifiers for base provider.
            progress: Progress ratio for the current phase.
            progress_completed_offset: Completed-work offset applied to progress reporting.
            progress_total: Total work units represented by the progress sample.

        Returns:
            tuple[dict[tuple[str, str, str, str], dict[str, Any] | None], list[str], int]: Returns parallel.
        """
        loop = asyncio.get_running_loop()
        warnings: list[str] = []
        sem = asyncio.Semaphore(bounded_io_concurrency(config.io_workers))
        compare_to_base = bool(config.market_compare_fares)
        compare_to_base = compare_to_base and (
            config.passengers.hand_bags > 0 or config.passengers.hold_bags > 0
        )
        max_connection_layover_seconds = (
            int(config.max_connection_layover_hours * SECONDS_PER_HOUR)
            if config.max_connection_layover_hours
            else None
        )
        base_fare_checked = 0

        out: dict[tuple[str, str, str, str], dict[str, Any] | None] = {}
        completed_trips = 0
        total_trips = len(trip_keys)

        async def fetch_trip(
            source: str,
            destination: str,
            outbound_iso: str,
            inbound_iso: str,
        ) -> None:
            nonlocal base_fare_checked
            nonlocal completed_trips
            async with sem:
                try:
                    key = (source, destination, outbound_iso, inbound_iso)
                    provider_ids = provider_map.get(key) if provider_map else None
                    fn = partial(
                        search_client.get_best_return,
                        source=source,
                        destination=destination,
                        outbound_iso=outbound_iso,
                        inbound_iso=inbound_iso,
                        currency=config.currency,
                        max_stops_per_leg=config.max_stops_per_leg,
                        adults=config.passengers.adults,
                        hand_bags=config.passengers.hand_bags,
                        hold_bags=config.passengers.hold_bags,
                        max_connection_layover_seconds=max_connection_layover_seconds,
                        provider_ids=provider_ids,
                    )
                    item = await loop.run_in_executor(io_pool, fn)
                    base_item: dict[str, Any] | None = None

                    if compare_to_base:
                        base_compare_provider_ids: tuple[str, ...] | None = None
                        if base_provider_ids and "kiwi" in base_provider_ids:
                            base_compare_provider_ids = ("kiwi",)
                        if base_compare_provider_ids:
                            base_fn = partial(
                                search_client.get_best_return,
                                source=source,
                                destination=destination,
                                outbound_iso=outbound_iso,
                                inbound_iso=inbound_iso,
                                currency=config.currency,
                                max_stops_per_leg=config.max_stops_per_leg,
                                adults=config.passengers.adults,
                                hand_bags=0,
                                hold_bags=0,
                                max_connection_layover_seconds=max_connection_layover_seconds,
                                provider_ids=base_compare_provider_ids,
                            )
                            base_item = await loop.run_in_executor(io_pool, base_fn)
                            if base_item:
                                base_fare_checked += 1
                    item = self._merge_baggage_compared_fares(item, base_item)

                    out[key] = item
                except Exception as exc:
                    warnings.append(
                        "Return fetch failed "
                        f"{source}->{destination} {outbound_iso}/{inbound_iso}: {exc}"
                    )
                    out[(source, destination, outbound_iso, inbound_iso)] = None
                finally:
                    completed_trips += 1
                    if progress is not None:
                        phase_total = progress_total if progress_total is not None else total_trips
                        progress.advance_phase(
                            "returns",
                            completed=progress_completed_offset + completed_trips,
                            detail=(
                                f"Validated {progress_completed_offset + completed_trips}/"
                                f"{phase_total} round-trips "
                                f"(latest {source}->{destination} {outbound_iso}/{inbound_iso})."
                            ),
                        )

        await asyncio.gather(
            *(fetch_trip(s, d, out_day, in_day) for s, d, out_day, in_day in trip_keys)
        )
        return out, warnings, base_fare_checked

    async def _estimate_candidates_parallel(
        self,
        tasks: list[dict[str, Any]],
        config: SearchConfig,
        progress: SearchProgressTracker | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        """Estimate candidates parallel.

        Args:
            tasks: Mapping of tasks.
            config: Search configuration for the operation.
            progress: Progress ratio for the current phase.

        Returns:
            dict[str, list[dict[str, Any]]]: Estimated candidates parallel.
        """
        if not tasks:
            return {}

        if not all(
            "origins" in task and "outbound_hubs" in task and "inbound_hubs" in task
            for task in tasks
        ):
            if config.cpu_workers <= 1 or len(tasks) == 1:
                out: dict[str, list[dict[str, Any]]] = {}
                for index, task in enumerate(tasks, start=1):
                    out[task["destination"]] = _estimate_candidates_for_destination(task)
                    if progress is not None:
                        progress.advance_phase(
                            "candidates",
                            completed=index,
                            total=len(tasks),
                            detail=(
                                f"Scored {index}/{len(tasks)} destination candidate pools "
                                f"(latest {task['destination']})."
                            ),
                        )
                return out

            loop = asyncio.get_running_loop()
            results: dict[str, list[dict[str, Any]]] = {}
            futures: list[asyncio.Task[tuple[str, list[dict[str, Any]]]]] = []
            cpu_pool = ProcessPoolExecutor(max_workers=min(config.cpu_workers, len(tasks)))
            wait_for_shutdown = True
            try:

                async def run_task(task: dict[str, Any]) -> tuple[str, list[dict[str, Any]]]:
                    data = await loop.run_in_executor(
                        cpu_pool, _estimate_candidates_for_destination, task
                    )
                    return task["destination"], data

                futures = [asyncio.create_task(run_task(task)) for task in tasks]
                completed = 0
                for future in asyncio.as_completed(futures):
                    destination, data = await future
                    results[destination] = data
                    completed += 1
                    if progress is not None:
                        progress.advance_phase(
                            "candidates",
                            completed=completed,
                            total=len(tasks),
                            detail=(
                                f"Scored {completed}/{len(tasks)} destination candidate pools "
                                f"(latest {destination})."
                            ),
                        )
            except asyncio.CancelledError:
                wait_for_shutdown = False
                for future in futures:
                    future.cancel()
                cpu_pool.shutdown(wait=False, cancel_futures=True)
                raise
            except Exception:
                wait_for_shutdown = False
                for future in futures:
                    future.cancel()
                cpu_pool.shutdown(wait=False, cancel_futures=True)
                raise
            finally:
                if wait_for_shutdown:
                    with contextlib.suppress(Exception):
                        cpu_pool.shutdown(wait=True, cancel_futures=False)
            return results

        compact_tasks, chunk_specs = _build_candidate_chunk_specs(tasks, config.cpu_workers)
        if not chunk_specs:
            return {}

        if progress is not None:
            progress.add_phase_total(
                "candidates",
                total=len(chunk_specs),
                detail=(
                    f"Scoring {len(chunk_specs)} candidate chunks across "
                    f"{len(compact_tasks)} destinations."
                ),
            )

        destination_total_chunks: dict[str, int] = {}
        for chunk in chunk_specs:
            destination = str(chunk["destination"])
            destination_total_chunks[destination] = destination_total_chunks.get(destination, 0) + 1

        if config.cpu_workers <= 1 or len(chunk_specs) == 1:
            out: dict[str, list[dict[str, Any]]] = {}
            partial_batches: dict[str, list[list[dict[str, Any]]]] = {}
            completed_chunks = 0
            destination_completed_chunks: dict[str, int] = {}
            for chunk in chunk_specs:
                destination, batch = _estimate_candidates_for_chunk(chunk)
                partial_batches.setdefault(destination, []).append(batch)
                destination_completed_chunks[destination] = (
                    destination_completed_chunks.get(destination, 0) + 1
                )
                completed_chunks += 1
                if progress is not None:
                    finished_destinations = sum(
                        1
                        for key, total in destination_total_chunks.items()
                        if destination_completed_chunks.get(key, 0) >= total
                    )
                    progress.advance_phase(
                        "candidates",
                        completed=completed_chunks,
                        total=len(chunk_specs),
                        detail=(
                            f"Scored {completed_chunks}/{len(chunk_specs)} candidate chunks "
                            f"({finished_destinations}/{len(destination_total_chunks)} destinations "
                            f"finished, latest {destination} {chunk['chunk_label']})."
                        ),
                    )
            for destination, batches in partial_batches.items():
                merged = [candidate for batch in batches for candidate in batch]
                task_meta = compact_tasks[destination]
                out[destination] = _finalize_estimated_candidates(
                    merged,
                    objective=str(task_meta["objective"]),
                    max_candidates=int(task_meta["max_candidates"]),
                    max_direct_candidates=int(task_meta["max_direct_candidates"]),
                )
            return out

        loop = asyncio.get_running_loop()
        results: dict[str, list[dict[str, Any]]] = {}
        partial_batches: dict[str, list[list[dict[str, Any]]]] = {}
        futures: list[asyncio.Task[tuple[str, list[dict[str, Any]]]]] = []

        cpu_pool = ProcessPoolExecutor(
            max_workers=min(config.cpu_workers, len(chunk_specs)),
            initializer=_candidate_worker_init,
            initargs=(compact_tasks,),
        )
        wait_for_shutdown = True
        try:

            async def run_task(chunk: dict[str, Any]) -> tuple[str, list[dict[str, Any]]]:
                return await loop.run_in_executor(cpu_pool, _estimate_candidates_for_chunk, chunk)

            futures = [asyncio.create_task(run_task(chunk)) for chunk in chunk_specs]
            completed_chunks = 0
            destination_completed_chunks: dict[str, int] = {}
            for future in asyncio.as_completed(futures):
                destination, data = await future
                partial_batches.setdefault(destination, []).append(data)
                destination_completed_chunks[destination] = (
                    destination_completed_chunks.get(destination, 0) + 1
                )
                completed_chunks += 1
                if progress is not None:
                    finished_destinations = sum(
                        1
                        for key, total in destination_total_chunks.items()
                        if destination_completed_chunks.get(key, 0) >= total
                    )
                    progress.advance_phase(
                        "candidates",
                        completed=completed_chunks,
                        total=len(chunk_specs),
                        detail=(
                            f"Scored {completed_chunks}/{len(chunk_specs)} candidate chunks "
                            f"({finished_destinations}/{len(destination_total_chunks)} destinations "
                            f"finished, latest {destination})."
                        ),
                    )
            for destination, batches in partial_batches.items():
                merged = [candidate for batch in batches for candidate in batch]
                task_meta = compact_tasks[destination]
                results[destination] = _finalize_estimated_candidates(
                    merged,
                    objective=str(task_meta["objective"]),
                    max_candidates=int(task_meta["max_candidates"]),
                    max_direct_candidates=int(task_meta["max_direct_candidates"]),
                )
        except asyncio.CancelledError:
            wait_for_shutdown = False
            for future in futures:
                future.cancel()
            cpu_pool.shutdown(wait=False, cancel_futures=True)
            raise
        except Exception:
            wait_for_shutdown = False
            for future in futures:
                future.cancel()
            cpu_pool.shutdown(wait=False, cancel_futures=True)
            raise
        finally:
            if wait_for_shutdown:
                with contextlib.suppress(Exception):
                    cpu_pool.shutdown(wait=True, cancel_futures=False)
        return results

    async def _search_async(
        self,
        config: SearchConfig,
        io_pool: ThreadPoolExecutor,
        *,
        search_id: str | None = None,
        progress: SearchProgressTracker | None = None,
    ) -> dict[str, Any]:
        """Handle search async.

        Args:
            config: Search configuration for the operation.
            io_pool: Thread pool used for I/O-bound provider validation.
            search_id: Identifier of the current search.
            progress: Progress ratio for the current phase.

        Returns:
            dict[str, Any]: Handle search async.
        """
        start_ts = time.time()
        warnings: list[str] = []
        config, hub_resolution_meta, hub_resolution_warnings = (
            self._expand_route_graph_hub_candidates(config)
        )
        warnings.extend(hub_resolution_warnings)
        if progress is not None:
            progress.mark_running(SEARCH_EVENT_STARTED)
            progress.start_phase(
                "setup",
                total=1,
                detail="Preparing providers and runtime limits.",
            )
            if hub_resolution_meta.get("hub_candidates_graph_applied"):
                progress.log_message(
                    "Route graph discovered "
                    f"{hub_resolution_meta['hub_candidates_graph_count']} extra hub candidates.",
                    phase="setup",
                )
        search_client, provider_status, provider_warnings = self._build_search_client(config)
        warnings.extend(provider_warnings)
        if hasattr(search_client, "health_snapshot"):
            current_provider_health = search_client.health_snapshot()
        elif hasattr(search_client, "stats_snapshot"):
            current_provider_health = {
                "providers": {},
                "budget": dict((search_client.stats_snapshot() or {}).get("budget") or {}),
            }
        else:
            current_provider_health = {"providers": {}, "budget": {}}
        if progress is not None:
            progress.set_runtime_data("provider_health", current_provider_health)
            progress.set_runtime_data(
                "coverage_audit",
                {"destinations": [], "provider_ids": []},
            )
            stats_listener_setter = getattr(search_client, "set_stats_listener", None)
            if callable(stats_listener_setter):
                stats_listener_setter(
                    lambda payload: progress.set_runtime_data("provider_health", payload)
                )
        log_event(
            logging.INFO,
            "search_phase_providers_ready",
            search_id=search_id,
            providers_active=search_client.active_provider_ids,
            providers_requested=list(config.provider_ids),
            provider_status=provider_status,
            hub_candidates_effective=len(config.hub_candidates),
            hub_candidates_graph=int(hub_resolution_meta.get("hub_candidates_graph_count") or 0),
        )
        if progress is not None:
            active_label = "/".join(search_client.active_provider_ids) or "kiwi"
            progress.complete_phase(
                "setup",
                detail=f"Providers ready: {active_label}.",
            )
        if "kiwi" in search_client.active_provider_ids:
            # Kiwi calendar is free and broad; use it for dense seeding to preserve paid quotas.
            calendar_provider_ids = ("kiwi",)
        else:
            calendar_provider_ids = tuple(
                provider_id
                for provider_id in search_client.active_provider_ids
                if bool(getattr(self.providers.get(provider_id), "supports_calendar", True))
            )
        if not calendar_provider_ids:
            calendar_provider_ids = tuple(search_client.active_provider_ids)

        if config.calendar_hubs_prefetch is None:
            prefetch_hub_limit = len(config.hub_candidates)
        else:
            prefetch_hub_limit = min(
                len(config.hub_candidates),
                max(6, config.calendar_hubs_prefetch),
            )
            if config.exhaustive_hub_scan:
                prefetch_hub_limit = min(
                    len(config.hub_candidates),
                    max(
                        prefetch_hub_limit,
                        min(len(config.hub_candidates), config.calendar_hubs_prefetch * 2),
                    ),
                )
        prefetch_hubs = tuple(config.hub_candidates[:prefetch_hub_limit])
        if prefetch_hub_limit < len(config.hub_candidates):
            warnings.append(
                "Calendar prefetch scanned "
                f"{prefetch_hub_limit}/{len(config.hub_candidates)} hubs "
                "to keep API usage under control."
            )

        routes_set: set[tuple[str, str]] = set()
        for origin in config.origins:
            for hub in prefetch_hubs:
                if origin != hub:
                    routes_set.add((origin, hub))
                    routes_set.add((hub, origin))
        for destination in config.destinations:
            for hub in prefetch_hubs:
                if destination != hub:
                    routes_set.add((hub, destination))
                    routes_set.add((destination, hub))
            for origin in config.origins:
                if origin != destination:
                    routes_set.add((origin, destination))
                    routes_set.add((destination, origin))

        if config.max_transfers_per_direction >= 2 and "kiwi" in calendar_provider_ids:
            chain_hub_limit = min(
                len(prefetch_hubs),
                56 if config.exhaustive_hub_scan else 28,
            )
            chain_hubs = tuple(prefetch_hubs[:chain_hub_limit])
            for first_hub in chain_hubs:
                for second_hub in chain_hubs:
                    if first_hub == second_hub:
                        continue
                    routes_set.add((first_hub, second_hub))
            if chain_hub_limit > 0:
                warnings.append(
                    "Manual-chain mode enabled: prefetching "
                    f"{chain_hub_limit} hubs for hub-to-hub calendar links."
                )

        routes = sorted(routes_set)
        if progress is not None:
            progress.start_phase(
                "calendar",
                total=len(routes),
                detail=f"Fetching {len(routes)} route calendars.",
            )
        calendars, calendar_warnings = await self._fetch_calendars_parallel(
            search_client,
            routes,
            config,
            io_pool,
            calendar_provider_ids=calendar_provider_ids,
            progress=progress,
        )
        warnings.extend(calendar_warnings)
        log_event(
            logging.INFO,
            "search_phase_calendars_done",
            search_id=search_id,
            routes=len(routes),
            warnings_count=len(calendar_warnings),
            calendar_providers=list(calendar_provider_ids),
        )
        if progress is not None:
            progress.complete_phase(
                "calendar",
                detail=f"Calendar stage complete: {len(routes)} routes scanned.",
            )

        chosen_hubs: dict[str, dict[str, list[str]]] = {}
        candidate_tasks: list[dict[str, Any]] = []
        free_provider_discovery_metadata: dict[str, dict[str, Any]] = {}

        # Candidate pool multiplier should increase *estimated* coverage, not multiply
        # the number of live validations (which becomes explosive for multi-destination
        # searches). Validation stays anchored to validate_top_per_destination.
        validation_target_per_destination = config.validate_top_per_destination
        estimated_pool_target_per_destination = max(
            validation_target_per_destination,
            validation_target_per_destination * config.estimated_pool_multiplier,
        )
        coverage_priority_multiplier = (
            CANDIDATE_CHAIN_PAIR_PRIORITY_MULTIPLIER
            if config.objective in {"best", "cheapest"}
            else 1
        )
        max_candidates = estimated_pool_target_per_destination
        if config.exhaustive_hub_scan:
            max_candidates = max(
                max_candidates,
                min(
                    MAX_EXHAUSTIVE_SPLIT_CANDIDATES_PER_DESTINATION,
                    config.validate_top_per_destination
                    * max(24, config.estimated_pool_multiplier * coverage_priority_multiplier),
                ),
            )
        else:
            max_candidates = min(
                MAX_NON_EXHAUSTIVE_SPLIT_CANDIDATES_PER_DESTINATION,
                max(
                    max_candidates,
                    config.validate_top_per_destination
                    * max(8, config.estimated_pool_multiplier * coverage_priority_multiplier),
                ),
            )
        date_span_days = max(1, (config.period_end - config.period_start).days + 1)
        stay_options = max(1, config.max_stay_days - config.min_stay_days + 1)
        theoretical_direct_pairs = date_span_days * stay_options * max(1, len(config.origins))
        direct_target = max(
            theoretical_direct_pairs,
            max_candidates * 2,
        )
        if config.exhaustive_hub_scan:
            max_direct_candidates = min(
                MAX_EXHAUSTIVE_DIRECT_CANDIDATES_PER_DESTINATION,
                max(1600, direct_target),
            )
        else:
            max_direct_candidates = min(
                MAX_NON_EXHAUSTIVE_DIRECT_CANDIDATES_PER_DESTINATION,
                max(600, direct_target),
            )
        candidate_date_keys = tuple(
            day.isoformat() for day in date_range(config.period_start, config.period_end)
        )

        for destination in config.destinations:
            outbound_hubs, inbound_hubs = self._pick_auto_hubs(destination, config, calendars)
            chosen_hubs[destination] = {
                "outbound": outbound_hubs,
                "inbound": inbound_hubs,
            }

            if not outbound_hubs or not inbound_hubs:
                continue

            origin_to_hub: dict[tuple[str, str], tuple[int | None, ...]] = {}
            hub_to_origin: dict[tuple[str, str], tuple[int | None, ...]] = {}
            hub_to_destination: dict[str, tuple[int | None, ...]] = {}
            destination_to_hub: dict[str, tuple[int | None, ...]] = {}
            hub_to_hub: dict[tuple[str, str], tuple[int | None, ...]] = {}
            origin_to_destination: dict[tuple[str, str], tuple[int | None, ...]] = {}
            destination_to_origin: dict[tuple[str, str], tuple[int | None, ...]] = {}
            destination_distance_map: dict[tuple[str, str], float | None] = {}

            for origin in config.origins:
                for hub in outbound_hubs:
                    origin_to_hub[(origin, hub)] = _calendar_series_from_prices(
                        candidate_date_keys,
                        calendars.get((origin, hub), {}),
                    )
            for origin in config.origins:
                for hub in inbound_hubs:
                    hub_to_origin[(hub, origin)] = _calendar_series_from_prices(
                        candidate_date_keys,
                        calendars.get((hub, origin), {}),
                    )
            for hub in outbound_hubs:
                hub_to_destination[hub] = _calendar_series_from_prices(
                    candidate_date_keys,
                    calendars.get((hub, destination), {}),
                )
            for hub in inbound_hubs:
                destination_to_hub[hub] = _calendar_series_from_prices(
                    candidate_date_keys,
                    calendars.get((destination, hub), {}),
                )
            selected_hubs = list(dict.fromkeys(outbound_hubs + inbound_hubs))
            for first_hub in selected_hubs:
                for second_hub in selected_hubs:
                    if first_hub == second_hub:
                        continue
                    hub_to_hub[(first_hub, second_hub)] = _calendar_series_from_prices(
                        candidate_date_keys,
                        calendars.get((first_hub, second_hub), {}),
                    )

            for origin in config.origins:
                origin_to_destination[(origin, destination)] = _calendar_series_from_prices(
                    candidate_date_keys,
                    calendars.get((origin, destination), {}),
                )
                destination_to_origin[(destination, origin)] = _calendar_series_from_prices(
                    candidate_date_keys,
                    calendars.get((destination, origin), {}),
                )
                destination_distance_map[(origin, destination)] = self._distance_km(
                    origin,
                    destination,
                )

            candidate_tasks.append(
                {
                    "destination": destination,
                    "origins": list(config.origins),
                    "outbound_hubs": outbound_hubs,
                    "inbound_hubs": inbound_hubs,
                    "date_keys": candidate_date_keys,
                    "period_start": config.period_start.isoformat(),
                    "period_end": config.period_end.isoformat(),
                    "min_stay_days": config.min_stay_days,
                    "max_stay_days": config.max_stay_days,
                    "min_stopover_days": config.min_stopover_days,
                    "max_stopover_days": config.max_stopover_days,
                    "objective": config.objective,
                    "max_candidates": max_candidates,
                    "max_direct_candidates": max_direct_candidates,
                    "origin_to_hub": origin_to_hub,
                    "hub_to_origin": hub_to_origin,
                    "hub_to_destination": hub_to_destination,
                    "destination_to_hub": destination_to_hub,
                    "hub_to_hub": hub_to_hub,
                    "origin_to_destination": origin_to_destination,
                    "destination_to_origin": destination_to_origin,
                    "destination_distance_map": destination_distance_map,
                    "max_transfers_per_direction": config.max_transfers_per_direction,
                }
            )

        if candidate_tasks:
            (
                candidate_tasks,
                free_provider_discovery_metadata,
                free_provider_discovery_warnings,
            ) = await self._run_initial_free_provider_discovery(
                search_client=search_client,
                candidate_tasks=candidate_tasks,
                config=config,
                io_pool=io_pool,
                progress=progress,
            )
            warnings.extend(free_provider_discovery_warnings)

        if progress is not None:
            progress.start_phase(
                "candidates",
                total=len(candidate_tasks),
                detail=f"Scoring candidate pools for {len(candidate_tasks)} destinations.",
            )
        estimated_by_destination = await self._estimate_candidates_parallel(
            candidate_tasks,
            config,
            progress=progress,
        )
        coverage_audit_estimates: dict[str, list[dict[str, Any]]] = {}
        coverage_audit_metadata: dict[str, dict[str, Any]] = {}
        if estimated_by_destination:
            (
                coverage_audit_estimates,
                coverage_audit_metadata,
                coverage_audit_warnings,
            ) = await self._run_coverage_audit(
                search_client=search_client,
                candidate_tasks=candidate_tasks,
                estimated_by_destination=estimated_by_destination,
                config=config,
                io_pool=io_pool,
                progress=progress,
            )
            warnings.extend(coverage_audit_warnings)
            for destination, audited_candidates in coverage_audit_estimates.items():
                base_task = next(
                    (
                        task
                        for task in candidate_tasks
                        if str(task.get("destination") or "") == destination
                    ),
                    None,
                )
                if base_task is None:
                    continue
                audit_meta = coverage_audit_metadata.get(destination) or {}
                finalized_base_candidates = _filter_finalized_estimated_candidates(
                    list(estimated_by_destination.get(destination) or [])
                )
                finalized_audited_candidates = _filter_finalized_estimated_candidates(
                    list(audited_candidates or [])
                )
                if not finalized_base_candidates and not finalized_audited_candidates:
                    continue
                merged_candidates = _finalize_estimated_candidates(
                    finalized_base_candidates + finalized_audited_candidates,
                    objective=str(base_task["objective"]),
                    max_candidates=max(
                        int(base_task["max_candidates"]),
                        int(audit_meta.get("expanded_max_candidates") or 0),
                    ),
                    max_direct_candidates=max(
                        int(base_task.get("max_direct_candidates") or base_task["max_candidates"]),
                        int(audit_meta.get("expanded_direct_candidates") or 0),
                    ),
                )
                estimated_by_destination[destination] = merged_candidates
            if progress is not None and coverage_audit_metadata:
                progress.set_runtime_data(
                    "coverage_audit",
                    {
                        "destinations": list(coverage_audit_metadata.values()),
                        "provider_ids": sorted(
                            {
                                provider_id
                                for meta in coverage_audit_metadata.values()
                                for provider_id in (meta.get("provider_ids") or [])
                            }
                        ),
                    },
                )
        if progress is not None:
            progress.complete_phase(
                "candidates",
                detail=(
                    "Candidate scoring complete: "
                    f"{sum(len(items or []) for items in estimated_by_destination.values())} routes estimated."
                ),
            )
        log_event(
            logging.INFO,
            "search_phase_candidates_estimated",
            search_id=search_id,
            destinations=len(estimated_by_destination),
            total_candidates=sum(len(items or []) for items in estimated_by_destination.values()),
        )

        all_results: list[dict[str, Any]] = []
        total_oneway_legs = 0
        total_return_trips = 0
        filtered_by_connection_layover = 0
        filtered_invalid_split_boundaries = 0
        base_fare_selected_oneways = 0
        base_fare_selected_returns = 0
        max_connection_layover_seconds = (
            int(config.max_connection_layover_hours * SECONDS_PER_HOUR)
            if config.max_connection_layover_hours
            else None
        )
        active_provider_ids = tuple(search_client.active_provider_ids)
        core_provider_ids = tuple(
            provider for provider in active_provider_ids if provider != "serpapi"
        )
        if not core_provider_ids:
            core_provider_ids = active_provider_ids
        serpapi_active = "serpapi" in active_provider_ids
        origin_rank = {origin: idx for idx, origin in enumerate(config.origins)}
        comparison_links_cache: dict[tuple[str, str, str, str], dict[str, str]] = {}

        def cached_comparison_links(
            origin: str,
            destination: str,
            depart_iso: str,
            return_iso: str,
        ) -> dict[str, str]:
            cache_key = (origin, destination, depart_iso, return_iso)
            links = comparison_links_cache.get(cache_key)
            if links is None:
                links = build_comparison_links(
                    origin,
                    destination,
                    depart_iso,
                    return_iso,
                    adults=config.passengers.adults,
                    max_stops_per_leg=config.max_stops_per_leg,
                    currency=config.currency,
                )
                comparison_links_cache[cache_key] = links
            return links

        prepared_destinations: list[dict[str, Any]] = []
        total_build_candidates = 0
        for destination in config.destinations:
            estimated_candidates = estimated_by_destination.get(destination, [])
            if not estimated_candidates:
                continue

            destination_context, destination_warnings = (
                self._prepare_destination_validation_context(
                    destination=destination,
                    estimated_candidates=estimated_candidates,
                    config=config,
                    validation_target_per_destination=validation_target_per_destination,
                    origin_rank=origin_rank,
                    core_provider_ids=core_provider_ids,
                    serpapi_active=serpapi_active,
                    audit_destinations=set(coverage_audit_metadata),
                    audit_metadata=coverage_audit_metadata,
                )
            )
            warnings.extend(destination_warnings)
            prepared_destinations.append(destination_context)
            total_oneway_legs += len(destination_context["ordered_oneway_keys"])
            total_return_trips += len(destination_context["ordered_return_keys"])
            total_build_candidates += len(destination_context["limited_candidates"])

            log_event(
                logging.INFO,
                "destination_validation_start",
                search_id=search_id,
                destination=destination,
                destination_name=destination_context["destination_name"],
                estimated_candidates=destination_context["estimated_candidates_count"],
                limited_candidates=len(destination_context["limited_candidates"]),
                validation_target=destination_context["validation_target"],
                validate_oneway_keys=len(destination_context["ordered_oneway_keys"]),
                validate_return_keys=len(destination_context["ordered_return_keys"]),
            )

        destinations_with_candidates = len(prepared_destinations)
        raw_total_oneway_legs = total_oneway_legs
        raw_total_return_trips = total_return_trips

        def merge_provider_scope(
            current_scope: tuple[str, ...] | None,
            next_scope: tuple[str, ...] | None,
        ) -> tuple[str, ...] | None:
            if current_scope is None or next_scope is None:
                return None
            return tuple(dict.fromkeys((*current_scope, *next_scope)))

        global_ordered_oneway_keys: list[tuple[str, str, str]] = []
        global_oneway_provider_map: dict[tuple[str, str, str], tuple[str, ...] | None] = {}
        global_ordered_return_keys: list[tuple[str, str, str, str]] = []
        global_return_provider_map: dict[
            tuple[str, str, str, str],
            tuple[str, ...] | None,
        ] = {}

        for destination_context in prepared_destinations:
            destination_oneway_provider_map = dict(destination_context["oneway_provider_map"])
            for leg_key in destination_context["ordered_oneway_keys"]:
                provider_scope = destination_oneway_provider_map.get(leg_key)
                if leg_key not in global_oneway_provider_map:
                    global_ordered_oneway_keys.append(leg_key)
                    global_oneway_provider_map[leg_key] = provider_scope
                else:
                    global_oneway_provider_map[leg_key] = merge_provider_scope(
                        global_oneway_provider_map.get(leg_key),
                        provider_scope,
                    )

            destination_return_provider_map = dict(destination_context["return_provider_map"])
            for return_key in destination_context["ordered_return_keys"]:
                provider_scope = destination_return_provider_map.get(return_key)
                if return_key not in global_return_provider_map:
                    global_ordered_return_keys.append(return_key)
                    global_return_provider_map[return_key] = provider_scope
                else:
                    global_return_provider_map[return_key] = merge_provider_scope(
                        global_return_provider_map.get(return_key),
                        provider_scope,
                    )

        total_oneway_legs = len(global_ordered_oneway_keys)
        total_return_trips = len(global_ordered_return_keys)
        deduped_oneway_legs = max(0, raw_total_oneway_legs - total_oneway_legs)
        deduped_return_trips = max(0, raw_total_return_trips - total_return_trips)
        if deduped_oneway_legs > 0:
            warnings.append(
                f"Reused {deduped_oneway_legs} duplicate one-way validations across destinations."
            )
        if deduped_return_trips > 0:
            warnings.append(
                "Reused "
                f"{deduped_return_trips} duplicate round-trip validations across destinations."
            )

        returns_phase_started = False
        oneways_phase_started = False
        build_phase_started = False
        completed_build_candidates = 0

        if total_return_trips:
            if progress is not None:
                progress.start_phase(
                    "returns",
                    total=max(1, total_return_trips),
                    detail=f"Validating {total_return_trips} round-trip fare key(s).",
                )
                returns_phase_started = True
            (
                global_return_map,
                return_warnings,
                return_base_count,
            ) = await self._fetch_returns_parallel(
                search_client,
                global_ordered_return_keys,
                config,
                io_pool,
                provider_map=global_return_provider_map or None,
                base_provider_ids=core_provider_ids,
                progress=progress,
                progress_total=total_return_trips,
            )
            warnings.extend(return_warnings)
            base_fare_selected_returns += return_base_count
        else:
            global_return_map = {}

        if total_oneway_legs:
            if progress is not None:
                progress.start_phase(
                    "oneways",
                    total=max(1, total_oneway_legs),
                    detail=f"Validating {total_oneway_legs} one-way fare key(s).",
                )
                oneways_phase_started = True
            global_oneway_map, leg_warnings, leg_base_count = await self._fetch_oneways_parallel(
                search_client,
                global_ordered_oneway_keys,
                config,
                io_pool,
                provider_map=global_oneway_provider_map or None,
                base_provider_ids=core_provider_ids,
                progress=progress,
                progress_total=total_oneway_legs,
            )
            warnings.extend(leg_warnings)
            base_fare_selected_oneways += leg_base_count
        else:
            global_oneway_map = {}

        global_oneway_entry_cache = self._prepare_oneway_entry_cache(global_oneway_map)
        global_return_trip_cache = self._prepare_return_trip_cache(global_return_map)

        if progress is not None and total_build_candidates > 0:
            progress.start_phase(
                "build",
                total=max(1, total_build_candidates),
                detail=(
                    "Building "
                    f"{total_build_candidates} itinerary candidate(s) across "
                    f"{destinations_with_candidates} destination(s)."
                ),
            )
            build_phase_started = True

        for destination_context in prepared_destinations:
            destination = str(destination_context["destination"])
            destination_name = str(destination_context["destination_name"])
            notes = destination_context["notes"]
            limited_candidates = list(destination_context["limited_candidates"])
            ordered_return_keys = list(destination_context["ordered_return_keys"])
            ordered_oneway_keys = list(destination_context["ordered_oneway_keys"])
            destination_wall_start = time.time()
            return_map = {key: global_return_map.get(key) for key in ordered_return_keys}
            oneway_map = {key: global_oneway_map.get(key) for key in ordered_oneway_keys}
            oneway_entry_cache = {
                key: global_oneway_entry_cache.get(key) for key in ordered_oneway_keys
            }
            return_trip_cache = {
                key: global_return_trip_cache.get(key) for key in ordered_return_keys
            }
            log_event(
                logging.INFO,
                "destination_fares_fetched",
                search_id=search_id,
                destination=destination,
                elapsed_seconds=round(time.time() - destination_wall_start, 2),
                return_fares_found=sum(1 for item in return_map.values() if item),
                oneway_fares_found=sum(1 for item in oneway_map.values() if item),
            )

            candidate_total = len(limited_candidates)
            build_progress_chunk = max(10, min(250, candidate_total // 20 or 1))
            for candidate_index, candidate in enumerate(limited_candidates, start=1):
                if (
                    progress is not None
                    and build_phase_started
                    and candidate_index > 1
                    and (candidate_index - 1) % build_progress_chunk == 0
                ):
                    progress.advance_phase(
                        "build",
                        completed=completed_build_candidates + candidate_index - 1,
                        total=max(1, total_build_candidates),
                        detail=(
                            f"{destination}: assembled {candidate_index - 1}/{candidate_total} "
                            "candidate itinerary checks."
                        ),
                    )
                candidate_type = str(
                    candidate.get("_candidate_type")
                    or candidate.get("candidate_type")
                    or "split_stopover"
                )
                distance_basis_km = candidate.get("distance_basis_km")

                if candidate_type == "direct_roundtrip":
                    direct_return_key = candidate.get("_direct_return_key") or (
                        candidate["origin"],
                        candidate["destination"],
                        candidate["depart_origin_date"],
                        candidate["return_origin_date"],
                    )
                    direct_trip_meta = return_trip_cache.get(direct_return_key)
                    if not direct_trip_meta:
                        continue
                    direct_trip = direct_trip_meta["fare"]

                    outbound_layovers = int(
                        direct_trip.get("outbound_transfer_events") or direct_trip["outbound_stops"]
                    )
                    inbound_layovers = int(
                        direct_trip.get("inbound_transfer_events") or direct_trip["inbound_stops"]
                    )
                    if (
                        outbound_layovers > config.max_layovers_per_direction
                        or inbound_layovers > config.max_layovers_per_direction
                    ):
                        continue

                    total_price = direct_trip["price"]
                    score = self._score_candidate(
                        total_price,
                        distance_basis_km,
                        config.objective,
                    )
                    price_per_1000_km = (
                        round((total_price / distance_basis_km) * 1000.0, 1)
                        if distance_basis_km and distance_basis_km > 0
                        else None
                    )

                    outbound_segments = direct_trip_meta["outbound_segments"]
                    inbound_segments = direct_trip_meta["inbound_segments"]
                    if self._exceeds_connection_layover_limit(
                        outbound_segments,
                        max_connection_layover_seconds,
                    ) or self._exceeds_connection_layover_limit(
                        inbound_segments,
                        max_connection_layover_seconds,
                    ):
                        filtered_by_connection_layover += 1
                        continue

                    out_transfers = self._transfer_airports(outbound_segments)
                    in_transfers = self._transfer_airports(inbound_segments)
                    outbound_time_to_destination_seconds = direct_trip_meta[
                        "outbound_duration_seconds"
                    ]
                    inbound_time_to_origin_seconds = direct_trip_meta["inbound_duration_seconds"]
                    outbound_leg_source = str(direct_trip_meta["outbound_source"])
                    outbound_leg_destination = str(direct_trip_meta["outbound_destination"])
                    inbound_leg_source = str(direct_trip_meta["inbound_source"])
                    inbound_leg_destination = str(direct_trip_meta["inbound_destination"])
                    if not self._leg_matches_expected_route(
                        outbound_leg_source,
                        outbound_leg_destination,
                        candidate["origin"],
                        candidate["destination"],
                    ) or not self._leg_matches_expected_route(
                        inbound_leg_source,
                        inbound_leg_destination,
                        candidate["destination"],
                        candidate["arrival_origin"],
                    ):
                        continue
                    roundtrip_url = kiwi_return_url(
                        outbound_leg_source,
                        outbound_leg_destination,
                        candidate["depart_origin_date"],
                        candidate["return_origin_date"],
                        config.max_stops_per_leg,
                    )
                    booking_url = direct_trip.get("booking_url") or roundtrip_url

                    legs = [
                        {
                            "source": outbound_leg_source,
                            "destination": outbound_leg_destination,
                            "date": candidate["depart_origin_date"],
                            "price": None,
                            "formatted_price": "Part of round-trip fare",
                            "stops": direct_trip["outbound_stops"],
                            "segments": outbound_segments,
                            "duration_seconds": outbound_time_to_destination_seconds,
                            "departure_local": (
                                outbound_segments[0].get("depart_local")
                                if outbound_segments
                                else None
                            ),
                            "arrival_local": (
                                outbound_segments[-1].get("arrive_local")
                                if outbound_segments
                                else None
                            ),
                            "fare_mode": direct_trip.get("fare_mode", "selected_bags"),
                            "provider": direct_trip.get("provider", "kiwi"),
                            "price_mode": direct_trip.get("price_mode"),
                            "booking_url": booking_url,
                        },
                        {
                            "source": inbound_leg_source,
                            "destination": inbound_leg_destination,
                            "date": candidate["return_origin_date"],
                            "price": None,
                            "formatted_price": "Part of round-trip fare",
                            "stops": direct_trip["inbound_stops"],
                            "segments": inbound_segments,
                            "duration_seconds": inbound_time_to_origin_seconds,
                            "departure_local": (
                                inbound_segments[0].get("depart_local")
                                if inbound_segments
                                else None
                            ),
                            "arrival_local": (
                                inbound_segments[-1].get("arrive_local")
                                if inbound_segments
                                else None
                            ),
                            "fare_mode": direct_trip.get("fare_mode", "selected_bags"),
                            "provider": direct_trip.get("provider", "kiwi"),
                            "price_mode": direct_trip.get("price_mode"),
                            "booking_url": booking_url,
                        },
                    ]
                    per_adult_price = (
                        round(total_price / max(1, int(config.passengers.adults)), 2)
                        if total_price is not None
                        else None
                    )
                    direct_price_mode = str(direct_trip.get("price_mode") or "").strip()

                    all_results.append(
                        {
                            "result_id": (
                                f"{destination}|direct|{candidate['origin']}|"
                                f"{candidate['depart_origin_date']}|{candidate['return_origin_date']}"
                            ),
                            "itinerary_type": "direct_roundtrip",
                            "destination_code": destination,
                            "destination_name": destination_name,
                            "destination_note": notes.get("note"),
                            "total_price": total_price,
                            "passengers_adults": int(config.passengers.adults),
                            "price_per_adult": per_adult_price,
                            "price_modes": [direct_price_mode] if direct_price_mode else [],
                            "currency": config.currency,
                            "formatted_total_price": direct_trip["formatted_price"],
                            "price_per_1000_km": price_per_1000_km,
                            "distance_km": (
                                round(distance_basis_km, 1) if distance_basis_km else None
                            ),
                            "distance_basis": "direct_origin_to_destination",
                            "score": score,
                            "outbound_time_to_destination_seconds": outbound_time_to_destination_seconds,
                            "inbound_time_to_origin_seconds": inbound_time_to_origin_seconds,
                            "objective": config.objective,
                            "provider": direct_trip.get("provider", "kiwi"),
                            "outbound": {
                                "origin": candidate["origin"],
                                "hub": "/".join(out_transfers) if out_transfers else "DIRECT",
                                "transfer_airports": out_transfers,
                                "date_from_origin": candidate["depart_origin_date"],
                                "date_to_destination": candidate["depart_origin_date"],
                                "stopover_days": 0,
                                "layovers_count": outbound_layovers,
                                "provider": direct_trip.get("provider", "kiwi"),
                            },
                            "fare_mode": direct_trip.get("fare_mode", "selected_bags"),
                            "main_destination_stay_days": candidate["main_stay_days"],
                            "inbound": {
                                "hub": "/".join(in_transfers) if in_transfers else "DIRECT",
                                "transfer_airports": in_transfers,
                                "arrival_origin": candidate["arrival_origin"],
                                "date_from_destination": candidate["return_origin_date"],
                                "date_to_origin": candidate["return_origin_date"],
                                "stopover_days": 0,
                                "layovers_count": inbound_layovers,
                                "provider": direct_trip.get("provider", "kiwi"),
                            },
                            "comparison_links": cached_comparison_links(
                                candidate["origin"],
                                candidate["destination"],
                                candidate["depart_origin_date"],
                                candidate["return_origin_date"],
                            ),
                            "legs": legs,
                            "risk_notes": [
                                "Standard round-trip pricing can be lower than 2 separate one-ways.",
                                "Baggage and fare rules can differ by operating carrier.",
                            ],
                        }
                    )
                    continue

                inner_return_plan = candidate.get("_inner_return_plan")
                if inner_return_plan is None:
                    inner_return_plan = self._candidate_inner_return_plan(candidate)
                if inner_return_plan is not None:
                    bundled_result = self._build_split_candidate_with_inner_return_bundle(
                        candidate=candidate,
                        inner_return_plan=inner_return_plan,
                        oneway_map=oneway_map,
                        return_map=return_map,
                        config=config,
                        distance_basis_km=distance_basis_km,
                        max_connection_layover_seconds=max_connection_layover_seconds,
                        destination_name=destination_name,
                        notes=notes,
                        oneway_entry_cache=oneway_entry_cache,
                        comparison_links=cached_comparison_links(
                            candidate["origin"],
                            candidate["destination"],
                            candidate["depart_origin_date"],
                            candidate["leave_destination_date"],
                        ),
                    )
                    if bundled_result is not None:
                        all_results.append(bundled_result)
                        continue

                if candidate_type == "split_chain":
                    split_plans = candidate.get("_split_plans")
                    if split_plans is None:
                        split_plans = self._candidate_split_plans(candidate)
                    if not split_plans:
                        continue

                    outbound_leg_entries = self._materialize_oneway_plan_entries(
                        list(split_plans["outbound_plan"]),
                        oneway_map,
                        oneway_entry_cache,
                    )
                    inbound_leg_entries = self._materialize_oneway_plan_entries(
                        list(split_plans["inbound_plan"]),
                        oneway_map,
                        oneway_entry_cache,
                    )
                    if outbound_leg_entries is None or inbound_leg_entries is None:
                        continue

                    outbound_boundary_days = list(split_plans["outbound_boundary_days"])
                    inbound_boundary_days = list(split_plans["inbound_boundary_days"])
                    while len(outbound_boundary_days) < max(0, len(outbound_leg_entries) - 1):
                        outbound_boundary_days.append(0)
                    while len(inbound_boundary_days) < max(0, len(inbound_leg_entries) - 1):
                        inbound_boundary_days.append(0)

                    invalid_boundary = False
                    chain_filtered_by_connection = False
                    outbound_boundary_events = 0
                    inbound_boundary_events = 0

                    all_chain_entries = [*outbound_leg_entries, *inbound_leg_entries]
                    if max_connection_layover_seconds is not None:
                        if any(
                            self._exceeds_connection_layover_limit(
                                entry.get("segments") or [],
                                max_connection_layover_seconds,
                            )
                            for entry in all_chain_entries
                        ):
                            filtered_by_connection_layover += 1
                            continue

                    for idx in range(len(outbound_leg_entries) - 1):
                        current_entry = outbound_leg_entries[idx]
                        next_entry = outbound_leg_entries[idx + 1]
                        current_segments = current_entry.get("segments") or []
                        next_segments = next_entry.get("segments") or []
                        if not current_segments or not next_segments:
                            invalid_boundary = True
                            break
                        gap_seconds = connection_gap_seconds(
                            current_segments[-1].get("arrive_local"),
                            next_segments[0].get("depart_local"),
                        )
                        min_boundary = minimum_split_boundary_connection_seconds(
                            current_entry.get("destination") or "",
                            next_entry.get("source") or "",
                        )
                        if gap_seconds is None or gap_seconds < min_boundary:
                            invalid_boundary = True
                            break
                        boundary_days = int(outbound_boundary_days[idx] or 0)
                        if (
                            max_connection_layover_seconds is not None
                            and boundary_days <= 0
                            and gap_seconds > max_connection_layover_seconds
                        ):
                            chain_filtered_by_connection = True
                            break
                        outbound_boundary_events += boundary_transfer_events(
                            current_entry.get("destination") or "",
                            next_entry.get("source") or "",
                        )
                    if chain_filtered_by_connection:
                        filtered_by_connection_layover += 1
                        continue
                    if invalid_boundary:
                        filtered_invalid_split_boundaries += 1
                        continue

                    for idx in range(len(inbound_leg_entries) - 1):
                        current_entry = inbound_leg_entries[idx]
                        next_entry = inbound_leg_entries[idx + 1]
                        current_segments = current_entry.get("segments") or []
                        next_segments = next_entry.get("segments") or []
                        if not current_segments or not next_segments:
                            invalid_boundary = True
                            break
                        gap_seconds = connection_gap_seconds(
                            current_segments[-1].get("arrive_local"),
                            next_segments[0].get("depart_local"),
                        )
                        min_boundary = minimum_split_boundary_connection_seconds(
                            current_entry.get("destination") or "",
                            next_entry.get("source") or "",
                        )
                        if gap_seconds is None or gap_seconds < min_boundary:
                            invalid_boundary = True
                            break
                        boundary_days = int(inbound_boundary_days[idx] or 0)
                        if (
                            max_connection_layover_seconds is not None
                            and boundary_days <= 0
                            and gap_seconds > max_connection_layover_seconds
                        ):
                            chain_filtered_by_connection = True
                            break
                        inbound_boundary_events += boundary_transfer_events(
                            current_entry.get("destination") or "",
                            next_entry.get("source") or "",
                        )
                    if chain_filtered_by_connection:
                        filtered_by_connection_layover += 1
                        continue
                    if invalid_boundary:
                        filtered_invalid_split_boundaries += 1
                        continue

                    outbound_layovers = (
                        sum(
                            int(
                                entry.get("fare", {}).get("transfer_events", entry.get("stops", 0))
                                or 0
                            )
                            for entry in outbound_leg_entries
                        )
                        + outbound_boundary_events
                    )
                    inbound_layovers = (
                        sum(
                            int(
                                entry.get("fare", {}).get("transfer_events", entry.get("stops", 0))
                                or 0
                            )
                            for entry in inbound_leg_entries
                        )
                        + inbound_boundary_events
                    )
                    if (
                        outbound_layovers > config.max_layovers_per_direction
                        or inbound_layovers > config.max_layovers_per_direction
                    ):
                        continue

                    total_price = sum(
                        int(entry.get("fare", {}).get("price") or 0) for entry in all_chain_entries
                    )
                    score = self._score_candidate(
                        total_price,
                        distance_basis_km,
                        config.objective,
                    )
                    price_per_1000_km = (
                        round((total_price / distance_basis_km) * 1000.0, 1)
                        if distance_basis_km and distance_basis_km > 0
                        else None
                    )

                    outbound_durations = [
                        entry.get("duration_seconds") for entry in outbound_leg_entries
                    ]
                    inbound_durations = [
                        entry.get("duration_seconds") for entry in inbound_leg_entries
                    ]
                    outbound_time_to_destination_seconds = None
                    inbound_time_to_origin_seconds = None
                    if all(value is not None for value in outbound_durations):
                        outbound_time_to_destination_seconds = int(
                            sum(int(v or 0) for v in outbound_durations)
                        )
                        outbound_time_to_destination_seconds += (
                            int(sum(int(v or 0) for v in outbound_boundary_days)) * SECONDS_PER_DAY
                        )
                    if all(value is not None for value in inbound_durations):
                        inbound_time_to_origin_seconds = int(
                            sum(int(v or 0) for v in inbound_durations)
                        )
                        inbound_time_to_origin_seconds += (
                            int(sum(int(v or 0) for v in inbound_boundary_days)) * SECONDS_PER_DAY
                        )

                    legs: list[dict[str, Any]] = []
                    for entry in all_chain_entries:
                        fare = entry.get("fare") or {}
                        segments = entry.get("segments") or []
                        legs.append(
                            {
                                "source": entry.get("source"),
                                "destination": entry.get("destination"),
                                "date": entry.get("date"),
                                "price": fare.get("price"),
                                "formatted_price": fare.get("formatted_price"),
                                "stops": fare.get("stops"),
                                "segments": segments,
                                "duration_seconds": entry.get("duration_seconds"),
                                "departure_local": (
                                    segments[0].get("depart_local") if segments else None
                                ),
                                "arrival_local": (
                                    segments[-1].get("arrive_local") if segments else None
                                ),
                                "fare_mode": fare.get("fare_mode", "selected_bags"),
                                "provider": fare.get("provider", "kiwi"),
                                "price_mode": fare.get("price_mode"),
                                "booking_url": fare.get("booking_url")
                                or kiwi_oneway_url(
                                    str(entry.get("source") or ""),
                                    str(entry.get("destination") or ""),
                                    str(entry.get("date") or ""),
                                    config.max_stops_per_leg,
                                ),
                            }
                        )

                    fare_mode = "selected_bags"
                    if any(
                        str((entry.get("fare") or {}).get("fare_mode") or "") == "base_no_bags"
                        for entry in all_chain_entries
                    ):
                        fare_mode = "base_no_bags"
                    price_modes = sorted(
                        {
                            str((entry.get("fare") or {}).get("price_mode") or "").strip()
                            for entry in all_chain_entries
                            if str((entry.get("fare") or {}).get("price_mode") or "").strip()
                        }
                    )
                    outbound_hubs_chain = [
                        str(entry.get("destination") or "") for entry in outbound_leg_entries[:-1]
                    ]
                    inbound_hubs_chain = [
                        str(entry.get("destination") or "") for entry in inbound_leg_entries[:-1]
                    ]

                    all_results.append(
                        {
                            "result_id": (
                                f"{destination}|splitchain|{candidate.get('origin')}|"
                                f"{candidate.get('arrival_origin')}|"
                                f"{candidate.get('depart_origin_date')}|{candidate.get('return_origin_date')}|"
                                f"{candidate.get('outbound_hub')}|{candidate.get('inbound_hub')}"
                            ),
                            "itinerary_type": "split_stopover",
                            "destination_code": destination,
                            "destination_name": destination_name,
                            "destination_note": notes.get("note"),
                            "total_price": total_price,
                            "passengers_adults": int(config.passengers.adults),
                            "price_per_adult": round(
                                total_price / max(1, int(config.passengers.adults)), 2
                            ),
                            "price_modes": price_modes,
                            "currency": config.currency,
                            "price_per_1000_km": price_per_1000_km,
                            "distance_km": (
                                round(distance_basis_km, 1) if distance_basis_km else None
                            ),
                            "distance_basis": "direct_origin_to_destination",
                            "score": score,
                            "outbound_time_to_destination_seconds": outbound_time_to_destination_seconds,
                            "inbound_time_to_origin_seconds": inbound_time_to_origin_seconds,
                            "objective": config.objective,
                            "provider": (outbound_leg_entries[0].get("fare") or {}).get(
                                "provider", "kiwi"
                            ),
                            "outbound": {
                                "origin": candidate["origin"],
                                "hub": "/".join(outbound_hubs_chain),
                                "date_from_origin": candidate["depart_origin_date"],
                                "date_to_destination": candidate["depart_destination_date"],
                                "stopover_days": int(candidate.get("outbound_stopover_days") or 0),
                                "layovers_count": outbound_layovers,
                                "fare_mode": fare_mode,
                                "provider": (outbound_leg_entries[0].get("fare") or {}).get(
                                    "provider", "kiwi"
                                ),
                            },
                            "fare_mode": fare_mode,
                            "main_destination_stay_days": candidate["main_stay_days"],
                            "inbound": {
                                "hub": "/".join(inbound_hubs_chain),
                                "arrival_origin": candidate["arrival_origin"],
                                "date_from_destination": candidate["leave_destination_date"],
                                "date_to_origin": candidate["return_origin_date"],
                                "stopover_days": int(candidate.get("inbound_stopover_days") or 0),
                                "layovers_count": inbound_layovers,
                                "fare_mode": fare_mode,
                                "provider": (inbound_leg_entries[0].get("fare") or {}).get(
                                    "provider", "kiwi"
                                ),
                            },
                            "comparison_links": cached_comparison_links(
                                candidate["origin"],
                                candidate["destination"],
                                candidate["depart_origin_date"],
                                candidate["leave_destination_date"],
                            ),
                            "legs": legs,
                            "risk_notes": [
                                "Split-chain tickets: self-transfer and missed-connection risk applies.",
                                "Baggage and fare rules can differ per leg and airline.",
                            ],
                        }
                    )
                    continue

                split_plans = candidate.get("_split_plans")
                if split_plans is None:
                    split_plans = self._candidate_split_plans(candidate)
                if not split_plans:
                    continue

                all_plan_entries = self._materialize_oneway_plan_entries(
                    list(split_plans["outbound_plan"]) + list(split_plans["inbound_plan"]),
                    oneway_map,
                    oneway_entry_cache,
                )
                if all_plan_entries is None or len(all_plan_entries) != 4:
                    continue

                leg1_entry, leg2_entry, leg3_entry, leg4_entry = all_plan_entries
                leg1 = leg1_entry["fare"]
                leg2 = leg2_entry["fare"]
                leg3 = leg3_entry["fare"]
                leg4 = leg4_entry["fare"]
                leg1_duration = leg1_entry["duration_seconds"]
                leg2_duration = leg2_entry["duration_seconds"]
                leg3_duration = leg3_entry["duration_seconds"]
                leg4_duration = leg4_entry["duration_seconds"]
                leg1_segments = leg1_entry["segments"]
                leg2_segments = leg2_entry["segments"]
                leg3_segments = leg3_entry["segments"]
                leg4_segments = leg4_entry["segments"]
                leg1_source = str(leg1_entry["source"])
                leg1_destination = str(leg1_entry["destination"])
                leg2_source = str(leg2_entry["source"])
                leg2_destination = str(leg2_entry["destination"])
                leg3_source = str(leg3_entry["source"])
                leg3_destination = str(leg3_entry["destination"])
                leg4_source = str(leg4_entry["source"])
                leg4_destination = str(leg4_entry["destination"])
                outbound_boundary_gap = connection_gap_seconds(
                    leg1_segments[-1].get("arrive_local") if leg1_segments else None,
                    leg2_segments[0].get("depart_local") if leg2_segments else None,
                )
                inbound_boundary_gap = connection_gap_seconds(
                    leg3_segments[-1].get("arrive_local") if leg3_segments else None,
                    leg4_segments[0].get("depart_local") if leg4_segments else None,
                )
                min_outbound_boundary_seconds = minimum_split_boundary_connection_seconds(
                    leg1_destination,
                    leg2_source,
                )
                min_inbound_boundary_seconds = minimum_split_boundary_connection_seconds(
                    leg3_destination,
                    leg4_source,
                )
                if (
                    outbound_boundary_gap is None
                    or outbound_boundary_gap < min_outbound_boundary_seconds
                    or inbound_boundary_gap is None
                    or inbound_boundary_gap < min_inbound_boundary_seconds
                ):
                    filtered_invalid_split_boundaries += 1
                    continue
                if max_connection_layover_seconds is not None:
                    if (
                        self._exceeds_connection_layover_limit(
                            leg1_segments,
                            max_connection_layover_seconds,
                        )
                        or self._exceeds_connection_layover_limit(
                            leg2_segments,
                            max_connection_layover_seconds,
                        )
                        or self._exceeds_connection_layover_limit(
                            leg3_segments,
                            max_connection_layover_seconds,
                        )
                        or self._exceeds_connection_layover_limit(
                            leg4_segments,
                            max_connection_layover_seconds,
                        )
                    ):
                        filtered_by_connection_layover += 1
                        continue

                    if int(candidate["outbound_stopover_days"]) <= 0:
                        if outbound_boundary_gap > max_connection_layover_seconds:
                            filtered_by_connection_layover += 1
                            continue

                    if int(candidate["inbound_stopover_days"]) <= 0:
                        if inbound_boundary_gap > max_connection_layover_seconds:
                            filtered_by_connection_layover += 1
                            continue

                outbound_boundary_events = boundary_transfer_events(
                    leg1_destination,
                    leg2_source,
                )
                inbound_boundary_events = boundary_transfer_events(
                    leg3_destination,
                    leg4_source,
                )
                outbound_layovers = (
                    int(leg1.get("transfer_events", leg1["stops"]))
                    + int(leg2.get("transfer_events", leg2["stops"]))
                    + outbound_boundary_events
                )
                inbound_layovers = (
                    int(leg3.get("transfer_events", leg3["stops"]))
                    + int(leg4.get("transfer_events", leg4["stops"]))
                    + inbound_boundary_events
                )

                if (
                    outbound_layovers > config.max_layovers_per_direction
                    or inbound_layovers > config.max_layovers_per_direction
                ):
                    continue

                total_price = leg1["price"] + leg2["price"] + leg3["price"] + leg4["price"]
                score = self._score_candidate(
                    total_price,
                    distance_basis_km,
                    config.objective,
                )
                price_per_1000_km = (
                    round((total_price / distance_basis_km) * 1000.0, 1)
                    if distance_basis_km and distance_basis_km > 0
                    else None
                )
                outbound_time_to_destination_seconds = None
                inbound_time_to_origin_seconds = None
                if leg1_duration is not None and leg2_duration is not None:
                    outbound_time_to_destination_seconds = (
                        leg1_duration
                        + leg2_duration
                        + (int(candidate["outbound_stopover_days"]) * SECONDS_PER_DAY)
                    )
                if leg3_duration is not None and leg4_duration is not None:
                    inbound_time_to_origin_seconds = (
                        leg3_duration
                        + leg4_duration
                        + (int(candidate["inbound_stopover_days"]) * SECONDS_PER_DAY)
                    )

                legs = [
                    {
                        "source": leg1_source,
                        "destination": leg1_destination,
                        "date": candidate["depart_origin_date"],
                        "price": leg1["price"],
                        "formatted_price": leg1["formatted_price"],
                        "stops": leg1["stops"],
                        "segments": leg1_segments,
                        "duration_seconds": leg1_duration,
                        "departure_local": (
                            leg1_segments[0].get("depart_local") if leg1_segments else None
                        ),
                        "arrival_local": (
                            leg1_segments[-1].get("arrive_local") if leg1_segments else None
                        ),
                        "fare_mode": leg1.get("fare_mode", "selected_bags"),
                        "provider": leg1.get("provider", "kiwi"),
                        "price_mode": leg1.get("price_mode"),
                        "booking_url": leg1.get("booking_url")
                        or kiwi_oneway_url(
                            leg1_source,
                            leg1_destination,
                            candidate["depart_origin_date"],
                            config.max_stops_per_leg,
                        ),
                    },
                    {
                        "source": leg2_source,
                        "destination": leg2_destination,
                        "date": candidate["depart_destination_date"],
                        "price": leg2["price"],
                        "formatted_price": leg2["formatted_price"],
                        "stops": leg2["stops"],
                        "segments": leg2_segments,
                        "duration_seconds": leg2_duration,
                        "departure_local": (
                            leg2_segments[0].get("depart_local") if leg2_segments else None
                        ),
                        "arrival_local": (
                            leg2_segments[-1].get("arrive_local") if leg2_segments else None
                        ),
                        "fare_mode": leg2.get("fare_mode", "selected_bags"),
                        "provider": leg2.get("provider", "kiwi"),
                        "price_mode": leg2.get("price_mode"),
                        "booking_url": leg2.get("booking_url")
                        or kiwi_oneway_url(
                            leg2_source,
                            leg2_destination,
                            candidate["depart_destination_date"],
                            config.max_stops_per_leg,
                        ),
                    },
                    {
                        "source": leg3_source,
                        "destination": leg3_destination,
                        "date": candidate["leave_destination_date"],
                        "price": leg3["price"],
                        "formatted_price": leg3["formatted_price"],
                        "stops": leg3["stops"],
                        "segments": leg3_segments,
                        "duration_seconds": leg3_duration,
                        "departure_local": (
                            leg3_segments[0].get("depart_local") if leg3_segments else None
                        ),
                        "arrival_local": (
                            leg3_segments[-1].get("arrive_local") if leg3_segments else None
                        ),
                        "fare_mode": leg3.get("fare_mode", "selected_bags"),
                        "provider": leg3.get("provider", "kiwi"),
                        "price_mode": leg3.get("price_mode"),
                        "booking_url": leg3.get("booking_url")
                        or kiwi_oneway_url(
                            leg3_source,
                            leg3_destination,
                            candidate["leave_destination_date"],
                            config.max_stops_per_leg,
                        ),
                    },
                    {
                        "source": leg4_source,
                        "destination": leg4_destination,
                        "date": candidate["return_origin_date"],
                        "price": leg4["price"],
                        "formatted_price": leg4["formatted_price"],
                        "stops": leg4["stops"],
                        "segments": leg4_segments,
                        "duration_seconds": leg4_duration,
                        "departure_local": (
                            leg4_segments[0].get("depart_local") if leg4_segments else None
                        ),
                        "arrival_local": (
                            leg4_segments[-1].get("arrive_local") if leg4_segments else None
                        ),
                        "fare_mode": leg4.get("fare_mode", "selected_bags"),
                        "provider": leg4.get("provider", "kiwi"),
                        "price_mode": leg4.get("price_mode"),
                        "booking_url": leg4.get("booking_url")
                        or kiwi_oneway_url(
                            leg4_source,
                            leg4_destination,
                            candidate["return_origin_date"],
                            config.max_stops_per_leg,
                        ),
                    },
                ]

                all_results.append(
                    {
                        "result_id": (
                            f"{destination}|split|{candidate['origin']}|{candidate['outbound_hub']}|"
                            f"{candidate['inbound_hub']}|{candidate['depart_origin_date']}|"
                            f"{candidate['return_origin_date']}"
                        ),
                        "itinerary_type": "split_stopover",
                        "destination_code": destination,
                        "destination_name": destination_name,
                        "destination_note": notes.get("note"),
                        "total_price": total_price,
                        "passengers_adults": int(config.passengers.adults),
                        "price_per_adult": round(
                            total_price / max(1, int(config.passengers.adults)), 2
                        ),
                        "price_modes": sorted(
                            {
                                str(mode).strip()
                                for mode in (
                                    leg1.get("price_mode"),
                                    leg2.get("price_mode"),
                                    leg3.get("price_mode"),
                                    leg4.get("price_mode"),
                                )
                                if str(mode or "").strip()
                            }
                        ),
                        "currency": config.currency,
                        "price_per_1000_km": price_per_1000_km,
                        "distance_km": (round(distance_basis_km, 1) if distance_basis_km else None),
                        "distance_basis": "direct_origin_to_destination",
                        "score": score,
                        "outbound_time_to_destination_seconds": outbound_time_to_destination_seconds,
                        "inbound_time_to_origin_seconds": inbound_time_to_origin_seconds,
                        "objective": config.objective,
                        "provider": leg1.get("provider", "kiwi"),
                        "outbound": {
                            "origin": candidate["origin"],
                            "hub": candidate["outbound_hub"],
                            "date_from_origin": candidate["depart_origin_date"],
                            "date_to_destination": candidate["depart_destination_date"],
                            "stopover_days": candidate["outbound_stopover_days"],
                            "layovers_count": outbound_layovers,
                            "fare_mode": leg1.get("fare_mode", "selected_bags"),
                            "provider": leg1.get("provider", "kiwi"),
                        },
                        "main_destination_stay_days": candidate["main_stay_days"],
                        "inbound": {
                            "hub": candidate["inbound_hub"],
                            "arrival_origin": candidate["arrival_origin"],
                            "date_from_destination": candidate["leave_destination_date"],
                            "date_to_origin": candidate["return_origin_date"],
                            "stopover_days": candidate["inbound_stopover_days"],
                            "layovers_count": inbound_layovers,
                            "fare_mode": leg3.get("fare_mode", "selected_bags"),
                            "provider": leg3.get("provider", "kiwi"),
                        },
                        "comparison_links": cached_comparison_links(
                            candidate["origin"],
                            candidate["destination"],
                            candidate["depart_origin_date"],
                            candidate["leave_destination_date"],
                        ),
                        "legs": legs,
                        "risk_notes": [
                            "Split tickets: self-transfer and missed-connection risk applies.",
                            "Baggage and fare rules can differ per leg and airline.",
                        ],
                    }
                )
            if progress is not None:
                completed_build_candidates += candidate_total
                progress.advance_phase(
                    "build",
                    completed=completed_build_candidates,
                    total=max(1, total_build_candidates),
                    detail=(
                        f"{destination}: assembled {candidate_total}/{candidate_total} "
                        "candidate itinerary checks."
                    ),
                )

        if progress is not None:
            if returns_phase_started:
                progress.complete_phase(
                    "returns",
                    detail=f"Round-trip validation complete: {total_return_trips} fare keys checked.",
                )
            else:
                progress.start_phase(
                    "returns",
                    total=1,
                    detail="No round-trip fare validation was needed.",
                )
                progress.complete_phase(
                    "returns",
                    detail="No round-trip fare validation was needed.",
                )
            if oneways_phase_started:
                progress.complete_phase(
                    "oneways",
                    detail=f"One-way validation complete: {total_oneway_legs} fare keys checked.",
                )
            else:
                progress.start_phase(
                    "oneways",
                    total=1,
                    detail="No one-way fare validation was needed.",
                )
                progress.complete_phase(
                    "oneways",
                    detail="No one-way fare validation was needed.",
                )
            if build_phase_started:
                progress.complete_phase(
                    "build",
                    detail=f"Itinerary assembly complete for {destinations_with_candidates} destinations.",
                )
            else:
                progress.start_phase(
                    "build",
                    total=1,
                    detail="No itinerary assembly was needed.",
                )
                progress.complete_phase(
                    "build",
                    detail="No itinerary assembly was needed.",
                )
            progress.start_phase(
                "finalize",
                total=1,
                detail="Ranking final results and packaging the response.",
            )
        all_results, dominated_removed = self._prune_dominated_split_results(all_results)
        if dominated_removed > 0:
            warnings.append(
                "Pruned "
                f"{dominated_removed} split-stopover itineraries dominated by cheaper/equally-fast direct options."
            )
        long_stopover_count = sum(
            1
            for item in all_results
            if item.get("itinerary_type") == "split_stopover"
            and (
                int((item.get("outbound") or {}).get("stopover_days") or 0) > 0
                or int((item.get("inbound") or {}).get("stopover_days") or 0) > 0
            )
        )
        if config.max_stopover_days >= 1 and long_stopover_count == 0:
            warnings.append(
                "No long-stopover (>24h) fares survived ranking for current constraints/time window."
            )
        if config.max_connection_layover_hours and filtered_by_connection_layover > 0:
            warnings.append(
                "Filtered "
                f"{filtered_by_connection_layover} itineraries exceeding max connection layover "
                f"of {config.max_connection_layover_hours}h."
            )
        if filtered_invalid_split_boundaries > 0:
            min_same_h = round(
                MIN_SPLIT_CONNECTION_SAME_AIRPORT_SECONDS / SECONDS_PER_HOUR,
                1,
            )
            min_cross_h = round(
                MIN_SPLIT_CONNECTION_CROSS_AIRPORT_SECONDS / SECONDS_PER_HOUR,
                1,
            )
            warnings.append(
                "Filtered "
                f"{filtered_invalid_split_boundaries} split itineraries with invalid or too-short self-transfer boundaries "
                f"(min {min_same_h}h same-airport, {min_cross_h}h cross-airport)."
            )
        if config.market_compare_fares and (
            base_fare_selected_oneways or base_fare_selected_returns
        ):
            warnings.append(
                "Market compare mode also fetched Kiwi base no-bag fares for "
                f"{base_fare_selected_returns} round-trips and "
                f"{base_fare_selected_oneways} one-way legs, keeping them when cheaper "
                "than the selected baggage fare."
            )

        if (config.passengers.hand_bags > 0 or config.passengers.hold_bags > 0) and any(
            provider_id != "kiwi" for provider_id in search_client.active_provider_ids
        ):
            warnings.append(
                "Baggage pricing note: Kiwi accounts for cabin/hold bag add-ons in prices. "
                "Other providers may return base fares without bag fees."
            )

        if config.objective == "best":
            self._compute_best_value_scores(all_results)
            all_results.sort(
                key=lambda item: (
                    item.get("best_value_score", float("inf")),
                    item["total_price"],
                    (
                        item.get("outbound_time_to_destination_seconds")
                        if item.get("outbound_time_to_destination_seconds") is not None
                        else float("inf")
                    ),
                )
            )
        elif config.objective == "cheapest":
            all_results.sort(
                key=lambda item: (
                    item["total_price"],
                    (
                        item.get("outbound_time_to_destination_seconds")
                        if item.get("outbound_time_to_destination_seconds") is not None
                        else float("inf")
                    ),
                    item["score"],
                )
            )
        elif config.objective == "fastest":
            all_results.sort(
                key=lambda item: (
                    (
                        item.get("outbound_time_to_destination_seconds")
                        if item.get("outbound_time_to_destination_seconds") is not None
                        else float("inf")
                    ),
                    item["total_price"],
                    item["score"],
                )
            )
        else:
            all_results.sort(
                key=lambda item: (
                    item["score"],
                    item["total_price"],
                    (
                        item.get("outbound_time_to_destination_seconds")
                        if item.get("outbound_time_to_destination_seconds") is not None
                        else float("inf")
                    ),
                )
            )

        def itinerary_uses_provider(item: dict[str, Any], provider_id: str) -> bool:
            normalized = str(provider_id or "").strip().lower()
            if not normalized:
                return False
            providers = {
                str(item.get("provider") or "").strip().lower(),
                str((item.get("outbound") or {}).get("provider") or "").strip().lower(),
                str((item.get("inbound") or {}).get("provider") or "").strip().lower(),
            }
            for leg in item.get("legs") or []:
                providers.add(str((leg or {}).get("provider") or "").strip().lower())
            providers.discard("")
            return normalized in providers

        required_results_by_destination: dict[str, list[dict[str, Any]]] = {}
        if (
            "kiwi" in search_client.active_provider_ids
            and len(search_client.active_provider_ids) > 1
        ):
            grouped_for_floor: dict[str, list[dict[str, Any]]] = {}
            for item in all_results:
                code = str(item.get("destination_code") or "").upper().strip()
                if not code:
                    continue
                grouped_for_floor.setdefault(code, []).append(item)
            for code, items in grouped_for_floor.items():
                if not items:
                    continue
                cheapest_overall = min(
                    items,
                    key=lambda entry: (
                        int(entry.get("total_price") or PRICE_SENTINEL),
                        int(entry.get("outbound_time_to_destination_seconds") or PRICE_SENTINEL),
                    ),
                )
                required: list[dict[str, Any]] = [cheapest_overall]
                kiwi_items = [entry for entry in items if itinerary_uses_provider(entry, "kiwi")]
                if kiwi_items:
                    cheapest_kiwi = min(
                        kiwi_items,
                        key=lambda entry: (
                            int(entry.get("total_price") or PRICE_SENTINEL),
                            int(
                                entry.get("outbound_time_to_destination_seconds") or PRICE_SENTINEL
                            ),
                        ),
                    )
                    if str(cheapest_kiwi.get("result_id") or "") != str(
                        cheapest_overall.get("result_id") or ""
                    ):
                        required.append(cheapest_kiwi)
                required_results_by_destination[code] = required

        trimmed, per_destination_counts = self._cap_results_per_destination(
            all_results,
            config.top_results,
            config.destinations,
            required_by_destination=required_results_by_destination or None,
        )

        provider_stats = search_client.stats_snapshot()
        provider_health = (
            search_client.health_snapshot()
            if hasattr(search_client, "health_snapshot")
            else {
                "providers": {},
                "budget": dict(provider_stats.get("budget") or {}),
            }
        )
        provider_health_entries = provider_health.get("providers") or {}
        if progress is not None:
            progress.set_runtime_data("provider_health", provider_health)
        provider_error_messages: list[str] = []
        for provider_id, entry in provider_health_entries.items():
            blocked_count = int((entry or {}).get("blocked") or 0)
            if blocked_count <= 0:
                continue
            blocked_message = (
                f"Provider {provider_id} hit anti-bot protection on {blocked_count} live checks."
            )
            cooldown_seconds = int((entry or {}).get("cooldown_seconds") or 0)
            if cooldown_seconds > 0:
                blocked_message += f" Retry in ~{cooldown_seconds}s."
            if str((entry or {}).get("manual_search_url") or "").strip():
                blocked_message += " Manual provider search is available from provider health."
            provider_error_messages.append(blocked_message)
        for bucket in ("calendar_errors", "oneway_errors", "return_errors"):
            errors = provider_stats.get(bucket) or {}
            for provider_id, count in errors.items():
                if int(count) <= 0:
                    continue
                provider_error_messages.append(
                    f"Provider {provider_id} had {count} {bucket.replace('_', ' ')}."
                )
        for bucket in ("calendar_skipped_budget", "oneway_skipped_budget", "return_skipped_budget"):
            skipped = provider_stats.get(bucket) or {}
            for provider_id, count in skipped.items():
                if int(count) <= 0:
                    continue
                provider_error_messages.append(
                    f"Provider {provider_id} skipped {count} {bucket.replace('_', ' ')} due to API budget caps."
                )
        for bucket in (
            "calendar_skipped_cooldown",
            "oneway_skipped_cooldown",
            "return_skipped_cooldown",
        ):
            skipped = provider_stats.get(bucket) or {}
            for provider_id, count in skipped.items():
                if int(count) <= 0:
                    continue
                issue_type = str(
                    (provider_health_entries.get(provider_id) or {}).get("last_issue_type") or ""
                )
                if issue_type == "blocked":
                    provider_error_messages.append(
                        f"Provider {provider_id} temporarily paused after bot protection; "
                        f"skipped {count} {bucket.replace('_', ' ')} checks."
                    )
                else:
                    provider_error_messages.append(
                        f"Provider {provider_id} temporarily paused after runtime errors; "
                        f"skipped {count} {bucket.replace('_', ' ')} checks."
                    )
        for bucket in ("calendar_no_result", "oneway_no_result", "return_no_result"):
            no_results = provider_stats.get(bucket) or {}
            for provider_id, count in no_results.items():
                if int(count) <= 0:
                    continue
                provider_error_messages.append(
                    f"Provider {provider_id} returned no offers on {count} {bucket.replace('_', ' ')} checks."
                )
        if "serpapi" in search_client.active_provider_ids:
            serpapi_call_count = sum(
                int((provider_stats.get(bucket) or {}).get("serpapi", 0))
                for bucket in ("calendar_calls", "oneway_calls", "return_calls")
            )
            if serpapi_call_count == 0:
                provider_error_messages.append(
                    "Provider serpapi was active but got 0 live fare calls. "
                    "Increase SerpApi probes and/or caps."
                )
        warnings.extend(provider_error_messages)
        providers_used = sorted(
            {
                str(provider_id)
                for provider_id in (
                    list((provider_stats.get("calendar_selected") or {}).keys())
                    + list((provider_stats.get("oneway_selected") or {}).keys())
                    + list((provider_stats.get("return_selected") or {}).keys())
                    + [
                        str(leg.get("provider") or "")
                        for item in trimmed
                        for leg in (item.get("legs") or [])
                    ]
                    + [str(item.get("provider") or "") for item in trimmed]
                )
                if provider_id
            }
        )
        if not providers_used:
            providers_used = list(dict.fromkeys(search_client.active_provider_ids))

        elapsed = round(time.time() - start_ts, 2)
        if progress is not None:
            progress.complete_phase(
                "finalize",
                detail=f"Packed {len(trimmed)} result(s) in {elapsed}s.",
            )

        return {
            "meta": {
                "generated_at": dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z"),
                "currency": config.currency,
                "objective": config.objective,
                "price_per_km_basis": "direct origin->main destination distance",
                "ranking_mode": {
                    "best": "Best (72% normalized price + 28% normalized time-to-destination)",
                    "cheapest": "Cheapest (lowest total price)",
                    "fastest": "Fastest (shortest outbound time to destination)",
                    "price_per_km": "Price per 1000 km (direct origin->destination distance)",
                }.get(config.objective, config.objective),
                "results_count": len(trimmed),
                "elapsed_seconds": elapsed,
                "origins": list(config.origins),
                "destinations": list(config.destinations),
                "destination_display_names": {
                    destination: self._destination_display_name(destination)
                    for destination in config.destinations
                },
                "period_start": config.period_start.isoformat(),
                "period_end": config.period_end.isoformat(),
                "auto_discovered_hubs": chosen_hubs,
                "engine": {
                    "io_workers": config.io_workers,
                    "cpu_workers": config.cpu_workers,
                    "cpu_workers_auto": config.cpu_workers_auto,
                    "cpu_workers_available": self._available_cpu_workers(),
                    "exhaustive_hub_scan": config.exhaustive_hub_scan,
                    "calendar_hubs_prefetched": len(prefetch_hubs),
                    "calendar_hubs_prefetch_limit": config.calendar_hubs_prefetch,
                    "calendar_providers_active": list(calendar_provider_ids),
                    "calendar_routes_prefetched": len(routes),
                    "oneway_legs_requested": total_oneway_legs,
                    "roundtrip_itineraries_requested": total_return_trips,
                    "max_validate_oneway_keys_per_destination": (
                        config.max_validate_oneway_keys_per_destination
                    ),
                    "max_validate_return_keys_per_destination": (
                        config.max_validate_return_keys_per_destination
                    ),
                    "hub_candidates_count": len(config.hub_candidates),
                    "hub_candidates_input_count": int(
                        hub_resolution_meta.get("hub_candidates_input_count") or 0
                    ),
                    "hub_candidates_graph_count": int(
                        hub_resolution_meta.get("hub_candidates_graph_count") or 0
                    ),
                    "hub_candidates_graph_applied": bool(
                        hub_resolution_meta.get("hub_candidates_graph_applied")
                    ),
                    "hub_candidates_graph_available": bool(
                        hub_resolution_meta.get("hub_candidates_graph_available")
                    ),
                    "hub_candidates_graph_source": str(
                        hub_resolution_meta.get("hub_candidates_graph_source") or ""
                    ),
                    "providers_requested": list(config.provider_ids),
                    "providers_active": search_client.active_provider_ids,
                    "providers_used": providers_used,
                    "provider_status": provider_status,
                    "provider_stats": provider_stats,
                    "provider_health": provider_health,
                    "free_provider_discovery": list(free_provider_discovery_metadata.values()),
                    "coverage_audit": list(coverage_audit_metadata.values()),
                    "max_total_provider_calls": config.max_total_provider_calls,
                    "max_calls_kiwi": config.max_calls_kiwi,
                    "max_calls_amadeus": config.max_calls_amadeus,
                    "max_calls_serpapi": config.max_calls_serpapi,
                    "serpapi_probe_oneway_keys": config.serpapi_probe_oneway_keys,
                    "serpapi_probe_return_keys": config.serpapi_probe_return_keys,
                    "market_compare_fares": config.market_compare_fares,
                    "top_results_per_destination": config.top_results,
                    "results_count_by_destination": per_destination_counts,
                    "max_transfers_per_direction": config.max_transfers_per_direction,
                    "max_stops_per_leg": config.max_stops_per_leg,
                    "base_fare_selected_oneways": base_fare_selected_oneways,
                    "base_fare_selected_returns": base_fare_selected_returns,
                    "long_stopover_results": long_stopover_count,
                    "max_connection_layover_hours": config.max_connection_layover_hours,
                    "filtered_by_connection_layover": filtered_by_connection_layover,
                    "filtered_invalid_split_boundaries": filtered_invalid_split_boundaries,
                    "min_split_connection_same_airport_hours": round(
                        MIN_SPLIT_CONNECTION_SAME_AIRPORT_SECONDS / SECONDS_PER_HOUR,
                        2,
                    ),
                    "min_split_connection_cross_airport_hours": round(
                        MIN_SPLIT_CONNECTION_CROSS_AIRPORT_SECONDS / SECONDS_PER_HOUR,
                        2,
                    ),
                },
            },
            "warnings": warnings[-60:],
            "results": trimmed,
        }

    def search(
        self,
        config: SearchConfig,
        *,
        search_id: str | None = None,
        progress: SearchProgressTracker | None = None,
    ) -> dict[str, Any]:
        """Handle search.

        Args:
            config: Search configuration for the operation.
            search_id: Identifier of the current search.
            progress: Progress ratio for the current phase.

        Returns:
            dict[str, Any]: Handle search.
        """
        if not search_id:
            search_id = uuid.uuid4().hex[:12]
        wall_start = time.time()
        if progress is not None:
            progress.mark_running(SEARCH_EVENT_REQUEST_ACCEPTED)
        log_event(
            logging.INFO,
            "search_started",
            search_id=search_id,
            origins=list(config.origins),
            destinations=list(config.destinations),
            providers=list(config.provider_ids),
            io_workers=config.io_workers,
            cpu_workers=config.cpu_workers,
            timeout_seconds=(config.search_timeout_seconds or None),
            objective=config.objective,
            top_results=config.top_results,
            validate_top_per_destination=config.validate_top_per_destination,
            candidate_pool_multiplier=config.estimated_pool_multiplier,
        )
        io_pool = ThreadPoolExecutor(max_workers=config.io_workers)
        wait_for_shutdown = True
        try:
            search_coro = self._search_async(
                config,
                io_pool,
                search_id=search_id,
                progress=progress,
            )
            if config.search_timeout_seconds and config.search_timeout_seconds > 0:
                result = asyncio.run(
                    asyncio.wait_for(
                        search_coro,
                        timeout=config.search_timeout_seconds,
                    )
                )
            else:
                result = asyncio.run(search_coro)
        except TimeoutError as exc:
            wait_for_shutdown = False
            io_pool.shutdown(wait=False, cancel_futures=True)
            elapsed_seconds = round(time.time() - wall_start, 2)
            log_event(
                logging.WARNING,
                "search_timeout",
                search_id=search_id,
                elapsed_seconds=elapsed_seconds,
                timeout_seconds=config.search_timeout_seconds,
            )
            if progress is not None:
                progress.mark_failed(f"Search exceeded timeout ({config.search_timeout_seconds}s).")
            raise TimeoutError(
                f"Search exceeded timeout ({config.search_timeout_seconds}s). "
                "Reduce scope (fewer destinations/date span/providers) or raise search timeout."
            ) from exc
        except Exception as exc:
            wait_for_shutdown = False
            io_pool.shutdown(wait=False, cancel_futures=True)
            elapsed_seconds = round(time.time() - wall_start, 2)
            log_event(
                logging.ERROR,
                "search_failed",
                search_id=search_id,
                elapsed_seconds=elapsed_seconds,
                error=str(exc),
            )
            if progress is not None:
                progress.mark_failed(str(exc))
            raise
        finally:
            if wait_for_shutdown:
                # Flush all work on success. On failure/timeouts we already shut down without
                # waiting to avoid hanging the request.
                with contextlib.suppress(Exception):
                    io_pool.shutdown(wait=True, cancel_futures=False)

        elapsed_seconds = round(time.time() - wall_start, 2)
        meta = result.setdefault("meta", {})
        meta["search_id"] = search_id
        meta["search_timeout_seconds"] = config.search_timeout_seconds or None
        if progress is not None:
            progress.mark_completed(result_count=len(result.get("results") or []))
        log_event(
            logging.INFO,
            "search_completed",
            search_id=search_id,
            elapsed_seconds=elapsed_seconds,
            results_count=len(result.get("results") or []),
            warnings_count=len(result.get("warnings") or []),
        )
        return result
