from __future__ import annotations

import asyncio
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import replace
from datetime import date

import pytest

from src.data.airports import AirportCoordinates
from src.engine import optimizer as optimizer_module
from src.engine.optimizer import (
    SplitTripOptimizer,
    _apply_inner_return_bundle_estimate,
    _apply_price_time_score,
    _estimate_inner_return_bundle_price,
    _estimate_objective_score,
    _estimated_outbound_time_proxy_seconds,
    _min_calendar_price,
    _rank_chain_pairs,
    _rank_inbound_chain_pairs,
)
from src.providers import KiwiClient
from src.services.progress import SearchProgressTracker


def test_optimizer_top_level_helper_functions_cover_price_and_ranking_paths() -> None:
    assert _min_calendar_price(None) is None
    assert _min_calendar_price({"2026-01-01": 10, "2026-01-02": 5}) == 5
    assert _min_calendar_price({"2026-01-01": "x"}) is None

    assert _estimate_inner_return_bundle_price(None, None) is None
    assert _estimate_inner_return_bundle_price("bad", 100) == 200
    assert _estimate_inner_return_bundle_price(100, None) == 200
    assert _estimate_inner_return_bundle_price(100, 300) == 200

    assert _apply_inner_return_bundle_estimate(
        base_total=1000, outbound_market_price=400, inbound_market_price=200
    ) == (800, 400)
    assert _apply_inner_return_bundle_estimate(
        base_total=1000, outbound_market_price=100, inbound_market_price=None
    ) == (1000, None)
    assert _apply_inner_return_bundle_estimate(
        base_total=1000, outbound_market_price=0, inbound_market_price=0
    ) == (1000, None)

    items = [
        {"price": 100, "time": 10},
        {"price": 150, "time": 5},
    ]
    _apply_price_time_score(items, price_key="price", time_key="time", score_key="score")
    assert items[0]["score"] < items[1]["score"]
    items_with_missing_time = [
        {"price": 100, "time": None},
        {"price": 100, "time": 5},
    ]
    _apply_price_time_score(
        items_with_missing_time,
        price_key="price",
        time_key="time",
        score_key="score",
    )
    assert items_with_missing_time[0]["score"] == 0.28

    assert (
        _estimated_outbound_time_proxy_seconds(
            depart_origin_date=date(2026, 4, 25),
            depart_destination_date=date(2026, 4, 27),
            outbound_transfer_count=2,
        )
        == 223200
    )
    assert (
        _estimate_objective_score(
            objective="fastest",
            estimated_total=1000,
            distance_basis_km=7000.0,
            outbound_time_proxy_seconds=3600,
        )
        == 3610.0
    )
    assert (
        _estimate_objective_score(
            objective="fastest",
            estimated_total=1000,
            distance_basis_km=7000.0,
            outbound_time_proxy_seconds=None,
        )
        == 10.0
    )
    assert (
        _estimate_objective_score(
            objective="best",
            estimated_total=1000,
            distance_basis_km=7000.0,
            outbound_time_proxy_seconds=7200,
        )
        == 1024.0
    )
    assert (
        _estimate_objective_score(
            objective="best",
            estimated_total=1000,
            distance_basis_km=7000.0,
            outbound_time_proxy_seconds=None,
        )
        == 1000.0
    )
    assert (
        _estimate_objective_score(
            objective="cheapest",
            estimated_total=1000,
            distance_basis_km=5000.0,
            outbound_time_proxy_seconds=None,
        )
        == 200.0
    )
    assert (
        _estimate_objective_score(
            objective="cheapest",
            estimated_total=1000,
            distance_basis_km=None,
            outbound_time_proxy_seconds=None,
        )
        == 1000.0
    )

    chain_pairs = _rank_chain_pairs(
        origins=["OTP"],
        first_hubs=["IST"],
        second_hubs=["BKK"],
        leg_a_map={"OTP|IST": {"2026-04-25": 800}},
        leg_b_map={"IST|BKK": {"2026-04-25": 2400}},
        leg_c_map={"BKK": {"2026-04-27": 1000}},
        reverse_leg_c_map={"BKK": {"2026-05-05": 5000}},
        pair_limit=2,
    )
    assert chain_pairs == [("OTP", "IST", "BKK")]
    duplicate_chain_pairs = _rank_chain_pairs(
        origins=["OTP", "OTP"],
        first_hubs=["IST"],
        second_hubs=["BKK"],
        leg_a_map={"OTP|IST": {"2026-04-25": 800}},
        leg_b_map={"IST|BKK": {"2026-04-25": 2400}},
        leg_c_map={"BKK": {"2026-04-25": 1000}},
        reverse_leg_c_map={"BKK": {"2026-05-05": 5000}},
        pair_limit=4,
    )
    assert duplicate_chain_pairs == [("OTP", "IST", "BKK")]

    inbound_pairs = _rank_inbound_chain_pairs(
        origins=["OTP"],
        first_hubs=["BKK"],
        second_hubs=["AMM"],
        destination_to_hub={"BKK": {"2026-05-05": 5000}},
        hub_to_destination={"BKK": {"2026-04-27": 1000}},
        hub_to_hub={"BKK|AMM": {"2026-05-05": 2400}},
        hub_to_origin={"AMM|OTP": {"2026-05-08": 500}},
        pair_limit=2,
    )
    assert inbound_pairs == [("OTP", "BKK", "AMM")]
    inbound_pairs_with_gaps = _rank_inbound_chain_pairs(
        origins=["OTP", "OTP"],
        first_hubs=["BKK"],
        second_hubs=["AMM", "DOH"],
        destination_to_hub={"BKK": {"2026-05-05": 5000}},
        hub_to_destination={"BKK": {"2026-04-27": 1000}},
        hub_to_hub={"BKK|AMM": {"2026-05-05": 2400}, "BKK|DOH": {"2026-05-05": 2400}},
        hub_to_origin={"AMM|OTP": {"2026-05-08": 500}},
        pair_limit=4,
    )
    assert inbound_pairs_with_gaps == [("OTP", "BKK", "AMM")]


def test_optimizer_helper_methods_cover_plans_entries_and_boundary_validation() -> None:
    optimizer = SplitTripOptimizer(KiwiClient(), AirportCoordinates())
    assert optimizer._score_candidate(1200, 6000.0, "cheapest") == 1200.0
    assert optimizer._score_candidate(1200, 6000.0, "distance") == 200.0
    assert optimizer._score_candidate(1200, None, "distance") == float("inf")
    assert optimizer._transfer_airports([{"to": "IST"}, {"to": "IST"}, {"to": "BKK"}]) == ["IST"]
    assert optimizer._as_int("12") == 12
    assert optimizer._as_int("bad") is None
    assert not optimizer._exceeds_connection_layover_limit([], None)
    assert optimizer._exceeds_connection_layover_limit(
        [
            {"arrive_local": "2026-03-10T10:00:00"},
            {"depart_local": "2026-03-10T13:30:00"},
        ],
        3600,
    )

    stopover_candidate = {
        "candidate_type": "split_stopover",
        "origin": "OTP",
        "arrival_origin": "OTP",
        "destination": "MGA",
        "outbound_hub": "IST",
        "inbound_hub": "IST",
        "depart_origin_date": "2026-03-10",
        "depart_destination_date": "2026-03-12",
        "leave_destination_date": "2026-03-18",
        "return_origin_date": "2026-03-20",
        "outbound_stopover_days": 2,
        "inbound_stopover_days": 1,
    }
    chain_candidate = {
        "candidate_type": "split_chain",
        "origin": "OTP",
        "arrival_origin": "OTP",
        "destination": "USM",
        "outbound_boundary_stopover_days": [0],
        "inbound_boundary_stopover_days": [1],
        "outbound_legs": [
            {"source": "OTP", "destination": "BKK", "date": "2026-04-25"},
            {"source": "BKK", "destination": "USM", "date": "2026-04-27"},
        ],
        "inbound_legs": [
            {"source": "USM", "destination": "BKK", "date": "2026-05-05"},
            {"source": "BKK", "destination": "OTP", "date": "2026-05-08"},
        ],
    }
    assert optimizer._candidate_split_plans({"candidate_type": "direct_roundtrip"}) is None
    stopover_plans = optimizer._candidate_split_plans(stopover_candidate)
    assert stopover_plans is not None
    assert stopover_plans["outbound_plan"][0] == ("OTP", "IST", "2026-03-10")

    chain_plans = optimizer._candidate_split_plans(chain_candidate)
    assert chain_plans is not None
    assert chain_plans["inbound_boundary_days"] == [1]

    inner_plan = optimizer._candidate_inner_return_plan(chain_candidate)
    assert inner_plan is not None
    assert inner_plan["return_key"] == ("BKK", "USM", "2026-04-27", "2026-05-05")
    assert (
        optimizer._candidate_inner_return_plan({**chain_candidate, "arrival_origin": "BBU"}) is None
    )

    fare = {
        "price": 500,
        "formatted_price": "500 RON",
        "duration_seconds": 7200,
        "stops": 1,
        "segments": [
            {
                "from": "OTP",
                "to": "IST",
                "depart_local": "2026-03-10T08:00:00",
                "arrive_local": "2026-03-10T09:00:00",
            },
            {
                "from": "IST",
                "to": "MGA",
                "depart_local": "2026-03-10T10:30:00",
                "arrive_local": "2026-03-10T13:00:00",
            },
        ],
    }
    oneway_map = {("OTP", "MGA", "2026-03-10"): fare}
    entries = optimizer._materialize_oneway_plan_entries([("OTP", "MGA", "2026-03-10")], oneway_map)
    assert entries is not None
    assert entries[0]["source"] == "OTP"
    bad_cache = {("OTP", "MGA", "2026-03-10"): {"source": "CDG", "destination": "MGA"}}
    assert (
        optimizer._materialize_oneway_plan_entries(
            [("OTP", "MGA", "2026-03-10")], oneway_map, bad_cache
        )
        is None
    )

    boundary_entries = [
        {
            "source": "OTP",
            "destination": "IST",
            "segments": [{"arrive_local": "2026-03-10T09:00:00"}],
        },
        {
            "source": "IST",
            "destination": "MGA",
            "segments": [{"depart_local": "2026-03-10T12:00:00"}],
        },
    ]
    assert optimizer._validate_entry_boundaries(boundary_entries, [0], 4 * 3600) == (True, 1)
    assert optimizer._validate_entry_boundaries(boundary_entries, [0], 60) == (False, 0)
    assert not optimizer._validate_entry_boundaries(
        [{"segments": []}, {"segments": []}],
        [0],
        None,
    )[0]

    assert optimizer._leg_matches_expected_route("otp", "mga", "OTP", "MGA")
    oneway_leg = optimizer._oneway_entry_to_leg(
        {
            "source": "OTP",
            "destination": "MGA",
            "date": "2026-03-10",
            "fare": {"price": 500, "formatted_price": "500 RON", "stops": 1},
            "segments": fare["segments"],
            "duration_seconds": 7200,
        },
        fallback_source="OTP",
        fallback_destination="MGA",
        fallback_date="2026-03-10",
        max_stops_per_leg=2,
    )
    assert "kiwi.com" in str(oneway_leg["booking_url"])

    return_leg = optimizer._return_fare_to_ticket_leg(
        {
            "price": 900,
            "formatted_price": "900 RON",
            "outbound_stops": 1,
            "inbound_stops": 1,
            "outbound_segments": fare["segments"],
            "inbound_segments": list(reversed(fare["segments"])),
        },
        source="OTP",
        destination="MGA",
        outbound_iso="2026-03-10",
        inbound_iso="2026-03-24",
        max_stops_per_leg=2,
    )
    assert "kiwi.com" in str(return_leg["booking_url"])

    oneway_cache = optimizer._prepare_oneway_entry_cache(oneway_map)
    assert oneway_cache[("OTP", "MGA", "2026-03-10")]["destination"] == "MGA"
    return_cache = optimizer._prepare_return_trip_cache(
        {
            ("OTP", "MGA", "2026-03-10", "2026-03-24"): {
                "outbound_segments": fare["segments"],
                "inbound_segments": list(reversed(fare["segments"])),
                "outbound_duration_seconds": 7200,
                "inbound_duration_seconds": 7000,
            }
        }
    )
    assert return_cache[("OTP", "MGA", "2026-03-10", "2026-03-24")]["outbound_destination"] == "MGA"


def test_optimizer_runtime_helpers_cover_distances_names_and_best_value_scores(monkeypatch) -> None:
    airports = AirportCoordinates()
    airports.coordinates = {"OTP": (44.57, 26.08), "IST": (41.27, 28.75), "BKK": (13.69, 100.75)}
    airports.airport_display_names = {"BKK": "Bangkok"}
    optimizer = SplitTripOptimizer(KiwiClient(), airports)
    monkeypatch.setattr(optimizer, "_available_cpu_workers", lambda: 20)

    caps = optimizer.runtime_capabilities()
    assert caps["cpu_workers_max"] == 20
    assert optimizer._distance_km("OTP", "IST") is not None
    assert optimizer._distance_km("OTP", "XXX") is None
    assert optimizer._distance_for_route(["OTP", "IST", "BKK"]) is not None
    assert optimizer._distance_for_route(["OTP"]) == 0.0
    assert optimizer._destination_display_name("BKK") == "Bangkok"
    assert optimizer._destination_display_name("XXX") == "XXX"

    results = [
        {"total_price": 1000, "outbound_time_to_destination_seconds": 10_000},
        {"total_price": 1400, "outbound_time_to_destination_seconds": 40_000},
    ]
    optimizer._compute_best_value_scores(results)
    assert results[0]["best_value_score"] < results[1]["best_value_score"]


def test_optimizer_validation_context_and_provider_fallback_helpers_cover_budget_paths() -> None:
    optimizer = SplitTripOptimizer(KiwiClient(), AirportCoordinates())

    fallback_config = optimizer.parse_search_config(
        {
            "origins": ["OTP"],
            "destinations": ["USM"],
            "period_start": "2026-04-20",
            "period_end": "2026-05-10",
            "providers": "amadeus,serpapi",
        }
    )
    client, provider_status, fallback_warnings = optimizer._build_search_client(fallback_config)
    assert client.active_provider_ids == ["kiwi"]
    assert {item["id"]: item["configured"] for item in provider_status}["amadeus"] is False
    assert any("missing credentials" in warning for warning in fallback_warnings)
    assert any("Falling back to Kiwi" in warning for warning in fallback_warnings)

    config = optimizer.parse_search_config(
        {
            "origins": ["OTP"],
            "destinations": ["USM"],
            "period_start": "2026-04-20",
            "period_end": "2026-05-10",
            "objective": "best",
            "validate_top_per_destination": 32,
            "max_validate_oneway_keys_per_destination": 1,
            "max_validate_return_keys_per_destination": 1,
            "serpapi_probe_oneway_keys": 0,
            "serpapi_probe_return_keys": 0,
            "exhaustive_hub_scan": True,
        }
    )

    estimated_candidates = [
        {
            "candidate_type": "split_chain",
            "destination": "USM",
            "origin": "OTP",
            "arrival_origin": "OTP",
            "outbound_hub": "IST/BKK",
            "inbound_hub": "BKK/AMM",
            "depart_origin_date": "2026-04-25",
            "depart_destination_date": "2026-04-27",
            "leave_destination_date": "2026-05-05",
            "return_origin_date": "2026-05-08",
            "outbound_boundary_stopover_days": [0, 0],
            "inbound_boundary_stopover_days": [0, 0],
            "outbound_legs": [
                {"source": "OTP", "destination": "IST", "date": "2026-04-25"},
                {"source": "IST", "destination": "BKK", "date": "2026-04-25"},
                {"source": "BKK", "destination": "USM", "date": "2026-04-27"},
            ],
            "inbound_legs": [
                {"source": "USM", "destination": "BKK", "date": "2026-05-05"},
                {"source": "BKK", "destination": "AMM", "date": "2026-05-05"},
                {"source": "AMM", "destination": "OTP", "date": "2026-05-08"},
            ],
            "main_stay_days": 8,
            "estimated_total": 7600,
            "estimated_score": 7600.0,
            "estimated_best_value_score": 0.11,
            "estimated_outbound_time_to_destination_seconds": 60_000,
        },
        {
            "candidate_type": "split_stopover",
            "destination": "USM",
            "origin": "OTP",
            "arrival_origin": "OTP",
            "outbound_hub": "SIN",
            "inbound_hub": "SIN",
            "depart_origin_date": "2026-04-24",
            "depart_destination_date": "2026-04-27",
            "leave_destination_date": "2026-05-05",
            "return_origin_date": "2026-05-08",
            "outbound_stopover_days": 3,
            "inbound_stopover_days": 0,
            "main_stay_days": 8,
            "estimated_total": 7800,
            "estimated_score": 7800.0,
            "estimated_best_value_score": 0.18,
            "estimated_outbound_time_to_destination_seconds": 70_000,
        },
        {
            "candidate_type": "direct_roundtrip",
            "destination": "USM",
            "origin": "OTP",
            "arrival_origin": "OTP",
            "depart_origin_date": "2026-04-24",
            "return_origin_date": "2026-05-08",
            "main_stay_days": 8,
            "estimated_total": 8100,
            "estimated_score": 8100.0,
            "estimated_best_value_score": 0.14,
            "estimated_outbound_time_to_destination_seconds": 20_000,
        },
        {
            "candidate_type": "direct_roundtrip",
            "destination": "USM",
            "origin": "OTP",
            "arrival_origin": "OTP",
            "depart_origin_date": "2026-04-25",
            "return_origin_date": "2026-05-08",
            "main_stay_days": 7,
            "estimated_total": 8300,
            "estimated_score": 8300.0,
            "estimated_best_value_score": 0.17,
            "estimated_outbound_time_to_destination_seconds": 22_000,
        },
    ]
    for index in range(12):
        estimated_candidates.append(
            {
                "candidate_type": "split_stopover",
                "destination": "USM",
                "origin": "OTP",
                "arrival_origin": "OTP",
                "outbound_hub": f"H{index:02d}",
                "inbound_hub": f"R{index:02d}",
                "depart_origin_date": f"2026-04-{20 + index:02d}",
                "depart_destination_date": f"2026-04-{21 + index:02d}",
                "leave_destination_date": f"2026-05-{1 + index:02d}",
                "return_origin_date": f"2026-05-{2 + index:02d}",
                "outbound_stopover_days": 1,
                "inbound_stopover_days": 1,
                "main_stay_days": 7,
                "estimated_total": 8400 + index,
                "estimated_score": 8400.0 + index,
                "estimated_best_value_score": 0.2 + (index * 0.01),
                "estimated_outbound_time_to_destination_seconds": 80_000 + (index * 100),
            }
        )
    for index in range(12):
        estimated_candidates.append(
            {
                "candidate_type": "direct_roundtrip",
                "destination": "USM",
                "origin": "OTP",
                "arrival_origin": "OTP",
                "depart_origin_date": f"2026-04-{10 + index:02d}",
                "return_origin_date": f"2026-05-{10 + index:02d}",
                "main_stay_days": 6 + (index % 3),
                "estimated_total": 9000 + index,
                "estimated_score": 9000.0 + index,
                "estimated_best_value_score": 0.4 + (index * 0.01),
                "estimated_outbound_time_to_destination_seconds": 40_000 + (index * 50),
            }
        )

    context, warnings = optimizer._prepare_destination_validation_context(
        destination="USM",
        estimated_candidates=estimated_candidates,
        config=config,
        validation_target_per_destination=32,
        origin_rank={"OTP": 0},
        core_provider_ids=("kiwi",),
        serpapi_active=True,
    )

    assert context["destination_name"]
    assert context["split_chain_estimated_count"] == 1
    assert len(context["limited_candidates"]) >= 2
    assert len(context["ordered_oneway_keys"]) == 20
    assert len(context["ordered_return_keys"]) == 10
    assert any("destination-first split-chain candidates" in warning for warning in warnings)
    assert any("capped one-way key validations" in warning for warning in warnings)
    assert any("capped return key validations" in warning for warning in warnings)
    assert any("SerpApi probes were 0" in warning for warning in warnings)
    assert any("SerpApi probe scope" in warning for warning in warnings)
    assert context["oneway_provider_map"][context["ordered_oneway_keys"][0]] == ("kiwi", "serpapi")
    assert context["return_provider_map"][context["ordered_return_keys"][0]] == ("kiwi", "serpapi")

    direct = {
        "result_id": "direct-best",
        "itinerary_type": "direct_roundtrip",
        "destination_code": "USM",
        "total_price": 7800,
        "main_destination_stay_days": 8,
        "outbound_time_to_destination_seconds": 18_000,
        "outbound": {"origin": "OTP", "date_from_origin": "2026-04-24"},
        "inbound": {"arrival_origin": "OTP"},
    }
    split_removed = {
        "result_id": "split-removed",
        "itinerary_type": "split_stopover",
        "destination_code": "USM",
        "total_price": 7900,
        "main_destination_stay_days": 8,
        "outbound_time_to_destination_seconds": 25_000,
        "outbound": {"origin": "OTP", "date_from_origin": "2026-04-24"},
        "inbound": {"arrival_origin": "OTP"},
    }
    split_kept = {
        "result_id": "split-kept",
        "itinerary_type": "split_stopover",
        "destination_code": "USM",
        "total_price": 7600,
        "main_destination_stay_days": 8,
        "outbound_time_to_destination_seconds": 28_000,
        "outbound": {"origin": "OTP", "date_from_origin": "2026-04-24"},
        "inbound": {"arrival_origin": "OTP"},
    }
    split_unknown_time = {
        "result_id": "split-unknown-time",
        "itinerary_type": "split_stopover",
        "destination_code": "USM",
        "total_price": 7900,
        "main_destination_stay_days": 8,
        "outbound_time_to_destination_seconds": None,
        "outbound": {"origin": "OTP", "date_from_origin": "2026-04-24"},
        "inbound": {"arrival_origin": "OTP"},
    }
    filtered, removed = optimizer._prune_dominated_split_results(
        [direct, split_removed, split_kept, split_unknown_time]
    )
    filtered_ids = {item["result_id"] for item in filtered}
    assert removed == 2
    assert filtered_ids == {"direct-best", "split-kept"}


def test_prepare_destination_validation_context_preserves_price_floor_candidates_for_best_objective() -> (
    None
):
    optimizer = SplitTripOptimizer(KiwiClient(), AirportCoordinates())
    config = optimizer.parse_search_config(
        {
            "origins": ["OTP"],
            "destinations": ["USM"],
            "providers": ["kiwi"],
            "period_start": "2026-04-01",
            "period_end": "2026-04-20",
            "hub_candidates": ["IST", "DOH", "SIN", "BKK"],
            "auto_hubs_per_direction": 4,
            "min_stay_days": 6,
            "max_stay_days": 6,
            "min_stopover_days": 0,
            "max_stopover_days": 0,
            "max_transfers_per_direction": 1,
            "objective": "best",
            "validate_top_per_destination": 2,
            "io_workers": 4,
            "cpu_workers": 1,
        }
    )
    estimated_candidates = [
        {
            "candidate_type": "split_stopover",
            "destination": "USM",
            "origin": "OTP",
            "arrival_origin": "OTP",
            "outbound_hub": "IST",
            "inbound_hub": "IST",
            "depart_origin_date": "2026-04-01",
            "depart_destination_date": "2026-04-01",
            "leave_destination_date": "2026-04-07",
            "return_origin_date": "2026-04-07",
            "outbound_stopover_days": 0,
            "inbound_stopover_days": 0,
            "main_stay_days": 6,
            "estimated_total": 1300,
            "estimated_score": 1300.0,
            "estimated_best_value_score": 1300.0,
            "estimated_outbound_time_to_destination_seconds": 18_000,
        },
        {
            "candidate_type": "split_stopover",
            "destination": "USM",
            "origin": "OTP",
            "arrival_origin": "OTP",
            "outbound_hub": "DOH",
            "inbound_hub": "DOH",
            "depart_origin_date": "2026-04-02",
            "depart_destination_date": "2026-04-02",
            "leave_destination_date": "2026-04-08",
            "return_origin_date": "2026-04-08",
            "outbound_stopover_days": 0,
            "inbound_stopover_days": 0,
            "main_stay_days": 6,
            "estimated_total": 1320,
            "estimated_score": 1320.0,
            "estimated_best_value_score": 1320.0,
            "estimated_outbound_time_to_destination_seconds": 19_000,
        },
        {
            "candidate_type": "split_stopover",
            "destination": "USM",
            "origin": "OTP",
            "arrival_origin": "OTP",
            "outbound_hub": "SIN",
            "inbound_hub": "SIN",
            "depart_origin_date": "2026-04-03",
            "depart_destination_date": "2026-04-03",
            "leave_destination_date": "2026-04-09",
            "return_origin_date": "2026-04-09",
            "outbound_stopover_days": 0,
            "inbound_stopover_days": 0,
            "main_stay_days": 6,
            "estimated_total": 1340,
            "estimated_score": 1340.0,
            "estimated_best_value_score": 1340.0,
            "estimated_outbound_time_to_destination_seconds": 20_000,
        },
        {
            "candidate_type": "split_stopover",
            "destination": "USM",
            "origin": "OTP",
            "arrival_origin": "OTP",
            "outbound_hub": "BKK",
            "inbound_hub": "BKK",
            "depart_origin_date": "2026-04-04",
            "depart_destination_date": "2026-04-04",
            "leave_destination_date": "2026-04-10",
            "return_origin_date": "2026-04-10",
            "outbound_stopover_days": 0,
            "inbound_stopover_days": 0,
            "main_stay_days": 6,
            "estimated_total": 1360,
            "estimated_score": 1360.0,
            "estimated_best_value_score": 1360.0,
            "estimated_outbound_time_to_destination_seconds": 21_000,
        },
        {
            "candidate_type": "split_stopover",
            "destination": "USM",
            "origin": "OTP",
            "arrival_origin": "OTP",
            "outbound_hub": "IST",
            "inbound_hub": "IST",
            "depart_origin_date": "2026-04-05",
            "depart_destination_date": "2026-04-05",
            "leave_destination_date": "2026-04-11",
            "return_origin_date": "2026-04-11",
            "outbound_stopover_days": 0,
            "inbound_stopover_days": 0,
            "main_stay_days": 6,
            "estimated_total": 900,
            "estimated_score": 2100.0,
            "estimated_best_value_score": 2100.0,
            "estimated_outbound_time_to_destination_seconds": 90_000,
        },
    ]

    context, warnings = optimizer._prepare_destination_validation_context(
        destination="USM",
        estimated_candidates=estimated_candidates,
        config=config,
        validation_target_per_destination=2,
        origin_rank={"OTP": 0},
        core_provider_ids=("kiwi",),
        serpapi_active=False,
    )

    limited_totals = {
        int(item.get("estimated_total") or 0) for item in context["limited_candidates"]
    }
    assert 900 in limited_totals
    assert any("price-floor candidate" in warning for warning in warnings)


def test_optimizer_graph_strategy_and_hub_helpers_cover_diversity_and_selection_paths(
    monkeypatch,
) -> None:
    optimizer = SplitTripOptimizer(KiwiClient(), AirportCoordinates())
    optimizer.coords.coordinates = {
        "OTP": (44.57, 26.08),
        "IST": (41.27, 28.75),
        "BKK": (13.69, 100.75),
        "SIN": (1.36, 103.99),
        "AMM": (31.72, 35.99),
        "USM": (9.55, 100.06),
    }

    config = optimizer.parse_search_config(
        {
            "origins": ["OTP"],
            "destinations": ["USM"],
            "period_start": "2026-04-20",
            "period_end": "2026-05-10",
            "max_transfers_per_direction": 2,
            "auto_hubs_per_direction": 1,
        }
    )
    config = replace(config, hub_candidates=("IST", "SIN"))

    class _RouteGraph:
        def __init__(self, available: bool, scores: dict[str, int]) -> None:
            self._available = available
            self._scores = scores
            self.calls: list[tuple[tuple[str, ...], tuple[str, ...], int]] = []

        def available(self) -> bool:
            return self._available

        def score_path_hubs(
            self,
            *,
            origins: tuple[str, ...],
            destinations: tuple[str, ...],
            max_split_hubs: int,
        ) -> dict[str, int]:
            self.calls.append((tuple(origins), tuple(destinations), max_split_hubs))
            return dict(self._scores)

    unavailable_graph = _RouteGraph(False, {})
    monkeypatch.setattr(optimizer, "route_graph", unavailable_graph)
    unchanged_config, unavailable_meta, unavailable_warnings = (
        optimizer._expand_route_graph_hub_candidates(config)
    )
    assert unchanged_config.hub_candidates == ("IST", "SIN")
    assert unavailable_meta["hub_candidates_graph_available"] is False
    assert unavailable_warnings == []

    available_graph = _RouteGraph(True, {"BKK": 5, "SIN": 4, "AMM": 2})
    monkeypatch.setattr(optimizer, "route_graph", available_graph)
    expanded_config, expanded_meta, expanded_warnings = (
        optimizer._expand_route_graph_hub_candidates(config)
    )
    assert available_graph.calls == [(("OTP",), ("USM",), 2)]
    assert expanded_config.hub_candidates[:4] == ("BKK", "SIN", "AMM", "IST")
    assert expanded_meta["hub_candidates_graph_applied"] is True
    assert expanded_meta["hub_candidates_graph_count"] == 3
    assert any("expanded the hub pool" in warning for warning in expanded_warnings)

    split_candidates = [
        {
            "candidate_type": "split_stopover",
            "candidate_id": "base",
            "origin": "OTP",
            "arrival_origin": "OTP",
            "outbound_hub": "IST",
            "inbound_hub": "IST",
            "depart_origin_date": "2026-04-25",
            "depart_destination_date": "2026-04-27",
            "leave_destination_date": "2026-05-05",
            "return_origin_date": "2026-05-08",
        },
        {
            "candidate_type": "split_stopover",
            "candidate_id": "duplicate",
            "origin": "OTP",
            "arrival_origin": "OTP",
            "outbound_hub": "IST",
            "inbound_hub": "IST",
            "depart_origin_date": "2026-04-25",
            "depart_destination_date": "2026-04-27",
            "leave_destination_date": "2026-05-05",
            "return_origin_date": "2026-05-08",
        },
        {
            "candidate_type": "split_stopover",
            "candidate_id": "new-outbound",
            "origin": "OTP",
            "arrival_origin": "OTP",
            "outbound_hub": "BKK",
            "inbound_hub": "IST",
            "depart_origin_date": "2026-04-26",
            "depart_destination_date": "2026-04-27",
            "leave_destination_date": "2026-05-05",
            "return_origin_date": "2026-05-08",
        },
        {
            "candidate_type": "split_stopover",
            "candidate_id": "new-inbound",
            "origin": "OTP",
            "arrival_origin": "OTP",
            "outbound_hub": "BKK",
            "inbound_hub": "AMM",
            "depart_origin_date": "2026-04-27",
            "depart_destination_date": "2026-04-28",
            "leave_destination_date": "2026-05-05",
            "return_origin_date": "2026-05-08",
        },
        {
            "candidate_type": "split_stopover",
            "candidate_id": "new-pair",
            "origin": "OTP",
            "arrival_origin": "OTP",
            "outbound_hub": "SIN",
            "inbound_hub": "BKK",
            "depart_origin_date": "2026-04-28",
            "depart_destination_date": "2026-04-29",
            "leave_destination_date": "2026-05-05",
            "return_origin_date": "2026-05-08",
        },
    ]
    diversified = optimizer._select_split_candidates_with_diversity(
        split_candidates,
        base_quota=1,
        extra_quota=3,
    )
    diversified_ids = [item["candidate_id"] for item in diversified]
    assert diversified_ids[0] == "base"
    assert set(diversified_ids) == {"base", "new-outbound", "new-inbound", "new-pair"}

    ranked = [
        {
            "result_id": "usm-cheapest",
            "destination_code": "USM",
            "itinerary_type": "direct_roundtrip",
            "total_price": 7000,
            "outbound_time_to_destination_seconds": 30_000,
            "outbound": {
                "layovers_count": 0,
                "hub": "DIRECT",
                "origin": "OTP",
                "date_from_origin": "2026-04-25",
            },
            "inbound": {"layovers_count": 0, "hub": "DIRECT", "arrival_origin": "OTP"},
        },
        {
            "result_id": "usm-fastest",
            "destination_code": "USM",
            "itinerary_type": "direct_roundtrip",
            "total_price": 7200,
            "outbound_time_to_destination_seconds": 10_000,
            "outbound": {
                "layovers_count": 0,
                "hub": "IST",
                "origin": "OTP",
                "date_from_origin": "2026-04-25",
            },
            "inbound": {"layovers_count": 0, "hub": "IST", "arrival_origin": "OTP"},
        },
        {
            "result_id": "usm-one-layover",
            "destination_code": "USM",
            "itinerary_type": "split_stopover",
            "total_price": 7100,
            "outbound_time_to_destination_seconds": 20_000,
            "outbound": {
                "layovers_count": 1,
                "hub": "BKK",
                "origin": "OTP",
                "date_from_origin": "2026-04-25",
            },
            "inbound": {"layovers_count": 1, "hub": "BKK", "arrival_origin": "OTP"},
        },
        {
            "result_id": "usm-two-layovers",
            "destination_code": "USM",
            "itinerary_type": "split_stopover",
            "total_price": 7050,
            "outbound_time_to_destination_seconds": 24_000,
            "outbound": {
                "layovers_count": 2,
                "hub": "SIN",
                "origin": "OTP",
                "date_from_origin": "2026-04-25",
            },
            "inbound": {"layovers_count": 1, "hub": "SIN", "arrival_origin": "OTP"},
        },
        {
            "result_id": "usm-stopover",
            "destination_code": "USM",
            "itinerary_type": "split_stopover",
            "total_price": 7150,
            "outbound_time_to_destination_seconds": 26_000,
            "outbound": {
                "layovers_count": 1,
                "stopover_days": 2,
                "hub": "AMM",
                "origin": "OTP",
                "date_from_origin": "2026-04-25",
            },
            "inbound": {
                "layovers_count": 1,
                "stopover_days": 0,
                "hub": "AMM",
                "arrival_origin": "OTP",
            },
        },
        {
            "result_id": "pqc-direct",
            "destination_code": "PQC",
            "itinerary_type": "direct_roundtrip",
            "total_price": 6900,
            "outbound_time_to_destination_seconds": 32_000,
            "outbound": {
                "layovers_count": 0,
                "hub": "DIRECT",
                "origin": "OTP",
                "date_from_origin": "2026-04-25",
            },
            "inbound": {"layovers_count": 0, "hub": "DIRECT", "arrival_origin": "OTP"},
        },
    ]
    merged = optimizer._merge_strategy_anchors(ranked, top_results=4)
    merged_ids = {item["result_id"] for item in merged}
    assert {"usm-cheapest", "usm-fastest", "pqc-direct"} <= merged_ids
    assert merged_ids & {"usm-one-layover", "usm-two-layovers", "usm-stopover"}

    capped, per_destination_counts = optimizer._cap_results_per_destination(
        ranked,
        top_results_per_destination=2,
        destination_order=["PQC", "USM"],
        required_by_destination={"USM": [ranked[4]]},
    )
    assert per_destination_counts == {"PQC": 1, "USM": 2}
    assert [item["destination_code"] for item in capped[:3]] == ["PQC", "USM", "USM"]
    assert any(item["result_id"] == "usm-stopover" for item in capped)

    auto_hub_config = replace(
        optimizer.parse_search_config(
            {
                "origins": ["OTP"],
                "destinations": ["USM"],
                "period_start": "2026-04-20",
                "period_end": "2026-05-10",
                "max_transfers_per_direction": 2,
                "auto_hubs_per_direction": 1,
            }
        ),
        hub_candidates=("IST", "BKK", "SIN", "AMM"),
        exhaustive_hub_scan=False,
    )
    calendars = {
        ("OTP", "IST"): {"2026-04-25": 500},
        ("OTP", "BKK"): {"2026-04-25": 2800},
        ("OTP", "SIN"): {"2026-04-25": 3100},
        ("OTP", "AMM"): {"2026-04-25": 700},
        ("IST", "OTP"): {"2026-05-08": 550},
        ("BKK", "OTP"): {"2026-05-08": 2900},
        ("SIN", "OTP"): {"2026-05-08": 3000},
        ("AMM", "OTP"): {"2026-05-08": 650},
        ("IST", "USM"): {"2026-04-27": 3500},
        ("BKK", "USM"): {"2026-04-27": 800},
        ("SIN", "USM"): {"2026-04-27": 1000},
        ("AMM", "USM"): {"2026-04-27": 4000},
        ("USM", "IST"): {"2026-05-05": 3600},
        ("USM", "BKK"): {"2026-05-05": 750},
        ("USM", "SIN"): {"2026-05-05": 1200},
        ("USM", "AMM"): {"2026-05-05": 4200},
    }
    outbound_hubs, inbound_hubs = optimizer._pick_auto_hubs("USM", auto_hub_config, calendars)
    assert outbound_hubs[0] == "BKK"
    assert inbound_hubs[0] == "BKK"
    assert len(outbound_hubs) == len(auto_hub_config.hub_candidates)

    exhaustive_config = replace(auto_hub_config, exhaustive_hub_scan=True)
    exhaustive_outbound, exhaustive_inbound = optimizer._pick_auto_hubs(
        "USM",
        exhaustive_config,
        calendars,
    )
    assert exhaustive_outbound[0] == "BKK"
    assert exhaustive_inbound[0] == "BKK"
    assert set(exhaustive_outbound) == set(exhaustive_config.hub_candidates)


def test_optimizer_async_fetch_and_search_wrapper_cover_base_compare_and_failures(
    monkeypatch,
) -> None:
    optimizer = SplitTripOptimizer(KiwiClient(), AirportCoordinates())
    config = optimizer.parse_search_config(
        {
            "origins": ["OTP"],
            "destinations": ["MGA"],
            "period_start": "2026-03-10",
            "period_end": "2026-03-24",
            "providers": ["kiwi", "amadeus", "serpapi"],
            "market_compare_fares": True,
            "cabin_bags_per_adult": 1,
            "hold_bags_per_adult": 0,
            "max_connection_layover_hours": 2,
            "io_workers": 2,
            "cpu_workers": 1,
        }
    )

    class _FakeSearchClient:
        def get_best_oneway(self, **kwargs: object) -> dict[str, object] | None:
            destination = str(kwargs["destination"])
            provider_ids = tuple(kwargs.get("provider_ids") or ())
            date_iso = str(kwargs["departure_iso"])
            if destination == "ERR":
                raise RuntimeError("oneway boom")
            if provider_ids == ("kiwi",):
                return {
                    "price": 90,
                    "formatted_price": "90 RON",
                    "currency": "RON",
                    "duration_seconds": 3600,
                    "stops": 0,
                    "transfer_events": 0,
                    "booking_url": f"https://example.test/base-{destination.lower()}-{date_iso}",
                    "segments": [
                        {
                            "from": kwargs["source"],
                            "to": kwargs["destination"],
                            "depart_local": f"{date_iso}T08:00:00",
                            "arrive_local": f"{date_iso}T09:00:00",
                        }
                    ],
                    "provider": "kiwi",
                }
            if destination == "BASEONLY":
                return None
            return {
                "price": 150,
                "formatted_price": "150 RON",
                "currency": "RON",
                "duration_seconds": 5400,
                "stops": 1,
                "transfer_events": 1,
                "booking_url": f"https://example.test/selected-{destination.lower()}-{date_iso}",
                "segments": [
                    {
                        "from": kwargs["source"],
                        "to": "IST",
                        "depart_local": f"{date_iso}T08:00:00",
                        "arrive_local": f"{date_iso}T09:00:00",
                    },
                    {
                        "from": "IST",
                        "to": kwargs["destination"],
                        "depart_local": f"{date_iso}T10:00:00",
                        "arrive_local": f"{date_iso}T11:30:00",
                    },
                ],
                "provider": "amadeus",
            }

        def get_best_return(self, **kwargs: object) -> dict[str, object] | None:
            destination = str(kwargs["destination"])
            provider_ids = tuple(kwargs.get("provider_ids") or ())
            outbound_iso = str(kwargs["outbound_iso"])
            inbound_iso = str(kwargs["inbound_iso"])
            if destination == "ERR":
                raise RuntimeError("return boom")
            if provider_ids == ("kiwi",):
                return {
                    "price": 240,
                    "formatted_price": "240 RON",
                    "currency": "RON",
                    "duration_seconds": 7200,
                    "outbound_duration_seconds": 3600,
                    "inbound_duration_seconds": 3600,
                    "outbound_stops": 0,
                    "inbound_stops": 0,
                    "outbound_transfer_events": 0,
                    "inbound_transfer_events": 0,
                    "booking_url": f"https://example.test/base-return-{destination.lower()}",
                    "outbound_segments": [
                        {
                            "from": kwargs["source"],
                            "to": kwargs["destination"],
                            "depart_local": f"{outbound_iso}T08:00:00",
                            "arrive_local": f"{outbound_iso}T09:00:00",
                        }
                    ],
                    "inbound_segments": [
                        {
                            "from": kwargs["destination"],
                            "to": kwargs["source"],
                            "depart_local": f"{inbound_iso}T08:00:00",
                            "arrive_local": f"{inbound_iso}T09:00:00",
                        }
                    ],
                    "provider": "kiwi",
                }
            if destination == "BASEONLY":
                return None
            return {
                "price": 300,
                "formatted_price": "300 RON",
                "currency": "RON",
                "duration_seconds": 10800,
                "outbound_duration_seconds": 5400,
                "inbound_duration_seconds": 5400,
                "outbound_stops": 1,
                "inbound_stops": 1,
                "outbound_transfer_events": 1,
                "inbound_transfer_events": 1,
                "booking_url": f"https://example.test/selected-return-{destination.lower()}",
                "outbound_segments": [
                    {
                        "from": kwargs["source"],
                        "to": "IST",
                        "depart_local": f"{outbound_iso}T08:00:00",
                        "arrive_local": f"{outbound_iso}T09:00:00",
                    },
                    {
                        "from": "IST",
                        "to": kwargs["destination"],
                        "depart_local": f"{outbound_iso}T10:00:00",
                        "arrive_local": f"{outbound_iso}T11:30:00",
                    },
                ],
                "inbound_segments": [
                    {
                        "from": kwargs["destination"],
                        "to": "IST",
                        "depart_local": f"{inbound_iso}T08:00:00",
                        "arrive_local": f"{inbound_iso}T09:00:00",
                    },
                    {
                        "from": "IST",
                        "to": kwargs["source"],
                        "depart_local": f"{inbound_iso}T10:00:00",
                        "arrive_local": f"{inbound_iso}T11:30:00",
                    },
                ],
                "provider": "amadeus",
            }

    fetch_client = _FakeSearchClient()
    fetch_progress = SearchProgressTracker("fetch-phase")
    fetch_progress.start_phase("oneways", total=3, detail="Start one-way checks.")
    with ThreadPoolExecutor(max_workers=2) as io_pool:
        oneway_map, oneway_warnings, oneway_base_count = asyncio.run(
            optimizer._fetch_oneways_parallel(
                fetch_client,
                [
                    ("OTP", "MGA", "2026-03-10"),
                    ("OTP", "BASEONLY", "2026-03-11"),
                    ("OTP", "ERR", "2026-03-12"),
                ],
                config,
                io_pool,
                provider_map={
                    ("OTP", "MGA", "2026-03-10"): ("amadeus",),
                    ("OTP", "BASEONLY", "2026-03-11"): ("amadeus",),
                },
                base_provider_ids=("kiwi", "amadeus"),
                progress=fetch_progress,
            )
        )
    assert oneway_base_count == 2
    assert oneway_map[("OTP", "MGA", "2026-03-10")]["fare_mode"] == "selected_bags"
    assert oneway_map[("OTP", "MGA", "2026-03-10")]["base_no_bags_price"] == 90
    assert oneway_map[("OTP", "BASEONLY", "2026-03-11")]["fare_mode"] == "base_no_bags"
    assert oneway_map[("OTP", "ERR", "2026-03-12")] is None
    assert any("oneway boom" in warning for warning in oneway_warnings)
    assert fetch_progress.snapshot()["current"] == 3

    fetch_progress.start_phase("returns", total=3, detail="Start return checks.")
    with ThreadPoolExecutor(max_workers=2) as io_pool:
        return_map, return_warnings, return_base_count = asyncio.run(
            optimizer._fetch_returns_parallel(
                fetch_client,
                [
                    ("OTP", "MGA", "2026-03-10", "2026-03-24"),
                    ("OTP", "BASEONLY", "2026-03-11", "2026-03-25"),
                    ("OTP", "ERR", "2026-03-12", "2026-03-26"),
                ],
                config,
                io_pool,
                provider_map={
                    ("OTP", "MGA", "2026-03-10", "2026-03-24"): ("amadeus",),
                    ("OTP", "BASEONLY", "2026-03-11", "2026-03-25"): ("amadeus",),
                },
                base_provider_ids=("kiwi", "amadeus"),
                progress=fetch_progress,
            )
        )
    assert return_base_count == 2
    assert return_map[("OTP", "MGA", "2026-03-10", "2026-03-24")]["fare_mode"] == "selected_bags"
    assert return_map[("OTP", "MGA", "2026-03-10", "2026-03-24")]["base_no_bags_price"] == 240
    assert (
        return_map[("OTP", "BASEONLY", "2026-03-11", "2026-03-25")]["fare_mode"] == "base_no_bags"
    )
    assert return_map[("OTP", "ERR", "2026-03-12", "2026-03-26")] is None
    assert any("return boom" in warning for warning in return_warnings)

    async def _successful_search_async(*_args, **_kwargs) -> dict[str, object]:
        return {"meta": {}, "results": [{"result_id": "ok"}]}

    async def _timeout_search_async(*_args, **_kwargs) -> dict[str, object]:
        raise TimeoutError("slow")

    async def _error_search_async(*_args, **_kwargs) -> dict[str, object]:
        raise ValueError("boom")

    success_progress = SearchProgressTracker("search-success")
    monkeypatch.setattr(optimizer, "_search_async", _successful_search_async)
    success_result = optimizer.search(config, search_id="success-id", progress=success_progress)
    assert success_result["meta"]["search_id"] == "success-id"
    assert success_result["meta"]["search_timeout_seconds"] is None
    assert success_progress.snapshot()["status"] == "completed"

    timeout_progress = SearchProgressTracker("search-timeout")
    monkeypatch.setattr(optimizer, "_search_async", _timeout_search_async)
    with pytest.raises(TimeoutError, match="Search exceeded timeout"):
        optimizer.search(
            replace(config, search_timeout_seconds=30),
            search_id="timeout-id",
            progress=timeout_progress,
        )
    assert timeout_progress.snapshot()["status"] == "failed"

    error_progress = SearchProgressTracker("search-error")
    monkeypatch.setattr(optimizer, "_search_async", _error_search_async)
    with pytest.raises(ValueError, match="boom"):
        optimizer.search(config, search_id="error-id", progress=error_progress)
    assert error_progress.snapshot()["status"] == "failed"


def test_optimizer_search_async_covers_empty_finalize_and_mixed_itinerary_build_paths(
    monkeypatch,
) -> None:
    optimizer = SplitTripOptimizer(KiwiClient(), AirportCoordinates())
    base_config = optimizer.parse_search_config(
        {
            "origins": ["OTP"],
            "destinations": ["MGA"],
            "period_start": "2026-03-10",
            "period_end": "2026-03-24",
            "providers": ["kiwi", "amadeus", "serpapi"],
            "hub_candidates": ["IST", "DXB", "LCA"],
            "auto_hubs_per_direction": 2,
            "calendar_hubs_prefetch": 1,
            "max_transfers_per_direction": 2,
            "min_stay_days": 8,
            "max_stay_days": 10,
            "min_stopover_days": 0,
            "max_stopover_days": 2,
            "top_results": 1,
            "validate_top_per_destination": 6,
            "market_compare_fares": True,
            "cabin_bags_per_adult": 1,
            "hold_bags_per_adult": 0,
            "max_connection_layover_hours": 2,
            "io_workers": 2,
            "cpu_workers": 1,
        }
    )

    class _StatsOnlyClient:
        active_provider_ids = ["kiwi", "amadeus", "serpapi"]

        def __init__(self, stats: dict[str, dict[str, int]]) -> None:
            self._stats = stats

        def stats_snapshot(self) -> dict[str, dict[str, int]]:
            return {bucket: dict(values) for bucket, values in self._stats.items()}

    async def _fake_fetch_calendars(
        *_args,
        **_kwargs,
    ) -> tuple[dict[tuple[str, str], dict[str, int]], list[str]]:
        return {}, ["Calendar probe note."]

    monkeypatch.setattr(
        optimizer,
        "_expand_route_graph_hub_candidates",
        lambda cfg: (
            cfg,
            {
                "hub_candidates_graph_applied": True,
                "hub_candidates_graph_count": 2,
                "hub_candidates_graph_available": True,
                "hub_candidates_input_count": len(cfg.hub_candidates),
                "hub_candidates_graph_source": "test",
            },
            ["Route graph test warning."],
        ),
    )
    monkeypatch.setattr(optimizer, "_pick_auto_hubs", lambda *_args, **_kwargs: (["IST"], ["DXB"]))
    monkeypatch.setattr(optimizer, "_fetch_calendars_parallel", _fake_fetch_calendars)

    empty_client = _StatsOnlyClient(
        {
            "calendar_selected": {},
            "oneway_selected": {},
            "return_selected": {},
            "calendar_errors": {"amadeus": 1},
            "oneway_errors": {},
            "return_errors": {},
            "calendar_skipped_budget": {},
            "oneway_skipped_budget": {},
            "return_skipped_budget": {},
            "calendar_skipped_cooldown": {},
            "oneway_skipped_cooldown": {"serpapi": 1},
            "return_skipped_cooldown": {},
            "calendar_no_result": {},
            "oneway_no_result": {},
            "return_no_result": {},
            "calendar_calls": {},
            "oneway_calls": {},
            "return_calls": {},
        }
    )
    monkeypatch.setattr(
        optimizer,
        "_build_search_client",
        lambda cfg: (
            empty_client,
            [
                {"id": provider_id, "configured": True}
                for provider_id in empty_client.active_provider_ids
            ],
            ["Provider bootstrap warning."],
        ),
    )
    monkeypatch.setattr(
        optimizer,
        "_estimate_candidates_parallel",
        lambda *_args, **_kwargs: asyncio.sleep(0, result={}),
    )
    empty_progress = SearchProgressTracker("search-empty")
    with ThreadPoolExecutor(max_workers=2) as io_pool:
        empty_result = asyncio.run(
            optimizer._search_async(
                replace(base_config, objective="fastest"),
                io_pool,
                search_id="empty-search",
                progress=empty_progress,
            )
        )
    empty_messages = [event["message"] for event in empty_progress.snapshot()["events"]]
    assert empty_result["results"] == []
    assert "No itinerary assembly was needed." in empty_messages
    assert any("Route graph test warning." in warning for warning in empty_result["warnings"])
    assert any("Calendar probe note." in warning for warning in empty_result["warnings"])
    assert any(
        "Provider serpapi was active but got 0 live fare calls." in warning
        for warning in empty_result["warnings"]
    )
    assert any("Baggage pricing note" in warning for warning in empty_result["warnings"])

    built_client = _StatsOnlyClient(
        {
            "calendar_selected": {"kiwi": 1},
            "oneway_selected": {"kiwi": 3, "amadeus": 2},
            "return_selected": {"amadeus": 1},
            "calendar_errors": {},
            "oneway_errors": {"serpapi": 1},
            "return_errors": {},
            "calendar_skipped_budget": {"serpapi": 1},
            "oneway_skipped_budget": {},
            "return_skipped_budget": {"amadeus": 1},
            "calendar_skipped_cooldown": {},
            "oneway_skipped_cooldown": {"serpapi": 2},
            "return_skipped_cooldown": {},
            "calendar_no_result": {"kiwi": 1},
            "oneway_no_result": {},
            "return_no_result": {"amadeus": 1},
            "calendar_calls": {"kiwi": 6},
            "oneway_calls": {"kiwi": 5, "amadeus": 2},
            "return_calls": {},
        }
    )

    def seg(source: str, destination: str, depart: str, arrive: str) -> dict[str, str]:
        return {
            "from": source,
            "to": destination,
            "depart_local": depart,
            "arrive_local": arrive,
        }

    def oneway(
        source: str,
        destination: str,
        date_iso: str,
        depart: str,
        arrive: str,
        *,
        price: int,
        duration_seconds: int,
        provider: str = "kiwi",
        fare_mode: str = "selected_bags",
        price_mode: str = "explicit_total",
    ) -> tuple[tuple[str, str, str], dict[str, object]]:
        return (
            (source, destination, date_iso),
            {
                "price": price,
                "formatted_price": f"{price} RON",
                "stops": 0,
                "transfer_events": 0,
                "segments": [seg(source, destination, depart, arrive)],
                "duration_seconds": duration_seconds,
                "provider": provider,
                "fare_mode": fare_mode,
                "price_mode": price_mode,
                "booking_url": f"https://example.test/{source.lower()}-{destination.lower()}-{date_iso}",
            },
        )

    direct_candidate = {
        "candidate_type": "direct_roundtrip",
        "origin": "OTP",
        "arrival_origin": "OTP",
        "destination": "MGA",
        "depart_origin_date": "2026-03-10",
        "return_origin_date": "2026-03-24",
        "main_stay_days": 14,
        "distance_basis_km": 1000.0,
    }
    chain_candidate = {
        "candidate_type": "split_chain",
        "origin": "OTP",
        "arrival_origin": "BBU",
        "destination": "MGA",
        "outbound_hub": "IST",
        "inbound_hub": "DXB",
        "depart_origin_date": "2026-03-10",
        "depart_destination_date": "2026-03-10",
        "leave_destination_date": "2026-03-24",
        "return_origin_date": "2026-03-24",
        "outbound_boundary_stopover_days": [0],
        "inbound_boundary_stopover_days": [0],
        "outbound_legs": [
            {"source": "OTP", "destination": "IST", "date": "2026-03-10"},
            {"source": "IST", "destination": "MGA", "date": "2026-03-10"},
        ],
        "inbound_legs": [
            {"source": "MGA", "destination": "DXB", "date": "2026-03-24"},
            {"source": "DXB", "destination": "BBU", "date": "2026-03-24"},
        ],
        "main_stay_days": 14,
        "distance_basis_km": 1000.0,
    }
    stopover_candidate = {
        "candidate_type": "split_stopover",
        "origin": "OTP",
        "arrival_origin": "OTP",
        "destination": "MGA",
        "outbound_hub": "IST",
        "inbound_hub": "DXB",
        "depart_origin_date": "2026-03-11",
        "depart_destination_date": "2026-03-11",
        "leave_destination_date": "2026-03-21",
        "return_origin_date": "2026-03-21",
        "outbound_stopover_days": 0,
        "inbound_stopover_days": 0,
        "main_stay_days": 8,
        "distance_basis_km": 1200.0,
    }
    invalid_boundary_candidate = {
        **stopover_candidate,
        "depart_origin_date": "2026-03-12",
        "depart_destination_date": "2026-03-12",
        "leave_destination_date": "2026-03-22",
        "return_origin_date": "2026-03-22",
    }
    layover_filtered_candidate = {
        **stopover_candidate,
        "depart_origin_date": "2026-03-14",
        "depart_destination_date": "2026-03-14",
        "leave_destination_date": "2026-03-23",
        "return_origin_date": "2026-03-23",
        "outbound_stopover_days": 0,
        "inbound_stopover_days": 0,
    }
    limited_candidates = [
        direct_candidate,
        chain_candidate,
        stopover_candidate,
        invalid_boundary_candidate,
        layover_filtered_candidate,
    ]

    direct_return_key = ("OTP", "MGA", "2026-03-10", "2026-03-24")
    built_return_map = {
        direct_return_key: {
            "price": 500,
            "formatted_price": "500 RON",
            "currency": "RON",
            "duration_seconds": 41000,
            "outbound_duration_seconds": 20000,
            "inbound_duration_seconds": 21000,
            "outbound_stops": 0,
            "inbound_stops": 0,
            "outbound_transfer_events": 0,
            "inbound_transfer_events": 0,
            "booking_url": "https://example.test/direct-return",
            "outbound_segments": [seg("OTP", "MGA", "2026-03-10T08:00:00", "2026-03-10T09:00:00")],
            "inbound_segments": [seg("MGA", "OTP", "2026-03-24T18:00:00", "2026-03-24T19:00:00")],
            "provider": "amadeus",
            "price_mode": "explicit_total",
            "fare_mode": "selected_bags",
        }
    }
    built_oneway_map = dict(
        [
            oneway(
                "OTP",
                "IST",
                "2026-03-10",
                "2026-03-10T08:00:00",
                "2026-03-10T09:00:00",
                price=120,
                duration_seconds=3600,
                fare_mode="base_no_bags",
                price_mode="base_total",
            ),
            oneway(
                "IST",
                "MGA",
                "2026-03-10",
                "2026-03-10T11:00:00",
                "2026-03-10T12:30:00",
                price=160,
                duration_seconds=5400,
            ),
            oneway(
                "MGA",
                "DXB",
                "2026-03-24",
                "2026-03-24T08:00:00",
                "2026-03-24T09:00:00",
                price=180,
                duration_seconds=3600,
            ),
            oneway(
                "DXB",
                "BBU",
                "2026-03-24",
                "2026-03-24T11:00:00",
                "2026-03-24T13:00:00",
                price=200,
                duration_seconds=7200,
            ),
            oneway(
                "OTP",
                "IST",
                "2026-03-11",
                "2026-03-11T08:00:00",
                "2026-03-11T09:00:00",
                price=130,
                duration_seconds=3600,
            ),
            oneway(
                "IST",
                "MGA",
                "2026-03-11",
                "2026-03-11T11:00:00",
                "2026-03-11T13:00:00",
                price=150,
                duration_seconds=7200,
            ),
            oneway(
                "MGA",
                "DXB",
                "2026-03-21",
                "2026-03-21T18:00:00",
                "2026-03-21T19:00:00",
                price=140,
                duration_seconds=3600,
            ),
            oneway(
                "DXB",
                "OTP",
                "2026-03-21",
                "2026-03-21T21:00:00",
                "2026-03-21T23:00:00",
                price=130,
                duration_seconds=7200,
            ),
            oneway(
                "OTP",
                "IST",
                "2026-03-12",
                "2026-03-12T08:00:00",
                "2026-03-12T09:00:00",
                price=130,
                duration_seconds=3600,
            ),
            oneway(
                "IST",
                "MGA",
                "2026-03-12",
                "2026-03-12T09:10:00",
                "2026-03-12T11:00:00",
                price=150,
                duration_seconds=6600,
            ),
            oneway(
                "MGA",
                "DXB",
                "2026-03-22",
                "2026-03-22T08:00:00",
                "2026-03-22T09:00:00",
                price=140,
                duration_seconds=3600,
            ),
            oneway(
                "DXB",
                "OTP",
                "2026-03-22",
                "2026-03-22T09:15:00",
                "2026-03-22T11:15:00",
                price=130,
                duration_seconds=7200,
            ),
            oneway(
                "OTP",
                "IST",
                "2026-03-14",
                "2026-03-14T08:00:00",
                "2026-03-14T09:00:00",
                price=130,
                duration_seconds=3600,
            ),
            oneway(
                "IST",
                "MGA",
                "2026-03-14",
                "2026-03-14T13:30:00",
                "2026-03-14T15:00:00",
                price=150,
                duration_seconds=5400,
            ),
            oneway(
                "MGA",
                "DXB",
                "2026-03-23",
                "2026-03-23T08:00:00",
                "2026-03-23T09:00:00",
                price=140,
                duration_seconds=3600,
            ),
            oneway(
                "DXB",
                "OTP",
                "2026-03-23",
                "2026-03-23T13:30:00",
                "2026-03-23T15:30:00",
                price=130,
                duration_seconds=7200,
            ),
        ]
    )

    monkeypatch.setattr(
        optimizer,
        "_build_search_client",
        lambda cfg: (
            built_client,
            [
                {"id": provider_id, "configured": True}
                for provider_id in built_client.active_provider_ids
            ],
            ["Provider comparison warning."],
        ),
    )
    monkeypatch.setattr(
        optimizer,
        "_estimate_candidates_parallel",
        lambda *_args, **_kwargs: asyncio.sleep(0, result={"MGA": list(limited_candidates)}),
    )
    monkeypatch.setattr(
        optimizer,
        "_prepare_destination_validation_context",
        lambda **kwargs: (
            {
                "destination": "MGA",
                "destination_name": "Managua",
                "notes": {"note": "Scenario note."},
                "limited_candidates": list(limited_candidates),
                "ordered_return_keys": [direct_return_key],
                "ordered_oneway_keys": list(built_oneway_map),
                "estimated_candidates_count": len(limited_candidates),
                "validation_target": 6,
                "oneway_provider_map": {key: ("kiwi",) for key in built_oneway_map},
                "return_provider_map": {direct_return_key: ("amadeus",)},
            },
            ["Destination validation note."],
        ),
    )
    monkeypatch.setattr(
        optimizer,
        "_fetch_returns_parallel",
        lambda *_args, **_kwargs: asyncio.sleep(
            0,
            result=(dict(built_return_map), ["Return validation note."], 1),
        ),
    )
    monkeypatch.setattr(
        optimizer,
        "_fetch_oneways_parallel",
        lambda *_args, **_kwargs: asyncio.sleep(
            0,
            result=(dict(built_oneway_map), ["One-way validation note."], 2),
        ),
    )
    built_progress = SearchProgressTracker("search-built")
    with ThreadPoolExecutor(max_workers=2) as io_pool:
        built_result = asyncio.run(
            optimizer._search_async(
                replace(base_config, objective="price_per_km", top_results=2),
                io_pool,
                search_id="built-search",
                progress=built_progress,
            )
        )
    result_types = {item["itinerary_type"] for item in built_result["results"]}
    assert result_types == {"direct_roundtrip", "split_stopover"}, built_result
    assert built_result["meta"]["engine"]["base_fare_selected_oneways"] == 2
    assert built_result["meta"]["engine"]["base_fare_selected_returns"] == 1
    assert built_result["meta"]["engine"]["filtered_by_connection_layover"] == 1
    assert built_result["meta"]["engine"]["filtered_invalid_split_boundaries"] == 1
    assert built_result["meta"]["engine"]["providers_used"] == ["amadeus", "kiwi"]
    assert built_result["meta"]["engine"]["results_count_by_destination"] == {"MGA": 2}
    assert any(
        "Filtered 1 itineraries exceeding max connection layover" in warning
        for warning in built_result["warnings"]
    )
    assert any(
        "invalid or too-short self-transfer boundaries" in warning
        for warning in built_result["warnings"]
    )
    assert any(
        "Market compare mode also fetched Kiwi base no-bag fares" in warning
        for warning in built_result["warnings"]
    )
    assert any("Baggage pricing note" in warning for warning in built_result["warnings"])
    assert any(
        "Provider serpapi skipped 1 calendar skipped budget" in warning
        for warning in built_result["warnings"]
    )
    assert any(
        "Provider serpapi temporarily paused" in warning for warning in built_result["warnings"]
    )
    assert any(
        "Provider amadeus skipped 1 return skipped budget" in warning
        for warning in built_result["warnings"]
    )
    assert any(
        "Provider kiwi returned no offers on 1 calendar no result checks." in warning
        for warning in built_result["warnings"]
    )
    assert any(
        "Provider amadeus returned no offers on 1 return no result checks." in warning
        for warning in built_result["warnings"]
    )
    assert "Packed 2 result(s)" in built_progress.snapshot()["phase_detail"]


def test_optimizer_strategy_and_diversity_helpers_cover_anchor_and_capping_paths() -> None:
    optimizer = SplitTripOptimizer(KiwiClient(), AirportCoordinates())

    def result(
        result_id: str,
        destination_code: str,
        itinerary_type: str,
        total_price: int,
        outbound_time: int | None,
        outbound_layovers: int,
        inbound_layovers: int,
        *,
        outbound_stopover_days: int = 0,
        inbound_stopover_days: int = 0,
        outbound_hub: str = "DIRECT",
        inbound_hub: str = "DIRECT",
    ) -> dict[str, object]:
        return {
            "result_id": result_id,
            "destination_code": destination_code,
            "itinerary_type": itinerary_type,
            "total_price": total_price,
            "outbound_time_to_destination_seconds": outbound_time,
            "outbound": {
                "hub": outbound_hub,
                "layovers_count": outbound_layovers,
                "stopover_days": outbound_stopover_days,
            },
            "inbound": {
                "hub": inbound_hub,
                "layovers_count": inbound_layovers,
                "stopover_days": inbound_stopover_days,
            },
        }

    ranked = [
        result("mga-direct", "MGA", "direct_roundtrip", 700, 30_000, 0, 0),
        result("mga-one-stop", "MGA", "split_stopover", 650, 32_000, 1, 1, outbound_hub="IST"),
        result("mga-two-stop", "MGA", "split_stopover", 640, 35_000, 2, 2, outbound_hub="DXB"),
        result(
            "mga-long-stop",
            "MGA",
            "split_stopover",
            630,
            36_000,
            1,
            1,
            outbound_stopover_days=2,
            outbound_hub="LCA",
        ),
        result("usm-direct", "USM", "direct_roundtrip", 620, 28_000, 0, 0),
    ]

    selected = optimizer._merge_strategy_anchors(ranked, top_results=5)
    selected_ids = [item["result_id"] for item in selected]
    assert "mga-direct" in selected_ids
    assert "mga-long-stop" in selected_ids
    assert "usm-direct" in selected_ids
    assert optimizer._merge_strategy_anchors([], top_results=5) == []
    assert optimizer._merge_strategy_anchors(ranked, top_results=0) == []

    capped, counts = optimizer._cap_results_per_destination(
        ranked,
        top_results_per_destination=2,
        destination_order=["USM", "MGA"],
        required_by_destination={
            "MGA": [ranked[3], ranked[3], {"result_id": "", "destination_code": "MGA"}]
        },
    )
    assert counts == {"USM": 1, "MGA": 2}
    assert [item["destination_code"] for item in capped] == ["USM", "MGA", "MGA"]
    assert capped[1]["result_id"] == "mga-long-stop"
    assert optimizer._cap_results_per_destination([], top_results_per_destination=2) == ([], {})

    split_chain_a = {
        "candidate_type": "split_chain",
        "origin": "OTP",
        "arrival_origin": "OTP",
        "outbound_hub": "IST/BKK",
        "inbound_hub": "AMM/BUD",
        "depart_origin_date": "2026-04-25",
        "depart_destination_date": "2026-04-27",
        "leave_destination_date": "2026-05-05",
        "return_origin_date": "2026-05-08",
        "outbound_boundary_stopover_days": [0, 1],
        "inbound_boundary_stopover_days": [1, 0],
        "outbound_legs": [
            {"source": "OTP", "destination": "IST", "date": "2026-04-25T08:00:00"},
            {"source": "IST", "destination": "BKK", "date": "2026-04-25T11:00:00"},
            {"source": "BKK", "destination": "USM", "date": "2026-04-27T06:00:00"},
        ],
        "inbound_legs": [
            {"source": "USM", "destination": "AMM", "date": "2026-05-05T06:00:00"},
            {"source": "AMM", "destination": "BUD", "date": "2026-05-05T12:00:00"},
            {"source": "BUD", "destination": "OTP", "date": "2026-05-08T09:00:00"},
        ],
    }
    split_chain_duplicate = dict(split_chain_a)
    split_chain_outbound_diverse = {
        **split_chain_a,
        "outbound_hub": "ATH/BKK",
        "outbound_legs": [
            {"source": "OTP", "destination": "ATH", "date": "2026-04-25T08:00:00"},
            {"source": "ATH", "destination": "BKK", "date": "2026-04-25T11:00:00"},
            {"source": "BKK", "destination": "USM", "date": "2026-04-27T06:00:00"},
        ],
    }
    split_chain_inbound_diverse = {
        **split_chain_a,
        "inbound_hub": "DOH/BUD",
        "inbound_legs": [
            {"source": "USM", "destination": "DOH", "date": "2026-05-05T06:00:00"},
            {"source": "DOH", "destination": "BUD", "date": "2026-05-05T12:00:00"},
            {"source": "BUD", "destination": "OTP", "date": "2026-05-08T09:00:00"},
        ],
    }
    split_stopover = {
        "candidate_type": "split_stopover",
        "origin": "OTP",
        "arrival_origin": "OTP",
        "outbound_hub": "IST",
        "inbound_hub": "DXB",
        "depart_origin_date": "2026-04-25",
        "depart_destination_date": "2026-04-27",
        "leave_destination_date": "2026-05-05",
        "return_origin_date": "2026-05-08",
    }

    assert optimizer._split_candidate_key(split_stopover)[:2] == ("split_stopover", "OTP")
    assert optimizer._split_candidate_key(split_chain_a) != optimizer._split_candidate_key(
        split_chain_outbound_diverse
    )

    diverse = optimizer._select_split_candidates_with_diversity(
        [
            split_chain_a,
            split_chain_duplicate,
            split_chain_outbound_diverse,
            split_chain_inbound_diverse,
            split_stopover,
        ],
        base_quota=1,
        extra_quota=3,
    )
    diverse_keys = {optimizer._split_candidate_key(item) for item in diverse}
    assert len(diverse) == 4
    assert len(diverse_keys) == 4
    assert optimizer._select_split_candidates_with_diversity([], base_quota=1, extra_quota=1) == []
    assert (
        optimizer._select_split_candidates_with_diversity(
            [split_chain_a], base_quota=0, extra_quota=2
        )
        == []
    )


def test_optimizer_remaining_parallel_and_split_chain_paths_cover_real_executor_and_chain_assembly(
    monkeypatch,
) -> None:
    optimizer = SplitTripOptimizer(KiwiClient(), AirportCoordinates())

    class _FakeProcessPool:
        created: list[_FakeProcessPool] = []

        def __init__(self, max_workers: int) -> None:
            self.max_workers = max_workers
            self.shutdown_calls: list[tuple[bool, bool]] = []
            self.__class__.created.append(self)

        def submit(self, fn, *args, **kwargs):  # type: ignore[no-untyped-def]
            future: Future = Future()
            try:
                future.set_result(fn(*args, **kwargs))
            except Exception as exc:  # pragma: no cover - exercised by assertions below
                future.set_exception(exc)
            return future

        def shutdown(self, wait: bool = True, cancel_futures: bool = False) -> None:
            self.shutdown_calls.append((wait, cancel_futures))

    def _fake_estimator(task: dict[str, object]) -> list[dict[str, object]]:
        destination = str(task["destination"])
        if destination == "ERR":
            raise RuntimeError("estimator boom")
        return [{"destination": destination, "estimated_total": 100, "estimated_score": 100.0}]

    monkeypatch.setattr(optimizer_module, "ProcessPoolExecutor", _FakeProcessPool)
    monkeypatch.setattr(
        optimizer_module,
        "_estimate_candidates_for_destination",
        _fake_estimator,
    )

    parallel_config = optimizer.parse_search_config(
        {
            "origins": ["OTP"],
            "destinations": ["USM"],
            "period_start": "2026-03-10",
            "period_end": "2026-03-24",
            "cpu_workers": 4,
        }
    )
    parallel_progress = SearchProgressTracker("parallel-coverage")
    parallel_progress.start_phase("candidates", total=2, detail="Scoring.")
    parallel_results = asyncio.run(
        optimizer._estimate_candidates_parallel(
            [{"destination": "USM"}, {"destination": "MGA"}],
            parallel_config,
            parallel_progress,
        )
    )
    assert set(parallel_results) == {"USM", "MGA"}
    assert _FakeProcessPool.created[0].max_workers == 2
    assert _FakeProcessPool.created[0].shutdown_calls[-1] == (True, False)
    assert "Scored 2/2 destination candidate pools" in parallel_progress.snapshot()["phase_detail"]

    with pytest.raises(RuntimeError, match="estimator boom"):
        asyncio.run(
            optimizer._estimate_candidates_parallel(
                [{"destination": "ERR"}, {"destination": "USM"}],
                parallel_config,
            )
        )
    assert _FakeProcessPool.created[1].shutdown_calls[-1] == (False, True)

    class _StatsOnlyClient:
        active_provider_ids = ["kiwi"]

        def stats_snapshot(self) -> dict[str, dict[str, int]]:
            return {
                "calendar_selected": {"kiwi": 1},
                "oneway_selected": {"kiwi": 18},
                "return_selected": {},
                "calendar_errors": {},
                "oneway_errors": {},
                "return_errors": {},
                "calendar_skipped_budget": {},
                "oneway_skipped_budget": {},
                "return_skipped_budget": {},
                "calendar_skipped_cooldown": {},
                "oneway_skipped_cooldown": {},
                "return_skipped_cooldown": {},
                "calendar_no_result": {},
                "oneway_no_result": {},
                "return_no_result": {},
                "calendar_calls": {"kiwi": 4},
                "oneway_calls": {"kiwi": 18},
                "return_calls": {},
            }

    def seg(
        source: str,
        destination: str,
        depart: str,
        arrive: str,
    ) -> dict[str, str]:
        return {
            "from": source,
            "to": destination,
            "depart_local": depart,
            "arrive_local": arrive,
        }

    def fare(
        source: str,
        destination: str,
        date_iso: str,
        segments: list[dict[str, str]],
        *,
        price: int,
        duration_seconds: int | None,
        fare_mode: str = "selected_bags",
        price_mode: str = "explicit_total",
        booking_url: str | None = None,
        provider: str = "kiwi",
    ) -> tuple[tuple[str, str, str], dict[str, object]]:
        return (
            (source, destination, date_iso),
            {
                "price": price,
                "formatted_price": f"{price} RON",
                "stops": max(0, len(segments) - 1),
                "transfer_events": max(0, len(segments) - 1),
                "segments": segments,
                "duration_seconds": duration_seconds,
                "provider": provider,
                "fare_mode": fare_mode,
                "price_mode": price_mode,
                "booking_url": booking_url,
            },
        )

    def chain_candidate(
        depart_origin_date: str,
        depart_destination_date: str,
        leave_destination_date: str,
        return_origin_date: str,
        *,
        outbound_boundary_days: list[int],
        inbound_boundary_days: list[int],
        outbound_hub: str = "IST/BKK",
        inbound_hub: str = "BKK/AMM",
    ) -> dict[str, object]:
        return {
            "candidate_type": "split_chain",
            "destination": "USM",
            "origin": "OTP",
            "arrival_origin": "OTP",
            "outbound_hub": outbound_hub,
            "inbound_hub": inbound_hub,
            "depart_origin_date": depart_origin_date,
            "depart_destination_date": depart_destination_date,
            "leave_destination_date": leave_destination_date,
            "return_origin_date": return_origin_date,
            "outbound_stopover_days": sum(outbound_boundary_days),
            "inbound_stopover_days": sum(inbound_boundary_days),
            "outbound_boundary_stopover_days": list(outbound_boundary_days),
            "inbound_boundary_stopover_days": list(inbound_boundary_days),
            "main_stay_days": 8,
            "distance_basis_km": 7997.3,
            "outbound_legs": [
                {"source": "OTP", "destination": "IST", "date": depart_origin_date},
                {"source": "IST", "destination": "BKK", "date": depart_origin_date},
                {"source": "BKK", "destination": "USM", "date": depart_destination_date},
            ],
            "inbound_legs": [
                {"source": "USM", "destination": "BKK", "date": leave_destination_date},
                {"source": "BKK", "destination": "AMM", "date": leave_destination_date},
                {"source": "AMM", "destination": "OTP", "date": return_origin_date},
            ],
        }

    valid_candidate = chain_candidate(
        "2026-03-10",
        "2026-03-12",
        "2026-03-20",
        "2026-03-22",
        outbound_boundary_days=[0, 2],
        inbound_boundary_days=[0, 2],
    )
    missing_materialized_candidate = chain_candidate(
        "2026-03-11",
        "2026-03-13",
        "2026-03-21",
        "2026-03-23",
        outbound_boundary_days=[0, 2],
        inbound_boundary_days=[0, 2],
    )
    outbound_layover_filtered_candidate = chain_candidate(
        "2026-03-12",
        "2026-03-14",
        "2026-03-20",
        "2026-03-22",
        outbound_boundary_days=[0, 0],
        inbound_boundary_days=[0, 2],
    )
    outbound_invalid_boundary_candidate = chain_candidate(
        "2026-03-13",
        "2026-03-15",
        "2026-03-20",
        "2026-03-22",
        outbound_boundary_days=[0, 0],
        inbound_boundary_days=[0, 2],
    )
    outbound_connection_filtered_candidate = chain_candidate(
        "2026-03-14",
        "2026-03-16",
        "2026-03-20",
        "2026-03-22",
        outbound_boundary_days=[0, 0],
        inbound_boundary_days=[0, 2],
    )
    inbound_invalid_boundary_candidate = chain_candidate(
        "2026-03-15",
        "2026-03-17",
        "2026-03-21",
        "2026-03-23",
        outbound_boundary_days=[0, 2],
        inbound_boundary_days=[0, 0],
    )
    inbound_connection_filtered_candidate = chain_candidate(
        "2026-03-16",
        "2026-03-18",
        "2026-03-22",
        "2026-03-24",
        outbound_boundary_days=[0, 2],
        inbound_boundary_days=[0, 0],
    )
    limited_candidates = [
        {
            "candidate_type": "split_chain",
            "destination": "USM",
            "origin": "OTP",
            "arrival_origin": "OTP",
            "main_stay_days": 8,
            "distance_basis_km": 7997.3,
        },
        missing_materialized_candidate,
        outbound_layover_filtered_candidate,
        outbound_invalid_boundary_candidate,
        outbound_connection_filtered_candidate,
        inbound_invalid_boundary_candidate,
        inbound_connection_filtered_candidate,
        valid_candidate,
    ]

    built_oneway_map = dict(
        [
            fare(
                "OTP",
                "IST",
                "2026-03-10",
                [seg("OTP", "IST", "2026-03-10T08:00:00", "2026-03-10T09:00:00")],
                price=200,
                duration_seconds=3600,
                booking_url=None,
            ),
            fare(
                "IST",
                "BKK",
                "2026-03-10",
                [seg("IST", "BKK", "2026-03-10T11:00:00", "2026-03-10T15:00:00")],
                price=800,
                duration_seconds=14_400,
                fare_mode="base_no_bags",
                price_mode="per_person_scaled",
            ),
            fare(
                "BKK",
                "USM",
                "2026-03-12",
                [seg("BKK", "USM", "2026-03-12T08:00:00", "2026-03-12T09:00:00")],
                price=300,
                duration_seconds=3600,
            ),
            fare(
                "USM",
                "BKK",
                "2026-03-20",
                [seg("USM", "BKK", "2026-03-20T08:00:00", "2026-03-20T09:00:00")],
                price=320,
                duration_seconds=3600,
            ),
            fare(
                "BKK",
                "AMM",
                "2026-03-20",
                [seg("BKK", "AMM", "2026-03-20T11:00:00", "2026-03-20T16:00:00")],
                price=700,
                duration_seconds=18_000,
                booking_url="https://example.test/bkk-amm",
            ),
            fare(
                "AMM",
                "OTP",
                "2026-03-22",
                [seg("AMM", "OTP", "2026-03-22T10:00:00", "2026-03-22T12:00:00")],
                price=210,
                duration_seconds=7200,
            ),
            fare(
                "OTP",
                "IST",
                "2026-03-12",
                [
                    seg("OTP", "SOF", "2026-03-12T08:00:00", "2026-03-12T09:00:00"),
                    seg("SOF", "IST", "2026-03-12T13:30:00", "2026-03-12T15:00:00"),
                ],
                price=220,
                duration_seconds=25_200,
            ),
            fare(
                "IST",
                "BKK",
                "2026-03-12",
                [seg("IST", "BKK", "2026-03-12T17:00:00", "2026-03-12T21:00:00")],
                price=810,
                duration_seconds=14_400,
            ),
            fare(
                "BKK",
                "USM",
                "2026-03-14",
                [seg("BKK", "USM", "2026-03-14T08:00:00", "2026-03-14T09:00:00")],
                price=310,
                duration_seconds=3600,
            ),
            fare(
                "USM",
                "BKK",
                "2026-03-20",
                [seg("USM", "BKK", "2026-03-20T08:00:00", "2026-03-20T09:00:00")],
                price=320,
                duration_seconds=3600,
            ),
            fare(
                "OTP",
                "IST",
                "2026-03-13",
                [seg("OTP", "IST", "2026-03-13T08:00:00", "2026-03-13T09:00:00")],
                price=220,
                duration_seconds=3600,
            ),
            fare(
                "IST",
                "BKK",
                "2026-03-13",
                [seg("IST", "BKK", "2026-03-13T09:10:00", "2026-03-13T13:10:00")],
                price=820,
                duration_seconds=14_400,
            ),
            fare(
                "BKK",
                "USM",
                "2026-03-15",
                [seg("BKK", "USM", "2026-03-15T08:00:00", "2026-03-15T09:00:00")],
                price=315,
                duration_seconds=3600,
            ),
            fare(
                "OTP",
                "IST",
                "2026-03-14",
                [seg("OTP", "IST", "2026-03-14T08:00:00", "2026-03-14T09:00:00")],
                price=220,
                duration_seconds=3600,
            ),
            fare(
                "IST",
                "BKK",
                "2026-03-14",
                [seg("IST", "BKK", "2026-03-14T13:30:00", "2026-03-14T17:30:00")],
                price=830,
                duration_seconds=14_400,
            ),
            fare(
                "BKK",
                "USM",
                "2026-03-16",
                [seg("BKK", "USM", "2026-03-16T08:00:00", "2026-03-16T09:00:00")],
                price=315,
                duration_seconds=3600,
            ),
            fare(
                "OTP",
                "IST",
                "2026-03-15",
                [seg("OTP", "IST", "2026-03-15T08:00:00", "2026-03-15T09:00:00")],
                price=220,
                duration_seconds=3600,
            ),
            fare(
                "IST",
                "BKK",
                "2026-03-15",
                [seg("IST", "BKK", "2026-03-15T11:00:00", "2026-03-15T15:00:00")],
                price=830,
                duration_seconds=14_400,
            ),
            fare(
                "BKK",
                "USM",
                "2026-03-17",
                [seg("BKK", "USM", "2026-03-17T08:00:00", "2026-03-17T09:00:00")],
                price=315,
                duration_seconds=3600,
            ),
            fare(
                "USM",
                "BKK",
                "2026-03-21",
                [seg("USM", "BKK", "2026-03-21T08:00:00", "2026-03-21T09:00:00")],
                price=320,
                duration_seconds=3600,
            ),
            fare(
                "BKK",
                "AMM",
                "2026-03-21",
                [seg("BKK", "AMM", "2026-03-21T09:10:00", "2026-03-21T14:10:00")],
                price=700,
                duration_seconds=18_000,
            ),
            fare(
                "AMM",
                "OTP",
                "2026-03-23",
                [seg("AMM", "OTP", "2026-03-23T10:00:00", "2026-03-23T12:00:00")],
                price=210,
                duration_seconds=7200,
            ),
            fare(
                "OTP",
                "IST",
                "2026-03-16",
                [seg("OTP", "IST", "2026-03-16T08:00:00", "2026-03-16T09:00:00")],
                price=220,
                duration_seconds=3600,
            ),
            fare(
                "IST",
                "BKK",
                "2026-03-16",
                [seg("IST", "BKK", "2026-03-16T11:00:00", "2026-03-16T15:00:00")],
                price=830,
                duration_seconds=14_400,
            ),
            fare(
                "BKK",
                "USM",
                "2026-03-18",
                [seg("BKK", "USM", "2026-03-18T08:00:00", "2026-03-18T09:00:00")],
                price=315,
                duration_seconds=3600,
            ),
            fare(
                "USM",
                "BKK",
                "2026-03-22",
                [seg("USM", "BKK", "2026-03-22T08:00:00", "2026-03-22T09:00:00")],
                price=320,
                duration_seconds=3600,
            ),
            fare(
                "BKK",
                "AMM",
                "2026-03-22",
                [seg("BKK", "AMM", "2026-03-22T13:30:00", "2026-03-22T18:30:00")],
                price=700,
                duration_seconds=18_000,
            ),
            fare(
                "AMM",
                "OTP",
                "2026-03-24",
                [seg("AMM", "OTP", "2026-03-24T10:00:00", "2026-03-24T12:00:00")],
                price=210,
                duration_seconds=7200,
            ),
        ]
    )

    base_config = optimizer.parse_search_config(
        {
            "origins": ["OTP"],
            "destinations": ["USM"],
            "period_start": "2026-03-10",
            "period_end": "2026-03-24",
            "max_transfers_per_direction": 2,
            "max_connection_layover_hours": 2,
            "top_results": 10,
        }
    )

    monkeypatch.setattr(
        optimizer,
        "_expand_route_graph_hub_candidates",
        lambda cfg: (
            cfg,
            {
                "hub_candidates_graph_applied": False,
                "hub_candidates_graph_count": 0,
                "hub_candidates_graph_available": True,
                "hub_candidates_input_count": len(cfg.hub_candidates),
                "hub_candidates_graph_source": "test",
            },
            [],
        ),
    )
    monkeypatch.setattr(optimizer, "_pick_auto_hubs", lambda *_args, **_kwargs: (["IST"], ["AMM"]))
    monkeypatch.setattr(
        optimizer,
        "_fetch_calendars_parallel",
        lambda *_args, **_kwargs: asyncio.sleep(0, result=({}, [])),
    )
    monkeypatch.setattr(
        optimizer,
        "_build_search_client",
        lambda cfg: (
            _StatsOnlyClient(),
            [{"id": "kiwi", "configured": True}],
            [],
        ),
    )
    monkeypatch.setattr(
        optimizer,
        "_estimate_candidates_parallel",
        lambda *_args, **_kwargs: asyncio.sleep(0, result={"USM": list(limited_candidates)}),
    )
    monkeypatch.setattr(
        optimizer,
        "_prepare_destination_validation_context",
        lambda **_kwargs: (
            {
                "destination": "USM",
                "destination_name": "Ko Samui",
                "notes": {"note": "Island chain."},
                "limited_candidates": list(limited_candidates),
                "ordered_return_keys": [],
                "ordered_oneway_keys": list(built_oneway_map),
                "estimated_candidates_count": len(limited_candidates),
                "validation_target": len(limited_candidates),
                "oneway_provider_map": {key: ("kiwi",) for key in built_oneway_map},
                "return_provider_map": {},
            },
            [],
        ),
    )
    monkeypatch.setattr(
        optimizer,
        "_fetch_returns_parallel",
        lambda *_args, **_kwargs: asyncio.sleep(0, result=({}, [], 0)),
    )
    monkeypatch.setattr(
        optimizer,
        "_fetch_oneways_parallel",
        lambda *_args, **_kwargs: asyncio.sleep(0, result=(dict(built_oneway_map), [], 0)),
    )

    search_progress = SearchProgressTracker("split-chain-coverage")
    with ThreadPoolExecutor(max_workers=2) as io_pool:
        result = asyncio.run(
            optimizer._search_async(
                base_config,
                io_pool,
                search_id="split-chain-coverage",
                progress=search_progress,
            )
        )

    assert len(result["results"]) == 1
    packed = result["results"][0]
    assert packed["destination_code"] == "USM"
    assert packed["outbound"]["hub"] == "IST/BKK"
    assert packed["inbound"]["hub"] == "BKK/AMM"
    assert packed["fare_mode"] == "base_no_bags"
    assert packed["price_modes"] == ["explicit_total", "per_person_scaled"]
    assert packed["outbound_time_to_destination_seconds"] == 194_400
    assert packed["inbound_time_to_origin_seconds"] == 201_600
    assert len(packed["legs"]) == 6
    assert any("kiwi.com" in str(leg["booking_url"]) for leg in packed["legs"])
    assert result["meta"]["engine"]["filtered_by_connection_layover"] == 3
    assert result["meta"]["engine"]["filtered_invalid_split_boundaries"] == 2
    assert any(
        "Filtered 3 itineraries exceeding max connection layover" in warning
        for warning in result["warnings"]
    )
    assert any(
        "Filtered 2 split itineraries with invalid or too-short self-transfer boundaries" in warning
        for warning in result["warnings"]
    )


def test_optimizer_chunked_candidate_parallelism_and_progress_cover_chunk_scheduler(
    monkeypatch,
) -> None:
    optimizer = SplitTripOptimizer(KiwiClient(), AirportCoordinates())
    captured_chunks: list[dict[str, object]] = []

    class _FakeChunkProcessPool:
        created: list[_FakeChunkProcessPool] = []

        def __init__(self, max_workers: int, initializer=None, initargs=()) -> None:  # type: ignore[no-untyped-def]
            self.max_workers = max_workers
            self.shutdown_calls: list[tuple[bool, bool]] = []
            if initializer is not None:
                initializer(*initargs)
            self.__class__.created.append(self)

        def submit(self, fn, *args, **kwargs):  # type: ignore[no-untyped-def]
            future: Future = Future()
            try:
                future.set_result(fn(*args, **kwargs))
            except Exception as exc:  # pragma: no cover - asserted below
                future.set_exception(exc)
            return future

        def shutdown(self, wait: bool = True, cancel_futures: bool = False) -> None:
            self.shutdown_calls.append((wait, cancel_futures))

    def _fake_chunk_estimator(chunk: dict[str, object]) -> tuple[str, list[dict[str, object]]]:
        captured_chunks.append(chunk)
        destination = str(chunk["destination"])
        start_index = int(chunk["chunk_start_index"])
        end_index = int(chunk["chunk_end_index"])
        return (
            destination,
            [
                {
                    "candidate_type": "split_stopover",
                    "destination": destination,
                    "origin": "OTP",
                    "arrival_origin": "OTP",
                    "outbound_hub": "IST",
                    "inbound_hub": "IST",
                    "depart_origin_date": f"2026-04-{10 + start_index:02d}",
                    "depart_destination_date": f"2026-04-{11 + start_index:02d}",
                    "leave_destination_date": f"2026-04-{12 + start_index:02d}",
                    "return_origin_date": f"2026-04-{12 + end_index:02d}",
                    "outbound_stopover_days": 0,
                    "inbound_stopover_days": 0,
                    "main_stay_days": 1,
                    "estimated_total": 1000 + start_index,
                    "distance_basis_km": 1000.0,
                    "estimated_score": float(1000 + start_index),
                    "estimated_outbound_time_to_destination_seconds": 7200,
                }
            ],
        )

    monkeypatch.setattr(optimizer_module, "ProcessPoolExecutor", _FakeChunkProcessPool)
    monkeypatch.setattr(optimizer_module, "_estimate_candidates_for_chunk", _fake_chunk_estimator)

    progress = SearchProgressTracker("chunked-parallel")
    progress.start_phase("candidates", total=2, detail="Scoring candidate pools.")
    config = optimizer.parse_search_config(
        {
            "origins": ["OTP"],
            "destinations": ["USM", "MGA"],
            "period_start": "2026-04-10",
            "period_end": "2026-04-15",
            "cpu_workers": 8,
        }
    )
    task_template = {
        "origins": ["OTP"],
        "outbound_hubs": ["IST"],
        "inbound_hubs": ["IST"],
        "period_start": "2026-04-10",
        "period_end": "2026-04-15",
        "min_stay_days": 1,
        "max_stay_days": 1,
        "min_stopover_days": 0,
        "max_stopover_days": 0,
        "objective": "cheapest",
        "max_candidates": 10,
        "max_direct_candidates": 4,
        "max_transfers_per_direction": 1,
        "origin_to_hub": {"OTP|IST": {"2026-04-10": 100}},
        "hub_to_origin": {"IST|OTP": {"2026-04-12": 100}},
        "hub_to_destination": {"IST": {"2026-04-11": 100}},
        "destination_to_hub": {"IST": {"2026-04-12": 100}},
        "hub_to_hub": {},
        "origin_to_destination": {"OTP|USM": {"2026-04-10": 200}},
        "destination_to_origin": {"USM|OTP": {"2026-04-12": 200}},
        "destination_distance_map": {"OTP|USM": 1000.0},
    }

    results = asyncio.run(
        optimizer._estimate_candidates_parallel(
            [
                {**task_template, "destination": "USM"},
                {
                    **task_template,
                    "destination": "MGA",
                    "origin_to_destination": {"OTP|MGA": {"2026-04-10": 220}},
                    "destination_to_origin": {"MGA|OTP": {"2026-04-12": 220}},
                    "destination_distance_map": {"OTP|MGA": 1200.0},
                },
            ],
            config,
            progress,
        )
    )

    assert set(results) == {"USM", "MGA"}
    assert len(captured_chunks) > 2
    assert _FakeChunkProcessPool.created[0].max_workers == min(8, len(captured_chunks))
    assert _FakeChunkProcessPool.created[0].shutdown_calls[-1] == (True, False)
    assert all("chunk_label" in chunk for chunk in captured_chunks)
    assert "candidate chunks" in progress.snapshot()["phase_detail"]


def test_optimizer_free_provider_discovery_helpers_cover_sampling_seed_build_probe_and_run(
    monkeypatch,
) -> None:
    optimizer = SplitTripOptimizer(KiwiClient(), AirportCoordinates())

    assert optimizer._sample_discovery_dates((), max_dates=4) == ()
    assert optimizer._sample_discovery_dates(("2026-04-20",), max_dates=4) == ("2026-04-20",)
    assert optimizer._sample_discovery_dates(
        tuple(f"2026-04-{day:02d}" for day in range(20, 25)),
        max_dates=1,
    ) == ("2026-04-20",)
    assert optimizer._sample_discovery_dates(
        tuple(f"2026-04-{day:02d}" for day in range(20, 25)),
        max_dates=3,
    ) == ("2026-04-20", "2026-04-22", "2026-04-24")

    task = {
        "destination": "USM",
        "date_keys": ("2026-04-20", "2026-04-21", "2026-04-22", "2026-04-23"),
        "origin_to_destination": {("OTP", "USM"): (700, None, 680, 690)},
        "destination_to_origin": {("USM", "OTP"): (710, 720, None, 700)},
        "origin_to_hub": {("OTP", "BKK"): (300, 320, 310, None), ("", "BKK"): (1, 2, 3, 4)},
        "hub_to_origin": {("BKK", "OTP"): (320, 310, None, 300)},
        "hub_to_destination": {"BKK": (140, 150, 145, 148)},
        "destination_to_hub": {"BKK": (150, 145, 148, 149)},
        "hub_to_hub": {("IST", "BKK"): (180, 175, 170, 172)},
    }
    seed_map = optimizer._build_initial_free_provider_discovery_seed_map(task=task)
    assert ("OTP", "USM") in seed_map
    assert ("BKK", "USM") in seed_map
    assert ("USM", "BKK") in seed_map
    assert ("", "BKK") not in seed_map
    assert all(len(dates) <= 8 for dates in seed_map.values())

    config = optimizer.parse_search_config(
        {
            "origins": ["OTP"],
            "destinations": ["USM"],
            "period_start": "2026-04-20",
            "period_end": "2026-04-23",
            "providers": ["kiwi", "kayak", "momondo", "googleflights"],
            "cpu_workers": 1,
            "io_workers": 2,
        }
    )
    assert (
        optimizer._build_free_provider_discovery_seed_map(
            destination="USM",
            estimated_candidates=[],
            config=config,
        )
        == {}
    )

    class _DiscoveryClient:
        active_provider_ids = ["kiwi", "kayak", "momondo"]

        def __init__(self) -> None:
            self._calls: dict[tuple[str, str, str], int] = {}

        def get_best_oneway(self, **kwargs):  # type: ignore[no-untyped-def]
            source = kwargs["source"]
            destination = kwargs["destination"]
            date_iso = kwargs["departure_iso"]
            key = (source, destination, date_iso)
            self._calls[key] = self._calls.get(key, 0) + 1
            if destination == "DXB":
                raise RuntimeError("discovery exploded")
            if destination == "LCA":
                return None
            if source == "OTP" and destination == "IST" and date_iso == "2026-04-20":
                return {"price": 150 if self._calls[key] == 1 else 120}
            if source == "OTP" and destination == "IST" and date_iso == "2026-04-21":
                return {"price": 0}
            return None

    discovery_client = _DiscoveryClient()
    with ThreadPoolExecutor(max_workers=2) as io_pool:
        discovered, warnings = asyncio.run(
            optimizer._probe_free_provider_discovery(
                search_client=discovery_client,  # type: ignore[arg-type]
                provider_ids=("kayak", "momondo"),
                route_dates={
                    ("OTP", "IST"): ("2026-04-20", "2026-04-20", "2026-04-21"),
                    ("OTP", "LCA"): ("2026-04-20",),
                    ("OTP", "DXB"): ("2026-04-20",),
                },
                config=config,
                io_pool=io_pool,
                io_cap=2,
            )
        )
        empty_discovered, empty_warnings = asyncio.run(
            optimizer._probe_free_provider_discovery(
                search_client=_DiscoveryClient(),  # type: ignore[arg-type]
                provider_ids=(),
                route_dates={},
                config=config,
                io_pool=io_pool,
            )
        )
    assert discovered == {("OTP", "IST"): {"2026-04-20": 120}}
    assert any("OTP->DXB 2026-04-20" in warning for warning in warnings)
    assert empty_discovered == {}
    assert empty_warnings == []

    progress = SearchProgressTracker("free-discovery")

    async def _fake_probe(**kwargs):  # type: ignore[no-untyped-def]
        if kwargs["route_dates"]:
            return ({("OTP", "USM"): {"2026-04-20": 111}}, ["probe warning"])
        return ({}, [])

    monkeypatch.setattr(optimizer, "_probe_free_provider_discovery", _fake_probe)

    with ThreadPoolExecutor(max_workers=2) as io_pool:
        updated_tasks, metadata, run_warnings = asyncio.run(
            optimizer._run_initial_free_provider_discovery(
                search_client=_DiscoveryClient(),  # type: ignore[arg-type]
                candidate_tasks=[
                    task,
                    {
                        "destination": "MGA",
                        "date_keys": (),
                        "origin_to_destination": {},
                        "destination_to_origin": {},
                        "origin_to_hub": {},
                        "hub_to_origin": {},
                        "hub_to_destination": {},
                        "destination_to_hub": {},
                        "hub_to_hub": {},
                    },
                ],
                config=config,
                io_pool=io_pool,
                progress=progress,
            )
        )

    assert updated_tasks[0]["origin_to_destination"][("OTP", "USM")][0] == 111
    assert updated_tasks[1]["destination"] == "MGA"
    assert metadata["USM"]["discovered_routes"] == 1
    assert metadata["USM"]["discovered_price_points"] == 1
    assert run_warnings == ["probe warning"]
    messages = [event["message"] for event in progress.snapshot()["events"]]
    assert any("Free-provider discovery" in message for message in messages)
    assert any("USM: probing" in message for message in messages)


def test_optimizer_coverage_audit_helpers_cover_empty_and_success_paths(monkeypatch) -> None:
    optimizer = SplitTripOptimizer(KiwiClient(), AirportCoordinates())
    config = optimizer.parse_search_config(
        {
            "origins": ["OTP"],
            "destinations": ["USM"],
            "period_start": "2026-04-20",
            "period_end": "2026-04-23",
            "providers": ["kiwi", "kayak", "momondo"],
            "cpu_workers": 1,
        }
    )

    class _AuditClient:
        active_provider_ids = ["kiwi", "kayak", "momondo"]

    with ThreadPoolExecutor(max_workers=2) as io_pool:
        empty_audit = asyncio.run(
            optimizer._run_coverage_audit(
                search_client=_AuditClient(),  # type: ignore[arg-type]
                candidate_tasks=[],
                estimated_by_destination={},
                config=config,
                io_pool=io_pool,
                progress=None,
            )
        )
    assert empty_audit == ({}, {}, [])

    candidate_task = {
        "destination": "USM",
        "date_keys": ("2026-04-20", "2026-04-21"),
        "max_candidates": 10,
        "max_direct_candidates": 4,
        "origin_to_hub": {("OTP", "BKK"): (300, 310)},
        "hub_to_origin": {("BKK", "OTP"): (320, 330)},
        "hub_to_destination": {"BKK": (120, 125)},
        "destination_to_hub": {"BKK": (130, 135)},
        "hub_to_hub": {},
        "origin_to_destination": {("OTP", "USM"): (700, 680)},
        "destination_to_origin": {("USM", "OTP"): (710, 690)},
    }
    estimated_by_destination = {
        "USM": [
            {
                "candidate_type": "split_stopover",
                "destination": "USM",
                "origin": "OTP",
                "arrival_origin": "OTP",
                "outbound_hub": "BKK",
                "inbound_hub": "BKK",
                "depart_origin_date": "2026-04-20",
                "depart_destination_date": "2026-04-20",
                "leave_destination_date": "2026-04-23",
                "return_origin_date": "2026-04-23",
                "estimated_total": 2000,
                "estimated_score": 2000.0,
                "estimated_outbound_time_to_destination_seconds": 7200,
                "main_stay_days": 3,
            }
        ],
        "MGA": [],
    }

    monkeypatch.setattr(
        optimizer,
        "_select_coverage_audit_destinations",
        lambda *_args, **_kwargs: ["USM", "MGA"],
    )
    monkeypatch.setattr(
        optimizer,
        "_build_free_provider_discovery_seed_map",
        lambda **_kwargs: {("OTP", "USM"): ("2026-04-20",)},
    )

    async def _fake_probe(**kwargs):  # type: ignore[no-untyped-def]
        return ({("OTP", "USM"): {"2026-04-20": 650}}, ["audit warning"])

    monkeypatch.setattr(optimizer, "_probe_free_provider_discovery", _fake_probe)
    monkeypatch.setattr(
        optimizer,
        "_estimate_candidates_parallel",
        lambda *_args, **_kwargs: asyncio.sleep(
            0,
            result={
                "USM": [
                    {
                        "candidate_type": "split_stopover",
                        "destination": "USM",
                        "origin": "OTP",
                        "arrival_origin": "OTP",
                        "outbound_hub": "BKK",
                        "inbound_hub": "BKK",
                        "depart_origin_date": "2026-04-20",
                        "depart_destination_date": "2026-04-20",
                        "leave_destination_date": "2026-04-23",
                        "return_origin_date": "2026-04-23",
                        "estimated_total": 1800,
                        "estimated_score": 1800.0,
                        "estimated_outbound_time_to_destination_seconds": 7000,
                        "main_stay_days": 3,
                    }
                ]
            },
        ),
    )

    progress = SearchProgressTracker("coverage-audit")
    progress.start_phase("candidates", total=1, detail="Scoring route candidates.")
    with ThreadPoolExecutor(max_workers=2) as io_pool:
        audited_estimates, audit_metadata, warnings = asyncio.run(
            optimizer._run_coverage_audit(
                search_client=_AuditClient(),  # type: ignore[arg-type]
                candidate_tasks=[candidate_task],
                estimated_by_destination=estimated_by_destination,
                config=config,
                io_pool=io_pool,
                progress=progress,
            )
        )

    assert audited_estimates["USM"][0]["estimated_total"] == 1800
    assert audit_metadata["USM"]["discovered_routes"] == 1
    assert audit_metadata["USM"]["discovered_price_points"] == 1
    assert audit_metadata["USM"]["expanded_max_candidates"] > candidate_task["max_candidates"]
    assert warnings == ["audit warning"]
    snapshot = progress.snapshot()
    assert snapshot["coverage_audit"]["destinations"][0]["destination"] == "USM"
    assert any("Coverage audit complete" in event["message"] for event in snapshot["events"])
