from __future__ import annotations

from collections import Counter

import pytest

from flight_layover_lab.airports import AirportCoordinates
from flight_layover_lab.config import (
    MIN_SPLIT_CONNECTION_CROSS_AIRPORT_SECONDS,
    MIN_SPLIT_CONNECTION_SAME_AIRPORT_SECONDS,
)
from flight_layover_lab.engine import SplitTripOptimizer, _estimate_candidates_for_destination
from flight_layover_lab.exceptions import ProviderNoResultError
from flight_layover_lab.providers import KiwiClient, MultiProviderClient, SerpApiGoogleFlightsClient
from flight_layover_lab.utils import (
    boundary_transfer_events,
    build_comparison_links,
    connection_gap_seconds,
    itinerary_booking_url,
    max_segment_layover_seconds,
    minimum_split_boundary_connection_seconds,
    normalize_provider_ids,
    transfer_events_from_segments,
)


@pytest.mark.parametrize(
    ("segments", "expected"),
    [
        (
            [
                {"from": "OTP", "to": "CDG"},
                {"from": "ORY", "to": "SFO"},
                {"from": "SFO", "to": "HNL"},
            ],
            3,
        ),
        (
            [
                {"from": "OTP", "to": "IST"},
                {"from": "IST", "to": "DOH"},
                {"from": "DOH", "to": "SEZ"},
            ],
            2,
        ),
    ],
)
def test_transfer_events_from_segments(segments: list[dict[str, str]], expected: int) -> None:
    assert transfer_events_from_segments(segments) == expected


@pytest.mark.parametrize(
    ("arrival_airport", "next_departure_airport", "expected"),
    [
        ("MAD", "MAD", 1),
        ("CDG", "ORY", 2),
    ],
)
def test_boundary_transfer_events(
    arrival_airport: str,
    next_departure_airport: str,
    expected: int,
) -> None:
    assert boundary_transfer_events(arrival_airport, next_departure_airport) == expected


@pytest.mark.parametrize(
    ("arrive_local", "depart_local", "expected"),
    [
        ("2026-03-13T09:45:00", "2026-03-13T15:10:00", 5 * 3600 + 25 * 60),
        ("2026-03-13T15:10:00", "2026-03-13T09:45:00", None),
    ],
)
def test_connection_gap_seconds(
    arrive_local: str,
    depart_local: str,
    expected: int | None,
) -> None:
    assert connection_gap_seconds(arrive_local, depart_local) == expected


def test_max_segment_layover_seconds() -> None:
    segments = [
        {
            "from": "OTP",
            "to": "MAD",
            "arrive_local": "2026-03-13T09:45:00",
        },
        {
            "from": "MAD",
            "to": "PTY",
            "depart_local": "2026-03-13T15:10:00",
            "arrive_local": "2026-03-13T19:45:00",
        },
        {
            "from": "PTY",
            "to": "MGA",
            "depart_local": "2026-03-13T21:26:00",
        },
    ]
    assert max_segment_layover_seconds(segments) == 5 * 3600 + 25 * 60


@pytest.mark.parametrize(
    ("arrival_airport", "next_departure_airport", "expected"),
    [
        ("CDG", "CDG", MIN_SPLIT_CONNECTION_SAME_AIRPORT_SECONDS),
        ("CDG", "ORY", MIN_SPLIT_CONNECTION_CROSS_AIRPORT_SECONDS),
    ],
)
def test_minimum_split_boundary_connection_seconds(
    arrival_airport: str,
    next_departure_airport: str,
    expected: int,
) -> None:
    assert (
        minimum_split_boundary_connection_seconds(arrival_airport, next_departure_airport)
        == expected
    )
    assert MIN_SPLIT_CONNECTION_CROSS_AIRPORT_SECONDS >= MIN_SPLIT_CONNECTION_SAME_AIRPORT_SECONDS


def test_itinerary_booking_url_extracts_absolute_url() -> None:
    itinerary = {
        "bookingOptions": {
            "edges": [
                {
                    "node": {
                        "bookingUrl": "/en/booking/?token=abc123",
                    }
                }
            ]
        }
    }
    assert itinerary_booking_url(itinerary) == "https://www.kiwi.com/en/booking/?token=abc123"


def test_build_comparison_links_has_expected_sources() -> None:
    links = build_comparison_links(
        "OTP",
        "MGA",
        "2026-04-20",
        "2026-04-27",
        adults=1,
        max_stops_per_leg=2,
        currency="RON",
    )
    assert "google_flights" in links
    assert "skyscanner" in links
    assert "kayak" in links
    assert "momondo" in links
    assert "travel/flights" in links["google_flights"]
    assert "q=" in links["google_flights"]


@pytest.mark.parametrize(
    ("item", "expected"),
    [
        (
            {
                "itinerary_type": "split_stopover",
                "outbound": {"layovers_count": 1, "stopover_days": 2},
                "inbound": {"stopover_days": 0},
            },
            "has_long_stopover",
        ),
        (
            {
                "itinerary_type": "direct_roundtrip",
                "outbound": {"layovers_count": 2},
                "inbound": {},
            },
            "outbound_layovers_2plus",
        ),
    ],
)
def test_strategy_anchor_key(item: dict[str, object], expected: str) -> None:
    optimizer = SplitTripOptimizer(KiwiClient(), AirportCoordinates())
    assert optimizer._strategy_anchor_key(item) == expected


def test_diversified_split_selection_includes_extra_hub() -> None:
    optimizer = SplitTripOptimizer(KiwiClient(), AirportCoordinates())
    split_candidates = [
        {
            "origin": "OTP",
            "arrival_origin": "OTP",
            "outbound_hub": "FRA",
            "inbound_hub": "FRA",
            "depart_origin_date": f"2026-04-{day:02d}",
            "return_origin_date": f"2026-04-{day + 7:02d}",
            "estimated_score": day,
            "estimated_total": 1000 + day,
        }
        for day in range(1, 5)
    ]
    split_candidates.append(
        {
            "origin": "OTP",
            "arrival_origin": "OTP",
            "outbound_hub": "IST",
            "inbound_hub": "IST",
            "depart_origin_date": "2026-04-10",
            "return_origin_date": "2026-04-17",
            "estimated_score": 99,
            "estimated_total": 1200,
        }
    )

    selected = optimizer._select_split_candidates_with_diversity(
        split_candidates,
        base_quota=2,
        extra_quota=2,
    )
    outbound_hubs = {item["outbound_hub"] for item in selected}
    assert "IST" in outbound_hubs


def test_diversified_split_selection_respects_zero_extra_quota() -> None:
    optimizer = SplitTripOptimizer(KiwiClient(), AirportCoordinates())
    split_candidates = [
        {
            "origin": "OTP",
            "arrival_origin": "OTP",
            "outbound_hub": "FRA",
            "inbound_hub": "FRA",
            "depart_origin_date": "2026-04-01",
            "return_origin_date": "2026-04-08",
            "estimated_score": 1,
            "estimated_total": 1001,
        },
        {
            "origin": "OTP",
            "arrival_origin": "OTP",
            "outbound_hub": "CDG",
            "inbound_hub": "CDG",
            "depart_origin_date": "2026-04-02",
            "return_origin_date": "2026-04-09",
            "estimated_score": 2,
            "estimated_total": 1002,
        },
    ]

    selected = optimizer._select_split_candidates_with_diversity(
        split_candidates,
        base_quota=1,
        extra_quota=0,
    )
    assert len(selected) == 1
    assert selected[0]["outbound_hub"] == "FRA"


def test_estimator_builds_split_chain_candidates_for_two_transfer_mode() -> None:
    task = {
        "destination": "HNL",
        "origins": ["OTP"],
        "outbound_hubs": ["LON", "LAX"],
        "inbound_hubs": ["LAX", "LON"],
        "period_start": "2026-03-10",
        "period_end": "2026-03-15",
        "min_stay_days": 1,
        "max_stay_days": 1,
        "min_stopover_days": 0,
        "max_stopover_days": 0,
        "objective": "cheapest",
        "max_candidates": 40,
        "max_direct_candidates": 0,
        "max_transfers_per_direction": 2,
        "origin_to_hub": {
            "OTP|LON": {"2026-03-10": 100},
        },
        "hub_to_origin": {
            "LON|OTP": {"2026-03-11": 120},
        },
        "hub_to_destination": {
            "LAX": {"2026-03-10": 300},
        },
        "destination_to_hub": {
            "LAX": {"2026-03-11": 300},
        },
        "hub_to_hub": {
            "LON|LAX": {"2026-03-10": 200},
            "LAX|LON": {"2026-03-11": 210},
        },
        "origin_to_destination": {},
        "destination_to_origin": {},
        "destination_distance_map": {"OTP|HNL": 12000.0},
    }

    candidates = _estimate_candidates_for_destination(task)
    chain_candidates = [item for item in candidates if item.get("candidate_type") == "split_chain"]
    assert chain_candidates
    cheapest_chain = min(
        chain_candidates, key=lambda item: int(item.get("estimated_total") or 10**9)
    )
    assert cheapest_chain.get("estimated_total") == 1032
    assert cheapest_chain.get("estimated_pricing_strategy") == "inner_return_bundle_proxy"
    assert len(cheapest_chain.get("outbound_legs") or []) == 3
    assert len(cheapest_chain.get("inbound_legs") or []) == 3


def test_estimator_retains_bundle_aware_split_chain_under_candidate_cap() -> None:
    filler_first_hubs = [f"F{idx:02d}" for idx in range(1, 10)]
    filler_second_hubs = [f"G{idx:02d}" for idx in range(1, 10)]
    inbound_hubs = ["BKK", "AMM", *filler_first_hubs, *filler_second_hubs]

    destination_to_hub = {
        "BKK": {"2026-05-05": 5100},
    }
    hub_to_origin = {
        "AMM|OTP": {"2026-05-05": 520},
    }
    hub_to_hub = {
        "IST|BKK": {"2026-04-25": 2443},
        "BKK|AMM": {"2026-05-05": 2421},
    }
    for first_hub, second_hub in zip(filler_first_hubs, filler_second_hubs, strict=True):
        destination_to_hub[first_hub] = {"2026-05-05": 1700}
        hub_to_hub[f"{first_hub}|{second_hub}"] = {"2026-05-05": 1700}
        hub_to_origin[f"{second_hub}|OTP"] = {"2026-05-05": 1700}

    task = {
        "destination": "USM",
        "origins": ["OTP"],
        "outbound_hubs": ["IST", "BKK"],
        "inbound_hubs": inbound_hubs,
        "period_start": "2026-04-25",
        "period_end": "2026-05-05",
        "min_stay_days": 10,
        "max_stay_days": 10,
        "min_stopover_days": 0,
        "max_stopover_days": 0,
        "objective": "cheapest",
        "max_candidates": 8,
        "max_direct_candidates": 0,
        "max_transfers_per_direction": 2,
        "origin_to_hub": {
            "OTP|IST": {"2026-04-25": 814},
        },
        "hub_to_origin": hub_to_origin,
        "hub_to_destination": {
            "BKK": {"2026-04-25": 981},
        },
        "destination_to_hub": destination_to_hub,
        "hub_to_hub": hub_to_hub,
        "origin_to_destination": {},
        "destination_to_origin": {},
        "destination_distance_map": {"OTP|USM": 7997.3},
    }

    candidates = _estimate_candidates_for_destination(task)
    bundle_candidate = next(
        item
        for item in candidates
        if item.get("candidate_type") == "split_chain"
        and item.get("outbound_hub") == "IST/BKK"
        and item.get("inbound_hub") == "BKK/AMM"
    )

    assert bundle_candidate["estimated_total"] == 8160
    assert bundle_candidate["estimated_pricing_strategy"] == "inner_return_bundle_proxy"


def test_best_estimator_prefers_faster_candidate_before_validation() -> None:
    base_task = {
        "destination": "BKK",
        "origins": ["OTP", "BBU"],
        "outbound_hubs": ["DOH"],
        "inbound_hubs": ["DOH"],
        "period_start": "2026-03-10",
        "period_end": "2026-03-20",
        "min_stay_days": 1,
        "max_stay_days": 1,
        "min_stopover_days": 0,
        "max_stopover_days": 3,
        "max_candidates": 20,
        "max_direct_candidates": 20,
        "max_transfers_per_direction": 1,
        "origin_to_hub": {
            "OTP|DOH": {"2026-03-10": 100},
        },
        "hub_to_origin": {
            "DOH|OTP": {"2026-03-14": 100},
        },
        "hub_to_destination": {
            "DOH": {"2026-03-13": 100},
        },
        "destination_to_hub": {
            "DOH": {"2026-03-14": 100},
        },
        "origin_to_destination": {
            "OTP|BKK": {"2026-03-10": 650},
            "BBU|BKK": {"2026-03-10": 1800},
        },
        "destination_to_origin": {
            "BKK|OTP": {"2026-03-11": 550},
            "BKK|BBU": {"2026-03-11": 1200},
        },
        "destination_distance_map": {"OTP|BKK": 7735.8, "BBU|BKK": 7735.8},
    }

    cheapest_candidates = _estimate_candidates_for_destination(
        {
            **base_task,
            "objective": "cheapest",
        }
    )
    best_candidates = _estimate_candidates_for_destination(
        {
            **base_task,
            "objective": "best",
        }
    )

    assert cheapest_candidates[0]["candidate_type"] == "split_stopover"
    assert cheapest_candidates[0]["estimated_total"] == 334
    assert best_candidates[0]["candidate_type"] == "direct_roundtrip"
    assert best_candidates[0]["estimated_total"] == 1200
    assert (
        best_candidates[0]["estimated_best_value_score"]
        < best_candidates[1]["estimated_best_value_score"]
    )


def test_split_candidate_key_distinguishes_same_summary_dates_with_different_leg_dates() -> None:
    optimizer = SplitTripOptimizer({"kiwi": KiwiClient()}, AirportCoordinates())
    early_chain = {
        "candidate_type": "split_chain",
        "origin": "OTP",
        "arrival_origin": "OTP",
        "outbound_hub": "IST/BKK",
        "inbound_hub": "BKK/AMM",
        "depart_origin_date": "2026-04-25",
        "depart_destination_date": "2026-04-27",
        "leave_destination_date": "2026-05-06",
        "return_origin_date": "2026-05-08",
        "outbound_boundary_stopover_days": [0, 2],
        "inbound_boundary_stopover_days": [0, 2],
        "outbound_legs": [
            {"source": "OTP", "destination": "IST", "date": "2026-04-25"},
            {"source": "IST", "destination": "BKK", "date": "2026-04-25"},
            {"source": "BKK", "destination": "USM", "date": "2026-04-27"},
        ],
        "inbound_legs": [
            {"source": "USM", "destination": "BKK", "date": "2026-05-06"},
            {"source": "BKK", "destination": "AMM", "date": "2026-05-06"},
            {"source": "AMM", "destination": "OTP", "date": "2026-05-08"},
        ],
        "estimated_total": 7238,
        "estimated_score": 7238.0,
    }
    late_chain = {
        **early_chain,
        "outbound_boundary_stopover_days": [2, 0],
        "outbound_legs": [
            {"source": "OTP", "destination": "IST", "date": "2026-04-25"},
            {"source": "IST", "destination": "BKK", "date": "2026-04-27"},
            {"source": "BKK", "destination": "USM", "date": "2026-04-27"},
        ],
        "estimated_total": 6691,
        "estimated_score": 6691.0,
    }

    assert optimizer._split_candidate_key(early_chain) != optimizer._split_candidate_key(late_chain)

    selected = optimizer._select_split_candidates_with_diversity(
        [late_chain, early_chain],
        base_quota=1,
        extra_quota=1,
    )

    assert len(selected) == 2


def test_pick_auto_hubs_keeps_chain_bridge_hubs_for_destination_first_routes() -> None:
    optimizer = SplitTripOptimizer(KiwiClient(), AirportCoordinates())
    filler_hubs = [f"H{idx:02d}" for idx in range(1, 19)]
    hub_candidates = ["MXP", "SIN", "IST", "BKK", "DMK", "RUH", *filler_hubs]
    config = optimizer.parse_search_config(
        {
            "origins": ["OTP"],
            "destinations": ["USM"],
            "period_start": "2026-04-20",
            "period_end": "2026-05-10",
            "hub_candidates": hub_candidates,
            "auto_hubs_per_direction": 2,
            "max_transfers_per_direction": 2,
        }
    )

    calendars: dict[tuple[str, str], dict[str, int]] = {
        ("OTP", "MXP"): {"2026-04-29": 575},
        ("MXP", "USM"): {"2026-05-02": 3434},
        ("USM", "SIN"): {"2026-05-09": 935},
        ("SIN", "OTP"): {"2026-05-10": 3630},
        ("OTP", "IST"): {"2026-04-25": 814},
        ("IST", "BKK"): {"2026-04-25": 2443},
        ("BKK", "USM"): {"2026-04-27": 1962},
        ("USM", "DMK"): {"2026-05-05": 1962},
        ("DMK", "RUH"): {"2026-05-05": 1454},
        ("RUH", "OTP"): {"2026-05-08": 1568},
        # Make these hubs unattractive on direct-only scoring so they only survive
        # if chain-aware hub scoring is applied.
        ("OTP", "BKK"): {"2026-04-25": 9200},
        ("BKK", "OTP"): {"2026-05-08": 9200},
        ("USM", "BKK"): {"2026-05-05": 9100},
        ("OTP", "DMK"): {"2026-04-25": 9400},
        ("DMK", "OTP"): {"2026-05-08": 9400},
        ("USM", "RUH"): {"2026-05-05": 9000},
    }

    for hub in filler_hubs:
        calendars[("OTP", hub)] = {"2026-04-24": 3500}
        calendars[(hub, "OTP")] = {"2026-05-08": 3600}
        calendars[(hub, "USM")] = {"2026-04-27": 3600}
        calendars[("USM", hub)] = {"2026-05-05": 3500}

    outbound_hubs, inbound_hubs = optimizer._pick_auto_hubs("USM", config, calendars)

    assert len(outbound_hubs) == 18
    assert len(inbound_hubs) == 18
    assert "BKK" in outbound_hubs
    assert "IST" in outbound_hubs
    assert "DMK" in inbound_hubs
    assert "RUH" in inbound_hubs


def test_pick_auto_hubs_keeps_bundle_friendly_inbound_bridge_hubs() -> None:
    optimizer = SplitTripOptimizer(KiwiClient(), AirportCoordinates())
    filler_hubs = [f"H{idx:02d}" for idx in range(1, 21)]
    hub_candidates = ["BKK", "AMM", *filler_hubs]
    config = optimizer.parse_search_config(
        {
            "origins": ["OTP"],
            "destinations": ["USM"],
            "period_start": "2026-04-20",
            "period_end": "2026-05-10",
            "hub_candidates": hub_candidates,
            "auto_hubs_per_direction": 2,
            "max_transfers_per_direction": 2,
        }
    )

    calendars: dict[tuple[str, str], dict[str, int]] = {
        ("BKK", "USM"): {"2026-04-25": 981},
        ("USM", "BKK"): {"2026-05-05": 5100},
        ("BKK", "AMM"): {"2026-05-05": 2421},
        ("AMM", "OTP"): {"2026-05-08": 520},
    }
    for hub in filler_hubs:
        calendars[("USM", hub)] = {"2026-05-05": 2600}
        calendars[(hub, "OTP")] = {"2026-05-08": 2600}

    _, inbound_hubs = optimizer._pick_auto_hubs("USM", config, calendars)

    assert len(inbound_hubs) == 18
    assert "AMM" in inbound_hubs


def test_search_prefers_inner_roundtrip_bundle_inside_split_chain() -> None:
    def segment(
        source: str,
        destination: str,
        depart_local: str,
        arrive_local: str,
    ) -> dict[str, str]:
        return {
            "from": source,
            "to": destination,
            "depart_local": depart_local,
            "arrive_local": arrive_local,
        }

    class BundleAwareProvider:
        provider_id = "kiwi"
        display_name = "Kiwi"
        supports_calendar = True
        requires_credentials = False
        credential_env: tuple[str, ...] = ()
        default_enabled = True

        def is_configured(self) -> bool:
            return True

        def __init__(self) -> None:
            self.calendars = {
                ("OTP", "IST"): {"2026-04-25": 814},
                ("IST", "BKK"): {"2026-04-25": 2443},
                ("BKK", "USM"): {"2026-04-27": 981},
                ("USM", "DMK"): {"2026-05-05": 981},
                ("DMK", "RUH"): {"2026-05-05": 1454},
                ("RUH", "OTP"): {"2026-05-08": 1568},
                ("OTP", "BKK"): {"2026-04-25": 5200},
                ("BKK", "OTP"): {"2026-05-08": 5200},
                ("USM", "BKK"): {"2026-05-05": 5100},
                ("IST", "DMK"): {"2026-05-05": 9999},
                ("DMK", "IST"): {"2026-05-05": 9999},
            }
            self.oneways = {
                ("OTP", "IST", "2026-04-25"): {
                    "price": 814,
                    "formatted_price": "814 lei",
                    "currency": "RON",
                    "duration_seconds": 90 * 60,
                    "stops": 0,
                    "transfer_events": 0,
                    "booking_url": "https://example.test/otp-ist",
                    "segments": [
                        segment("OTP", "IST", "2026-04-25T16:00:00", "2026-04-25T17:30:00"),
                    ],
                    "provider": "kiwi",
                    "fare_mode": "selected_bags",
                    "price_mode": "explicit_total",
                },
                ("IST", "BKK", "2026-04-25"): {
                    "price": 2443,
                    "formatted_price": "2,443 lei",
                    "currency": "RON",
                    "duration_seconds": 14 * 3600,
                    "stops": 1,
                    "transfer_events": 1,
                    "booking_url": "https://example.test/ist-bkk",
                    "segments": [
                        segment("IST", "AMM", "2026-04-25T21:30:00", "2026-04-26T00:39:00"),
                        segment("AMM", "BKK", "2026-04-26T03:20:00", "2026-04-26T15:35:00"),
                    ],
                    "provider": "kiwi",
                    "fare_mode": "selected_bags",
                    "price_mode": "explicit_total",
                },
                ("BKK", "USM", "2026-04-27"): {
                    "price": 1500,
                    "formatted_price": "1,500 lei",
                    "currency": "RON",
                    "duration_seconds": 65 * 60,
                    "stops": 0,
                    "transfer_events": 0,
                    "booking_url": "https://example.test/bkk-usm-ow",
                    "segments": [
                        segment("BKK", "USM", "2026-04-27T06:05:00", "2026-04-27T07:10:00"),
                    ],
                    "provider": "kiwi",
                    "fare_mode": "selected_bags",
                    "price_mode": "explicit_total",
                },
                ("USM", "DMK", "2026-05-05"): {
                    "price": 1500,
                    "formatted_price": "1,500 lei",
                    "currency": "RON",
                    "duration_seconds": 90 * 60,
                    "stops": 0,
                    "transfer_events": 0,
                    "booking_url": "https://example.test/usm-dmk-ow",
                    "segments": [
                        segment("USM", "DMK", "2026-05-05T06:55:00", "2026-05-05T08:25:00"),
                    ],
                    "provider": "kiwi",
                    "fare_mode": "selected_bags",
                    "price_mode": "explicit_total",
                },
                ("DMK", "RUH", "2026-05-05"): {
                    "price": 1454,
                    "formatted_price": "1,454 lei",
                    "currency": "RON",
                    "duration_seconds": 8 * 3600 + 20 * 60,
                    "stops": 0,
                    "transfer_events": 0,
                    "booking_url": "https://example.test/dmk-ruh",
                    "segments": [
                        segment("DMK", "RUH", "2026-05-05T15:15:00", "2026-05-05T19:35:00"),
                    ],
                    "provider": "kiwi",
                    "fare_mode": "selected_bags",
                    "price_mode": "explicit_total",
                },
                ("RUH", "OTP", "2026-05-08"): {
                    "price": 1568,
                    "formatted_price": "1,568 lei",
                    "currency": "RON",
                    "duration_seconds": 8 * 3600 + 35 * 60,
                    "stops": 1,
                    "transfer_events": 1,
                    "booking_url": "https://example.test/ruh-otp",
                    "segments": [
                        segment("RUH", "AMM", "2026-05-08T15:00:00", "2026-05-08T17:15:00"),
                        segment("AMM", "OTP", "2026-05-08T20:35:00", "2026-05-08T23:35:00"),
                    ],
                    "provider": "kiwi",
                    "fare_mode": "selected_bags",
                    "price_mode": "explicit_total",
                },
            }
            self.returns = {
                ("BKK", "USM", "2026-04-27", "2026-05-05"): {
                    "price": 1962,
                    "formatted_price": "1,962 lei",
                    "currency": "RON",
                    "duration_seconds": 4 * 3600 + 5 * 60,
                    "outbound_duration_seconds": 65 * 60,
                    "inbound_duration_seconds": 90 * 60,
                    "outbound_stops": 0,
                    "inbound_stops": 0,
                    "outbound_transfer_events": 0,
                    "inbound_transfer_events": 0,
                    "booking_url": "https://example.test/bkk-usm-rt",
                    "outbound_segments": [
                        segment("BKK", "USM", "2026-04-27T06:05:00", "2026-04-27T07:10:00"),
                    ],
                    "inbound_segments": [
                        segment("USM", "DMK", "2026-05-05T06:55:00", "2026-05-05T08:25:00"),
                    ],
                    "provider": "kiwi",
                    "fare_mode": "selected_bags",
                    "price_mode": "explicit_total",
                },
            }

        def get_calendar_prices(self, **kwargs):  # type: ignore[no-untyped-def]
            return dict(
                self.calendars.get(
                    (kwargs.get("source"), kwargs.get("destination")),
                    {},
                )
            )

        def get_best_oneway(self, **kwargs):  # type: ignore[no-untyped-def]
            item = self.oneways.get(
                (kwargs.get("source"), kwargs.get("destination"), kwargs.get("departure_iso"))
            )
            return dict(item) if item else None

        def get_best_return(self, **kwargs):  # type: ignore[no-untyped-def]
            item = self.returns.get(
                (
                    kwargs.get("source"),
                    kwargs.get("destination"),
                    kwargs.get("outbound_iso"),
                    kwargs.get("inbound_iso"),
                )
            )
            return dict(item) if item else None

    optimizer = SplitTripOptimizer({"kiwi": BundleAwareProvider()}, AirportCoordinates())
    config = optimizer.parse_search_config(
        {
            "origins": ["OTP"],
            "destinations": ["USM"],
            "providers": ["kiwi"],
            "period_start": "2026-04-25",
            "period_end": "2026-05-10",
            "hub_candidates": ["IST", "BKK", "DMK", "RUH"],
            "auto_hubs_per_direction": 2,
            "min_stay_days": 8,
            "max_stay_days": 8,
            "min_stopover_days": 0,
            "max_stopover_days": 3,
            "max_transfers_per_direction": 3,
            "top_results": 5,
            "validate_top_per_destination": 20,
            "io_workers": 4,
            "cpu_workers": 1,
        }
    )

    result = optimizer.search(config)
    assert result["results"]
    best = result["results"][0]
    assert best["total_price"] == 8241
    assert best["pricing_strategy"] == "inner_return_bundle"
    assert any(str(leg.get("ticket_type") or "") == "roundtrip" for leg in best["legs"])


def test_search_rejects_inner_return_bundle_with_mismatched_inbound_gateway() -> None:
    def segment(
        source: str,
        destination: str,
        depart_local: str,
        arrive_local: str,
    ) -> dict[str, str]:
        return {
            "from": source,
            "to": destination,
            "depart_local": depart_local,
            "arrive_local": arrive_local,
        }

    class MismatchBundleProvider:
        provider_id = "kiwi"
        display_name = "Kiwi"
        supports_calendar = True
        requires_credentials = False
        credential_env: tuple[str, ...] = ()
        default_enabled = True

        def is_configured(self) -> bool:
            return True

        def __init__(self) -> None:
            self.calendars = {
                ("OTP", "SIN"): {"2026-04-24": 3926},
                ("SIN", "PQC"): {"2026-04-28": 766},
                ("PQC", "MAN"): {"2026-05-06": 1500},
                ("MAN", "OTP"): {"2026-05-10": 932},
            }
            self.oneways = {
                ("OTP", "SIN", "2026-04-24"): {
                    "price": 3926,
                    "formatted_price": "3,926 lei",
                    "currency": "RON",
                    "duration_seconds": 18 * 3600 + 25 * 60,
                    "stops": 1,
                    "transfer_events": 1,
                    "booking_url": "https://example.test/otp-sin",
                    "segments": [
                        segment("OTP", "IST", "2026-04-24T10:05:00", "2026-04-24T11:25:00"),
                        segment("IST", "SIN", "2026-04-24T17:45:00", "2026-04-25T09:30:00"),
                    ],
                    "provider": "kiwi",
                    "fare_mode": "selected_bags",
                    "price_mode": "explicit_total",
                },
                ("MAN", "OTP", "2026-05-10"): {
                    "price": 932,
                    "formatted_price": "932 lei",
                    "currency": "RON",
                    "duration_seconds": 3 * 3600 + 20 * 60,
                    "stops": 0,
                    "transfer_events": 0,
                    "booking_url": "https://example.test/man-otp",
                    "segments": [
                        segment("MAN", "OTP", "2026-05-10T06:15:00", "2026-05-10T11:35:00"),
                    ],
                    "provider": "kiwi",
                    "fare_mode": "selected_bags",
                    "price_mode": "explicit_total",
                },
            }
            self.returns = {
                ("SIN", "PQC", "2026-04-28", "2026-05-06"): {
                    "price": 1532,
                    "formatted_price": "1,532 lei",
                    "currency": "RON",
                    "duration_seconds": 3 * 3600 + 25 * 60,
                    "outbound_duration_seconds": 40 * 60,
                    "inbound_duration_seconds": 45 * 60,
                    "outbound_stops": 0,
                    "inbound_stops": 0,
                    "outbound_transfer_events": 0,
                    "inbound_transfer_events": 0,
                    "booking_url": "https://example.test/sin-pqc-rt",
                    "outbound_segments": [
                        segment("SIN", "PQC", "2026-04-28T13:05:00", "2026-04-28T13:45:00"),
                    ],
                    "inbound_segments": [
                        segment("PQC", "SIN", "2026-05-06T15:45:00", "2026-05-06T18:30:00"),
                    ],
                    "provider": "kiwi",
                    "fare_mode": "selected_bags",
                    "price_mode": "explicit_total",
                },
            }

        def get_calendar_prices(self, **kwargs):  # type: ignore[no-untyped-def]
            return dict(
                self.calendars.get(
                    (kwargs.get("source"), kwargs.get("destination")),
                    {},
                )
            )

        def get_best_oneway(self, **kwargs):  # type: ignore[no-untyped-def]
            item = self.oneways.get(
                (kwargs.get("source"), kwargs.get("destination"), kwargs.get("departure_iso"))
            )
            return dict(item) if item else None

        def get_best_return(self, **kwargs):  # type: ignore[no-untyped-def]
            item = self.returns.get(
                (
                    kwargs.get("source"),
                    kwargs.get("destination"),
                    kwargs.get("outbound_iso"),
                    kwargs.get("inbound_iso"),
                )
            )
            return dict(item) if item else None

    class UnavailableRouteGraph:
        def available(self) -> bool:
            return False

    optimizer = SplitTripOptimizer({"kiwi": MismatchBundleProvider()}, AirportCoordinates())
    optimizer.route_graph = UnavailableRouteGraph()
    config = optimizer.parse_search_config(
        {
            "origins": ["OTP"],
            "destinations": ["PQC"],
            "providers": ["kiwi"],
            "period_start": "2026-04-24",
            "period_end": "2026-05-10",
            "hub_candidates": ["SIN", "MAN"],
            "auto_hubs_per_direction": 2,
            "min_stay_days": 8,
            "max_stay_days": 8,
            "min_stopover_days": 4,
            "max_stopover_days": 4,
            "max_transfers_per_direction": 3,
            "top_results": 5,
            "validate_top_per_destination": 20,
            "io_workers": 4,
            "cpu_workers": 1,
        }
    )

    result = optimizer.search(config)
    assert result["results"] == []


def test_search_reuses_shared_oneway_validation_across_destinations() -> None:
    def segment(
        source: str,
        destination: str,
        depart_local: str,
        arrive_local: str,
    ) -> dict[str, str]:
        return {
            "from": source,
            "to": destination,
            "depart_local": depart_local,
            "arrive_local": arrive_local,
        }

    class SharedLegProvider:
        provider_id = "kiwi"
        display_name = "Kiwi"
        supports_calendar = True
        requires_credentials = False
        credential_env: tuple[str, ...] = ()
        default_enabled = True

        def __init__(self) -> None:
            self.oneway_calls: Counter[tuple[str, str, str]] = Counter()
            self.return_calls: Counter[tuple[str, str, str, str]] = Counter()
            self.calendars = {
                ("OTP", "IST"): {"2026-06-01": 100},
                ("IST", "OTP"): {"2026-06-03": 110},
                ("IST", "AAA"): {"2026-06-01": 420},
                ("AAA", "IST"): {"2026-06-03": 430},
                ("IST", "BBB"): {"2026-06-01": 450},
                ("BBB", "IST"): {"2026-06-03": 460},
            }
            self.oneways = {
                ("OTP", "IST", "2026-06-01"): {
                    "price": 100,
                    "formatted_price": "100 lei",
                    "currency": "RON",
                    "duration_seconds": 90 * 60,
                    "stops": 0,
                    "transfer_events": 0,
                    "booking_url": "https://example.test/otp-ist",
                    "segments": [
                        segment("OTP", "IST", "2026-06-01T08:00:00", "2026-06-01T09:30:00"),
                    ],
                    "provider": "kiwi",
                    "fare_mode": "selected_bags",
                    "price_mode": "explicit_total",
                },
                ("IST", "OTP", "2026-06-03"): {
                    "price": 110,
                    "formatted_price": "110 lei",
                    "currency": "RON",
                    "duration_seconds": 100 * 60,
                    "stops": 0,
                    "transfer_events": 0,
                    "booking_url": "https://example.test/ist-otp",
                    "segments": [
                        segment("IST", "OTP", "2026-06-03T18:00:00", "2026-06-03T19:40:00"),
                    ],
                    "provider": "kiwi",
                    "fare_mode": "selected_bags",
                    "price_mode": "explicit_total",
                },
            }
            self.returns = {
                ("IST", "AAA", "2026-06-01", "2026-06-03"): {
                    "price": 700,
                    "formatted_price": "700 lei",
                    "currency": "RON",
                    "duration_seconds": 4 * 3600,
                    "outbound_duration_seconds": 2 * 3600,
                    "inbound_duration_seconds": 2 * 3600,
                    "outbound_stops": 0,
                    "inbound_stops": 0,
                    "outbound_transfer_events": 0,
                    "inbound_transfer_events": 0,
                    "booking_url": "https://example.test/ist-aaa",
                    "outbound_segments": [
                        segment("IST", "AAA", "2026-06-01T11:00:00", "2026-06-01T13:00:00"),
                    ],
                    "inbound_segments": [
                        segment("AAA", "IST", "2026-06-03T14:00:00", "2026-06-03T16:00:00"),
                    ],
                    "provider": "kiwi",
                    "fare_mode": "selected_bags",
                    "price_mode": "explicit_total",
                },
                ("IST", "BBB", "2026-06-01", "2026-06-03"): {
                    "price": 760,
                    "formatted_price": "760 lei",
                    "currency": "RON",
                    "duration_seconds": 4 * 3600,
                    "outbound_duration_seconds": 2 * 3600,
                    "inbound_duration_seconds": 2 * 3600,
                    "outbound_stops": 0,
                    "inbound_stops": 0,
                    "outbound_transfer_events": 0,
                    "inbound_transfer_events": 0,
                    "booking_url": "https://example.test/ist-bbb",
                    "outbound_segments": [
                        segment("IST", "BBB", "2026-06-01T11:30:00", "2026-06-01T13:30:00"),
                    ],
                    "inbound_segments": [
                        segment("BBB", "IST", "2026-06-03T14:30:00", "2026-06-03T16:30:00"),
                    ],
                    "provider": "kiwi",
                    "fare_mode": "selected_bags",
                    "price_mode": "explicit_total",
                },
            }

        def is_configured(self) -> bool:
            return True

        def get_calendar_prices(self, **kwargs):  # type: ignore[no-untyped-def]
            return dict(
                self.calendars.get(
                    (kwargs.get("source"), kwargs.get("destination")),
                    {},
                )
            )

        def get_best_oneway(self, **kwargs):  # type: ignore[no-untyped-def]
            key = (kwargs.get("source"), kwargs.get("destination"), kwargs.get("departure_iso"))
            self.oneway_calls[key] += 1
            item = self.oneways.get(key)
            return dict(item) if item else None

        def get_best_return(self, **kwargs):  # type: ignore[no-untyped-def]
            key = (
                kwargs.get("source"),
                kwargs.get("destination"),
                kwargs.get("outbound_iso"),
                kwargs.get("inbound_iso"),
            )
            self.return_calls[key] += 1
            item = self.returns.get(key)
            return dict(item) if item else None

    class UnavailableRouteGraph:
        def available(self) -> bool:
            return False

    provider = SharedLegProvider()
    optimizer = SplitTripOptimizer({"kiwi": provider}, AirportCoordinates())
    optimizer.route_graph = UnavailableRouteGraph()
    config = optimizer.parse_search_config(
        {
            "origins": ["OTP"],
            "destinations": ["AAA", "BBB"],
            "providers": ["kiwi"],
            "period_start": "2026-06-01",
            "period_end": "2026-06-03",
            "hub_candidates": ["IST"],
            "auto_hubs_per_direction": 1,
            "min_stay_days": 2,
            "max_stay_days": 2,
            "min_stopover_days": 0,
            "max_stopover_days": 0,
            "max_transfers_per_direction": 1,
            "objective": "cheapest",
            "market_compare_fares": False,
            "top_results": 4,
            "validate_top_per_destination": 1,
            "io_workers": 4,
            "cpu_workers": 1,
        }
    )

    result = optimizer.search(config)

    assert "Reused 2 duplicate one-way validations across destinations." in result["warnings"]
    assert provider.oneway_calls[("OTP", "IST", "2026-06-01")] == 1
    assert provider.oneway_calls[("IST", "OTP", "2026-06-03")] == 1
    assert provider.return_calls[("IST", "AAA", "2026-06-01", "2026-06-03")] == 1
    assert provider.return_calls[("IST", "BBB", "2026-06-01", "2026-06-03")] == 1


def test_merge_strategy_anchors_keeps_destination_coverage() -> None:
    optimizer = SplitTripOptimizer(KiwiClient(), AirportCoordinates())
    ranked = [
        {
            "result_id": "mle-1",
            "destination_code": "MLE",
            "itinerary_type": "direct_roundtrip",
            "outbound": {"layovers_count": 0, "hub": "DIRECT"},
            "inbound": {"hub": "DIRECT"},
        },
        {
            "result_id": "mle-2",
            "destination_code": "MLE",
            "itinerary_type": "direct_roundtrip",
            "outbound": {"layovers_count": 1, "hub": "IST"},
            "inbound": {"hub": "IST"},
        },
        {
            "result_id": "mle-3",
            "destination_code": "MLE",
            "itinerary_type": "split_stopover",
            "outbound": {"layovers_count": 2, "stopover_days": 0},
            "inbound": {"stopover_days": 0},
        },
        {
            "result_id": "mru-1",
            "destination_code": "MRU",
            "itinerary_type": "direct_roundtrip",
            "outbound": {"layovers_count": 1, "hub": "AUH"},
            "inbound": {"hub": "AUH"},
        },
        {
            "result_id": "hnl-1",
            "destination_code": "HNL",
            "itinerary_type": "direct_roundtrip",
            "outbound": {"layovers_count": 2, "hub": "LAX"},
            "inbound": {"hub": "LAX"},
        },
    ]
    merged = optimizer._merge_strategy_anchors(ranked, top_results=3)
    destination_codes = {item["destination_code"] for item in merged}
    assert "MLE" in destination_codes
    assert "MRU" in destination_codes
    assert "HNL" in destination_codes


def test_cap_results_per_destination_applies_limit_for_each_destination() -> None:
    optimizer = SplitTripOptimizer(KiwiClient(), AirportCoordinates())
    ranked: list[dict[str, object]] = []
    for idx in range(5):
        ranked.append(
            {
                "result_id": f"mle-{idx}",
                "destination_code": "MLE",
                "itinerary_type": "direct_roundtrip",
                "outbound": {"layovers_count": 1, "hub": "IST"},
                "inbound": {"hub": "IST"},
            }
        )
    for idx in range(4):
        ranked.append(
            {
                "result_id": f"mru-{idx}",
                "destination_code": "MRU",
                "itinerary_type": "direct_roundtrip",
                "outbound": {"layovers_count": 1, "hub": "AUH"},
                "inbound": {"hub": "AUH"},
            }
        )

    capped, counts = optimizer._cap_results_per_destination(
        ranked,
        top_results_per_destination=2,
        destination_order=["MRU", "MLE"],
    )
    assert len(capped) == 4
    assert counts.get("MRU") == 2
    assert counts.get("MLE") == 2


def test_cap_results_per_destination_keeps_required_destination_floors() -> None:
    optimizer = SplitTripOptimizer(KiwiClient(), AirportCoordinates())
    ranked: list[dict[str, object]] = [
        {
            "result_id": "mru-best-rank",
            "destination_code": "MRU",
            "itinerary_type": "direct_roundtrip",
            "total_price": 8200,
            "outbound": {"layovers_count": 1, "hub": "AUH"},
            "inbound": {"hub": "AUH"},
        },
        {
            "result_id": "mru-second",
            "destination_code": "MRU",
            "itinerary_type": "direct_roundtrip",
            "total_price": 8300,
            "outbound": {"layovers_count": 1, "hub": "IST"},
            "inbound": {"hub": "IST"},
        },
        {
            "result_id": "mru-kiwi-floor",
            "destination_code": "MRU",
            "itinerary_type": "direct_roundtrip",
            "total_price": 7900,
            "outbound": {"layovers_count": 2, "hub": "FRA"},
            "inbound": {"hub": "FRA"},
        },
    ]
    capped, counts = optimizer._cap_results_per_destination(
        ranked,
        top_results_per_destination=2,
        destination_order=["MRU"],
        required_by_destination={"MRU": [ranked[2]]},
    )
    assert counts.get("MRU") == 2
    capped_ids = {str(item.get("result_id")) for item in capped}
    assert "mru-kiwi-floor" in capped_ids


def test_merge_strategy_anchors_keeps_cheapest_option_visible() -> None:
    optimizer = SplitTripOptimizer(KiwiClient(), AirportCoordinates())
    ranked: list[dict[str, object]] = []
    for idx in range(1, 22):
        ranked.append(
            {
                "result_id": f"mru-{idx}",
                "destination_code": "MRU",
                "itinerary_type": "direct_roundtrip",
                "total_price": 8000 + idx,
                "outbound_time_to_destination_seconds": 30000 + idx,
                "outbound": {"layovers_count": 1, "hub": "IST"},
                "inbound": {"hub": "IST"},
            }
        )
    ranked.append(
        {
            "result_id": "mru-cheapest",
            "destination_code": "MRU",
            "itinerary_type": "direct_roundtrip",
            "total_price": 7300,
            "outbound_time_to_destination_seconds": 70000,
            "outbound": {"layovers_count": 2, "hub": "FRA"},
            "inbound": {"hub": "FRA"},
        }
    )
    merged = optimizer._merge_strategy_anchors(ranked, top_results=20)
    merged_ids = {str(item.get("result_id")) for item in merged}
    assert "mru-cheapest" in merged_ids


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (["KIWI", "foo", "AMADEUS"], ("kiwi", "amadeus")),
        ("all", ("kiwi", "kayak", "momondo", "googleflights", "skyscanner", "amadeus", "serpapi")),
        ("", ("kiwi",)),
    ],
)
def test_normalize_provider_ids(value: object, expected: tuple[str, ...]) -> None:
    assert normalize_provider_ids(value) == expected


def test_multi_provider_client_picks_cheapest_oneway() -> None:
    class OnewayProvider:
        def __init__(self, provider_id: str, price: int, stops: int = 1) -> None:
            self.provider_id = provider_id
            self.price = price
            self.stops = stops

        def get_calendar_prices(self, **kwargs):  # type: ignore[no-untyped-def]
            return {}

        def get_best_oneway(self, **kwargs):  # type: ignore[no-untyped-def]
            return {
                "price": self.price,
                "formatted_price": str(self.price),
                "currency": "RON",
                "duration_seconds": 3600,
                "stops": self.stops,
                "transfer_events": self.stops,
                "booking_url": None,
                "segments": [],
            }

        def get_best_return(self, **kwargs):  # type: ignore[no-untyped-def]
            return None

    client = MultiProviderClient(
        [
            OnewayProvider("kiwi", 1800, 1),
            OnewayProvider("amadeus", 1700, 2),
        ]
    )
    best = client.get_best_oneway(
        source="OTP",
        destination="SEZ",
        departure_iso="2026-04-20",
        currency="RON",
        max_stops_per_leg=2,
        adults=1,
        hand_bags=1,
        hold_bags=0,
    )
    assert best is not None
    assert best.get("price") == 1700
    assert best.get("provider") == "amadeus"


def test_parse_config_auto_provider_defaults_to_configured() -> None:
    optimizer = SplitTripOptimizer(KiwiClient(), AirportCoordinates())
    config = optimizer.parse_search_config(
        {
            "origins": ["OTP"],
            "destinations": ["MAD"],
            "period_start": "2026-04-01",
            "period_end": "2026-04-20",
            "providers": [],
        }
    )
    assert config.provider_ids == ("kiwi", "kayak", "momondo", "googleflights", "skyscanner")


def test_runtime_provider_secrets_enable_optional_providers() -> None:
    optimizer = SplitTripOptimizer(KiwiClient(), AirportCoordinates())
    optimizer.update_runtime_provider_secrets(
        {
            "amadeus_client_id": "id123",
            "amadeus_client_secret": "secret123",
            "serpapi_api_key": "key123",
            "serpapi_return_option_scan_limit": "3",
        }
    )
    status = optimizer.runtime_provider_config_status()
    assert status["amadeus_client_id_set"]
    assert status["amadeus_client_secret_set"]
    assert status["serpapi_api_key_set"]
    assert status["serpapi_return_option_scan_limit_set"]

    catalog = {item["id"]: item for item in optimizer.provider_catalog()}
    assert catalog["amadeus"]["configured"]
    assert catalog["serpapi"]["configured"]


def test_provider_catalog_enables_free_scrapers_by_default() -> None:
    optimizer = SplitTripOptimizer(KiwiClient(), AirportCoordinates())
    catalog = {item["id"]: item for item in optimizer.provider_catalog()}
    assert catalog["googleflights"]["default_enabled"] is True
    assert catalog["skyscanner"]["default_enabled"] is True


def test_multi_provider_budget_total_cap_applies_to_paid_only_not_kiwi() -> None:
    class OnewayProvider:
        def __init__(self, provider_id: str, price: int) -> None:
            self.provider_id = provider_id
            self.price = price

        def get_calendar_prices(self, **kwargs):  # type: ignore[no-untyped-def]
            return {}

        def get_best_oneway(self, **kwargs):  # type: ignore[no-untyped-def]
            return {
                "price": self.price,
                "formatted_price": str(self.price),
                "currency": "RON",
                "duration_seconds": 3600,
                "stops": 1,
                "transfer_events": 1,
                "booking_url": None,
                "segments": [],
                "provider": self.provider_id,
            }

        def get_best_return(self, **kwargs):  # type: ignore[no-untyped-def]
            return None

    client = MultiProviderClient(
        [OnewayProvider("kiwi", 1800), OnewayProvider("amadeus", 1500)],
        max_total_calls=1,
        max_calls_by_provider={"kiwi": 1, "amadeus": 1},
    )
    best = client.get_best_oneway(
        source="OTP",
        destination="SEZ",
        departure_iso="2026-04-20",
        currency="RON",
        max_stops_per_leg=2,
        adults=1,
        hand_bags=1,
        hold_bags=0,
    )
    assert best is not None
    assert best.get("provider") == "amadeus"
    stats = client.stats_snapshot()
    assert (stats.get("budget") or {}).get("used_calls_by_provider", {}).get("kiwi") == 1
    assert (stats.get("budget") or {}).get("used_calls_by_provider", {}).get("amadeus") == 1
    assert (stats.get("budget") or {}).get("used_total_calls") == 1
    assert not (stats.get("oneway_skipped_budget") or {}).get("amadeus")


@pytest.mark.parametrize("free_provider_id", ["kayak", "momondo"])
def test_multi_provider_budget_total_cap_excludes_free_scrapers(free_provider_id: str) -> None:
    class OnewayProvider:
        def __init__(self, provider_id: str, price: int) -> None:
            self.provider_id = provider_id
            self.price = price

        def get_calendar_prices(self, **kwargs):  # type: ignore[no-untyped-def]
            return {}

        def get_best_oneway(self, **kwargs):  # type: ignore[no-untyped-def]
            return {
                "price": self.price,
                "formatted_price": str(self.price),
                "currency": "RON",
                "duration_seconds": 3600,
                "stops": 1,
                "transfer_events": 1,
                "booking_url": None,
                "segments": [],
                "provider": self.provider_id,
            }

        def get_best_return(self, **kwargs):  # type: ignore[no-untyped-def]
            return None

    client = MultiProviderClient(
        [
            OnewayProvider("kiwi", 1800),
            OnewayProvider(free_provider_id, 1700),
            OnewayProvider("amadeus", 1600),
        ],
        max_total_calls=1,
        max_calls_by_provider={"kiwi": 1, free_provider_id: 1, "amadeus": 1},
    )
    best = client.get_best_oneway(
        source="OTP",
        destination="SEZ",
        departure_iso="2026-04-20",
        currency="RON",
        max_stops_per_leg=2,
        adults=1,
        hand_bags=1,
        hold_bags=0,
    )
    assert best is not None
    assert best.get("provider") == "amadeus"
    stats = client.stats_snapshot()
    assert (stats.get("budget") or {}).get("used_calls_by_provider", {}).get("kiwi") == 1
    assert (stats.get("budget") or {}).get("used_calls_by_provider", {}).get(free_provider_id) == 1
    assert (stats.get("budget") or {}).get("used_calls_by_provider", {}).get("amadeus") == 1
    assert (stats.get("budget") or {}).get("used_total_calls") == 1


def test_multi_provider_budget_total_cap_still_limits_other_paid_providers() -> None:
    class OnewayProvider:
        def __init__(self, provider_id: str, price: int) -> None:
            self.provider_id = provider_id
            self.price = price

        def get_calendar_prices(self, **kwargs):  # type: ignore[no-untyped-def]
            return {}

        def get_best_oneway(self, **kwargs):  # type: ignore[no-untyped-def]
            return {
                "price": self.price,
                "formatted_price": str(self.price),
                "currency": "RON",
                "duration_seconds": 3600,
                "stops": 1,
                "transfer_events": 1,
                "booking_url": None,
                "segments": [],
                "provider": self.provider_id,
            }

        def get_best_return(self, **kwargs):  # type: ignore[no-untyped-def]
            return None

    client = MultiProviderClient(
        [
            OnewayProvider("kiwi", 1800),
            OnewayProvider("amadeus", 1700),
            OnewayProvider("serpapi", 1200),
        ],
        max_total_calls=1,
        max_calls_by_provider={"kiwi": 1, "amadeus": 1, "serpapi": 1},
    )
    best = client.get_best_oneway(
        source="OTP",
        destination="SEZ",
        departure_iso="2026-04-20",
        currency="RON",
        max_stops_per_leg=2,
        adults=1,
        hand_bags=1,
        hold_bags=0,
    )
    assert best is not None
    assert best.get("provider") == "amadeus"
    stats = client.stats_snapshot()
    assert (stats.get("budget") or {}).get("used_total_calls") == 1
    assert (stats.get("oneway_skipped_budget") or {}).get("serpapi") == 1


@pytest.mark.parametrize(
    ("max_stops", "expected"),
    [
        (0, 1),
        (1, 2),
        (2, 3),
        (3, 3),
    ],
)
def test_serpapi_stops_mapping_matches_api_semantics(max_stops: int, expected: int) -> None:
    assert SerpApiGoogleFlightsClient._stops_param(max_stops) == expected


def test_parse_config_zero_budget_values_disable_caps() -> None:
    optimizer = SplitTripOptimizer(KiwiClient(), AirportCoordinates())
    config = optimizer.parse_search_config(
        {
            "origins": ["OTP"],
            "destinations": ["MGA"],
            "period_start": "2026-03-10",
            "period_end": "2026-03-24",
            "calendar_hubs_prefetch": 0,
            "max_validate_oneway_keys_per_destination": 0,
            "max_validate_return_keys_per_destination": 0,
            "max_total_provider_calls": 0,
            "max_calls_kiwi": 0,
            "max_calls_amadeus": 0,
            "max_calls_serpapi": 0,
        }
    )
    assert config.calendar_hubs_prefetch is None
    assert config.max_validate_oneway_keys_per_destination is None
    assert config.max_validate_return_keys_per_destination is None
    assert config.max_total_provider_calls is None
    assert config.max_calls_kiwi is None
    assert config.max_calls_amadeus is None
    assert config.max_calls_serpapi is None


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        ({"max_transfers_per_direction": 2}, 2),
        ({"max_layovers_per_direction": 3}, 3),
    ],
)
def test_parse_config_unified_transfer_cap(payload: dict[str, object], expected: int) -> None:
    optimizer = SplitTripOptimizer(KiwiClient(), AirportCoordinates())
    base = {
        "origins": ["OTP"],
        "destinations": ["MGA"],
        "period_start": "2026-03-10",
        "period_end": "2026-03-24",
    }
    base.update(payload)
    config = optimizer.parse_search_config(base)
    assert config.max_transfers_per_direction == expected
    assert config.max_layovers_per_direction == expected
    assert config.max_stops_per_leg == expected


def test_parse_config_default_budget_caps_are_unbounded() -> None:
    optimizer = SplitTripOptimizer(KiwiClient(), AirportCoordinates())
    config = optimizer.parse_search_config(
        {
            "origins": ["OTP"],
            "destinations": ["MGA"],
            "period_start": "2026-03-10",
            "period_end": "2026-03-24",
        }
    )
    assert config.max_total_provider_calls is None
    assert config.max_calls_kiwi is None
    assert config.max_calls_amadeus is None
    assert config.max_calls_serpapi is None


def test_parse_config_candidate_pool_multiplier_allows_up_to_50() -> None:
    optimizer = SplitTripOptimizer(KiwiClient(), AirportCoordinates())
    config = optimizer.parse_search_config(
        {
            "origins": ["OTP"],
            "destinations": ["MGA"],
            "period_start": "2026-03-10",
            "period_end": "2026-03-24",
            "estimated_pool_multiplier": 80,
        }
    )
    assert config.estimated_pool_multiplier == 50


def test_provider_no_result_is_tracked_separately_from_errors() -> None:
    class NoResultProvider:
        def __init__(self, provider_id: str) -> None:
            self.provider_id = provider_id

        def get_calendar_prices(self, **kwargs):  # type: ignore[no-untyped-def]
            return {}

        def get_best_oneway(self, **kwargs):  # type: ignore[no-untyped-def]
            raise ProviderNoResultError("No offers for route")

        def get_best_return(self, **kwargs):  # type: ignore[no-untyped-def]
            raise ProviderNoResultError("No offers for route")

    client = MultiProviderClient([NoResultProvider("amadeus")], max_total_calls=4)
    oneway = client.get_best_oneway(
        source="OTP",
        destination="MGA",
        departure_iso="2026-03-10",
        currency="RON",
        max_stops_per_leg=2,
        adults=1,
        hand_bags=0,
        hold_bags=0,
    )
    assert oneway is None
    returned = client.get_best_return(
        source="OTP",
        destination="MGA",
        outbound_iso="2026-03-10",
        inbound_iso="2026-03-24",
        currency="RON",
        max_stops_per_leg=2,
        adults=1,
        hand_bags=0,
        hold_bags=0,
    )
    assert returned is None
    stats = client.stats_snapshot()
    assert (stats.get("oneway_no_result") or {}).get("amadeus") == 1
    assert (stats.get("return_no_result") or {}).get("amadeus") == 1
    assert not (stats.get("oneway_errors") or {}).get("amadeus")
    assert not (stats.get("return_errors") or {}).get("amadeus")


def test_parse_config_default_search_timeout_is_disabled() -> None:
    optimizer = SplitTripOptimizer(KiwiClient(), AirportCoordinates())
    config = optimizer.parse_search_config(
        {
            "origins": ["OTP"],
            "destinations": ["MGA"],
            "period_start": "2026-03-10",
            "period_end": "2026-03-24",
        }
    )
    assert config.search_timeout_seconds == 0


def test_parse_config_explicit_search_timeout_is_preserved() -> None:
    optimizer = SplitTripOptimizer(KiwiClient(), AirportCoordinates())
    config = optimizer.parse_search_config(
        {
            "origins": ["OTP"],
            "destinations": ["MGA"],
            "period_start": "2026-03-10",
            "period_end": "2026-03-24",
            "search_timeout_seconds": 1800,
        }
    )
    assert config.search_timeout_seconds == 1800


def test_build_search_client_applies_kiwi_cap_only_to_kiwi() -> None:
    optimizer = SplitTripOptimizer(KiwiClient(), AirportCoordinates())
    optimizer.update_runtime_provider_secrets(
        {
            "amadeus_client_id": "id",
            "amadeus_client_secret": "secret",
            "serpapi_api_key": "key",
        }
    )
    config = optimizer.parse_search_config(
        {
            "origins": ["OTP"],
            "destinations": ["MGA"],
            "period_start": "2026-03-10",
            "period_end": "2026-03-24",
            "providers": "kiwi,kayak,momondo,amadeus,serpapi",
            "max_calls_kiwi": 42,
            "max_calls_amadeus": 99,
            "max_calls_serpapi": 77,
        }
    )
    client, _, _ = optimizer._build_search_client(config)
    budget = client.stats_snapshot().get("budget") or {}
    caps = budget.get("max_calls_by_provider") or {}
    assert caps.get("kiwi") == 42
    assert caps.get("kayak") is None
    assert caps.get("momondo") is None
    assert caps.get("googleflights") is None
    assert caps.get("skyscanner") is None
    assert caps.get("amadeus") == 99
    assert caps.get("serpapi") == 77


def test_provider_fd_exhaustion_triggers_cooldown_skip() -> None:
    class FDExhaustionProvider:
        def __init__(self) -> None:
            self.provider_id = "fdtest"
            self.calls = 0

        def get_calendar_prices(self, **kwargs):  # type: ignore[no-untyped-def]
            return {}

        def get_best_oneway(self, **kwargs):  # type: ignore[no-untyped-def]
            self.calls += 1
            raise OSError(24, "Too many open files")

        def get_best_return(self, **kwargs):  # type: ignore[no-untyped-def]
            self.calls += 1
            raise OSError(24, "Too many open files")

    provider = FDExhaustionProvider()
    client = MultiProviderClient([provider])

    first = client.get_best_oneway(
        source="OTP",
        destination="MGA",
        departure_iso="2026-03-10",
        currency="RON",
        max_stops_per_leg=2,
        adults=1,
        hand_bags=0,
        hold_bags=0,
    )
    second = client.get_best_oneway(
        source="OTP",
        destination="MGA",
        departure_iso="2026-03-11",
        currency="RON",
        max_stops_per_leg=2,
        adults=1,
        hand_bags=0,
        hold_bags=0,
    )

    assert first is None
    assert second is None
    assert provider.calls == 1
    stats = client.stats_snapshot()
    assert (stats.get("oneway_errors") or {}).get("fdtest") == 1
    assert (stats.get("oneway_skipped_cooldown") or {}).get("fdtest") == 1
