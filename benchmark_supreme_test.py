#!/usr/bin/env python3
"""Find opportunities where split-stopover beats standard round-trip pricing.

Baseline = best direct_roundtrip itinerary returned by Kiwi returnItineraries.
Opportunity = best split_stopover result cheaper than baseline in same search window.
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from urllib.parse import quote

from server import AirportCoordinates, KiwiClient, SplitTripOptimizer

REPORT_PATH = Path(__file__).resolve().parent / "cache" / "supreme_test_report.json"

SCENARIOS = [
    {
        "destination": "DPS",
        "period_start": "2026-03-01",
        "period_end": "2026-05-31",
        "min_stay": 6,
        "max_stay": 8,
    },
    {
        "destination": "ZNZ",
        "period_start": "2026-03-01",
        "period_end": "2026-05-31",
        "min_stay": 6,
        "max_stay": 8,
    },
    {
        "destination": "HKT",
        "period_start": "2026-03-01",
        "period_end": "2026-05-31",
        "min_stay": 6,
        "max_stay": 8,
    },
    {
        "destination": "MLE",
        "period_start": "2026-03-01",
        "period_end": "2026-05-31",
        "min_stay": 6,
        "max_stay": 8,
    },
    {
        "destination": "PUJ",
        "period_start": "2026-03-01",
        "period_end": "2026-05-31",
        "min_stay": 6,
        "max_stay": 8,
    },
    {
        "destination": "SEZ",
        "period_start": "2026-03-01",
        "period_end": "2026-05-31",
        "min_stay": 6,
        "max_stay": 8,
    },
    {
        "destination": "CUN",
        "period_start": "2026-03-01",
        "period_end": "2026-05-31",
        "min_stay": 6,
        "max_stay": 8,
    },
    {
        "destination": "MRU",
        "period_start": "2026-03-01",
        "period_end": "2026-05-31",
        "min_stay": 6,
        "max_stay": 8,
    },
    {
        "destination": "CMB",
        "period_start": "2026-03-01",
        "period_end": "2026-05-31",
        "min_stay": 6,
        "max_stay": 8,
    },
    {
        "destination": "BKK",
        "period_start": "2026-03-01",
        "period_end": "2026-05-31",
        "min_stay": 6,
        "max_stay": 8,
    },
    {
        "destination": "MIA",
        "period_start": "2026-03-01",
        "period_end": "2026-05-31",
        "min_stay": 6,
        "max_stay": 8,
    },
    {
        "destination": "DAR",
        "period_start": "2026-03-01",
        "period_end": "2026-05-31",
        "min_stay": 6,
        "max_stay": 8,
    },
    {
        "destination": "NBO",
        "period_start": "2026-03-01",
        "period_end": "2026-05-31",
        "min_stay": 6,
        "max_stay": 8,
    },
    {
        "destination": "DPS",
        "period_start": "2026-03-01",
        "period_end": "2026-05-31",
        "min_stay": 7,
        "max_stay": 10,
    },
    {
        "destination": "ZNZ",
        "period_start": "2026-03-01",
        "period_end": "2026-05-31",
        "min_stay": 7,
        "max_stay": 10,
    },
    {
        "destination": "HKT",
        "period_start": "2026-03-01",
        "period_end": "2026-05-31",
        "min_stay": 7,
        "max_stay": 10,
    },
    {
        "destination": "MLE",
        "period_start": "2026-03-01",
        "period_end": "2026-05-31",
        "min_stay": 7,
        "max_stay": 10,
    },
    {
        "destination": "PUJ",
        "period_start": "2026-03-01",
        "period_end": "2026-05-31",
        "min_stay": 7,
        "max_stay": 10,
    },
    {
        "destination": "SEZ",
        "period_start": "2026-03-01",
        "period_end": "2026-05-31",
        "min_stay": 7,
        "max_stay": 10,
    },
    {
        "destination": "CUN",
        "period_start": "2026-03-01",
        "period_end": "2026-05-31",
        "min_stay": 7,
        "max_stay": 10,
    },
    {
        "destination": "MRU",
        "period_start": "2026-03-01",
        "period_end": "2026-05-31",
        "min_stay": 7,
        "max_stay": 10,
    },
    {
        "destination": "MIA",
        "period_start": "2026-03-01",
        "period_end": "2026-05-31",
        "min_stay": 7,
        "max_stay": 10,
    },
    {
        "destination": "DAR",
        "period_start": "2026-03-01",
        "period_end": "2026-05-31",
        "min_stay": 7,
        "max_stay": 10,
    },
    {
        "destination": "NBO",
        "period_start": "2026-03-01",
        "period_end": "2026-05-31",
        "min_stay": 7,
        "max_stay": 10,
    },
]


def build_payload(scenario: dict[str, str | int]) -> dict[str, object]:
    return {
        "origins": ["OTP", "BBU"],
        "destinations": [scenario["destination"]],
        "period_start": scenario["period_start"],
        "period_end": scenario["period_end"],
        "min_stay_days": scenario["min_stay"],
        "max_stay_days": scenario["max_stay"],
        "min_stopover_days": 0,
        "max_stopover_days": 5,
        "max_stops_per_leg": 2,
        "max_layovers_per_direction": 2,
        "currency": "RON",
        "objective": "cheapest",
        "providers": ["kiwi"],
        "top_results": 50,
        "validate_top_per_destination": 70,
        "estimated_pool_multiplier": 5,
        "auto_hubs_per_direction": 24,
        "exhaustive_hub_scan": False,
        "io_workers": 28,
        "cpu_workers": 4,
        "passengers": {"adults": 1, "hand_bags": 1, "hold_bags": 0},
        "use_beach_presets": False,
    }


def summarize_result(item: dict[str, object]) -> dict[str, object]:
    outbound = item.get("outbound") or {}
    inbound = item.get("inbound") or {}
    return {
        "price_ron": item.get("total_price"),
        "itinerary_type": item.get("itinerary_type"),
        "origin": outbound.get("origin"),
        "destination": item.get("destination_code"),
        "outbound_date": outbound.get("date_from_origin"),
        "inbound_date": inbound.get("date_from_destination"),
        "outbound_hub": outbound.get("hub"),
        "inbound_hub": inbound.get("hub"),
        "outbound_layovers": outbound.get("layovers_count"),
        "inbound_layovers": inbound.get("layovers_count"),
    }


def date_only(value: object) -> str:
    text = str(value or "")
    if not text:
        return ""
    return text.split("T", 1)[0]


def build_comparison_links(
    origin: str, destination: str, depart_date: str, return_date: str
) -> dict[str, str]:
    orig = (origin or "").upper()
    dest = (destination or "").upper()
    dep = date_only(depart_date)
    ret = date_only(return_date)
    if not (orig and dest and dep and ret):
        return {}

    google_route = quote(f"{orig}.{dest}.{dep}*{dest}.{orig}.{ret}", safe=".*")
    return {
        "google_flights": f"https://www.google.com/travel/flights?hl=en#flt={google_route}",
        "skyscanner": (
            "https://www.skyscanner.com/transport/flights/"
            f"{orig.lower()}/{dest.lower()}/{dep.replace('-', '')}/{ret.replace('-', '')}/"
            "?adults=1&cabinclass=economy"
        ),
        "kayak": f"https://www.kayak.com/flights/{orig}-{dest}/{dep}/{ret}?sort=bestflight_a",
        "momondo": f"https://www.momondo.com/flight-search/{orig}-{dest}/{dep}/{ret}?sort=bestflight_a",
        "kiwi": f"https://www.kiwi.com/en/search/results/{orig}/{dest}/{dep}/{ret}",
    }


def main() -> int:
    optimizer = SplitTripOptimizer(KiwiClient(), AirportCoordinates())
    target_count = 10
    opportunities: list[dict[str, object]] = []
    seen_destinations: set[str] = set()

    for idx, scenario in enumerate(SCENARIOS, start=1):
        payload = build_payload(scenario)
        config = optimizer.parse_search_config(payload)
        result = optimizer.search(config)
        results = result.get("results") or []

        split = [item for item in results if item.get("itinerary_type") == "split_stopover"]
        direct = [item for item in results if item.get("itinerary_type") == "direct_roundtrip"]
        if not split or not direct:
            print(
                f"[{idx}/{len(SCENARIOS)}] {scenario['destination']}: skipped (missing split/direct)",
                flush=True,
            )
            continue

        best_split = min(split, key=lambda x: int(x.get("total_price") or 10**9))
        best_direct = min(direct, key=lambda x: int(x.get("total_price") or 10**9))
        split_price = int(best_split.get("total_price") or 10**9)
        direct_price = int(best_direct.get("total_price") or 10**9)
        improvement = direct_price - split_price

        print(
            f"[{idx}/{len(SCENARIOS)}] {scenario['destination']}: "
            f"split {split_price} vs direct {direct_price} (delta {improvement})",
            flush=True,
        )

        if improvement <= 0:
            continue

        destination_code = str(scenario["destination"]).upper()
        if destination_code in seen_destinations:
            continue

        seen_destinations.add(destination_code)
        opportunities.append(
            {
                "scenario": scenario,
                "improvement_ron": improvement,
                "engine_split_best": summarize_result(best_split),
                "baseline_direct_best": summarize_result(best_direct),
                "comparison_links": build_comparison_links(
                    origin=str((best_split.get("outbound") or {}).get("origin") or ""),
                    destination=str(scenario["destination"]),
                    depart_date=str(
                        (best_split.get("outbound") or {}).get("date_from_origin") or ""
                    ),
                    return_date=str(
                        (best_split.get("inbound") or {}).get("date_from_destination") or ""
                    ),
                ),
                "note": (
                    "Baseline uses Kiwi returnItineraries one-ticket round-trip pricing, "
                    "which closely matches what metasearch engines typically surface first."
                ),
            }
        )

        if len(opportunities) >= target_count:
            break

    opportunities.sort(key=lambda x: int(x["improvement_ron"]), reverse=True)

    report = {
        "generated_at": dt.datetime.now(dt.UTC).isoformat(),
        "target_count": target_count,
        "found_count": len(opportunities),
        "opportunities": opportunities,
    }
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(f"\nReport written to: {REPORT_PATH}", flush=True)
    print(f"Found opportunities: {len(opportunities)}", flush=True)

    for i, item in enumerate(opportunities[:target_count], start=1):
        s = item["scenario"]
        split = item["engine_split_best"]
        direct = item["baseline_direct_best"]
        print(
            f"{i}. {s['destination']} {s['period_start']}..{s['period_end']} | "
            f"split {split['price_ron']} RON ({split['outbound_hub']}->{split['inbound_hub']}) vs "
            f"direct {direct['price_ron']} RON ({direct['outbound_hub']}->{direct['inbound_hub']}) | "
            f"save {item['improvement_ron']} RON",
            flush=True,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
