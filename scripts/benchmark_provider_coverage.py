from __future__ import annotations

import argparse
import json
from collections import defaultdict
from typing import Any

from src.data.airports import AirportCoordinates
from src.providers import (
    AmadeusClient,
    AzairScrapeClient,
    GoogleFlightsLocalClient,
    KayakScrapeClient,
    KiwiClient,
    MomondoScrapeClient,
    RyanairFareFinderClient,
    SerpApiGoogleFlightsClient,
    SkyscannerScrapeClient,
    TravelpayoutsDataClient,
)


def _provider_factory(provider_id: str, coordinates: AirportCoordinates) -> Any:
    normalized = str(provider_id or "").strip().lower()
    if normalized == "kiwi":
        return KiwiClient()
    if normalized == "azair":
        return AzairScrapeClient(coordinates=coordinates)
    if normalized == "ryanair":
        return RyanairFareFinderClient()
    if normalized == "travelpayouts":
        return TravelpayoutsDataClient()
    if normalized == "amadeus":
        return AmadeusClient()
    if normalized == "serpapi":
        return SerpApiGoogleFlightsClient()
    if normalized == "googleflights":
        return GoogleFlightsLocalClient()
    if normalized == "kayak":
        return KayakScrapeClient()
    if normalized == "momondo":
        return MomondoScrapeClient()
    if normalized == "skyscanner":
        return SkyscannerScrapeClient()
    raise ValueError(f"Unsupported provider id: {provider_id}")


def _parse_provider_sets(raw_sets: list[str]) -> list[tuple[str, tuple[str, ...]]]:
    parsed: list[tuple[str, tuple[str, ...]]] = []
    for raw in raw_sets:
        provider_ids = tuple(
            dict.fromkeys(
                str(item or "").strip().lower()
                for item in str(raw or "").split(",")
                if str(item or "").strip()
            )
        )
        if not provider_ids:
            continue
        parsed.append(("+".join(provider_ids), provider_ids))
    return parsed


def _date_range(start_iso: str, end_iso: str) -> list[str]:
    from datetime import date, timedelta

    start = date.fromisoformat(start_iso)
    end = date.fromisoformat(end_iso)
    if end < start:
        return []
    days = (end - start).days
    return [(start + timedelta(days=offset)).isoformat() for offset in range(days + 1)]


def _discover_destinations(
    *,
    source: str,
    provider_id: str,
    limit: int,
    require_return_route: bool,
    coordinates: AirportCoordinates,
) -> list[str]:
    provider = _provider_factory(provider_id, coordinates)
    route_destinations = getattr(provider, "_route_destinations", None)
    market_supported = getattr(provider, "_market_supported", None)
    if not callable(route_destinations):
        raise ValueError(
            f"Provider {provider_id} does not expose route discovery for --destinations-from-provider."
        )
    destinations = [str(item or "").strip().upper() for item in route_destinations(source)]
    out: list[str] = []
    for destination in destinations:
        if not destination:
            continue
        if require_return_route and callable(market_supported):
            if not market_supported(destination, source):
                continue
        out.append(destination)
        if limit > 0 and len(out) >= limit:
            break
    return out


def _safe_calendar_prices(
    provider: Any,
    *,
    source: str,
    destination: str,
    date_start_iso: str,
    date_end_iso: str,
    currency: str,
    max_stops_per_leg: int,
    adults: int,
    hand_bags: int,
    hold_bags: int,
) -> tuple[dict[str, int], str | None]:
    try:
        prices = provider.get_calendar_prices(
            source=source,
            destination=destination,
            date_start_iso=date_start_iso,
            date_end_iso=date_end_iso,
            currency=currency,
            max_stops_per_leg=max_stops_per_leg,
            adults=adults,
            hand_bags=hand_bags,
            hold_bags=hold_bags,
        )
    except Exception as exc:  # pragma: no cover - live benchmark helper
        return {}, f"{type(exc).__name__}: {exc}"
    if not isinstance(prices, dict):
        return {}, None
    normalized: dict[str, int] = {}
    for date_iso, amount in prices.items():
        try:
            normalized[str(date_iso)[:10]] = int(amount)
        except (TypeError, ValueError):
            continue
    return normalized, None


def _safe_exact_oneway(
    provider: Any,
    *,
    source: str,
    destination: str,
    departure_iso: str,
    currency: str,
    max_stops_per_leg: int,
    adults: int,
    hand_bags: int,
    hold_bags: int,
) -> tuple[dict[str, Any] | None, str | None]:
    try:
        return (
            provider.get_best_oneway(
                source=source,
                destination=destination,
                departure_iso=departure_iso,
                currency=currency,
                max_stops_per_leg=max_stops_per_leg,
                adults=adults,
                hand_bags=hand_bags,
                hold_bags=hold_bags,
            ),
            None,
        )
    except Exception as exc:  # pragma: no cover - live benchmark helper
        return None, f"{type(exc).__name__}: {exc}"


def _safe_exact_return(
    provider: Any,
    *,
    source: str,
    destination: str,
    outbound_iso: str,
    inbound_iso: str,
    currency: str,
    max_stops_per_leg: int,
    adults: int,
    hand_bags: int,
    hold_bags: int,
) -> tuple[dict[str, Any] | None, str | None]:
    try:
        return (
            provider.get_best_return(
                source=source,
                destination=destination,
                outbound_iso=outbound_iso,
                inbound_iso=inbound_iso,
                currency=currency,
                max_stops_per_leg=max_stops_per_leg,
                adults=adults,
                hand_bags=hand_bags,
                hold_bags=hold_bags,
            ),
            None,
        )
    except Exception as exc:  # pragma: no cover - live benchmark helper
        return None, f"{type(exc).__name__}: {exc}"


def _collect_set_metrics(
    *,
    provider_ids: tuple[str, ...],
    source: str,
    destinations: list[str],
    date_start_iso: str,
    date_end_iso: str,
    outbound_iso: str,
    inbound_iso: str | None,
    currency: str,
    max_stops_per_leg: int,
    adults: int,
    hand_bags: int,
    hold_bags: int,
    coordinates: AirportCoordinates,
) -> dict[str, Any]:
    providers = {
        provider_id: _provider_factory(provider_id, coordinates) for provider_id in provider_ids
    }
    calendar_days = set(_date_range(date_start_iso, date_end_iso))
    unique_calendar_cells: set[tuple[str, str]] = set()
    unique_calendar_routes: set[str] = set()
    unique_oneway_routes: set[str] = set()
    unique_return_routes: set[str] = set()
    provider_calendar_cell_counts: dict[str, int] = defaultdict(int)
    provider_calendar_route_counts: dict[str, int] = defaultdict(int)
    provider_oneway_counts: dict[str, int] = defaultdict(int)
    provider_return_counts: dict[str, int] = defaultdict(int)
    errors: dict[str, list[str]] = defaultdict(list)
    exact_details: dict[str, dict[str, dict[str, Any]]] = {
        "oneway": {},
        "return": {},
    }

    for destination in destinations:
        route_calendar_hit_by_provider: set[str] = set()
        route_oneway_prices: dict[str, int] = {}
        route_return_prices: dict[str, int] = {}
        for provider_id, provider in providers.items():
            prices, calendar_error = _safe_calendar_prices(
                provider,
                source=source,
                destination=destination,
                date_start_iso=date_start_iso,
                date_end_iso=date_end_iso,
                currency=currency,
                max_stops_per_leg=max_stops_per_leg,
                adults=adults,
                hand_bags=hand_bags,
                hold_bags=hold_bags,
            )
            if calendar_error:
                errors[provider_id].append(f"calendar {source}->{destination}: {calendar_error}")
            cell_count = 0
            for date_iso, amount in prices.items():
                if date_iso not in calendar_days:
                    continue
                try:
                    int(amount)
                except (TypeError, ValueError):
                    continue
                unique_calendar_cells.add((destination, date_iso))
                unique_calendar_routes.add(destination)
                route_calendar_hit_by_provider.add(provider_id)
                cell_count += 1
            provider_calendar_cell_counts[provider_id] += cell_count

            oneway_result, oneway_error = _safe_exact_oneway(
                provider,
                source=source,
                destination=destination,
                departure_iso=outbound_iso,
                currency=currency,
                max_stops_per_leg=max_stops_per_leg,
                adults=adults,
                hand_bags=hand_bags,
                hold_bags=hold_bags,
            )
            if oneway_error:
                errors[provider_id].append(f"oneway {source}->{destination}: {oneway_error}")
            elif oneway_result and oneway_result.get("price") is not None:
                provider_oneway_counts[provider_id] += 1
                unique_oneway_routes.add(destination)
                route_oneway_prices[provider_id] = int(oneway_result["price"])

            if inbound_iso:
                return_result, return_error = _safe_exact_return(
                    provider,
                    source=source,
                    destination=destination,
                    outbound_iso=outbound_iso,
                    inbound_iso=inbound_iso,
                    currency=currency,
                    max_stops_per_leg=max_stops_per_leg,
                    adults=adults,
                    hand_bags=hand_bags,
                    hold_bags=hold_bags,
                )
                if return_error:
                    errors[provider_id].append(
                        f"return {source}->{destination} {outbound_iso}/{inbound_iso}: {return_error}"
                    )
                elif return_result and return_result.get("price") is not None:
                    provider_return_counts[provider_id] += 1
                    unique_return_routes.add(destination)
                    route_return_prices[provider_id] = int(return_result["price"])

        for provider_id in route_calendar_hit_by_provider:
            provider_calendar_route_counts[provider_id] += 1

        exact_details["oneway"][destination] = route_oneway_prices
        if inbound_iso:
            exact_details["return"][destination] = route_return_prices

    return {
        "provider_ids": list(provider_ids),
        "routes_tested": len(destinations),
        "calendar": {
            "unique_routes_with_prices": len(unique_calendar_routes),
            "unique_date_cells_with_prices": len(unique_calendar_cells),
            "provider_route_quotes": dict(sorted(provider_calendar_route_counts.items())),
            "provider_date_cell_quotes": dict(sorted(provider_calendar_cell_counts.items())),
        },
        "exact_oneway": {
            "unique_routes_with_results": len(unique_oneway_routes),
            "provider_route_quotes": dict(sorted(provider_oneway_counts.items())),
        },
        "exact_return": {
            "unique_routes_with_results": len(unique_return_routes),
            "provider_route_quotes": dict(sorted(provider_return_counts.items())),
        },
        "exact_details": exact_details,
        "errors": {key: value[:25] for key, value in sorted(errors.items())},
    }


def _add_baseline_comparison(
    baseline: dict[str, Any],
    current: dict[str, Any],
) -> None:
    baseline_oneway = baseline.get("exact_details", {}).get("oneway", {})
    current_oneway = current.get("exact_details", {}).get("oneway", {})
    baseline_return = baseline.get("exact_details", {}).get("return", {})
    current_return = current.get("exact_details", {}).get("return", {})

    comparison: dict[str, Any] = {
        "calendar": {},
        "exact_oneway": {
            "unique_route_delta": int(current["exact_oneway"]["unique_routes_with_results"])
            - int(baseline["exact_oneway"]["unique_routes_with_results"]),
            "cheaper_routes": 0,
            "same_price_routes": 0,
            "worse_routes": 0,
            "new_routes": 0,
        },
        "exact_return": {
            "unique_route_delta": int(current["exact_return"]["unique_routes_with_results"])
            - int(baseline["exact_return"]["unique_routes_with_results"]),
            "cheaper_routes": 0,
            "same_price_routes": 0,
            "worse_routes": 0,
            "new_routes": 0,
        },
    }
    comparison["calendar"] = {
        "unique_route_delta": int(current["calendar"]["unique_routes_with_prices"])
        - int(baseline["calendar"]["unique_routes_with_prices"]),
        "unique_date_cell_delta": int(current["calendar"]["unique_date_cells_with_prices"])
        - int(baseline["calendar"]["unique_date_cells_with_prices"]),
    }

    for destination, route_prices in current_oneway.items():
        current_best = min(route_prices.values()) if route_prices else None
        baseline_prices = baseline_oneway.get(destination) or {}
        baseline_best = min(baseline_prices.values()) if baseline_prices else None
        if current_best is None:
            continue
        if baseline_best is None:
            comparison["exact_oneway"]["new_routes"] += 1
        elif current_best < baseline_best:
            comparison["exact_oneway"]["cheaper_routes"] += 1
        elif current_best == baseline_best:
            comparison["exact_oneway"]["same_price_routes"] += 1
        else:
            comparison["exact_oneway"]["worse_routes"] += 1

    for destination, route_prices in current_return.items():
        current_best = min(route_prices.values()) if route_prices else None
        baseline_prices = baseline_return.get(destination) or {}
        baseline_best = min(baseline_prices.values()) if baseline_prices else None
        if current_best is None:
            continue
        if baseline_best is None:
            comparison["exact_return"]["new_routes"] += 1
        elif current_best < baseline_best:
            comparison["exact_return"]["cheaper_routes"] += 1
        elif current_best == baseline_best:
            comparison["exact_return"]["same_price_routes"] += 1
        else:
            comparison["exact_return"]["worse_routes"] += 1

    current["vs_baseline"] = comparison


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark provider sets on the same route bundle. "
            "Primary success metric is unique route/date coverage, not price drift."
        )
    )
    parser.add_argument("--source", required=True)
    parser.add_argument("--destinations", default="")
    parser.add_argument("--destinations-from-provider", default="")
    parser.add_argument("--limit-destinations", type=int, default=0)
    parser.add_argument("--require-return-route", action="store_true")
    parser.add_argument("--date-start", required=True)
    parser.add_argument("--date-end", required=True)
    parser.add_argument("--outbound-date", required=True)
    parser.add_argument("--inbound-date", default="")
    parser.add_argument("--currency", default="EUR")
    parser.add_argument("--max-stops", type=int, default=1)
    parser.add_argument("--adults", type=int, default=2)
    parser.add_argument("--hand-bags", type=int, default=1)
    parser.add_argument("--hold-bags", type=int, default=0)
    parser.add_argument(
        "--set",
        dest="provider_sets",
        action="append",
        required=True,
        help="Comma-separated provider ids. Repeat this flag to compare multiple sets.",
    )
    args = parser.parse_args()

    coordinates = AirportCoordinates()
    source = str(args.source or "").strip().upper()
    provider_sets = _parse_provider_sets(args.provider_sets)
    if not provider_sets:
        raise SystemExit("No valid provider sets supplied.")

    destinations = [
        str(item or "").strip().upper()
        for item in str(args.destinations or "").split(",")
        if str(item or "").strip()
    ]
    if not destinations and args.destinations_from_provider:
        destinations = _discover_destinations(
            source=source,
            provider_id=str(args.destinations_from_provider or "").strip().lower(),
            limit=max(0, int(args.limit_destinations or 0)),
            require_return_route=bool(args.require_return_route),
            coordinates=coordinates,
        )
    if args.limit_destinations and int(args.limit_destinations) > 0:
        destinations = destinations[: int(args.limit_destinations)]
    if not destinations:
        raise SystemExit("No destinations supplied or discovered.")

    summary: dict[str, Any] = {
        "benchmark": {
            "source": source,
            "destinations": destinations,
            "date_start": args.date_start,
            "date_end": args.date_end,
            "outbound_date": args.outbound_date,
            "inbound_date": args.inbound_date or None,
            "currency": str(args.currency or "EUR").strip().upper() or "EUR",
            "max_stops": int(args.max_stops),
            "adults": int(args.adults),
            "hand_bags": int(args.hand_bags),
            "hold_bags": int(args.hold_bags),
        },
        "sets": {},
    }

    baseline_result: dict[str, Any] | None = None
    for label, provider_ids in provider_sets:
        result = _collect_set_metrics(
            provider_ids=provider_ids,
            source=source,
            destinations=destinations,
            date_start_iso=args.date_start,
            date_end_iso=args.date_end,
            outbound_iso=args.outbound_date,
            inbound_iso=args.inbound_date or None,
            currency=str(args.currency or "EUR").strip().upper() or "EUR",
            max_stops_per_leg=int(args.max_stops),
            adults=max(1, int(args.adults)),
            hand_bags=max(0, int(args.hand_bags)),
            hold_bags=max(0, int(args.hold_bags)),
            coordinates=coordinates,
        )
        if baseline_result is None:
            baseline_result = result
        else:
            _add_baseline_comparison(baseline_result, result)
        summary["sets"][label] = result

    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
