from __future__ import annotations

import os
import re
from typing import Any

from ..config import ALLOW_PLAYWRIGHT_PROVIDERS
from ..exceptions import ProviderNoResultError
from ..utils import (
    build_comparison_links,
    parse_duration_text_seconds,
    parse_google_flights_text_datetime,
    parse_money_amount_int,
)
from ._cache import per_instance_lru_cache


class GoogleFlightsLocalClient:
    provider_id = "googleflights"
    display_name = "Google Flights (local browser)"
    supports_calendar = False
    requires_credentials = False
    credential_env: tuple[str, ...] = ()
    docs_url = "https://www.google.com/travel/flights"
    default_enabled = True

    def __init__(
        self,
        fetch_mode: str | None = None,
    ) -> None:
        normalized_fetch_mode = (
            str(fetch_mode or os.getenv("GOOGLE_FLIGHTS_FETCH_MODE") or "common").strip().lower()
        )
        if normalized_fetch_mode not in {"common", "local"}:
            normalized_fetch_mode = "common"
        if normalized_fetch_mode == "local" and not ALLOW_PLAYWRIGHT_PROVIDERS:
            normalized_fetch_mode = "common"
        self._fetch_mode = normalized_fetch_mode
        self._fast_flights_loaded = False
        self._fast_flights_available = False
        self._fast_flights_error = ""
        self._FlightData: Any = None
        self._Passengers: Any = None
        self._get_flights_fn: Any = None

    def _ensure_fast_flights(self) -> bool:
        if self._fast_flights_loaded:
            return self._fast_flights_available
        self._fast_flights_loaded = True
        try:
            from fast_flights import FlightData as FF_FlightData
            from fast_flights import Passengers as FF_Passengers
            from fast_flights import get_flights as ff_get_flights

            self._FlightData = FF_FlightData
            self._Passengers = FF_Passengers
            self._get_flights_fn = ff_get_flights
            self._fast_flights_available = True
            self._fast_flights_error = ""
        except Exception as exc:
            self._fast_flights_error = str(exc or "fast-flights import failed")
            self._fast_flights_available = False
        if self._fast_flights_available and self._fetch_mode == "local":
            try:
                import playwright.async_api  # noqa: F401
            except Exception as exc:
                self._fast_flights_available = False
                self._fast_flights_error = (
                    "playwright is required for local Google Flights mode "
                    f"(install with `python3 -m pip install playwright` and "
                    f"`python3 -m playwright install chromium`): {exc}"
                )
        return self._fast_flights_available

    def is_configured(self) -> bool:
        return self._ensure_fast_flights()

    def configuration_hint(self) -> str | None:
        if self._ensure_fast_flights():
            return None
        return "Install fast-flights to enable Google Flights."

    @staticmethod
    def _carrier_from_name(name: Any) -> tuple[str, str]:
        raw = str(name or "").strip()
        if not raw:
            return "GF", "Google Flights"
        primary = raw.split(",", 1)[0].strip()
        if not primary:
            primary = raw
        code = re.sub(r"[^A-Z]", "", primary.upper())[:3] or "GF"
        return code, primary

    @staticmethod
    def _flight_stops(value: Any) -> int:
        try:
            stops = int(value)
        except (TypeError, ValueError):
            return 0
        return max(0, stops)

    def _flight_to_oneway_candidate(
        self,
        *,
        source: str,
        destination: str,
        departure_iso: str,
        currency: str,
        flight: Any,
        booking_url: str | None,
        max_stops_per_leg: int,
    ) -> dict[str, Any] | None:
        price = parse_money_amount_int(getattr(flight, "price", None))
        if price is None:
            return None
        stops = self._flight_stops(getattr(flight, "stops", None))
        if stops > max_stops_per_leg:
            return None
        duration_seconds = parse_duration_text_seconds(getattr(flight, "duration", None))
        carrier_code, carrier_name = self._carrier_from_name(getattr(flight, "name", None))
        depart_local = parse_google_flights_text_datetime(
            getattr(flight, "departure", None),
            departure_iso,
        )
        arrive_local = parse_google_flights_text_datetime(
            getattr(flight, "arrival", None),
            departure_iso,
        )
        segments = [
            {
                "from": source,
                "to": destination,
                "from_name": source,
                "to_name": destination,
                "depart_local": depart_local,
                "arrive_local": arrive_local,
                "carrier": carrier_code,
                "carrier_name": carrier_name,
            }
        ]
        return {
            "price": int(price),
            "formatted_price": f"{int(price)} {currency}",
            "currency": currency,
            "duration_seconds": duration_seconds,
            "stops": stops,
            "transfer_events": stops,
            "booking_url": booking_url,
            "segments": segments,
            "provider": self.provider_id,
            "booking_provider": "Google Flights",
        }

    @staticmethod
    def _candidate_sort_key(candidate: dict[str, Any]) -> tuple[int, int, int]:
        return (
            int(candidate.get("price") or 10**12),
            int(candidate.get("stops") or 0),
            int(candidate.get("duration_seconds") or 10**12),
        )

    def _fetch_flights(
        self,
        *,
        source: str,
        destination: str,
        date_iso: str,
        currency: str,
        adults: int,
        max_stops_per_leg: int,
    ) -> list[Any]:
        if not self._ensure_fast_flights():
            raise RuntimeError(
                "Google Flights provider is not ready. "
                + (self._fast_flights_error or "fast-flights is unavailable.")
            )
        flight_data = [
            self._FlightData(
                date=date_iso,
                from_airport=source.upper(),
                to_airport=destination.upper(),
                max_stops=max(0, int(max_stops_per_leg)),
            )
        ]
        passengers = self._Passengers(adults=max(1, int(adults or 1)))
        try:
            result = self._get_flights_fn(
                flight_data=flight_data,
                trip="one-way",
                passengers=passengers,
                seat="economy",
                fetch_mode=self._fetch_mode,
                max_stops=max(0, int(max_stops_per_leg)),
            )
        except RuntimeError as exc:
            message = str(exc)
            lowered = message.lower()
            if "no flights found" in lowered or "before you continue" in lowered:
                raise ProviderNoResultError(
                    "Google Flights returned no offers for this query."
                ) from exc
            raise
        except AssertionError as exc:
            raise RuntimeError(f"Google Flights request failed: {exc}") from exc
        flights = list(getattr(result, "flights", []) or [])
        if not flights:
            raise ProviderNoResultError("Google Flights returned no offers for this query.")
        return flights

    @per_instance_lru_cache(maxsize=16384)
    def get_calendar_prices(
        self,
        source: str,
        destination: str,
        date_start_iso: str,
        date_end_iso: str,
        currency: str,
        max_stops_per_leg: int,
        adults: int,
        hand_bags: int,
        hold_bags: int,
    ) -> dict[str, int]:
        return {}

    @per_instance_lru_cache(maxsize=32768)
    def get_best_oneway(
        self,
        source: str,
        destination: str,
        departure_iso: str,
        currency: str,
        max_stops_per_leg: int,
        adults: int,
        hand_bags: int,
        hold_bags: int,
        max_connection_layover_seconds: int | None = None,
    ) -> dict[str, Any] | None:
        if not self.is_configured():
            return None
        source_code = source.upper()
        destination_code = destination.upper()
        comparison_links = build_comparison_links(
            source_code,
            destination_code,
            departure_iso,
            departure_iso,
            adults=adults,
            max_stops_per_leg=max_stops_per_leg,
            currency=currency,
        )
        booking_url = comparison_links.get("google_flights")
        flights = self._fetch_flights(
            source=source_code,
            destination=destination_code,
            date_iso=departure_iso,
            currency=currency,
            adults=adults,
            max_stops_per_leg=max_stops_per_leg,
        )
        best: dict[str, Any] | None = None
        for flight in flights:
            candidate = self._flight_to_oneway_candidate(
                source=source_code,
                destination=destination_code,
                departure_iso=departure_iso,
                currency=currency,
                flight=flight,
                booking_url=booking_url,
                max_stops_per_leg=max_stops_per_leg,
            )
            if not candidate:
                continue
            if best is None or self._candidate_sort_key(candidate) < self._candidate_sort_key(best):
                best = candidate
        return best

    @per_instance_lru_cache(maxsize=32768)
    def get_best_return(
        self,
        source: str,
        destination: str,
        outbound_iso: str,
        inbound_iso: str,
        currency: str,
        max_stops_per_leg: int,
        adults: int,
        hand_bags: int,
        hold_bags: int,
        max_connection_layover_seconds: int | None = None,
    ) -> dict[str, Any] | None:
        if not self.is_configured():
            return None
        outbound = self.get_best_oneway(
            source=source,
            destination=destination,
            departure_iso=outbound_iso,
            currency=currency,
            max_stops_per_leg=max_stops_per_leg,
            adults=adults,
            hand_bags=hand_bags,
            hold_bags=hold_bags,
            max_connection_layover_seconds=max_connection_layover_seconds,
        )
        inbound = self.get_best_oneway(
            source=destination,
            destination=source,
            departure_iso=inbound_iso,
            currency=currency,
            max_stops_per_leg=max_stops_per_leg,
            adults=adults,
            hand_bags=hand_bags,
            hold_bags=hold_bags,
            max_connection_layover_seconds=max_connection_layover_seconds,
        )
        if not outbound or not inbound:
            return None
        total_price = int(outbound["price"]) + int(inbound["price"])
        comparison_links = build_comparison_links(
            source,
            destination,
            outbound_iso,
            inbound_iso,
            adults=adults,
            max_stops_per_leg=max_stops_per_leg,
            currency=currency,
        )
        booking_url = comparison_links.get("google_flights")
        outbound_duration_seconds = outbound.get("duration_seconds")
        inbound_duration_seconds = inbound.get("duration_seconds")
        total_duration_seconds = (
            (int(outbound_duration_seconds or 0) + int(inbound_duration_seconds or 0))
            if outbound_duration_seconds is not None and inbound_duration_seconds is not None
            else None
        )
        return {
            "price": total_price,
            "formatted_price": f"{total_price} {currency}",
            "currency": currency,
            "duration_seconds": total_duration_seconds,
            "outbound_duration_seconds": outbound_duration_seconds,
            "inbound_duration_seconds": inbound_duration_seconds,
            "outbound_stops": int(outbound.get("stops") or 0),
            "inbound_stops": int(inbound.get("stops") or 0),
            "outbound_transfer_events": int(outbound.get("transfer_events") or 0),
            "inbound_transfer_events": int(inbound.get("transfer_events") or 0),
            "booking_url": booking_url,
            "outbound_segments": list(outbound.get("segments") or []),
            "inbound_segments": list(inbound.get("segments") or []),
            "provider": self.provider_id,
            "booking_provider": "Google Flights",
        }
