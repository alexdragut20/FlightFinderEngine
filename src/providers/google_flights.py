from __future__ import annotations

import os
import re
from typing import Any

from ..config import ALLOW_PLAYWRIGHT_PROVIDERS
from ..exceptions import ProviderBlockedError, ProviderNoResultError
from ..utils import (
    build_comparison_links,
    parse_duration_text_seconds,
    parse_google_flights_text_datetime,
    parse_money_amount_int,
)
from ..utils.constants import (
    GOOGLE_FLIGHTS_CONSENT_MARKER,
    GOOGLE_FLIGHTS_IMPERSONATE_PROFILE,
    PRICE_SENTINEL,
)
from ._cache import per_instance_lru_cache


class GoogleFlightsLocalClient:
    """Provider client for Google Flights local-browser queries."""

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
        """Initialize the GoogleFlightsLocalClient.

        Args:
            fetch_mode: Fetch strategy to use for the provider request.
        """
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
        """Handle ensure fast flights.

        Returns:
            bool: Handle ensure fast flights.
        """
        if self._fast_flights_loaded:
            return self._fast_flights_available
        self._fast_flights_loaded = True
        try:
            import fast_flights.core as fast_flights_core
            from fast_flights import FlightData as FF_FlightData
            from fast_flights import Passengers as FF_Passengers
            from fast_flights import get_flights as ff_get_flights
            from primp import Client as PrimpClient

            def _patched_fetch(params: dict[str, Any]) -> Any:
                client = PrimpClient(
                    impersonate=GOOGLE_FLIGHTS_IMPERSONATE_PROFILE,
                    verify=False,
                    headers={"Accept-Language": "en-US,en;q=0.9"},
                )
                response = client.get("https://www.google.com/travel/flights", params=params)
                response_text = str(
                    getattr(response, "text_markdown", None) or getattr(response, "text", "") or ""
                )
                if response.status_code != 200:
                    raise AssertionError(f"{response.status_code} Result: {response_text}")
                if GOOGLE_FLIGHTS_CONSENT_MARKER in response_text:
                    raise RuntimeError("Google Flights returned a consent page instead of fares.")
                return response

            fast_flights_core.fetch = _patched_fetch
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

    def _fetch_mode_ready(self, fetch_mode: str) -> bool:
        """Return whether the requested fetch mode is ready for use.

        Args:
            fetch_mode: Fetch strategy to use for the provider request.

        Returns:
            bool: True when the requested mode is ready; otherwise, False.
        """
        if not self._ensure_fast_flights():
            return False
        normalized_mode = str(fetch_mode or "common").strip().lower()
        if normalized_mode != "local":
            return True
        if not ALLOW_PLAYWRIGHT_PROVIDERS:
            self._fast_flights_error = (
                "Google Flights local mode requires ALLOW_PLAYWRIGHT_PROVIDERS=1."
            )
            return False
        try:
            import playwright.async_api  # noqa: F401
        except Exception as exc:
            self._fast_flights_error = (
                "playwright is required for local Google Flights mode "
                f"(install with `python3 -m pip install playwright` and "
                f"`python3 -m playwright install chromium`): {exc}"
            )
            return False
        return True

    def is_configured(self) -> bool:
        """Return whether the client is configured for use.

        Returns:
            bool: True when the client is configured for use; otherwise, False.
        """
        return self._ensure_fast_flights()

    def configuration_hint(self) -> str | None:
        """Return setup guidance for the client.

        Returns:
            str | None: Setup guidance for the client.
        """
        if self._ensure_fast_flights():
            return None
        return "Install fast-flights to enable Google Flights."

    @staticmethod
    def _carrier_from_name(name: Any) -> tuple[str, str]:
        """Extract a carrier code from the carrier name.

        Args:
            name: Human-readable name for the object.

        Returns:
            tuple[str, str]: Extract a carrier code from the carrier name.
        """
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
        """Count the stops in a flight option.

        Args:
            value: Input value to process.

        Returns:
            int: Count the stops in a flight option.
        """
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
        """Convert a provider result into the normalized one-way schema.

        Args:
            source: Origin airport code for the request.
            destination: Destination airport code for the request.
            departure_iso: Departure date in ISO 8601 format.
            currency: Currency code for pricing output.
            flight: Flight payload to inspect.
            booking_url: URL for booking.
            max_stops_per_leg: Max stops per leg.

        Returns:
            dict[str, Any] | None: Converted provider result into the normalized one-way schema.
        """
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
        """Build the sort key for candidate ranking.

        Args:
            candidate: Mapping of candidate.

        Returns:
            tuple[int, int, int]: The sort key for candidate ranking.
        """
        return (
            int(candidate.get("price") or PRICE_SENTINEL),
            int(candidate.get("stops") or 0),
            int(candidate.get("duration_seconds") or PRICE_SENTINEL),
        )

    @staticmethod
    def _manual_search_url(
        *,
        source: str,
        destination: str,
        outbound_iso: str,
        inbound_iso: str | None,
        currency: str,
        adults: int,
        max_stops_per_leg: int,
    ) -> str | None:
        """Build the Google Flights manual search URL for the current query."""
        comparison_links = build_comparison_links(
            source,
            destination,
            outbound_iso,
            inbound_iso or outbound_iso,
            adults=adults,
            max_stops_per_leg=max_stops_per_leg,
            currency=currency,
        )
        return str(comparison_links.get("google_flights") or "").strip() or None

    def _fetch_flights(
        self,
        *,
        source: str,
        destination: str,
        date_iso: str,
        currency: str,
        adults: int,
        max_stops_per_leg: int,
        manual_search_url: str | None = None,
    ) -> list[Any]:
        """Fetch flight options from the provider backend.

        Args:
            source: Origin airport code for the request.
            destination: Destination airport code for the request.
            date_iso: Travel date in ISO 8601 format.
            currency: Currency code for pricing output.
            adults: Number of adult travelers.
            max_stops_per_leg: Max stops per leg.

        Returns:
            list[Any]: Flight options from the provider backend.
        """
        if not self._ensure_fast_flights():
            raise RuntimeError(
                "Google Flights provider is not ready. "
                + (self._fast_flights_error or "fast-flights is unavailable.")
            )
        candidate_modes = [self._fetch_mode]
        if self._fetch_mode == "common" and ALLOW_PLAYWRIGHT_PROVIDERS:
            candidate_modes.append("local")
        elif self._fetch_mode == "local":
            candidate_modes.append("common")

        last_error: Exception | None = None
        seen_modes: set[str] = set()
        for fetch_mode in candidate_modes:
            normalized_mode = str(fetch_mode or "common").strip().lower()
            if normalized_mode in seen_modes:
                continue
            seen_modes.add(normalized_mode)
            if not self._fetch_mode_ready(normalized_mode):
                continue
            try:
                return self._fetch_flights_for_mode(
                    source=source,
                    destination=destination,
                    date_iso=date_iso,
                    currency=currency,
                    adults=adults,
                    max_stops_per_leg=max_stops_per_leg,
                    fetch_mode=normalized_mode,
                    manual_search_url=manual_search_url,
                )
            except ProviderNoResultError as exc:
                last_error = exc
                continue
            except RuntimeError as exc:
                last_error = exc
                continue
        if last_error is not None:
            raise last_error
        raise ProviderNoResultError("Google Flights returned no offers for this query.")

    def _fetch_flights_for_mode(
        self,
        *,
        source: str,
        destination: str,
        date_iso: str,
        currency: str,
        adults: int,
        max_stops_per_leg: int,
        fetch_mode: str,
        manual_search_url: str | None = None,
    ) -> list[Any]:
        """Fetch flight options for a specific Google Flights fetch mode.

        Args:
            source: Origin airport code for the request.
            destination: Destination airport code for the request.
            date_iso: Travel date in ISO 8601 format.
            currency: Currency code for pricing output.
            adults: Number of adult travelers.
            max_stops_per_leg: Max stops per leg.
            fetch_mode: Fetch strategy to use for the provider request.

        Returns:
            list[Any]: Flight options returned by the provider backend.
        """
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
                fetch_mode=fetch_mode,
                max_stops=max(0, int(max_stops_per_leg)),
            )
        except RuntimeError as exc:
            message = str(exc)
            lowered = message.lower()
            if "before you continue" in lowered or "consent page" in lowered:
                raise ProviderBlockedError(
                    "Google Flights returned a consent or challenge page instead of fares.",
                    manual_search_url=manual_search_url,
                ) from exc
            if "no flights found" in lowered:
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
        """Fetch calendar prices for the requested market.

        Args:
            source: Origin airport code for the request.
            destination: Destination airport code for the request.
            date_start_iso: Start date in ISO 8601 format.
            date_end_iso: End date in ISO 8601 format.
            currency: Currency code for pricing output.
            max_stops_per_leg: Max stops per leg.
            adults: Number of adult travelers.
            hand_bags: Number of cabin bags per adult traveler.
            hold_bags: Number of checked bags per adult traveler.

        Returns:
            dict[str, int]: Calendar prices for the requested market.
        """
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
        """Fetch the best one-way itinerary for the requested market.

        Args:
            source: Origin airport code for the request.
            destination: Destination airport code for the request.
            departure_iso: Departure date in ISO 8601 format.
            currency: Currency code for pricing output.
            max_stops_per_leg: Max stops per leg.
            adults: Number of adult travelers.
            hand_bags: Number of cabin bags per adult traveler.
            hold_bags: Number of checked bags per adult traveler.
            max_connection_layover_seconds: Duration in seconds for max connection layover.

        Returns:
            dict[str, Any] | None: The best one-way itinerary for the requested market.
        """
        if not self.is_configured():
            return None
        source_code = source.upper()
        destination_code = destination.upper()
        booking_url = self._manual_search_url(
            source=source_code,
            destination=destination_code,
            outbound_iso=departure_iso,
            inbound_iso=None,
            currency=currency,
            adults=adults,
            max_stops_per_leg=max_stops_per_leg,
        )
        flights = self._fetch_flights(
            source=source_code,
            destination=destination_code,
            date_iso=departure_iso,
            currency=currency,
            adults=adults,
            max_stops_per_leg=max_stops_per_leg,
            manual_search_url=booking_url,
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
        """Fetch the best round-trip itinerary for the requested market.

        Args:
            source: Origin airport code for the request.
            destination: Destination airport code for the request.
            outbound_iso: Outbound travel date in ISO 8601 format.
            inbound_iso: Inbound travel date in ISO 8601 format.
            currency: Currency code for pricing output.
            max_stops_per_leg: Max stops per leg.
            adults: Number of adult travelers.
            hand_bags: Number of cabin bags per adult traveler.
            hold_bags: Number of checked bags per adult traveler.
            max_connection_layover_seconds: Duration in seconds for max connection layover.

        Returns:
            dict[str, Any] | None: The best round-trip itinerary for the requested market.
        """
        if not self.is_configured():
            return None
        booking_url = self._manual_search_url(
            source=source.upper(),
            destination=destination.upper(),
            outbound_iso=outbound_iso,
            inbound_iso=inbound_iso,
            currency=currency,
            adults=adults,
            max_stops_per_leg=max_stops_per_leg,
        )
        try:
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
        except ProviderBlockedError as exc:
            raise ProviderBlockedError(
                str(exc),
                manual_search_url=booking_url or exc.manual_search_url,
                cooldown_seconds=exc.cooldown_seconds,
            ) from exc
        if not outbound or not inbound:
            return None
        total_price = int(outbound["price"]) + int(inbound["price"])
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
