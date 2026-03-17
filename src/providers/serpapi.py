from __future__ import annotations

import os
import threading
from typing import Any

import requests

from ..config import SERPAPI_RETURN_OPTION_SCAN_LIMIT, SERPAPI_SEARCH_URL
from ..utils import (
    max_segment_layover_seconds,
    parse_datetime_guess,
    parse_duration_text_seconds,
    parse_money_amount_int,
    transfer_events_from_segments,
)
from ..utils.constants import PRICE_SENTINEL
from ..utils.logging import capture_provider_response as _capture_provider_response
from ._cache import per_instance_lru_cache


class SerpApiGoogleFlightsClient:
    """Provider client for SerpApi-backed Google Flights queries."""

    provider_id = "serpapi"
    display_name = "SerpApi Google Flights"
    supports_calendar = False
    requires_credentials = True
    credential_env: tuple[str, ...] = ("SERPAPI_API_KEY",)
    docs_url = "https://serpapi.com/google-flights-api"
    default_enabled = False

    def __init__(
        self,
        api_key: str | None = None,
        search_url: str | None = None,
        return_option_scan_limit: int | None = None,
    ) -> None:
        """Initialize the SerpApiGoogleFlightsClient.

        Args:
            api_key: Dictionary key used for api.
            search_url: URL for search.
            return_option_scan_limit: Maximum number of return options to scan.
        """
        self._api_key = (
            api_key if api_key is not None else os.getenv("SERPAPI_API_KEY") or ""
        ).strip()
        self._search_url = str(search_url or SERPAPI_SEARCH_URL).rstrip("/")
        self._return_option_scan_limit = max(
            1,
            min(
                5,
                (
                    int(return_option_scan_limit)
                    if return_option_scan_limit is not None
                    else SERPAPI_RETURN_OPTION_SCAN_LIMIT
                ),
            ),
        )
        self._local = threading.local()

    def is_configured(self) -> bool:
        """Return whether the client is configured for use.

        Returns:
            bool: True when the client is configured for use; otherwise, False.
        """
        return bool(self._api_key)

    def _session(self) -> requests.Session:
        """Return the cached requests session.

        Returns:
            requests.Session: The cached requests session.
        """
        if not hasattr(self._local, "session"):
            self._local.session = requests.Session()
        return self._local.session

    def _search(self, params: dict[str, Any]) -> dict[str, Any]:
        """Execute the provider search request.

        Args:
            params: Request parameters to send to the provider.

        Returns:
            dict[str, Any]: Execute the provider search request.
        """
        if not self.is_configured():
            raise RuntimeError("SerpApi API key is missing")
        query = dict(params)
        query["engine"] = "google_flights"
        query["api_key"] = self._api_key
        query.setdefault("hl", "en")
        query.setdefault("gl", "us")
        query.setdefault("deep_search", "true")
        query.setdefault("no_cache", "false")
        response = self._session().get(
            self._search_url,
            params=query,
            timeout=45,
        )
        try:
            payload = response.json()
        except ValueError:
            payload = {}
        if response.status_code >= 400:
            error_message = f"HTTP {response.status_code}"
            payload_error = payload.get("error") if isinstance(payload, dict) else None
            if payload_error:
                error_message = str(payload_error)
            _capture_provider_response(
                self.provider_id,
                "google_flights_search",
                {"params": query},
                payload,
                status_code=response.status_code,
                error=error_message,
            )
            response.raise_for_status()
            raise RuntimeError(error_message)
        if not isinstance(payload, dict):
            payload = {}
        error = payload.get("error")
        if error:
            _capture_provider_response(
                self.provider_id,
                "google_flights_search",
                {"params": query},
                payload,
                status_code=response.status_code,
                error=str(error),
            )
            raise RuntimeError(str(error))
        _capture_provider_response(
            self.provider_id,
            "google_flights_search",
            {"params": query},
            payload,
            status_code=response.status_code,
        )
        return payload

    @staticmethod
    def _iter_options(payload: dict[str, Any]) -> list[dict[str, Any]]:
        """Iterate through normalized provider options.

        Args:
            payload: JSON-serializable payload for the operation.

        Returns:
            list[dict[str, Any]]: Iterate through normalized provider options.
        """
        out: list[dict[str, Any]] = []
        for key in ("best_flights", "other_flights"):
            for item in payload.get(key) or []:
                if isinstance(item, dict):
                    out.append(item)
        return out

    @staticmethod
    def _option_segments(option: dict[str, Any]) -> list[dict[str, Any]]:
        """Extract normalized segments for a provider option.

        Args:
            option: Mapping of option.

        Returns:
            list[dict[str, Any]]: Extract normalized segments for a provider option.
        """
        segments: list[dict[str, Any]] = []
        for flight in option.get("flights") or []:
            if not isinstance(flight, dict):
                continue
            dep = flight.get("departure_airport") or {}
            arr = flight.get("arrival_airport") or {}
            carrier = flight.get("airline")
            carrier_code = flight.get("airline_logo") or carrier
            segments.append(
                {
                    "from": dep.get("id"),
                    "to": arr.get("id"),
                    "from_name": dep.get("name"),
                    "to_name": arr.get("name"),
                    "depart_local": parse_datetime_guess(dep.get("time")),
                    "arrive_local": parse_datetime_guess(arr.get("time")),
                    "carrier": carrier_code,
                    "carrier_name": carrier,
                }
            )
        return segments

    @staticmethod
    def _option_duration_seconds(option: dict[str, Any]) -> int | None:
        """Calculate the duration for a provider option.

        Args:
            option: Mapping of option.

        Returns:
            int | None: Calculated duration for a provider option.
        """
        total_duration = option.get("total_duration")
        if isinstance(total_duration, int | float):
            return int(total_duration) * 60
        parsed = parse_duration_text_seconds(total_duration)
        if parsed is not None:
            return parsed
        return parse_duration_text_seconds(option.get("duration"))

    @staticmethod
    def _option_price(option: dict[str, Any]) -> int | None:
        """Extract the price for a provider option.

        Args:
            option: Mapping of option.

        Returns:
            int | None: Extract the price for a provider option.
        """
        for key in ("price", "total_price", "price_rounded"):
            amount = parse_money_amount_int(option.get(key))
            if amount is not None:
                return amount
        return None

    @staticmethod
    def _stops_param(max_stops_per_leg: int) -> int:
        # SerpApi Google Flights semantics:
        # 0 any stops, 1 non-stop, 2 <=1 stop, 3 <=2 stops.
        """Build the provider stops parameter.

        Args:
            max_stops_per_leg: Max stops per leg.

        Returns:
            int: The provider stops parameter.
        """
        if max_stops_per_leg <= 0:
            return 1
        if max_stops_per_leg == 1:
            return 2
        return 3

    @staticmethod
    def _booking_url(payload: dict[str, Any]) -> str | None:
        """Build the booking URL for the selected option.

        Args:
            payload: JSON-serializable payload for the operation.

        Returns:
            str | None: The booking URL for the selected option.
        """
        metadata = payload.get("search_metadata") or {}
        for key in ("google_flights_url", "raw_html_file"):
            value = str(metadata.get(key) or "").strip()
            if value:
                return value
        return None

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
        # SerpApi does not provide a cheap dense calendar endpoint like Kiwi's.
        # Keep it empty and let the engine rely on other providers for seeding.
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

        payload = self._search(
            {
                "departure_id": source_code,
                "arrival_id": destination_code,
                "outbound_date": departure_iso,
                "type": 2,
                "adults": max(1, adults),
                "currency": currency,
                "stops": self._stops_param(max_stops_per_leg),
            }
        )

        booking_url = self._booking_url(payload)
        best: dict[str, Any] | None = None
        for option in self._iter_options(payload):
            price = self._option_price(option)
            if price is None:
                continue
            segments = self._option_segments(option)
            if not segments:
                continue
            stops = max(0, len(segments) - 1)
            if stops > max_stops_per_leg:
                continue
            if max_connection_layover_seconds is not None:
                max_layover = max_segment_layover_seconds(segments)
                if max_layover is not None and max_layover > max_connection_layover_seconds:
                    continue
            duration_seconds = self._option_duration_seconds(option)
            candidate = {
                "price": price,
                "formatted_price": f"{price} {currency}",
                "currency": currency,
                "duration_seconds": duration_seconds,
                "stops": stops,
                "transfer_events": transfer_events_from_segments(segments),
                "booking_url": booking_url,
                "segments": segments,
                "provider": self.provider_id,
            }
            if best is None:
                best = candidate
                continue
            if int(candidate["price"]) < int(best["price"]):
                best = candidate
                continue
            if int(candidate["price"]) == int(best["price"]) and int(candidate["stops"]) < int(
                best["stops"]
            ):
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
        source_code = source.upper()
        destination_code = destination.upper()

        base_payload = self._search(
            {
                "departure_id": source_code,
                "arrival_id": destination_code,
                "outbound_date": outbound_iso,
                "return_date": inbound_iso,
                "type": 1,
                "adults": max(1, adults),
                "currency": currency,
                "stops": self._stops_param(max_stops_per_leg),
            }
        )

        departure_options = self._iter_options(base_payload)
        departure_options.sort(
            key=lambda option: (
                self._option_price(option)
                if self._option_price(option) is not None
                else PRICE_SENTINEL
            )
        )

        best: dict[str, Any] | None = None
        booking_url = self._booking_url(base_payload)
        for departure_option in departure_options[: self._return_option_scan_limit]:
            outbound_segments = self._option_segments(departure_option)
            if not outbound_segments:
                continue
            outbound_stops = max(0, len(outbound_segments) - 1)
            if outbound_stops > max_stops_per_leg:
                continue
            if max_connection_layover_seconds is not None:
                out_max_layover = max_segment_layover_seconds(outbound_segments)
                if out_max_layover is not None and out_max_layover > max_connection_layover_seconds:
                    continue

            departure_token = str(departure_option.get("departure_token") or "").strip()
            if departure_token:
                return_payload = self._search(
                    {
                        "departure_token": departure_token,
                        "adults": max(1, adults),
                        "currency": currency,
                    }
                )
                return_options = self._iter_options(return_payload)
                if not booking_url:
                    booking_url = self._booking_url(return_payload)
            else:
                return_options = []

            if not return_options:
                continue

            for return_option in return_options:
                inbound_segments = self._option_segments(return_option)
                if not inbound_segments:
                    continue
                inbound_stops = max(0, len(inbound_segments) - 1)
                if inbound_stops > max_stops_per_leg:
                    continue
                if max_connection_layover_seconds is not None:
                    in_max_layover = max_segment_layover_seconds(inbound_segments)
                    if (
                        in_max_layover is not None
                        and in_max_layover > max_connection_layover_seconds
                    ):
                        continue

                total_price = self._option_price(return_option)
                if total_price is None:
                    total_price = self._option_price(departure_option)
                if total_price is None:
                    continue

                outbound_duration_seconds = self._option_duration_seconds(departure_option)
                inbound_duration_seconds = self._option_duration_seconds(return_option)
                total_duration_seconds = (
                    (outbound_duration_seconds or 0) + (inbound_duration_seconds or 0)
                    if outbound_duration_seconds is not None
                    and inbound_duration_seconds is not None
                    else None
                )
                candidate = {
                    "price": total_price,
                    "formatted_price": f"{total_price} {currency}",
                    "currency": currency,
                    "duration_seconds": total_duration_seconds,
                    "outbound_duration_seconds": outbound_duration_seconds,
                    "inbound_duration_seconds": inbound_duration_seconds,
                    "outbound_stops": outbound_stops,
                    "inbound_stops": inbound_stops,
                    "outbound_transfer_events": transfer_events_from_segments(outbound_segments),
                    "inbound_transfer_events": transfer_events_from_segments(inbound_segments),
                    "booking_url": booking_url,
                    "outbound_segments": outbound_segments,
                    "inbound_segments": inbound_segments,
                    "provider": self.provider_id,
                }
                if best is None:
                    best = candidate
                    continue
                if int(candidate["price"]) < int(best["price"]):
                    best = candidate
                    continue
                candidate_stops = int(candidate["outbound_stops"]) + int(candidate["inbound_stops"])
                best_stops = int(best["outbound_stops"]) + int(best["inbound_stops"])
                if int(candidate["price"]) == int(best["price"]) and candidate_stops < best_stops:
                    best = candidate
        return best
