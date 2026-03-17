from __future__ import annotations

import threading
from typing import Any

import requests

from ..config import (
    CALENDAR_QUERY,
    GRAPHQL_ENDPOINT,
    KIWI_ITINERARY_SCAN_LIMIT,
    ONEWAY_QUERY,
    RETURN_QUERY,
)
from ..models import PassengerConfig
from ..utils import (
    itinerary_booking_url,
    max_segment_layover_seconds,
    transfer_events_from_segments,
)
from ._cache import per_instance_lru_cache


class KiwiClient:
    """Provider client for Kiwi GraphQL flight lookups."""

    provider_id = "kiwi"
    display_name = "Kiwi"
    supports_calendar = True
    requires_credentials = False
    credential_env: tuple[str, ...] = ()
    docs_url = "https://www.kiwi.com/"

    def __init__(self) -> None:
        """Initialize the KiwiClient."""
        self._local = threading.local()

    def is_configured(self) -> bool:
        """Return whether the client is configured for use.

        Returns:
            bool: True when the client is configured for use; otherwise, False.
        """
        return True

    def _session(self) -> requests.Session:
        """Return the cached requests session.

        Returns:
            requests.Session: The cached requests session.
        """
        if not hasattr(self._local, "session"):
            self._local.session = requests.Session()
        return self._local.session

    @staticmethod
    def _passengers_payload(passengers: PassengerConfig) -> dict[str, Any]:
        """Build the passenger payload for the provider request.

        Args:
            passengers: Passenger configuration to serialize for the provider request.

        Returns:
            dict[str, Any]: The passenger payload for the provider request.
        """
        adults = max(1, passengers.adults)
        return {
            "adults": adults,
            "adultsHandBags": [max(0, passengers.hand_bags)] * adults,
            "adultsHoldBags": [max(0, passengers.hold_bags)] * adults,
        }

    def _post(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        """Send a POST request to the provider.

        Args:
            query: Provider query string to submit.
            variables: Variables payload for the provider request.

        Returns:
            dict[str, Any]: Result of sending POST request to the provider.
        """
        response = self._session().post(
            GRAPHQL_ENDPOINT,
            json={"query": query, "variables": variables},
            timeout=45,
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get("errors"):
            raise RuntimeError(payload["errors"][0].get("message", "GraphQL error"))
        return payload

    @staticmethod
    def _parse_sector_segments(sector: dict[str, Any] | None) -> list[dict[str, Any]]:
        """Parse sector data into normalized flight segments.

        Args:
            sector: Mapping of sector.

        Returns:
            list[dict[str, Any]]: Parsed sector data into normalized flight segments.
        """
        segments_raw = (sector or {}).get("sectorSegments", [])
        segments: list[dict[str, Any]] = []
        for item in segments_raw:
            seg = item.get("segment") or {}
            src_station = seg.get("source", {}).get("station", {})
            dst_station = seg.get("destination", {}).get("station", {})
            carrier = seg.get("carrier") or {}
            segments.append(
                {
                    "from": src_station.get("code"),
                    "to": dst_station.get("code"),
                    "from_name": src_station.get("name"),
                    "to_name": dst_station.get("name"),
                    "depart_local": seg.get("source", {}).get("localTime"),
                    "arrive_local": seg.get("destination", {}).get("localTime"),
                    "carrier": carrier.get("code"),
                    "carrier_name": carrier.get("name"),
                }
            )
        return segments

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
        passengers = PassengerConfig(adults=adults, hand_bags=hand_bags, hold_bags=hold_bags)
        variables = {
            "search": {
                "source": {"ids": [source]},
                "destination": {"ids": [destination]},
                "dates": {
                    "start": f"{date_start_iso}T00:00:00",
                    "end": f"{date_end_iso}T23:59:59",
                },
                "passengers": self._passengers_payload(passengers),
            },
            "filter": {"maxStopsCount": max_stops_per_leg},
            "options": {
                "partner": "skypicker",
                "currency": currency,
                "locale": "en",
            },
        }

        payload = self._post(CALENDAR_QUERY, variables)
        result = payload.get("data", {}).get("itineraryPricesCalendar")
        if not isinstance(result, dict) or "calendar" not in result:
            return {}

        prices: dict[str, int] = {}
        for item in result["calendar"]:
            date_str = str(item.get("date", ""))[:10]
            rated = item.get("ratedPrice") or {}
            price = rated.get("price") or {}
            amount = price.get("amount")
            if not date_str or amount in (None, ""):
                continue
            try:
                prices[date_str] = int(float(amount))
            except (TypeError, ValueError):
                continue
        return prices

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
        passengers = PassengerConfig(adults=adults, hand_bags=hand_bags, hold_bags=hold_bags)
        variables = {
            "search": {
                "itinerary": {
                    "source": {"ids": [source]},
                    "destination": {"ids": [destination]},
                    "outboundDepartureDate": {
                        "start": f"{departure_iso}T00:00:00",
                        "end": f"{departure_iso}T23:59:59",
                    },
                },
                "passengers": self._passengers_payload(passengers),
            },
            "filter": {
                "limit": KIWI_ITINERARY_SCAN_LIMIT,
                "maxStopsCount": max_stops_per_leg,
            },
            "options": {
                "partner": "skypicker",
                "currency": currency,
                "locale": "en",
                "sortBy": "PRICE",
                "sortOrder": "ASCENDING",
            },
        }

        payload = self._post(ONEWAY_QUERY, variables)
        result = payload.get("data", {}).get("onewayItineraries")
        if not isinstance(result, dict):
            return None
        itineraries = result.get("itineraries")
        if not itineraries:
            return None

        best: dict[str, Any] | None = None
        for itinerary in itineraries:
            price_obj = itinerary.get("price") or {}
            amount = price_obj.get("amount")
            if amount in (None, ""):
                continue
            try:
                amount_value = int(float(amount))
            except (TypeError, ValueError):
                continue

            segments = self._parse_sector_segments(itinerary.get("sector"))
            stops = max(0, len(segments) - 1)
            if stops > max_stops_per_leg:
                continue
            if max_connection_layover_seconds is not None:
                leg_max_layover = max_segment_layover_seconds(segments)
                if leg_max_layover is not None and leg_max_layover > max_connection_layover_seconds:
                    continue

            candidate = {
                "price": amount_value,
                "formatted_price": price_obj.get("formattedValue"),
                "currency": (price_obj.get("currency") or {}).get("code"),
                "duration_seconds": itinerary.get("duration"),
                "stops": stops,
                "transfer_events": transfer_events_from_segments(segments),
                "booking_url": itinerary_booking_url(itinerary),
                "segments": segments,
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
        passengers = PassengerConfig(adults=adults, hand_bags=hand_bags, hold_bags=hold_bags)
        variables = {
            "search": {
                "itinerary": {
                    "source": {"ids": [source]},
                    "destination": {"ids": [destination]},
                    "outboundDepartureDate": {
                        "start": f"{outbound_iso}T00:00:00",
                        "end": f"{outbound_iso}T23:59:59",
                    },
                    "inboundDepartureDate": {
                        "start": f"{inbound_iso}T00:00:00",
                        "end": f"{inbound_iso}T23:59:59",
                    },
                },
                "passengers": self._passengers_payload(passengers),
            },
            "filter": {
                "limit": KIWI_ITINERARY_SCAN_LIMIT,
                "maxStopsCount": max_stops_per_leg,
            },
            "options": {
                "partner": "skypicker",
                "currency": currency,
                "locale": "en",
                "sortBy": "PRICE",
                "sortOrder": "ASCENDING",
            },
        }

        payload = self._post(RETURN_QUERY, variables)
        result = payload.get("data", {}).get("returnItineraries")
        if not isinstance(result, dict):
            return None
        itineraries = result.get("itineraries")
        if not itineraries:
            return None

        best: dict[str, Any] | None = None
        for itinerary in itineraries:
            price_obj = itinerary.get("price") or {}
            amount = price_obj.get("amount")
            if amount in (None, ""):
                continue

            try:
                amount_value = int(float(amount))
            except (TypeError, ValueError):
                continue

            outbound_segments = self._parse_sector_segments(itinerary.get("outbound"))
            inbound_segments = self._parse_sector_segments(itinerary.get("inbound"))
            outbound_stops = max(0, len(outbound_segments) - 1)
            inbound_stops = max(0, len(inbound_segments) - 1)
            if outbound_stops > max_stops_per_leg or inbound_stops > max_stops_per_leg:
                continue
            if max_connection_layover_seconds is not None:
                out_max_layover = max_segment_layover_seconds(outbound_segments)
                in_max_layover = max_segment_layover_seconds(inbound_segments)
                if (
                    out_max_layover is not None and out_max_layover > max_connection_layover_seconds
                ) or (
                    in_max_layover is not None and in_max_layover > max_connection_layover_seconds
                ):
                    continue

            outbound_duration_seconds = (itinerary.get("outbound") or {}).get("duration")
            inbound_duration_seconds = (itinerary.get("inbound") or {}).get("duration")
            candidate = {
                "price": amount_value,
                "formatted_price": price_obj.get("formattedValue"),
                "currency": (price_obj.get("currency") or {}).get("code"),
                "duration_seconds": itinerary.get("duration"),
                "outbound_duration_seconds": outbound_duration_seconds,
                "inbound_duration_seconds": inbound_duration_seconds,
                "outbound_stops": outbound_stops,
                "inbound_stops": inbound_stops,
                "outbound_transfer_events": transfer_events_from_segments(outbound_segments),
                "inbound_transfer_events": transfer_events_from_segments(inbound_segments),
                "booking_url": itinerary_booking_url(itinerary),
                "outbound_segments": outbound_segments,
                "inbound_segments": inbound_segments,
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
