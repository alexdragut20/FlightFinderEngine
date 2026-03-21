from __future__ import annotations

import datetime as dt
import threading
from typing import Any
from urllib.parse import urlencode

import requests

from ..config import RYANAIR_API_LANGUAGE, RYANAIR_BASE_URL, RYANAIR_SITE_PATH
from ..exceptions import ProviderNoResultError
from ..utils import date_only, parse_money_amount_int
from ..utils.logging import capture_provider_response as _capture_provider_response
from ._cache import per_instance_lru_cache


class RyanairFareFinderClient:
    """Provider client for Ryanair fare-finder and route endpoints."""

    provider_id = "ryanair"
    display_name = "Ryanair Fare Finder"
    supports_calendar = True
    docs_url = "https://www.ryanair.com/gb/en/fare-finder"
    default_enabled = True

    def __init__(
        self,
        *,
        base_url: str | None = None,
        api_language: str | None = None,
        site_path: str | None = None,
    ) -> None:
        """Initialize the Ryanair client."""
        self._base_url = str(base_url or RYANAIR_BASE_URL).rstrip("/")
        self._api_language = (
            str(api_language or RYANAIR_API_LANGUAGE).strip().lower() or RYANAIR_API_LANGUAGE
        )
        self._site_path = (
            str(site_path or RYANAIR_SITE_PATH).strip().strip("/") or RYANAIR_SITE_PATH
        )
        self._local = threading.local()

    def is_configured(self) -> bool:
        """Return whether the token-free Ryanair provider is ready."""
        return True

    @staticmethod
    def configuration_hint() -> str:
        """Return a short provider note for the UI."""
        return "Official Ryanair fare-finder API for direct one-way and round-trip fares."

    def _session(self) -> requests.Session:
        """Return a cached requests session."""
        if not hasattr(self._local, "session"):
            session = requests.Session()
            session.headers.update(
                {
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0.0.0 Safari/537.36"
                    ),
                    "Accept": "application/json",
                    "Accept-Language": "en-US,en;q=0.9",
                }
            )
            self._local.session = session
        return self._local.session

    @staticmethod
    def _month_key(date_iso: str) -> str:
        """Return the first day of the date's month in ISO format."""
        parsed = dt.date.fromisoformat(date_only(date_iso))
        return dt.date(parsed.year, parsed.month, 1).isoformat()

    @staticmethod
    def _month_starts_between(date_start_iso: str, date_end_iso: str) -> tuple[str, ...]:
        """Return inclusive month keys between two ISO dates."""
        start = dt.date.fromisoformat(date_only(date_start_iso))
        end = dt.date.fromisoformat(date_only(date_end_iso))
        if end < start:
            return ()
        months: list[str] = []
        cursor = dt.date(start.year, start.month, 1)
        end_cursor = dt.date(end.year, end.month, 1)
        while cursor <= end_cursor:
            months.append(cursor.isoformat())
            if cursor.month == 12:
                cursor = dt.date(cursor.year + 1, 1, 1)
            else:
                cursor = dt.date(cursor.year, cursor.month + 1, 1)
        return tuple(months)

    @staticmethod
    def _parse_duration_seconds(
        departure_iso: str | None,
        arrival_iso: str | None,
    ) -> int | None:
        """Return a naive positive duration from local departure and arrival timestamps."""
        departure_raw = str(departure_iso or "").strip()
        arrival_raw = str(arrival_iso or "").strip()
        if not departure_raw or not arrival_raw:
            return None
        try:
            departure_dt = dt.datetime.fromisoformat(departure_raw)
            arrival_dt = dt.datetime.fromisoformat(arrival_raw)
        except ValueError:
            return None
        if arrival_dt < departure_dt:
            arrival_dt += dt.timedelta(days=1)
        return int((arrival_dt - departure_dt).total_seconds())

    @staticmethod
    def _price_int(item: dict[str, Any]) -> int | None:
        """Return the normalized price for a Ryanair fare row."""
        return parse_money_amount_int((item.get("price") or {}).get("value"))

    def _build_booking_url(
        self,
        *,
        source: str,
        destination: str,
        outbound_iso: str,
        inbound_iso: str | None = None,
        adults: int,
    ) -> str:
        """Return a Ryanair search URL that opens the official booking flow."""
        params = {
            "adults": max(1, int(adults)),
            "teens": 0,
            "children": 0,
            "infants": 0,
            "originIata": str(source or "").strip().upper(),
            "destinationIata": str(destination or "").strip().upper(),
            "dateOut": date_only(outbound_iso),
            "isReturn": "true" if inbound_iso else "false",
            "discount": 0,
            "promoCode": "",
        }
        if inbound_iso:
            params["dateIn"] = date_only(inbound_iso)
        return f"{self._base_url}/{self._site_path}/trip/flights/select?{urlencode(params)}"

    def _request_json(self, path: str, *, params: dict[str, Any] | None = None) -> Any:
        """Execute a JSON GET request against a Ryanair endpoint."""
        response = self._session().get(
            f"{self._base_url}{path}",
            params=params,
            timeout=35,
        )
        error: str | None = None
        if response.status_code >= 400:
            error = f"HTTP {response.status_code}"
        try:
            payload = response.json()
        except ValueError:
            payload = {}
            if error is None:
                error = "Invalid JSON"
        _capture_provider_response(
            self.provider_id,
            "json_request",
            {"path": path, "params": params or {}},
            payload,
            status_code=response.status_code,
            error=error,
        )
        if error:
            raise RuntimeError(f"Ryanair request failed: {error}")
        return payload

    @per_instance_lru_cache(maxsize=512)
    def _route_destinations(self, source: str) -> tuple[str, ...]:
        """Return the active Ryanair destinations served from the source airport."""
        normalized_source = str(source or "").strip().upper()
        if not normalized_source:
            return ()
        payload = self._request_json(
            f"/api/views/locate/searchWidget/routes/{self._api_language}/airport/{normalized_source}"
        )
        if not isinstance(payload, list):
            return ()
        destinations: list[str] = []
        for item in payload:
            if not isinstance(item, dict):
                continue
            arrival_airport = item.get("arrivalAirport")
            if not isinstance(arrival_airport, dict):
                continue
            destination = str(arrival_airport.get("code") or "").strip().upper()
            if destination:
                destinations.append(destination)
        return tuple(dict.fromkeys(destinations))

    def _market_supported(self, source: str, destination: str) -> bool:
        """Return whether the market appears in Ryanair's search-widget route graph."""
        normalized_source = str(source or "").strip().upper()
        normalized_destination = str(destination or "").strip().upper()
        if not normalized_source or not normalized_destination:
            return False
        return normalized_destination in set(self._route_destinations(normalized_source))

    @per_instance_lru_cache(maxsize=4096)
    def _oneway_month(
        self,
        source: str,
        destination: str,
        month_start_iso: str,
        currency: str,
    ) -> tuple[dict[str, Any], ...]:
        """Fetch a month of one-way fares for a market."""
        normalized_source = str(source or "").strip().upper()
        normalized_destination = str(destination or "").strip().upper()
        if not self._market_supported(normalized_source, normalized_destination):
            return ()
        payload = self._request_json(
            f"/api/farfnd/v4/oneWayFares/{normalized_source}/{normalized_destination}/cheapestPerDay",
            params={
                "outboundMonthOfDate": month_start_iso,
                "currency": str(currency or "EUR").strip().upper() or "EUR",
            },
        )
        fares = (
            ((payload.get("outbound") or {}).get("fares") or [])
            if isinstance(payload, dict)
            else []
        )
        return tuple(item for item in fares if isinstance(item, dict))

    @per_instance_lru_cache(maxsize=4096)
    def _return_month(
        self,
        source: str,
        destination: str,
        outbound_month_start_iso: str,
        inbound_month_start_iso: str,
        currency: str,
    ) -> tuple[tuple[dict[str, Any], ...], tuple[dict[str, Any], ...]]:
        """Fetch outbound and inbound daily fares for a round-trip market."""
        normalized_source = str(source or "").strip().upper()
        normalized_destination = str(destination or "").strip().upper()
        if not self._market_supported(normalized_source, normalized_destination):
            return (), ()
        if not self._market_supported(normalized_destination, normalized_source):
            return (), ()
        payload = self._request_json(
            f"/api/farfnd/v4/roundTripFares/{normalized_source}/{normalized_destination}/cheapestPerDay",
            params={
                "outboundMonthOfDate": outbound_month_start_iso,
                "inboundMonthOfDate": inbound_month_start_iso,
                "currency": str(currency or "EUR").strip().upper() or "EUR",
            },
        )
        if not isinstance(payload, dict):
            return (), ()
        outbound = tuple(
            item
            for item in ((payload.get("outbound") or {}).get("fares") or [])
            if isinstance(item, dict)
        )
        inbound = tuple(
            item
            for item in ((payload.get("inbound") or {}).get("fares") or [])
            if isinstance(item, dict)
        )
        return outbound, inbound

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
        """Return the cheapest Ryanair direct fare per departure day in the range."""
        del max_stops_per_leg, adults, hand_bags, hold_bags
        normalized_start = date_only(date_start_iso)
        normalized_end = date_only(date_end_iso)
        out: dict[str, int] = {}
        for month_start in self._month_starts_between(normalized_start, normalized_end):
            for item in self._oneway_month(source, destination, month_start, currency):
                departure_day = date_only(item.get("day"))
                if (
                    not departure_day
                    or departure_day < normalized_start
                    or departure_day > normalized_end
                ):
                    continue
                price = self._price_int(item)
                if price is None:
                    continue
                previous = out.get(departure_day)
                if previous is None or price < previous:
                    out[departure_day] = price
        return out

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
        """Return the best exact Ryanair one-way fare for the requested day."""
        del max_stops_per_leg, hand_bags, hold_bags, max_connection_layover_seconds
        if not self._market_supported(source, destination):
            raise ProviderNoResultError(f"Ryanair does not serve {source}->{destination}.")
        for item in self._oneway_month(
            source, destination, self._month_key(departure_iso), currency
        ):
            if date_only(item.get("day")) != date_only(departure_iso):
                continue
            price = self._price_int(item)
            if price is None:
                continue
            duration_seconds = self._parse_duration_seconds(
                str(item.get("departureDate") or "").strip() or None,
                str(item.get("arrivalDate") or "").strip() or None,
            )
            return {
                "price": price,
                "formatted_price": f"{price} {str(currency or 'EUR').strip().upper() or 'EUR'}",
                "currency": str(currency or "EUR").strip().upper() or "EUR",
                "duration_seconds": duration_seconds,
                "stops": 0,
                "transfer_events": 0,
                "booking_url": self._build_booking_url(
                    source=source,
                    destination=destination,
                    outbound_iso=departure_iso,
                    inbound_iso=None,
                    adults=adults,
                ),
                "segments": [],
                "provider": self.provider_id,
            }
        raise ProviderNoResultError(
            f"Ryanair returned no exact one-way fare for {source}->{destination} on {departure_iso}."
        )

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
        """Return the best exact Ryanair round-trip fare for the requested day pair."""
        del max_stops_per_leg, hand_bags, hold_bags, max_connection_layover_seconds
        if not self._market_supported(source, destination):
            raise ProviderNoResultError(f"Ryanair does not serve {source}->{destination}.")
        if not self._market_supported(destination, source):
            raise ProviderNoResultError(f"Ryanair does not serve {destination}->{source}.")
        outbound_rows, inbound_rows = self._return_month(
            source,
            destination,
            self._month_key(outbound_iso),
            self._month_key(inbound_iso),
            currency,
        )
        outbound_item = next(
            (
                item
                for item in outbound_rows
                if date_only(item.get("day")) == date_only(outbound_iso)
            ),
            None,
        )
        inbound_item = next(
            (item for item in inbound_rows if date_only(item.get("day")) == date_only(inbound_iso)),
            None,
        )
        if outbound_item is None or inbound_item is None:
            raise ProviderNoResultError(
                "Ryanair returned no exact round-trip fare "
                f"for {source}->{destination} on {outbound_iso}/{inbound_iso}."
            )
        outbound_price = self._price_int(outbound_item)
        inbound_price = self._price_int(inbound_item)
        if outbound_price is None or inbound_price is None:
            raise ProviderNoResultError(
                "Ryanair returned no exact round-trip fare "
                f"for {source}->{destination} on {outbound_iso}/{inbound_iso}."
            )
        outbound_duration = self._parse_duration_seconds(
            str(outbound_item.get("departureDate") or "").strip() or None,
            str(outbound_item.get("arrivalDate") or "").strip() or None,
        )
        inbound_duration = self._parse_duration_seconds(
            str(inbound_item.get("departureDate") or "").strip() or None,
            str(inbound_item.get("arrivalDate") or "").strip() or None,
        )
        total_price = outbound_price + inbound_price
        total_duration = None
        if outbound_duration is not None and inbound_duration is not None:
            total_duration = outbound_duration + inbound_duration
        return {
            "price": total_price,
            "formatted_price": f"{total_price} {str(currency or 'EUR').strip().upper() or 'EUR'}",
            "currency": str(currency or "EUR").strip().upper() or "EUR",
            "duration_seconds": total_duration,
            "outbound_duration_seconds": outbound_duration,
            "inbound_duration_seconds": inbound_duration,
            "outbound_stops": 0,
            "inbound_stops": 0,
            "transfer_events": 0,
            "booking_url": self._build_booking_url(
                source=source,
                destination=destination,
                outbound_iso=outbound_iso,
                inbound_iso=inbound_iso,
                adults=adults,
            ),
            "outbound_segments": [],
            "inbound_segments": [],
            "provider": self.provider_id,
        }
