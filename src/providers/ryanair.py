from __future__ import annotations

import datetime as dt
import math
import threading
from typing import Any
from urllib.parse import urlencode

import requests

from ..config import RYANAIR_API_LANGUAGE, RYANAIR_BASE_URL, RYANAIR_SITE_PATH
from ..exceptions import ProviderNoResultError
from ..utils import convert_currency_amount, date_only, parse_money_amount_int
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
        return "Official Ryanair fare-finder API for direct base fares; skipped when paid bags are requested."

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
    def _price_amount(item: dict[str, Any]) -> float | None:
        """Return the source-currency per-adult base price for a Ryanair fare row."""
        if bool(item.get("soldOut")) or bool(item.get("unavailable")):
            return None
        price = item.get("price") or {}
        raw_value = price.get("value") if isinstance(price, dict) else None
        try:
            amount = float(raw_value)
        except (TypeError, ValueError):
            parsed = parse_money_amount_int(raw_value)
            return float(parsed) if parsed is not None else None
        if not math.isfinite(amount) or amount <= 0:
            return None
        return amount

    @staticmethod
    def _price_currency(item: dict[str, Any], fallback_currency: str) -> str:
        """Return the provider source currency for a Ryanair fare row."""
        price = item.get("price") or {}
        source_currency = (
            str((price.get("currencyCode") if isinstance(price, dict) else "") or "")
            .strip()
            .upper()
        )
        return source_currency or (str(fallback_currency or "EUR").strip().upper() or "EUR")

    @classmethod
    def _normalized_price(
        cls,
        item: dict[str, Any],
        *,
        target_currency: str,
        adults: int,
    ) -> tuple[int, float, str] | None:
        """Return total target price, source total, and source currency for a fare row."""
        per_adult_source_price = cls._price_amount(item)
        if per_adult_source_price is None:
            return None
        source_currency = cls._price_currency(item, target_currency)
        source_total = per_adult_source_price * max(1, int(adults or 1))
        normalized_target = (
            str(target_currency or source_currency).strip().upper() or source_currency
        )
        converted = convert_currency_amount(source_total, source_currency, normalized_target)
        if converted is None:
            return None
        return converted, source_total, source_currency

    @staticmethod
    def _format_price(
        price: int,
        *,
        currency: str,
        source_total: float | None = None,
        source_currency: str | None = None,
    ) -> str:
        """Return a display label that keeps converted Ryanair prices auditable."""
        target_currency = str(currency or "").strip().upper()
        formatted = f"{price} {target_currency}"
        if (
            source_total is not None
            and source_currency
            and str(source_currency).strip().upper() != target_currency
        ):
            formatted += f" (source {source_total:.2f} {str(source_currency).strip().upper()})"
        return formatted

    @staticmethod
    def _direct_segments(
        item: dict[str, Any],
        *,
        source: str,
        destination: str,
    ) -> list[dict[str, Any]]:
        """Return a one-segment Ryanair leg when the fare row exposes local times."""
        departure = str(item.get("departureDate") or "").strip()
        arrival = str(item.get("arrivalDate") or "").strip()
        if not departure or not arrival:
            return []
        return [
            {
                "from": str(source or "").strip().upper(),
                "to": str(destination or "").strip().upper(),
                "depart_local": departure,
                "arrive_local": arrival,
                "carrier": "FR",
                "carrier_name": "Ryanair",
            }
        ]

    @staticmethod
    def _reject_baggage_profile(hand_bags: int, hold_bags: int) -> None:
        """Reject Ryanair fare-finder fares when paid bag add-ons are requested."""
        if int(hand_bags or 0) > 0 or int(hold_bags or 0) > 0:
            raise ProviderNoResultError(
                "Ryanair fare-finder exposes base fares only; skipping because cabin/hold bag "
                "add-ons were requested."
            )

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

    @staticmethod
    def _looks_like_missing_market_error(exc: Exception) -> bool:
        """Return whether an exception looks like an unsupported market/airport."""
        message = str(exc or "").upper()
        return "HTTP 400" in message or "HTTP 404" in message

    @per_instance_lru_cache(maxsize=512)
    def _route_destinations(self, source: str) -> tuple[str, ...]:
        """Return the active Ryanair destinations served from the source airport."""
        normalized_source = str(source or "").strip().upper()
        if not normalized_source:
            return ()
        try:
            payload = self._request_json(
                f"/api/views/locate/searchWidget/routes/{self._api_language}/airport/{normalized_source}"
            )
        except RuntimeError as exc:
            if self._looks_like_missing_market_error(exc):
                return ()
            raise
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
        try:
            payload = self._request_json(
                f"/api/farfnd/v4/oneWayFares/{normalized_source}/{normalized_destination}/cheapestPerDay",
                params={
                    "outboundMonthOfDate": month_start_iso,
                    "currency": str(currency or "EUR").strip().upper() or "EUR",
                },
            )
        except RuntimeError as exc:
            if self._looks_like_missing_market_error(exc):
                return ()
            raise
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
        try:
            payload = self._request_json(
                f"/api/farfnd/v4/roundTripFares/{normalized_source}/{normalized_destination}/cheapestPerDay",
                params={
                    "outboundMonthOfDate": outbound_month_start_iso,
                    "inboundMonthOfDate": inbound_month_start_iso,
                    "currency": str(currency or "EUR").strip().upper() or "EUR",
                },
            )
        except RuntimeError as exc:
            if self._looks_like_missing_market_error(exc):
                return (), ()
            raise
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
        del max_stops_per_leg
        if int(hand_bags or 0) > 0 or int(hold_bags or 0) > 0:
            return {}
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
                normalized_price = self._normalized_price(
                    item,
                    target_currency=currency,
                    adults=adults,
                )
                if normalized_price is None:
                    continue
                price = normalized_price[0]
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
        del max_stops_per_leg, max_connection_layover_seconds
        self._reject_baggage_profile(hand_bags, hold_bags)
        if not self._market_supported(source, destination):
            raise ProviderNoResultError(f"Ryanair does not serve {source}->{destination}.")
        for item in self._oneway_month(
            source, destination, self._month_key(departure_iso), currency
        ):
            if date_only(item.get("day")) != date_only(departure_iso):
                continue
            normalized_price = self._normalized_price(
                item,
                target_currency=currency,
                adults=adults,
            )
            if normalized_price is None:
                continue
            price, source_total, source_currency = normalized_price
            duration_seconds = self._parse_duration_seconds(
                str(item.get("departureDate") or "").strip() or None,
                str(item.get("arrivalDate") or "").strip() or None,
            )
            target_currency = str(currency or source_currency).strip().upper() or source_currency
            return {
                "price": price,
                "formatted_price": self._format_price(
                    price,
                    currency=target_currency,
                    source_total=source_total,
                    source_currency=source_currency,
                ),
                "currency": target_currency,
                "source_price": round(source_total, 2),
                "source_currency": source_currency,
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
                "segments": self._direct_segments(item, source=source, destination=destination),
                "provider": self.provider_id,
                "fare_mode": "base_no_bags",
                "price_mode": "base_per_adult_scaled_converted",
                "baggage_included": False,
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
        del max_stops_per_leg, max_connection_layover_seconds
        self._reject_baggage_profile(hand_bags, hold_bags)
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
        outbound_normalized = self._normalized_price(
            outbound_item,
            target_currency=currency,
            adults=adults,
        )
        inbound_normalized = self._normalized_price(
            inbound_item,
            target_currency=currency,
            adults=adults,
        )
        if outbound_normalized is None or inbound_normalized is None:
            raise ProviderNoResultError(
                "Ryanair returned no exact round-trip fare "
                f"for {source}->{destination} on {outbound_iso}/{inbound_iso}."
            )
        outbound_price, outbound_source_total, outbound_source_currency = outbound_normalized
        inbound_price, inbound_source_total, inbound_source_currency = inbound_normalized
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
        target_currency = str(currency or outbound_source_currency).strip().upper() or "EUR"
        source_currency = (
            outbound_source_currency
            if outbound_source_currency == inbound_source_currency
            else f"{outbound_source_currency}/{inbound_source_currency}"
        )
        source_total = outbound_source_total + inbound_source_total
        return {
            "price": total_price,
            "formatted_price": self._format_price(
                total_price,
                currency=target_currency,
                source_total=source_total,
                source_currency=source_currency,
            ),
            "currency": target_currency,
            "source_price": round(source_total, 2),
            "source_currency": source_currency,
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
            "outbound_segments": self._direct_segments(
                outbound_item,
                source=source,
                destination=destination,
            ),
            "inbound_segments": self._direct_segments(
                inbound_item,
                source=destination,
                destination=source,
            ),
            "provider": self.provider_id,
            "fare_mode": "base_no_bags",
            "price_mode": "base_per_adult_scaled_converted",
            "baggage_included": False,
        }
