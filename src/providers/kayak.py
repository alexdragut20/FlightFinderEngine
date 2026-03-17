from __future__ import annotations

import json
import re
import threading
import time
from typing import Any
from urllib.parse import urlencode

import requests

from ..config import (
    KAYAK_SCRAPE_HOST,
    KAYAK_SCRAPE_POLL_ROUNDS,
    KAYAK_SCRAPE_SCHEME,
    MOMONDO_SCRAPE_HOST,
)
from ..exceptions import ProviderNoResultError
from ..utils import (
    absolute_kayak_url,
    convert_currency_amount,
    max_segment_layover_seconds,
    parse_local_datetime,
    parse_money_amount_int,
    transfer_events_from_segments,
)
from ..utils.constants import PRICE_SENTINEL
from ..utils.logging import capture_provider_response as _capture_provider_response
from ._cache import per_instance_lru_cache


class KayakScrapeClient:
    """Provider client for Kayak scrape-based flight lookups."""

    provider_id = "kayak"
    display_name = "Kayak Scrape"
    supports_calendar = False
    requires_credentials = False
    credential_env: tuple[str, ...] = ()
    docs_url = "https://www.kayak.com/flights/"
    _NO_RESULT_CODES = {"NO_RESULTS", "NO_RESULTS_FOUND", "NO_RESULTS_AVAILABLE"}

    def __init__(
        self,
        host: str | None = None,
        poll_rounds: int | None = None,
    ) -> None:
        """Initialize the KayakScrapeClient.

        Args:
            host: Host name for the request.
            poll_rounds: Number of polling rounds to execute.
        """
        normalized_host = str(host or KAYAK_SCRAPE_HOST).strip().lower()
        if not normalized_host:
            normalized_host = KAYAK_SCRAPE_HOST
        self._host = normalized_host
        self._poll_rounds = max(
            1,
            min(
                6,
                int(poll_rounds) if poll_rounds is not None else KAYAK_SCRAPE_POLL_ROUNDS,
            ),
        )
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
            session = requests.Session()
            session.headers.update(
                {
                    "User-Agent": (
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/122.0.0.0 Safari/537.36"
                    ),
                    "Accept-Language": "en-US,en;q=0.9",
                }
            )
            self._local.session = session
        return self._local.session

    def _search_page_url(
        self,
        source: str,
        destination: str,
        outbound_iso: str,
        inbound_iso: str | None,
        currency: str,
        adults: int,
    ) -> str:
        """Build the provider search page URL.

        Args:
            source: Origin airport code for the request.
            destination: Destination airport code for the request.
            outbound_iso: Outbound travel date in ISO 8601 format.
            inbound_iso: Inbound travel date in ISO 8601 format.
            currency: Currency code for pricing output.
            adults: Number of adult travelers.

        Returns:
            str: The provider search page URL.
        """
        source_code = str(source or "").strip().upper()
        destination_code = str(destination or "").strip().upper()
        if inbound_iso:
            path = f"/flights/{source_code}-{destination_code}/{outbound_iso}/{inbound_iso}"
        else:
            path = f"/flights/{source_code}-{destination_code}/{outbound_iso}"
        params = {
            "sort": "price_a",
            "adults": max(1, int(adults or 1)),
            "currency": str(currency or "RON").strip().upper() or "RON",
        }
        return f"{KAYAK_SCRAPE_SCHEME}://{self._host}{path}?{urlencode(params)}"

    @staticmethod
    def _safe_json_from_response(response: requests.Response) -> dict[str, Any]:
        """Safely decode a JSON response payload.

        Args:
            response: HTTP response object to inspect.

        Returns:
            dict[str, Any]: Safely decode a JSON response payload.
        """
        try:
            payload = response.json()
        except ValueError:
            return {}
        return payload if isinstance(payload, dict) else {}

    @staticmethod
    def _extract_error_detail(payload: dict[str, Any]) -> str:
        """Extract a human-readable error detail from the provider response.

        Args:
            payload: JSON-serializable payload for the operation.

        Returns:
            str: Extract a human-readable error detail from the provider response.
        """
        errors = payload.get("errors")
        if not isinstance(errors, list) or not errors:
            return ""
        first = errors[0] or {}
        if isinstance(first, dict):
            code = str(first.get("code") or "").strip()
            description = str(first.get("description") or "").strip()
            if code and description:
                return f"{code}: {description}"
            if description:
                return description
            if code:
                return code
        return str(first).strip()

    def _extract_bootstrap(self, html: str) -> tuple[str, dict[str, Any]]:
        """Extract bootstrap data from the provider page.

        Args:
            html: HTML document to parse.

        Returns:
            tuple[str, dict[str, Any]]: Extract bootstrap data from the provider page.
        """
        match = re.search(
            r'<script[^>]*id="jsonData_R9DataStorage"[^>]*>(.*?)</script>',
            html,
            re.IGNORECASE | re.DOTALL,
        )
        if not match:
            raise RuntimeError("Kayak bootstrap data missing (jsonData_R9DataStorage not found)")
        try:
            bootstrap = json.loads(match.group(1))
        except ValueError as exc:
            raise RuntimeError(f"Kayak bootstrap JSON parse failed: {exc}") from exc

        formtoken = str(
            ((bootstrap.get("serverData") or {}).get("global") or {}).get("formtoken") or ""
        ).strip()
        if not formtoken:
            raise RuntimeError("Kayak formtoken is missing")
        return formtoken, bootstrap

    def _post_poll(
        self,
        referer_url: str,
        csrf_token: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Submit a poll request to the provider backend.

        Args:
            referer_url: URL for referer.
            csrf_token: CSRF token to send with the request.
            payload: JSON-serializable payload for the operation.

        Returns:
            dict[str, Any]: Submit a poll request to the provider backend.
        """
        endpoint = f"{KAYAK_SCRAPE_SCHEME}://{self._host}/i/api/search/dynamic/flights/poll"
        headers = {
            "Content-Type": "application/json",
            "x-requested-with": "XMLHttpRequest",
            "x-csrf": csrf_token,
            "referer": referer_url,
        }
        response = self._session().post(
            endpoint,
            json=payload,
            headers=headers,
            timeout=45,
        )
        body = self._safe_json_from_response(response)
        if response.status_code >= 400:
            detail = self._extract_error_detail(body) or f"HTTP {response.status_code}"
            _capture_provider_response(
                self.provider_id,
                "poll",
                {"endpoint": endpoint, "payload": payload},
                body,
                status_code=response.status_code,
                error=detail,
            )
            lowered = detail.lower()
            if any(code.lower() in lowered for code in self._NO_RESULT_CODES):
                raise ProviderNoResultError(detail)
            response.raise_for_status()
            raise RuntimeError(detail)

        errors = body.get("errors")
        if isinstance(errors, list) and errors:
            detail = self._extract_error_detail(body) or str(errors[0])
            _capture_provider_response(
                self.provider_id,
                "poll",
                {"endpoint": endpoint, "payload": payload},
                body,
                status_code=response.status_code,
                error=detail,
            )
            lowered = detail.lower()
            if any(code.lower() in lowered for code in self._NO_RESULT_CODES):
                raise ProviderNoResultError(detail)
            raise RuntimeError(detail)
        _capture_provider_response(
            self.provider_id,
            "poll",
            {"endpoint": endpoint, "payload": payload},
            body,
            status_code=response.status_code,
        )
        return body

    @staticmethod
    def _build_legs_payload(
        source: str,
        destination: str,
        outbound_iso: str,
        inbound_iso: str | None,
    ) -> list[dict[str, Any]]:
        """Build the provider legs payload.

        Args:
            source: Origin airport code for the request.
            destination: Destination airport code for the request.
            outbound_iso: Outbound travel date in ISO 8601 format.
            inbound_iso: Inbound travel date in ISO 8601 format.

        Returns:
            list[dict[str, Any]]: The provider legs payload.
        """
        source_code = str(source or "").strip().upper()
        destination_code = str(destination or "").strip().upper()
        legs = [
            {
                "origin": {"airports": [source_code], "locationType": "airports"},
                "destination": {"airports": [destination_code], "locationType": "airports"},
                "date": outbound_iso,
                "flex": "exact",
            }
        ]
        if inbound_iso:
            legs.append(
                {
                    "origin": {"airports": [destination_code], "locationType": "airports"},
                    "destination": {"airports": [source_code], "locationType": "airports"},
                    "date": inbound_iso,
                    "flex": "exact",
                }
            )
        return legs

    def _search_payload(
        self,
        source: str,
        destination: str,
        outbound_iso: str,
        inbound_iso: str | None,
        currency: str,
        adults: int,
    ) -> dict[str, Any]:
        """Build the provider search payload.

        Args:
            source: Origin airport code for the request.
            destination: Destination airport code for the request.
            outbound_iso: Outbound travel date in ISO 8601 format.
            inbound_iso: Inbound travel date in ISO 8601 format.
            currency: Currency code for pricing output.
            adults: Number of adult travelers.

        Returns:
            dict[str, Any]: The provider search payload.
        """
        page_url = self._search_page_url(
            source=source,
            destination=destination,
            outbound_iso=outbound_iso,
            inbound_iso=inbound_iso,
            currency=currency,
            adults=adults,
        )
        page_response = self._session().get(
            page_url,
            timeout=45,
        )
        page_response.raise_for_status()
        csrf_token, _bootstrap = self._extract_bootstrap(page_response.text)

        passenger_count = max(1, int(adults or 1))
        passengers = ["ADT"] * passenger_count
        passenger_details = [{"ptc": "ADT"} for _ in range(passenger_count)]
        search_payload = {
            "filterParams": {},
            "userSearchParams": {
                "legs": self._build_legs_payload(source, destination, outbound_iso, inbound_iso),
                "passengers": passengers,
                "passengerDetails": passenger_details,
                "sortMode": "price_a",
            },
            "searchMetaData": {
                "pageNumber": 1,
                "searchTypes": [],
            },
        }

        latest = self._post_poll(
            referer_url=page_response.url,
            csrf_token=csrf_token,
            payload=search_payload,
        )
        search_id = str(latest.get("searchId") or "").strip()
        for _ in range(self._poll_rounds):
            core_results = self._core_results(latest)
            status = str(latest.get("status") or "").strip().lower()
            if core_results and status in {"first-phase", "complete"}:
                break
            if not search_id:
                break
            search_payload["userSearchParams"]["searchId"] = search_id
            search_payload["searchMetaData"]["skipResultsInSecondPhase"] = False
            time.sleep(0.6)
            latest = self._post_poll(
                referer_url=page_response.url,
                csrf_token=csrf_token,
                payload=search_payload,
            )
            search_id = str(latest.get("searchId") or search_id).strip()
        return latest

    @staticmethod
    def _core_results(payload: dict[str, Any]) -> list[dict[str, Any]]:
        """Extract the core result payload from the provider response.

        Args:
            payload: JSON-serializable payload for the operation.

        Returns:
            list[dict[str, Any]]: Extract the core result payload from the provider response.
        """
        out: list[dict[str, Any]] = []
        for item in payload.get("results") or []:
            if not isinstance(item, dict):
                continue
            if str(item.get("type") or "").strip().lower() != "core":
                continue
            out.append(item)
        return out

    @staticmethod
    def _first_money_value(values: list[Any]) -> int | None:
        """Extract the first monetary value from the payload.

        Args:
            values: Input values for the operation.

        Returns:
            int | None: Extract the first monetary value from the payload.
        """
        for value in values:
            amount = parse_money_amount_int(value)
            if amount is not None:
                return amount
        return None

    @classmethod
    def _booking_explicit_total_amount(cls, booking: dict[str, Any]) -> int | None:
        """Extract an explicit total booking amount when available.

        Args:
            booking: Mapping of booking.

        Returns:
            int | None: Extract an explicit total booking amount when available.
        """
        display_price = booking.get("displayPrice") or {}
        price_obj = booking.get("price") or {}
        pricing_obj = booking.get("pricing") or {}
        return cls._first_money_value(
            [
                display_price.get("totalPrice"),
                display_price.get("totalLocalizedPrice"),
                display_price.get("localizedTotalPrice"),
                display_price.get("priceTotal"),
                display_price.get("total"),
                display_price.get("allInTotalPrice"),
                display_price.get("allInPrice"),
                booking.get("totalPrice"),
                booking.get("total"),
                booking.get("grandTotal"),
                booking.get("priceTotal"),
                price_obj.get("total"),
                price_obj.get("grandTotal"),
                pricing_obj.get("total"),
                pricing_obj.get("grandTotal"),
            ]
        )

    @classmethod
    def _booking_price_per_person_flag(cls, booking: dict[str, Any]) -> bool | None:
        """Return whether the booking price is per passenger.

        Args:
            booking: Mapping of booking.

        Returns:
            bool | None: True when the booking price is per passenger; otherwise, False.
        """
        stack: list[Any] = [booking]
        visited = 0
        max_nodes = 500
        while stack and visited < max_nodes:
            node = stack.pop()
            visited += 1
            if isinstance(node, dict):
                for key, value in node.items():
                    key_l = str(key or "").strip().lower()
                    if not key_l:
                        continue

                    if any(
                        marker in key_l
                        for marker in (
                            "perperson",
                            "per_person",
                            "per-passenger",
                            "per_passenger",
                            "pertraveler",
                            "per_traveler",
                        )
                    ):
                        if isinstance(value, bool):
                            return value
                        if isinstance(value, int | float):
                            return bool(value)
                        value_l = str(value or "").strip().lower()
                        if "total" in value_l:
                            return False
                        if any(
                            marker in value_l
                            for marker in ("person", "passenger", "traveler", "pp")
                        ):
                            return True

                    if key_l in {"pricemode", "price_mode", "pricetype", "price_type"}:
                        value_l = str(value or "").strip().lower()
                        if "total" in value_l:
                            return False
                        if any(
                            marker in value_l
                            for marker in ("person", "passenger", "traveler", "pp")
                        ):
                            return True

                    if isinstance(value, dict | list | tuple):
                        stack.append(value)
            elif isinstance(node, list | tuple):
                for value in node:
                    if isinstance(value, dict | list | tuple):
                        stack.append(value)
        return None

    @classmethod
    def _booking_option_amount(
        cls,
        booking: dict[str, Any],
        adults: int,
    ) -> tuple[int | None, str | None, str]:
        """Extract the numeric amount for a booking option.

        Args:
            booking: Mapping of booking.
            adults: Number of adult travelers.

        Returns:
            tuple[int | None, str | None, str]: Extract the numeric amount for a booking option.
        """
        display_price = booking.get("displayPrice") or {}
        source_currency = str(display_price.get("currency") or "").strip().upper() or None
        displayed_amount = cls._first_money_value(
            [
                display_price.get("price"),
                display_price.get("localizedPrice"),
            ]
        )
        explicit_total_amount = cls._booking_explicit_total_amount(booking)
        if explicit_total_amount is not None:
            return explicit_total_amount, source_currency, "explicit_total"

        if displayed_amount is None:
            return None, source_currency, "missing_price"

        requested_adults = max(1, int(adults or 1))
        per_person_flag = cls._booking_price_per_person_flag(booking)
        if requested_adults > 1 and per_person_flag is not False:
            # Kayak/Momondo often expose per-traveler display prices.
            return displayed_amount * requested_adults, source_currency, "per_person_scaled"
        return displayed_amount, source_currency, "displayed"

    @classmethod
    def _best_booking_option(
        cls,
        result: dict[str, Any],
        adults: int,
    ) -> tuple[dict[str, Any] | None, int | None, str | None, str]:
        """Select the best booking option from the provider results.

        Args:
            result: Result record for the current operation.
            adults: Number of adult travelers.

        Returns:
            tuple[dict[str, Any] | None, int | None, str | None, str]: Selected best booking option from the provider results.
        """
        best: dict[str, Any] | None = None
        best_amount: int | None = None
        best_currency: str | None = None
        best_assumption = "missing_price"
        for option in result.get("bookingOptions") or []:
            if not isinstance(option, dict):
                continue
            amount, source_currency, assumption = cls._booking_option_amount(option, adults)
            if amount is None:
                continue
            if best is None or best_amount is None or amount < best_amount:
                best = option
                best_amount = amount
                best_currency = source_currency
                best_assumption = assumption
        return best, best_amount, best_currency, best_assumption

    @staticmethod
    def _segment_ids_for_leg(
        leg_ref: dict[str, Any],
        legs_map: dict[str, Any],
    ) -> list[str]:
        """Extract segment identifiers for a leg payload.

        Args:
            leg_ref: Mapping of leg ref.
            legs_map: Mapping of legs.

        Returns:
            list[str]: Extract segment identifiers for a leg payload.
        """
        segment_ids: list[str] = []
        for segment_ref in leg_ref.get("segments") or []:
            if isinstance(segment_ref, dict):
                segment_id = str(segment_ref.get("id") or "").strip()
            else:
                segment_id = str(segment_ref or "").strip()
            if segment_id:
                segment_ids.append(segment_id)
        if segment_ids:
            return segment_ids

        leg_id = str(leg_ref.get("id") or "").strip()
        if not leg_id:
            return []
        mapped_leg = legs_map.get(leg_id) or {}
        for segment_ref in mapped_leg.get("segments") or []:
            if isinstance(segment_ref, dict):
                segment_id = str(segment_ref.get("id") or "").strip()
            else:
                segment_id = str(segment_ref or "").strip()
            if segment_id:
                segment_ids.append(segment_id)
        return segment_ids

    @staticmethod
    def _segment_entry(
        segment_id: str,
        segments_map: dict[str, Any],
        airports_map: dict[str, Any],
        airlines_map: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Build a normalized segment entry.

        Args:
            segment_id: Identifier for segment.
            segments_map: Mapping of segments.
            airports_map: Mapping of airports.
            airlines_map: Mapping of airlines.

        Returns:
            dict[str, Any] | None: A normalized segment entry.
        """
        raw = segments_map.get(segment_id) or {}
        if not isinstance(raw, dict):
            return None
        source_code = str(raw.get("origin") or "").strip().upper()
        destination_code = str(raw.get("destination") or "").strip().upper()
        if not source_code or not destination_code:
            return None
        source_meta = airports_map.get(source_code) or {}
        destination_meta = airports_map.get(destination_code) or {}
        carrier_code = str(raw.get("airline") or "").strip().upper()
        carrier_meta = airlines_map.get(carrier_code) or {}
        carrier_name = (
            str(carrier_meta.get("name") or "").strip()
            or str(raw.get("operationalDisplay") or "").strip()
            or carrier_code
        )
        return {
            "from": source_code,
            "to": destination_code,
            "from_name": str(
                source_meta.get("displayName") or source_meta.get("fullDisplayName") or source_code
            ),
            "to_name": str(
                destination_meta.get("displayName")
                or destination_meta.get("fullDisplayName")
                or destination_code
            ),
            "depart_local": str(raw.get("departure") or "").strip() or None,
            "arrive_local": str(raw.get("arrival") or "").strip() or None,
            "carrier": carrier_code or carrier_name,
            "carrier_name": carrier_name,
        }

    def _segments_for_leg(
        self,
        leg_ref: dict[str, Any],
        legs_map: dict[str, Any],
        segments_map: dict[str, Any],
        airports_map: dict[str, Any],
        airlines_map: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Build normalized segments for a leg payload.

        Args:
            leg_ref: Mapping of leg ref.
            legs_map: Mapping of legs.
            segments_map: Mapping of segments.
            airports_map: Mapping of airports.
            airlines_map: Mapping of airlines.

        Returns:
            list[dict[str, Any]]: Normalized segments for a leg payload.
        """
        parsed: list[dict[str, Any]] = []
        for segment_id in self._segment_ids_for_leg(leg_ref, legs_map):
            segment = self._segment_entry(
                segment_id=segment_id,
                segments_map=segments_map,
                airports_map=airports_map,
                airlines_map=airlines_map,
            )
            if segment:
                parsed.append(segment)
        return parsed

    @staticmethod
    def _leg_duration_seconds(
        leg_ref: dict[str, Any],
        legs_map: dict[str, Any],
        segments: list[dict[str, Any]],
    ) -> int | None:
        """Handle leg duration seconds.

        Args:
            leg_ref: Mapping of leg ref.
            legs_map: Mapping of legs.
            segments: Mapping of segments.

        Returns:
            int | None: Handle leg duration seconds.
        """
        leg_id = str(leg_ref.get("id") or "").strip()
        mapped_leg = legs_map.get(leg_id) or {}
        duration_minutes = mapped_leg.get("duration")
        if isinstance(duration_minutes, int | float):
            return int(duration_minutes) * 60
        if segments:
            start = parse_local_datetime(segments[0].get("depart_local"))
            end = parse_local_datetime(segments[-1].get("arrive_local"))
            if start and end and end >= start:
                return int((end - start).total_seconds())
        return None

    def _normalize_price(
        self,
        amount: int | None,
        source_currency: str,
        target_currency: str,
    ) -> tuple[int | None, str]:
        """Normalize price.

        Args:
            amount: Numeric amount to convert or format.
            source_currency: Source currency code for conversion.
            target_currency: Target currency code for conversion.

        Returns:
            tuple[int | None, str]: Normalized price.
        """
        source = str(source_currency or "").strip().upper()
        target = str(target_currency or "").strip().upper()
        if amount is None:
            return None, target or source
        if not source:
            source = target
        if not target:
            target = source
        if source == target:
            return amount, target
        converted = convert_currency_amount(amount, source, target)
        if converted is not None:
            return converted, target
        return amount, source

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
            int(candidate.get("stops") or candidate.get("outbound_stops") or 0)
            + int(candidate.get("inbound_stops") or 0),
            int(candidate.get("duration_seconds") or PRICE_SENTINEL),
        )

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
        payload = self._search_payload(
            source=source,
            destination=destination,
            outbound_iso=departure_iso,
            inbound_iso=None,
            currency=currency,
            adults=adults,
        )
        core_results = self._core_results(payload)
        if not core_results:
            return None
        legs_map = payload.get("legs") or {}
        segments_map = payload.get("segments") or {}
        airports_map = payload.get("airports") or {}
        airlines_map = payload.get("airlines") or {}
        providers_map = payload.get("providers") or {}

        best: dict[str, Any] | None = None
        target_currency = str(currency or "RON").upper()
        for result in core_results:
            booking, booking_amount, booking_currency, booking_price_mode = (
                self._best_booking_option(
                    result,
                    adults=adults,
                )
            )
            if not booking:
                continue
            source_currency = str(booking_currency or target_currency).upper()
            normalized_price, normalized_currency = self._normalize_price(
                amount=booking_amount,
                source_currency=source_currency,
                target_currency=target_currency,
            )
            if normalized_price is None:
                continue

            leg_refs = result.get("legs") or []
            if not leg_refs or not isinstance(leg_refs[0], dict):
                continue
            outbound_segments = self._segments_for_leg(
                leg_ref=leg_refs[0],
                legs_map=legs_map,
                segments_map=segments_map,
                airports_map=airports_map,
                airlines_map=airlines_map,
            )
            if not outbound_segments:
                continue
            stops = max(0, len(outbound_segments) - 1)
            if stops > max_stops_per_leg:
                continue
            if max_connection_layover_seconds is not None:
                max_layover = max_segment_layover_seconds(outbound_segments)
                if max_layover is not None and max_layover > max_connection_layover_seconds:
                    continue

            duration_seconds = self._leg_duration_seconds(
                leg_ref=leg_refs[0],
                legs_map=legs_map,
                segments=outbound_segments,
            )
            booking_provider = str(booking.get("providerCode") or "").strip().upper()
            booking_provider_name = str(
                (providers_map.get(booking_provider) or {}).get("displayName") or ""
            ).strip()
            booking_url = absolute_kayak_url(
                ((booking.get("bookingUrl") or {}).get("url")),
                host=self._host,
            )
            if not booking_url:
                booking_url = absolute_kayak_url(result.get("shareableUrl"), host=self._host)
            candidate = {
                "price": int(normalized_price),
                "formatted_price": f"{int(normalized_price)} {normalized_currency}",
                "currency": normalized_currency,
                "duration_seconds": duration_seconds,
                "stops": stops,
                "transfer_events": transfer_events_from_segments(outbound_segments),
                "booking_url": booking_url,
                "segments": outbound_segments,
                "provider": self.provider_id,
                "booking_provider": booking_provider_name or booking_provider or self.display_name,
                "price_mode": booking_price_mode,
            }
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
        payload = self._search_payload(
            source=source,
            destination=destination,
            outbound_iso=outbound_iso,
            inbound_iso=inbound_iso,
            currency=currency,
            adults=adults,
        )
        core_results = self._core_results(payload)
        if not core_results:
            return None
        legs_map = payload.get("legs") or {}
        segments_map = payload.get("segments") or {}
        airports_map = payload.get("airports") or {}
        airlines_map = payload.get("airlines") or {}
        providers_map = payload.get("providers") or {}

        best: dict[str, Any] | None = None
        target_currency = str(currency or "RON").upper()
        for result in core_results:
            leg_refs = result.get("legs") or []
            if len(leg_refs) < 2:
                continue
            if not isinstance(leg_refs[0], dict) or not isinstance(leg_refs[1], dict):
                continue

            booking, booking_amount, booking_currency, booking_price_mode = (
                self._best_booking_option(
                    result,
                    adults=adults,
                )
            )
            if not booking:
                continue
            source_currency = str(booking_currency or target_currency).upper()
            normalized_price, normalized_currency = self._normalize_price(
                amount=booking_amount,
                source_currency=source_currency,
                target_currency=target_currency,
            )
            if normalized_price is None:
                continue

            outbound_segments = self._segments_for_leg(
                leg_ref=leg_refs[0],
                legs_map=legs_map,
                segments_map=segments_map,
                airports_map=airports_map,
                airlines_map=airlines_map,
            )
            inbound_segments = self._segments_for_leg(
                leg_ref=leg_refs[1],
                legs_map=legs_map,
                segments_map=segments_map,
                airports_map=airports_map,
                airlines_map=airlines_map,
            )
            if not outbound_segments or not inbound_segments:
                continue
            outbound_stops = max(0, len(outbound_segments) - 1)
            inbound_stops = max(0, len(inbound_segments) - 1)
            if outbound_stops > max_stops_per_leg or inbound_stops > max_stops_per_leg:
                continue
            if max_connection_layover_seconds is not None:
                outbound_max_layover = max_segment_layover_seconds(outbound_segments)
                inbound_max_layover = max_segment_layover_seconds(inbound_segments)
                if (
                    outbound_max_layover is not None
                    and outbound_max_layover > max_connection_layover_seconds
                ) or (
                    inbound_max_layover is not None
                    and inbound_max_layover > max_connection_layover_seconds
                ):
                    continue

            outbound_duration_seconds = self._leg_duration_seconds(
                leg_ref=leg_refs[0],
                legs_map=legs_map,
                segments=outbound_segments,
            )
            inbound_duration_seconds = self._leg_duration_seconds(
                leg_ref=leg_refs[1],
                legs_map=legs_map,
                segments=inbound_segments,
            )
            total_duration_seconds = (
                (outbound_duration_seconds or 0) + (inbound_duration_seconds or 0)
                if outbound_duration_seconds is not None and inbound_duration_seconds is not None
                else None
            )

            booking_provider = str(booking.get("providerCode") or "").strip().upper()
            booking_provider_name = str(
                (providers_map.get(booking_provider) or {}).get("displayName") or ""
            ).strip()
            booking_url = absolute_kayak_url(
                ((booking.get("bookingUrl") or {}).get("url")),
                host=self._host,
            )
            if not booking_url:
                booking_url = absolute_kayak_url(result.get("shareableUrl"), host=self._host)
            candidate = {
                "price": int(normalized_price),
                "formatted_price": f"{int(normalized_price)} {normalized_currency}",
                "currency": normalized_currency,
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
                "booking_provider": booking_provider_name or booking_provider or self.display_name,
                "price_mode": booking_price_mode,
            }
            if best is None or self._candidate_sort_key(candidate) < self._candidate_sort_key(best):
                best = candidate
        return best


class MomondoScrapeClient(KayakScrapeClient):
    """Represent MomondoScrapeClient."""

    provider_id = "momondo"
    display_name = "Momondo Scrape"
    docs_url = "https://www.momondo.com/flight-search/"

    def __init__(
        self,
        host: str | None = None,
        poll_rounds: int | None = None,
    ) -> None:
        """Initialize the MomondoScrapeClient.

        Args:
            host: Host name for the request.
            poll_rounds: Number of polling rounds to execute.
        """
        super().__init__(
            host=host if host is not None else MOMONDO_SCRAPE_HOST,
            poll_rounds=poll_rounds,
        )
