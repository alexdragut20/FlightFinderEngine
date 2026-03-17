from __future__ import annotations

import os
import threading
import time
from http import HTTPStatus
from typing import Any

import requests

from ..config import AMADEUS_BASE_URL, AMADEUS_FLIGHT_OFFERS_MAX
from ..exceptions import ProviderNoResultError
from ..logging_utils import capture_provider_response as _capture_provider_response
from ..utils import (
    date_only,
    max_segment_layover_seconds,
    parse_iso8601_duration_seconds,
    transfer_events_from_segments,
)
from ._cache import per_instance_lru_cache


class AmadeusClient:
    provider_id = "amadeus"
    display_name = "Amadeus Self-Service"
    supports_calendar = True
    requires_credentials = True
    credential_env: tuple[str, ...] = ("AMADEUS_CLIENT_ID", "AMADEUS_CLIENT_SECRET")
    docs_url = "https://developers.amadeus.com/"
    default_enabled = False
    _TRANSIENT_HTTP_STATUSES = {429, 500, 502, 503, 504}
    _NO_RESULT_MARKERS = (
        "no flight offers found",
        "no matching flight",
        "no offers found",
        "no journey found",
        "no flights found",
        "no result",
        "originlocationcode",
        "destinationlocationcode",
        "invalid iata",
    )

    def __init__(
        self,
        client_id: str | None = None,
        client_secret: str | None = None,
        base_url: str | None = None,
    ) -> None:
        self._client_id = (
            client_id if client_id is not None else os.getenv("AMADEUS_CLIENT_ID") or ""
        ).strip()
        self._client_secret = (
            client_secret if client_secret is not None else os.getenv("AMADEUS_CLIENT_SECRET") or ""
        ).strip()
        self._base_url = str(base_url or AMADEUS_BASE_URL).rstrip("/")
        self._local = threading.local()
        self._token_lock = threading.Lock()
        self._access_token = ""
        self._token_expires_at = 0.0

    def is_configured(self) -> bool:
        return bool(self._client_id and self._client_secret)

    def _session(self) -> requests.Session:
        if not hasattr(self._local, "session"):
            self._local.session = requests.Session()
        return self._local.session

    def _fetch_token(self) -> str:
        if not self.is_configured():
            raise RuntimeError("Amadeus credentials are missing")

        now = time.time()
        with self._token_lock:
            if self._access_token and now < self._token_expires_at:
                return self._access_token

            response = self._session().post(
                f"{self._base_url}/v1/security/oauth2/token",
                data={
                    "grant_type": "client_credentials",
                    "client_id": self._client_id,
                    "client_secret": self._client_secret,
                },
                timeout=30,
            )
            response.raise_for_status()
            payload = response.json()
            token = str(payload.get("access_token") or "").strip()
            if not token:
                raise RuntimeError("Amadeus token response missing access_token")
            expires_in = int(payload.get("expires_in") or 900)
            self._access_token = token
            self._token_expires_at = now + max(60, expires_in - 45)
            return self._access_token

    @staticmethod
    def _safe_json(response: requests.Response) -> dict[str, Any]:
        try:
            payload = response.json()
        except ValueError:
            return {}
        return payload if isinstance(payload, dict) else {}

    @classmethod
    def _error_detail(cls, payload: dict[str, Any]) -> str:
        errors = payload.get("errors")
        if not isinstance(errors, list) or not errors:
            return ""
        first = errors[0] or {}
        if isinstance(first, dict):
            detail = str(
                first.get("detail") or first.get("title") or first.get("code") or ""
            ).strip()
            if detail:
                return detail
        return str(first).strip()

    @classmethod
    def _is_no_result_error(cls, status_code: int, detail: str) -> bool:
        if status_code not in {400, 404, 422}:
            return False
        lowered = str(detail or "").strip().lower()
        if not lowered:
            return False
        return any(marker in lowered for marker in cls._NO_RESULT_MARKERS)

    def _get(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        max_attempts = 3
        for attempt in range(max_attempts):
            token = self._fetch_token()
            response = self._session().get(
                f"{self._base_url}{path}",
                params=params,
                headers={"Authorization": f"Bearer {token}"},
                timeout=45,
            )
            payload = self._safe_json(response)

            if response.status_code == HTTPStatus.UNAUTHORIZED:
                _capture_provider_response(
                    self.provider_id,
                    "flight_request",
                    {"path": path, "params": params, "attempt": attempt + 1},
                    payload,
                    status_code=response.status_code,
                    error="HTTP 401 unauthorized",
                )
                with self._token_lock:
                    self._access_token = ""
                    self._token_expires_at = 0.0
                if attempt + 1 < max_attempts:
                    continue

            if response.status_code in self._TRANSIENT_HTTP_STATUSES and attempt + 1 < max_attempts:
                _capture_provider_response(
                    self.provider_id,
                    "flight_request",
                    {"path": path, "params": params, "attempt": attempt + 1},
                    payload,
                    status_code=response.status_code,
                    error=f"HTTP {response.status_code} transient",
                )
                retry_after_raw = str(response.headers.get("Retry-After") or "").strip()
                try:
                    retry_after = float(retry_after_raw)
                except ValueError:
                    retry_after = 0.0
                delay = max(0.35 * (2**attempt), retry_after)
                time.sleep(min(3.0, delay))
                continue

            if response.status_code >= 400:
                detail = self._error_detail(payload) or f"HTTP {response.status_code}"
                _capture_provider_response(
                    self.provider_id,
                    "flight_request",
                    {"path": path, "params": params, "attempt": attempt + 1},
                    payload,
                    status_code=response.status_code,
                    error=detail,
                )
                if self._is_no_result_error(response.status_code, detail):
                    raise ProviderNoResultError(detail)
                response.raise_for_status()

            errors = payload.get("errors")
            if isinstance(errors, list) and errors:
                detail = self._error_detail(payload) or str(errors[0])
                _capture_provider_response(
                    self.provider_id,
                    "flight_request",
                    {"path": path, "params": params, "attempt": attempt + 1},
                    payload,
                    status_code=response.status_code,
                    error=detail,
                )
                if self._is_no_result_error(response.status_code, detail):
                    raise ProviderNoResultError(detail)
                raise RuntimeError(detail)
            _capture_provider_response(
                self.provider_id,
                "flight_request",
                {"path": path, "params": params, "attempt": attempt + 1},
                payload,
                status_code=response.status_code,
            )
            return payload

        raise RuntimeError("Amadeus request retries exhausted")

    @staticmethod
    def _amount_to_int(value: Any) -> int | None:
        if value in (None, ""):
            return None
        try:
            return int(round(float(value)))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _format_price(amount: int | None, currency: str) -> str | None:
        if amount is None:
            return None
        return f"{amount} {currency}"

    @staticmethod
    def _parse_segments(itinerary: dict[str, Any] | None) -> list[dict[str, Any]]:
        parsed: list[dict[str, Any]] = []
        for segment in (itinerary or {}).get("segments") or []:
            departure = segment.get("departure") or {}
            arrival = segment.get("arrival") or {}
            carrier_code = str(segment.get("carrierCode") or "").upper() or None
            parsed.append(
                {
                    "from": departure.get("iataCode"),
                    "to": arrival.get("iataCode"),
                    "from_name": None,
                    "to_name": None,
                    "depart_local": departure.get("at"),
                    "arrive_local": arrival.get("at"),
                    "carrier": carrier_code,
                    "carrier_name": carrier_code,
                }
            )
        return parsed

    @staticmethod
    def _duration_seconds(itinerary: dict[str, Any] | None) -> int | None:
        return parse_iso8601_duration_seconds((itinerary or {}).get("duration"))

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
        if not self.is_configured():
            return {}
        source_code = source.upper()
        destination_code = destination.upper()
        params: dict[str, Any] = {
            "origin": source_code,
            "destination": destination_code,
            "departureDate": f"{date_start_iso},{date_end_iso}",
            "oneWay": "true",
            "currencyCode": currency,
        }
        if max_stops_per_leg == 0:
            params["nonStop"] = "true"

        try:
            payload = self._get("/v1/shopping/flight-dates", params)
        except ProviderNoResultError:
            return {}
        prices: dict[str, int] = {}
        for item in payload.get("data") or []:
            date_iso = date_only(item.get("departureDate"))
            amount = self._amount_to_int((item.get("price") or {}).get("total"))
            if not date_iso or amount is None:
                continue
            existing = prices.get(date_iso)
            if existing is None or amount < existing:
                prices[date_iso] = amount
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
        if not self.is_configured():
            return None
        source_code = source.upper()
        destination_code = destination.upper()

        params: dict[str, Any] = {
            "originLocationCode": source_code,
            "destinationLocationCode": destination_code,
            "departureDate": departure_iso,
            "adults": max(1, adults),
            "currencyCode": currency,
            "max": AMADEUS_FLIGHT_OFFERS_MAX,
        }
        if max_stops_per_leg == 0:
            params["nonStop"] = "true"

        try:
            payload = self._get("/v2/shopping/flight-offers", params)
        except ProviderNoResultError:
            return None
        best: dict[str, Any] | None = None
        for offer in payload.get("data") or []:
            itineraries = offer.get("itineraries") or []
            if not itineraries:
                continue

            itinerary = itineraries[0]
            segments = self._parse_segments(itinerary)
            stops = max(0, len(segments) - 1)
            if stops > max_stops_per_leg:
                continue
            if max_connection_layover_seconds is not None:
                leg_max_layover = max_segment_layover_seconds(segments)
                if leg_max_layover is not None and leg_max_layover > max_connection_layover_seconds:
                    continue

            amount = self._amount_to_int((offer.get("price") or {}).get("grandTotal"))
            if amount is None:
                continue
            currency_code = str((offer.get("price") or {}).get("currency") or currency).upper()
            duration_seconds = self._duration_seconds(itinerary)
            candidate = {
                "price": amount,
                "formatted_price": self._format_price(amount, currency_code),
                "currency": currency_code,
                "duration_seconds": duration_seconds,
                "stops": stops,
                "transfer_events": transfer_events_from_segments(segments),
                "booking_url": None,
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
        if not self.is_configured():
            return None
        source_code = source.upper()
        destination_code = destination.upper()

        params: dict[str, Any] = {
            "originLocationCode": source_code,
            "destinationLocationCode": destination_code,
            "departureDate": outbound_iso,
            "returnDate": inbound_iso,
            "adults": max(1, adults),
            "currencyCode": currency,
            "max": AMADEUS_FLIGHT_OFFERS_MAX,
        }
        if max_stops_per_leg == 0:
            params["nonStop"] = "true"

        try:
            payload = self._get("/v2/shopping/flight-offers", params)
        except ProviderNoResultError:
            return None
        best: dict[str, Any] | None = None
        for offer in payload.get("data") or []:
            itineraries = offer.get("itineraries") or []
            if len(itineraries) < 2:
                continue

            outbound_itinerary = itineraries[0]
            inbound_itinerary = itineraries[1]
            outbound_segments = self._parse_segments(outbound_itinerary)
            inbound_segments = self._parse_segments(inbound_itinerary)
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

            amount = self._amount_to_int((offer.get("price") or {}).get("grandTotal"))
            if amount is None:
                continue
            currency_code = str((offer.get("price") or {}).get("currency") or currency).upper()
            outbound_duration_seconds = self._duration_seconds(outbound_itinerary)
            inbound_duration_seconds = self._duration_seconds(inbound_itinerary)
            total_duration_seconds = (
                (outbound_duration_seconds or 0) + (inbound_duration_seconds or 0)
                if outbound_duration_seconds is not None and inbound_duration_seconds is not None
                else None
            )

            candidate = {
                "price": amount,
                "formatted_price": self._format_price(amount, currency_code),
                "currency": currency_code,
                "duration_seconds": total_duration_seconds,
                "outbound_duration_seconds": outbound_duration_seconds,
                "inbound_duration_seconds": inbound_duration_seconds,
                "outbound_stops": outbound_stops,
                "inbound_stops": inbound_stops,
                "outbound_transfer_events": transfer_events_from_segments(outbound_segments),
                "inbound_transfer_events": transfer_events_from_segments(inbound_segments),
                "booking_url": None,
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
            if int(candidate["price"]) == int(best["price"]):
                candidate_total_stops = int(candidate["outbound_stops"]) + int(
                    candidate["inbound_stops"]
                )
                best_total_stops = int(best["outbound_stops"]) + int(best["inbound_stops"])
                if candidate_total_stops < best_total_stops:
                    best = candidate
        return best
