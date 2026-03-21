from __future__ import annotations

import datetime as dt
import os
import threading
from typing import Any

import requests

from ..config import TRAVELPAYOUTS_DATA_API_URL
from ..exceptions import ProviderNoResultError
from ..utils import date_only, parse_money_amount_int
from ..utils.logging import capture_provider_response as _capture_provider_response
from ._cache import per_instance_lru_cache


class TravelpayoutsDataClient:
    """Provider client for the Aviasales Data API exposed through Travelpayouts."""

    provider_id = "travelpayouts"
    display_name = "Aviasales Data API (Travelpayouts)"
    supports_calendar = True
    requires_credentials = True
    credential_env: tuple[str, ...] = ("TRAVELPAYOUTS_API_TOKEN",)
    docs_url = "https://support.travelpayouts.com/hc/en-us/articles/203956163-Aviasales-Data-API"
    default_enabled = False

    def __init__(
        self,
        api_token: str | None = None,
        base_url: str | None = None,
        market: str | None = None,
    ) -> None:
        """Initialize the Travelpayouts client."""
        self._api_token = (
            api_token if api_token is not None else os.getenv("TRAVELPAYOUTS_API_TOKEN") or ""
        ).strip()
        self._base_url = str(base_url or TRAVELPAYOUTS_DATA_API_URL).rstrip("/")
        self._market = str(
            market if market is not None else os.getenv("TRAVELPAYOUTS_MARKET") or ""
        )
        self._market = self._market.strip().lower()
        self._local = threading.local()

    def is_configured(self) -> bool:
        """Return whether the client has a usable API token."""
        return bool(self._api_token)

    def configuration_hint(self) -> str | None:
        """Return a short setup hint for the UI."""
        if self.is_configured():
            if self._market:
                return f"Cached fare data enabled for market {self._market}."
            return "Cached fare data enabled."
        return "Needs a Travelpayouts API token for the Aviasales Data API."

    def _session(self) -> requests.Session:
        """Return a cached requests session."""
        if not hasattr(self._local, "session"):
            self._local.session = requests.Session()
        return self._local.session

    @staticmethod
    def _safe_json(response: requests.Response) -> dict[str, Any]:
        """Safely decode a JSON payload."""
        try:
            payload = response.json()
        except ValueError:
            return {}
        return payload if isinstance(payload, dict) else {}

    def _request(self, path: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        """Execute a GET request against the Travelpayouts data API."""
        if not self.is_configured():
            raise RuntimeError("Travelpayouts API token is missing")
        query = {key: value for key, value in params.items() if value not in (None, "", False)}
        if self._market and "market" not in query:
            query["market"] = self._market
        response = self._session().get(
            f"{self._base_url}{path}",
            params=query,
            headers={
                "X-Access-Token": self._api_token,
                "Accept-Encoding": "gzip, deflate",
            },
            timeout=45,
        )
        payload = self._safe_json(response)
        error = ""
        if response.status_code >= 400:
            error = str(payload.get("error") or f"HTTP {response.status_code}").strip()
        elif payload.get("success") is False:
            error = str(payload.get("error") or "Travelpayouts returned success=false").strip()
        _capture_provider_response(
            self.provider_id,
            "data_request",
            {"path": path, "params": query},
            payload,
            status_code=response.status_code,
            error=error or None,
        )
        if error:
            raise RuntimeError(f"Travelpayouts request failed: {error}")
        data = payload.get("data")
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
        if isinstance(data, dict):
            return [data]
        return []

    @staticmethod
    def _month_key(date_iso: str) -> str:
        """Return the year-month prefix for an ISO date."""
        return str(date_iso or "").strip()[:7]

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
            months.append(cursor.isoformat()[:7])
            if cursor.month == 12:
                cursor = dt.date(cursor.year + 1, 1, 1)
            else:
                cursor = dt.date(cursor.year, cursor.month + 1, 1)
        return tuple(months)

    @staticmethod
    def _absolute_aviasales_url(path_or_url: str | None) -> str | None:
        """Return an absolute Aviasales URL for a ticket link."""
        raw = str(path_or_url or "").strip()
        if not raw:
            return None
        lowered = raw.lower()
        if lowered.startswith("http://") or lowered.startswith("https://"):
            return raw
        if raw.startswith("/"):
            return f"https://www.aviasales.com{raw}"
        return f"https://www.aviasales.com/{raw.lstrip('/')}"

    @staticmethod
    def _price_int(item: dict[str, Any]) -> int | None:
        """Return the normalized price from a Travelpayouts item."""
        return parse_money_amount_int(item.get("price") or item.get("value"))

    @staticmethod
    def _int_value(item: dict[str, Any], *keys: str) -> int | None:
        """Return the first integer-like value among the given keys."""
        for key in keys:
            raw = item.get(key)
            if raw in (None, ""):
                continue
            try:
                return int(raw)
            except (TypeError, ValueError):
                continue
        return None

    @per_instance_lru_cache(maxsize=256)
    def _prices_for_dates_month(
        self,
        source: str,
        destination: str,
        departure_month: str,
        *,
        return_month: str | None,
        currency: str,
        direct: bool,
        one_way: bool,
    ) -> tuple[dict[str, Any], ...]:
        """Fetch a cached month-level price bucket from Travelpayouts."""
        params: dict[str, Any] = {
            "origin": str(source or "").strip().upper(),
            "destination": str(destination or "").strip().upper(),
            "departure_at": departure_month,
            "one_way": str(bool(one_way)).lower(),
            "sorting": "price",
            "direct": str(bool(direct)).lower(),
            "limit": 1000,
            "page": 1,
            "currency": str(currency or "USD").strip().lower() or "usd",
        }
        if not one_way and return_month:
            params["return_at"] = return_month
        return tuple(
            self._request(
                "/v3/prices_for_dates",
                params,
            )
        )

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
        """Return the cheapest cached fare per departure date for the requested range."""
        del adults, hand_bags, hold_bags
        out: dict[str, int] = {}
        for month_key in self._month_starts_between(date_start_iso, date_end_iso):
            rows = self._prices_for_dates_month(
                source,
                destination,
                month_key,
                return_month=None,
                currency=currency,
                direct=max_stops_per_leg <= 0,
                one_way=True,
            )
            for item in rows:
                departure_date = date_only(item.get("departure_at") or item.get("depart_date"))
                if (
                    not departure_date
                    or departure_date < date_start_iso
                    or departure_date > date_end_iso
                ):
                    continue
                stops = self._int_value(item, "transfers", "number_of_changes") or 0
                if stops > max_stops_per_leg:
                    continue
                price = self._price_int(item)
                if price is None:
                    continue
                previous = out.get(departure_date)
                if previous is None or price < previous:
                    out[departure_date] = price
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
        """Return the best cached one-way fare for an exact departure date."""
        del adults, hand_bags, hold_bags, max_connection_layover_seconds
        rows = self._prices_for_dates_month(
            source,
            destination,
            self._month_key(departure_iso),
            return_month=None,
            currency=currency,
            direct=max_stops_per_leg <= 0,
            one_way=True,
        )
        best: dict[str, Any] | None = None
        for item in rows:
            if date_only(item.get("departure_at") or item.get("depart_date")) != departure_iso:
                continue
            stops = self._int_value(item, "transfers", "number_of_changes") or 0
            if stops > max_stops_per_leg:
                continue
            price = self._price_int(item)
            if price is None:
                continue
            duration_minutes = self._int_value(item, "duration_to", "duration") or 0
            candidate = {
                "price": price,
                "formatted_price": f"{price} {str(currency or 'USD').strip().upper() or 'USD'}",
                "currency": str(currency or "USD").strip().upper() or "USD",
                "duration_seconds": max(0, duration_minutes) * 60,
                "stops": stops,
                "transfer_events": stops,
                "booking_url": self._absolute_aviasales_url(
                    item.get("ticket_link") or item.get("link")
                ),
                "segments": [],
                "provider": self.provider_id,
            }
            if best is None or candidate["price"] < best["price"]:
                best = candidate
        if best is None:
            raise ProviderNoResultError(
                f"Travelpayouts returned no exact one-way fare for {source}->{destination} on {departure_iso}."
            )
        return best

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
        """Return the best cached round-trip fare for exact outbound and inbound dates."""
        del adults, hand_bags, hold_bags, max_connection_layover_seconds
        rows = self._prices_for_dates_month(
            source,
            destination,
            self._month_key(outbound_iso),
            return_month=self._month_key(inbound_iso),
            currency=currency,
            direct=max_stops_per_leg <= 0,
            one_way=False,
        )
        best: dict[str, Any] | None = None
        for item in rows:
            if date_only(item.get("departure_at") or item.get("depart_date")) != outbound_iso:
                continue
            if date_only(item.get("return_at") or item.get("return_date")) != inbound_iso:
                continue
            outbound_stops = self._int_value(item, "transfers", "number_of_changes") or 0
            inbound_stops = self._int_value(item, "return_transfers") or 0
            if outbound_stops > max_stops_per_leg or inbound_stops > max_stops_per_leg:
                continue
            price = self._price_int(item)
            if price is None:
                continue
            outbound_minutes = self._int_value(item, "duration_to", "duration") or 0
            inbound_minutes = self._int_value(item, "duration_back") or 0
            total_minutes = self._int_value(item, "duration") or (
                max(0, outbound_minutes) + max(0, inbound_minutes)
            )
            candidate = {
                "price": price,
                "formatted_price": f"{price} {str(currency or 'USD').strip().upper() or 'USD'}",
                "currency": str(currency or "USD").strip().upper() or "USD",
                "duration_seconds": max(0, total_minutes) * 60,
                "outbound_duration_seconds": max(0, outbound_minutes) * 60,
                "inbound_duration_seconds": max(0, inbound_minutes) * 60,
                "outbound_stops": outbound_stops,
                "inbound_stops": inbound_stops,
                "transfer_events": outbound_stops + inbound_stops,
                "booking_url": self._absolute_aviasales_url(
                    item.get("ticket_link") or item.get("link")
                ),
                "outbound_segments": [],
                "inbound_segments": [],
                "provider": self.provider_id,
            }
            if best is None or candidate["price"] < best["price"]:
                best = candidate
        if best is None:
            raise ProviderNoResultError(
                "Travelpayouts returned no exact round-trip fare "
                f"for {source}->{destination} on {outbound_iso}/{inbound_iso}."
            )
        return best
