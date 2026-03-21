from __future__ import annotations

import datetime as dt
import html
import re
import threading
from typing import Any

import requests

from ..config import AZAIR_BASE_URL
from ..data.airports import AirportCoordinates
from ..exceptions import ProviderNoResultError
from ..utils import date_only, parse_money_amount_int
from ..utils.logging import capture_provider_response as _capture_provider_response
from ._cache import per_instance_lru_cache

_RESULT_BLOCK_PATTERN = re.compile(
    r'<div class="result .*?<div class="smBarDetail">\s*</div>\s*</div>',
    re.DOTALL,
)
_TOTAL_PRICE_PATTERN = re.compile(
    r'<span class="sumPrice">Total:\s*<span class="bp">([^<]+)</span>',
    re.DOTALL,
)
_SUMMARY_PATTERN = re.compile(
    r'<span class="caption (?P<kind>[^"]+)">(?P<label>[^<]+)</span>\s*'
    r'<span class="date">(?P<date>[^<]+)</span>.*?'
    r'<span class="durcha">(?P<duration>[^<]+)</span>\s*'
    r'<span class="subPrice">(?P<price>[^<]+)</span>',
    re.DOTALL,
)
_BOOKMARK_PATTERN = re.compile(
    r'<div class="bookmark"[^>]*>\s*<a href="([^"]+)"',
    re.DOTALL,
)
_BOOK_FLIGHT_PATTERN = re.compile(r'<a href="([^"]+)"[^>]*>Book&nbsp;flight</a>', re.DOTALL)
_DATE_TOKEN_PATTERN = re.compile(r"(\d{2}/\d{2}/\d{2,4})")
_TIME_HOURS_MINUTES_PATTERN = re.compile(r"(\d+):(\d+)\s*h", re.IGNORECASE)
_TIME_HOURS_PATTERN = re.compile(r"(\d+)\s*h", re.IGNORECASE)
_TIME_MINUTES_PATTERN = re.compile(r"(\d+)\s*m", re.IGNORECASE)
_CHANGE_PATTERN = re.compile(r"/\s*(\d+)\s+change", re.IGNORECASE)


class AzairScrapeClient:
    """Provider client for token-free exact and flexi fare searches on AZair."""

    provider_id = "azair"
    display_name = "AZair (budget flights)"
    supports_calendar = True
    docs_url = "https://www.azair.eu/"
    default_enabled = True

    def __init__(
        self,
        *,
        base_url: str | None = None,
        coordinates: AirportCoordinates | None = None,
    ) -> None:
        """Initialize the AZair scrape client."""
        self._base_url = str(base_url or AZAIR_BASE_URL).rstrip("/")
        self._coords = coordinates if coordinates is not None else AirportCoordinates()
        self._local = threading.local()

    def is_configured(self) -> bool:
        """Return whether the token-free AZair provider is ready."""
        return True

    @staticmethod
    def configuration_hint() -> str:
        """Return a short provider note for the UI."""
        return "Token-free flexi fares focused on Europe and the Middle East."

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
                    "Accept-Language": "en-US,en;q=0.9",
                }
            )
            self._local.session = session
        return self._local.session

    @staticmethod
    def _coord_supported(coord: tuple[float, float] | None) -> bool:
        """Return whether a coordinate falls inside AZair's practical coverage footprint."""
        if coord is None:
            return False
        lat, lon = coord
        return 10.0 <= float(lat) <= 72.0 and -25.0 <= float(lon) <= 65.0

    def _market_supported(self, source: str, destination: str) -> bool:
        """Return whether both airports fit the AZair Europe/MENA footprint."""
        return self._coord_supported(self._coords.get(source)) and self._coord_supported(
            self._coords.get(destination)
        )

    @staticmethod
    def _airport_field(code: str) -> str:
        """Return the lightest airport selector string accepted by AZair."""
        normalized = str(code or "").strip().upper()
        return f"[{normalized}]" if normalized else ""

    @staticmethod
    def _parse_date_label(text: str) -> str | None:
        """Return an ISO date from an AZair summary label."""
        match = _DATE_TOKEN_PATTERN.search(str(text or ""))
        if not match:
            return None
        token = match.group(1)
        for fmt in ("%d/%m/%y", "%d/%m/%Y"):
            try:
                return dt.datetime.strptime(token, fmt).date().isoformat()
            except ValueError:
                continue
        return None

    @staticmethod
    def _parse_duration_seconds(text: str) -> int | None:
        """Return a duration in seconds from an AZair summary duration string."""
        raw = html.unescape(str(text or "")).replace("\xa0", " ").strip().lower()
        if not raw:
            return None
        hours = 0
        minutes = 0
        hours_minutes_match = _TIME_HOURS_MINUTES_PATTERN.search(raw)
        if hours_minutes_match:
            hours = int(hours_minutes_match.group(1))
            minutes = int(hours_minutes_match.group(2))
        else:
            hours_match = _TIME_HOURS_PATTERN.search(raw)
            minutes_match = _TIME_MINUTES_PATTERN.search(raw)
            if hours_match:
                hours = int(hours_match.group(1))
            if minutes_match:
                minutes = int(minutes_match.group(1))
        if hours == 0 and minutes == 0:
            return None
        return (hours * 3600) + (minutes * 60)

    @staticmethod
    def _parse_stops(text: str) -> int:
        """Return the stop count from an AZair summary duration string."""
        raw = html.unescape(str(text or "")).replace("\xa0", " ").strip().lower()
        if "direct" in raw:
            return 0
        match = _CHANGE_PATTERN.search(raw)
        if match:
            return max(0, int(match.group(1)))
        return 0

    def _absolute_url(self, path_or_url: str | None) -> str | None:
        """Return an absolute AZair URL."""
        raw = str(path_or_url or "").replace("&amp;", "&").strip()
        if not raw:
            return None
        lowered = raw.lower()
        if lowered.startswith("http://") or lowered.startswith("https://"):
            return raw
        if raw.startswith("/"):
            return f"{self._base_url}{raw}"
        return f"{self._base_url}/{raw.lstrip('/')}"

    @classmethod
    def _sort_key(cls, candidate: dict[str, Any]) -> tuple[int, int, int]:
        """Return a stable comparison key for parsed AZair candidates."""
        price = int(candidate.get("price") or 0)
        transfer_events = int(candidate.get("transfer_events") or 0)
        duration_seconds = int(candidate.get("duration_seconds") or 0)
        return (
            price if price > 0 else 10**9,
            transfer_events,
            duration_seconds if duration_seconds > 0 else 10**9,
        )

    def _parse_results(self, html_text: str, *, currency: str) -> tuple[dict[str, Any], ...]:
        """Parse AZair result cards into normalized itinerary payloads."""
        rows: list[dict[str, Any]] = []
        currency_code = str(currency or "EUR").strip().upper() or "EUR"
        for block in _RESULT_BLOCK_PATTERN.findall(html_text):
            total_match = _TOTAL_PRICE_PATTERN.search(block)
            total_price = parse_money_amount_int(
                html.unescape(total_match.group(1) if total_match else "")
            )
            if total_price is None or total_price <= 0:
                continue
            summaries = []
            for match in _SUMMARY_PATTERN.finditer(block):
                duration_text = html.unescape(match.group("duration")).replace("\xa0", " ").strip()
                summaries.append(
                    {
                        "kind": str(match.group("kind") or "").strip().lower(),
                        "label": html.unescape(match.group("label")).strip(),
                        "date_iso": self._parse_date_label(match.group("date")),
                        "duration_seconds": self._parse_duration_seconds(duration_text),
                        "stops": self._parse_stops(duration_text),
                        "summary_price": parse_money_amount_int(match.group("price")),
                    }
                )
            if not summaries:
                continue
            outbound = summaries[0]
            inbound = summaries[1] if len(summaries) > 1 else None
            outbound_duration = int(outbound.get("duration_seconds") or 0)
            inbound_duration = int(inbound.get("duration_seconds") or 0) if inbound else 0
            bookmark_match = _BOOKMARK_PATTERN.search(block)
            booking_url = self._absolute_url(bookmark_match.group(1) if bookmark_match else None)
            if not booking_url:
                fallback_match = _BOOK_FLIGHT_PATTERN.search(block)
                booking_url = self._absolute_url(
                    fallback_match.group(1) if fallback_match else None
                )
            payload: dict[str, Any] = {
                "provider": self.provider_id,
                "price": total_price,
                "formatted_price": f"{total_price} {currency_code}",
                "currency": currency_code,
                "duration_seconds": max(0, outbound_duration + inbound_duration),
                "booking_url": booking_url,
            }
            if inbound is None:
                payload.update(
                    {
                        "departure_iso": outbound.get("date_iso"),
                        "stops": int(outbound.get("stops") or 0),
                        "transfer_events": int(outbound.get("stops") or 0),
                        "segments": [],
                    }
                )
            else:
                payload.update(
                    {
                        "outbound_iso": outbound.get("date_iso"),
                        "inbound_iso": inbound.get("date_iso"),
                        "outbound_duration_seconds": outbound_duration,
                        "inbound_duration_seconds": inbound_duration,
                        "outbound_stops": int(outbound.get("stops") or 0),
                        "inbound_stops": int(inbound.get("stops") or 0),
                        "transfer_events": int(outbound.get("stops") or 0)
                        + int(inbound.get("stops") or 0),
                        "outbound_segments": [],
                        "inbound_segments": [],
                    }
                )
            rows.append(payload)
        return tuple(sorted(rows, key=self._sort_key))

    @per_instance_lru_cache(maxsize=4096)
    def _search_results(
        self,
        source: str,
        destination: str,
        date_start_iso: str,
        date_end_iso: str,
        currency: str,
        max_stops_per_leg: int,
        *,
        one_way: bool,
        stay_nights: int | None,
    ) -> tuple[dict[str, Any], ...]:
        """Execute and parse an AZair result page for the requested search window."""
        normalized_source = str(source or "").strip().upper()
        normalized_destination = str(destination or "").strip().upper()
        normalized_currency = str(currency or "EUR").strip().upper() or "EUR"
        if not self._market_supported(normalized_source, normalized_destination):
            return ()
        params: dict[str, Any] = {
            "adults": 1,
            "children": 0,
            "infants": 0,
            "currency": normalized_currency,
            "isOneway": "oneway" if one_way else "return",
            "searchtype": "flexi",
            "srcAirport": self._airport_field(normalized_source),
            "dstAirport": self._airport_field(normalized_destination),
            "depdate": date_only(date_start_iso),
            "arrdate": date_only(date_end_iso),
            "indexSubmit": "Search",
            "lang": "en",
            "maxChng": max(0, int(max_stops_per_leg)),
        }
        if not one_way and stay_nights is not None:
            params["minDaysStay"] = max(1, int(stay_nights))
            params["maxDaysStay"] = max(1, int(stay_nights))
        response = self._session().get(
            f"{self._base_url}/azfin.php",
            params=params,
            timeout=35,
        )
        error: str | None = None
        if response.status_code >= 400:
            error = f"HTTP {response.status_code}"
        _capture_provider_response(
            self.provider_id,
            "search_results",
            params,
            {"url": response.url, "result_blocks": response.text.count('<div class="result ')},
            status_code=response.status_code,
            error=error,
        )
        if error:
            raise RuntimeError(f"AZair request failed: {error}")
        return self._parse_results(response.text, currency=normalized_currency)

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
        """Return the cheapest flexi one-way fare per departure date."""
        del adults, hand_bags, hold_bags
        rows = self._search_results(
            source,
            destination,
            date_start_iso,
            date_end_iso,
            currency,
            max_stops_per_leg,
            one_way=True,
            stay_nights=None,
        )
        out: dict[str, int] = {}
        for row in rows:
            departure_iso = date_only(row.get("departure_iso"))
            if not departure_iso or departure_iso < date_start_iso or departure_iso > date_end_iso:
                continue
            price = int(row.get("price") or 0)
            if price <= 0:
                continue
            existing = out.get(departure_iso)
            if existing is None or price < existing:
                out[departure_iso] = price
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
        """Return the best AZair one-way fare for an exact departure date."""
        del adults, hand_bags, hold_bags, max_connection_layover_seconds
        if not self._market_supported(source, destination):
            raise ProviderNoResultError(
                f"AZair does not cover {source}->{destination} in its Europe/Middle East footprint."
            )
        rows = self._search_results(
            source,
            destination,
            departure_iso,
            departure_iso,
            currency,
            max_stops_per_leg,
            one_way=True,
            stay_nights=None,
        )
        for row in rows:
            if date_only(row.get("departure_iso")) == departure_iso:
                return dict(row)
        raise ProviderNoResultError(
            f"AZair returned no exact one-way fare for {source}->{destination} on {departure_iso}."
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
        """Return the best AZair round-trip fare for exact outbound and inbound dates."""
        del adults, hand_bags, hold_bags, max_connection_layover_seconds
        if not self._market_supported(source, destination):
            raise ProviderNoResultError(
                f"AZair does not cover {source}->{destination} in its Europe/Middle East footprint."
            )
        stay_nights = max(
            1,
            (
                dt.date.fromisoformat(date_only(inbound_iso))
                - dt.date.fromisoformat(date_only(outbound_iso))
            ).days,
        )
        rows = self._search_results(
            source,
            destination,
            outbound_iso,
            inbound_iso,
            currency,
            max_stops_per_leg,
            one_way=False,
            stay_nights=stay_nights,
        )
        for row in rows:
            if (
                date_only(row.get("outbound_iso")) == outbound_iso
                and date_only(row.get("inbound_iso")) == inbound_iso
            ):
                return dict(row)
        raise ProviderNoResultError(
            "AZair returned no exact round-trip fare "
            f"for {source}->{destination} on {outbound_iso}/{inbound_iso}."
        )
