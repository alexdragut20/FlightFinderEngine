from __future__ import annotations

import datetime as dt
import math
import re
import time
from typing import Any
from urllib.parse import urlencode, urljoin

import requests

from ..config import (
    _FX_CACHE_LOCK,
    _FX_CACHE_TTL_SECONDS,
    _FX_RATE_CACHE,
    KAYAK_SCRAPE_HOST,
    KAYAK_SCRAPE_SCHEME,
    MIN_SPLIT_CONNECTION_CROSS_AIRPORT_SECONDS,
    MIN_SPLIT_CONNECTION_SAME_AIRPORT_SECONDS,
    SUPPORTED_PROVIDER_IDS,
)
from .constants import SECONDS_PER_DAY, SECONDS_PER_HOUR, SECONDS_PER_MINUTE


def normalize_codes(value: Any, fallback: list[str]) -> tuple[str, ...]:
    """Normalize airport codes from the provided input.

    Args:
        value: Input value to process.
        fallback: Fallback value to use when parsing fails.

    Returns:
        tuple[str, ...]: Normalized airport codes from the provided input.
    """
    if isinstance(value, str):
        raw = value.replace(";", ",").split(",")
    elif isinstance(value, list):
        raw = value
    else:
        raw = fallback

    out: list[str] = []
    for item in raw:
        code = str(item).strip().upper()
        if not code:
            continue
        if code not in out:
            out.append(code)
    if not out:
        out = fallback
    return tuple(out)


def normalize_provider_ids(value: Any) -> tuple[str, ...]:
    """Normalize provider identifiers from the provided input.

    Args:
        value: Input value to process.

    Returns:
        tuple[str, ...]: Normalized provider identifiers from the provided input.
    """
    if isinstance(value, str):
        raw = value.replace(";", ",").split(",")
    elif isinstance(value, list):
        raw = value
    else:
        raw = list(SUPPORTED_PROVIDER_IDS)

    out: list[str] = []
    for item in raw:
        provider = str(item or "").strip().lower()
        if not provider:
            continue
        if provider in {"all", "*"}:
            return tuple(SUPPORTED_PROVIDER_IDS)
        if provider not in SUPPORTED_PROVIDER_IDS:
            continue
        if provider not in out:
            out.append(provider)

    if not out:
        out = ["kiwi"]
    return tuple(out)


def to_date(value: Any, fallback: dt.date) -> dt.date:
    """Convert the input value to a date.

    Args:
        value: Input value to process.
        fallback: Fallback value to use when parsing fails.

    Returns:
        dt.date: Converted input value to a date.
    """
    if not value:
        return fallback
    return dt.date.fromisoformat(str(value))


def clamp_int(value: Any, fallback: int, low: int, high: int) -> int:
    """Clamp an integer value to the allowed range.

    Args:
        value: Input value to process.
        fallback: Fallback value to use when parsing fails.
        low: Lower bound for the accepted range.
        high: Upper bound for the accepted range.

    Returns:
        int: Clamped integer value to the allowed range.
    """
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return fallback
    return max(low, min(high, parsed))


def clamp_optional_int(value: Any, fallback: int | None, low: int, high: int) -> int | None:
    """Clamp an optional integer value to the allowed range.

    Args:
        value: Input value to process.
        fallback: Fallback value to use when parsing fails.
        low: Lower bound for the accepted range.
        high: Upper bound for the accepted range.

    Returns:
        int | None: Clamped optional integer value to the allowed range.
    """
    if value in (None, ""):
        return fallback
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return fallback
    if parsed <= 0:
        return None
    return max(low, min(high, parsed))


def bounded_io_concurrency(io_workers: int) -> int:
    """Return the bounded I/O concurrency level.

    Args:
        io_workers: Maximum number of I/O workers to use.

    Returns:
        int: The bounded I/O concurrency level.
    """
    try:
        parsed = int(io_workers)
    except (TypeError, ValueError):
        parsed = 8
    return max(4, min(24, parsed))


def to_bool(value: Any, fallback: bool = False) -> bool:
    """Convert the input value to a boolean.

    Args:
        value: Input value to process.
        fallback: Fallback value to use when parsing fails.

    Returns:
        bool: Converted input value to a boolean.
    """
    if isinstance(value, bool):
        return value
    if value is None:
        return fallback
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    return fallback


def date_range(start: dt.date, end: dt.date) -> list[dt.date]:
    """Build an inclusive range of dates.

    Args:
        start: Start value for the requested range.
        end: End value for the requested range.

    Returns:
        list[dt.date]: An inclusive range of dates.
    """
    days = (end - start).days
    if days < 0:
        return []
    return [start + dt.timedelta(days=offset) for offset in range(days + 1)]


def haversine_km(a: tuple[float, float], b: tuple[float, float]) -> float:
    """Calculate the great-circle distance in kilometers.

    Args:
        a: First latitude and longitude pair.
        b: Second latitude and longitude pair.

    Returns:
        float: Calculated great-circle distance in kilometers.
    """
    radius = 6371.0
    lat1, lon1 = map(math.radians, a)
    lat2, lon2 = map(math.radians, b)
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    base = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 2 * radius * math.asin(math.sqrt(base))


def kiwi_oneway_url(
    source: str,
    destination: str,
    date_iso: str,
    max_stops_per_leg: int | None = None,
) -> str:
    """Build the Kiwi one-way booking URL.

    Args:
        source: Origin airport code for the request.
        destination: Destination airport code for the request.
        date_iso: Travel date in ISO 8601 format.
        max_stops_per_leg: Max stops per leg.

    Returns:
        str: The Kiwi one-way booking URL.
    """
    src = str(source or "").strip().lower()
    dst = str(destination or "").strip().lower()
    if not src or not dst:
        return "https://www.kiwi.com/en/search/results/"
    params: dict[str, Any] = {
        "from": src.upper(),
        "to": dst.upper(),
        "departure": date_iso,
    }
    if max_stops_per_leg is not None and max_stops_per_leg >= 0:
        params["maxStopsCount"] = int(max_stops_per_leg)
    query = urlencode(params)
    return f"https://www.kiwi.com/deep?{query}"


def kiwi_return_url(
    source: str,
    destination: str,
    outbound_iso: str,
    inbound_iso: str,
    max_stops_per_leg: int | None = None,
) -> str:
    """Build the Kiwi round-trip booking URL.

    Args:
        source: Origin airport code for the request.
        destination: Destination airport code for the request.
        outbound_iso: Outbound travel date in ISO 8601 format.
        inbound_iso: Inbound travel date in ISO 8601 format.
        max_stops_per_leg: Max stops per leg.

    Returns:
        str: The Kiwi round-trip booking URL.
    """
    src = str(source or "").strip().lower()
    dst = str(destination or "").strip().lower()
    if not src or not dst:
        return "https://www.kiwi.com/en/search/results/"
    params: dict[str, Any] = {
        "from": src.upper(),
        "to": dst.upper(),
        "departure": outbound_iso,
        "return": inbound_iso,
    }
    if max_stops_per_leg is not None and max_stops_per_leg >= 0:
        params["maxStopsCount"] = int(max_stops_per_leg)
    query = urlencode(params)
    return f"https://www.kiwi.com/deep?{query}"


def absolute_kiwi_url(path_or_url: str | None) -> str | None:
    """Return an absolute Kiwi URL.

    Args:
        path_or_url: URL for path or.

    Returns:
        str | None: An absolute Kiwi URL.
    """
    raw = str(path_or_url or "").strip()
    if not raw:
        return None
    if raw.startswith("http://") or raw.startswith("https://"):
        return raw
    if raw.startswith("/"):
        return f"https://www.kiwi.com{raw}"
    return f"https://www.kiwi.com/{raw.lstrip('/')}"


def absolute_kayak_url(path_or_url: str | None, host: str = KAYAK_SCRAPE_HOST) -> str | None:
    """Return an absolute Kayak URL.

    Args:
        path_or_url: URL for path or.
        host: Host name for the request.

    Returns:
        str | None: An absolute Kayak URL.
    """
    raw = str(path_or_url or "").strip()
    if not raw:
        return None
    base = f"{KAYAK_SCRAPE_SCHEME}://{host.strip().lower() or KAYAK_SCRAPE_HOST}"
    if raw.startswith("http://") or raw.startswith("https://"):
        return raw
    return urljoin(base, raw)


def itinerary_booking_url(itinerary: dict[str, Any] | None) -> str | None:
    """Resolve the preferred booking URL for an itinerary.

    Args:
        itinerary: Mapping of itinerary.

    Returns:
        str | None: Resolved preferred booking URL for an itinerary.
    """
    booking_options = (itinerary or {}).get("bookingOptions") or {}
    edges = booking_options.get("edges") or []
    if not isinstance(edges, list):
        return None

    for edge in edges:
        node = (edge or {}).get("node") or {}
        booking_url = absolute_kiwi_url(node.get("bookingUrl"))
        if booking_url:
            return booking_url
    return None


def parse_local_datetime(value: Any) -> dt.datetime | None:
    """Parse a local date-time string.

    Args:
        value: Input value to process.

    Returns:
        dt.datetime | None: Parsed local date-time string.
    """
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = f"{raw[:-1]}+00:00"
    try:
        parsed = dt.datetime.fromisoformat(raw)
    except ValueError:
        return None

    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(dt.UTC).replace(tzinfo=None)
    return parsed


def connection_gap_seconds(arrive_local: Any, depart_local: Any) -> int | None:
    """Calculate the connection gap in seconds.

    Args:
        arrive_local: Localized arrival timestamp for the segment.
        depart_local: Depart local.

    Returns:
        int | None: Calculated connection gap in seconds.
    """
    arrive = parse_local_datetime(arrive_local)
    depart = parse_local_datetime(depart_local)
    if not arrive or not depart:
        return None

    gap_seconds = int((depart - arrive).total_seconds())
    if gap_seconds < 0:
        return None
    return gap_seconds


def max_segment_layover_seconds(segments: list[dict[str, Any]] | None) -> int | None:
    """Return the maximum layover across itinerary segments.

    Args:
        segments: Mapping of segments.

    Returns:
        int | None: The maximum layover across itinerary segments.
    """
    if not segments:
        return 0
    if len(segments) < 2:
        return 0

    best: int | None = 0
    for idx in range(len(segments) - 1):
        gap_seconds = connection_gap_seconds(
            segments[idx].get("arrive_local"),
            segments[idx + 1].get("depart_local"),
        )
        if gap_seconds is None:
            continue
        if best is None or gap_seconds > best:
            best = gap_seconds
    return best


def minimum_split_boundary_connection_seconds(
    arrival_airport: str,
    next_departure_airport: str,
) -> int:
    """Return the minimum allowed split-boundary connection time.

    Args:
        arrival_airport: Arrival airport code for the current leg.
        next_departure_airport: Departure airport code for the following leg.

    Returns:
        int: The minimum allowed split-boundary connection time.
    """
    arrived = str(arrival_airport or "").strip().upper()
    departed = str(next_departure_airport or "").strip().upper()
    if arrived and departed and arrived == departed:
        return MIN_SPLIT_CONNECTION_SAME_AIRPORT_SECONDS
    return MIN_SPLIT_CONNECTION_CROSS_AIRPORT_SECONDS


def date_only(value: Any) -> str:
    """Return the date portion of a date-time string.

    Args:
        value: Input value to process.

    Returns:
        str: The date portion of a date-time string.
    """
    return str(value or "").strip()[:10]


def parse_iso8601_duration_seconds(value: Any) -> int | None:
    """Parse an ISO 8601 duration into seconds.

    Args:
        value: Input value to process.

    Returns:
        int | None: Parsed ISO 8601 duration into seconds.
    """
    raw = str(value or "").strip().upper()
    if not raw:
        return None
    match = re.fullmatch(
        r"P(?:(?P<days>\d+)D)?(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+)S)?)?",
        raw,
    )
    if not match:
        return None
    days = int(match.group("days") or 0)
    hours = int(match.group("hours") or 0)
    minutes = int(match.group("minutes") or 0)
    seconds = int(match.group("seconds") or 0)
    return (
        (days * SECONDS_PER_DAY)
        + (hours * SECONDS_PER_HOUR)
        + (minutes * SECONDS_PER_MINUTE)
        + seconds
    )


def parse_duration_text_seconds(value: Any) -> int | None:
    """Parse a duration string into seconds.

    Args:
        value: Input value to process.

    Returns:
        int | None: Parsed duration string into seconds.
    """
    raw = str(value or "").strip().lower()
    if not raw:
        return None
    if raw.isdigit():
        return int(raw) * 60
    hours = 0
    minutes = 0
    hour_match = re.search(r"(\d+)\s*h", raw)
    minute_match = re.search(r"(\d+)\s*m", raw)
    if hour_match:
        hours = int(hour_match.group(1))
    if minute_match:
        minutes = int(minute_match.group(1))
    if hours == 0 and minutes == 0:
        return None
    return (hours * SECONDS_PER_HOUR) + (minutes * SECONDS_PER_MINUTE)


def parse_money_amount_int(value: Any) -> int | None:
    """Parse a money amount into an integer value.

    Args:
        value: Input value to process.

    Returns:
        int | None: Parsed money amount into an integer value.
    """
    if value in (None, ""):
        return None
    if isinstance(value, int | float):
        try:
            return int(round(float(value)))
        except (TypeError, ValueError):
            return None
    raw = str(value).strip()
    if not raw:
        return None
    cleaned = re.sub(r"[^0-9.,]", "", raw)
    if not cleaned:
        return None
    if "," in cleaned and "." in cleaned:
        if cleaned.rfind(",") > cleaned.rfind("."):
            cleaned = cleaned.replace(".", "").replace(",", ".")
        else:
            cleaned = cleaned.replace(",", "")
    elif "," in cleaned:
        if cleaned.count(",") > 1:
            cleaned = cleaned.replace(",", "")
        else:
            cleaned = cleaned.replace(",", ".")
    try:
        return int(round(float(cleaned)))
    except ValueError:
        return None


def _get_fx_rates(base_currency: str) -> dict[str, float] | None:
    """Get fx rates.

    Args:
        base_currency: Base currency.

    Returns:
        dict[str, float] | None: Get fx rates.
    """
    normalized_base = str(base_currency or "").strip().upper()
    if not normalized_base:
        return None
    now = time.time()
    with _FX_CACHE_LOCK:
        cached = _FX_RATE_CACHE.get(normalized_base)
        if cached and now < cached[0]:
            return dict(cached[1])
    try:
        response = requests.get(
            f"https://open.er-api.com/v6/latest/{normalized_base}",
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
    except Exception:
        return None

    if not isinstance(payload, dict):
        return None
    if str(payload.get("result") or "").lower() != "success":
        return None
    rates = payload.get("rates")
    if not isinstance(rates, dict):
        return None
    normalized_rates: dict[str, float] = {}
    for code, raw_rate in rates.items():
        normalized_code = str(code or "").strip().upper()
        if not normalized_code:
            continue
        try:
            rate = float(raw_rate)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(rate) or rate <= 0:
            continue
        normalized_rates[normalized_code] = rate
    if not normalized_rates:
        return None
    with _FX_CACHE_LOCK:
        _FX_RATE_CACHE[normalized_base] = (now + _FX_CACHE_TTL_SECONDS, dict(normalized_rates))
    return normalized_rates


def convert_currency_amount(
    amount: int | float | None,
    from_currency: str,
    to_currency: str,
) -> int | None:
    """Convert an amount between currencies.

    Args:
        amount: Numeric amount to convert or format.
        from_currency: From currency.
        to_currency: To currency.

    Returns:
        int | None: Converted amount between currencies.
    """
    if amount is None:
        return None
    try:
        numeric_amount = float(amount)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(numeric_amount):
        return None

    source = str(from_currency or "").strip().upper()
    target = str(to_currency or "").strip().upper()
    if not source or not target:
        return None
    if source == target:
        return int(round(numeric_amount))
    rates = _get_fx_rates(source)
    if not rates:
        return None
    rate = rates.get(target)
    if rate is None:
        return None
    return int(round(numeric_amount * rate))


def parse_datetime_guess(value: Any) -> str | None:
    """Parse a date-time value using the supported heuristics.

    Args:
        value: Input value to process.

    Returns:
        str | None: Parsed date-time value using the supported heuristics.
    """
    raw = str(value or "").strip()
    if not raw:
        return None
    if "T" in raw:
        parsed = parse_local_datetime(raw)
        if parsed:
            return parsed.isoformat(timespec="seconds")
    formats = (
        "%Y-%m-%d %I:%M %p",
        "%Y-%m-%d %H:%M",
        "%b %d, %Y %I:%M %p",
        "%a, %b %d, %Y %I:%M %p",
        "%Y/%m/%d %H:%M",
    )
    for fmt in formats:
        try:
            return dt.datetime.strptime(raw, fmt).isoformat(timespec="seconds")
        except ValueError:
            continue
    return None


def parse_google_flights_text_datetime(value: Any, date_hint_iso: str) -> str | None:
    """Parse a Google Flights text date-time value.

    Args:
        value: Input value to process.
        date_hint_iso: Date hint iso.

    Returns:
        str | None: Parsed Google Flights text date-time value.
    """
    raw = str(value or "").strip()
    if not raw:
        return None
    hint = date_only(date_hint_iso)
    try:
        hint_date = dt.date.fromisoformat(hint)
    except ValueError:
        hint_date = dt.date.today()

    cleaned = raw.replace("+1", "").replace("+2", "").strip()
    for fmt in ("%I:%M %p on %a, %b %d", "%I:%M %p on %b %d"):
        try:
            parsed = dt.datetime.strptime(cleaned, fmt)
        except ValueError:
            continue
        normalized = dt.datetime(
            year=hint_date.year,
            month=parsed.month,
            day=parsed.day,
            hour=parsed.hour,
            minute=parsed.minute,
        )
        return normalized.isoformat(timespec="seconds")
    return None


def build_comparison_links(
    origin: str,
    destination: str,
    depart_date: str,
    return_date: str,
    adults: int = 1,
    max_stops_per_leg: int | None = None,
    currency: str = "RON",
) -> dict[str, str]:
    """Build comparison links for external flight sites.

    Args:
        origin: Origin code for the operation.
        destination: Destination airport code for the request.
        depart_date: Date for depart.
        return_date: Date for return.
        adults: Number of adult travelers.
        max_stops_per_leg: Max stops per leg.
        currency: Currency code for pricing output.

    Returns:
        dict[str, str]: Comparison links for external flight sites.
    """
    orig = str(origin or "").strip().upper()
    dest = str(destination or "").strip().upper()
    dep = date_only(depart_date)
    ret = date_only(return_date)
    if not (orig and dest and dep and ret):
        return {}

    adults_count = max(1, int(adults or 1))
    currency_code = str(currency or "RON").strip().upper() or "RON"
    stops_hint = ""
    if max_stops_per_leg is not None:
        if int(max_stops_per_leg) <= 0:
            stops_hint = " nonstop"
        elif int(max_stops_per_leg) == 1:
            stops_hint = " with up to 1 stop"
        elif int(max_stops_per_leg) == 2:
            stops_hint = " with up to 2 stops"
    google_query = (
        f"Flights to {dest} from {orig} on {dep} through {ret} "
        f"with {adults_count} {'adult' if adults_count == 1 else 'adults'}{stops_hint}"
    )

    skyscanner_params = {
        "adults": adults_count,
        "adultsv2": adults_count,
        "cabinclass": "economy",
        "rtn": 1,
        "currency": currency_code,
        "preferdirects": "true" if max_stops_per_leg == 0 else "false",
        "outboundaltsenabled": "false",
        "inboundaltsenabled": "false",
    }

    kayak_params = {
        "sort": "bestflight_a",
        "adults": adults_count,
        "currency": currency_code,
    }
    momondo_params = {
        "sort": "bestflight_a",
        "adults": adults_count,
        "currency": currency_code,
    }
    if max_stops_per_leg == 0:
        kayak_params["stops"] = "0"
        momondo_params["stops"] = "0"

    return {
        "google_flights": (
            "https://www.google.com/travel/flights?"
            f"{urlencode({'q': google_query, 'hl': 'en', 'curr': currency_code})}"
        ),
        "skyscanner": (
            "https://www.skyscanner.com/transport/flights/"
            f"{orig.lower()}/{dest.lower()}/{dep.replace('-', '')}/{ret.replace('-', '')}/"
            f"?{urlencode(skyscanner_params)}"
        ),
        "kayak": (
            f"https://www.kayak.com/flights/{orig}-{dest}/{dep}/{ret}?{urlencode(kayak_params)}"
        ),
        "momondo": (
            f"https://www.momondo.com/flight-search/{orig}-{dest}/{dep}/{ret}?"
            f"{urlencode(momondo_params)}"
        ),
        "kiwi_search": (
            f"https://www.kiwi.com/en/search/results/{orig.lower()}/{dest.lower()}/{dep}/{ret}/"
        ),
    }


def leg_endpoints_from_segments(
    segments: list[dict[str, Any]] | None,
    fallback_source: str,
    fallback_destination: str,
) -> tuple[str, str]:
    """Return the origin and destination for a segment list.

    Args:
        segments: Mapping of segments.
        fallback_source: Fallback source airport code to use when segments are missing.
        fallback_destination: Fallback destination airport code to use when segments are missing.

    Returns:
        tuple[str, str]: The origin and destination for a segment list.
    """
    if not segments:
        return fallback_source, fallback_destination
    source = str(segments[0].get("from") or fallback_source).strip().upper()
    destination = str(segments[-1].get("to") or fallback_destination).strip().upper()
    return source or fallback_source, destination or fallback_destination


def transfer_events_from_segments(segments: list[dict[str, Any]] | None) -> int:
    """Build transfer events from normalized segments.

    Args:
        segments: Mapping of segments.

    Returns:
        int: Transfer events from normalized segments.
    """
    if not segments:
        return 0

    total = 0
    for idx in range(len(segments) - 1):
        total += 1
        arrived = str(segments[idx].get("to") or "").strip().upper()
        next_departure = str(segments[idx + 1].get("from") or "").strip().upper()
        if arrived and next_departure and arrived != next_departure:
            total += 1
    return total


def boundary_transfer_events(arrival_airport: str, next_departure_airport: str) -> int:
    """Build transfer events for itinerary boundaries.

    Args:
        arrival_airport: Arrival airport code for the current leg.
        next_departure_airport: Departure airport code for the following leg.

    Returns:
        int: Transfer events for itinerary boundaries.
    """
    events = 1
    arrived = str(arrival_airport or "").strip().upper()
    next_departure = str(next_departure_airport or "").strip().upper()
    if arrived and next_departure and arrived != next_departure:
        events += 1
    return events
