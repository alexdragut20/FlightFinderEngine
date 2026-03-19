from __future__ import annotations

import builtins
import itertools
import json
import logging
from pathlib import Path
from types import SimpleNamespace

import pytest
import requests

from src.exceptions import ProviderNoResultError
from src.providers import google_flights as google_flights_module
from src.providers.amadeus import AmadeusClient
from src.providers.google_flights import GoogleFlightsLocalClient
from src.providers.kayak import KayakScrapeClient, MomondoScrapeClient
from src.providers.serpapi import SerpApiGoogleFlightsClient
from src.providers.skyscanner import SkyscannerScrapeClient
from src.services.progress import SearchProgressTracker
from src.utils import logging as logging_utils


class _FakeResponse:
    def __init__(
        self,
        payload: object,
        *,
        text: str = "",
        status_code: int = 200,
        headers: dict[str, str] | None = None,
        url: str = "https://example.test/path",
    ) -> None:
        self._payload = payload
        self.text = text
        self.status_code = status_code
        self.headers = headers or {}
        self.url = url

    def json(self) -> object:
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


class _FakeSession:
    def __init__(
        self,
        *,
        get_responses: list[_FakeResponse] | None = None,
        post_responses: list[_FakeResponse] | None = None,
    ) -> None:
        self.get_responses = list(get_responses or [])
        self.post_responses = list(post_responses or [])
        self.headers: dict[str, str] = {}
        self.get_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
        self.post_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def get(self, *_args: object, **_kwargs: object) -> _FakeResponse:
        self.get_calls.append((_args, dict(_kwargs)))
        return self.get_responses.pop(0)

    def post(self, *_args: object, **_kwargs: object) -> _FakeResponse:
        self.post_calls.append((_args, dict(_kwargs)))
        return self.post_responses.pop(0)


def test_logging_utils_sanitizes_and_logs_without_crashing(monkeypatch, tmp_path: Path) -> None:
    class _ExplodingLogger:
        def log(self, *_args: object, **_kwargs: object) -> None:
            raise RuntimeError("logger failed")

    payload = {
        "token": "secret",
        "nested": [{"authorization": "abc"}],
        "tuple_values": ("x", "y"),
        "long": "a" * 9000,
    }

    monkeypatch.setattr(logging_utils, "_PROVIDER_RESPONSE_CAPTURE_MAX_STRING", 12)
    cleaned = logging_utils._sanitize_debug_value(payload)
    assert cleaned["token"] == "[REDACTED]"
    assert cleaned["nested"][0]["authorization"] == "[REDACTED]"
    assert cleaned["tuple_values"] == ["x", "y"]
    assert str(cleaned["long"]).endswith("...[truncated]")

    monkeypatch.setattr(logging_utils, "ENGINE_LOGGER", _ExplodingLogger())
    logging_utils.log_event(logging.INFO, "still_safe", api_key="secret")

    monkeypatch.setattr(logging_utils, "_DEBUG_PROVIDER_RESPONSES", True)
    monkeypatch.setattr(logging_utils, "_PROVIDER_RESPONSE_CAPTURE_TARGETS", {"amadeus"})
    monkeypatch.setattr(logging_utils, "_PROVIDER_RESPONSE_CAPTURE_MAX_FILES", 0)
    monkeypatch.setattr(logging_utils, "_PROVIDER_RESPONSE_CAPTURE_TOTAL", 0)
    monkeypatch.setattr(logging_utils, "_PROVIDER_RESPONSE_CAPTURE_SYNCED", False)
    monkeypatch.setattr(logging_utils, "_PROVIDER_RESPONSE_CAPTURE_SEQ", itertools.count(1))
    monkeypatch.setattr(logging_utils, "RESPONSES_DIR", tmp_path)

    logging_utils.capture_provider_response(
        "amadeus", "offer scan", {"token": "secret"}, {"ok": True}
    )

    files = sorted(tmp_path.glob("*.json"))
    assert len(files) == 1
    saved = json.loads(files[0].read_text(encoding="utf-8"))
    assert saved["request"]["token"] == "[REDACTED]"
    assert saved["provider"] == "amadeus"


def test_logging_utils_open_fd_count_uses_cache_and_fallbacks(monkeypatch) -> None:
    times = iter([10.0, 10.0, 10.2, 11.5, 11.5])
    calls: list[str] = []

    def fake_time() -> float:
        return next(times)

    def fake_listdir(path: str) -> list[str]:
        calls.append(path)
        if path == "/dev/fd":
            raise OSError("missing")
        return ["0", "1", "2"]

    monkeypatch.setattr(logging_utils.time, "time", fake_time)
    monkeypatch.setattr(logging_utils.os, "listdir", fake_listdir)
    monkeypatch.setattr(logging_utils, "_FD_COUNT_CACHE_VALUE", None)
    monkeypatch.setattr(logging_utils, "_FD_COUNT_CACHE_TS", 0.0)

    first = logging_utils._open_file_descriptor_count()
    second = logging_utils._open_file_descriptor_count()
    third = logging_utils._open_file_descriptor_count()

    assert first == 3
    assert second == 3
    assert third == 3
    assert calls.count("/proc/self/fd") == 1


def test_progress_tracker_helper_paths_cover_failures_and_totals() -> None:
    tracker = SearchProgressTracker("job-extra")
    tracker.start_phase("setup", total=0, detail="Preparing.")
    tracker.add_phase_total("setup", total_increment=2, detail="Added work.")
    tracker.advance_phase("setup", step=1)
    tracker.complete_phase("setup")
    tracker.mark_failed("boom")

    snapshot = tracker.snapshot()
    assert snapshot["status"] == "failed"
    assert snapshot["error"] == "boom"
    assert snapshot["phase"] == "setup"
    assert snapshot["events"][-1]["message"] == "Search failed: boom"

    with pytest.raises(ValueError):
        tracker._phase("unknown")


def test_google_flights_helper_paths_cover_error_and_candidate_filters(monkeypatch) -> None:
    client = GoogleFlightsLocalClient(fetch_mode="weird")
    assert client._fetch_mode == "common"
    assert client._carrier_from_name("") == ("GF", "Google Flights")
    assert client._carrier_from_name("Royal Jordanian, codeshare") == ("ROY", "Royal Jordanian")
    assert client._flight_stops("bad") == 0

    no_price = client._flight_to_oneway_candidate(
        source="OTP",
        destination="BKK",
        departure_iso="2026-04-25",
        currency="RON",
        flight=SimpleNamespace(price=None, stops=0, duration="2h", name="Carrier"),
        booking_url="https://google.example",
        max_stops_per_leg=1,
    )
    too_many_stops = client._flight_to_oneway_candidate(
        source="OTP",
        destination="BKK",
        departure_iso="2026-04-25",
        currency="RON",
        flight=SimpleNamespace(price=100, stops=3, duration="2h", name="Carrier"),
        booking_url="https://google.example",
        max_stops_per_leg=1,
    )
    assert no_price is None
    assert too_many_stops is None

    monkeypatch.setattr(client, "_ensure_fast_flights", lambda: True)
    client._FlightData = lambda **kwargs: kwargs
    client._Passengers = lambda **kwargs: kwargs
    client._get_flights_fn = lambda **kwargs: (_ for _ in ()).throw(
        RuntimeError("No flights found")
    )
    with pytest.raises(ProviderNoResultError):
        client._fetch_flights(
            source="OTP",
            destination="BKK",
            date_iso="2026-04-25",
            currency="RON",
            adults=2,
            max_stops_per_leg=1,
        )

    monkeypatch.setattr(client, "_ensure_fast_flights", lambda: False)
    assert client._fetch_mode_ready("common") is False

    local_gate_client = GoogleFlightsLocalClient(fetch_mode="local")
    monkeypatch.setattr(local_gate_client, "_ensure_fast_flights", lambda: True)
    monkeypatch.setattr(google_flights_module, "ALLOW_PLAYWRIGHT_PROVIDERS", False)
    assert local_gate_client._fetch_mode_ready("local") is False
    assert "ALLOW_PLAYWRIGHT_PROVIDERS=1" in local_gate_client._fast_flights_error

    playwright_client = GoogleFlightsLocalClient(fetch_mode="local")
    monkeypatch.setattr(playwright_client, "_ensure_fast_flights", lambda: True)
    monkeypatch.setattr(google_flights_module, "ALLOW_PLAYWRIGHT_PROVIDERS", True)
    original_import = builtins.__import__

    def fake_import(name: str, *args: object, **kwargs: object):  # type: ignore[no-untyped-def]
        if name == "playwright.async_api":
            raise ImportError("playwright missing")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    assert playwright_client._fetch_mode_ready("local") is False
    assert "playwright is required" in playwright_client._fast_flights_error

    ready_local_client = GoogleFlightsLocalClient(fetch_mode="local")
    monkeypatch.setattr(ready_local_client, "_ensure_fast_flights", lambda: True)

    def fake_import_success(name: str, *args: object, **kwargs: object):  # type: ignore[no-untyped-def]
        if name == "playwright.async_api":
            return object()
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import_success)
    assert ready_local_client._fetch_mode_ready("local") is True

    fallback_client = GoogleFlightsLocalClient(fetch_mode="common")
    monkeypatch.setattr(fallback_client, "_ensure_fast_flights", lambda: True)
    monkeypatch.setattr(google_flights_module, "ALLOW_PLAYWRIGHT_PROVIDERS", True)
    tried_modes: list[str] = []
    monkeypatch.setattr(
        fallback_client,
        "_fetch_mode_ready",
        lambda fetch_mode: fetch_mode == "common",
    )
    monkeypatch.setattr(
        fallback_client,
        "_fetch_flights_for_mode",
        lambda **kwargs: tried_modes.append(str(kwargs["fetch_mode"]))
        or [SimpleNamespace(price=100, stops=0, duration="2h", name="Carrier")],
    )
    assert (
        len(
            fallback_client._fetch_flights(
                source="OTP",
                destination="BKK",
                date_iso="2026-04-25",
                currency="RON",
                adults=1,
                max_stops_per_leg=1,
            )
        )
        == 1
    )
    assert tried_modes == ["common"]

    local_fallback_client = GoogleFlightsLocalClient(fetch_mode="local")
    monkeypatch.setattr(local_fallback_client, "_ensure_fast_flights", lambda: True)
    local_tried_modes: list[str] = []
    monkeypatch.setattr(
        local_fallback_client,
        "_fetch_mode_ready",
        lambda fetch_mode: fetch_mode == "common",
    )
    monkeypatch.setattr(
        local_fallback_client,
        "_fetch_flights_for_mode",
        lambda **kwargs: local_tried_modes.append(str(kwargs["fetch_mode"]))
        or [SimpleNamespace(price=100, stops=0, duration="2h", name="Carrier")],
    )
    assert (
        len(
            local_fallback_client._fetch_flights(
                source="OTP",
                destination="BKK",
                date_iso="2026-04-25",
                currency="RON",
                adults=1,
                max_stops_per_leg=1,
            )
        )
        == 1
    )
    assert local_tried_modes == ["common"]

    no_mode_client = GoogleFlightsLocalClient(fetch_mode="common")
    monkeypatch.setattr(no_mode_client, "_ensure_fast_flights", lambda: True)
    monkeypatch.setattr(no_mode_client, "_fetch_mode_ready", lambda _mode: False)
    with pytest.raises(ProviderNoResultError):
        no_mode_client._fetch_flights(
            source="OTP",
            destination="BKK",
            date_iso="2026-04-25",
            currency="RON",
            adults=1,
            max_stops_per_leg=1,
        )

    monkeypatch.setattr(client, "is_configured", lambda: False)
    assert (
        client.get_best_return(
            source="OTP",
            destination="BKK",
            outbound_iso="2026-04-25",
            inbound_iso="2026-05-05",
            currency="RON",
            max_stops_per_leg=1,
            adults=1,
            hand_bags=0,
            hold_bags=0,
        )
        is None
    )


def test_amadeus_helper_paths_cover_static_methods_and_payload_errors(monkeypatch) -> None:
    assert AmadeusClient._safe_json(_FakeResponse(ValueError("bad"))) == {}
    assert AmadeusClient._safe_json(_FakeResponse(["not-a-dict"])) == {}
    assert AmadeusClient._error_detail({"errors": [{"detail": "missing"}]}) == "missing"
    assert AmadeusClient._error_detail({"errors": ["boom"]}) == "boom"
    assert AmadeusClient._is_no_result_error(400, "No flights found")
    assert not AmadeusClient._is_no_result_error(500, "No flights found")
    assert AmadeusClient._amount_to_int("12.6") == 13
    assert AmadeusClient._amount_to_int("bad") is None
    assert AmadeusClient._format_price(120, "EUR") == "120 EUR"
    assert AmadeusClient._format_price(None, "EUR") is None
    assert AmadeusClient._parse_segments(None) == []
    assert AmadeusClient._duration_seconds({"duration": "PT2H30M"}) == 9000

    client = AmadeusClient(client_id="id", client_secret="secret", base_url="https://amadeus.test")
    session = _FakeSession(
        get_responses=[
            _FakeResponse({"errors": [{"detail": "No flight offers found"}]}, status_code=400)
        ]
    )
    monkeypatch.setattr(client, "_session", lambda: session)
    monkeypatch.setattr(client, "_fetch_token", lambda: "token")
    monkeypatch.setattr(
        "src.providers.amadeus._capture_provider_response",
        lambda *args, **kwargs: None,
    )

    with pytest.raises(ProviderNoResultError):
        client._get("/v2/shopping/flight-offers", {"originLocationCode": "OTP"})


def test_serpapi_helper_paths_cover_payload_parsing_and_errors(monkeypatch) -> None:
    client = SerpApiGoogleFlightsClient(api_key="key", search_url="https://serpapi.example")

    session = _FakeSession(get_responses=[_FakeResponse({"error": "quota"}, status_code=200)])
    monkeypatch.setattr(client, "_session", lambda: session)
    monkeypatch.setattr(
        "src.providers.serpapi._capture_provider_response",
        lambda *args, **kwargs: None,
    )
    with pytest.raises(RuntimeError, match="quota"):
        client._search({"departure_id": "OTP"})

    options = client._iter_options(
        {"best_flights": [{"price": 1}], "other_flights": ["bad", {"price": 2}]}
    )
    assert len(options) == 2
    assert client._option_duration_seconds({"total_duration": 180}) == 10800
    assert client._option_duration_seconds({"duration": "2h 10m"}) == 7800
    assert client._option_price({"total_price": "120 EUR"}) == 120
    assert (
        client._booking_url({"search_metadata": {"raw_html_file": "https://google.example"}})
        == "https://google.example"
    )

    payload = {
        "search_metadata": {"google_flights_url": "https://google.example/out"},
        "best_flights": [
            {
                "price": 500,
                "total_duration": "2h 10m",
                "flights": [
                    {
                        "departure_airport": {
                            "id": "OTP",
                            "name": "OTP",
                            "time": "2026-03-10 08:00",
                        },
                        "arrival_airport": {"id": "IST", "name": "IST", "time": "2026-03-10 09:00"},
                        "airline": "TK",
                    },
                    {
                        "departure_airport": {
                            "id": "IST",
                            "name": "IST",
                            "time": "2026-03-10 10:00",
                        },
                        "arrival_airport": {"id": "MGA", "name": "MGA", "time": "2026-03-10 13:00"},
                        "airline": "TK",
                    },
                ],
            }
        ],
    }
    monkeypatch.setattr(client, "_search", lambda params: payload)
    best = client.get_best_oneway(
        "OTP", "MGA", "2026-03-10", "EUR", 1, 1, 0, 0, max_connection_layover_seconds=1800
    )
    assert best is None


def test_kayak_helper_paths_cover_bootstrap_polling_and_static_helpers(monkeypatch) -> None:
    client = KayakScrapeClient(host="www.kayak.com", poll_rounds=2)
    momondo = MomondoScrapeClient(host="www.momondo.com", poll_rounds=2)
    assert "www.kayak.com" in client._search_page_url("otp", "mga", "2026-03-10", None, "eur", 2)
    assert "/flight-search/" in momondo._search_page_url("otp", "mga", "2026-03-10", None, "eur", 2)
    assert len(client._build_legs_payload("otp", "mga", "2026-03-10", "2026-03-24")) == 2
    assert KayakScrapeClient._safe_json_from_response(_FakeResponse(ValueError("bad"))) == {}
    assert (
        KayakScrapeClient._extract_error_detail(
            {"errors": [{"code": "NO_RESULTS", "description": "No results"}]}
        )
        == "NO_RESULTS: No results"
    )
    assert KayakScrapeClient._first_money_value([None, "bad", "123"]) == 123
    assert (
        KayakScrapeClient._booking_explicit_total_amount({"displayPrice": {"totalPrice": "456"}})
        == 456
    )
    assert KayakScrapeClient._booking_price_per_person_flag({"priceMode": "perPassenger"}) is True
    assert KayakScrapeClient._booking_price_per_person_flag({"priceMode": "total"}) is False
    assert KayakScrapeClient._booking_price_per_person_flag({"nested": {"perPerson": True}}) is True

    with pytest.raises(RuntimeError, match="missing"):
        client._extract_bootstrap("<html></html>")
    with pytest.raises(RuntimeError, match="parse failed"):
        client._extract_bootstrap('<script id="jsonData_R9DataStorage">{bad}</script>')
    with pytest.raises(RuntimeError, match="formtoken is missing"):
        client._extract_bootstrap(
            '<script id="jsonData_R9DataStorage">{"serverData":{"global":{}}}</script>'
        )

    csrf_token, bootstrap = client._extract_bootstrap(
        '<script id="jsonData_R9DataStorage">{"serverData":{"global":{"formtoken":"abc"}}}</script>'
    )
    assert csrf_token == "abc"
    assert isinstance(bootstrap, dict)

    session = _FakeSession(
        get_responses=[
            _FakeResponse(
                {},
                text='<script id="jsonData_R9DataStorage">{"serverData":{"global":{"formtoken":"abc"}}}</script>',
                url="https://www.kayak.com/flights/OTP-MGA/2026-03-10",
            )
        ]
    )
    monkeypatch.setattr(client, "_session", lambda: session)
    polls = iter(
        [
            {"searchId": "sid-1", "status": "first-phase", "results": []},
            {"searchId": "sid-1", "status": "complete", "results": [{"type": "core"}]},
        ]
    )
    monkeypatch.setattr(client, "_post_poll", lambda **kwargs: next(polls))
    payload = client._search_payload("OTP", "MGA", "2026-03-10", None, "RON", 2)
    assert payload["status"] == "complete"

    result = {
        "bookingOptions": [{"displayPrice": {"price": "100", "currency": "USD"}}],
        "shareableUrl": "/share/deal",
    }
    best_booking, amount, source_currency, assumption = client._best_booking_option(
        result, adults=2
    )
    assert best_booking is not None
    assert amount == 200
    assert source_currency == "USD"
    assert assumption == "per_person_scaled"

    legs_map = {"LEG": {"segments": ["SEG-2"]}}
    assert client._segment_ids_for_leg({"id": "LEG"}, legs_map) == ["SEG-2"]
    assert client._segment_ids_for_leg({"segments": [{"id": "SEG-1"}]}, legs_map) == ["SEG-1"]
    assert client._segment_entry("missing", {}, {}, {}) is None
    segment_entry = client._segment_entry(
        "SEG-1",
        {
            "SEG-1": {
                "origin": "OTP",
                "destination": "MGA",
                "airline": "TK",
                "departure": "2026-03-10T08:00:00",
                "arrival": "2026-03-10T10:00:00",
            }
        },
        {"OTP": {"displayName": "Bucharest"}, "MGA": {"fullDisplayName": "Managua"}},
        {"TK": {"name": "Turkish Airlines"}},
    )
    assert segment_entry is not None
    assert segment_entry["from_name"] == "Bucharest"
    assert segment_entry["to_name"] == "Managua"
    assert client._leg_duration_seconds({}, {}, [segment_entry]) == 7200
    assert client._normalize_price(100, "USD", "USD") == (100, "USD")
    monkeypatch.setattr(
        "src.providers.kayak.convert_currency_amount",
        lambda amount, source, target: None,
    )
    assert client._normalize_price(100, "USD", "RON") == (100, "USD")

    session = _FakeSession(
        post_responses=[_FakeResponse({"errors": [{"code": "NO_RESULTS"}]}, status_code=200)]
    )
    monkeypatch.setattr(client, "_session", lambda: session)
    monkeypatch.setattr(
        client, "_post_poll", KayakScrapeClient._post_poll.__get__(client, KayakScrapeClient)
    )
    monkeypatch.setattr(
        "src.providers.kayak._capture_provider_response",
        lambda *args, **kwargs: None,
    )
    with pytest.raises(ProviderNoResultError):
        client._post_poll("https://www.kayak.com", "csrf", {"legs": []})


def test_skyscanner_helper_paths_cover_regex_and_runtime_errors(monkeypatch) -> None:
    client = SkyscannerScrapeClient(
        host="www.skyscanner.com",
        host_candidates=["www.skyscanner.net"],
        http_retries=1,
        playwright_fallback=False,
    )
    assert (
        client._replace_url_host("/transport/flights", "example.com")
        == "https://example.com/transport/flights"
    )
    assert "adultsv2=2" in client._search_page_url(
        "OTP", "MGA", "2026-03-10", "2026-03-24", "eur", 2
    )
    assert client._is_bot_blocked_response("", "https://example.com/captcha", 200)
    assert client._is_bot_blocked_response("Verify you are human", "https://example.com", 200)
    assert client._extract_best_price('"lowestPrice":"456"') == 456
    payloads = client._extract_json_script_payloads(
        '<script>{"offers":[{"providerName":"OTA","rawPrice":444}]}</script>'
        '<script>window.__DATA__ = {"offers":[{"providerName":"OTA2","rawPrice":445}]};</script>'
    )
    assert len(payloads) == 2
    regex_offers = client._extract_offer_options_regex('"providerName":"OTA","rawPrice":"333"')
    assert regex_offers[0]["price"] == 333

    node = {
        "agent": {"name": "Vayama"},
        "pricing": {"rawPrice": 444, "currency": "EUR", "deeplink": "https://book.example"},
    }
    offer = client._offer_from_node(node)
    assert offer is not None
    assert offer["provider"] == "Vayama"
    assert offer["currency"] == "EUR"
    assert offer["booking_url"] == "https://book.example"
    assert client._extract_stops_hint('"stops":2', 3) == 2
    assert client._synthetic_segments("OTP", "MGA")[0]["carrier"] == "SKY"

    monkeypatch.setattr(
        client,
        "_http_fetch_search_html",
        lambda url, attempt_idx=0: (_ for _ in ()).throw(requests.RequestException("network down")),
    )
    with pytest.raises(RuntimeError, match="network down"):
        client._fetch_search_html("https://www.skyscanner.com/transport/flights/otp/mga/20260310")

    monkeypatch.setattr(
        client,
        "_fetch_search_html",
        lambda url: ("<html>No fares</html>", "https://www.skyscanner.com"),
    )
    with pytest.raises(ProviderNoResultError):
        client.get_best_oneway("OTP", "MGA", "2026-03-10", "RON", 1, 1, 0, 0)

    monkeypatch.setattr(
        client,
        "_fetch_search_html",
        lambda url: ('{"rawPrice": 200, "stops": 3}', "https://www.skyscanner.com"),
    )
    monkeypatch.setattr(client, "_extract_best_price", lambda html: 200)
    monkeypatch.setattr(
        client, "_extract_stops_hint", lambda html, max_stops_per_leg: max_stops_per_leg + 1
    )
    with pytest.raises(ProviderNoResultError):
        client.get_best_return("OTP", "MGA", "2026-03-10", "2026-03-24", "RON", 1, 1, 0, 0)


def test_skyscanner_playwright_runtime_paths_cover_gate_cooldown_and_shutdown(
    monkeypatch,
) -> None:
    client = SkyscannerScrapeClient(
        host="www.skyscanner.com",
        host_candidates=["www.skyscanner.net"],
        http_retries=1,
        playwright_fallback=True,
    )

    class _Gate:
        def __init__(self, acquired: bool) -> None:
            self.acquired = acquired
            self.released = 0

        def acquire(self, timeout: float | None = None) -> bool:
            return self.acquired

        def release(self) -> None:
            self.released += 1

    busy_gate = _Gate(False)
    monkeypatch.setattr(SkyscannerScrapeClient, "_PLAYWRIGHT_GATE", busy_gate)
    monkeypatch.setattr(client, "_playwright_cooldown_remaining_seconds", lambda: 0)
    with pytest.raises(ProviderNoResultError, match="busy"):
        client._fetch_search_html_playwright("https://www.skyscanner.com")
    assert busy_gate.released == 0

    monkeypatch.setattr(client, "_playwright_cooldown_remaining_seconds", lambda: 7)
    with pytest.raises(ProviderNoResultError, match="retry in ~7s"):
        client._fetch_search_html_playwright("https://www.skyscanner.com")

    class _Browser:
        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

    class _Playwright:
        def __init__(self) -> None:
            self.stopped = False

        def stop(self) -> None:
            self.stopped = True

    browser = _Browser()
    playwright = _Playwright()
    monkeypatch.setattr(SkyscannerScrapeClient, "_PLAYWRIGHT_RUNTIME", (playwright, browser))
    SkyscannerScrapeClient._close_playwright_runtime()
    assert browser.closed is True
    assert playwright.stopped is True


def test_kayak_helper_paths_cover_session_poll_errors_and_result_selection(monkeypatch) -> None:
    client = KayakScrapeClient(host="WWW.KAYAK.COM", poll_rounds=0)
    assert client._host == "www.kayak.com"
    assert client._poll_rounds == 1
    cached_session = client._session()
    assert client._session() is cached_session
    assert "Mozilla/5.0" in cached_session.headers["User-Agent"]

    assert KayakScrapeClient._core_results(
        {"results": ["bad", {"type": "meta"}, {"type": "core"}]}
    ) == [{"type": "core"}]
    assert KayakScrapeClient._booking_option_amount(
        {
            "displayPrice": {"price": "100", "currency": "USD", "totalPrice": "180"},
            "pricing": {"grandTotal": "200"},
        },
        adults=2,
    ) == (180, "USD", "explicit_total")
    assert KayakScrapeClient._booking_option_amount(
        {"displayPrice": {"currency": "USD"}},
        adults=2,
    ) == (None, "USD", "missing_price")
    assert KayakScrapeClient._booking_option_amount(
        {
            "displayPrice": {"price": "150", "currency": "USD"},
            "pricing": {"priceMode": "total"},
        },
        adults=2,
    ) == (150, "USD", "displayed")
    assert KayakScrapeClient._booking_price_per_person_flag({"per_person": 0}) is False
    assert KayakScrapeClient._booking_price_per_person_flag({"perTraveler": "per traveler"}) is True
    best_booking, best_amount, best_currency, best_assumption = client._best_booking_option(
        {
            "bookingOptions": [
                {"displayPrice": {"price": "bad", "currency": "USD"}},
                {"displayPrice": {"price": "200", "currency": "USD"}},
                {"displayPrice": {"price": "120", "currency": "USD"}},
            ]
        },
        adults=1,
    )
    assert best_booking is not None
    assert (best_amount, best_currency, best_assumption) == (120, "USD", "displayed")

    monkeypatch.setattr(
        "src.providers.kayak._capture_provider_response",
        lambda *args, **kwargs: None,
    )
    generic_error_session = _FakeSession(
        post_responses=[
            _FakeResponse({"errors": [{"description": "Broken poll"}]}, status_code=200)
        ]
    )
    monkeypatch.setattr(client, "_session", lambda: generic_error_session)
    with pytest.raises(RuntimeError, match="Broken poll"):
        client._post_poll("https://www.kayak.com/flights", "csrf", {"legs": []})

    no_result_session = _FakeSession(
        post_responses=[
            _FakeResponse({"errors": [{"code": "NO_RESULTS_FOUND"}]}, status_code=404),
        ]
    )
    monkeypatch.setattr(client, "_session", lambda: no_result_session)
    with pytest.raises(ProviderNoResultError):
        client._post_poll("https://www.kayak.com/flights", "csrf", {"legs": []})

    success_session = _FakeSession(post_responses=[_FakeResponse({"status": "complete"})])
    monkeypatch.setattr(client, "_session", lambda: success_session)
    assert client._post_poll("https://www.kayak.com/flights", "csrf", {"legs": []}) == {
        "status": "complete"
    }
    assert client._segment_ids_for_leg(
        {"id": "LEG-1"},
        {"LEG-1": {"segments": [{"id": "SEG-A"}, "SEG-B"]}},
    ) == ["SEG-A", "SEG-B"]
    assert client._segment_entry("SEG-X", {"SEG-X": "bad"}, {}, {}) is None
    assert client._normalize_price(None, "", "RON") == (None, "RON")
    monkeypatch.setattr(
        "src.providers.kayak.convert_currency_amount",
        lambda amount, source, target: (
            250 if (amount, source, target) == (100, "USD", "RON") else None
        ),
    )
    assert client._normalize_price(100, "USD", "RON") == (250, "RON")

    payload = {
        "results": [
            {
                "type": "core",
                "legs": [{"segments": ["SEG-LONG-1", "SEG-LONG-2"]}],
                "bookingOptions": [
                    {
                        "displayPrice": {"price": "100", "currency": "USD"},
                        "providerCode": "",
                    }
                ],
                "shareableUrl": "/share/too-long",
            },
            {
                "type": "core",
                "legs": [{"segments": ["SEG-GOOD"]}],
                "bookingOptions": [
                    {
                        "displayPrice": {"price": "120", "currency": "USD"},
                        "providerCode": "",
                    }
                ],
                "shareableUrl": "/share/oneway",
            },
            {
                "type": "core",
                "legs": [{"segments": ["SEG-LONG-1", "SEG-LONG-2"]}],
                "bookingOptions": [
                    {
                        "displayPrice": {"price": "95", "currency": "USD"},
                        "providerCode": "OTA1",
                        "bookingUrl": {"url": "/book/return"},
                    }
                ],
                "shareableUrl": "/share/return-invalid",
            },
            {
                "type": "core",
                "legs": [{"segments": ["SEG-OUT"]}, {"segments": ["SEG-IN-1", "SEG-IN-2"]}],
                "bookingOptions": [
                    {
                        "displayPrice": {"price": "130", "currency": "USD"},
                        "providerCode": "OTA1",
                        "bookingUrl": {"url": "/book/return"},
                    }
                ],
                "shareableUrl": "/share/return",
            },
        ],
        "segments": {
            "SEG-LONG-1": {
                "origin": "OTP",
                "destination": "IST",
                "departure": "2026-03-10T08:00:00",
                "arrival": "2026-03-10T09:00:00",
                "airline": "TK",
            },
            "SEG-LONG-2": {
                "origin": "IST",
                "destination": "MGA",
                "departure": "2026-03-10T12:30:00",
                "arrival": "2026-03-10T15:00:00",
                "airline": "TK",
            },
            "SEG-GOOD": {
                "origin": "OTP",
                "destination": "MGA",
                "departure": "2026-03-10T10:00:00",
                "arrival": "2026-03-10T14:00:00",
                "airline": "TK",
            },
            "SEG-OUT": {
                "origin": "OTP",
                "destination": "MGA",
                "departure": "2026-03-10T09:00:00",
                "arrival": "2026-03-10T13:00:00",
                "airline": "TK",
            },
            "SEG-IN-1": {
                "origin": "MGA",
                "destination": "IST",
                "departure": "2026-03-24T08:00:00",
                "arrival": "2026-03-24T09:00:00",
                "airline": "TK",
            },
            "SEG-IN-2": {
                "origin": "IST",
                "destination": "OTP",
                "departure": "2026-03-24T09:35:00",
                "arrival": "2026-03-24T12:00:00",
                "airline": "TK",
            },
        },
        "airports": {
            "OTP": {"displayName": "OTP"},
            "IST": {"displayName": "IST"},
            "MGA": {"displayName": "MGA"},
        },
        "airlines": {"TK": {"name": "Turkish Airlines"}},
        "providers": {"OTA1": {"displayName": "Example OTA"}},
    }
    monkeypatch.setattr(client, "_search_payload", lambda **kwargs: payload)
    best_oneway = client.get_best_oneway(
        "OTP",
        "MGA",
        "2026-03-10",
        "USD",
        2,
        1,
        0,
        0,
        max_connection_layover_seconds=3600,
    )
    best_return = client.get_best_return(
        "OTP",
        "MGA",
        "2026-03-10",
        "2026-03-24",
        "USD",
        2,
        1,
        0,
        0,
        max_connection_layover_seconds=3600,
    )
    assert best_oneway is not None
    assert best_oneway["price"] == 120
    assert best_oneway["booking_provider"] == client.display_name
    assert str(best_oneway["booking_url"]).endswith("/share/oneway")
    assert best_return is not None
    assert best_return["price"] == 130
    assert best_return["booking_provider"] == "Example OTA"
    assert str(best_return["booking_url"]).endswith("/book/return")
    assert (
        client.get_best_return(
            "OTP",
            "MGA",
            "2026-03-10",
            "2026-03-24",
            "USD",
            0,
            1,
            0,
            0,
            max_connection_layover_seconds=3600,
        )
        is None
    )


def test_skyscanner_helper_paths_cover_cooldowns_runtime_fallbacks_and_fare_selection(
    monkeypatch,
) -> None:
    client = SkyscannerScrapeClient(
        host="WWW.SKYSCANNER.COM",
        host_candidates=["www.skyscanner.net", "www.skyscanner.com"],
        http_retries=2,
        playwright_fallback=True,
    )
    cached_session = client._session()
    assert client._session() is cached_session
    assert cached_session.headers["Cache-Control"] == "no-cache"
    http_session = _FakeSession(
        get_responses=[
            _FakeResponse(
                {},
                text="<html>ok</html>",
                status_code=202,
                url="https://final.example",
            )
        ]
    )
    client._local.session = http_session
    html, final_url, status_code = client._http_fetch_search_html(
        "https://www.skyscanner.com/path",
        attempt_idx=1,
    )
    assert (html, final_url, status_code) == ("<html>ok</html>", "https://final.example", 202)
    assert http_session.headers["User-Agent"] == client._USER_AGENTS[1]

    monkeypatch.setattr("src.providers.skyscanner.time.time", lambda: 100.0)
    monkeypatch.setattr(SkyscannerScrapeClient, "_PROVIDER_COOLDOWN_UNTIL", 0.0)
    monkeypatch.setattr(SkyscannerScrapeClient, "_PLAYWRIGHT_COOLDOWN_UNTIL", 0.0)
    SkyscannerScrapeClient._set_provider_cooldown(5)
    SkyscannerScrapeClient._set_playwright_cooldown(3)
    assert SkyscannerScrapeClient._provider_cooldown_remaining_seconds() == 5
    assert SkyscannerScrapeClient._playwright_cooldown_remaining_seconds() == 3

    runtime = (object(), object())
    monkeypatch.setattr(SkyscannerScrapeClient, "_PLAYWRIGHT_RUNTIME", runtime)
    assert SkyscannerScrapeClient._get_or_start_playwright_runtime() is runtime

    real_import = builtins.__import__

    def missing_playwright_import(
        name: str,
        globals: object | None = None,
        locals: object | None = None,
        fromlist: object = (),
        level: int = 0,
    ) -> object:
        if name == "playwright.sync_api":
            raise ImportError("missing playwright")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(SkyscannerScrapeClient, "_PLAYWRIGHT_RUNTIME", None)
    monkeypatch.setattr(builtins, "__import__", missing_playwright_import)
    with pytest.raises(ProviderNoResultError, match="Playwright fallback unavailable"):
        SkyscannerScrapeClient._get_or_start_playwright_runtime()

    class _StartedPlaywright:
        def __init__(self) -> None:
            self.chromium = SimpleNamespace(launch=lambda headless=True: "browser")

        def start(self) -> _StartedPlaywright:
            return self

    created_runtime_holder: dict[str, object] = {}

    def available_playwright_import(
        name: str,
        globals: object | None = None,
        locals: object | None = None,
        fromlist: object = (),
        level: int = 0,
    ) -> object:
        if name == "playwright.sync_api":
            return SimpleNamespace(
                sync_playwright=lambda: created_runtime_holder.setdefault(
                    "playwright",
                    _StartedPlaywright(),
                ),
                TimeoutError=RuntimeError,
            )
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", available_playwright_import)
    created_runtime = SkyscannerScrapeClient._get_or_start_playwright_runtime()
    assert created_runtime == (created_runtime_holder["playwright"], "browser")
    monkeypatch.setattr(SkyscannerScrapeClient, "_PLAYWRIGHT_RUNTIME", None)

    class _Gate:
        def __init__(self) -> None:
            self.released = 0

        def acquire(self, timeout: float | None = None) -> bool:
            return True

        def release(self) -> None:
            self.released += 1

    class _FakeTimeoutError(Exception):
        pass

    class _FakePage:
        def __init__(self) -> None:
            self.url = "https://www.skyscanner.net/transport/flights/otp/mga/20260310"
            self.closed = False

        def goto(self, *_args: object, **_kwargs: object) -> None:
            return None

        def wait_for_load_state(self, *_args: object, **_kwargs: object) -> None:
            raise _FakeTimeoutError("still polling")

        def wait_for_timeout(self, *_args: object, **_kwargs: object) -> None:
            return None

        def content(self) -> str:
            return '{"rawPrice": 321, "stops": 1}'

        def close(self) -> None:
            self.closed = True

    class _FakeContext:
        def __init__(self) -> None:
            self.closed = False

        def new_page(self) -> _FakePage:
            return _FakePage()

        def close(self) -> None:
            self.closed = True

    class _FakeBrowser:
        def __init__(self, connected: bool) -> None:
            self.connected = connected

        def is_connected(self) -> bool:
            return self.connected

        def new_context(self, **_kwargs: object) -> _FakeContext:
            return _FakeContext()

    playwright_gate = _Gate()
    monkeypatch.setattr(SkyscannerScrapeClient, "_PLAYWRIGHT_GATE", playwright_gate)
    monkeypatch.setattr(client, "_playwright_cooldown_remaining_seconds", lambda: 0)

    def fake_playwright_import(
        name: str,
        globals: object | None = None,
        locals: object | None = None,
        fromlist: object = (),
        level: int = 0,
    ) -> object:
        if name == "playwright.sync_api":
            return SimpleNamespace(TimeoutError=_FakeTimeoutError)
        return real_import(name, globals, locals, fromlist, level)

    runtimes = iter([(None, _FakeBrowser(False)), (None, _FakeBrowser(True))])
    closed_runtimes: list[str] = []
    monkeypatch.setattr(builtins, "__import__", fake_playwright_import)
    monkeypatch.setattr(client, "_get_or_start_playwright_runtime", lambda: next(runtimes))
    monkeypatch.setattr(
        client, "_close_playwright_runtime", lambda: closed_runtimes.append("closed")
    )
    html, final_url = client._fetch_search_html_playwright("https://www.skyscanner.com/path")
    assert '"rawPrice": 321' in html
    assert "skyscanner.net" in final_url
    assert closed_runtimes == ["closed"]
    assert playwright_gate.released == 1

    monkeypatch.setattr(
        client,
        "_get_or_start_playwright_runtime",
        lambda: (_ for _ in ()).throw(Exception("too many open files")),
    )
    with pytest.raises(ProviderNoResultError, match="file-descriptor exhaustion"):
        client._fetch_search_html_playwright("https://www.skyscanner.com/path")

    class _BrokenBrowser:
        def is_connected(self) -> bool:
            return True

        def new_context(self, **_kwargs: object) -> object:
            raise OSError(24, "Too many open files")

    monkeypatch.setattr(
        client, "_get_or_start_playwright_runtime", lambda: (None, _BrokenBrowser())
    )
    with pytest.raises(ProviderNoResultError, match="OS file-descriptor exhaustion"):
        client._fetch_search_html_playwright("https://www.skyscanner.com/path")

    monkeypatch.setattr(client, "_provider_cooldown_remaining_seconds", lambda: 4)
    with pytest.raises(ProviderNoResultError, match="retry in ~4s"):
        client._fetch_search_html("https://www.skyscanner.com/path")

    monkeypatch.setattr(client, "_provider_cooldown_remaining_seconds", lambda: 0)
    monkeypatch.setattr(
        client, "_http_fetch_search_html", lambda url, attempt_idx=0: ("", url, 200)
    )
    monkeypatch.setattr(
        client,
        "_fetch_search_html_playwright",
        lambda url: (_ for _ in ()).throw(ProviderNoResultError("captcha blocked")),
    )
    with pytest.raises(ProviderNoResultError, match="blocked automated scraping"):
        client._fetch_search_html("https://www.skyscanner.com/path")

    client._playwright_fallback = False
    monkeypatch.setattr(
        client,
        "_http_fetch_search_html",
        lambda url, attempt_idx=0: ("server error", url, 500),
    )
    with pytest.raises(RuntimeError, match="HTTP 500"):
        client._fetch_search_html("https://www.skyscanner.com/path")

    monkeypatch.setattr(
        client,
        "_fetch_search_html",
        lambda url: ('{"rawPrice": 321, "stops": 1}', "https://www.skyscanner.com/final"),
    )
    monkeypatch.setattr(client, "_extract_offer_options", lambda html: [])
    best_oneway = client.get_best_oneway("OTP", "MGA", "2026-03-10", "RON", 1, 1, 0, 0)
    best_return = client.get_best_return(
        "OTP",
        "MGA",
        "2026-03-10",
        "2026-03-24",
        "RON",
        1,
        1,
        0,
        0,
    )
    assert best_oneway is not None
    assert best_oneway["price"] == 321
    assert best_oneway["booking_url"] == "https://www.skyscanner.com/final"
    assert best_return is not None
    assert best_return["price"] == 321
    assert best_return["booking_provider"] == "Skyscanner"


def test_kayak_client_remaining_edge_paths_cover_search_payload_and_filtering(monkeypatch) -> None:
    client = KayakScrapeClient(host="   ", poll_rounds=9)
    assert client._host == "www.kayak.com"
    assert client._poll_rounds == 6
    assert "/2026-03-24" in client._search_page_url(
        "otp",
        "mga",
        "2026-03-10",
        "2026-03-24",
        "ron",
        2,
    )
    assert KayakScrapeClient._extract_error_detail({"errors": []}) == ""
    assert KayakScrapeClient._extract_error_detail({"errors": ["plain error"]}) == "plain error"
    assert (
        KayakScrapeClient._booking_price_per_person_flag({"": True, "mode": {"priceMode": "total"}})
        is False
    )
    assert KayakScrapeClient._booking_price_per_person_flag({"perTraveler": "total"}) is False

    class _NoRaiseResponse(_FakeResponse):
        def raise_for_status(self) -> None:
            return None

    monkeypatch.setattr(
        "src.providers.kayak._capture_provider_response",
        lambda *args, **kwargs: None,
    )

    error_session = _FakeSession(
        post_responses=[
            _NoRaiseResponse(
                {"errors": [{"description": "server broke"}]},
                status_code=500,
            )
        ]
    )
    monkeypatch.setattr(client, "_session", lambda: error_session)
    with pytest.raises(RuntimeError, match="server broke"):
        client._post_poll("https://www.kayak.com/flights", "csrf", {"legs": []})

    search_client = KayakScrapeClient(host="www.kayak.com", poll_rounds=2)
    search_session = _FakeSession(
        get_responses=[_FakeResponse({}, text="<html></html>", url="https://www.kayak.com/flights")]
    )
    monkeypatch.setattr(search_client, "_session", lambda: search_session)
    monkeypatch.setattr(search_client, "_extract_bootstrap", lambda _html: ("csrf", {}))
    monkeypatch.setattr(search_client, "_post_poll", lambda **_kwargs: {"status": "pending"})
    assert search_client._search_payload(
        source="OTP",
        destination="MGA",
        outbound_iso="2026-03-10",
        inbound_iso="2026-03-24",
        currency="RON",
        adults=2,
    ) == {"status": "pending"}

    best_option, best_amount, best_currency, best_assumption = client._best_booking_option(
        {
            "bookingOptions": [
                "bad",
                {"displayPrice": {"price": "100", "currency": "USD"}},
                {"displayPrice": {"price": "90", "currency": "USD"}},
            ]
        },
        adults=1,
    )
    assert best_option is not None
    assert (best_amount, best_currency, best_assumption) == (90, "USD", "displayed")
    assert client._segment_ids_for_leg({}, {}) == []
    assert client._segments_for_leg(
        {"segments": ["MISSING", "SEG-VALID"]},
        {},
        {"MISSING": "bad", "SEG-VALID": {"origin": "OTP", "destination": "MGA"}},
        {},
        {},
    ) == [
        {
            "from": "OTP",
            "to": "MGA",
            "from_name": "OTP",
            "to_name": "MGA",
            "depart_local": None,
            "arrive_local": None,
            "carrier": "",
            "carrier_name": "",
        }
    ]
    assert (
        client._leg_duration_seconds(
            {"id": "LEG-1"},
            {"LEG-1": {}},
            [{"depart_local": "bad", "arrive_local": "bad"}],
        )
        is None
    )
    assert client._normalize_price(100, "", "") == (100, "")
    assert (
        client.get_calendar_prices("OTP", "MGA", "2026-03-10", "2026-03-12", "RON", 1, 1, 0, 0)
        == {}
    )

    original_normalize = client._normalize_price

    def patched_normalize(amount: int | None, source_currency: str, target_currency: str):
        if amount in {96, 300}:
            return None, str(target_currency or source_currency).upper()
        return original_normalize(amount, source_currency, target_currency)

    monkeypatch.setattr(client, "_normalize_price", patched_normalize)

    payload_oneway_empty = {"results": [{"type": "meta"}]}
    payload_oneway_mixed = {
        "results": [
            {
                "type": "core",
                "legs": [{"segments": ["SEG-VALID"]}],
                "bookingOptions": [],
            },
            {
                "type": "core",
                "legs": ["bad"],
                "bookingOptions": [{"displayPrice": {"price": "105", "currency": "USD"}}],
            },
            {
                "type": "core",
                "legs": [{"segments": ["MISSING"]}],
                "bookingOptions": [{"displayPrice": {"price": "106", "currency": "USD"}}],
            },
            {
                "type": "core",
                "legs": [{"segments": ["SEG-LONG-1", "SEG-LONG-2"]}],
                "bookingOptions": [{"displayPrice": {"price": "96", "currency": "USD"}}],
            },
            {
                "type": "core",
                "legs": [{"segments": ["SEG-VALID"]}],
                "bookingOptions": [
                    {"displayPrice": {"price": "110", "currency": "USD"}, "providerCode": "OTA1"}
                ],
                "shareableUrl": "/share/oneway",
            },
        ],
        "segments": {
            "SEG-LONG-1": {
                "origin": "OTP",
                "destination": "IST",
                "departure": "2026-03-10T08:00:00",
                "arrival": "2026-03-10T09:00:00",
            },
            "SEG-LONG-2": {
                "origin": "IST",
                "destination": "MGA",
                "departure": "2026-03-10T13:30:00",
                "arrival": "2026-03-10T15:00:00",
            },
            "SEG-VALID": {
                "origin": "OTP",
                "destination": "MGA",
                "departure": "2026-03-10T10:00:00",
                "arrival": "2026-03-10T14:00:00",
            },
        },
        "airports": {
            "OTP": {"displayName": "OTP"},
            "IST": {"displayName": "IST"},
            "MGA": {"displayName": "MGA"},
        },
        "airlines": {},
        "providers": {"OTA1": {"displayName": "Example OTA"}},
    }
    payload_return_empty = {"results": []}
    payload_return_mixed = {
        "results": [
            {
                "type": "core",
                "legs": ["bad", "bad"],
                "bookingOptions": [{"displayPrice": {"price": "200", "currency": "USD"}}],
            },
            {
                "type": "core",
                "legs": [{"segments": ["SEG-OUT"]}, {"segments": ["SEG-IN"]}],
                "bookingOptions": [],
            },
            {
                "type": "core",
                "legs": [{"segments": ["SEG-OUT"]}, {"segments": ["SEG-IN"]}],
                "bookingOptions": [{"displayPrice": {"price": "300", "currency": "USD"}}],
            },
            {
                "type": "core",
                "legs": [{"segments": ["SEG-OUT"]}, {"segments": ["MISSING"]}],
                "bookingOptions": [{"displayPrice": {"price": "220", "currency": "USD"}}],
            },
            {
                "type": "core",
                "legs": [
                    {"segments": ["SEG-OUT"]},
                    {"segments": ["SEG-IN-LONG-1", "SEG-IN-LONG-2"]},
                ],
                "bookingOptions": [{"displayPrice": {"price": "230", "currency": "USD"}}],
            },
            {
                "type": "core",
                "legs": [{"segments": ["SEG-OUT"]}, {"segments": ["SEG-IN"]}],
                "bookingOptions": [
                    {"displayPrice": {"price": "240", "currency": "USD"}, "providerCode": ""}
                ],
                "shareableUrl": "/share/return",
            },
        ],
        "segments": {
            "SEG-OUT": {
                "origin": "OTP",
                "destination": "MGA",
                "departure": "2026-03-10T10:00:00",
                "arrival": "2026-03-10T14:00:00",
            },
            "SEG-IN": {
                "origin": "MGA",
                "destination": "OTP",
                "departure": "2026-03-24T10:00:00",
                "arrival": "2026-03-24T14:00:00",
            },
            "SEG-IN-LONG-1": {
                "origin": "MGA",
                "destination": "IST",
                "departure": "2026-03-24T08:00:00",
                "arrival": "2026-03-24T09:00:00",
            },
            "SEG-IN-LONG-2": {
                "origin": "IST",
                "destination": "OTP",
                "departure": "2026-03-24T13:30:00",
                "arrival": "2026-03-24T15:30:00",
            },
        },
        "airports": {
            "OTP": {"displayName": "OTP"},
            "IST": {"displayName": "IST"},
            "MGA": {"displayName": "MGA"},
        },
        "airlines": {},
        "providers": {"OTA1": {"displayName": "Example OTA"}},
    }
    payloads = iter(
        [payload_oneway_empty, payload_oneway_mixed, payload_return_empty, payload_return_mixed]
    )
    monkeypatch.setattr(client, "_search_payload", lambda **_kwargs: next(payloads))

    assert client.get_best_oneway("OTP", "MGA", "2026-03-10", "USD", 1, 1, 0, 0) is None
    best_oneway = client.get_best_oneway(
        "OTP",
        "MGA",
        "2026-03-11",
        "USD",
        1,
        1,
        0,
        0,
        max_connection_layover_seconds=3600,
    )
    assert best_oneway is not None
    assert best_oneway["price"] == 110
    assert best_oneway["booking_provider"] == "Example OTA"
    assert str(best_oneway["booking_url"]).endswith("/share/oneway")

    assert (
        client.get_best_return("OTP", "MGA", "2026-03-10", "2026-03-24", "USD", 1, 1, 0, 0) is None
    )
    best_return = client.get_best_return(
        "OTP",
        "MGA",
        "2026-03-11",
        "2026-03-25",
        "USD",
        1,
        1,
        0,
        0,
        max_connection_layover_seconds=3600,
    )
    assert best_return is not None
    assert best_return["price"] == 240
    assert best_return["booking_provider"] == client.display_name
    assert str(best_return["booking_url"]).endswith("/share/return")


def test_skyscanner_client_remaining_edge_paths_cover_offer_parsing_and_selection_failures(
    monkeypatch,
) -> None:
    client = SkyscannerScrapeClient(
        host="www.skyscanner.com",
        host_candidates=["www.skyscanner.net", "www.skyscanner.com"],
        playwright_fallback=True,
    )
    SkyscannerScrapeClient._PLAYWRIGHT_RUNTIME = None
    SkyscannerScrapeClient._close_playwright_runtime()

    class _Gate:
        def __init__(self) -> None:
            self.released = 0

        def acquire(self, timeout: float | None = None) -> bool:
            return True

        def release(self) -> None:
            self.released += 1

    gate = _Gate()
    cooldowns = iter([0, 5])
    monkeypatch.setattr(SkyscannerScrapeClient, "_PLAYWRIGHT_GATE", gate)
    monkeypatch.setattr(client, "_playwright_cooldown_remaining_seconds", lambda: next(cooldowns))
    with pytest.raises(ProviderNoResultError, match="retry in ~5s"):
        client._fetch_search_html_playwright("https://www.skyscanner.com/path")
    assert gate.released == 1

    class _PlaywrightTimeoutError(Exception):
        pass

    class _TimeoutPage:
        def goto(self, *_args: object, **_kwargs: object) -> None:
            raise _PlaywrightTimeoutError("timed out")

        def wait_for_load_state(self, *_args: object, **_kwargs: object) -> None:
            return None

        def wait_for_timeout(self, *_args: object, **_kwargs: object) -> None:
            return None

        def content(self) -> str:
            return ""

        @property
        def url(self) -> str:
            return "https://www.skyscanner.com/path"

        def close(self) -> None:
            return None

    class _TimeoutContext:
        def new_page(self) -> _TimeoutPage:
            return _TimeoutPage()

        def close(self) -> None:
            return None

    class _TimeoutBrowser:
        def is_connected(self) -> bool:
            return True

        def new_context(self, **_kwargs: object) -> _TimeoutContext:
            return _TimeoutContext()

    real_import = builtins.__import__

    def playwright_timeout_import(
        name: str,
        globals: object | None = None,
        locals: object | None = None,
        fromlist: object = (),
        level: int = 0,
    ) -> object:
        if name == "playwright.sync_api":
            return SimpleNamespace(TimeoutError=_PlaywrightTimeoutError)
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", playwright_timeout_import)
    monkeypatch.setattr(client, "_playwright_cooldown_remaining_seconds", lambda: 0)
    monkeypatch.setattr(
        client, "_get_or_start_playwright_runtime", lambda: (None, _TimeoutBrowser())
    )
    with pytest.raises(RuntimeError, match="navigation timed out"):
        client._fetch_search_html_playwright("https://www.skyscanner.com/path")

    monkeypatch.setattr(client, "_provider_cooldown_remaining_seconds", lambda: 0)
    monkeypatch.setattr(
        client,
        "_http_fetch_search_html",
        lambda url, attempt_idx=0: ("server error", url, 500),
    )
    monkeypatch.setattr(
        client,
        "_fetch_search_html_playwright",
        lambda url: (_ for _ in ()).throw(Exception("playwright blew up")),
    )
    with pytest.raises(RuntimeError, match="Playwright error"):
        client._fetch_search_html("https://www.skyscanner.com/path")

    monkeypatch.setattr(
        client,
        "_http_fetch_search_html",
        lambda url, attempt_idx=0: ("captcha blocked", url, 403),
    )
    monkeypatch.setattr(
        client,
        "_fetch_search_html_playwright",
        lambda url: ("captcha blocked", url),
    )
    with pytest.raises(ProviderNoResultError, match="blocked automated scraping"):
        client._fetch_search_html("https://www.skyscanner.com/path")

    malformed_html = """
    <script> </script>
    <script>window.__data = {"providerName": "Bad JSON", "price": bad};</script>
    """
    payloads = SkyscannerScrapeClient._extract_json_script_payloads(malformed_html)
    assert payloads == []
    assert (
        SkyscannerScrapeClient._extract_offer_options_regex(
            '"providerName":"Too High","price":"501000"'
        )
        == []
    )
    regex_offers = SkyscannerScrapeClient._extract_offer_options_regex(
        '"price":"222","providerName":"Regex OTA"'
    )
    assert regex_offers == [
        {
            "provider": "Regex OTA",
            "price": 222,
            "currency": None,
            "formatted_price": "222",
            "booking_url": None,
        }
    ]

    scripted_offers: list[dict[str, object]] = [
        {"provider": "", "price": 100},
        {"provider": "Zero", "price": 0},
        {"provider": "Dup", "price": 200, "currency": "eur"},
        {"provider": "Dup", "price": 200, "currency": "EUR"},
        {
            "provider": "Good",
            "price": 210,
            "currency": "usd",
            "formatted_price": "",
            "booking_url": "https://good",
        },
    ]
    monkeypatch.setattr(
        SkyscannerScrapeClient,
        "_extract_json_script_payloads",
        lambda html: [{"offers": []}],
    )
    monkeypatch.setattr(
        SkyscannerScrapeClient,
        "_collect_offer_nodes",
        lambda payload, offers: offers.extend(scripted_offers),
    )
    deduped = SkyscannerScrapeClient._extract_offer_options("<html></html>")
    assert deduped == [
        {
            "provider": "Dup",
            "price": 200,
            "currency": "EUR",
            "formatted_price": "200 EUR",
            "booking_url": None,
        },
        {
            "provider": "Good",
            "price": 210,
            "currency": "USD",
            "formatted_price": "210 USD",
            "booking_url": "https://good",
        },
    ]

    monkeypatch.setattr(
        SkyscannerScrapeClient,
        "_extract_offer_options",
        lambda html: [{"price": 444}],
    )
    assert SkyscannerScrapeClient._extract_best_price("<html></html>") == 444
    assert SkyscannerScrapeClient._offer_from_node({"name": "Fallback", "price": 211}) == {
        "provider": "Fallback",
        "price": 211,
        "currency": None,
        "formatted_price": "211",
        "booking_url": None,
    }
    assert SkyscannerScrapeClient._offer_from_node({"providerName": "No Price"}) is None
    assert SkyscannerScrapeClient._parse_price_value({"pricing": {"value": "333"}}, depth=1) == 333
    assert SkyscannerScrapeClient._parse_price_value({"pricing": {}}, depth=1) is None
    assert SkyscannerScrapeClient._parse_price_value("bad") is None
    assert (
        SkyscannerScrapeClient._extract_currency_from_node({"price": {"currencyCode": "usd"}})
        == "USD"
    )
    assert SkyscannerScrapeClient._extract_currency_from_node({"currency": "US"}) is None
    assert (
        SkyscannerScrapeClient._extract_booking_url_from_node(
            {"price": {"bookingUrl": "https://book.example"}, "clickUrl": "ftp://bad"}
        )
        == "https://book.example"
    )
    assert SkyscannerScrapeClient._extract_booking_url_from_node({"url": "ftp://bad"}) is None
    assert (
        SkyscannerScrapeClient.get_calendar_prices(
            client,
            "OTP",
            "MGA",
            "2026-03-10",
            "2026-03-12",
            "RON",
            1,
            1,
            0,
            0,
        )
        == {}
    )

    selection_client = SkyscannerScrapeClient(
        host="www.skyscanner.com",
        host_candidates=["www.skyscanner.com"],
        playwright_fallback=False,
    )
    monkeypatch.setattr(
        selection_client,
        "_fetch_search_html",
        lambda url: ('{"rawPrice":321}', "https://www.skyscanner.com/final"),
    )
    monkeypatch.setattr(selection_client, "_extract_offer_options", lambda html: [])
    monkeypatch.setattr(selection_client, "_extract_best_price", lambda html: 321)
    monkeypatch.setattr(selection_client, "_extract_stops_hint", lambda html, max_stops: 2)
    with pytest.raises(ProviderNoResultError, match="stops cap"):
        selection_client.get_best_oneway("OTP", "MGA", "2026-03-10", "RON", 1, 1, 0, 0)

    monkeypatch.setattr(
        selection_client,
        "_extract_offer_options",
        lambda html: [{"price": 0, "provider": "OTA", "booking_url": ""}],
    )
    monkeypatch.setattr(selection_client, "_extract_stops_hint", lambda html, max_stops: 0)
    with pytest.raises(ProviderNoResultError, match="no parsable fares"):
        selection_client.get_best_return(
            "OTP", "MGA", "2026-03-10", "2026-03-24", "RON", 1, 1, 0, 0
        )


def test_google_and_amadeus_helper_paths_cover_runtime_caching_and_tiebreaks(
    monkeypatch,
) -> None:
    real_import = builtins.__import__

    def missing_fast_flights_import(
        name: str,
        globals: object | None = None,
        locals: object | None = None,
        fromlist: object = (),
        level: int = 0,
    ) -> object:
        if name == "fast_flights":
            raise ImportError("fast-flights missing")
        return real_import(name, globals, locals, fromlist, level)

    google_client = GoogleFlightsLocalClient(fetch_mode="common")
    monkeypatch.setattr(builtins, "__import__", missing_fast_flights_import)
    assert google_client._ensure_fast_flights() is False
    assert google_client.configuration_hint() == "Install fast-flights to enable Google Flights."

    class _FastFlightsModule:
        FlightData = staticmethod(lambda **kwargs: kwargs)
        Passengers = staticmethod(lambda **kwargs: kwargs)

        @staticmethod
        def get_flights(**kwargs: object) -> object:
            return SimpleNamespace(flights=[])

    def missing_playwright_import(
        name: str,
        globals: object | None = None,
        locals: object | None = None,
        fromlist: object = (),
        level: int = 0,
    ) -> object:
        if name == "fast_flights":
            return _FastFlightsModule
        if name == "playwright.async_api":
            raise ImportError("playwright missing")
        return real_import(name, globals, locals, fromlist, level)

    local_google_client = GoogleFlightsLocalClient(fetch_mode="local")
    monkeypatch.setattr(builtins, "__import__", missing_playwright_import)
    if local_google_client._fetch_mode == "local":
        assert local_google_client._ensure_fast_flights() is False
        assert "playwright is required" in local_google_client._fast_flights_error
    else:
        assert local_google_client._ensure_fast_flights() is True

    def available_google_import(
        name: str,
        globals: object | None = None,
        locals: object | None = None,
        fromlist: object = (),
        level: int = 0,
    ) -> object:
        if name == "fast_flights":
            return _FastFlightsModule
        if name == "playwright.async_api":
            return SimpleNamespace()
        return real_import(name, globals, locals, fromlist, level)

    ready_google_client = GoogleFlightsLocalClient(fetch_mode="common")
    monkeypatch.setattr(builtins, "__import__", available_google_import)
    assert ready_google_client._ensure_fast_flights() is True
    with pytest.raises(ProviderNoResultError):
        ready_google_client._fetch_flights(
            source="OTP",
            destination="MGA",
            date_iso="2026-03-10",
            currency="RON",
            adults=1,
            max_stops_per_leg=1,
        )

    amadeus_client = AmadeusClient(
        client_id="id",
        client_secret="secret",
        base_url="https://amadeus.test",
    )
    token_session = _FakeSession(
        post_responses=[_FakeResponse({"access_token": "token", "expires_in": 120})]
    )
    monkeypatch.setattr(amadeus_client, "_session", lambda: token_session)
    monkeypatch.setattr("src.providers.amadeus.time.time", lambda: 100.0)
    assert amadeus_client._session() is token_session
    assert amadeus_client._fetch_token() == "token"
    assert amadeus_client._fetch_token() == "token"
    assert len(token_session.post_calls) == 1
    assert AmadeusClient._error_detail({"errors": [{"code": "ERR"}]}) == "ERR"
    assert AmadeusClient._is_no_result_error(400, "") is False

    search_client = AmadeusClient(
        client_id="id",
        client_secret="secret",
        base_url="https://amadeus.test",
    )
    captured: list[dict[str, object]] = []

    def fake_get(_path: str, params: dict[str, object]) -> dict[str, object]:
        captured.append(dict(params))
        if "returnDate" in params:
            return {
                "data": [
                    {"price": {"grandTotal": "bad", "currency": "EUR"}, "itineraries": [{}, {}]},
                    {
                        "price": {"grandTotal": "900", "currency": "EUR"},
                        "itineraries": [
                            {
                                "duration": "PT4H",
                                "segments": [
                                    {
                                        "departure": {
                                            "iataCode": "OTP",
                                            "at": "2026-03-10T08:00:00",
                                        },
                                        "arrival": {
                                            "iataCode": "MGA",
                                            "at": "2026-03-10T12:00:00",
                                        },
                                        "carrierCode": "TK",
                                    }
                                ],
                            },
                            {
                                "duration": "PT4H",
                                "segments": [
                                    {
                                        "departure": {
                                            "iataCode": "MGA",
                                            "at": "2026-03-24T08:00:00",
                                        },
                                        "arrival": {
                                            "iataCode": "OTP",
                                            "at": "2026-03-24T12:00:00",
                                        },
                                        "carrierCode": "TK",
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "price": {"grandTotal": "900", "currency": "EUR"},
                        "itineraries": [
                            {
                                "duration": "PT5H",
                                "segments": [
                                    {
                                        "departure": {
                                            "iataCode": "OTP",
                                            "at": "2026-03-10T08:00:00",
                                        },
                                        "arrival": {
                                            "iataCode": "IST",
                                            "at": "2026-03-10T09:00:00",
                                        },
                                        "carrierCode": "TK",
                                    },
                                    {
                                        "departure": {
                                            "iataCode": "IST",
                                            "at": "2026-03-10T12:40:00",
                                        },
                                        "arrival": {
                                            "iataCode": "MGA",
                                            "at": "2026-03-10T15:00:00",
                                        },
                                        "carrierCode": "TK",
                                    },
                                ],
                            },
                            {
                                "duration": "PT5H",
                                "segments": [
                                    {
                                        "departure": {
                                            "iataCode": "MGA",
                                            "at": "2026-03-24T08:00:00",
                                        },
                                        "arrival": {
                                            "iataCode": "IST",
                                            "at": "2026-03-24T09:00:00",
                                        },
                                        "carrierCode": "TK",
                                    },
                                    {
                                        "departure": {
                                            "iataCode": "IST",
                                            "at": "2026-03-24T12:40:00",
                                        },
                                        "arrival": {
                                            "iataCode": "OTP",
                                            "at": "2026-03-24T15:00:00",
                                        },
                                        "carrierCode": "TK",
                                    },
                                ],
                            },
                        ],
                    },
                ]
            }
        return {
            "data": [
                {"price": {"grandTotal": "bad", "currency": "EUR"}, "itineraries": [{}]},
                {
                    "price": {"grandTotal": "620", "currency": "EUR"},
                    "itineraries": [
                        {
                            "duration": "PT4H",
                            "segments": [
                                {
                                    "departure": {
                                        "iataCode": "OTP",
                                        "at": "2026-03-10T08:00:00",
                                    },
                                    "arrival": {
                                        "iataCode": "MGA",
                                        "at": "2026-03-10T12:00:00",
                                    },
                                    "carrierCode": "TK",
                                }
                            ],
                        }
                    ],
                },
                {
                    "price": {"grandTotal": "620", "currency": "EUR"},
                    "itineraries": [
                        {
                            "duration": "PT5H",
                            "segments": [
                                {
                                    "departure": {
                                        "iataCode": "OTP",
                                        "at": "2026-03-10T08:00:00",
                                    },
                                    "arrival": {
                                        "iataCode": "IST",
                                        "at": "2026-03-10T09:00:00",
                                    },
                                    "carrierCode": "TK",
                                },
                                {
                                    "departure": {
                                        "iataCode": "IST",
                                        "at": "2026-03-10T12:40:00",
                                    },
                                    "arrival": {
                                        "iataCode": "MGA",
                                        "at": "2026-03-10T15:00:00",
                                    },
                                    "carrierCode": "TK",
                                },
                            ],
                        }
                    ],
                },
            ]
        }

    monkeypatch.setattr(search_client, "_get", fake_get)
    monkeypatch.setattr(search_client, "is_configured", lambda: True)
    best_oneway = search_client.get_best_oneway("OTP", "MGA", "2026-03-10", "EUR", 0, 1, 0, 0)
    best_return = search_client.get_best_return(
        "OTP",
        "MGA",
        "2026-03-10",
        "2026-03-24",
        "EUR",
        0,
        1,
        0,
        0,
    )
    assert best_oneway is not None
    assert best_oneway["price"] == 620
    assert best_oneway["stops"] == 0
    assert best_return is not None
    assert best_return["price"] == 900
    assert best_return["outbound_stops"] == 0
    assert best_return["inbound_stops"] == 0
    assert all(params.get("nonStop") == "true" for params in captured)


def test_google_flights_client_remaining_edge_paths_cover_runtime_and_return_logic(
    monkeypatch,
) -> None:
    client = GoogleFlightsLocalClient(fetch_mode="common")
    assert client._carrier_from_name(",") == ("GF", ",")
    assert (
        client.get_calendar_prices("OTP", "MGA", "2026-03-10", "2026-03-12", "RON", 1, 1, 0, 0)
        == {}
    )

    monkeypatch.setattr(client, "_ensure_fast_flights", lambda: False)
    client._fast_flights_error = "missing runtime"
    with pytest.raises(RuntimeError, match="missing runtime"):
        client._fetch_flights(
            source="OTP",
            destination="MGA",
            date_iso="2026-03-10",
            currency="RON",
            adults=1,
            max_stops_per_leg=1,
        )

    monkeypatch.setattr(client, "_ensure_fast_flights", lambda: True)
    client._FlightData = lambda **kwargs: kwargs
    client._Passengers = lambda **kwargs: kwargs
    client._get_flights_fn = lambda **kwargs: (_ for _ in ()).throw(RuntimeError("backend failed"))
    with pytest.raises(RuntimeError, match="backend failed"):
        client._fetch_flights(
            source="OTP",
            destination="MGA",
            date_iso="2026-03-10",
            currency="RON",
            adults=1,
            max_stops_per_leg=1,
        )

    client._get_flights_fn = lambda **kwargs: (_ for _ in ()).throw(AssertionError("assertion"))
    with pytest.raises(RuntimeError, match="Google Flights request failed: assertion"):
        client._fetch_flights(
            source="OTP",
            destination="MGA",
            date_iso="2026-03-10",
            currency="RON",
            adults=1,
            max_stops_per_leg=1,
        )

    client._get_flights_fn = lambda **kwargs: SimpleNamespace(flights=[])
    with pytest.raises(ProviderNoResultError):
        client._fetch_flights(
            source="OTP",
            destination="MGA",
            date_iso="2026-03-10",
            currency="RON",
            adults=1,
            max_stops_per_leg=1,
        )

    client._get_flights_fn = lambda **kwargs: SimpleNamespace(
        flights=[SimpleNamespace(price="600", stops=0, duration="2h", name="Carrier")]
    )
    fetched = client._fetch_flights(
        source="OTP",
        destination="MGA",
        date_iso="2026-03-10",
        currency="RON",
        adults=1,
        max_stops_per_leg=1,
    )
    assert len(fetched) == 1

    disabled_client = GoogleFlightsLocalClient(fetch_mode="common")
    monkeypatch.setattr(disabled_client, "is_configured", lambda: False)
    assert disabled_client.get_best_oneway("OTP", "MGA", "2026-03-10", "RON", 1, 1, 0, 0) is None

    real_import = builtins.__import__

    class _FastFlightsModule:
        FlightData = staticmethod(lambda **kwargs: kwargs)
        Passengers = staticmethod(lambda **kwargs: kwargs)
        get_flights = staticmethod(lambda **kwargs: SimpleNamespace(flights=[]))

    def local_missing_playwright_import(
        name: str,
        globals: object | None = None,
        locals: object | None = None,
        fromlist: object = (),
        level: int = 0,
    ) -> object:
        if name == "fast_flights":
            return _FastFlightsModule
        if name == "playwright.async_api":
            raise ImportError("playwright missing")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr("src.providers.google_flights.ALLOW_PLAYWRIGHT_PROVIDERS", True)
    local_client = GoogleFlightsLocalClient(fetch_mode="local")
    monkeypatch.setattr(builtins, "__import__", local_missing_playwright_import)
    assert local_client._ensure_fast_flights() is False
    assert "playwright is required" in local_client._fast_flights_error

    flight_without_price = SimpleNamespace(
        price=None,
        stops=0,
        duration="1h 0m",
        name="Carrier",
        departure="5:00 AM on Mar 10",
        arrival="6:00 AM on Mar 10",
    )
    one_stop_flight = SimpleNamespace(
        price="700",
        stops=1,
        duration="4h 0m",
        name="Carrier",
        departure="5:00 AM on Mar 10",
        arrival="9:00 AM on Mar 10",
    )
    direct_flight = SimpleNamespace(
        price="700",
        stops=0,
        duration="5h 0m",
        name="Carrier",
        departure="6:00 AM on Mar 10",
        arrival="11:00 AM on Mar 10",
    )
    monkeypatch.setattr(client, "is_configured", lambda: True)
    monkeypatch.setattr(
        client,
        "_fetch_flights",
        lambda **kwargs: [flight_without_price, one_stop_flight, direct_flight],
    )
    best_oneway = client.get_best_oneway("OTP", "MGA", "2026-03-10", "RON", 1, 1, 0, 0)
    assert best_oneway is not None
    assert best_oneway["price"] == 700
    assert best_oneway["stops"] == 0

    return_none_client = GoogleFlightsLocalClient(fetch_mode="common")
    monkeypatch.setattr(return_none_client, "is_configured", lambda: True)
    monkeypatch.setattr(return_none_client, "get_best_oneway", lambda *args, **kwargs: None)
    assert (
        return_none_client.get_best_return(
            "OTP", "MGA", "2026-03-10", "2026-03-24", "RON", 1, 1, 0, 0
        )
        is None
    )

    return_client = GoogleFlightsLocalClient(fetch_mode="common")
    monkeypatch.setattr(return_client, "is_configured", lambda: True)
    oneway_results = iter(
        [
            {
                "price": 500,
                "duration_seconds": None,
                "stops": 0,
                "transfer_events": 0,
                "segments": [{"from": "OTP", "to": "MGA"}],
            },
            {
                "price": 400,
                "duration_seconds": 3600,
                "stops": 0,
                "transfer_events": 0,
                "segments": [{"from": "MGA", "to": "OTP"}],
            },
        ]
    )
    monkeypatch.setattr(
        return_client, "get_best_oneway", lambda *args, **kwargs: next(oneway_results)
    )
    best_return = return_client.get_best_return(
        "OTP", "MGA", "2026-03-10", "2026-03-24", "RON", 1, 1, 0, 0
    )
    assert best_return is not None
    assert best_return["price"] == 900
    assert best_return["duration_seconds"] is None


def test_serpapi_client_remaining_edge_paths_cover_search_and_selection_logic(
    monkeypatch,
) -> None:
    unconfigured = SerpApiGoogleFlightsClient(api_key="")
    assert unconfigured._session() is unconfigured._session()
    with pytest.raises(RuntimeError, match="API key is missing"):
        unconfigured._search({"departure_id": "OTP"})

    client = SerpApiGoogleFlightsClient(
        api_key="key",
        search_url="https://serpapi.example",
        return_option_scan_limit=99,
    )
    assert client._return_option_scan_limit == 5
    assert (
        client.get_calendar_prices("OTP", "MGA", "2026-03-10", "2026-03-12", "RON", 1, 1, 0, 0)
        == {}
    )
    assert client._stops_param(0) == 1
    assert client._stops_param(1) == 2
    assert client._stops_param(2) == 3
    assert client._option_duration_seconds({"total_duration": "2h 15m"}) == 8100

    class _NoRaiseResponse(_FakeResponse):
        def raise_for_status(self) -> None:
            return None

    session = _FakeSession(
        get_responses=[_NoRaiseResponse(ValueError("bad payload"), status_code=400)]
    )
    monkeypatch.setattr(client, "_session", lambda: session)
    monkeypatch.setattr(
        "src.providers.serpapi._capture_provider_response",
        lambda *args, **kwargs: None,
    )
    with pytest.raises(RuntimeError, match="HTTP 400"):
        client._search({"departure_id": "OTP"})

    oneway_payload = {
        "search_metadata": {"raw_html_file": "https://google.example/out"},
        "best_flights": [
            {"price": None, "flights": [{}]},
            {"price": 800, "flights": []},
            {
                "price": 600,
                "total_duration": "6h 0m",
                "flights": [
                    {
                        "departure_airport": {
                            "id": "OTP",
                            "name": "OTP",
                            "time": "2026-03-10 08:00",
                        },
                        "arrival_airport": {"id": "IST", "name": "IST", "time": "2026-03-10 09:00"},
                        "airline": "TK",
                    },
                    {
                        "departure_airport": {
                            "id": "IST",
                            "name": "IST",
                            "time": "2026-03-10 10:00",
                        },
                        "arrival_airport": {"id": "LCA", "name": "LCA", "time": "2026-03-10 11:00"},
                        "airline": "TK",
                    },
                    {
                        "departure_airport": {
                            "id": "LCA",
                            "name": "LCA",
                            "time": "2026-03-10 12:00",
                        },
                        "arrival_airport": {"id": "MGA", "name": "MGA", "time": "2026-03-10 15:00"},
                        "airline": "TK",
                    },
                ],
            },
            {
                "price": 700,
                "total_duration": "5h 0m",
                "flights": [
                    {
                        "departure_airport": {
                            "id": "OTP",
                            "name": "OTP",
                            "time": "2026-03-10 08:00",
                        },
                        "arrival_airport": {"id": "IST", "name": "IST", "time": "2026-03-10 09:00"},
                        "airline": "TK",
                    },
                    {
                        "departure_airport": {
                            "id": "IST",
                            "name": "IST",
                            "time": "2026-03-10 13:30",
                        },
                        "arrival_airport": {"id": "MGA", "name": "MGA", "time": "2026-03-10 15:00"},
                        "airline": "TK",
                    },
                ],
            },
            {
                "price": 700,
                "total_duration": "4h 0m",
                "flights": [
                    {
                        "departure_airport": {
                            "id": "OTP",
                            "name": "OTP",
                            "time": "2026-03-10 08:00",
                        },
                        "arrival_airport": {"id": "MGA", "name": "MGA", "time": "2026-03-10 12:00"},
                        "airline": "TK",
                    }
                ],
            },
            {
                "price": 650,
                "duration": "3h 0m",
                "flights": [
                    {
                        "departure_airport": {
                            "id": "OTP",
                            "name": "OTP",
                            "time": "2026-03-10 08:00",
                        },
                        "arrival_airport": {"id": "IST", "name": "IST", "time": "2026-03-10 09:00"},
                        "airline": "TK",
                    },
                    {
                        "departure_airport": {
                            "id": "IST",
                            "name": "IST",
                            "time": "2026-03-10 09:40",
                        },
                        "arrival_airport": {"id": "MGA", "name": "MGA", "time": "2026-03-10 12:00"},
                        "airline": "TK",
                    },
                ],
            },
            {
                "price": 650,
                "total_duration": 120,
                "flights": [
                    {
                        "departure_airport": {
                            "id": "OTP",
                            "name": "OTP",
                            "time": "2026-03-10 08:00",
                        },
                        "arrival_airport": {"id": "MGA", "name": "MGA", "time": "2026-03-10 10:00"},
                        "airline": "TK",
                    }
                ],
            },
        ],
    }
    monkeypatch.setattr(client, "_search", lambda params: oneway_payload)
    best_oneway = client.get_best_oneway(
        "OTP",
        "MGA",
        "2026-03-10",
        "EUR",
        1,
        1,
        0,
        0,
        max_connection_layover_seconds=3600,
    )
    assert best_oneway is not None
    assert best_oneway["price"] == 650
    assert best_oneway["stops"] == 0
    assert best_oneway["booking_url"] == "https://google.example/out"

    return_payloads = {
        "base": {
            "search_metadata": {},
            "best_flights": [
                {"price": 900, "flights": []},
                {
                    "price": 900,
                    "departure_token": "too-many-inbound-stops",
                    "flights": [
                        {
                            "departure_airport": {
                                "id": "OTP",
                                "name": "OTP",
                                "time": "2026-03-10 08:00",
                            },
                            "arrival_airport": {
                                "id": "MGA",
                                "name": "MGA",
                                "time": "2026-03-10 12:00",
                            },
                            "airline": "TK",
                        }
                    ],
                },
                {
                    "price": 900,
                    "departure_token": "too-many-stops",
                    "flights": [
                        {
                            "departure_airport": {
                                "id": "OTP",
                                "name": "OTP",
                                "time": "2026-03-10 08:00",
                            },
                            "arrival_airport": {
                                "id": "IST",
                                "name": "IST",
                                "time": "2026-03-10 09:00",
                            },
                            "airline": "TK",
                        },
                        {
                            "departure_airport": {
                                "id": "IST",
                                "name": "IST",
                                "time": "2026-03-10 10:00",
                            },
                            "arrival_airport": {
                                "id": "LCA",
                                "name": "LCA",
                                "time": "2026-03-10 11:00",
                            },
                            "airline": "TK",
                        },
                        {
                            "departure_airport": {
                                "id": "LCA",
                                "name": "LCA",
                                "time": "2026-03-10 12:00",
                            },
                            "arrival_airport": {
                                "id": "MGA",
                                "name": "MGA",
                                "time": "2026-03-10 15:00",
                            },
                            "airline": "TK",
                        },
                    ],
                },
                {
                    "price": 900,
                    "departure_token": "long-layover",
                    "flights": [
                        {
                            "departure_airport": {
                                "id": "OTP",
                                "name": "OTP",
                                "time": "2026-03-10 08:00",
                            },
                            "arrival_airport": {
                                "id": "IST",
                                "name": "IST",
                                "time": "2026-03-10 09:00",
                            },
                            "airline": "TK",
                        },
                        {
                            "departure_airport": {
                                "id": "IST",
                                "name": "IST",
                                "time": "2026-03-10 13:30",
                            },
                            "arrival_airport": {
                                "id": "MGA",
                                "name": "MGA",
                                "time": "2026-03-10 15:00",
                            },
                            "airline": "TK",
                        },
                    ],
                },
                {
                    "departure_token": "no-price",
                    "flights": [
                        {
                            "departure_airport": {
                                "id": "OTP",
                                "name": "OTP",
                                "time": "2026-03-10 08:00",
                            },
                            "arrival_airport": {
                                "id": "MGA",
                                "name": "MGA",
                                "time": "2026-03-10 12:00",
                            },
                            "airline": "TK",
                        }
                    ],
                },
                {
                    "price": 900,
                    "departure_token": "good",
                    "flights": [
                        {
                            "departure_airport": {
                                "id": "OTP",
                                "name": "OTP",
                                "time": "2026-03-10 08:00",
                            },
                            "arrival_airport": {
                                "id": "MGA",
                                "name": "MGA",
                                "time": "2026-03-10 12:00",
                            },
                            "airline": "TK",
                        }
                    ],
                },
                {
                    "price": 900,
                    "departure_token": "long-inbound-layover",
                    "flights": [
                        {
                            "departure_airport": {
                                "id": "OTP",
                                "name": "OTP",
                                "time": "2026-03-10 08:00",
                            },
                            "arrival_airport": {
                                "id": "MGA",
                                "name": "MGA",
                                "time": "2026-03-10 12:00",
                            },
                            "airline": "TK",
                        }
                    ],
                },
                {
                    "departure_token": "still-no-price",
                    "flights": [
                        {
                            "departure_airport": {
                                "id": "OTP",
                                "name": "OTP",
                                "time": "2026-03-10 08:00",
                            },
                            "arrival_airport": {
                                "id": "MGA",
                                "name": "MGA",
                                "time": "2026-03-10 12:00",
                            },
                            "airline": "TK",
                        }
                    ],
                },
                {
                    "price": 850,
                    "departure_token": "cheaper",
                    "flights": [
                        {
                            "departure_airport": {
                                "id": "OTP",
                                "name": "OTP",
                                "time": "2026-03-10 08:00",
                            },
                            "arrival_airport": {
                                "id": "MGA",
                                "name": "MGA",
                                "time": "2026-03-10 12:00",
                            },
                            "airline": "TK",
                        }
                    ],
                },
            ],
        },
        "too-many-inbound-stops": {
            "search_metadata": {"raw_html_file": "https://google.example/return"},
            "best_flights": [
                {
                    "price": 900,
                    "flights": [
                        {
                            "departure_airport": {
                                "id": "MGA",
                                "name": "MGA",
                                "time": "2026-03-24 08:00",
                            },
                            "arrival_airport": {
                                "id": "IST",
                                "name": "IST",
                                "time": "2026-03-24 09:00",
                            },
                            "airline": "TK",
                        },
                        {
                            "departure_airport": {
                                "id": "IST",
                                "name": "IST",
                                "time": "2026-03-24 10:00",
                            },
                            "arrival_airport": {
                                "id": "LCA",
                                "name": "LCA",
                                "time": "2026-03-24 11:00",
                            },
                            "airline": "TK",
                        },
                        {
                            "departure_airport": {
                                "id": "LCA",
                                "name": "LCA",
                                "time": "2026-03-24 12:00",
                            },
                            "arrival_airport": {
                                "id": "OTP",
                                "name": "OTP",
                                "time": "2026-03-24 15:00",
                            },
                            "airline": "TK",
                        },
                    ],
                }
            ],
        },
        "too-many-stops": {"best_flights": []},
        "long-layover": {"best_flights": []},
        "no-price": {
            "search_metadata": {"raw_html_file": "https://google.example/return"},
            "best_flights": [
                {"price": None, "flights": [{"departure_airport": {}, "arrival_airport": {}}]}
            ],
        },
        "good": {
            "search_metadata": {"raw_html_file": "https://google.example/return"},
            "best_flights": [
                {"price": 900, "flights": []},
                {
                    "price": 900,
                    "total_duration": "3h 0m",
                    "flights": [
                        {
                            "departure_airport": {
                                "id": "MGA",
                                "name": "MGA",
                                "time": "2026-03-24 08:00",
                            },
                            "arrival_airport": {
                                "id": "IST",
                                "name": "IST",
                                "time": "2026-03-24 09:00",
                            },
                            "airline": "TK",
                        },
                        {
                            "departure_airport": {
                                "id": "IST",
                                "name": "IST",
                                "time": "2026-03-24 09:40",
                            },
                            "arrival_airport": {
                                "id": "OTP",
                                "name": "OTP",
                                "time": "2026-03-24 14:30",
                            },
                            "airline": "TK",
                        },
                    ],
                },
                {
                    "price": 900,
                    "total_duration": "2h 0m",
                    "flights": [
                        {
                            "departure_airport": {
                                "id": "MGA",
                                "name": "MGA",
                                "time": "2026-03-24 08:00",
                            },
                            "arrival_airport": {
                                "id": "OTP",
                                "name": "OTP",
                                "time": "2026-03-24 10:00",
                            },
                            "airline": "TK",
                        }
                    ],
                },
            ],
        },
        "long-inbound-layover": {
            "search_metadata": {"raw_html_file": "https://google.example/return"},
            "best_flights": [
                {
                    "price": 900,
                    "flights": [
                        {
                            "departure_airport": {
                                "id": "MGA",
                                "name": "MGA",
                                "time": "2026-03-24 08:00",
                            },
                            "arrival_airport": {
                                "id": "IST",
                                "name": "IST",
                                "time": "2026-03-24 09:00",
                            },
                            "airline": "TK",
                        },
                        {
                            "departure_airport": {
                                "id": "IST",
                                "name": "IST",
                                "time": "2026-03-24 13:30",
                            },
                            "arrival_airport": {
                                "id": "OTP",
                                "name": "OTP",
                                "time": "2026-03-24 16:00",
                            },
                            "airline": "TK",
                        },
                    ],
                }
            ],
        },
        "still-no-price": {
            "search_metadata": {"raw_html_file": "https://google.example/return"},
            "best_flights": [
                {
                    "price": None,
                    "flights": [
                        {
                            "departure_airport": {
                                "id": "MGA",
                                "name": "MGA",
                                "time": "2026-03-24 08:00",
                            },
                            "arrival_airport": {
                                "id": "OTP",
                                "name": "OTP",
                                "time": "2026-03-24 10:00",
                            },
                            "airline": "TK",
                        }
                    ],
                }
            ],
        },
        "cheaper": {
            "search_metadata": {"raw_html_file": "https://google.example/return-cheaper"},
            "best_flights": [
                {
                    "price": 850,
                    "total_duration": "2h 0m",
                    "flights": [
                        {
                            "departure_airport": {
                                "id": "MGA",
                                "name": "MGA",
                                "time": "2026-03-24 08:00",
                            },
                            "arrival_airport": {
                                "id": "OTP",
                                "name": "OTP",
                                "time": "2026-03-24 10:00",
                            },
                            "airline": "TK",
                        }
                    ],
                }
            ],
        },
    }

    def _return_search(params: dict[str, object]) -> dict[str, object]:
        departure_token = str(params.get("departure_token") or "")
        return return_payloads[departure_token or "base"]

    monkeypatch.setattr(client, "_search", _return_search)
    best_return = client.get_best_return(
        "OTP",
        "MGA",
        "2026-03-10",
        "2026-03-24",
        "EUR",
        1,
        1,
        0,
        0,
        max_connection_layover_seconds=3600,
    )
    assert best_return is not None
    assert best_return["price"] == 850
    assert best_return["outbound_stops"] == 0
    assert best_return["inbound_stops"] == 0
    assert best_return["booking_url"] == "https://google.example/return-cheaper"

    extra_return_payloads = {
        "base": {
            "best_flights": [
                {
                    "price": 900,
                    "departure_token": "no-inbound-segments",
                    "flights": [
                        {
                            "departure_airport": {
                                "id": "OTP",
                                "name": "OTP",
                                "time": "2026-03-10 08:00",
                            },
                            "arrival_airport": {
                                "id": "MGA",
                                "name": "MGA",
                                "time": "2026-03-10 12:00",
                            },
                            "airline": "TK",
                        }
                    ],
                },
                {
                    "price": 900,
                    "departure_token": "long-inbound-layover-hit",
                    "flights": [
                        {
                            "departure_airport": {
                                "id": "OTP",
                                "name": "OTP",
                                "time": "2026-03-10 08:00",
                            },
                            "arrival_airport": {
                                "id": "MGA",
                                "name": "MGA",
                                "time": "2026-03-10 12:00",
                            },
                            "airline": "TK",
                        }
                    ],
                },
                {
                    "departure_token": "still-no-price-hit",
                    "flights": [
                        {
                            "departure_airport": {
                                "id": "OTP",
                                "name": "OTP",
                                "time": "2026-03-10 08:00",
                            },
                            "arrival_airport": {
                                "id": "MGA",
                                "name": "MGA",
                                "time": "2026-03-10 12:00",
                            },
                            "airline": "TK",
                        }
                    ],
                },
                {
                    "price": 900,
                    "departure_token": "good-direct-hit",
                    "flights": [
                        {
                            "departure_airport": {
                                "id": "OTP",
                                "name": "OTP",
                                "time": "2026-03-10 08:00",
                            },
                            "arrival_airport": {
                                "id": "MGA",
                                "name": "MGA",
                                "time": "2026-03-10 12:00",
                            },
                            "airline": "TK",
                        }
                    ],
                },
                {
                    "price": 850,
                    "departure_token": "cheaper-hit",
                    "flights": [
                        {
                            "departure_airport": {
                                "id": "OTP",
                                "name": "OTP",
                                "time": "2026-03-10 08:00",
                            },
                            "arrival_airport": {
                                "id": "MGA",
                                "name": "MGA",
                                "time": "2026-03-10 12:00",
                            },
                            "airline": "TK",
                        }
                    ],
                },
            ]
        },
        "no-inbound-segments": {"best_flights": [{"price": 900, "flights": []}]},
        "long-inbound-layover-hit": {
            "best_flights": [
                {
                    "price": 900,
                    "flights": [
                        {
                            "departure_airport": {
                                "id": "MGA",
                                "name": "MGA",
                                "time": "2026-03-24 08:00",
                            },
                            "arrival_airport": {
                                "id": "IST",
                                "name": "IST",
                                "time": "2026-03-24 09:00",
                            },
                            "airline": "TK",
                        },
                        {
                            "departure_airport": {
                                "id": "IST",
                                "name": "IST",
                                "time": "2026-03-24 13:30",
                            },
                            "arrival_airport": {
                                "id": "OTP",
                                "name": "OTP",
                                "time": "2026-03-24 16:00",
                            },
                            "airline": "TK",
                        },
                    ],
                }
            ]
        },
        "still-no-price-hit": {
            "best_flights": [
                {
                    "price": None,
                    "flights": [
                        {
                            "departure_airport": {
                                "id": "MGA",
                                "name": "MGA",
                                "time": "2026-03-24 08:00",
                            },
                            "arrival_airport": {
                                "id": "OTP",
                                "name": "OTP",
                                "time": "2026-03-24 10:00",
                            },
                            "airline": "TK",
                        }
                    ],
                }
            ]
        },
        "good-direct-hit": {
            "search_metadata": {"raw_html_file": "https://google.example/return-good"},
            "best_flights": [
                {
                    "price": 900,
                    "flights": [
                        {
                            "departure_airport": {
                                "id": "MGA",
                                "name": "MGA",
                                "time": "2026-03-24 08:00",
                            },
                            "arrival_airport": {
                                "id": "OTP",
                                "name": "OTP",
                                "time": "2026-03-24 10:00",
                            },
                            "airline": "TK",
                        }
                    ],
                }
            ],
        },
        "cheaper-hit": {
            "search_metadata": {"raw_html_file": "https://google.example/return-cheaper-hit"},
            "best_flights": [
                {
                    "price": 850,
                    "flights": [
                        {
                            "departure_airport": {
                                "id": "MGA",
                                "name": "MGA",
                                "time": "2026-03-24 08:00",
                            },
                            "arrival_airport": {
                                "id": "OTP",
                                "name": "OTP",
                                "time": "2026-03-24 10:00",
                            },
                            "airline": "TK",
                        }
                    ],
                }
            ],
        },
    }

    monkeypatch.setattr(
        client,
        "_search",
        lambda params: extra_return_payloads[str(params.get("departure_token") or "base")],
    )
    extra_best_return = client.get_best_return(
        "OTP",
        "MGA",
        "2026-03-11",
        "2026-03-25",
        "EUR",
        1,
        1,
        0,
        0,
        max_connection_layover_seconds=3600,
    )
    assert extra_best_return is not None
    assert extra_best_return["price"] == 850
    assert extra_best_return["booking_url"] == "https://google.example/return-cheaper-hit"


def test_amadeus_client_remaining_edge_paths_cover_session_errors_and_tiebreaks(
    monkeypatch,
) -> None:
    class _NoRaiseResponse(_FakeResponse):
        def raise_for_status(self) -> None:
            return None

    client = AmadeusClient(client_id="id", client_secret="secret", base_url="https://amadeus.test")
    assert client._session() is client._session()
    assert AmadeusClient._error_detail({}) == ""
    assert AmadeusClient._error_detail({"errors": [{}]}) == "{}"
    assert AmadeusClient._amount_to_int(None) is None

    retry_client = AmadeusClient(
        client_id="id",
        client_secret="secret",
        base_url="https://amadeus.test",
    )
    retry_session = _FakeSession(
        post_responses=[_FakeResponse({"access_token": "token", "expires_in": 120})],
        get_responses=[
            _FakeResponse({}, status_code=503, headers={"Retry-After": "bad"}),
            _FakeResponse({"data": []}, status_code=200),
        ],
    )
    monkeypatch.setattr(retry_client, "_session", lambda: retry_session)
    monkeypatch.setattr(
        "src.providers.amadeus._capture_provider_response",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "src.providers.amadeus.time.sleep",
        lambda *_args, **_kwargs: None,
    )
    assert retry_client._get("/v2/shopping/flight-offers", {"originLocationCode": "OTP"}) == {
        "data": []
    }

    http_error_client = AmadeusClient(
        client_id="id",
        client_secret="secret",
        base_url="https://amadeus.test",
    )
    http_error_session = _FakeSession(
        post_responses=[
            _FakeResponse({"access_token": "token", "expires_in": 120}),
        ],
        get_responses=[
            _FakeResponse({"errors": [{"detail": "server broke"}]}, status_code=418),
        ],
    )
    monkeypatch.setattr(http_error_client, "_session", lambda: http_error_session)
    monkeypatch.setattr(
        "src.providers.amadeus._capture_provider_response",
        lambda *args, **kwargs: None,
    )
    with pytest.raises(requests.HTTPError):
        http_error_client._get("/v2/shopping/flight-offers", {"originLocationCode": "OTP"})

    payload_error_client = AmadeusClient(
        client_id="id",
        client_secret="secret",
        base_url="https://amadeus.test",
    )
    payload_error_session = _FakeSession(
        post_responses=[_FakeResponse({"access_token": "token", "expires_in": 120})],
        get_responses=[_FakeResponse({"errors": [{"title": "payload broke"}]}, status_code=200)],
    )
    monkeypatch.setattr(payload_error_client, "_session", lambda: payload_error_session)
    monkeypatch.setattr(
        "src.providers.amadeus._capture_provider_response",
        lambda *args, **kwargs: None,
    )
    with pytest.raises(RuntimeError, match="payload broke"):
        payload_error_client._get("/v2/shopping/flight-offers", {"originLocationCode": "OTP"})

    no_result_payload_client = AmadeusClient(
        client_id="id",
        client_secret="secret",
        base_url="https://amadeus.test",
    )
    no_result_payload_session = _FakeSession(
        post_responses=[_FakeResponse({"access_token": "token", "expires_in": 120})],
        get_responses=[
            _NoRaiseResponse(
                {"errors": [{"detail": "no flight offers found"}]},
                status_code=400,
            )
        ],
    )
    monkeypatch.setattr(no_result_payload_client, "_session", lambda: no_result_payload_session)
    monkeypatch.setattr(
        "src.providers.amadeus._capture_provider_response",
        lambda *args, **kwargs: None,
    )
    with pytest.raises(ProviderNoResultError):
        no_result_payload_client._get("/v2/shopping/flight-offers", {"originLocationCode": "OTP"})

    no_result_client = AmadeusClient(client_id="id", client_secret="secret")
    monkeypatch.setattr(no_result_client, "is_configured", lambda: True)
    monkeypatch.setattr(
        no_result_client,
        "_get",
        lambda *args, **kwargs: (_ for _ in ()).throw(ProviderNoResultError("no offers")),
    )
    assert (
        no_result_client.get_calendar_prices(
            "OTP", "MGA", "2026-03-10", "2026-03-12", "EUR", 1, 1, 0, 0
        )
        == {}
    )
    assert no_result_client.get_best_oneway("OTP", "MGA", "2026-03-10", "EUR", 1, 1, 0, 0) is None
    assert (
        no_result_client.get_best_return(
            "OTP", "MGA", "2026-03-10", "2026-03-24", "EUR", 1, 1, 0, 0
        )
        is None
    )

    tiebreak_client = AmadeusClient(client_id="id", client_secret="secret")
    monkeypatch.setattr(tiebreak_client, "is_configured", lambda: True)
    monkeypatch.setattr(
        tiebreak_client,
        "_get",
        lambda path, params: {
            "data": [
                {
                    "price": {"grandTotal": "900", "currency": "EUR"},
                    "itineraries": [
                        {
                            "duration": "PT5H",
                            "segments": [
                                {
                                    "departure": {"iataCode": "OTP", "at": "2026-03-10T08:00:00"},
                                    "arrival": {"iataCode": "IST", "at": "2026-03-10T09:00:00"},
                                    "carrierCode": "TK",
                                },
                                {
                                    "departure": {"iataCode": "IST", "at": "2026-03-10T09:45:00"},
                                    "arrival": {"iataCode": "MGA", "at": "2026-03-10T12:00:00"},
                                    "carrierCode": "TK",
                                },
                            ],
                        },
                        {
                            "duration": "PT5H",
                            "segments": [
                                {
                                    "departure": {"iataCode": "MGA", "at": "2026-03-24T08:00:00"},
                                    "arrival": {"iataCode": "IST", "at": "2026-03-24T09:00:00"},
                                    "carrierCode": "TK",
                                },
                                {
                                    "departure": {"iataCode": "IST", "at": "2026-03-24T09:45:00"},
                                    "arrival": {"iataCode": "OTP", "at": "2026-03-24T12:00:00"},
                                    "carrierCode": "TK",
                                },
                            ],
                        },
                    ],
                },
                {
                    "price": {"grandTotal": "900", "currency": "EUR"},
                    "itineraries": [
                        {
                            "duration": "PT4H",
                            "segments": [
                                {
                                    "departure": {"iataCode": "OTP", "at": "2026-03-10T08:00:00"},
                                    "arrival": {"iataCode": "MGA", "at": "2026-03-10T12:00:00"},
                                    "carrierCode": "TK",
                                }
                            ],
                        },
                        {
                            "duration": "PT4H",
                            "segments": [
                                {
                                    "departure": {"iataCode": "MGA", "at": "2026-03-24T08:00:00"},
                                    "arrival": {"iataCode": "OTP", "at": "2026-03-24T12:00:00"},
                                    "carrierCode": "TK",
                                }
                            ],
                        },
                    ],
                },
            ]
        },
    )
    best_return = tiebreak_client.get_best_return(
        "OTP", "MGA", "2026-03-10", "2026-03-24", "EUR", 1, 1, 0, 0
    )
    assert best_return is not None
    assert best_return["outbound_stops"] == 0
    assert best_return["inbound_stops"] == 0

    oneway_tiebreak_client = AmadeusClient(client_id="id", client_secret="secret")
    monkeypatch.setattr(oneway_tiebreak_client, "is_configured", lambda: True)
    monkeypatch.setattr(
        oneway_tiebreak_client,
        "_get",
        lambda path, params: {
            "data": [
                {
                    "price": {"grandTotal": "500", "currency": "EUR"},
                    "itineraries": [
                        {
                            "segments": [
                                {
                                    "departure": {"iataCode": "OTP", "at": "2026-03-10T08:00:00"},
                                    "arrival": {"iataCode": "IST", "at": "2026-03-10T09:00:00"},
                                    "carrierCode": "TK",
                                },
                                {
                                    "departure": {"iataCode": "IST", "at": "2026-03-10T09:45:00"},
                                    "arrival": {"iataCode": "MGA", "at": "2026-03-10T12:00:00"},
                                    "carrierCode": "TK",
                                },
                            ]
                        }
                    ],
                },
                {
                    "price": {"grandTotal": "500", "currency": "EUR"},
                    "itineraries": [
                        {
                            "segments": [
                                {
                                    "departure": {"iataCode": "OTP", "at": "2026-03-10T08:00:00"},
                                    "arrival": {"iataCode": "MGA", "at": "2026-03-10T12:00:00"},
                                    "carrierCode": "TK",
                                }
                            ]
                        }
                    ],
                },
            ]
        },
    )
    best_oneway = oneway_tiebreak_client.get_best_oneway(
        "OTP",
        "MGA",
        "2026-03-10",
        "EUR",
        1,
        1,
        0,
        0,
    )
    assert best_oneway is not None
    assert best_oneway["stops"] == 0

    cheaper_return_client = AmadeusClient(client_id="id", client_secret="secret")
    monkeypatch.setattr(cheaper_return_client, "is_configured", lambda: True)
    monkeypatch.setattr(
        cheaper_return_client,
        "_get",
        lambda path, params: {
            "data": [
                {
                    "price": {"grandTotal": "900", "currency": "EUR"},
                    "itineraries": [
                        {
                            "segments": [
                                {
                                    "departure": {"iataCode": "OTP", "at": "2026-03-10T08:00:00"},
                                    "arrival": {"iataCode": "MGA", "at": "2026-03-10T12:00:00"},
                                    "carrierCode": "TK",
                                }
                            ]
                        },
                        {
                            "segments": [
                                {
                                    "departure": {"iataCode": "MGA", "at": "2026-03-24T08:00:00"},
                                    "arrival": {"iataCode": "OTP", "at": "2026-03-24T12:00:00"},
                                    "carrierCode": "TK",
                                }
                            ]
                        },
                    ],
                },
                {
                    "price": {"grandTotal": "850", "currency": "EUR"},
                    "itineraries": [
                        {
                            "segments": [
                                {
                                    "departure": {"iataCode": "OTP", "at": "2026-03-10T08:00:00"},
                                    "arrival": {"iataCode": "MGA", "at": "2026-03-10T12:00:00"},
                                    "carrierCode": "TK",
                                }
                            ]
                        },
                        {
                            "segments": [
                                {
                                    "departure": {"iataCode": "MGA", "at": "2026-03-24T08:00:00"},
                                    "arrival": {"iataCode": "OTP", "at": "2026-03-24T12:00:00"},
                                    "carrierCode": "TK",
                                }
                            ]
                        },
                    ],
                },
            ]
        },
    )
    cheaper_return = cheaper_return_client.get_best_return(
        "OTP",
        "MGA",
        "2026-03-10",
        "2026-03-24",
        "EUR",
        1,
        1,
        0,
        0,
    )
    assert cheaper_return is not None
    assert cheaper_return["price"] == 850
