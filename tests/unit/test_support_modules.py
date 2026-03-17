from __future__ import annotations

import importlib
import runpy
import warnings
from datetime import date
from pathlib import Path

from src import app as app_module
from src import config as config_module
from src import utils as utils_module
from src.data import airports as airports_module
from src.data import resources as resources_module
from src.data.airports import AirportCoordinates
from src.providers._cache import _build_cache_key, per_instance_lru_cache
from src.services import progress as progress_module
from src.services import route_graph as route_graph_module
from src.services.progress import SearchProgressTracker
from src.services.search_jobs import SearchJob, SearchJobStore


class _FakeResponse:
    def __init__(self, payload: object, *, text: str = "", status_code: int = 200) -> None:
        self._payload = payload
        self.text = text
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self) -> object:
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


def test_app_module_reexports_core_symbols() -> None:
    assert app_module.AppHandler is not None
    assert app_module.SplitTripOptimizer is not None
    assert callable(app_module.normalize_codes)
    assert callable(app_module.convert_currency_amount)


def test_package_entrypoints_delegate_to_run_server(monkeypatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(
        "src.services.http_server.run_server",
        lambda: calls.append("run"),
    )

    runpy.run_module("src.__main__", run_name="__main__")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        runpy.run_module("src.app", run_name="__main__")

    assert calls == ["run", "run"]


def test_reload_config_handles_invalid_env_values(monkeypatch) -> None:
    monkeypatch.setenv("DEFAULT_SEARCH_TIMEOUT_SECONDS", "oops")
    monkeypatch.setenv("PROVIDER_ERROR_COOLDOWN_SECONDS", "oops")
    monkeypatch.setenv("MIN_SPLIT_CONNECTION_SAME_AIRPORT_MINUTES", "oops")
    monkeypatch.setenv("MIN_SPLIT_CONNECTION_CROSS_AIRPORT_MINUTES", "oops")
    monkeypatch.setenv("KIWI_ITINERARY_SCAN_LIMIT", "oops")
    monkeypatch.setenv("SERPAPI_RETURN_OPTION_SCAN_LIMIT", "oops")
    monkeypatch.setenv("KAYAK_SCRAPE_POLL_ROUNDS", "oops")
    monkeypatch.setenv("SKYSCANNER_SCRAPE_HTTP_RETRIES", "oops")
    monkeypatch.setenv("SKYSCANNER_PLAYWRIGHT_MAX_CONCURRENCY", "oops")
    monkeypatch.setenv("SKYSCANNER_PLAYWRIGHT_HOST_ATTEMPTS", "oops")
    monkeypatch.setenv("SKYSCANNER_PLAYWRIGHT_ACQUIRE_TIMEOUT_SECONDS", "oops")
    monkeypatch.setenv("SKYSCANNER_WAF_COOLDOWN_SECONDS", "oops")
    monkeypatch.setenv("SKYSCANNER_PLAYWRIGHT_ERROR_COOLDOWN_SECONDS", "oops")
    monkeypatch.setenv("SKYSCANNER_SCRAPE_HOSTS", "one.example, two.example ")
    monkeypatch.setenv("ALLOW_PLAYWRIGHT_PROVIDERS", "yes")
    monkeypatch.setenv("SKYSCANNER_SCRAPE_PLAYWRIGHT_FALLBACK", "yes")

    reloaded = importlib.reload(config_module)
    try:
        assert reloaded.DEFAULT_SEARCH_TIMEOUT_SECONDS == 1500
        assert reloaded.PROVIDER_ERROR_COOLDOWN_SECONDS == 300
        assert reloaded.MIN_SPLIT_CONNECTION_SAME_AIRPORT_SECONDS == 120 * 60
        assert reloaded.MIN_SPLIT_CONNECTION_CROSS_AIRPORT_SECONDS == 300 * 60
        assert reloaded.KIWI_ITINERARY_SCAN_LIMIT == 50
        assert reloaded.SERPAPI_RETURN_OPTION_SCAN_LIMIT == 2
        assert reloaded.KAYAK_SCRAPE_POLL_ROUNDS == 2
        assert reloaded.SKYSCANNER_SCRAPE_HTTP_RETRIES == 2
        assert reloaded.SKYSCANNER_PLAYWRIGHT_MAX_CONCURRENCY == 1
        assert reloaded.SKYSCANNER_PLAYWRIGHT_HOST_ATTEMPTS == 1
        assert reloaded.SKYSCANNER_PLAYWRIGHT_ACQUIRE_TIMEOUT_SECONDS == 6.0
        assert reloaded.SKYSCANNER_WAF_COOLDOWN_SECONDS == 900
        assert reloaded.SKYSCANNER_PLAYWRIGHT_ERROR_COOLDOWN_SECONDS == 300
        assert reloaded.SKYSCANNER_SCRAPE_HOSTS == ["one.example", "two.example"]
        assert reloaded.SKYSCANNER_SCRAPE_PLAYWRIGHT_FALLBACK is True
    finally:
        monkeypatch.delenv("DEFAULT_SEARCH_TIMEOUT_SECONDS", raising=False)
        monkeypatch.delenv("PROVIDER_ERROR_COOLDOWN_SECONDS", raising=False)
        monkeypatch.delenv("MIN_SPLIT_CONNECTION_SAME_AIRPORT_MINUTES", raising=False)
        monkeypatch.delenv("MIN_SPLIT_CONNECTION_CROSS_AIRPORT_MINUTES", raising=False)
        monkeypatch.delenv("KIWI_ITINERARY_SCAN_LIMIT", raising=False)
        monkeypatch.delenv("SERPAPI_RETURN_OPTION_SCAN_LIMIT", raising=False)
        monkeypatch.delenv("KAYAK_SCRAPE_POLL_ROUNDS", raising=False)
        monkeypatch.delenv("SKYSCANNER_SCRAPE_HTTP_RETRIES", raising=False)
        monkeypatch.delenv("SKYSCANNER_PLAYWRIGHT_MAX_CONCURRENCY", raising=False)
        monkeypatch.delenv("SKYSCANNER_PLAYWRIGHT_HOST_ATTEMPTS", raising=False)
        monkeypatch.delenv("SKYSCANNER_PLAYWRIGHT_ACQUIRE_TIMEOUT_SECONDS", raising=False)
        monkeypatch.delenv("SKYSCANNER_WAF_COOLDOWN_SECONDS", raising=False)
        monkeypatch.delenv("SKYSCANNER_PLAYWRIGHT_ERROR_COOLDOWN_SECONDS", raising=False)
        monkeypatch.delenv("SKYSCANNER_SCRAPE_HOSTS", raising=False)
        monkeypatch.delenv("ALLOW_PLAYWRIGHT_PROVIDERS", raising=False)
        monkeypatch.delenv("SKYSCANNER_SCRAPE_PLAYWRIGHT_FALLBACK", raising=False)
        importlib.reload(config_module)


def test_resolve_project_root_prefers_env_var(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("FLIGHT_LAYOVER_LAB_ROOT", str(tmp_path))

    assert resources_module.resolve_project_root() == tmp_path.resolve()


def test_airport_coordinates_downloads_and_reads_cache(monkeypatch, tmp_path: Path) -> None:
    cache_path = tmp_path / "airports.dat"
    monkeypatch.setattr(airports_module, "CACHE_DIR", tmp_path)
    monkeypatch.setattr(airports_module, "AIRPORTS_CACHE_PATH", cache_path)
    monkeypatch.setattr(
        airports_module.requests,
        "get",
        lambda *args, **kwargs: _FakeResponse(
            {},
            text="1,Test Airport,Test City,Country,TST,ICAO,10.5,20.5\n",
        ),
    )

    airports = AirportCoordinates()

    assert airports.get("TST") == (10.5, 20.5)
    assert airports.display_name("TST") == "Test City"
    assert cache_path.exists()


def test_airport_coordinates_gracefully_handles_invalid_cache(monkeypatch, tmp_path: Path) -> None:
    cache_path = tmp_path / "airports.dat"
    cache_path.write_text("1,Bad Airport,Bad City,Country,\\N,ICAO,abc,20.0\n", encoding="utf-8")
    monkeypatch.setattr(airports_module, "CACHE_DIR", tmp_path)
    monkeypatch.setattr(airports_module, "AIRPORTS_CACHE_PATH", cache_path)

    airports = AirportCoordinates()

    assert airports.get("OTP") == config_module.FALLBACK_COORDS["OTP"]
    assert airports.get("BAD") is None
    assert airports.display_name("BAD") is None


def test_airport_coordinates_cover_empty_codes_download_failure_and_label_fallbacks(
    monkeypatch,
    tmp_path: Path,
) -> None:
    cache_path = tmp_path / "airports.dat"
    monkeypatch.setattr(airports_module, "CACHE_DIR", tmp_path)
    monkeypatch.setattr(airports_module, "AIRPORTS_CACHE_PATH", cache_path)

    monkeypatch.setattr(
        airports_module.requests,
        "get",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("download failed")),
    )
    failed = AirportCoordinates()
    assert failed.get("") is None
    assert failed.display_name("") is None
    assert failed.get("OTP") == config_module.FALLBACK_COORDS["OTP"]

    cache_path.write_text(
        "1,Airport Name,\\N,Country,TST,ICAO,10.0,20.0\n"
        "2,Short Row\n"
        "3,Airport Name,City,Country,TWO,ICAO,bad,20.0\n",
        encoding="utf-8",
    )
    loaded = AirportCoordinates()
    assert loaded.get("TST") == (10.0, 20.0)
    assert loaded.display_name("TST") == "Airport Name"
    assert loaded.get("TWO") is None


def test_airport_coordinates_loading_failures_cover_download_and_open_errors(
    monkeypatch,
    tmp_path: Path,
) -> None:
    missing_cache_path = tmp_path / "missing-airports.dat"
    monkeypatch.setattr(airports_module, "CACHE_DIR", tmp_path)
    monkeypatch.setattr(airports_module, "AIRPORTS_CACHE_PATH", missing_cache_path)
    monkeypatch.setattr(
        airports_module.requests,
        "get",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("download failed")),
    )

    missing = AirportCoordinates()
    assert missing.get("TST") is None

    class _BrokenAirportPath:
        def exists(self) -> bool:
            return True

        def open(self, *args: object, **kwargs: object) -> object:
            raise OSError("broken cache")

    monkeypatch.setattr(airports_module, "AIRPORTS_CACHE_PATH", _BrokenAirportPath())
    broken = AirportCoordinates()
    assert broken.get("ZZZ") is None


def test_utils_cover_basic_normalization_and_ranges() -> None:
    assert utils_module.normalize_codes("otp; bbu,otp", ["OTP"]) == ("OTP", "BBU")
    assert utils_module.normalize_codes([], ["OTP"]) == ("OTP",)
    assert utils_module.normalize_provider_ids("all") == config_module.SUPPORTED_PROVIDER_IDS
    assert utils_module.normalize_provider_ids(["kiwi", "bad", "KIWI"]) == ("kiwi",)
    assert utils_module.to_date("", date(2026, 1, 1)) == date(2026, 1, 1)
    assert utils_module.clamp_int("7", fallback=1, low=2, high=5) == 5
    assert utils_module.clamp_int("x", fallback=4, low=2, high=5) == 4
    assert utils_module.clamp_optional_int("0", fallback=9, low=1, high=5) is None
    assert utils_module.clamp_optional_int("", fallback=3, low=1, high=5) == 3
    assert utils_module.bounded_io_concurrency("bad") == 8
    assert utils_module.to_bool("YES") is True
    assert utils_module.to_bool("off", fallback=True) is False
    assert utils_module.to_bool("maybe", fallback=True) is True
    assert utils_module.date_range(date(2026, 1, 1), date(2026, 1, 3)) == [
        date(2026, 1, 1),
        date(2026, 1, 2),
        date(2026, 1, 3),
    ]
    assert utils_module.date_range(date(2026, 1, 3), date(2026, 1, 1)) == []
    assert utils_module.haversine_km((0.0, 0.0), (0.0, 0.0)) == 0.0


def test_utils_cover_urls_and_itinerary_helpers() -> None:
    assert "maxStopsCount=1" in utils_module.kiwi_oneway_url("otp", "mga", "2026-03-10", 1)
    assert "return=2026-03-24" in utils_module.kiwi_return_url(
        "otp",
        "mga",
        "2026-03-10",
        "2026-03-24",
        2,
    )
    assert utils_module.kiwi_oneway_url("", "", "2026-03-10").endswith("/results/")
    assert utils_module.absolute_kiwi_url("/deep") == "https://www.kiwi.com/deep"
    assert utils_module.absolute_kiwi_url("https://example.com") == "https://example.com"
    assert (
        utils_module.absolute_kayak_url("/flights", host="www.kayak.com")
        == "https://www.kayak.com/flights"
    )
    assert (
        utils_module.itinerary_booking_url(
            {"bookingOptions": {"edges": [{"node": {"bookingUrl": "/deep"}}]}}
        )
        == "https://www.kiwi.com/deep"
    )
    assert utils_module.itinerary_booking_url({"bookingOptions": {"edges": "bad"}}) is None


def test_utils_cover_time_and_money_parsing(monkeypatch) -> None:
    assert utils_module.parse_local_datetime("2026-03-10T08:00:00Z") is not None
    assert utils_module.parse_local_datetime("bad") is None
    assert utils_module.connection_gap_seconds("2026-03-10T08:00:00", "2026-03-10T10:30:00") == 9000
    assert utils_module.connection_gap_seconds("2026-03-10T10:30:00", "2026-03-10T08:00:00") is None
    assert utils_module.max_segment_layover_seconds(None) == 0
    assert utils_module.max_segment_layover_seconds([{"arrive_local": "", "depart_local": ""}]) == 0
    assert (
        utils_module.max_segment_layover_seconds(
            [
                {"arrive_local": "2026-03-10T10:00:00"},
                {"depart_local": "2026-03-10T13:30:00", "arrive_local": "2026-03-10T15:00:00"},
                {"depart_local": "2026-03-10T18:45:00"},
            ]
        )
        == 13500
    )
    assert (
        utils_module.minimum_split_boundary_connection_seconds("OTP", "OTP")
        == config_module.MIN_SPLIT_CONNECTION_SAME_AIRPORT_SECONDS
    )
    assert utils_module.date_only("2026-03-10T09:00:00") == "2026-03-10"
    assert utils_module.parse_iso8601_duration_seconds("P1DT2H3M4S") == 93784
    assert utils_module.parse_iso8601_duration_seconds("bad") is None
    assert utils_module.parse_duration_text_seconds("3h 10m") == 11400
    assert utils_module.parse_duration_text_seconds("45") == 2700
    assert utils_module.parse_duration_text_seconds("invalid") is None
    assert utils_module.parse_money_amount_int(123.4) == 123
    assert utils_module.parse_money_amount_int("1.234,56 lei") == 1235
    assert utils_module.parse_money_amount_int("12,34") == 12
    assert utils_module.parse_money_amount_int("bad") is None

    with config_module._FX_CACHE_LOCK:
        config_module._FX_RATE_CACHE.clear()

    monkeypatch.setattr(
        utils_module.requests,
        "get",
        lambda *args, **kwargs: _FakeResponse(
            {"result": "success", "rates": {"RON": 4.5, "USD": 1.0}},
        ),
    )

    assert utils_module._get_fx_rates("USD") == {"RON": 4.5, "USD": 1.0}
    assert utils_module.convert_currency_amount(10, "USD", "RON") == 45
    assert utils_module.convert_currency_amount("bad", "USD", "RON") is None
    assert utils_module.convert_currency_amount(10, "", "RON") is None
    assert utils_module.convert_currency_amount(10, "USD", "USD") == 10


def test_utils_cover_datetime_guess_links_and_segments(monkeypatch) -> None:
    assert utils_module.parse_datetime_guess("2026-03-10T08:00:00") == "2026-03-10T08:00:00"
    assert utils_module.parse_datetime_guess("Mar 10, 2026 08:00 PM") == "2026-03-10T20:00:00"
    assert (
        utils_module.parse_google_flights_text_datetime("5:50 AM on Thu, Mar 12", "2026-03-12")
        == "2026-03-12T05:50:00"
    )
    assert utils_module.parse_google_flights_text_datetime("bad", "2026-03-12") is None

    links = utils_module.build_comparison_links(
        "OTP",
        "MGA",
        "2026-03-10",
        "2026-03-24",
        adults=2,
        max_stops_per_leg=0,
        currency="EUR",
    )
    assert "google.com" in links["google_flights"]
    assert "preferdirects=true" in links["skyscanner"]
    assert "stops=0" in links["kayak"]
    assert utils_module.build_comparison_links("", "MGA", "2026-03-10", "2026-03-24") == {}

    segments = [
        {"from": "otp", "to": "ist"},
        {"from": "SAW", "to": "mga"},
    ]
    assert utils_module.leg_endpoints_from_segments(segments, "OTP", "MGA") == ("OTP", "MGA")
    assert utils_module.leg_endpoints_from_segments(None, "OTP", "MGA") == ("OTP", "MGA")
    assert utils_module.transfer_events_from_segments(segments) == 2
    assert utils_module.transfer_events_from_segments([]) == 0
    assert utils_module.boundary_transfer_events("OTP", "OTP") == 1
    assert utils_module.boundary_transfer_events("OTP", "BBU") == 2


def test_utils_and_cache_cover_additional_error_paths_and_eviction(monkeypatch) -> None:
    assert utils_module.normalize_provider_ids(["bad"]) == ("kiwi",)
    assert utils_module.normalize_provider_ids(None) == config_module.SUPPORTED_PROVIDER_IDS
    assert utils_module.absolute_kiwi_url(None) is None
    assert utils_module.absolute_kiwi_url("deep/path") == "https://www.kiwi.com/deep/path"
    assert utils_module.absolute_kayak_url(None) is None
    assert utils_module.connection_gap_seconds("bad", "2026-03-10T10:00:00") is None
    assert (
        utils_module.minimum_split_boundary_connection_seconds("OTP", "BBU")
        == config_module.MIN_SPLIT_CONNECTION_CROSS_AIRPORT_SECONDS
    )
    assert utils_module.parse_money_amount_int("1,234,567") == 1234567
    assert utils_module.parse_datetime_guess("2026-03-10Tbad") is None
    guessed = utils_module.parse_google_flights_text_datetime("5:50 AM on Mar 12", "bad-date")
    assert guessed is not None and guessed.endswith("T05:50:00")
    links = utils_module.build_comparison_links(
        "OTP",
        "MGA",
        "2026-03-10",
        "2026-03-24",
        adults=1,
        max_stops_per_leg=2,
        currency="EUR",
    )
    assert "kayak.com" in links["kayak"]
    assert utils_module.transfer_events_from_segments([{"to": "OTP"}, {"from": "BBU"}]) == 2

    with config_module._FX_CACHE_LOCK:
        config_module._FX_RATE_CACHE.clear()
    with utils_module._FX_CACHE_LOCK:
        utils_module._FX_RATE_CACHE.clear()

    monkeypatch.setattr(utils_module.requests, "get", lambda *args, **kwargs: _FakeResponse("bad"))
    assert utils_module._get_fx_rates("CAD") is None
    monkeypatch.setattr(
        utils_module.requests,
        "get",
        lambda *args, **kwargs: _FakeResponse({"result": "error", "rates": {}}),
    )
    assert utils_module._get_fx_rates("AUD") is None
    monkeypatch.setattr(
        utils_module.requests,
        "get",
        lambda *args, **kwargs: _FakeResponse(
            {"result": "success", "rates": {"RON": "bad", "USD": float("inf"), "EUR": 0}}
        ),
    )
    assert utils_module._get_fx_rates("CHF") is None

    clock = {"now": 100.0}
    monkeypatch.setattr(utils_module.time, "time", lambda: clock["now"])
    monkeypatch.setattr(
        utils_module.requests,
        "get",
        lambda *args, **kwargs: _FakeResponse(
            {"result": "success", "rates": {"RON": 4.5, "USD": 1.0}}
        ),
    )
    cached_rates = utils_module._get_fx_rates("USD")
    assert cached_rates == {"RON": 4.5, "USD": 1.0}
    monkeypatch.setattr(
        utils_module.requests,
        "get",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("network should not be used")),
    )
    clock["now"] += 1
    assert utils_module._get_fx_rates("USD") == {"RON": 4.5, "USD": 1.0}
    assert utils_module.convert_currency_amount(float("nan"), "USD", "RON") is None
    assert utils_module.convert_currency_amount(10, "USD", "GBP") is None

    class _Cached:
        def __init__(self) -> None:
            self.calls = 0

        @per_instance_lru_cache(maxsize=1)
        def compute(self, value: int, *, bonus: int = 0) -> int:
            self.calls += 1
            return self.calls + value + bonus

    first = _Cached()
    assert _build_cache_key((1,), {"bonus": 2}) != (1,)
    assert first.compute(1, bonus=2) == 4
    assert first.compute(1, bonus=2) == 4
    assert first.calls == 1
    assert first.compute(2, bonus=0) == 4
    assert first.compute(1, bonus=2) == 6
    assert first.calls == 3

    second = _Cached()
    assert second.compute(1, bonus=2) == 4
    assert second.calls == 1


def test_support_edge_paths_cover_progress_route_graph_search_jobs_and_more(
    monkeypatch,
    tmp_path: Path,
) -> None:
    clock = {"now": 100.0}
    monkeypatch.setattr(progress_module.time, "time", lambda: clock["now"])

    tracker = SearchProgressTracker("job-support-edge")
    tracker.start_phase("setup", total=1, detail="Setup.")
    tracker.advance_phase("calendar", step=1)
    clock["now"] += 5
    tracker.complete_phase("setup")
    clock["now"] += 10
    tracker.start_phase("build", total=10, detail="Building.")
    clock["now"] += 10
    tracker.advance_phase("build", completed=5, total=10)
    running = tracker.snapshot()
    assert running["eta_seconds"] is not None

    with tracker._lock:
        tracker._phase_state["build"]["total"] = 0
        tracker._maybe_log_bucket_locked("build")
        tracker._phase_state["build"]["total"] = 10
        tracker._phase_state["build"]["progress"] = 0.0
        tracker._phase_log_buckets["build"] = 0
        tracker._maybe_log_bucket_locked("build")
        assert tracker._estimate_eta_locked(clock["now"], 0.01, 1.0) is not None

    empty_tracker = SearchProgressTracker("job-support-empty")
    with empty_tracker._lock:
        assert empty_tracker._estimate_eta_locked(clock["now"], 0.01, 1.0) is None
    pending_tracker = SearchProgressTracker("job-support-pending")
    pending_tracker.start_phase("build", total=10, detail="Building.")
    with pending_tracker._lock:
        assert pending_tracker._estimate_eta_locked(clock["now"], 0.01, 1.0) is None

    assert utils_module.normalize_codes(["", "otp", "   "], ["BBU"]) == ("OTP",)
    assert utils_module.clamp_optional_int("bad", fallback=7, low=1, high=9) == 7
    assert utils_module.kiwi_return_url("", "", "2026-03-10", "2026-03-24").endswith("/results/")
    assert "maxStopsCount" not in utils_module.kiwi_return_url(
        "OTP",
        "MGA",
        "2026-03-10",
        "2026-03-24",
        None,
    )
    assert (
        utils_module.absolute_kayak_url("https://example.test/path") == "https://example.test/path"
    )
    assert utils_module.parse_local_datetime("") is None
    assert (
        utils_module.max_segment_layover_seconds(
            [
                {"arrive_local": "bad"},
                {"depart_local": "2026-03-10T10:00:00"},
            ]
        )
        == 0
    )
    assert utils_module.parse_iso8601_duration_seconds("") is None
    assert utils_module.parse_money_amount_int("   ") is None
    assert utils_module.parse_money_amount_int("1,234.56") == 1235
    assert utils_module.parse_money_amount_int(float("nan")) is None
    assert utils_module.parse_money_amount_int("1.2.3") is None
    assert utils_module._get_fx_rates("") is None
    assert utils_module.parse_datetime_guess("") is None
    assert utils_module.convert_currency_amount(None, "USD", "RON") is None

    with config_module._FX_CACHE_LOCK:
        config_module._FX_RATE_CACHE.clear()
    with utils_module._FX_CACHE_LOCK:
        utils_module._FX_RATE_CACHE.clear()

    monkeypatch.setattr(
        utils_module.requests,
        "get",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("fx down")),
    )
    assert utils_module._get_fx_rates("EUR") is None

    monkeypatch.setattr(
        utils_module.requests,
        "get",
        lambda *args, **kwargs: _FakeResponse({"result": "success", "rates": []}),
    )
    assert utils_module._get_fx_rates("GBP") is None

    monkeypatch.setattr(
        utils_module.requests,
        "get",
        lambda *args, **kwargs: _FakeResponse({"result": "success", "rates": {"": 4.0}}),
    )
    assert utils_module._get_fx_rates("CAD") is None
    monkeypatch.setattr(utils_module, "_get_fx_rates", lambda *_args, **_kwargs: None)
    assert utils_module.convert_currency_amount(10, "USD", "RON") is None

    links = utils_module.build_comparison_links(
        "OTP",
        "MGA",
        "2026-03-10",
        "2026-03-24",
        adults=1,
        max_stops_per_leg=2,
        currency="EUR",
    )
    assert "with+up+to+2+stops" in links["google_flights"]

    graph = route_graph_module.RouteConnectivityGraph()
    graph._loaded = True
    graph._outgoing = {
        "OTP": {"IST"},
        "IST": {"IST", "USM", "BKK"},
        "USM": {"DXB"},
        "DXB": {"OTP"},
    }
    graph._incoming = {
        "IST": {"OTP"},
        "USM": {"IST"},
        "OTP": {"DXB"},
    }
    blocked_scores = graph.score_path_hubs(origins=["OTP"], destinations=["IST"], max_split_hubs=2)
    assert "IST" not in blocked_scores
    assert blocked_scores["USM"] > 0
    assert blocked_scores["DXB"] > 0
    assert graph.score_path_hubs(origins=["OTP"], destinations=["ZZZ"], max_split_hubs=2) == {}
    graph._outgoing = {"OTP": {"LONG"}}
    graph._incoming = {"IST": {"LONG"}}
    assert graph.score_path_hubs(origins=["OTP"], destinations=["IST"], max_split_hubs=1) == {}

    monkeypatch.setattr(route_graph_module, "CACHE_DIR", tmp_path)

    class _BrokenRoutesPath:
        def exists(self) -> bool:
            return True

        def open(self, *args: object, **kwargs: object) -> object:
            raise OSError("routes broken")

    monkeypatch.setattr(route_graph_module, "ROUTES_CACHE_PATH", _BrokenRoutesPath())
    broken_graph = route_graph_module.RouteConnectivityGraph()
    assert broken_graph.available() is False

    store = SearchJobStore(max_jobs=4, ttl_seconds=300)
    expired_job = SearchJob(
        job_id="expired",
        progress=SearchProgressTracker("expired"),
        finished_at=clock["now"] - 1000,
    )
    store._jobs["expired"] = expired_job
    with store._lock:
        store._prune_locked()
    assert store.get_job("expired") is None
