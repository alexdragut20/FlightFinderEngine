from __future__ import annotations

from flight_layover_lab.airports import AirportCoordinates
from flight_layover_lab.engine import SplitTripOptimizer
from flight_layover_lab.providers import KiwiClient
from flight_layover_lab.route_graph import RouteConnectivityGraph, _normalize_codes


def test_route_graph_scores_bridge_hubs_on_outbound_and_return_paths() -> None:
    graph = RouteConnectivityGraph()
    graph._loaded = True
    graph._outgoing = {
        "OTP": {"IST", "FRA"},
        "IST": {"BKK"},
        "BKK": {"USM"},
        "USM": {"DMK"},
        "DMK": {"RUH"},
        "RUH": {"OTP"},
        "FRA": {"SIN"},
    }
    graph._incoming = {
        "IST": {"OTP"},
        "BKK": {"IST"},
        "USM": {"BKK"},
        "DMK": {"USM"},
        "RUH": {"DMK"},
        "OTP": {"RUH"},
        "SIN": {"FRA"},
    }

    scores = graph.score_path_hubs(
        origins=("OTP",),
        destinations=("USM",),
        max_split_hubs=2,
    )

    assert set(scores) >= {"IST", "BKK", "DMK", "RUH"}
    assert "FRA" not in scores
    assert scores["BKK"] > 0
    assert scores["IST"] > 0


def test_optimizer_expands_hubs_from_route_graph_before_fallback_pool() -> None:
    class StubRouteGraph:
        def available(self) -> bool:
            return True

        def score_path_hubs(
            self,
            *,
            origins: tuple[str, ...],
            destinations: tuple[str, ...],
            max_split_hubs: int,
        ) -> dict[str, int]:
            assert origins == ("OTP",)
            assert destinations == ("USM",)
            assert max_split_hubs == 2
            return {
                "IST": 900,
                "BKK": 800,
                "RUH": 700,
            }

    optimizer = SplitTripOptimizer(KiwiClient(), AirportCoordinates())
    optimizer.route_graph = StubRouteGraph()
    config = optimizer.parse_search_config(
        {
            "origins": ["OTP"],
            "destinations": ["USM"],
            "period_start": "2026-04-20",
            "period_end": "2026-05-10",
            "hub_candidates": ["MXP", "SIN", "IST"],
            "max_transfers_per_direction": 2,
        }
    )

    expanded_config, meta, warnings = optimizer._expand_route_graph_hub_candidates(config)

    assert expanded_config.hub_candidates[:5] == ("IST", "BKK", "RUH", "MXP", "SIN")
    assert meta["hub_candidates_graph_count"] == 3
    assert meta["hub_candidates_graph_applied"] is True
    assert any(
        "Route-graph auto discovery expanded the hub pool" in warning for warning in warnings
    )


def test_route_graph_loading_and_availability_paths(monkeypatch, tmp_path) -> None:
    cache_path = tmp_path / "routes.dat"
    monkeypatch.setattr("flight_layover_lab.route_graph.CACHE_DIR", tmp_path)
    monkeypatch.setattr("flight_layover_lab.route_graph.ROUTES_CACHE_PATH", cache_path)

    class _Response:
        text = "X,Y,OTP,Z,IST\nX,Y,IST,Z,BKK\nX,Y,USM,Z,DMK\nbad,row\nX,Y,OTP,Z,\\N\n"

        def raise_for_status(self) -> None:
            return None

    monkeypatch.setattr(
        "flight_layover_lab.route_graph.requests.get", lambda *args, **kwargs: _Response()
    )

    graph = RouteConnectivityGraph()
    assert graph.available() is True
    assert graph.outgoing("otp") == {"IST"}
    assert graph.incoming("bkk") == {"IST"}
    assert graph.score_path_hubs(origins=("OTP",), destinations=("BKK",), max_split_hubs=0) == {}

    cache_path.unlink()
    monkeypatch.setattr(
        "flight_layover_lab.route_graph.requests.get",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("download failed")),
    )
    missing_graph = RouteConnectivityGraph()
    assert missing_graph.available() is False


def test_route_graph_edge_loading_and_normalization_paths(monkeypatch, tmp_path) -> None:
    assert _normalize_codes([" otp ", "", "TOOLONG", "bkk"]) == {"OTP", "BKK"}

    class _BrokenRoutePath:
        def exists(self) -> bool:
            return True

        def open(self, *args: object, **kwargs: object) -> object:
            raise OSError("broken cache")

    monkeypatch.setattr("flight_layover_lab.route_graph.CACHE_DIR", tmp_path)
    monkeypatch.setattr("flight_layover_lab.route_graph.ROUTES_CACHE_PATH", _BrokenRoutePath())

    broken_graph = RouteConnectivityGraph()
    assert broken_graph.available() is False

    graph = RouteConnectivityGraph()
    graph._loaded = True
    graph._outgoing = {
        "OTP": {"IST"},
        "BKK": {"OTP"},
    }
    graph._incoming = {
        "USM": {"IST"},
        "OTP": {"BKK"},
    }

    assert graph.score_path_hubs(origins=("OTP",), destinations=("USM",), max_split_hubs=1) == {
        "IST": 320
    }
    assert graph.score_path_hubs(origins=("XXX",), destinations=("YYY",), max_split_hubs=2) == {}
