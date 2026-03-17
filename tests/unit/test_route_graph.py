from __future__ import annotations

from flight_layover_lab.airports import AirportCoordinates
from flight_layover_lab.engine import SplitTripOptimizer
from flight_layover_lab.providers import KiwiClient
from flight_layover_lab.route_graph import RouteConnectivityGraph


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
