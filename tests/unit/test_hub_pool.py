from __future__ import annotations

from src.data.hub_pool import AUTO_HUB_CANDIDATES, HUB_CONNECTIVITY_SEEDS


def test_hub_pool_expansion_invariants() -> None:
    hub_candidates = AUTO_HUB_CANDIDATES
    seed_codes = HUB_CONNECTIVITY_SEEDS

    hub_set = set(hub_candidates)
    seed_set = set(seed_codes)

    assert hub_candidates
    assert seed_codes
    assert len(hub_candidates) == len(hub_set)
    assert len(seed_codes) == len(seed_set)
    assert seed_set.issubset(hub_set)
    assert len(hub_set) >= 700

    for must_exist in ("FRA", "LHR", "HNL", "DXB", "CDG", "PTY"):
        assert must_exist in hub_set

    for code in hub_candidates:
        assert len(code) == 3
        assert code.isalpha()
        assert code == code.upper()
