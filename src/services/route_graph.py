from __future__ import annotations

import csv

import requests

from ..config import ROUTES_DATA_URL
from ..data.resources import CACHE_DIR, ROUTES_CACHE_PATH


def _normalize_codes(codes: list[str] | tuple[str, ...] | set[str]) -> set[str]:
    """Normalize a collection of airport codes.

    Args:
        codes: Airport or provider codes to process.

    Returns:
        set[str]: Normalized collection of airport codes.
    """
    normalized: set[str] = set()
    for code in codes:
        value = str(code or "").strip().upper()
        if len(value) == 3:
            normalized.add(value)
    return normalized


class RouteConnectivityGraph:
    """Route graph used to score likely split-ticket hub airports."""

    def __init__(self) -> None:
        """Initialize the RouteConnectivityGraph."""
        self._loaded = False
        self._outgoing: dict[str, set[str]] = {}
        self._incoming: dict[str, set[str]] = {}

    def available(self) -> bool:
        """Return whether the route graph is available for use.

        Returns:
            bool: True when the route graph is available for use; otherwise, False.
        """
        self._ensure_loaded()
        return bool(self._outgoing)

    def outgoing(self, code: str) -> set[str]:
        """Return outgoing connections for an airport code.

        Args:
            code: Airport or provider code to process.

        Returns:
            set[str]: Outgoing connections for an airport code.
        """
        self._ensure_loaded()
        normalized = str(code or "").strip().upper()
        return set(self._outgoing.get(normalized, set()))

    def incoming(self, code: str) -> set[str]:
        """Return incoming connections for an airport code.

        Args:
            code: Airport or provider code to process.

        Returns:
            set[str]: Incoming connections for an airport code.
        """
        self._ensure_loaded()
        normalized = str(code or "").strip().upper()
        return set(self._incoming.get(normalized, set()))

    def score_path_hubs(
        self,
        *,
        origins: list[str] | tuple[str, ...],
        destinations: list[str] | tuple[str, ...],
        max_split_hubs: int,
    ) -> dict[str, int]:
        """Score likely hub airports for the requested markets.

        Args:
            origins: Origins for the operation.
            destinations: Destinations for the operation.
            max_split_hubs: Max split hubs.

        Returns:
            dict[str, int]: Scored likely hub airports for the requested markets.
        """
        self._ensure_loaded()
        if max_split_hubs <= 0 or not self._outgoing:
            return {}

        origins_set = _normalize_codes(origins)
        destinations_set = _normalize_codes(destinations)
        blocked = origins_set | destinations_set
        scores: dict[str, int] = {}

        def add_score(code: str, points: int) -> None:
            normalized = str(code or "").strip().upper()
            if len(normalized) != 3 or normalized in blocked or points <= 0:
                return
            scores[normalized] = int(scores.get(normalized, 0)) + int(points)

        def scan_paths(
            *,
            start_codes: set[str],
            end_codes: set[str],
        ) -> None:
            for start in start_codes:
                first_hops = self._outgoing.get(start, set()) - blocked
                if not first_hops:
                    continue
                for end in end_codes:
                    direct_feeders = self._incoming.get(end, set()) - blocked
                    if not direct_feeders:
                        continue

                    for hub in first_hops & direct_feeders:
                        add_score(hub, 320)

                    if max_split_hubs < 2:
                        continue

                    for first_hub in first_hops:
                        second_hops = (
                            self._outgoing.get(first_hub, set()) - origins_set - {first_hub}
                        )
                        matching_second_hops = second_hops & direct_feeders
                        if not matching_second_hops:
                            continue
                        add_score(first_hub, 190 + max(0, len(matching_second_hops) - 1) * 20)
                        for second_hub in matching_second_hops:
                            if second_hub == first_hub:
                                continue
                            add_score(second_hub, 240)

        scan_paths(start_codes=origins_set, end_codes=destinations_set)
        scan_paths(start_codes=destinations_set, end_codes=origins_set)
        return scores

    def _ensure_loaded(self) -> None:
        """Load the backing dataset on first use."""
        if self._loaded:
            return
        self._loaded = True
        CACHE_DIR.mkdir(parents=True, exist_ok=True)

        if not ROUTES_CACHE_PATH.exists():
            try:
                response = requests.get(ROUTES_DATA_URL, timeout=30)
                response.raise_for_status()
                ROUTES_CACHE_PATH.write_text(response.text, encoding="utf-8")
            except Exception:
                return

        try:
            with ROUTES_CACHE_PATH.open("r", encoding="utf-8") as file:
                reader = csv.reader(file)
                for row in reader:
                    if len(row) < 5:
                        continue
                    source = str(row[2] or "").strip().upper()
                    destination = str(row[4] or "").strip().upper()
                    if len(source) != 3 or len(destination) != 3:
                        continue
                    if source == "\\N" or destination == "\\N" or source == destination:
                        continue
                    self._outgoing.setdefault(source, set()).add(destination)
                    self._incoming.setdefault(destination, set()).add(source)
        except Exception:
            self._outgoing = {}
            self._incoming = {}
