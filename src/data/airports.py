from __future__ import annotations

import csv

import requests

from ..config import AIRPORTS_DATA_URL, DESTINATION_NOTES, FALLBACK_COORDS
from .resources import AIRPORTS_CACHE_PATH, CACHE_DIR


class AirportCoordinates:
    """Airport coordinate lookup and caching service."""

    def __init__(self) -> None:
        """Initialize the AirportCoordinates."""
        self._coords: dict[str, tuple[float, float]] = dict(FALLBACK_COORDS)
        self._labels: dict[str, str] = {
            code: str(meta.get("name") or code) for code, meta in DESTINATION_NOTES.items()
        }
        self._loaded_from_file = False

    def get(self, code: str) -> tuple[float, float] | None:
        """Handle get.

        Args:
            code: Airport or provider code to process.

        Returns:
            tuple[float, float] | None: Handle get.
        """
        code = code.upper().strip()
        if not code:
            return None
        if code in self._coords:
            return self._coords[code]
        self._ensure_loaded()
        return self._coords.get(code)

    def display_name(self, code: str) -> str | None:
        """Handle display name.

        Args:
            code: Airport or provider code to process.

        Returns:
            str | None: Handle display name.
        """
        normalized = str(code or "").strip().upper()
        if not normalized:
            return None
        self._ensure_loaded()
        label = str(self._labels.get(normalized) or "").strip()
        if not label or label.upper() == normalized:
            return None
        return label

    def _ensure_loaded(self) -> None:
        """Load the backing dataset on first use."""
        if self._loaded_from_file:
            return
        self._loaded_from_file = True
        CACHE_DIR.mkdir(parents=True, exist_ok=True)

        if not AIRPORTS_CACHE_PATH.exists():
            try:
                response = requests.get(AIRPORTS_DATA_URL, timeout=30)
                response.raise_for_status()
                AIRPORTS_CACHE_PATH.write_text(response.text, encoding="utf-8")
            except Exception:
                return

        try:
            with AIRPORTS_CACHE_PATH.open("r", encoding="utf-8") as file:
                reader = csv.reader(file)
                for row in reader:
                    if len(row) < 8:
                        continue
                    iata = (row[4] or "").strip().upper()
                    if not iata or iata == "\\N":
                        continue
                    try:
                        lat = float(row[6])
                        lon = float(row[7])
                    except ValueError:
                        continue
                    self._coords[iata] = (lat, lon)
                    city_name = str(row[2] or "").strip()
                    airport_name = str(row[1] or "").strip()
                    if city_name and city_name != "\\N":
                        self._labels.setdefault(iata, city_name)
                    elif airport_name and airport_name != "\\N":
                        self._labels.setdefault(iata, airport_name)
        except Exception:
            return
