from __future__ import annotations

import os
from pathlib import Path


def resolve_project_root() -> Path:
    """Resolve the project root used for local assets.

    Returns:
        Path: Resolved project root used for local assets.
    """

    env_root = str(os.getenv("FLIGHT_LAYOVER_LAB_ROOT", "") or "").strip()
    if env_root:
        return Path(env_root).expanduser().resolve()

    # When running from the repo with the current `src/` package layout:
    #   repo-root/src/data/resources.py
    return Path(__file__).resolve().parents[2]


PROJECT_ROOT = resolve_project_root()
STATIC_DIR = PROJECT_ROOT / "static"
CACHE_DIR = PROJECT_ROOT / "cache"
AIRPORTS_CACHE_PATH = CACHE_DIR / "airports.dat"
ROUTES_CACHE_PATH = CACHE_DIR / "routes.dat"
RESPONSES_DIR = PROJECT_ROOT / "responses"
LOG_DIR = PROJECT_ROOT / "logs"
