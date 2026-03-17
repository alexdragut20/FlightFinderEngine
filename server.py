#!/usr/bin/env python3
"""Backward-compatible entrypoint for FlightFinder Engine.

The core app code lives in `src/flight_layover_lab/`.

This thin wrapper keeps existing commands working:
  - `python3 server.py`
  - scripts importing `from server import ...`
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

# Ensure `src/` is importable when running from the repo root.
_PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = _PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

_app = importlib.import_module("flight_layover_lab.app")

# Re-export all names (including underscore-prefixed helpers used by tests).
for _name, _value in _app.__dict__.items():
    if _name.startswith("__"):
        continue
    globals()[_name] = _value

run_server = _app.run_server


if __name__ == "__main__":
    run_server()
