#!/usr/bin/env python3
"""Backward-compatible entrypoint for FlightFinder Engine.

The core app code lives in the `src/` package.

This thin wrapper keeps existing commands working:
  - `python3 server.py`
  - scripts importing `from server import ...`
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

# Ensure the repo root is importable when running from the checkout.
_PROJECT_ROOT = Path(__file__).resolve().parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

_app = importlib.import_module("src.app")

# Re-export all names (including underscore-prefixed helpers used by tests).
for _name, _value in _app.__dict__.items():
    if _name.startswith("__"):
        continue
    globals()[_name] = _value

run_server = _app.run_server


if __name__ == "__main__":
    run_server()
