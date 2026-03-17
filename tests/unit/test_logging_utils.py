from __future__ import annotations

import itertools
from pathlib import Path

from flight_layover_lab import logging_utils


def _captured_files(path: Path) -> list[Path]:
    return sorted(path.glob("*.json"))


def test_capture_provider_response_prunes_old_files_when_limit_reached(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(logging_utils, "_DEBUG_PROVIDER_RESPONSES", True)
    monkeypatch.setattr(logging_utils, "_PROVIDER_RESPONSE_CAPTURE_TARGETS", {"amadeus"})
    monkeypatch.setattr(logging_utils, "_PROVIDER_RESPONSE_CAPTURE_MAX_FILES", 2)
    monkeypatch.setattr(logging_utils, "RESPONSES_DIR", tmp_path)
    monkeypatch.setattr(logging_utils, "_PROVIDER_RESPONSE_CAPTURE_TOTAL", 0)
    monkeypatch.setattr(logging_utils, "_PROVIDER_RESPONSE_CAPTURE_SYNCED", False)
    monkeypatch.setattr(logging_utils, "_PROVIDER_RESPONSE_CAPTURE_SEQ", itertools.count(1))

    logging_utils.capture_provider_response("amadeus", "oneway", {"k": "v1"}, {"price": 1})
    logging_utils.capture_provider_response("amadeus", "oneway", {"k": "v2"}, {"price": 2})
    logging_utils.capture_provider_response("amadeus", "oneway", {"k": "v3"}, {"price": 3})

    files = _captured_files(tmp_path)
    assert len(files) == 2
    contents = [path.read_text(encoding="utf-8") for path in files]
    assert any('"v2"' in item for item in contents)
    assert any('"v3"' in item for item in contents)
    assert not any('"v1"' in item for item in contents)


def test_capture_provider_response_ignores_non_target_provider(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(logging_utils, "_DEBUG_PROVIDER_RESPONSES", True)
    monkeypatch.setattr(logging_utils, "_PROVIDER_RESPONSE_CAPTURE_TARGETS", {"serpapi"})
    monkeypatch.setattr(logging_utils, "RESPONSES_DIR", tmp_path)
    monkeypatch.setattr(logging_utils, "_PROVIDER_RESPONSE_CAPTURE_TOTAL", 0)
    monkeypatch.setattr(logging_utils, "_PROVIDER_RESPONSE_CAPTURE_SYNCED", False)
    monkeypatch.setattr(logging_utils, "_PROVIDER_RESPONSE_CAPTURE_SEQ", itertools.count(1))

    logging_utils.capture_provider_response("amadeus", "oneway", {"k": "v"}, {"price": 1})
    assert not _captured_files(tmp_path)
