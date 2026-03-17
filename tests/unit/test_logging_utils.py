from __future__ import annotations

import importlib
import itertools
import logging
from pathlib import Path

from flight_layover_lab import logging_utils
from flight_layover_lab import resources as resources_module


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


def test_logging_utils_reload_and_internal_helpers_cover_runtime_paths(
    monkeypatch,
    tmp_path: Path,
) -> None:
    original_log_dir = resources_module.LOG_DIR
    original_responses_dir = resources_module.RESPONSES_DIR
    logger = logging.getLogger("flight_layover_lab")

    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        try:
            handler.close()
        except Exception:
            pass

    logs_dir = tmp_path / "logs"
    responses_dir = tmp_path / "responses"
    responses_dir.mkdir(parents=True, exist_ok=True)
    (responses_dir / "20260317T090000_amadeus_poll_000005.json").write_text(
        "{}",
        encoding="utf-8",
    )
    (responses_dir / "unexpected_name.json").write_text("{}", encoding="utf-8")
    (responses_dir / "20260317T090001_amadeus_poll_000006.json").write_text(
        "{}",
        encoding="utf-8",
    )

    monkeypatch.setenv("DEBUG_PROVIDER_RESPONSES", "1")
    monkeypatch.setenv("DEBUG_PROVIDER_RESPONSES_MAX_FILES", "bad")
    monkeypatch.setenv("SEARCH_LOG_MAX_BYTES", "bad")
    monkeypatch.setenv("SEARCH_LOG_BACKUP_COUNT", "bad")
    monkeypatch.setattr(resources_module, "LOG_DIR", logs_dir)
    monkeypatch.setattr(resources_module, "RESPONSES_DIR", responses_dir)

    reloaded = importlib.reload(logging_utils)
    try:
        assert reloaded._PROVIDER_RESPONSE_CAPTURE_MAX_FILES == 1200
        assert reloaded.SEARCH_LOG_MAX_BYTES == 8 * 1024 * 1024
        assert reloaded.SEARCH_LOG_BACKUP_COUNT == 4
        assert reloaded._build_engine_logger() is reloaded.ENGINE_LOGGER
        assert reloaded.ENGINE_LOGGER.handlers
        assert (logs_dir / "engine.log").parent == logs_dir

        class _BadDir:
            def glob(self, _pattern: str) -> list[Path]:
                raise OSError("boom")

        monkeypatch.setattr(reloaded, "RESPONSES_DIR", _BadDir())
        assert reloaded._response_capture_files() == []

        monkeypatch.setattr(reloaded, "RESPONSES_DIR", responses_dir)
        reloaded._sync_response_capture_state_locked()
        assert reloaded._PROVIDER_RESPONSE_CAPTURE_TOTAL == 3
        assert next(reloaded._PROVIDER_RESPONSE_CAPTURE_SEQ) == 7

        monkeypatch.setattr(reloaded, "_PROVIDER_RESPONSE_CAPTURE_MAX_FILES", 2)
        reloaded._PROVIDER_RESPONSE_CAPTURE_TOTAL = 2
        reloaded._prune_response_capture_files_locked()
        assert len(_captured_files(responses_dir)) == 1
        assert reloaded._PROVIDER_RESPONSE_CAPTURE_TOTAL == 1

        monkeypatch.setattr(reloaded, "_PROVIDER_RESPONSE_CAPTURE_MAX_FILES", 0)
        reloaded._PROVIDER_RESPONSE_CAPTURE_TOTAL = 10
        reloaded._prune_response_capture_files_locked()
        assert reloaded._PROVIDER_RESPONSE_CAPTURE_TOTAL == 10

        monkeypatch.setattr(reloaded, "_PROVIDER_RESPONSE_CAPTURE_MAX_FILES", 5)
        reloaded._PROVIDER_RESPONSE_CAPTURE_TOTAL = 5
        reloaded._prune_response_capture_files_locked()
        assert reloaded._PROVIDER_RESPONSE_CAPTURE_TOTAL == 1

        class _BadPath:
            def __init__(self, name: str, mtime: float, fail: bool) -> None:
                self.name = name
                self._mtime = mtime
                self._fail = fail

            @property
            def stem(self) -> str:
                return self.name

            def stat(self) -> object:
                return type("_Stat", (), {"st_mtime": self._mtime})()

            def unlink(self, missing_ok: bool = False) -> None:
                if self._fail:
                    raise OSError("locked")

        monkeypatch.setattr(
            reloaded,
            "_response_capture_files",
            lambda: [
                _BadPath("20260317_amadeus_poll_000001", 1.0, True),
                _BadPath("20260317_amadeus_poll_000002", 2.0, False),
                _BadPath("20260317_amadeus_poll_000003", 3.0, False),
            ],
        )
        monkeypatch.setattr(reloaded, "_PROVIDER_RESPONSE_CAPTURE_MAX_FILES", 2)
        reloaded._PROVIDER_RESPONSE_CAPTURE_TOTAL = 2
        reloaded._prune_response_capture_files_locked()
        assert reloaded._PROVIDER_RESPONSE_CAPTURE_TOTAL == 2

        existing_files = len(_captured_files(responses_dir))
        monkeypatch.setattr(reloaded, "_DEBUG_PROVIDER_RESPONSES", False)
        reloaded.capture_provider_response("amadeus", "poll", {"k": "v"}, {"price": 1})
        assert len(_captured_files(responses_dir)) == existing_files

        monkeypatch.setattr(reloaded, "_DEBUG_PROVIDER_RESPONSES", False)
        reloaded._initialize_provider_capture_retention()
        assert reloaded._PROVIDER_RESPONSE_CAPTURE_TOTAL == 2

        monkeypatch.setattr(reloaded, "_DEBUG_PROVIDER_RESPONSES", True)
        monkeypatch.setattr(reloaded, "_PROVIDER_RESPONSE_CAPTURE_MAX_FILES", 1)
        monkeypatch.setattr(reloaded, "_PROVIDER_RESPONSE_CAPTURE_SYNCED", True)
        monkeypatch.setattr(reloaded, "_PROVIDER_RESPONSE_CAPTURE_TOTAL", 1)
        monkeypatch.setattr(reloaded, "_prune_response_capture_files_locked", lambda: None)
        reloaded.capture_provider_response("amadeus", "poll", {"k": "v"}, {"price": 2})
        assert len(_captured_files(responses_dir)) == existing_files

        class _BadResponseDir:
            def mkdir(self, parents: bool = False, exist_ok: bool = False) -> None:
                raise OSError("mkdir failed")

        monkeypatch.setattr(reloaded, "RESPONSES_DIR", _BadResponseDir())
        reloaded.capture_provider_response("amadeus", "poll", {"k": "v"}, {"price": 3})
    finally:
        monkeypatch.setattr(resources_module, "LOG_DIR", original_log_dir)
        monkeypatch.setattr(resources_module, "RESPONSES_DIR", original_responses_dir)
        monkeypatch.delenv("DEBUG_PROVIDER_RESPONSES", raising=False)
        monkeypatch.delenv("DEBUG_PROVIDER_RESPONSES_MAX_FILES", raising=False)
        monkeypatch.delenv("SEARCH_LOG_MAX_BYTES", raising=False)
        monkeypatch.delenv("SEARCH_LOG_BACKUP_COUNT", raising=False)
        current_logger = logging.getLogger("flight_layover_lab")
        for handler in list(current_logger.handlers):
            current_logger.removeHandler(handler)
            try:
                handler.close()
            except Exception:
                pass
        importlib.reload(reloaded)
