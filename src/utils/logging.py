from __future__ import annotations

import itertools
import json
import logging
import logging.handlers
import os
import threading
import time
from pathlib import Path
from typing import Any

from ..data.resources import LOG_DIR, RESPONSES_DIR

_DEBUG_PROVIDER_RESPONSES = str(os.getenv("DEBUG_PROVIDER_RESPONSES", "0")).strip().lower() not in {
    "0",
    "false",
    "off",
    "no",
}
_PROVIDER_RESPONSE_CAPTURE_TARGETS = {"amadeus", "serpapi"}
_PROVIDER_RESPONSE_CAPTURE_MAX_STRING = 8000
try:
    _debug_capture_max_files = int(os.getenv("DEBUG_PROVIDER_RESPONSES_MAX_FILES", "1200"))
except ValueError:
    _debug_capture_max_files = 1200
_PROVIDER_RESPONSE_CAPTURE_MAX_FILES = max(0, min(50000, _debug_capture_max_files))

try:
    _log_max_bytes_env = int(os.getenv("SEARCH_LOG_MAX_BYTES", str(8 * 1024 * 1024)))
except ValueError:
    _log_max_bytes_env = 8 * 1024 * 1024
SEARCH_LOG_MAX_BYTES = max(1024 * 1024, min(256 * 1024 * 1024, _log_max_bytes_env))
try:
    _log_backup_count_env = int(os.getenv("SEARCH_LOG_BACKUP_COUNT", "4"))
except ValueError:
    _log_backup_count_env = 4
SEARCH_LOG_BACKUP_COUNT = max(1, min(20, _log_backup_count_env))

_PROVIDER_RESPONSE_CAPTURE_LOCK = threading.Lock()
_PROVIDER_RESPONSE_CAPTURE_SEQ = itertools.count(1)
_PROVIDER_RESPONSE_CAPTURE_TOTAL = 0
_PROVIDER_RESPONSE_CAPTURE_SYNCED = False
_FD_COUNT_CACHE_LOCK = threading.Lock()
_FD_COUNT_CACHE_VALUE: int | None = None
_FD_COUNT_CACHE_TS = 0.0
_FD_COUNT_SAMPLE_INTERVAL_SECONDS = 1.0


def _response_capture_files() -> list[Path]:
    """Return the response capture files on disk.

    Returns:
        list[Path]: The response capture files on disk.
    """
    try:
        return [path for path in RESPONSES_DIR.glob("*.json") if path.is_file()]
    except Exception:
        return []


def _sync_response_capture_state_locked() -> None:
    """Synchronize response capture state while the lock is held."""
    global _PROVIDER_RESPONSE_CAPTURE_TOTAL
    global _PROVIDER_RESPONSE_CAPTURE_SEQ
    global _PROVIDER_RESPONSE_CAPTURE_SYNCED

    files = _response_capture_files()
    _PROVIDER_RESPONSE_CAPTURE_TOTAL = len(files)

    max_seq = 0
    for path in files:
        stem = path.stem
        seq_part = stem.rsplit("_", 1)[-1]
        try:
            max_seq = max(max_seq, int(seq_part))
        except Exception:
            continue
    _PROVIDER_RESPONSE_CAPTURE_SEQ = itertools.count(max_seq + 1)
    _PROVIDER_RESPONSE_CAPTURE_SYNCED = True


def _prune_response_capture_files_locked() -> None:
    """Prune response capture files while the lock is held."""
    global _PROVIDER_RESPONSE_CAPTURE_TOTAL
    if _PROVIDER_RESPONSE_CAPTURE_MAX_FILES <= 0:
        return
    if _PROVIDER_RESPONSE_CAPTURE_TOTAL < _PROVIDER_RESPONSE_CAPTURE_MAX_FILES:
        return

    files = _response_capture_files()
    if len(files) < _PROVIDER_RESPONSE_CAPTURE_MAX_FILES:
        _PROVIDER_RESPONSE_CAPTURE_TOTAL = len(files)
        return

    # Remove at least one file, and at most a small batch, to keep growth bounded
    # without scanning/deleting too aggressively on every write.
    files.sort(key=lambda path: path.stat().st_mtime)
    overflow = len(files) - _PROVIDER_RESPONSE_CAPTURE_MAX_FILES + 1
    delete_count = max(1, min(200, overflow))
    removed = 0
    for path in files[:delete_count]:
        try:
            path.unlink(missing_ok=True)
            removed += 1
        except Exception:
            continue
    _PROVIDER_RESPONSE_CAPTURE_TOTAL = max(0, len(files) - removed)


def _initialize_provider_capture_retention() -> None:
    """Initialize response capture retention metadata."""
    if not _DEBUG_PROVIDER_RESPONSES:
        return
    with _PROVIDER_RESPONSE_CAPTURE_LOCK:
        _sync_response_capture_state_locked()
        _prune_response_capture_files_locked()


def _sanitize_debug_value(value: Any) -> Any:
    """Sanitize a debug value before logging or persistence.

    Args:
        value: Input value to process.

    Returns:
        Any: Sanitize a debug value before logging or persistence.
    """
    sensitive_keys = {
        "api_key",
        "apikey",
        "authorization",
        "token",
        "access_token",
        "refresh_token",
        "client_secret",
        "amadeus_client_secret",
        "serpapi_api_key",
    }
    if isinstance(value, dict):
        cleaned: dict[str, Any] = {}
        for key, item in value.items():
            key_str = str(key)
            if key_str.lower() in sensitive_keys:
                cleaned[key_str] = "[REDACTED]"
            else:
                cleaned[key_str] = _sanitize_debug_value(item)
        return cleaned
    if isinstance(value, list):
        return [_sanitize_debug_value(item) for item in value]
    if isinstance(value, tuple):
        return [_sanitize_debug_value(item) for item in value]
    if isinstance(value, str):
        if len(value) > _PROVIDER_RESPONSE_CAPTURE_MAX_STRING:
            return value[:_PROVIDER_RESPONSE_CAPTURE_MAX_STRING] + "...[truncated]"
        return value
    return value


def _open_file_descriptor_count() -> int | None:
    """Return the open file descriptor count when available.

    Returns:
        int | None: The open file descriptor count when available.
    """
    global _FD_COUNT_CACHE_VALUE
    global _FD_COUNT_CACHE_TS

    now = time.time()
    with _FD_COUNT_CACHE_LOCK:
        if now - _FD_COUNT_CACHE_TS < _FD_COUNT_SAMPLE_INTERVAL_SECONDS:
            return _FD_COUNT_CACHE_VALUE

    count: int | None = None
    for path in ("/dev/fd", "/proc/self/fd"):
        try:
            count = len(os.listdir(path))
            break
        except Exception:
            continue
    with _FD_COUNT_CACHE_LOCK:
        _FD_COUNT_CACHE_VALUE = count
        _FD_COUNT_CACHE_TS = now
    return count


def _build_engine_logger() -> logging.Logger:
    """Build the shared engine logger.

    Returns:
        logging.Logger: The shared engine logger.
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("flight_layover_lab")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    logger.propagate = False
    handler = logging.handlers.RotatingFileHandler(
        LOG_DIR / "engine.log",
        maxBytes=SEARCH_LOG_MAX_BYTES,
        backupCount=SEARCH_LOG_BACKUP_COUNT,
        encoding="utf-8",
    )
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


_initialize_provider_capture_retention()
ENGINE_LOGGER = _build_engine_logger()


def log_event(level: int, event: str, **fields: Any) -> None:
    """Log an engine event with structured context.

    Args:
        level: Logging level for the event.
        event: Event name to record.
        fields: Fields.
    """
    payload: dict[str, Any] = {
        "event": str(event),
        "fd_open": _open_file_descriptor_count(),
        **fields,
    }
    try:
        ENGINE_LOGGER.log(level, json.dumps(_sanitize_debug_value(payload), ensure_ascii=True))
    except Exception:
        # Logging must never break search flows.
        pass


def capture_provider_response(
    provider_id: str,
    operation: str,
    request_payload: dict[str, Any],
    response_payload: Any = None,
    *,
    status_code: int | None = None,
    error: str | None = None,
) -> None:
    """Persist a provider response for debugging when enabled.

    Args:
        provider_id: Provider identifier involved in the request.
        operation: Operation name to record.
        request_payload: Mapping of request payload.
        response_payload: JSON response payload to send to the client.
        status_code: HTTP status code for the response.
        error: Error message to record.
    """
    global _PROVIDER_RESPONSE_CAPTURE_TOTAL
    normalized_provider = str(provider_id or "").strip().lower()
    if not _DEBUG_PROVIDER_RESPONSES:
        return
    if normalized_provider not in _PROVIDER_RESPONSE_CAPTURE_TARGETS:
        return

    try:
        RESPONSES_DIR.mkdir(parents=True, exist_ok=True)
        ts = time.strftime("%Y%m%dT%H%M%S", time.gmtime())
        safe_operation = "".join(
            ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in str(operation or "op")
        )

        with _PROVIDER_RESPONSE_CAPTURE_LOCK:
            if not _PROVIDER_RESPONSE_CAPTURE_SYNCED:
                _sync_response_capture_state_locked()
            if _PROVIDER_RESPONSE_CAPTURE_MAX_FILES > 0:
                _prune_response_capture_files_locked()
                if _PROVIDER_RESPONSE_CAPTURE_TOTAL >= _PROVIDER_RESPONSE_CAPTURE_MAX_FILES:
                    return
                _PROVIDER_RESPONSE_CAPTURE_TOTAL += 1
            seq = next(_PROVIDER_RESPONSE_CAPTURE_SEQ)

        file_path = RESPONSES_DIR / f"{ts}_{normalized_provider}_{safe_operation}_{seq:06d}.json"
        payload = {
            "provider": normalized_provider,
            "operation": operation,
            "status_code": status_code,
            "error": error,
            "request": _sanitize_debug_value(request_payload),
            "response": _sanitize_debug_value(response_payload),
        }
        file_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")
    except Exception:
        return
