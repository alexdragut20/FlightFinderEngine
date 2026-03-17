from __future__ import annotations

import datetime as dt
import threading
import time
from typing import Any

from ..utils.constants import (
    SEARCH_EVENT_COMPLETE,
    SEARCH_EVENT_FAILURE_PREFIX,
    SEARCH_EVENT_QUEUED,
    SEARCH_STATUS_COMPLETED,
    SEARCH_STATUS_FAILED,
    SEARCH_STATUS_QUEUED,
    SEARCH_STATUS_RUNNING,
)

_PHASE_ORDER = (
    ("setup", "Preparing search", 0.04),
    ("calendar", "Fetching route calendars", 0.20),
    ("candidates", "Scoring route candidates", 0.14),
    ("returns", "Validating round-trips", 0.12),
    ("oneways", "Validating one-way legs", 0.18),
    ("build", "Assembling itineraries", 0.22),
    ("finalize", "Ranking final results", 0.10),
)
_PHASE_META = {name: {"label": label, "weight": weight} for name, label, weight in _PHASE_ORDER}


def _utc_now_iso() -> str:
    """Return the current UTC time in ISO 8601 format.

    Returns:
        str: The current UTC time in ISO 8601 format.
    """
    return dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z")


class SearchProgressTracker:
    """Progress tracker for long-running search jobs."""

    def __init__(self, search_id: str) -> None:
        """Initialize the SearchProgressTracker.

        Args:
            search_id: Identifier of the current search.
        """
        self.search_id = search_id
        self.started_at = time.time()
        self.updated_at = self.started_at
        self._lock = threading.Lock()
        self._status = SEARCH_STATUS_QUEUED
        self._error: str | None = None
        self._current_phase = "setup"
        self._phase_state: dict[str, dict[str, Any]] = {
            name: {
                "label": meta["label"],
                "weight": meta["weight"],
                "completed": 0,
                "total": 0,
                "progress": 0.0,
                "detail": "",
                "active": False,
                "started_at": None,
                "completed_at": None,
            }
            for name, meta in _PHASE_META.items()
        }
        self._phase_log_buckets = {name: -1 for name in _PHASE_META}
        self._events: list[dict[str, Any]] = []
        self._append_event_locked(SEARCH_EVENT_QUEUED)

    def mark_running(self, detail: str | None = None) -> None:
        """Mark the search as running.

        Args:
            detail: Human-readable detail message for the current phase.
        """
        with self._lock:
            self._status = SEARCH_STATUS_RUNNING
            self.updated_at = time.time()
            if detail:
                self._append_event_locked(detail)

    def start_phase(
        self,
        phase: str,
        *,
        total: int | None = None,
        detail: str | None = None,
    ) -> None:
        """Start tracking a progress phase.

        Args:
            phase: Progress phase name to update.
            total: Total number of work units for the phase.
            detail: Human-readable detail message for the current phase.
        """
        with self._lock:
            state = self._phase(phase)
            self._status = SEARCH_STATUS_RUNNING
            self._current_phase = phase
            state["active"] = True
            if state["started_at"] is None:
                state["started_at"] = time.time()
            state["completed_at"] = None
            if total is not None:
                state["total"] = max(0, int(total))
            if detail is not None:
                state["detail"] = str(detail)
            self.updated_at = time.time()
            message = detail or f"{state['label']} started."
            self._append_event_locked(message, phase=phase)

    def add_phase_total(
        self,
        phase: str,
        *,
        total_increment: int = 0,
        total: int | None = None,
        detail: str | None = None,
    ) -> None:
        """Add work units to a progress phase.

        Args:
            phase: Progress phase name to update.
            total_increment: Additional work units to add to the phase total.
            total: Total number of work units for the phase.
            detail: Human-readable detail message for the current phase.
        """
        with self._lock:
            state = self._phase(phase)
            if total is not None:
                state["total"] = max(0, int(total))
            elif total_increment:
                state["total"] = max(0, int(state["total"]) + int(total_increment))
            if detail is not None:
                state["detail"] = str(detail)
            self.updated_at = time.time()

    def advance_phase(
        self,
        phase: str,
        *,
        step: int = 1,
        completed: int | None = None,
        total: int | None = None,
        detail: str | None = None,
    ) -> None:
        """Advance the completed count for a progress phase.

        Args:
            phase: Progress phase name to update.
            step: Number of work units to advance.
            completed: Number of completed work units for the phase.
            total: Total number of work units for the phase.
            detail: Human-readable detail message for the current phase.
        """
        with self._lock:
            state = self._phase(phase)
            self._status = SEARCH_STATUS_RUNNING
            self._current_phase = phase
            state["active"] = True
            if state["started_at"] is None:
                state["started_at"] = time.time()
            state["completed_at"] = None
            if total is not None:
                state["total"] = max(0, int(total))
            if completed is None:
                state["completed"] = max(0, int(state["completed"]) + int(step))
            else:
                state["completed"] = max(0, int(completed))
            if state["total"] > 0:
                state["progress"] = min(1.0, float(state["completed"]) / float(state["total"]))
            elif state["completed"] > 0:
                state["total"] = int(state["completed"])
                state["progress"] = 1.0
            if detail is not None:
                state["detail"] = str(detail)
            self.updated_at = time.time()
            self._maybe_log_bucket_locked(phase)

    def complete_phase(self, phase: str, *, detail: str | None = None) -> None:
        """Mark a progress phase as completed.

        Args:
            phase: Progress phase name to update.
            detail: Human-readable detail message for the current phase.
        """
        with self._lock:
            state = self._phase(phase)
            self._current_phase = phase
            state["active"] = False
            now = time.time()
            if state["started_at"] is None:
                state["started_at"] = now
            state["completed_at"] = now
            if state["total"] <= 0:
                state["total"] = max(1, int(state["completed"]) or 1)
            state["completed"] = max(int(state["completed"]), int(state["total"]))
            state["progress"] = 1.0
            if detail is not None:
                state["detail"] = str(detail)
            self.updated_at = time.time()
            self._phase_log_buckets[phase] = 10
            self._append_event_locked(detail or f"{state['label']} complete.", phase=phase)

    def log_message(self, message: str, *, phase: str | None = None) -> None:
        """Append a human-readable log message to the progress stream.

        Args:
            message: Human-readable message to log or display.
            phase: Progress phase name to update.
        """
        with self._lock:
            if phase:
                self._phase(phase)
                self._current_phase = phase
            self.updated_at = time.time()
            self._append_event_locked(message, phase=phase)

    def mark_completed(self, *, result_count: int | None = None) -> None:
        """Mark the search as completed.

        Args:
            result_count: Number of result.
        """
        with self._lock:
            self._status = SEARCH_STATUS_COMPLETED
            self._error = None
            self._current_phase = "finalize"
            self.updated_at = time.time()
            summary = SEARCH_EVENT_COMPLETE
            if result_count is not None:
                summary = f"{SEARCH_EVENT_COMPLETE[:-1]}: {result_count} result(s)."
            self._append_event_locked(summary, phase="finalize")

    def mark_failed(self, error: str) -> None:
        """Mark the search as failed.

        Args:
            error: Error message to record.
        """
        with self._lock:
            self._status = SEARCH_STATUS_FAILED
            self._error = str(error)
            self.updated_at = time.time()
            self._append_event_locked(
                f"{SEARCH_EVENT_FAILURE_PREFIX}{error}",
                phase=self._current_phase,
            )

    def snapshot(self, *, since_event_index: int | None = None) -> dict[str, Any]:
        """Return a snapshot of the current progress state.

        Args:
            since_event_index: Last event index already received by the client.

        Returns:
            dict[str, Any]: A snapshot of the current progress state.
        """
        with self._lock:
            now = time.time()
            if self._status == SEARCH_STATUS_COMPLETED:
                overall = 1.0
            else:
                overall = sum(
                    float(state["weight"]) * float(state["progress"])
                    for state in self._phase_state.values()
                )
            current_state = self._phase_state.get(self._current_phase, {})
            elapsed_seconds = max(0.0, now - self.started_at)
            eta_seconds = self._estimate_eta_locked(now, overall, elapsed_seconds)
            event_start_index = min(
                max(0, int(since_event_index or 0)),
                len(self._events),
            )
            return {
                "search_id": self.search_id,
                "status": self._status,
                "phase": self._current_phase,
                "phase_label": str(current_state.get("label") or ""),
                "phase_detail": str(current_state.get("detail") or ""),
                "current": int(current_state.get("completed") or 0),
                "total": int(current_state.get("total") or 0),
                "progress_ratio": round(overall, 4),
                "progress_percent": round(overall * 100.0, 1),
                "elapsed_seconds": round(elapsed_seconds, 1),
                "eta_seconds": eta_seconds,
                "started_at": dt.datetime.fromtimestamp(self.started_at, dt.UTC)
                .isoformat()
                .replace("+00:00", "Z"),
                "updated_at": dt.datetime.fromtimestamp(self.updated_at, dt.UTC)
                .isoformat()
                .replace("+00:00", "Z"),
                "error": self._error,
                "events_start_index": event_start_index,
                "next_event_index": len(self._events),
                "events": list(self._events[event_start_index:]),
            }

    def _estimate_eta_locked(
        self,
        now: float,
        overall: float,
        elapsed_seconds: float,
    ) -> int | None:
        """Estimate the remaining time for the search.

        Args:
            now: Current timestamp for the operation.
            overall: Overall.
            elapsed_seconds: Duration in seconds for elapsed.

        Returns:
            int | None: Estimated remaining time for the search.
        """
        if self._status != SEARCH_STATUS_RUNNING or overall <= 0.0 or overall >= 1.0:
            return None

        current_state = self._phase_state.get(self._current_phase, {})
        current_weight = float(current_state.get("weight") or 0.0)
        current_progress = float(current_state.get("progress") or 0.0)
        current_phase_started_at = current_state.get("started_at")
        current_phase_eta: float | None = None

        if (
            current_phase_started_at is not None
            and 0.0 < current_progress < 1.0
            and current_weight > 0.0
        ):
            phase_elapsed = max(0.0, now - float(current_phase_started_at))
            current_phase_eta = phase_elapsed * ((1.0 - current_progress) / current_progress)

        completed_weight = 0.0
        completed_time = 0.0
        future_weight = 0.0
        seen_current = False
        for name, _, weight in _PHASE_ORDER:
            state = self._phase_state[name]
            phase_progress = float(state["progress"] or 0.0)
            phase_started_at = state.get("started_at")
            phase_completed_at = state.get("completed_at")
            if phase_progress >= 1.0:
                completed_weight += float(weight)
                if phase_started_at is not None and phase_completed_at is not None:
                    completed_time += max(
                        0.0,
                        float(phase_completed_at) - float(phase_started_at),
                    )
                continue
            if name == self._current_phase:
                seen_current = True
                continue
            if seen_current and phase_progress < 1.0:
                future_weight += float(weight) * (1.0 - phase_progress)

        seconds_per_weight: float | None = None
        if completed_weight > 0.0 and completed_time > 0.0:
            seconds_per_weight = completed_time / completed_weight
        elif overall >= 0.02:
            seconds_per_weight = elapsed_seconds / overall

        if current_phase_eta is None and seconds_per_weight is not None and current_weight > 0.0:
            current_phase_eta = seconds_per_weight * current_weight * (1.0 - current_progress)

        if current_phase_eta is None and seconds_per_weight is None:
            return None

        future_eta = 0.0
        if seconds_per_weight is not None and future_weight > 0.0:
            future_eta = seconds_per_weight * future_weight

        eta_seconds = max(0.0, (current_phase_eta or 0.0) + future_eta)
        return int(round(eta_seconds))

    def _phase(self, phase: str) -> dict[str, Any]:
        """Return the mutable state for a progress phase.

        Args:
            phase: Progress phase name to update.

        Returns:
            dict[str, Any]: The mutable state for a progress phase.
        """
        normalized = str(phase or "").strip().lower()
        if normalized not in self._phase_state:
            raise ValueError(f"Unknown progress phase: {phase}")
        return self._phase_state[normalized]

    def _append_event_locked(self, message: str, *, phase: str | None = None) -> None:
        """Append an event while the tracker lock is held.

        Args:
            message: Human-readable message to log or display.
            phase: Progress phase name to update.
        """
        normalized = str(message or "").strip()
        if not normalized:
            return
        self._events.append(
            {
                "timestamp": _utc_now_iso(),
                "phase": str(phase or ""),
                "message": normalized,
            }
        )

    def _maybe_log_bucket_locked(self, phase: str) -> None:
        """Emit a progress bucket log when the threshold changes.

        Args:
            phase: Progress phase name to update.
        """
        state = self._phase_state[phase]
        total = int(state["total"] or 0)
        if total <= 0:
            return
        progress = float(state["progress"] or 0.0)
        bucket = min(10, int(progress * 10))
        last_bucket = int(self._phase_log_buckets.get(phase, -1))
        if bucket <= last_bucket or bucket <= 0:
            return
        self._phase_log_buckets[phase] = bucket
        percent = min(100, bucket * 10)
        message = f"{state['label']}: {percent}% ({int(state['completed'])}/{total})."
        self._append_event_locked(message, phase=phase)
