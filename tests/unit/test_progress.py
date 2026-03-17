from __future__ import annotations

import pytest

from src.data.airports import AirportCoordinates
from src.engine import SplitTripOptimizer
from src.providers import KiwiClient
from src.services.progress import SearchProgressTracker


def test_parse_config_cpu_workers_zero_uses_detected_max(monkeypatch) -> None:
    optimizer = SplitTripOptimizer(KiwiClient(), AirportCoordinates())
    monkeypatch.setattr(optimizer, "_available_cpu_workers", lambda: 20)
    config = optimizer.parse_search_config(
        {
            "origins": ["OTP"],
            "destinations": ["MGA"],
            "period_start": "2026-03-10",
            "period_end": "2026-03-24",
            "cpu_workers": 0,
        }
    )
    assert config.cpu_workers == 20
    assert config.cpu_workers_auto is True


def test_parse_config_missing_cpu_workers_uses_detected_max(monkeypatch) -> None:
    optimizer = SplitTripOptimizer(KiwiClient(), AirportCoordinates())
    monkeypatch.setattr(optimizer, "_available_cpu_workers", lambda: 20)
    config = optimizer.parse_search_config(
        {
            "origins": ["OTP"],
            "destinations": ["MGA"],
            "period_start": "2026-03-10",
            "period_end": "2026-03-24",
        }
    )
    assert config.cpu_workers == 20
    assert config.cpu_workers_auto is True


def test_parse_config_cpu_workers_clamps_to_detected_max(monkeypatch) -> None:
    optimizer = SplitTripOptimizer(KiwiClient(), AirportCoordinates())
    monkeypatch.setattr(optimizer, "_available_cpu_workers", lambda: 20)
    config = optimizer.parse_search_config(
        {
            "origins": ["OTP"],
            "destinations": ["MGA"],
            "period_start": "2026-03-10",
            "period_end": "2026-03-24",
            "cpu_workers": 99,
        }
    )
    assert config.cpu_workers == 20
    assert config.cpu_workers_auto is False


def test_progress_tracker_reports_progress_and_completion() -> None:
    tracker = SearchProgressTracker("job-123")
    tracker.mark_running("Search started.")
    tracker.start_phase("calendar", total=10, detail="Fetching route calendars.")
    tracker.advance_phase("calendar", completed=5, total=10, detail="Halfway through calendars.")

    running = tracker.snapshot()
    assert running["status"] == "running"
    assert running["phase"] == "calendar"
    assert running["current"] == 5
    assert running["total"] == 10
    assert running["progress_percent"] > 0
    assert running["eta_seconds"] is not None

    tracker.complete_phase("calendar", detail="Calendar stage complete.")
    tracker.mark_completed(result_count=7)

    completed = tracker.snapshot()
    assert completed["status"] == "completed"
    assert completed["progress_percent"] == 100.0
    assert any("7 result(s)" in event["message"] for event in completed["events"])


def test_progress_tracker_keeps_full_event_history() -> None:
    tracker = SearchProgressTracker("job-logs")
    tracker.mark_running("Search started.")

    for index in range(60):
        tracker.log_message(f"Progress event {index}", phase="setup")

    snapshot = tracker.snapshot()
    assert len(snapshot["events"]) == 62
    assert snapshot["events_start_index"] == 0
    assert snapshot["next_event_index"] == 62
    assert snapshot["events"][2]["message"] == "Progress event 0"
    assert snapshot["events"][-1]["message"] == "Progress event 59"


def test_progress_tracker_snapshot_can_stream_incremental_events() -> None:
    tracker = SearchProgressTracker("job-stream")
    tracker.mark_running("Search started.")

    for index in range(5):
        tracker.log_message(f"Progress event {index}", phase="setup")

    snapshot = tracker.snapshot(since_event_index=5)
    assert snapshot["events_start_index"] == 5
    assert snapshot["next_event_index"] == 7
    assert [event["message"] for event in snapshot["events"]] == [
        "Progress event 3",
        "Progress event 4",
    ]


def test_progress_tracker_edge_paths_cover_default_messages_eta_and_empty_logs(
    monkeypatch,
) -> None:
    clock = {"now": 1000.0}
    monkeypatch.setattr("src.services.progress.time.time", lambda: clock["now"])

    tracker = SearchProgressTracker("job-edge")
    initial = tracker.snapshot(since_event_index=999)
    assert initial["status"] == "queued"
    assert initial["eta_seconds"] is None
    assert initial["events"] == []
    assert initial["events_start_index"] == initial["next_event_index"]

    tracker.mark_running()
    tracker.log_message("   ", phase="setup")
    tracker.start_phase("calendar", total=0)
    assert tracker.snapshot()["events"][-1]["message"] == "Fetching route calendars started."

    clock["now"] += 5
    tracker.advance_phase("calendar", step=1)
    calendar_snapshot = tracker.snapshot()
    assert calendar_snapshot["current"] == 1
    assert calendar_snapshot["total"] == 1
    assert any(
        "Fetching route calendars: 100% (1/1)." == event["message"]
        for event in calendar_snapshot["events"]
    )

    tracker.start_phase("oneways", total=0, detail="Validating one-way legs.")
    tracker.add_phase_total("oneways", total=10)
    clock["now"] += 10
    one_way_snapshot = tracker.snapshot()
    assert one_way_snapshot["phase"] == "oneways"
    assert one_way_snapshot["eta_seconds"] is not None

    clock["now"] += 5
    tracker.complete_phase("returns")
    returns_snapshot = tracker.snapshot()
    assert returns_snapshot["phase"] == "returns"
    assert returns_snapshot["progress_percent"] > 0

    tracker.mark_completed()
    completed = tracker.snapshot(since_event_index=999)
    assert completed["status"] == "completed"
    assert completed["progress_percent"] == 100.0
    assert completed["events"] == []
    assert completed["events_start_index"] == completed["next_event_index"]
    assert tracker.snapshot()["events"][-1]["message"] == "Search complete."

    with pytest.raises(ValueError):
        tracker.start_phase("unknown")
