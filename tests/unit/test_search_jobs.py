from __future__ import annotations

import time

from src.services.progress import SearchProgressTracker
from src.services.search_jobs import (
    SearchJob,
    SearchJobCapacityError,
    SearchJobStore,
)


def _make_job(
    job_id: str, *, status: str, updated_at: float, finished_at: float | None = None
) -> SearchJob:
    return SearchJob(
        job_id=job_id,
        progress=SearchProgressTracker(job_id),
        status=status,
        updated_at=updated_at,
        finished_at=finished_at,
    )


def test_prune_locked_never_evicts_running_jobs() -> None:
    store = SearchJobStore(max_jobs=4, ttl_seconds=3600)
    store._jobs = {
        "a": _make_job("a", status="running", updated_at=1.0),
        "b": _make_job("b", status="running", updated_at=2.0),
        "c": _make_job("c", status="running", updated_at=3.0),
        "d": _make_job("d", status="running", updated_at=4.0),
        "e": _make_job("e", status="running", updated_at=5.0),
    }

    store._prune_locked()

    assert sorted(store._jobs) == ["a", "b", "c", "d", "e"]


def test_start_job_rejects_when_store_is_full_of_active_jobs(monkeypatch) -> None:
    store = SearchJobStore(max_jobs=4, ttl_seconds=3600)
    store._jobs = {
        "a": _make_job("a", status="running", updated_at=1.0),
        "b": _make_job("b", status="running", updated_at=2.0),
        "c": _make_job("c", status="running", updated_at=3.0),
        "d": _make_job("d", status="queued", updated_at=4.0),
    }

    class FakeThread:
        def __init__(self, *args: object, **kwargs: object) -> None:
            self.args = args
            self.kwargs = kwargs

        def start(self) -> None:
            raise AssertionError("thread should not start when capacity is exhausted")

    monkeypatch.setattr("src.services.search_jobs.threading.Thread", FakeThread)

    try:
        store.start_job(object(), object())
    except SearchJobCapacityError as exc:
        assert "Too many searches" in str(exc)
    else:
        raise AssertionError("expected SearchJobCapacityError")


def test_start_job_reclaims_finished_job_before_inserting(monkeypatch) -> None:
    store = SearchJobStore(max_jobs=4, ttl_seconds=3600)
    finished_at = time.time() - 10
    store._jobs = {
        "a": _make_job("a", status="completed", updated_at=1.0, finished_at=finished_at),
        "b": _make_job("b", status="running", updated_at=2.0),
        "c": _make_job("c", status="running", updated_at=3.0),
        "d": _make_job("d", status="queued", updated_at=4.0),
    }

    started = {"value": False}

    class FakeThread:
        def __init__(self, *args: object, **kwargs: object) -> None:
            self.args = args
            self.kwargs = kwargs

        def start(self) -> None:
            started["value"] = True

    monkeypatch.setattr("src.services.search_jobs.threading.Thread", FakeThread)

    job = store.start_job(object(), object())

    assert started["value"] is True
    assert len(store._jobs) == 4
    assert "a" not in store._jobs
    assert job.job_id in store._jobs


def test_search_job_snapshot_includes_result_or_error() -> None:
    tracker = SearchProgressTracker("job-1")
    completed = SearchJob(job_id="job-1", progress=tracker, status="completed", result={"ok": True})
    failed = SearchJob(job_id="job-2", progress=tracker, status="failed", error="boom")

    completed_payload = completed.snapshot()
    failed_payload = failed.snapshot()

    assert completed_payload["result"] == {"ok": True}
    assert "error" not in completed_payload
    assert failed_payload["error"] == "boom"
    assert "result" not in failed_payload


def test_run_job_marks_completion_and_failure() -> None:
    store = SearchJobStore()
    success = SearchJob(job_id="done", progress=SearchProgressTracker("done"))
    failure = SearchJob(job_id="fail", progress=SearchProgressTracker("fail"))
    store._jobs = {"done": success, "fail": failure}

    class OptimizerSuccess:
        def search(self, config: object, search_id: str, progress: object) -> dict[str, object]:
            assert search_id == "done"
            return {"ok": True}

    class OptimizerFailure:
        def search(self, config: object, search_id: str, progress: object) -> dict[str, object]:
            raise RuntimeError("boom")

    store._run_job(OptimizerSuccess(), object(), "done")
    store._run_job(OptimizerFailure(), object(), "fail")

    assert store._jobs["done"].status == "completed"
    assert store._jobs["done"].result == {"ok": True}
    assert store._jobs["done"].finished_at is not None
    assert store._jobs["fail"].status == "failed"
    assert store._jobs["fail"].error == "boom"
    assert store._jobs["fail"].finished_at is not None


def test_run_job_returns_when_job_is_missing() -> None:
    store = SearchJobStore()

    class Optimizer:
        def search(self, config: object, search_id: str, progress: object) -> dict[str, object]:
            raise AssertionError("search should not run when job is missing")

    store._run_job(Optimizer(), object(), "missing")


def test_update_job_ignores_missing_id() -> None:
    store = SearchJobStore()
    store._update_job("missing", status="completed", result={"ok": True})

    assert store._jobs == {}
