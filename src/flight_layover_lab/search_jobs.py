from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from .engine import SplitTripOptimizer
from .models import SearchConfig
from .progress import SearchProgressTracker


class SearchJobCapacityError(RuntimeError):
    """Raised when the async job store has no safe capacity for a new search."""


@dataclass
class SearchJob:
    job_id: str
    progress: SearchProgressTracker
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    status: str = "queued"
    result: dict[str, Any] | None = None
    error: str | None = None
    finished_at: float | None = None

    def snapshot(self, *, since_event_index: int | None = None) -> dict[str, Any]:
        payload = {
            "job_id": self.job_id,
            "status": self.status,
            "progress": self.progress.snapshot(since_event_index=since_event_index),
        }
        if self.status == "completed":
            payload["result"] = self.result
        if self.status == "failed":
            payload["error"] = self.error
        return payload


class SearchJobStore:
    def __init__(self, *, max_jobs: int = 24, ttl_seconds: int = 3600) -> None:
        self._lock = threading.Lock()
        self._jobs: dict[str, SearchJob] = {}
        self._max_jobs = max(4, max_jobs)
        self._ttl_seconds = max(300, ttl_seconds)

    def start_job(self, optimizer: SplitTripOptimizer, config: SearchConfig) -> SearchJob:
        job_id = uuid.uuid4().hex[:12]
        job = SearchJob(job_id=job_id, progress=SearchProgressTracker(job_id))
        with self._lock:
            self._prune_locked(reserve_slots=1)
            if len(self._jobs) >= self._max_jobs:
                raise SearchJobCapacityError(
                    "Too many searches are already running. Wait for one to finish and try again."
                )
            self._jobs[job_id] = job

        worker = threading.Thread(
            target=self._run_job,
            args=(optimizer, config, job_id),
            name=f"flight-search-{job_id}",
            daemon=True,
        )
        worker.start()
        return job

    def get_job(self, job_id: str) -> SearchJob | None:
        with self._lock:
            self._prune_locked()
            job = self._jobs.get(str(job_id or "").strip())
            return job

    def _run_job(self, optimizer: SplitTripOptimizer, config: SearchConfig, job_id: str) -> None:
        job = self.get_job(job_id)
        if job is None:
            return
        self._update_job(job_id, status="running")
        try:
            result = optimizer.search(
                config,
                search_id=job_id,
                progress=job.progress,
            )
        except Exception as exc:
            self._update_job(
                job_id,
                status="failed",
                error=str(exc),
                finished_at=time.time(),
            )
            return

        self._update_job(
            job_id,
            status="completed",
            result=result,
            finished_at=time.time(),
        )

    def _update_job(
        self,
        job_id: str,
        *,
        status: str | None = None,
        result: dict[str, Any] | None = None,
        error: str | None = None,
        finished_at: float | None = None,
    ) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return
            if status is not None:
                job.status = status
            if result is not None:
                job.result = result
            if error is not None:
                job.error = error
            if finished_at is not None:
                job.finished_at = finished_at
            job.updated_at = time.time()

    def _prune_locked(self, *, reserve_slots: int = 0) -> None:
        now = time.time()
        expired = [
            job_id
            for job_id, job in self._jobs.items()
            if job.finished_at is not None and (now - job.finished_at) > self._ttl_seconds
        ]
        for job_id in expired:
            self._jobs.pop(job_id, None)
        max_kept_jobs = max(0, self._max_jobs - max(0, reserve_slots))
        if len(self._jobs) <= max_kept_jobs:
            return
        finished_jobs = sorted(
            (job for job in self._jobs.values() if job.finished_at is not None),
            key=lambda job: (job.finished_at or job.updated_at, job.updated_at),
        )
        while len(self._jobs) > max_kept_jobs and finished_jobs:
            oldest = finished_jobs.pop(0)
            self._jobs.pop(oldest.job_id, None)
