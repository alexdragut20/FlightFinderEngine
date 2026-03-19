from __future__ import annotations

import logging
import math
import threading
import time
from collections.abc import Callable
from typing import Any

from ..config import _FREE_PROVIDER_IDS, PROVIDER_ERROR_COOLDOWN_SECONDS
from ..exceptions import ProviderBlockedError, ProviderNoResultError
from ..utils.constants import PRICE_SENTINEL
from ..utils.logging import log_event
from ._cache import per_instance_lru_cache

CandidateResult = dict[str, Any]
CandidateFetcher = Callable[[Any, str], CandidateResult | None]
CandidateComparator = Callable[[CandidateResult, CandidateResult | None], bool]
_PROVIDER_CALL_SKIPPED = object()


class MultiProviderClient:
    """Coordinator that evaluates multiple provider clients under shared budgets."""

    def __init__(
        self,
        providers: list[Any],
        max_total_calls: int | None = None,
        max_calls_by_provider: dict[str, int | None] | None = None,
    ) -> None:
        """Initialize the MultiProviderClient.

        Args:
            providers: Provider instances for the operation.
            max_total_calls: Maximum number of provider calls allowed across the search.
            max_calls_by_provider: Mapping of max calls by provider.
        """
        self.providers = tuple(providers)
        self._provider_by_id = {
            str(getattr(provider, "provider_id", "") or "").lower(): provider
            for provider in self.providers
        }
        self._stats_lock = threading.Lock()
        self._stats: dict[str, dict[str, int]] = {
            "calendar_calls": {},
            "calendar_blocked": {},
            "calendar_errors": {},
            "calendar_no_result": {},
            "calendar_selected": {},
            "calendar_skipped_budget": {},
            "calendar_skipped_cooldown": {},
            "oneway_calls": {},
            "oneway_blocked": {},
            "oneway_errors": {},
            "oneway_no_result": {},
            "oneway_selected": {},
            "oneway_skipped_budget": {},
            "oneway_skipped_cooldown": {},
            "return_calls": {},
            "return_blocked": {},
            "return_errors": {},
            "return_no_result": {},
            "return_selected": {},
            "return_skipped_budget": {},
            "return_skipped_cooldown": {},
        }
        self._max_total_calls = (
            int(max_total_calls)
            if max_total_calls is not None and int(max_total_calls) > 0
            else None
        )
        normalized_caps: dict[str, int | None] = {}
        for provider_id in self._provider_by_id:
            cap_raw = (max_calls_by_provider or {}).get(provider_id)
            if cap_raw is None:
                normalized_caps[provider_id] = None
                continue
            cap = int(cap_raw)
            normalized_caps[provider_id] = cap if cap > 0 else None
        self._max_calls_by_provider = normalized_caps
        self._budget_total_used = 0
        self._budget_used_by_provider: dict[str, int] = {}
        self._provider_paused_until: dict[str, float] = {}
        self._provider_issue_state: dict[str, dict[str, Any]] = {}
        self._provider_request_locks: dict[str, threading.Lock] = {}
        self._provider_next_allowed_at: dict[str, float] = {}
        self._stats_listener: Callable[[dict[str, Any]], None] | None = None
        self._stats_listener_min_interval_seconds = 0.75
        self._stats_listener_last_sent_at = 0.0
        self._stats_listener_lock = threading.Lock()

    @property
    def active_provider_ids(self) -> list[str]:
        """Return the normalized provider identifiers currently in play.

        Returns:
            list[str]: Normalized provider identifiers currently in play.
        """
        return [
            str(getattr(provider, "provider_id", "") or "").lower() for provider in self.providers
        ]

    def _bump(self, bucket: str, provider_id: str, amount: int = 1) -> None:
        """Handle bump.

        Args:
            bucket: Log bucket used for throttled progress updates.
            provider_id: Provider identifier involved in the request.
            amount: Numeric amount to convert or format.
        """
        with self._stats_lock:
            target = self._stats.setdefault(bucket, {})
            target[provider_id] = target.get(provider_id, 0) + amount
        self._notify_stats_listener()

    def set_stats_listener(
        self,
        listener: Callable[[dict[str, Any]], None] | None,
        *,
        min_interval_seconds: float = 0.75,
    ) -> None:
        """Register a listener for throttled provider-health snapshots.

        Args:
            listener: Callback receiving aggregated provider-health payloads.
            min_interval_seconds: Minimum interval between callback updates.
        """
        with self._stats_listener_lock:
            self._stats_listener = listener
            self._stats_listener_min_interval_seconds = max(0.0, float(min_interval_seconds))
            self._stats_listener_last_sent_at = 0.0
        if listener is not None:
            self._notify_stats_listener(force=True)

    def stats_snapshot(self) -> dict[str, Any]:
        """Return a snapshot of provider selection and budget counters.

        Returns:
            dict[str, Any]: Provider selection and budget counters.
        """
        with self._stats_lock:
            snapshot = {key: dict(value) for key, value in self._stats.items()}
            snapshot["budget"] = {
                "max_total_calls": self._max_total_calls,
                "max_calls_by_provider": dict(self._max_calls_by_provider),
                "used_total_calls": self._budget_total_used,
                "used_calls_by_provider": dict(self._budget_used_by_provider),
            }
            return snapshot

    def health_snapshot(self) -> dict[str, Any]:
        """Return aggregated provider health counters suitable for the UI.

        Returns:
            dict[str, Any]: Aggregated provider-health counters.
        """
        stats = self.stats_snapshot()
        with self._stats_lock:
            issue_state = {key: dict(value) for key, value in self._provider_issue_state.items()}
        providers: dict[str, dict[str, Any]] = {}
        for provider_id in self.active_provider_ids:
            calls = sum(
                int((stats.get(bucket) or {}).get(provider_id, 0))
                for bucket in ("calendar_calls", "oneway_calls", "return_calls")
            )
            blocked = sum(
                int((stats.get(bucket) or {}).get(provider_id, 0))
                for bucket in ("calendar_blocked", "oneway_blocked", "return_blocked")
            )
            selected = sum(
                int((stats.get(bucket) or {}).get(provider_id, 0))
                for bucket in ("calendar_selected", "oneway_selected", "return_selected")
            )
            no_result = sum(
                int((stats.get(bucket) or {}).get(provider_id, 0))
                for bucket in ("calendar_no_result", "oneway_no_result", "return_no_result")
            )
            errors = sum(
                int((stats.get(bucket) or {}).get(provider_id, 0))
                for bucket in ("calendar_errors", "oneway_errors", "return_errors")
            )
            skipped_budget = sum(
                int((stats.get(bucket) or {}).get(provider_id, 0))
                for bucket in (
                    "calendar_skipped_budget",
                    "oneway_skipped_budget",
                    "return_skipped_budget",
                )
            )
            skipped_cooldown = sum(
                int((stats.get(bucket) or {}).get(provider_id, 0))
                for bucket in (
                    "calendar_skipped_cooldown",
                    "oneway_skipped_cooldown",
                    "return_skipped_cooldown",
                )
            )
            issue = issue_state.get(provider_id) or {}
            cooldown_seconds = self._provider_pause_remaining_seconds(provider_id)
            if cooldown_seconds <= 0:
                cooldown_seconds = int(issue.get("cooldown_seconds") or 0)
            if selected > 0:
                status = "selected"
            elif blocked > 0:
                status = "blocked"
            elif errors > 0:
                status = "error"
            elif no_result > 0:
                status = "no_result"
            elif calls > 0:
                status = "active"
            else:
                status = "idle"
            providers[provider_id] = {
                "provider_id": provider_id,
                "status": status,
                "calls": calls,
                "blocked": blocked,
                "selected": selected,
                "no_result": no_result,
                "errors": errors,
                "skipped_budget": skipped_budget,
                "skipped_cooldown": skipped_cooldown,
                "cooldown_seconds": cooldown_seconds,
                "last_issue_type": issue.get("issue_type"),
                "last_issue_message": issue.get("message"),
                "manual_search_url": issue.get("manual_search_url"),
                "calendar_calls": int((stats.get("calendar_calls") or {}).get(provider_id, 0)),
                "calendar_selected": int(
                    (stats.get("calendar_selected") or {}).get(provider_id, 0)
                ),
                "oneway_calls": int((stats.get("oneway_calls") or {}).get(provider_id, 0)),
                "oneway_selected": int((stats.get("oneway_selected") or {}).get(provider_id, 0)),
                "return_calls": int((stats.get("return_calls") or {}).get(provider_id, 0)),
                "return_selected": int((stats.get("return_selected") or {}).get(provider_id, 0)),
            }
        return {
            "providers": providers,
            "budget": stats.get("budget") or {},
            "updated_at": time.time(),
        }

    def _notify_stats_listener(self, *, force: bool = False) -> None:
        """Send a throttled provider-health snapshot to the listener.

        Args:
            force: When True, bypass throttle interval checks.
        """
        listener: Callable[[dict[str, Any]], None] | None
        with self._stats_listener_lock:
            listener = self._stats_listener
            if listener is None:
                return
            now = time.time()
            if (
                not force
                and self._stats_listener_min_interval_seconds > 0.0
                and (now - self._stats_listener_last_sent_at)
                < self._stats_listener_min_interval_seconds
            ):
                return
            self._stats_listener_last_sent_at = now
        try:
            listener(self.health_snapshot())
        except Exception:
            return

    def _consume_budget(self, provider_id: str) -> bool:
        """Consume a provider budget slot if one is available.

        Args:
            provider_id: Provider identifier involved in the request.

        Returns:
            bool: True when a budget slot was consumed; otherwise, False.
        """
        normalized_provider_id = str(provider_id or "").strip().lower()
        with self._stats_lock:
            cap = self._max_calls_by_provider.get(normalized_provider_id)
            used_by_provider = self._budget_used_by_provider.get(normalized_provider_id, 0)
            if cap is not None and used_by_provider >= cap:
                return False
            counts_towards_total_cap = normalized_provider_id not in _FREE_PROVIDER_IDS
            if (
                counts_towards_total_cap
                and self._max_total_calls is not None
                and self._budget_total_used >= self._max_total_calls
            ):
                return False
            if counts_towards_total_cap:
                self._budget_total_used += 1
            self._budget_used_by_provider[normalized_provider_id] = used_by_provider + 1
            return True

    def _provider_pause_remaining_seconds(self, provider_id: str) -> int:
        """Return the remaining provider cooldown in seconds.

        Args:
            provider_id: Provider identifier involved in the request.

        Returns:
            int: Remaining provider cooldown in seconds.
        """
        normalized_provider_id = str(provider_id or "").strip().lower()
        if not normalized_provider_id:
            return 0
        with self._stats_lock:
            pause_until = float(self._provider_paused_until.get(normalized_provider_id, 0.0) or 0.0)
        remaining = int(math.ceil(pause_until - time.time()))
        return max(0, remaining)

    def _pause_provider(self, provider_id: str, seconds: int) -> None:
        """Pause a provider for the requested cooldown window.

        Args:
            provider_id: Provider identifier involved in the request.
            seconds: Duration in seconds for the operation.
        """
        normalized_provider_id = str(provider_id or "").strip().lower()
        if not normalized_provider_id:
            return
        until = time.time() + max(0, int(seconds))
        with self._stats_lock:
            self._provider_paused_until[normalized_provider_id] = max(
                float(self._provider_paused_until.get(normalized_provider_id, 0.0) or 0.0),
                until,
            )

    @staticmethod
    def _provider_serialized_requests(provider: Any) -> bool:
        """Return whether requests to the provider should be serialized."""
        return bool(getattr(provider, "serialized_requests", False))

    @staticmethod
    def _provider_request_interval_seconds(provider: Any) -> float:
        """Return the minimum delay to keep between provider requests."""
        try:
            value = float(getattr(provider, "request_interval_seconds", 0.0) or 0.0)
        except (TypeError, ValueError):
            return 0.0
        return max(0.0, value)

    def _provider_request_lock(self, provider_id: str) -> threading.Lock:
        """Return the per-provider request lock, creating it on demand."""
        normalized_provider_id = str(provider_id or "").strip().lower()
        with self._stats_lock:
            lock = self._provider_request_locks.get(normalized_provider_id)
            if lock is None:
                lock = threading.Lock()
                self._provider_request_locks[normalized_provider_id] = lock
            return lock

    def _provider_next_allowed_at_seconds(self, provider_id: str) -> float:
        """Return the earliest timestamp when the provider can be queried again."""
        normalized_provider_id = str(provider_id or "").strip().lower()
        with self._stats_lock:
            return float(self._provider_next_allowed_at.get(normalized_provider_id, 0.0) or 0.0)

    def _set_provider_next_allowed_at_seconds(
        self, provider_id: str, next_allowed_at: float
    ) -> None:
        """Persist the next provider request timestamp."""
        normalized_provider_id = str(provider_id or "").strip().lower()
        with self._stats_lock:
            self._provider_next_allowed_at[normalized_provider_id] = max(
                float(self._provider_next_allowed_at.get(normalized_provider_id, 0.0) or 0.0),
                float(next_allowed_at or 0.0),
            )

    def _execute_provider_call(
        self,
        *,
        provider: Any,
        provider_id: str,
        skipped_cooldown_bucket: str,
        skipped_budget_bucket: str,
        calls_bucket: str,
        call: Callable[[], Any],
    ) -> Any:
        """Run a provider call with cooldown, budget, and optional serialization guards."""
        if not self._provider_serialized_requests(provider):
            if self._provider_pause_remaining_seconds(provider_id) > 0:
                self._bump(skipped_cooldown_bucket, provider_id)
                return _PROVIDER_CALL_SKIPPED
            if not self._consume_budget(provider_id):
                self._bump(skipped_budget_bucket, provider_id)
                return _PROVIDER_CALL_SKIPPED
            self._bump(calls_bucket, provider_id)
            return call()

        provider_lock = self._provider_request_lock(provider_id)
        with provider_lock:
            if self._provider_pause_remaining_seconds(provider_id) > 0:
                self._bump(skipped_cooldown_bucket, provider_id)
                return _PROVIDER_CALL_SKIPPED
            if not self._consume_budget(provider_id):
                self._bump(skipped_budget_bucket, provider_id)
                return _PROVIDER_CALL_SKIPPED
            wait_seconds = max(
                0.0,
                self._provider_next_allowed_at_seconds(provider_id) - time.time(),
            )
            if wait_seconds > 0.0:
                time.sleep(wait_seconds)
            self._bump(calls_bucket, provider_id)
            try:
                return call()
            finally:
                interval_seconds = self._provider_request_interval_seconds(provider)
                if interval_seconds > 0.0:
                    self._set_provider_next_allowed_at_seconds(
                        provider_id,
                        time.time() + interval_seconds,
                    )

    @staticmethod
    def _looks_like_blocked_error_text(text: str) -> bool:
        """Return whether error text looks like anti-bot blocking."""
        lowered = str(text or "").lower()
        return any(
            token in lowered
            for token in (
                "captcha",
                "anti-bot",
                "bot protection",
                "blocked automated scraping",
                "verify you are human",
                "verify you are a human",
                "security challenge",
                "access denied",
                "consent page",
                "before you continue",
                "temporarily paused after anti-bot blocking",
            )
        )

    @classmethod
    def _looks_like_blocked_exception(cls, exc: Exception) -> bool:
        """Return whether the exception should be treated as a provider block."""
        return isinstance(exc, ProviderBlockedError) or cls._looks_like_blocked_error_text(str(exc))

    @classmethod
    def _coerce_blocked_exception(cls, exc: Exception) -> ProviderBlockedError:
        """Normalize any block-like exception into ProviderBlockedError."""
        if isinstance(exc, ProviderBlockedError):
            return exc
        return ProviderBlockedError(str(exc))

    def _set_provider_issue(
        self,
        provider_id: str,
        *,
        issue_type: str,
        message: str,
        manual_search_url: str | None = None,
        cooldown_seconds: int | None = None,
    ) -> None:
        """Persist the latest provider issue details for the UI."""
        normalized_provider_id = str(provider_id or "").strip().lower()
        if not normalized_provider_id:
            return
        with self._stats_lock:
            self._provider_issue_state[normalized_provider_id] = {
                "issue_type": str(issue_type or "").strip().lower() or "error",
                "message": str(message or "").strip(),
                "manual_search_url": str(manual_search_url or "").strip() or None,
                "cooldown_seconds": int(cooldown_seconds or 0) or None,
                "updated_at": time.time(),
            }

    def _clear_provider_issue(self, provider_id: str) -> None:
        """Clear any stale provider issue details after a successful call."""
        normalized_provider_id = str(provider_id or "").strip().lower()
        if not normalized_provider_id:
            return
        with self._stats_lock:
            self._provider_issue_state.pop(normalized_provider_id, None)

    def _register_provider_block(self, provider_id: str, exc: Exception) -> None:
        """Record provider anti-bot blocking and pause future retries."""
        blocked_exc = self._coerce_blocked_exception(exc)
        cooldown_seconds = int(blocked_exc.cooldown_seconds or PROVIDER_ERROR_COOLDOWN_SECONDS)
        self._pause_provider(provider_id, cooldown_seconds)
        self._set_provider_issue(
            provider_id,
            issue_type="blocked",
            message=str(blocked_exc),
            manual_search_url=blocked_exc.manual_search_url,
            cooldown_seconds=cooldown_seconds,
        )
        log_event(
            logging.WARNING,
            "provider_paused_bot_block",
            provider_id=provider_id,
            cooldown_seconds=cooldown_seconds,
            manual_search_url=blocked_exc.manual_search_url,
            error=str(blocked_exc),
        )

    def _register_provider_exception(self, provider_id: str, exc: Exception) -> None:
        """Record provider exceptions that should trigger temporary cooldowns.

        Args:
            provider_id: Provider identifier involved in the request.
            exc: Exception instance for the failure path.
        """
        if self._looks_like_blocked_exception(exc):
            self._register_provider_block(provider_id, exc)
            return
        text = str(exc or "").lower()
        cooldown_seconds: int | None = None
        if "too many open files" in text or int(getattr(exc, "errno", 0) or 0) == 24:
            self._pause_provider(provider_id, PROVIDER_ERROR_COOLDOWN_SECONDS)
            cooldown_seconds = PROVIDER_ERROR_COOLDOWN_SECONDS
            log_event(
                logging.WARNING,
                "provider_paused_fd_exhaustion",
                provider_id=provider_id,
                cooldown_seconds=PROVIDER_ERROR_COOLDOWN_SECONDS,
                error=str(exc),
            )
        self._set_provider_issue(
            provider_id,
            issue_type="error",
            message=str(exc),
            cooldown_seconds=cooldown_seconds,
        )

    @per_instance_lru_cache(maxsize=128)
    def _providers_for_selection(self, provider_ids: tuple[str, ...] | None) -> tuple[Any, ...]:
        """Return the providers eligible for the current selection scope.

        Args:
            provider_ids: Provider identifiers involved in the request.

        Returns:
            tuple[Any, ...]: Providers eligible for the current selection scope.
        """
        if not provider_ids:
            return self.providers
        requested = {
            str(provider_id).strip().lower() for provider_id in provider_ids if provider_id
        }
        if not requested:
            return self.providers
        return tuple(
            provider
            for provider in self.providers
            if str(getattr(provider, "provider_id", "") or "").lower() in requested
        )

    @per_instance_lru_cache(maxsize=16384)
    def get_calendar_prices(
        self,
        source: str,
        destination: str,
        date_start_iso: str,
        date_end_iso: str,
        currency: str,
        max_stops_per_leg: int,
        adults: int,
        hand_bags: int,
        hold_bags: int,
        provider_ids: tuple[str, ...] | None = None,
    ) -> dict[str, int]:
        """Fetch calendar prices for the requested market.

        Args:
            source: Origin airport code for the request.
            destination: Destination airport code for the request.
            date_start_iso: Start date in ISO 8601 format.
            date_end_iso: End date in ISO 8601 format.
            currency: Currency code for pricing output.
            max_stops_per_leg: Max stops per leg.
            adults: Number of adult travelers.
            hand_bags: Number of cabin bags per adult traveler.
            hold_bags: Number of checked bags per adult traveler.
            provider_ids: Provider identifiers involved in the request.

        Returns:
            dict[str, int]: Calendar prices for the requested market.
        """
        merged: dict[str, int] = {}
        source_by_date: dict[str, str] = {}
        for provider in self._providers_for_selection(provider_ids):
            provider_id = str(getattr(provider, "provider_id", "unknown"))
            if not bool(getattr(provider, "supports_calendar", True)):
                continue
            try:
                prices = self._execute_provider_call(
                    provider=provider,
                    provider_id=provider_id,
                    skipped_cooldown_bucket="calendar_skipped_cooldown",
                    skipped_budget_bucket="calendar_skipped_budget",
                    calls_bucket="calendar_calls",
                    call=lambda provider=provider: provider.get_calendar_prices(
                        source=source,
                        destination=destination,
                        date_start_iso=date_start_iso,
                        date_end_iso=date_end_iso,
                        currency=currency,
                        max_stops_per_leg=max_stops_per_leg,
                        adults=adults,
                        hand_bags=hand_bags,
                        hold_bags=hold_bags,
                    ),
                )
            except ProviderBlockedError as exc:
                self._register_provider_block(provider_id, exc)
                self._bump("calendar_blocked", provider_id)
                continue
            except ProviderNoResultError as exc:
                if self._looks_like_blocked_exception(exc):
                    self._register_provider_block(provider_id, exc)
                    self._bump("calendar_blocked", provider_id)
                    continue
                self._set_provider_issue(provider_id, issue_type="no_result", message=str(exc))
                self._bump("calendar_no_result", provider_id)
                continue
            except Exception as exc:
                if self._looks_like_blocked_exception(exc):
                    self._register_provider_block(provider_id, exc)
                    self._bump("calendar_blocked", provider_id)
                    continue
                self._register_provider_exception(provider_id, exc)
                self._bump("calendar_errors", provider_id)
                continue
            if prices is _PROVIDER_CALL_SKIPPED:
                continue
            self._clear_provider_issue(provider_id)

            for date_iso, value in (prices or {}).items():
                try:
                    amount = int(value)
                except (TypeError, ValueError):
                    continue
                existing = merged.get(date_iso)
                if existing is None or amount < existing:
                    merged[date_iso] = amount
                    source_by_date[date_iso] = provider_id

        for provider_id in set(source_by_date.values()):
            self._bump("calendar_selected", provider_id)
        self._notify_stats_listener(force=True)
        return merged

    @staticmethod
    def _is_better_oneway(candidate: dict[str, Any], current: dict[str, Any] | None) -> bool:
        """Return whether better oneway.

        Args:
            candidate: Mapping of candidate.
            current: Mapping of current.

        Returns:
            bool: True when better oneway; otherwise, False.
        """
        if current is None:
            return True
        candidate_price = int(candidate.get("price") or PRICE_SENTINEL)
        current_price = int(current.get("price") or PRICE_SENTINEL)
        if candidate_price != current_price:
            return candidate_price < current_price
        candidate_stops = int(candidate.get("stops") or 0)
        current_stops = int(current.get("stops") or 0)
        if candidate_stops != current_stops:
            return candidate_stops < current_stops
        candidate_duration = candidate.get("duration_seconds")
        current_duration = current.get("duration_seconds")
        if candidate_duration is None:
            return False
        if current_duration is None:
            return True
        return int(candidate_duration) < int(current_duration)

    @staticmethod
    def _is_better_return(candidate: dict[str, Any], current: dict[str, Any] | None) -> bool:
        """Return whether better return.

        Args:
            candidate: Mapping of candidate.
            current: Mapping of current.

        Returns:
            bool: True when better return; otherwise, False.
        """
        if current is None:
            return True
        candidate_price = int(candidate.get("price") or PRICE_SENTINEL)
        current_price = int(current.get("price") or PRICE_SENTINEL)
        if candidate_price != current_price:
            return candidate_price < current_price
        candidate_stops = int(candidate.get("outbound_stops") or 0) + int(
            candidate.get("inbound_stops") or 0
        )
        current_stops = int(current.get("outbound_stops") or 0) + int(
            current.get("inbound_stops") or 0
        )
        if candidate_stops != current_stops:
            return candidate_stops < current_stops
        candidate_duration = candidate.get("duration_seconds")
        current_duration = current.get("duration_seconds")
        if candidate_duration is None:
            return False
        if current_duration is None:
            return True
        return int(candidate_duration) < int(current_duration)

    def _best_candidate_across_providers(
        self,
        *,
        provider_ids: tuple[str, ...] | None,
        skipped_cooldown_bucket: str,
        skipped_budget_bucket: str,
        calls_bucket: str,
        blocked_bucket: str,
        no_result_bucket: str,
        errors_bucket: str,
        selected_bucket: str,
        fetch_candidate: CandidateFetcher,
        better_than: CandidateComparator,
    ) -> dict[str, Any] | None:
        """Return the best candidate returned by the selected providers.

        Args:
            provider_ids: Provider identifiers involved in the request.
            skipped_cooldown_bucket: Stats bucket used for provider cooldown skips.
            skipped_budget_bucket: Stats bucket used for budget skip accounting.
            calls_bucket: Stats bucket used for provider call counting.
            blocked_bucket: Stats bucket used when providers are blocked by anti-bot controls.
            no_result_bucket: Stats bucket used when providers return no result.
            errors_bucket: Stats bucket used when providers raise an error.
            selected_bucket: Stats bucket used for the winning provider.
            fetch_candidate: Callback that fetches a candidate from a provider.
            better_than: Callback that compares a candidate against the current best result.

        Returns:
            dict[str, Any] | None: The best candidate returned by the selected providers.
        """
        best: dict[str, Any] | None = None
        best_provider = ""
        for provider in self._providers_for_selection(provider_ids):
            provider_id = str(getattr(provider, "provider_id", "unknown"))
            try:
                candidate = self._execute_provider_call(
                    provider=provider,
                    provider_id=provider_id,
                    skipped_cooldown_bucket=skipped_cooldown_bucket,
                    skipped_budget_bucket=skipped_budget_bucket,
                    calls_bucket=calls_bucket,
                    call=lambda provider=provider, provider_id=provider_id: fetch_candidate(
                        provider, provider_id
                    ),
                )
            except ProviderBlockedError as exc:
                self._register_provider_block(provider_id, exc)
                self._bump(blocked_bucket, provider_id)
                continue
            except ProviderNoResultError as exc:
                if self._looks_like_blocked_exception(exc):
                    self._register_provider_block(provider_id, exc)
                    self._bump(blocked_bucket, provider_id)
                    continue
                self._set_provider_issue(provider_id, issue_type="no_result", message=str(exc))
                self._bump(no_result_bucket, provider_id)
                continue
            except Exception as exc:
                if self._looks_like_blocked_exception(exc):
                    self._register_provider_block(provider_id, exc)
                    self._bump(blocked_bucket, provider_id)
                    continue
                self._register_provider_exception(provider_id, exc)
                self._bump(errors_bucket, provider_id)
                continue
            if candidate is _PROVIDER_CALL_SKIPPED:
                continue
            self._clear_provider_issue(provider_id)
            if not candidate:
                continue
            candidate = dict(candidate)
            candidate["provider"] = str(candidate.get("provider") or provider_id)
            if better_than(candidate, best):
                best = candidate
                best_provider = candidate["provider"]
        if best_provider:
            self._bump(selected_bucket, best_provider)
        self._notify_stats_listener(force=True)
        return best

    @per_instance_lru_cache(maxsize=32768)
    def get_best_oneway(
        self,
        source: str,
        destination: str,
        departure_iso: str,
        currency: str,
        max_stops_per_leg: int,
        adults: int,
        hand_bags: int,
        hold_bags: int,
        max_connection_layover_seconds: int | None = None,
        provider_ids: tuple[str, ...] | None = None,
    ) -> dict[str, Any] | None:
        """Fetch the best one-way itinerary for the requested market.

        Args:
            source: Origin airport code for the request.
            destination: Destination airport code for the request.
            departure_iso: Departure date in ISO 8601 format.
            currency: Currency code for pricing output.
            max_stops_per_leg: Max stops per leg.
            adults: Number of adult travelers.
            hand_bags: Number of cabin bags per adult traveler.
            hold_bags: Number of checked bags per adult traveler.
            max_connection_layover_seconds: Duration in seconds for max connection layover.
            provider_ids: Provider identifiers involved in the request.

        Returns:
            dict[str, Any] | None: The best one-way itinerary for the requested market.
        """

        def _fetch(provider: Any, _: str) -> dict[str, Any] | None:
            return provider.get_best_oneway(
                source=source,
                destination=destination,
                departure_iso=departure_iso,
                currency=currency,
                max_stops_per_leg=max_stops_per_leg,
                adults=adults,
                hand_bags=hand_bags,
                hold_bags=hold_bags,
                max_connection_layover_seconds=max_connection_layover_seconds,
            )

        return self._best_candidate_across_providers(
            provider_ids=provider_ids,
            skipped_cooldown_bucket="oneway_skipped_cooldown",
            skipped_budget_bucket="oneway_skipped_budget",
            calls_bucket="oneway_calls",
            blocked_bucket="oneway_blocked",
            no_result_bucket="oneway_no_result",
            errors_bucket="oneway_errors",
            selected_bucket="oneway_selected",
            fetch_candidate=_fetch,
            better_than=self._is_better_oneway,
        )

    @per_instance_lru_cache(maxsize=32768)
    def get_best_return(
        self,
        source: str,
        destination: str,
        outbound_iso: str,
        inbound_iso: str,
        currency: str,
        max_stops_per_leg: int,
        adults: int,
        hand_bags: int,
        hold_bags: int,
        max_connection_layover_seconds: int | None = None,
        provider_ids: tuple[str, ...] | None = None,
    ) -> dict[str, Any] | None:
        """Fetch the best round-trip itinerary for the requested market.

        Args:
            source: Origin airport code for the request.
            destination: Destination airport code for the request.
            outbound_iso: Outbound travel date in ISO 8601 format.
            inbound_iso: Inbound travel date in ISO 8601 format.
            currency: Currency code for pricing output.
            max_stops_per_leg: Max stops per leg.
            adults: Number of adult travelers.
            hand_bags: Number of cabin bags per adult traveler.
            hold_bags: Number of checked bags per adult traveler.
            max_connection_layover_seconds: Duration in seconds for max connection layover.
            provider_ids: Provider identifiers involved in the request.

        Returns:
            dict[str, Any] | None: The best round-trip itinerary for the requested market.
        """

        def _fetch(provider: Any, _: str) -> dict[str, Any] | None:
            return provider.get_best_return(
                source=source,
                destination=destination,
                outbound_iso=outbound_iso,
                inbound_iso=inbound_iso,
                currency=currency,
                max_stops_per_leg=max_stops_per_leg,
                adults=adults,
                hand_bags=hand_bags,
                hold_bags=hold_bags,
                max_connection_layover_seconds=max_connection_layover_seconds,
            )

        return self._best_candidate_across_providers(
            provider_ids=provider_ids,
            skipped_cooldown_bucket="return_skipped_cooldown",
            skipped_budget_bucket="return_skipped_budget",
            calls_bucket="return_calls",
            blocked_bucket="return_blocked",
            no_result_bucket="return_no_result",
            errors_bucket="return_errors",
            selected_bucket="return_selected",
            fetch_candidate=_fetch,
            better_than=self._is_better_return,
        )
