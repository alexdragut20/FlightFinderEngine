from __future__ import annotations

import logging
import math
import threading
import time
from collections.abc import Callable
from typing import Any

from ..config import _FREE_PROVIDER_IDS, PROVIDER_ERROR_COOLDOWN_SECONDS
from ..exceptions import ProviderNoResultError
from ..utils.constants import PRICE_SENTINEL
from ..utils.logging import log_event
from ._cache import per_instance_lru_cache

CandidateResult = dict[str, Any]
CandidateFetcher = Callable[[Any, str], CandidateResult | None]
CandidateComparator = Callable[[CandidateResult, CandidateResult | None], bool]


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
            "calendar_errors": {},
            "calendar_no_result": {},
            "calendar_selected": {},
            "calendar_skipped_budget": {},
            "calendar_skipped_cooldown": {},
            "oneway_calls": {},
            "oneway_errors": {},
            "oneway_no_result": {},
            "oneway_selected": {},
            "oneway_skipped_budget": {},
            "oneway_skipped_cooldown": {},
            "return_calls": {},
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
        providers: dict[str, dict[str, Any]] = {}
        for provider_id in self.active_provider_ids:
            calls = sum(
                int((stats.get(bucket) or {}).get(provider_id, 0))
                for bucket in ("calendar_calls", "oneway_calls", "return_calls")
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
            if selected > 0:
                status = "selected"
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
                "selected": selected,
                "no_result": no_result,
                "errors": errors,
                "skipped_budget": skipped_budget,
                "skipped_cooldown": skipped_cooldown,
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

    def _register_provider_exception(self, provider_id: str, exc: Exception) -> None:
        """Record provider exceptions that should trigger temporary cooldowns.

        Args:
            provider_id: Provider identifier involved in the request.
            exc: Exception instance for the failure path.
        """
        text = str(exc or "").lower()
        if "too many open files" in text or int(getattr(exc, "errno", 0) or 0) == 24:
            self._pause_provider(provider_id, PROVIDER_ERROR_COOLDOWN_SECONDS)
            log_event(
                logging.WARNING,
                "provider_paused_fd_exhaustion",
                provider_id=provider_id,
                cooldown_seconds=PROVIDER_ERROR_COOLDOWN_SECONDS,
                error=str(exc),
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
            if self._provider_pause_remaining_seconds(provider_id) > 0:
                self._bump("calendar_skipped_cooldown", provider_id)
                continue
            if not self._consume_budget(provider_id):
                self._bump("calendar_skipped_budget", provider_id)
                continue
            self._bump("calendar_calls", provider_id)
            try:
                prices = provider.get_calendar_prices(
                    source=source,
                    destination=destination,
                    date_start_iso=date_start_iso,
                    date_end_iso=date_end_iso,
                    currency=currency,
                    max_stops_per_leg=max_stops_per_leg,
                    adults=adults,
                    hand_bags=hand_bags,
                    hold_bags=hold_bags,
                )
            except ProviderNoResultError:
                self._bump("calendar_no_result", provider_id)
                continue
            except Exception as exc:
                self._register_provider_exception(provider_id, exc)
                self._bump("calendar_errors", provider_id)
                continue

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
            if self._provider_pause_remaining_seconds(provider_id) > 0:
                self._bump(skipped_cooldown_bucket, provider_id)
                continue
            if not self._consume_budget(provider_id):
                self._bump(skipped_budget_bucket, provider_id)
                continue
            self._bump(calls_bucket, provider_id)
            try:
                candidate = fetch_candidate(provider, provider_id)
            except ProviderNoResultError:
                self._bump(no_result_bucket, provider_id)
                continue
            except Exception as exc:
                self._register_provider_exception(provider_id, exc)
                self._bump(errors_bucket, provider_id)
                continue
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
            no_result_bucket="return_no_result",
            errors_bucket="return_errors",
            selected_bucket="return_selected",
            fetch_candidate=_fetch,
            better_than=self._is_better_return,
        )
