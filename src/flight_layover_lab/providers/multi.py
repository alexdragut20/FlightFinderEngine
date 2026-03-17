from __future__ import annotations

import logging
import math
import threading
import time
from typing import Any

from ..config import _FREE_PROVIDER_IDS, PROVIDER_ERROR_COOLDOWN_SECONDS
from ..exceptions import ProviderNoResultError
from ..logging_utils import log_event
from ._cache import per_instance_lru_cache


class MultiProviderClient:
    def __init__(
        self,
        providers: list[Any],
        max_total_calls: int | None = None,
        max_calls_by_provider: dict[str, int | None] | None = None,
    ) -> None:
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

    @property
    def active_provider_ids(self) -> list[str]:
        return [
            str(getattr(provider, "provider_id", "") or "").lower() for provider in self.providers
        ]

    def _bump(self, bucket: str, provider_id: str, amount: int = 1) -> None:
        with self._stats_lock:
            target = self._stats.setdefault(bucket, {})
            target[provider_id] = target.get(provider_id, 0) + amount

    def stats_snapshot(self) -> dict[str, Any]:
        with self._stats_lock:
            snapshot = {key: dict(value) for key, value in self._stats.items()}
            snapshot["budget"] = {
                "max_total_calls": self._max_total_calls,
                "max_calls_by_provider": dict(self._max_calls_by_provider),
                "used_total_calls": self._budget_total_used,
                "used_calls_by_provider": dict(self._budget_used_by_provider),
            }
            return snapshot

    def _consume_budget(self, provider_id: str) -> bool:
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
        normalized_provider_id = str(provider_id or "").strip().lower()
        if not normalized_provider_id:
            return 0
        with self._stats_lock:
            pause_until = float(self._provider_paused_until.get(normalized_provider_id, 0.0) or 0.0)
        remaining = int(math.ceil(pause_until - time.time()))
        return max(0, remaining)

    def _pause_provider(self, provider_id: str, seconds: int) -> None:
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
        return merged

    @staticmethod
    def _is_better_oneway(candidate: dict[str, Any], current: dict[str, Any] | None) -> bool:
        if current is None:
            return True
        candidate_price = int(candidate.get("price") or 10**12)
        current_price = int(current.get("price") or 10**12)
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
        if current is None:
            return True
        candidate_price = int(candidate.get("price") or 10**12)
        current_price = int(current.get("price") or 10**12)
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
        fetch_candidate,
        better_than,
    ) -> dict[str, Any] | None:
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
