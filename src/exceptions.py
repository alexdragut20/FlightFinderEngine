from __future__ import annotations


class ProviderNoResultError(RuntimeError):
    """Represent ProviderNoResultError."""


class ProviderBlockedError(ProviderNoResultError):
    """Represent a provider request blocked by anti-bot controls."""

    def __init__(
        self,
        message: str,
        *,
        manual_search_url: str | None = None,
        cooldown_seconds: int | None = None,
    ) -> None:
        """Initialize the blocked-provider error."""
        super().__init__(message)
        self.manual_search_url = str(manual_search_url or "").strip() or None
        try:
            normalized_cooldown = int(cooldown_seconds) if cooldown_seconds is not None else None
        except (TypeError, ValueError):
            normalized_cooldown = None
        self.cooldown_seconds = max(0, normalized_cooldown) if normalized_cooldown is not None else None
