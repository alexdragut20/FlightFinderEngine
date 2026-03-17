from __future__ import annotations


class ProviderNoResultError(RuntimeError):
    """Provider returned a valid response but no offers for the query constraints."""
