from __future__ import annotations

import atexit
import contextlib
import json
import math
import re
import threading
import time
from typing import Any
from urllib.parse import urlencode, urlsplit, urlunsplit

import requests

from ..config import (
    SKYSCANNER_PLAYWRIGHT_ACQUIRE_TIMEOUT_SECONDS,
    SKYSCANNER_PLAYWRIGHT_ERROR_COOLDOWN_SECONDS,
    SKYSCANNER_PLAYWRIGHT_HOST_ATTEMPTS,
    SKYSCANNER_PLAYWRIGHT_MAX_CONCURRENCY,
    SKYSCANNER_SCRAPE_HOST,
    SKYSCANNER_SCRAPE_HOSTS,
    SKYSCANNER_SCRAPE_HTTP_RETRIES,
    SKYSCANNER_SCRAPE_PLAYWRIGHT_FALLBACK,
    SKYSCANNER_WAF_COOLDOWN_SECONDS,
)
from ..exceptions import ProviderBlockedError, ProviderNoResultError
from ..utils import date_only, parse_money_amount_int
from ._cache import per_instance_lru_cache


class SkyscannerScrapeClient:
    """Provider client for Skyscanner scrape-based flight lookups."""

    provider_id = "skyscanner"
    display_name = "Skyscanner Scrape (experimental)"
    supports_calendar = False
    requires_credentials = False
    credential_env: tuple[str, ...] = ()
    docs_url = "https://www.skyscanner.com/transport/flights/"
    default_enabled = True
    _USER_AGENTS = (
        (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/121.0.0.0 Safari/537.36"
        ),
        (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
    )
    _BLOCK_MARKERS = (
        "captcha",
        "captcha-v2",
        "px-captcha",
        "perimeterx",
        "verify you are human",
        "are you a robot",
        "access denied",
        "bot protection",
        "enable javascript and cookies",
        "security challenge",
        "just a moment",
    )
    _PLAYWRIGHT_GATE = threading.BoundedSemaphore(SKYSCANNER_PLAYWRIGHT_MAX_CONCURRENCY)
    _COOLDOWN_LOCK = threading.Lock()
    _PROVIDER_COOLDOWN_UNTIL = 0.0
    _PLAYWRIGHT_COOLDOWN_UNTIL = 0.0
    _PLAYWRIGHT_RUNTIME_LOCK = threading.Lock()
    _PLAYWRIGHT_RUNTIME: tuple[Any, Any] | None = None

    def __init__(
        self,
        host: str | None = None,
        host_candidates: list[str] | tuple[str, ...] | None = None,
        http_retries: int | None = None,
        playwright_fallback: bool | None = None,
    ) -> None:
        """Initialize the SkyscannerScrapeClient.

        Args:
            host: Host name for the request.
            host_candidates: Collection of host candidates.
            http_retries: Number of HTTP retries to allow.
            playwright_fallback: Flag that controls whether Playwright fallback is enabled.
        """
        normalized_host = str(host or SKYSCANNER_SCRAPE_HOST).strip().lower()
        self._host = normalized_host or SKYSCANNER_SCRAPE_HOST
        configured_candidates = list(host_candidates or SKYSCANNER_SCRAPE_HOSTS)
        self._host_candidates_config = [
            str(candidate or "").strip().lower()
            for candidate in configured_candidates
            if str(candidate or "").strip()
        ]
        self._http_retries = max(
            1,
            min(
                6,
                int(http_retries) if http_retries is not None else SKYSCANNER_SCRAPE_HTTP_RETRIES,
            ),
        )
        self._playwright_fallback = (
            bool(playwright_fallback)
            if playwright_fallback is not None
            else SKYSCANNER_SCRAPE_PLAYWRIGHT_FALLBACK
        )
        self._local = threading.local()

    @classmethod
    def _set_provider_cooldown(cls, seconds: int) -> None:
        """Set the provider cooldown window.

        Args:
            seconds: Duration in seconds for the operation.
        """
        until = time.time() + max(0, int(seconds))
        with cls._COOLDOWN_LOCK:
            cls._PROVIDER_COOLDOWN_UNTIL = max(cls._PROVIDER_COOLDOWN_UNTIL, until)

    @classmethod
    def _set_playwright_cooldown(cls, seconds: int) -> None:
        """Set the Playwright cooldown window.

        Args:
            seconds: Duration in seconds for the operation.
        """
        until = time.time() + max(0, int(seconds))
        with cls._COOLDOWN_LOCK:
            cls._PLAYWRIGHT_COOLDOWN_UNTIL = max(cls._PLAYWRIGHT_COOLDOWN_UNTIL, until)

    @classmethod
    def _provider_cooldown_remaining_seconds(cls) -> int:
        """Return the remaining provider cooldown in seconds.

        Returns:
            int: The remaining provider cooldown in seconds.
        """
        with cls._COOLDOWN_LOCK:
            remaining = int(math.ceil(cls._PROVIDER_COOLDOWN_UNTIL - time.time()))
        return max(0, remaining)

    @classmethod
    def _playwright_cooldown_remaining_seconds(cls) -> int:
        """Return the remaining Playwright cooldown in seconds.

        Returns:
            int: The remaining Playwright cooldown in seconds.
        """
        with cls._COOLDOWN_LOCK:
            remaining = int(math.ceil(cls._PLAYWRIGHT_COOLDOWN_UNTIL - time.time()))
        return max(0, remaining)

    @classmethod
    def _get_or_start_playwright_runtime(cls) -> tuple[Any, Any]:
        """Get or start the shared Playwright runtime.

        Returns:
            tuple[Any, Any]: Get or start the shared Playwright runtime.
        """
        with cls._PLAYWRIGHT_RUNTIME_LOCK:
            if cls._PLAYWRIGHT_RUNTIME is not None:
                return cls._PLAYWRIGHT_RUNTIME
            try:
                from playwright.sync_api import sync_playwright
            except Exception as exc:
                raise ProviderNoResultError(
                    "Playwright fallback unavailable. Install with "
                    "`python3 -m pip install playwright` then "
                    "`python3 -m playwright install chromium`."
                ) from exc
            playwright = sync_playwright().start()
            browser = playwright.chromium.launch(headless=True)
            cls._PLAYWRIGHT_RUNTIME = (playwright, browser)
            return cls._PLAYWRIGHT_RUNTIME

    @classmethod
    def _close_playwright_runtime(cls) -> None:
        """Close the shared Playwright runtime."""
        with cls._PLAYWRIGHT_RUNTIME_LOCK:
            runtime = cls._PLAYWRIGHT_RUNTIME
            cls._PLAYWRIGHT_RUNTIME = None
        if runtime is None:
            return
        playwright, browser = runtime
        with contextlib.suppress(Exception):
            browser.close()
        with contextlib.suppress(Exception):
            playwright.stop()

    def is_configured(self) -> bool:
        """Return whether the client is configured for use.

        Returns:
            bool: True when the client is configured for use; otherwise, False.
        """
        return True

    def _session(self) -> requests.Session:
        """Return the cached requests session.

        Returns:
            requests.Session: The cached requests session.
        """
        if not hasattr(self._local, "session"):
            session = requests.Session()
            session.headers.update(
                {
                    "User-Agent": self._USER_AGENTS[0],
                    "Accept-Language": "en-US,en;q=0.9",
                    "Cache-Control": "no-cache",
                    "Pragma": "no-cache",
                }
            )
            self._local.session = session
        return self._local.session

    def _hosts_to_try(self) -> list[str]:
        """Return the ordered list of provider hosts to try.

        Returns:
            list[str]: The ordered list of provider hosts to try.
        """
        out: list[str] = []
        for candidate in [self._host, *self._host_candidates_config, "www.skyscanner.com"]:
            host = str(candidate or "").strip().lower()
            if not host or host in out:
                continue
            out.append(host)
        return out

    @staticmethod
    def _replace_url_host(url: str, host: str) -> str:
        """Replace the host portion of a URL.

        Args:
            url: URL to request or parse.
            host: Host name for the request.

        Returns:
            str: Replace the host portion of a URL.
        """
        parsed = urlsplit(url)
        if not parsed.scheme:
            return f"https://{host}{url}"
        return urlunsplit((parsed.scheme, host, parsed.path, parsed.query, parsed.fragment))

    def _search_page_url(
        self,
        source: str,
        destination: str,
        outbound_iso: str,
        inbound_iso: str | None,
        currency: str,
        adults: int,
    ) -> str:
        """Build the provider search page URL.

        Args:
            source: Origin airport code for the request.
            destination: Destination airport code for the request.
            outbound_iso: Outbound travel date in ISO 8601 format.
            inbound_iso: Inbound travel date in ISO 8601 format.
            currency: Currency code for pricing output.
            adults: Number of adult travelers.

        Returns:
            str: The provider search page URL.
        """
        outbound_part = date_only(outbound_iso).replace("-", "")
        inbound_part = date_only(inbound_iso).replace("-", "") if inbound_iso else ""
        path = f"/transport/flights/{source.lower()}/{destination.lower()}/{outbound_part}/"
        if inbound_part:
            path += f"{inbound_part}/"
        params = {
            "adults": max(1, int(adults or 1)),
            "adultsv2": max(1, int(adults or 1)),
            "cabinclass": "economy",
            "rtn": 1 if inbound_part else 0,
            "currency": str(currency or "RON").strip().upper() or "RON",
        }
        return f"https://{self._host}{path}?{urlencode(params)}"

    def _http_fetch_search_html(self, url: str, attempt_idx: int = 0) -> tuple[str, str, int]:
        """Fetch the search page over HTTP.

        Args:
            url: URL to request or parse.
            attempt_idx: Zero-based attempt index for the retry loop.

        Returns:
            tuple[str, str, int]: The search page over HTTP.
        """
        session = self._session()
        session.headers["User-Agent"] = self._USER_AGENTS[attempt_idx % len(self._USER_AGENTS)]
        response = session.get(url, timeout=45, allow_redirects=True)
        text = str(response.text or "")
        final_url = str(response.url or url)
        return text, final_url, int(response.status_code)

    @classmethod
    def _is_bot_blocked_response(
        cls,
        html: str,
        final_url: str,
        status_code: int,
    ) -> bool:
        """Return whether the response indicates bot blocking.

        Args:
            html: HTML document to parse.
            final_url: URL for final.
            status_code: HTTP status code for the response.

        Returns:
            bool: True when the response indicates bot blocking; otherwise, False.
        """
        if status_code in {403, 429, 451, 503}:
            return True
        lowered_url = final_url.lower()
        if any(
            marker in lowered_url for marker in ("/captcha", "captcha-v2", "/sttc/", "px-captcha")
        ):
            return True
        lowered_text = html.lower()
        return any(marker in lowered_text for marker in cls._BLOCK_MARKERS)

    def _fetch_search_html_playwright(self, url: str) -> tuple[str, str]:
        """Fetch the search page through Playwright.

        Args:
            url: URL to request or parse.

        Returns:
            tuple[str, str]: The search page through Playwright.
        """
        cooldown_remaining = self._playwright_cooldown_remaining_seconds()
        if cooldown_remaining > 0:
            raise ProviderNoResultError(
                "Skyscanner Playwright fallback temporarily paused "
                f"(retry in ~{cooldown_remaining}s)."
            )

        acquired = self._PLAYWRIGHT_GATE.acquire(
            timeout=SKYSCANNER_PLAYWRIGHT_ACQUIRE_TIMEOUT_SECONDS
        )
        if not acquired:
            raise ProviderNoResultError(
                "Skyscanner Playwright fallback is busy; skipped to protect system resources."
            )

        try:
            cooldown_remaining_after_wait = self._playwright_cooldown_remaining_seconds()
            if cooldown_remaining_after_wait > 0:
                raise ProviderNoResultError(
                    "Skyscanner Playwright fallback temporarily paused "
                    f"(retry in ~{cooldown_remaining_after_wait}s)."
                )

            try:
                from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

                _, browser = self._get_or_start_playwright_runtime()
                if not browser.is_connected():
                    self._close_playwright_runtime()
                    _, browser = self._get_or_start_playwright_runtime()
                context = browser.new_context(
                    locale="en-US",
                    user_agent=self._USER_AGENTS[0],
                )
                page = None
                try:
                    page = context.new_page()
                    try:
                        page.goto(url, wait_until="domcontentloaded", timeout=45000)
                    except PlaywrightTimeoutError as exc:
                        raise RuntimeError("Skyscanner Playwright navigation timed out.") from exc
                    try:
                        page.wait_for_load_state("networkidle", timeout=10000)
                    except PlaywrightTimeoutError:
                        # Keep best-effort HTML even when long-polling scripts never settle.
                        pass
                    page.wait_for_timeout(1500)
                    final_url = str(page.url or url)
                    html = str(page.content() or "")
                    return html, final_url
                finally:
                    if page is not None:
                        with contextlib.suppress(Exception):
                            page.close()
                    with contextlib.suppress(Exception):
                        context.close()
            except OSError as exc:
                if int(getattr(exc, "errno", 0) or 0) == 24:
                    self._set_playwright_cooldown(SKYSCANNER_PLAYWRIGHT_ERROR_COOLDOWN_SECONDS)
                    self._close_playwright_runtime()
                    raise ProviderNoResultError(
                        "Skyscanner Playwright temporarily disabled after "
                        "OS file-descriptor exhaustion (Too many open files)."
                    ) from exc
                raise
            except Exception as exc:
                lowered = str(exc).lower()
                if "too many open files" in lowered:
                    self._set_playwright_cooldown(SKYSCANNER_PLAYWRIGHT_ERROR_COOLDOWN_SECONDS)
                    self._close_playwright_runtime()
                    raise ProviderNoResultError(
                        "Skyscanner Playwright temporarily disabled after "
                        "file-descriptor exhaustion."
                    ) from exc
                raise
        finally:
            self._PLAYWRIGHT_GATE.release()

    def _fetch_search_html(self, url: str) -> tuple[str, str]:
        """Fetch the search page using the best available strategy.

        Args:
            url: URL to request or parse.

        Returns:
            tuple[str, str]: The search page using the best available strategy.
        """
        provider_cooldown = self._provider_cooldown_remaining_seconds()
        if provider_cooldown > 0:
            raise ProviderBlockedError(
                "Skyscanner temporarily paused after anti-bot blocking "
                f"(retry in ~{provider_cooldown}s).",
                manual_search_url=url,
                cooldown_seconds=provider_cooldown,
            )

        blocked_detected = False
        errors: list[str] = []
        hosts = self._hosts_to_try()
        for host in hosts:
            host_url = self._replace_url_host(url, host)
            for attempt in range(self._http_retries):
                try:
                    html, final_url, status_code = self._http_fetch_search_html(
                        host_url,
                        attempt_idx=attempt,
                    )
                except requests.RequestException as exc:
                    errors.append(f"{host} network error: {exc}")
                    continue

                if self._is_bot_blocked_response(html, final_url, status_code):
                    blocked_detected = True
                    errors.append(f"{host} blocked with status {status_code}")
                    break

                if status_code >= 400:
                    errors.append(f"{host} HTTP {status_code}")
                    break

                if html.strip():
                    return html, final_url
                errors.append(f"{host} empty HTML response")

        if self._playwright_fallback:
            hosts_for_playwright = hosts[:SKYSCANNER_PLAYWRIGHT_HOST_ATTEMPTS]
            for host in hosts_for_playwright:
                host_url = self._replace_url_host(url, host)
                try:
                    html, final_url = self._fetch_search_html_playwright(host_url)
                except ProviderNoResultError as exc:
                    if isinstance(exc, ProviderBlockedError):
                        blocked_detected = True
                    else:
                        message = str(exc or "").lower()
                        if any(
                            token in message
                            for token in ("blocked", "captcha", "anti-bot", "challenge")
                        ):
                            blocked_detected = True
                    errors.append(f"{host} Playwright skipped: {exc}")
                    continue
                except Exception as exc:
                    errors.append(f"{host} Playwright error: {exc}")
                    continue
                if self._is_bot_blocked_response(html, final_url, 200):
                    blocked_detected = True
                    errors.append(f"{host} Playwright blocked with challenge page")
                    continue
                if html.strip():
                    return html, final_url
                errors.append(f"{host} Playwright empty HTML response")

        if blocked_detected:
            self._set_provider_cooldown(SKYSCANNER_WAF_COOLDOWN_SECONDS)
            raise ProviderBlockedError(
                "Skyscanner blocked automated scraping (captcha/anti-bot challenge).",
                manual_search_url=url,
                cooldown_seconds=SKYSCANNER_WAF_COOLDOWN_SECONDS,
            )
        summary = "; ".join(errors[:3]) if errors else "unknown fetch failure"
        raise RuntimeError(f"Skyscanner scrape failed: {summary}")

    @staticmethod
    def _extract_best_price(html: str) -> int | None:
        """Extract the best visible price from the search page.

        Args:
            html: HTML document to parse.

        Returns:
            int | None: Extract the best visible price from the search page.
        """
        offers = SkyscannerScrapeClient._extract_offer_options(html)
        if offers:
            return int(offers[0].get("price") or 0) or None
        patterns = [
            r'"rawPrice"\s*:\s*([0-9]+(?:\.[0-9]+)?)',
            r'"price"\s*:\s*"?([0-9][0-9.,]{1,12})"?',
            r'"minPrice"\s*:\s*"?([0-9][0-9.,]{1,12})"?',
            r'"lowestPrice"\s*:\s*"?([0-9][0-9.,]{1,12})"?',
        ]
        candidates: list[int] = []
        for pattern in patterns:
            for match in re.finditer(pattern, html):
                amount = parse_money_amount_int(match.group(1))
                if amount is None:
                    continue
                if 20 <= amount <= 500000:
                    candidates.append(amount)
        if not candidates:
            return None
        return min(candidates)

    @staticmethod
    def _extract_offer_options(html: str) -> list[dict[str, Any]]:
        """Extract offer options from the provider payload.

        Args:
            html: HTML document to parse.

        Returns:
            list[dict[str, Any]]: Extract offer options from the provider payload.
        """
        offers: list[dict[str, Any]] = []
        for payload in SkyscannerScrapeClient._extract_json_script_payloads(html):
            SkyscannerScrapeClient._collect_offer_nodes(payload, offers)
        if not offers:
            offers.extend(SkyscannerScrapeClient._extract_offer_options_regex(html))
        deduped: list[dict[str, Any]] = []
        seen: set[tuple[str, int, str]] = set()
        for offer in sorted(
            offers,
            key=lambda item: (int(item.get("price") or 10**9), str(item.get("provider") or "")),
        ):
            provider = str(offer.get("provider") or "").strip()
            if not provider:
                continue
            try:
                price = int(offer.get("price") or 0)
            except Exception:
                continue
            if price <= 0:
                continue
            currency = str(offer.get("currency") or "").strip().upper()
            key = (provider.lower(), price, currency)
            if key in seen:
                continue
            seen.add(key)
            deduped.append(
                {
                    "provider": provider,
                    "price": price,
                    "currency": currency or None,
                    "formatted_price": str(
                        offer.get("formatted_price")
                        or f"{price}{(' ' + currency) if currency else ''}"
                    ).strip(),
                    "booking_url": str(offer.get("booking_url") or "").strip() or None,
                }
            )
        return deduped

    @staticmethod
    def _extract_json_script_payloads(html: str) -> list[Any]:
        """Extract JSON payloads embedded in script tags.

        Args:
            html: HTML document to parse.

        Returns:
            list[Any]: Extract JSON payloads embedded in script tags.
        """
        payloads: list[Any] = []
        for match in re.finditer(r"<script[^>]*>(.*?)</script>", html, re.IGNORECASE | re.DOTALL):
            raw = str(match.group(1) or "").strip()
            if not raw:
                continue
            candidates: list[str] = []
            if raw.startswith("{") or raw.startswith("["):
                candidates.append(raw)
            assignment_match = re.search(r"=\s*({.*})\s*;?\s*$", raw, re.DOTALL)
            if assignment_match:
                candidates.append(str(assignment_match.group(1) or "").strip())
            for candidate in candidates:
                if not candidate:
                    continue
                try:
                    payload = json.loads(candidate)
                except Exception:
                    continue
                if isinstance(payload, dict | list):
                    payloads.append(payload)
        return payloads

    @staticmethod
    def _extract_offer_options_regex(html: str) -> list[dict[str, Any]]:
        """Extract offer options regex.

        Args:
            html: HTML document to parse.

        Returns:
            list[dict[str, Any]]: Extract offer options regex.
        """
        offers: list[dict[str, Any]] = []
        name_keys = "bookingProviderName|providerName|agentName|merchantName|vendorName|name"
        price_keys = "rawPrice|totalPrice|minPrice|lowestPrice|price|amount"
        patterns = [
            rf'"(?:{name_keys})"\s*:\s*"([^"]{{2,90}})".{{0,260}}?"(?:{price_keys})"\s*:\s*"?([0-9][0-9.,]{{0,12}})"?',
            rf'"(?:{price_keys})"\s*:\s*"?([0-9][0-9.,]{{0,12}})"?.{{0,220}}?"(?:{name_keys})"\s*:\s*"([^"]{{2,90}})"',
        ]
        for pattern in patterns:
            for match in re.finditer(pattern, html, re.IGNORECASE | re.DOTALL):
                if pattern == patterns[0]:
                    provider = str(match.group(1) or "").strip()
                    price_raw = match.group(2)
                else:
                    price_raw = match.group(1)
                    provider = str(match.group(2) or "").strip()
                if not provider:
                    continue
                price = parse_money_amount_int(price_raw)
                if price is None or not (20 <= price <= 500000):
                    continue
                offers.append(
                    {
                        "provider": provider,
                        "price": int(price),
                        "currency": None,
                        "formatted_price": str(price),
                        "booking_url": None,
                    }
                )
        return offers

    @staticmethod
    def _collect_offer_nodes(payload: Any, offers: list[dict[str, Any]]) -> None:
        """Handle collect offer nodes.

        Args:
            payload: JSON-serializable payload for the operation.
            offers: Mapping of offers.
        """
        stack: list[Any] = [payload]
        max_nodes = 20000
        nodes_seen = 0
        while stack and nodes_seen < max_nodes:
            node = stack.pop()
            nodes_seen += 1
            if isinstance(node, dict):
                offer = SkyscannerScrapeClient._offer_from_node(node)
                if offer:
                    offers.append(offer)
                for value in node.values():
                    if isinstance(value, dict | list):
                        stack.append(value)
            elif isinstance(node, list):
                for value in node:
                    if isinstance(value, dict | list):
                        stack.append(value)

    @staticmethod
    def _offer_from_node(node: dict[str, Any]) -> dict[str, Any] | None:
        """Handle offer from node.

        Args:
            node: Mapping of node.

        Returns:
            dict[str, Any] | None: Handle offer from node.
        """
        provider = SkyscannerScrapeClient._extract_provider_name(node)
        if not provider:
            return None
        price = SkyscannerScrapeClient._extract_price_from_node(node)
        if price is None or not (20 <= price <= 500000):
            return None
        currency = SkyscannerScrapeClient._extract_currency_from_node(node)
        booking_url = SkyscannerScrapeClient._extract_booking_url_from_node(node)
        formatted_price = f"{price}{(' ' + currency) if currency else ''}"
        return {
            "provider": provider,
            "price": int(price),
            "currency": currency,
            "formatted_price": formatted_price,
            "booking_url": booking_url,
        }

    @staticmethod
    def _extract_provider_name(node: dict[str, Any]) -> str | None:
        """Extract provider name.

        Args:
            node: Mapping of node.

        Returns:
            str | None: Extract provider name.
        """
        preferred_keys = {
            "bookingprovidername",
            "bookingprovider",
            "providername",
            "agentname",
            "merchantname",
            "vendorname",
            "ota",
            "sellername",
        }
        fallback_keys = {"name", "label", "displayname"}
        for key, value in node.items():
            key_l = str(key or "").strip().lower()
            if key_l in preferred_keys and isinstance(value, str) and value.strip():
                return value.strip()
        for container_key in ("agent", "provider", "merchant", "vendor", "bookingprovider"):
            nested = node.get(container_key)
            if isinstance(nested, dict):
                for nested_name_key in ("name", "displayName", "providerName", "agentName"):
                    nested_value = nested.get(nested_name_key)
                    if isinstance(nested_value, str) and nested_value.strip():
                        return nested_value.strip()
        for key, value in node.items():
            key_l = str(key or "").strip().lower()
            if key_l in fallback_keys and isinstance(value, str) and value.strip():
                if SkyscannerScrapeClient._extract_price_from_node(node) is not None:
                    return value.strip()
        return None

    @staticmethod
    def _extract_price_from_node(node: dict[str, Any]) -> int | None:
        """Extract price from node.

        Args:
            node: Mapping of node.

        Returns:
            int | None: Extract price from node.
        """
        candidate_values: list[Any] = []
        for key, value in node.items():
            key_l = str(key or "").strip().lower()
            if key_l in {
                "rawprice",
                "price",
                "totalprice",
                "lowestprice",
                "minprice",
                "amount",
                "value",
                "total",
            }:
                candidate_values.append(value)
            elif "price" in key_l and not isinstance(value, dict | list):
                candidate_values.append(value)
            elif key_l in {"pricing", "fare"} and isinstance(value, dict):
                candidate_values.append(value)
        for value in candidate_values:
            parsed = SkyscannerScrapeClient._parse_price_value(value, depth=1)
            if parsed is not None and 20 <= parsed <= 500000:
                return parsed
        return None

    @staticmethod
    def _parse_price_value(value: Any, depth: int = 0) -> int | None:
        """Parse price value.

        Args:
            value: Input value to process.
            depth: Traversal depth for the graph search.

        Returns:
            int | None: Parsed price value.
        """
        if isinstance(value, dict):
            for key in (
                "rawPrice",
                "raw_price",
                "price",
                "totalPrice",
                "total",
                "amount",
                "value",
                "formattedPrice",
            ):
                parsed = parse_money_amount_int(value.get(key))
                if parsed is not None and 20 <= parsed <= 500000:
                    return int(parsed)
            if depth > 0:
                for nested in value.values():
                    parsed = SkyscannerScrapeClient._parse_price_value(nested, depth=depth - 1)
                    if parsed is not None and 20 <= parsed <= 500000:
                        return int(parsed)
            return None
        parsed = parse_money_amount_int(value)
        if parsed is None:
            return None
        return int(parsed)

    @staticmethod
    def _extract_currency_from_node(node: dict[str, Any]) -> str | None:
        """Extract currency from node.

        Args:
            node: Mapping of node.

        Returns:
            str | None: Extract currency from node.
        """
        for key, value in node.items():
            key_l = str(key or "").strip().lower()
            if key_l in {"currency", "currencycode", "curr"}:
                text = str(value or "").strip().upper()
                if len(text) == 3 and text.isalpha():
                    return text
            if key_l in {"price", "pricing", "fare"} and isinstance(value, dict):
                for nested_key in ("currency", "currencyCode", "curr"):
                    nested_value = value.get(nested_key)
                    nested_text = str(nested_value or "").strip().upper()
                    if len(nested_text) == 3 and nested_text.isalpha():
                        return nested_text
        return None

    @staticmethod
    def _extract_booking_url_from_node(node: dict[str, Any]) -> str | None:
        """Extract booking url from node.

        Args:
            node: Mapping of node.

        Returns:
            str | None: Extract booking url from node.
        """
        for key, value in node.items():
            key_l = str(key or "").strip().lower()
            if key_l in {
                "deeplink",
                "deep_link",
                "bookingurl",
                "url",
                "redirecturl",
                "externalurl",
                "clickurl",
            }:
                text = str(value or "").strip()
                if text.startswith("http://") or text.startswith("https://"):
                    return text
            if key_l in {"price", "pricing", "fare", "booking"} and isinstance(value, dict):
                for nested_key in ("deeplink", "bookingUrl", "url", "redirectUrl"):
                    nested_url = str(value.get(nested_key) or "").strip()
                    if nested_url.startswith("http://") or nested_url.startswith("https://"):
                        return nested_url
        return None

    @staticmethod
    def _extract_stops_hint(html: str, max_stops_per_leg: int) -> int:
        """Extract stops hint.

        Args:
            html: HTML document to parse.
            max_stops_per_leg: Max stops per leg.

        Returns:
            int: Extract stops hint.
        """
        for match in re.finditer(r'"stops"\s*:\s*([0-9])', html):
            try:
                parsed = int(match.group(1))
            except ValueError:
                continue
            if parsed <= max_stops_per_leg:
                return max(0, parsed)
        return 0

    @staticmethod
    def _synthetic_segments(source: str, destination: str) -> list[dict[str, Any]]:
        """Handle synthetic segments.

        Args:
            source: Origin airport code for the request.
            destination: Destination airport code for the request.

        Returns:
            list[dict[str, Any]]: Handle synthetic segments.
        """
        return [
            {
                "from": source,
                "to": destination,
                "from_name": source,
                "to_name": destination,
                "depart_local": None,
                "arrive_local": None,
                "carrier": "SKY",
                "carrier_name": "Skyscanner",
            }
        ]

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

        Returns:
            dict[str, int]: Calendar prices for the requested market.
        """
        return {}

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

        Returns:
            dict[str, Any] | None: The best one-way itinerary for the requested market.
        """
        source_code = source.upper()
        destination_code = destination.upper()
        url = self._search_page_url(
            source=source_code,
            destination=destination_code,
            outbound_iso=departure_iso,
            inbound_iso=None,
            currency=currency,
            adults=adults,
        )
        html, final_url = self._fetch_search_html(url)
        offer_options = self._extract_offer_options(html)
        if offer_options:
            best_offer = offer_options[0]
            price = int(best_offer.get("price") or 0) or None
        else:
            best_offer = None
            price = self._extract_best_price(html)
        if price is None:
            raise ProviderNoResultError("Skyscanner returned no parsable fares.")
        stops = self._extract_stops_hint(html, max_stops_per_leg)
        if stops > max_stops_per_leg:
            raise ProviderNoResultError("Skyscanner offers exceed stops cap.")
        booking_url = str((best_offer or {}).get("booking_url") or "").strip() or final_url
        booking_provider = str((best_offer or {}).get("provider") or "").strip() or "Skyscanner"
        return {
            "price": int(price),
            "formatted_price": f"{int(price)} {currency}",
            "currency": currency,
            "duration_seconds": None,
            "stops": int(stops),
            "transfer_events": int(stops),
            "booking_url": booking_url,
            "segments": self._synthetic_segments(source_code, destination_code),
            "provider": self.provider_id,
            "booking_provider": booking_provider,
            "offer_options": offer_options,
        }

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

        Returns:
            dict[str, Any] | None: The best round-trip itinerary for the requested market.
        """
        source_code = source.upper()
        destination_code = destination.upper()
        url = self._search_page_url(
            source=source_code,
            destination=destination_code,
            outbound_iso=outbound_iso,
            inbound_iso=inbound_iso,
            currency=currency,
            adults=adults,
        )
        html, final_url = self._fetch_search_html(url)
        offer_options = self._extract_offer_options(html)
        if offer_options:
            best_offer = offer_options[0]
            total_price = int(best_offer.get("price") or 0) or None
        else:
            best_offer = None
            total_price = self._extract_best_price(html)
        if total_price is None:
            raise ProviderNoResultError("Skyscanner returned no parsable fares.")
        stops_hint = self._extract_stops_hint(html, max_stops_per_leg)
        if stops_hint > max_stops_per_leg:
            raise ProviderNoResultError("Skyscanner offers exceed stops cap.")
        outbound_segments = self._synthetic_segments(source_code, destination_code)
        inbound_segments = self._synthetic_segments(destination_code, source_code)
        booking_url = str((best_offer or {}).get("booking_url") or "").strip() or final_url
        booking_provider = str((best_offer or {}).get("provider") or "").strip() or "Skyscanner"
        return {
            "price": int(total_price),
            "formatted_price": f"{int(total_price)} {currency}",
            "currency": currency,
            "duration_seconds": None,
            "outbound_duration_seconds": None,
            "inbound_duration_seconds": None,
            "outbound_stops": int(stops_hint),
            "inbound_stops": int(stops_hint),
            "outbound_transfer_events": int(stops_hint),
            "inbound_transfer_events": int(stops_hint),
            "booking_url": booking_url,
            "outbound_segments": outbound_segments,
            "inbound_segments": inbound_segments,
            "provider": self.provider_id,
            "booking_provider": booking_provider,
            "offer_options": offer_options,
        }


atexit.register(SkyscannerScrapeClient._close_playwright_runtime)
