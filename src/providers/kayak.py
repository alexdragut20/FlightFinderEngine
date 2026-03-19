from __future__ import annotations

import contextlib
import json
import re
import threading
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import requests

from ..config import (
    ALLOW_PLAYWRIGHT_PROVIDERS,
    KAYAK_PLAYWRIGHT_ASSIST_TIMEOUT_SECONDS,
    KAYAK_PLAYWRIGHT_BROWSER_CHANNEL,
    KAYAK_PLAYWRIGHT_PROFILE_ROOT,
    KAYAK_SCRAPE_HOST,
    KAYAK_SCRAPE_PLAYWRIGHT_ASSISTED,
    KAYAK_SCRAPE_POLL_ROUNDS,
    KAYAK_SCRAPE_SCHEME,
    MOMONDO_SCRAPE_HOST,
)
from ..exceptions import ProviderBlockedError, ProviderNoResultError
from ..utils import (
    absolute_kayak_url,
    convert_currency_amount,
    max_segment_layover_seconds,
    parse_local_datetime,
    parse_money_amount_int,
    transfer_events_from_segments,
)
from ..utils.constants import PRICE_SENTINEL
from ..utils.logging import capture_provider_response as _capture_provider_response
from ._cache import per_instance_lru_cache


class KayakScrapeClient:
    """Provider client for Kayak scrape-based flight lookups."""

    provider_id = "kayak"
    display_name = "Kayak Scrape"
    supports_calendar = False
    requires_credentials = False
    credential_env: tuple[str, ...] = ()
    docs_url = "https://www.kayak.com/flights/"
    serialized_requests = True
    request_interval_seconds = 2.5
    _NO_RESULT_CODES = {"NO_RESULTS", "NO_RESULTS_FOUND", "NO_RESULTS_AVAILABLE"}
    _BLOCK_MARKERS = (
        "captcha",
        "captcha-v2",
        "px-captcha",
        "perimeterx",
        "verify you are human",
        "verify you are a human",
        "security challenge",
        "security check",
        "access denied",
        "attention required",
        "are you a robot",
        "automated access",
        "just a moment",
        "unusual traffic",
        "bot protection",
        "cf-chl",
        "what is a bot",
        "only useful for humans",
    )
    _BLOCK_URL_MARKERS = (
        "/captcha",
        "captcha-v2",
        "px-captcha",
        "/challenge",
        "cf-chl",
        "/help/bots",
    )
    _BROWSER_ASSIST_LOCK = threading.RLock()
    _BROWSER_ASSIST_COOKIES: dict[tuple[str, str], list[dict[str, Any]]] = {}
    _PLAYWRIGHT_FETCH_GATE = threading.BoundedSemaphore(2)

    def __init__(
        self,
        host: str | None = None,
        poll_rounds: int | None = None,
        playwright_assisted: bool | None = None,
        playwright_assist_timeout_seconds: int | None = None,
        playwright_profile_root: str | None = None,
        playwright_browser_channel: str | None = None,
    ) -> None:
        """Initialize the KayakScrapeClient.

        Args:
            host: Host name for the request.
            poll_rounds: Number of polling rounds to execute.
        """
        normalized_host = str(host or KAYAK_SCRAPE_HOST).strip().lower()
        if not normalized_host:
            normalized_host = KAYAK_SCRAPE_HOST
        self._host = normalized_host
        self._poll_rounds = max(
            1,
            min(
                6,
                int(poll_rounds) if poll_rounds is not None else KAYAK_SCRAPE_POLL_ROUNDS,
            ),
        )
        self._playwright_assisted = (
            bool(playwright_assisted)
            if playwright_assisted is not None
            else KAYAK_SCRAPE_PLAYWRIGHT_ASSISTED
        )
        self._playwright_assist_timeout_seconds = max(
            15,
            int(
                playwright_assist_timeout_seconds
                if playwright_assist_timeout_seconds is not None
                else KAYAK_PLAYWRIGHT_ASSIST_TIMEOUT_SECONDS
            ),
        )
        self._playwright_profile_root = (
            str(playwright_profile_root or KAYAK_PLAYWRIGHT_PROFILE_ROOT).strip()
            or KAYAK_PLAYWRIGHT_PROFILE_ROOT
        )
        self._playwright_browser_channel = (
            str(playwright_browser_channel or KAYAK_PLAYWRIGHT_BROWSER_CHANNEL or "").strip()
            or None
        )
        self._local = threading.local()

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
                    "User-Agent": (
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/122.0.0.0 Safari/537.36"
                    ),
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Cache-Control": "no-cache",
                    "Pragma": "no-cache",
                    "Upgrade-Insecure-Requests": "1",
                }
            )
            self._local.session = session
        session = self._local.session
        self._apply_shared_browser_cookies(session)
        return session

    def _browser_cookie_key(self) -> tuple[str, str]:
        """Return the shared cookie-store key for the provider host."""
        return (self.provider_id, self._host)

    def _apply_shared_browser_cookies(self, session: requests.Session) -> None:
        """Apply any persisted browser-verified cookies to the requests session."""
        with self._BROWSER_ASSIST_LOCK:
            cookies = [
                dict(cookie)
                for cookie in self._BROWSER_ASSIST_COOKIES.get(self._browser_cookie_key(), [])
            ]
        for cookie in cookies:
            name = str(cookie.get("name") or "").strip()
            value = str(cookie.get("value") or "")
            if not name:
                continue
            session.cookies.set(
                name,
                value,
                domain=str(cookie.get("domain") or "").strip() or None,
                path=str(cookie.get("path") or "/").strip() or "/",
            )

    def _store_shared_browser_cookies(self, cookies: list[dict[str, Any]]) -> None:
        """Persist browser-verified cookies for reuse across search requests."""
        normalized_cookies = [
            dict(cookie) for cookie in cookies if str(cookie.get("name") or "").strip()
        ]
        if not normalized_cookies:
            return
        with self._BROWSER_ASSIST_LOCK:
            self._BROWSER_ASSIST_COOKIES[self._browser_cookie_key()] = normalized_cookies

    @staticmethod
    def _has_bootstrap_markup(html: str) -> bool:
        """Return whether the HTML contains the bootstrap payload needed for parsing."""
        lowered = str(html or "").lower()
        return "jsondata_r9datastorage" in lowered and "formtoken" in lowered

    def _playwright_profile_dir(self) -> Path:
        """Return the persistent Playwright profile directory for this provider."""
        return (
            Path(self._playwright_profile_root).expanduser()
            / f"{self.provider_id}-{self._host.replace('.', '-')}"
        )

    def _fetch_search_page_playwright_assisted(
        self,
        page_url: str,
    ) -> tuple[str, str]:
        """Open a visible browser session so a human can clear anti-bot checks once."""
        if not (ALLOW_PLAYWRIGHT_PROVIDERS and self._playwright_assisted):
            raise self._blocked_error(manual_search_url=page_url)

        try:
            from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
            from playwright.sync_api import sync_playwright
        except Exception as exc:
            raise ProviderNoResultError(
                f"{self.display_name} browser-assisted mode is unavailable. "
                "Install with `python3 -m pip install playwright` then "
                "`python3 -m playwright install chromium`."
            ) from exc

        profile_dir = self._playwright_profile_dir()
        profile_dir.mkdir(parents=True, exist_ok=True)
        launch_kwargs: dict[str, Any] = {
            "headless": False,
            "locale": "en-US",
            "user_agent": str(self._session().headers.get("User-Agent") or ""),
        }
        if self._playwright_browser_channel:
            launch_kwargs["channel"] = self._playwright_browser_channel

        with self._BROWSER_ASSIST_LOCK:
            with sync_playwright() as playwright:
                context = playwright.chromium.launch_persistent_context(
                    str(profile_dir),
                    **launch_kwargs,
                )
                try:
                    page = context.pages[0] if context.pages else context.new_page()
                    with contextlib.suppress(Exception):
                        page.bring_to_front()
                    try:
                        page.goto(page_url, wait_until="domcontentloaded", timeout=45000)
                    except PlaywrightTimeoutError as exc:
                        raise RuntimeError(
                            f"{self.display_name} browser-assisted navigation timed out."
                        ) from exc

                    deadline = time.time() + self._playwright_assist_timeout_seconds
                    last_html = ""
                    last_url = str(page.url or page_url)
                    while time.time() < deadline:
                        with contextlib.suppress(PlaywrightTimeoutError):
                            page.wait_for_load_state("networkidle", timeout=1500)
                        page.wait_for_timeout(1000)
                        last_url = str(page.url or page_url)
                        last_html = str(page.content() or "")
                        if self._is_blocked_page(last_html, last_url, 200):
                            continue
                        if self._has_bootstrap_markup(last_html):
                            self._store_shared_browser_cookies(context.cookies())
                            self._apply_shared_browser_cookies(self._session())
                            return last_html, last_url

                    if self._is_blocked_page(last_html, last_url, 200):
                        raise ProviderBlockedError(
                            f"{self.display_name} browser-assisted mode needs you to complete "
                            "the visible verification window, then rerun the search.",
                            manual_search_url=page_url,
                        )
                    if last_html.strip():
                        raise RuntimeError(
                            f"{self.display_name} browser-assisted mode loaded HTML without "
                            "Kayak bootstrap data."
                        )
                    raise RuntimeError(
                        f"{self.display_name} browser-assisted mode returned empty HTML."
                    )
                finally:
                    with contextlib.suppress(Exception):
                        context.close()

    def _resolve_poll_payload(
        self,
        payload: dict[str, Any],
        *,
        status_code: int,
        manual_search_url: str | None,
        allow_partial: bool = False,
    ) -> dict[str, Any] | None:
        """Normalize a provider poll payload into a usable search response."""
        detail = ""
        if status_code >= 400:
            detail = self._extract_error_detail(payload) or f"HTTP {status_code}"
            lowered = detail.lower()
            if any(code.lower() in lowered for code in self._NO_RESULT_CODES):
                raise ProviderNoResultError(detail)
            if self._is_blocked_detail(detail, status_code):
                raise self._blocked_error(manual_search_url=manual_search_url, detail=detail)
            raise RuntimeError(detail)

        errors = payload.get("errors")
        if isinstance(errors, list) and errors:
            detail = self._extract_error_detail(payload) or str(errors[0])
            lowered = detail.lower()
            if any(code.lower() in lowered for code in self._NO_RESULT_CODES):
                raise ProviderNoResultError(detail)
            if self._is_blocked_detail(detail, status_code):
                raise self._blocked_error(manual_search_url=manual_search_url, detail=detail)
            raise RuntimeError(detail)

        normalized_status = str(payload.get("status") or "").strip().lower()
        if self._core_results(payload) and normalized_status in {"first-phase", "complete"}:
            return payload
        if allow_partial and self._core_results(payload):
            return payload
        return None

    def _search_payload_playwright_backed(
        self,
        page_url: str,
        *,
        headless: bool = True,
        interactive: bool = False,
    ) -> dict[str, Any]:
        """Load the search page in Playwright and capture poll payloads from the browser."""
        if not ALLOW_PLAYWRIGHT_PROVIDERS:
            raise self._blocked_error(manual_search_url=page_url)

        try:
            from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
            from playwright.sync_api import sync_playwright
        except Exception as exc:
            raise ProviderNoResultError(
                f"{self.display_name} Playwright fallback is unavailable. "
                "Install with `python3 -m pip install playwright` then "
                "`python3 -m playwright install chromium`."
            ) from exc

        acquired = self._PLAYWRIGHT_FETCH_GATE.acquire(timeout=20)
        if not acquired:
            raise ProviderNoResultError(
                f"{self.display_name} Playwright fallback is busy; skipped to protect system resources."
            )

        try:
            with sync_playwright() as playwright:
                profile_dir = self._playwright_profile_dir()
                profile_dir.mkdir(parents=True, exist_ok=True)
                launch_kwargs: dict[str, Any] = {
                    "headless": headless,
                    "locale": "en-US",
                    "user_agent": str(self._session().headers.get("User-Agent") or ""),
                }
                if self._playwright_browser_channel:
                    launch_kwargs["channel"] = self._playwright_browser_channel
                with self._BROWSER_ASSIST_LOCK:
                    context = playwright.chromium.launch_persistent_context(
                        str(profile_dir),
                        **launch_kwargs,
                    )
                    try:
                        poll_payloads: list[tuple[int, dict[str, Any]]] = []
                        last_processed_count = 0
                        last_blocked_error: ProviderBlockedError | None = None
                        page = context.new_page()
                        if not headless:
                            with contextlib.suppress(Exception):
                                page.bring_to_front()

                        def on_response(response: Any) -> None:
                            if "/i/api/search/dynamic/flights/poll" not in str(response.url or ""):
                                return
                            try:
                                body_text = str(response.text() or "")
                            except Exception:
                                return
                            try:
                                payload = json.loads(body_text)
                            except ValueError:
                                return
                            if isinstance(payload, dict):
                                poll_payloads.append((int(response.status or 0), payload))

                        page.on("response", on_response)
                        try:
                            page.goto(page_url, wait_until="domcontentloaded", timeout=45000)
                        except PlaywrightTimeoutError as exc:
                            raise RuntimeError(
                                f"{self.display_name} Playwright navigation timed out."
                            ) from exc

                        deadline = time.time() + self._playwright_assist_timeout_seconds
                        last_html = ""
                        last_url = str(page.url or page_url)
                        while time.time() < deadline:
                            with contextlib.suppress(PlaywrightTimeoutError):
                                page.wait_for_load_state("networkidle", timeout=1500)
                            page.wait_for_timeout(1000)
                            last_url = str(page.url or page_url)
                            last_html = str(page.content() or "")
                            is_blocked_page = self._is_blocked_page(last_html, last_url, 200)
                            new_payloads = poll_payloads[last_processed_count:]
                            last_processed_count = len(poll_payloads)
                            for status_code, payload in new_payloads:
                                try:
                                    resolved = self._resolve_poll_payload(
                                        payload,
                                        status_code=status_code,
                                        manual_search_url=page_url,
                                    )
                                except ProviderBlockedError as exc:
                                    last_blocked_error = exc
                                    if interactive:
                                        continue
                                    raise
                                if resolved is not None:
                                    self._store_shared_browser_cookies(context.cookies())
                                    self._apply_shared_browser_cookies(self._session())
                                    return resolved
                            if is_blocked_page and not interactive:
                                raise last_blocked_error or self._blocked_error(
                                    manual_search_url=page_url
                                )

                        if poll_payloads:
                            status_code, payload = poll_payloads[-1]
                            try:
                                resolved = self._resolve_poll_payload(
                                    payload,
                                    status_code=status_code,
                                    manual_search_url=page_url,
                                    allow_partial=True,
                                )
                            except ProviderBlockedError as exc:
                                last_blocked_error = exc
                            else:
                                if resolved is not None:
                                    self._store_shared_browser_cookies(context.cookies())
                                    self._apply_shared_browser_cookies(self._session())
                                    return resolved
                        if interactive:
                            raise ProviderBlockedError(
                                f"{self.display_name} browser-assisted mode needs you to complete "
                                "the visible verification window, then rerun the search.",
                                manual_search_url=page_url,
                            )
                        if last_blocked_error is not None:
                            raise last_blocked_error
                        if self._is_blocked_page(last_html, last_url, 200):
                            raise self._blocked_error(manual_search_url=page_url)
                        raise RuntimeError(
                            f"{self.display_name} Playwright fallback loaded the page "
                            "but never captured usable poll payloads."
                        )
                    finally:
                        with contextlib.suppress(Exception):
                            context.close()
        finally:
            self._PLAYWRIGHT_FETCH_GATE.release()

    def _search_payload_playwright_assisted(
        self,
        page_url: str,
    ) -> dict[str, Any]:
        """Open a visible persistent browser session and wait for the user to clear verification."""
        return self._search_payload_playwright_backed(
            page_url,
            headless=False,
            interactive=True,
        )

    def _search_path_prefix(self) -> str:
        """Return the path prefix used by the search results page.

        Returns:
            str: Search page path prefix.
        """
        return "/flights"

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
        source_code = str(source or "").strip().upper()
        destination_code = str(destination or "").strip().upper()
        path_prefix = self._search_path_prefix().rstrip("/")
        if inbound_iso:
            path = f"{path_prefix}/{source_code}-{destination_code}/{outbound_iso}/{inbound_iso}"
        else:
            path = f"{path_prefix}/{source_code}-{destination_code}/{outbound_iso}"
        params = {
            "sort": "price_a",
            "adults": max(1, int(adults or 1)),
            "currency": str(currency or "RON").strip().upper() or "RON",
        }
        return f"{KAYAK_SCRAPE_SCHEME}://{self._host}{path}?{urlencode(params)}"

    @staticmethod
    def _safe_json_from_response(response: requests.Response) -> dict[str, Any]:
        """Safely decode a JSON response payload.

        Args:
            response: HTTP response object to inspect.

        Returns:
            dict[str, Any]: Safely decode a JSON response payload.
        """
        try:
            payload = response.json()
        except ValueError:
            return {}
        return payload if isinstance(payload, dict) else {}

    @staticmethod
    def _extract_error_detail(payload: dict[str, Any]) -> str:
        """Extract a human-readable error detail from the provider response.

        Args:
            payload: JSON-serializable payload for the operation.

        Returns:
            str: Extract a human-readable error detail from the provider response.
        """
        errors = payload.get("errors")
        if not isinstance(errors, list) or not errors:
            return ""
        first = errors[0] or {}
        if isinstance(first, dict):
            code = str(first.get("code") or "").strip()
            description = str(first.get("description") or "").strip()
            if code and description:
                return f"{code}: {description}"
            if description:
                return description
            if code:
                return code
        return str(first).strip()

    @classmethod
    def _is_blocked_page(
        cls,
        html: str,
        final_url: str | None = None,
        status_code: int | None = None,
    ) -> bool:
        """Return whether the HTML response indicates anti-bot blocking."""
        if status_code in {403, 429, 451, 503}:
            return True
        lowered_url = str(final_url or "").lower()
        if any(marker in lowered_url for marker in cls._BLOCK_URL_MARKERS):
            return True
        lowered_html = str(html or "").lower()
        return any(marker in lowered_html for marker in cls._BLOCK_MARKERS)

    @classmethod
    def _is_blocked_detail(cls, detail: str, status_code: int | None = None) -> bool:
        """Return whether an error detail looks like bot protection."""
        if status_code in {403, 429, 451, 503}:
            return True
        lowered = str(detail or "").lower()
        return any(marker in lowered for marker in cls._BLOCK_MARKERS)

    def _blocked_error(
        self,
        *,
        manual_search_url: str | None,
        detail: str | None = None,
    ) -> ProviderBlockedError:
        """Build a normalized anti-bot exception for the provider."""
        base_message = (
            f"{self.display_name} blocked automated scraping (captcha/anti-bot challenge)."
        )
        detail_text = str(detail or "").strip()
        if detail_text:
            base_message = f"{base_message} Detail: {detail_text}."
        return ProviderBlockedError(base_message, manual_search_url=manual_search_url)

    def _extract_bootstrap(
        self,
        html: str,
        *,
        manual_search_url: str | None = None,
    ) -> tuple[str, dict[str, Any]]:
        """Extract bootstrap data from the provider page.

        Args:
            html: HTML document to parse.

        Returns:
            tuple[str, dict[str, Any]]: Extract bootstrap data from the provider page.
        """
        match = re.search(
            r'<script[^>]*id="jsonData_R9DataStorage"[^>]*>(.*?)</script>',
            html,
            re.IGNORECASE | re.DOTALL,
        )
        if not match:
            if self._is_blocked_page(html, manual_search_url):
                raise self._blocked_error(manual_search_url=manual_search_url)
            raise RuntimeError("Kayak bootstrap data missing (jsonData_R9DataStorage not found)")
        try:
            bootstrap = json.loads(match.group(1))
        except ValueError as exc:
            raise RuntimeError(f"Kayak bootstrap JSON parse failed: {exc}") from exc

        formtoken = str(
            ((bootstrap.get("serverData") or {}).get("global") or {}).get("formtoken") or ""
        ).strip()
        if not formtoken:
            if self._is_blocked_page(html, manual_search_url):
                raise self._blocked_error(manual_search_url=manual_search_url)
            raise RuntimeError("Kayak formtoken is missing")
        return formtoken, bootstrap

    def _post_poll(
        self,
        referer_url: str,
        csrf_token: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Submit a poll request to the provider backend.

        Args:
            referer_url: URL for referer.
            csrf_token: CSRF token to send with the request.
            payload: JSON-serializable payload for the operation.

        Returns:
            dict[str, Any]: Submit a poll request to the provider backend.
        """
        endpoint = f"{KAYAK_SCRAPE_SCHEME}://{self._host}/i/api/search/dynamic/flights/poll"
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/plain, */*",
            "x-requested-with": "XMLHttpRequest",
            "x-csrf": csrf_token,
            "referer": referer_url,
            "origin": f"{KAYAK_SCRAPE_SCHEME}://{self._host}",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
        }
        response = self._session().post(
            endpoint,
            json=payload,
            headers=headers,
            timeout=45,
        )
        response_text = str(response.text or "")
        body = self._safe_json_from_response(response)
        if response.status_code >= 400:
            detail = self._extract_error_detail(body) or f"HTTP {response.status_code}"
            _capture_provider_response(
                self.provider_id,
                "poll",
                {"endpoint": endpoint, "payload": payload},
                body,
                status_code=response.status_code,
                error=detail,
            )
            lowered = detail.lower()
            if any(code.lower() in lowered for code in self._NO_RESULT_CODES):
                raise ProviderNoResultError(detail)
            if self._is_blocked_page(
                response_text, referer_url, response.status_code
            ) or self._is_blocked_detail(
                detail,
                response.status_code,
            ):
                raise self._blocked_error(manual_search_url=referer_url, detail=detail)
            raise RuntimeError(detail)

        errors = body.get("errors")
        if isinstance(errors, list) and errors:
            detail = self._extract_error_detail(body) or str(errors[0])
            _capture_provider_response(
                self.provider_id,
                "poll",
                {"endpoint": endpoint, "payload": payload},
                body,
                status_code=response.status_code,
                error=detail,
            )
            lowered = detail.lower()
            if any(code.lower() in lowered for code in self._NO_RESULT_CODES):
                raise ProviderNoResultError(detail)
            if self._is_blocked_page(
                response_text, referer_url, response.status_code
            ) or self._is_blocked_detail(
                detail,
                response.status_code,
            ):
                raise self._blocked_error(manual_search_url=referer_url, detail=detail)
            raise RuntimeError(detail)
        _capture_provider_response(
            self.provider_id,
            "poll",
            {"endpoint": endpoint, "payload": payload},
            body,
            status_code=response.status_code,
        )
        return body

    @staticmethod
    def _build_legs_payload(
        source: str,
        destination: str,
        outbound_iso: str,
        inbound_iso: str | None,
    ) -> list[dict[str, Any]]:
        """Build the provider legs payload.

        Args:
            source: Origin airport code for the request.
            destination: Destination airport code for the request.
            outbound_iso: Outbound travel date in ISO 8601 format.
            inbound_iso: Inbound travel date in ISO 8601 format.

        Returns:
            list[dict[str, Any]]: The provider legs payload.
        """
        source_code = str(source or "").strip().upper()
        destination_code = str(destination or "").strip().upper()
        legs = [
            {
                "origin": {"airports": [source_code], "locationType": "airports"},
                "destination": {"airports": [destination_code], "locationType": "airports"},
                "date": outbound_iso,
                "flex": "exact",
            }
        ]
        if inbound_iso:
            legs.append(
                {
                    "origin": {"airports": [destination_code], "locationType": "airports"},
                    "destination": {"airports": [source_code], "locationType": "airports"},
                    "date": inbound_iso,
                    "flex": "exact",
                }
            )
        return legs

    def _search_payload(
        self,
        source: str,
        destination: str,
        outbound_iso: str,
        inbound_iso: str | None,
        currency: str,
        adults: int,
    ) -> dict[str, Any]:
        """Build the provider search payload.

        Args:
            source: Origin airport code for the request.
            destination: Destination airport code for the request.
            outbound_iso: Outbound travel date in ISO 8601 format.
            inbound_iso: Inbound travel date in ISO 8601 format.
            currency: Currency code for pricing output.
            adults: Number of adult travelers.

        Returns:
            dict[str, Any]: The provider search payload.
        """
        page_url = self._search_page_url(
            source=source,
            destination=destination,
            outbound_iso=outbound_iso,
            inbound_iso=inbound_iso,
            currency=currency,
            adults=adults,
        )
        page_response = self._session().get(
            page_url,
            timeout=45,
        )
        page_html = str(page_response.text or "")
        final_page_url = str(page_response.url or page_url)
        if self._is_blocked_page(page_html, final_page_url, page_response.status_code):
            detail = (
                f"HTTP {page_response.status_code}" if page_response.status_code >= 400 else None
            )
            if ALLOW_PLAYWRIGHT_PROVIDERS and self._playwright_browser_channel:
                try:
                    return self._search_payload_playwright_backed(page_url)
                except ProviderBlockedError:
                    if self._playwright_assisted:
                        return self._search_payload_playwright_assisted(page_url)
                    else:
                        raise
            elif ALLOW_PLAYWRIGHT_PROVIDERS and self._playwright_assisted:
                page_html, final_page_url = self._fetch_search_page_playwright_assisted(page_url)
            else:
                raise self._blocked_error(manual_search_url=final_page_url, detail=detail)
        else:
            page_response.raise_for_status()
            if (
                ALLOW_PLAYWRIGHT_PROVIDERS
                and self._playwright_assisted
                and not self._has_bootstrap_markup(page_html)
            ):
                if self._playwright_browser_channel:
                    try:
                        return self._search_payload_playwright_backed(page_url)
                    except ProviderBlockedError:
                        return self._search_payload_playwright_assisted(page_url)
                else:
                    page_html, final_page_url = self._fetch_search_page_playwright_assisted(
                        page_url
                    )

        csrf_token, _bootstrap = self._extract_bootstrap(
            page_html, manual_search_url=final_page_url
        )

        passenger_count = max(1, int(adults or 1))
        passengers = ["ADT"] * passenger_count
        passenger_details = [{"ptc": "ADT"} for _ in range(passenger_count)]
        search_payload = {
            "filterParams": {},
            "userSearchParams": {
                "legs": self._build_legs_payload(source, destination, outbound_iso, inbound_iso),
                "passengers": passengers,
                "passengerDetails": passenger_details,
                "sortMode": "price_a",
            },
            "searchMetaData": {
                "pageNumber": 1,
                "searchTypes": [],
            },
        }

        try:
            latest = self._post_poll(
                referer_url=final_page_url,
                csrf_token=csrf_token,
                payload=search_payload,
            )
        except ProviderBlockedError:
            if ALLOW_PLAYWRIGHT_PROVIDERS and self._playwright_browser_channel:
                try:
                    return self._search_payload_playwright_backed(page_url)
                except ProviderBlockedError:
                    if self._playwright_assisted:
                        return self._search_payload_playwright_assisted(page_url)
                    raise
            raise
        search_id = str(latest.get("searchId") or "").strip()
        for _ in range(self._poll_rounds):
            core_results = self._core_results(latest)
            status = str(latest.get("status") or "").strip().lower()
            if core_results and status in {"first-phase", "complete"}:
                break
            if not search_id:
                break
            search_payload["userSearchParams"]["searchId"] = search_id
            search_payload["searchMetaData"]["skipResultsInSecondPhase"] = False
            time.sleep(0.6)
            try:
                latest = self._post_poll(
                    referer_url=final_page_url,
                    csrf_token=csrf_token,
                    payload=search_payload,
                )
            except ProviderBlockedError:
                if ALLOW_PLAYWRIGHT_PROVIDERS and self._playwright_browser_channel:
                    try:
                        return self._search_payload_playwright_backed(page_url)
                    except ProviderBlockedError:
                        if self._playwright_assisted:
                            return self._search_payload_playwright_assisted(page_url)
                        raise
                raise
            search_id = str(latest.get("searchId") or search_id).strip()
        return latest

    @staticmethod
    def _core_results(payload: dict[str, Any]) -> list[dict[str, Any]]:
        """Extract the core result payload from the provider response.

        Args:
            payload: JSON-serializable payload for the operation.

        Returns:
            list[dict[str, Any]]: Extract the core result payload from the provider response.
        """
        out: list[dict[str, Any]] = []
        for item in payload.get("results") or []:
            if not isinstance(item, dict):
                continue
            if str(item.get("type") or "").strip().lower() != "core":
                continue
            out.append(item)
        return out

    @staticmethod
    def _first_money_value(values: list[Any]) -> int | None:
        """Extract the first monetary value from the payload.

        Args:
            values: Input values for the operation.

        Returns:
            int | None: Extract the first monetary value from the payload.
        """
        for value in values:
            amount = parse_money_amount_int(value)
            if amount is not None:
                return amount
        return None

    @classmethod
    def _booking_explicit_total_amount(cls, booking: dict[str, Any]) -> int | None:
        """Extract an explicit total booking amount when available.

        Args:
            booking: Mapping of booking.

        Returns:
            int | None: Extract an explicit total booking amount when available.
        """
        display_price = booking.get("displayPrice") or {}
        price_obj = booking.get("price") or {}
        pricing_obj = booking.get("pricing") or {}
        return cls._first_money_value(
            [
                display_price.get("totalPrice"),
                display_price.get("totalLocalizedPrice"),
                display_price.get("localizedTotalPrice"),
                display_price.get("priceTotal"),
                display_price.get("total"),
                display_price.get("allInTotalPrice"),
                display_price.get("allInPrice"),
                booking.get("totalPrice"),
                booking.get("total"),
                booking.get("grandTotal"),
                booking.get("priceTotal"),
                price_obj.get("total"),
                price_obj.get("grandTotal"),
                pricing_obj.get("total"),
                pricing_obj.get("grandTotal"),
            ]
        )

    @classmethod
    def _booking_price_per_person_flag(cls, booking: dict[str, Any]) -> bool | None:
        """Return whether the booking price is per passenger.

        Args:
            booking: Mapping of booking.

        Returns:
            bool | None: True when the booking price is per passenger; otherwise, False.
        """
        stack: list[Any] = [booking]
        visited = 0
        max_nodes = 500
        while stack and visited < max_nodes:
            node = stack.pop()
            visited += 1
            if isinstance(node, dict):
                for key, value in node.items():
                    key_l = str(key or "").strip().lower()
                    if not key_l:
                        continue

                    if any(
                        marker in key_l
                        for marker in (
                            "perperson",
                            "per_person",
                            "per-passenger",
                            "per_passenger",
                            "pertraveler",
                            "per_traveler",
                        )
                    ):
                        if isinstance(value, bool):
                            return value
                        if isinstance(value, int | float):
                            return bool(value)
                        value_l = str(value or "").strip().lower()
                        if "total" in value_l:
                            return False
                        if any(
                            marker in value_l
                            for marker in ("person", "passenger", "traveler", "pp")
                        ):
                            return True

                    if key_l in {"pricemode", "price_mode", "pricetype", "price_type"}:
                        value_l = str(value or "").strip().lower()
                        if "total" in value_l:
                            return False
                        if any(
                            marker in value_l
                            for marker in ("person", "passenger", "traveler", "pp")
                        ):
                            return True

                    if isinstance(value, dict | list | tuple):
                        stack.append(value)
            elif isinstance(node, list | tuple):
                for value in node:
                    if isinstance(value, dict | list | tuple):
                        stack.append(value)
        return None

    @classmethod
    def _booking_option_amount(
        cls,
        booking: dict[str, Any],
        adults: int,
    ) -> tuple[int | None, str | None, str]:
        """Extract the numeric amount for a booking option.

        Args:
            booking: Mapping of booking.
            adults: Number of adult travelers.

        Returns:
            tuple[int | None, str | None, str]: Extract the numeric amount for a booking option.
        """
        display_price = booking.get("displayPrice") or {}
        source_currency = str(display_price.get("currency") or "").strip().upper() or None
        displayed_amount = cls._first_money_value(
            [
                display_price.get("price"),
                display_price.get("localizedPrice"),
            ]
        )
        explicit_total_amount = cls._booking_explicit_total_amount(booking)
        if explicit_total_amount is not None:
            return explicit_total_amount, source_currency, "explicit_total"

        if displayed_amount is None:
            return None, source_currency, "missing_price"

        requested_adults = max(1, int(adults or 1))
        per_person_flag = cls._booking_price_per_person_flag(booking)
        if requested_adults > 1 and per_person_flag is not False:
            # Kayak/Momondo often expose per-traveler display prices.
            return displayed_amount * requested_adults, source_currency, "per_person_scaled"
        return displayed_amount, source_currency, "displayed"

    @classmethod
    def _best_booking_option(
        cls,
        result: dict[str, Any],
        adults: int,
    ) -> tuple[dict[str, Any] | None, int | None, str | None, str]:
        """Select the best booking option from the provider results.

        Args:
            result: Result record for the current operation.
            adults: Number of adult travelers.

        Returns:
            tuple[dict[str, Any] | None, int | None, str | None, str]: Selected best booking option from the provider results.
        """
        best: dict[str, Any] | None = None
        best_amount: int | None = None
        best_currency: str | None = None
        best_assumption = "missing_price"
        for option in result.get("bookingOptions") or []:
            if not isinstance(option, dict):
                continue
            amount, source_currency, assumption = cls._booking_option_amount(option, adults)
            if amount is None:
                continue
            if best is None or best_amount is None or amount < best_amount:
                best = option
                best_amount = amount
                best_currency = source_currency
                best_assumption = assumption
        return best, best_amount, best_currency, best_assumption

    @staticmethod
    def _segment_ids_for_leg(
        leg_ref: dict[str, Any],
        legs_map: dict[str, Any],
    ) -> list[str]:
        """Extract segment identifiers for a leg payload.

        Args:
            leg_ref: Mapping of leg ref.
            legs_map: Mapping of legs.

        Returns:
            list[str]: Extract segment identifiers for a leg payload.
        """
        segment_ids: list[str] = []
        for segment_ref in leg_ref.get("segments") or []:
            if isinstance(segment_ref, dict):
                segment_id = str(segment_ref.get("id") or "").strip()
            else:
                segment_id = str(segment_ref or "").strip()
            if segment_id:
                segment_ids.append(segment_id)
        if segment_ids:
            return segment_ids

        leg_id = str(leg_ref.get("id") or "").strip()
        if not leg_id:
            return []
        mapped_leg = legs_map.get(leg_id) or {}
        for segment_ref in mapped_leg.get("segments") or []:
            if isinstance(segment_ref, dict):
                segment_id = str(segment_ref.get("id") or "").strip()
            else:
                segment_id = str(segment_ref or "").strip()
            if segment_id:
                segment_ids.append(segment_id)
        return segment_ids

    @staticmethod
    def _segment_entry(
        segment_id: str,
        segments_map: dict[str, Any],
        airports_map: dict[str, Any],
        airlines_map: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Build a normalized segment entry.

        Args:
            segment_id: Identifier for segment.
            segments_map: Mapping of segments.
            airports_map: Mapping of airports.
            airlines_map: Mapping of airlines.

        Returns:
            dict[str, Any] | None: A normalized segment entry.
        """
        raw = segments_map.get(segment_id) or {}
        if not isinstance(raw, dict):
            return None
        source_code = str(raw.get("origin") or "").strip().upper()
        destination_code = str(raw.get("destination") or "").strip().upper()
        if not source_code or not destination_code:
            return None
        source_meta = airports_map.get(source_code) or {}
        destination_meta = airports_map.get(destination_code) or {}
        carrier_code = str(raw.get("airline") or "").strip().upper()
        carrier_meta = airlines_map.get(carrier_code) or {}
        carrier_name = (
            str(carrier_meta.get("name") or "").strip()
            or str(raw.get("operationalDisplay") or "").strip()
            or carrier_code
        )
        return {
            "from": source_code,
            "to": destination_code,
            "from_name": str(
                source_meta.get("displayName") or source_meta.get("fullDisplayName") or source_code
            ),
            "to_name": str(
                destination_meta.get("displayName")
                or destination_meta.get("fullDisplayName")
                or destination_code
            ),
            "depart_local": str(raw.get("departure") or "").strip() or None,
            "arrive_local": str(raw.get("arrival") or "").strip() or None,
            "carrier": carrier_code or carrier_name,
            "carrier_name": carrier_name,
        }

    def _segments_for_leg(
        self,
        leg_ref: dict[str, Any],
        legs_map: dict[str, Any],
        segments_map: dict[str, Any],
        airports_map: dict[str, Any],
        airlines_map: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Build normalized segments for a leg payload.

        Args:
            leg_ref: Mapping of leg ref.
            legs_map: Mapping of legs.
            segments_map: Mapping of segments.
            airports_map: Mapping of airports.
            airlines_map: Mapping of airlines.

        Returns:
            list[dict[str, Any]]: Normalized segments for a leg payload.
        """
        parsed: list[dict[str, Any]] = []
        for segment_id in self._segment_ids_for_leg(leg_ref, legs_map):
            segment = self._segment_entry(
                segment_id=segment_id,
                segments_map=segments_map,
                airports_map=airports_map,
                airlines_map=airlines_map,
            )
            if segment:
                parsed.append(segment)
        return parsed

    @staticmethod
    def _leg_duration_seconds(
        leg_ref: dict[str, Any],
        legs_map: dict[str, Any],
        segments: list[dict[str, Any]],
    ) -> int | None:
        """Handle leg duration seconds.

        Args:
            leg_ref: Mapping of leg ref.
            legs_map: Mapping of legs.
            segments: Mapping of segments.

        Returns:
            int | None: Handle leg duration seconds.
        """
        leg_id = str(leg_ref.get("id") or "").strip()
        mapped_leg = legs_map.get(leg_id) or {}
        duration_minutes = mapped_leg.get("duration")
        if isinstance(duration_minutes, int | float):
            return int(duration_minutes) * 60
        if segments:
            start = parse_local_datetime(segments[0].get("depart_local"))
            end = parse_local_datetime(segments[-1].get("arrive_local"))
            if start and end and end >= start:
                return int((end - start).total_seconds())
        return None

    def _normalize_price(
        self,
        amount: int | None,
        source_currency: str,
        target_currency: str,
    ) -> tuple[int | None, str]:
        """Normalize price.

        Args:
            amount: Numeric amount to convert or format.
            source_currency: Source currency code for conversion.
            target_currency: Target currency code for conversion.

        Returns:
            tuple[int | None, str]: Normalized price.
        """
        source = str(source_currency or "").strip().upper()
        target = str(target_currency or "").strip().upper()
        if amount is None:
            return None, target or source
        if not source:
            source = target
        if not target:
            target = source
        if source == target:
            return amount, target
        converted = convert_currency_amount(amount, source, target)
        if converted is not None:
            return converted, target
        return amount, source

    @staticmethod
    def _candidate_sort_key(candidate: dict[str, Any]) -> tuple[int, int, int]:
        """Build the sort key for candidate ranking.

        Args:
            candidate: Mapping of candidate.

        Returns:
            tuple[int, int, int]: The sort key for candidate ranking.
        """
        return (
            int(candidate.get("price") or PRICE_SENTINEL),
            int(candidate.get("stops") or candidate.get("outbound_stops") or 0)
            + int(candidate.get("inbound_stops") or 0),
            int(candidate.get("duration_seconds") or PRICE_SENTINEL),
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
        payload = self._search_payload(
            source=source,
            destination=destination,
            outbound_iso=departure_iso,
            inbound_iso=None,
            currency=currency,
            adults=adults,
        )
        core_results = self._core_results(payload)
        if not core_results:
            return None
        legs_map = payload.get("legs") or {}
        segments_map = payload.get("segments") or {}
        airports_map = payload.get("airports") or {}
        airlines_map = payload.get("airlines") or {}
        providers_map = payload.get("providers") or {}

        best: dict[str, Any] | None = None
        target_currency = str(currency or "RON").upper()
        for result in core_results:
            booking, booking_amount, booking_currency, booking_price_mode = (
                self._best_booking_option(
                    result,
                    adults=adults,
                )
            )
            if not booking:
                continue
            source_currency = str(booking_currency or target_currency).upper()
            normalized_price, normalized_currency = self._normalize_price(
                amount=booking_amount,
                source_currency=source_currency,
                target_currency=target_currency,
            )
            if normalized_price is None:
                continue

            leg_refs = result.get("legs") or []
            if not leg_refs or not isinstance(leg_refs[0], dict):
                continue
            outbound_segments = self._segments_for_leg(
                leg_ref=leg_refs[0],
                legs_map=legs_map,
                segments_map=segments_map,
                airports_map=airports_map,
                airlines_map=airlines_map,
            )
            if not outbound_segments:
                continue
            stops = max(0, len(outbound_segments) - 1)
            if stops > max_stops_per_leg:
                continue
            if max_connection_layover_seconds is not None:
                max_layover = max_segment_layover_seconds(outbound_segments)
                if max_layover is not None and max_layover > max_connection_layover_seconds:
                    continue

            duration_seconds = self._leg_duration_seconds(
                leg_ref=leg_refs[0],
                legs_map=legs_map,
                segments=outbound_segments,
            )
            booking_provider = str(booking.get("providerCode") or "").strip().upper()
            booking_provider_name = str(
                (providers_map.get(booking_provider) or {}).get("displayName") or ""
            ).strip()
            booking_url = absolute_kayak_url(
                ((booking.get("bookingUrl") or {}).get("url")),
                host=self._host,
            )
            if not booking_url:
                booking_url = absolute_kayak_url(result.get("shareableUrl"), host=self._host)
            candidate = {
                "price": int(normalized_price),
                "formatted_price": f"{int(normalized_price)} {normalized_currency}",
                "currency": normalized_currency,
                "duration_seconds": duration_seconds,
                "stops": stops,
                "transfer_events": transfer_events_from_segments(outbound_segments),
                "booking_url": booking_url,
                "segments": outbound_segments,
                "provider": self.provider_id,
                "booking_provider": booking_provider_name or booking_provider or self.display_name,
                "price_mode": booking_price_mode,
            }
            if best is None or self._candidate_sort_key(candidate) < self._candidate_sort_key(best):
                best = candidate
        return best

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
        payload = self._search_payload(
            source=source,
            destination=destination,
            outbound_iso=outbound_iso,
            inbound_iso=inbound_iso,
            currency=currency,
            adults=adults,
        )
        core_results = self._core_results(payload)
        if not core_results:
            return None
        legs_map = payload.get("legs") or {}
        segments_map = payload.get("segments") or {}
        airports_map = payload.get("airports") or {}
        airlines_map = payload.get("airlines") or {}
        providers_map = payload.get("providers") or {}

        best: dict[str, Any] | None = None
        target_currency = str(currency or "RON").upper()
        for result in core_results:
            leg_refs = result.get("legs") or []
            if len(leg_refs) < 2:
                continue
            if not isinstance(leg_refs[0], dict) or not isinstance(leg_refs[1], dict):
                continue

            booking, booking_amount, booking_currency, booking_price_mode = (
                self._best_booking_option(
                    result,
                    adults=adults,
                )
            )
            if not booking:
                continue
            source_currency = str(booking_currency or target_currency).upper()
            normalized_price, normalized_currency = self._normalize_price(
                amount=booking_amount,
                source_currency=source_currency,
                target_currency=target_currency,
            )
            if normalized_price is None:
                continue

            outbound_segments = self._segments_for_leg(
                leg_ref=leg_refs[0],
                legs_map=legs_map,
                segments_map=segments_map,
                airports_map=airports_map,
                airlines_map=airlines_map,
            )
            inbound_segments = self._segments_for_leg(
                leg_ref=leg_refs[1],
                legs_map=legs_map,
                segments_map=segments_map,
                airports_map=airports_map,
                airlines_map=airlines_map,
            )
            if not outbound_segments or not inbound_segments:
                continue
            outbound_stops = max(0, len(outbound_segments) - 1)
            inbound_stops = max(0, len(inbound_segments) - 1)
            if outbound_stops > max_stops_per_leg or inbound_stops > max_stops_per_leg:
                continue
            if max_connection_layover_seconds is not None:
                outbound_max_layover = max_segment_layover_seconds(outbound_segments)
                inbound_max_layover = max_segment_layover_seconds(inbound_segments)
                if (
                    outbound_max_layover is not None
                    and outbound_max_layover > max_connection_layover_seconds
                ) or (
                    inbound_max_layover is not None
                    and inbound_max_layover > max_connection_layover_seconds
                ):
                    continue

            outbound_duration_seconds = self._leg_duration_seconds(
                leg_ref=leg_refs[0],
                legs_map=legs_map,
                segments=outbound_segments,
            )
            inbound_duration_seconds = self._leg_duration_seconds(
                leg_ref=leg_refs[1],
                legs_map=legs_map,
                segments=inbound_segments,
            )
            total_duration_seconds = (
                (outbound_duration_seconds or 0) + (inbound_duration_seconds or 0)
                if outbound_duration_seconds is not None and inbound_duration_seconds is not None
                else None
            )

            booking_provider = str(booking.get("providerCode") or "").strip().upper()
            booking_provider_name = str(
                (providers_map.get(booking_provider) or {}).get("displayName") or ""
            ).strip()
            booking_url = absolute_kayak_url(
                ((booking.get("bookingUrl") or {}).get("url")),
                host=self._host,
            )
            if not booking_url:
                booking_url = absolute_kayak_url(result.get("shareableUrl"), host=self._host)
            candidate = {
                "price": int(normalized_price),
                "formatted_price": f"{int(normalized_price)} {normalized_currency}",
                "currency": normalized_currency,
                "duration_seconds": total_duration_seconds,
                "outbound_duration_seconds": outbound_duration_seconds,
                "inbound_duration_seconds": inbound_duration_seconds,
                "outbound_stops": outbound_stops,
                "inbound_stops": inbound_stops,
                "outbound_transfer_events": transfer_events_from_segments(outbound_segments),
                "inbound_transfer_events": transfer_events_from_segments(inbound_segments),
                "booking_url": booking_url,
                "outbound_segments": outbound_segments,
                "inbound_segments": inbound_segments,
                "provider": self.provider_id,
                "booking_provider": booking_provider_name or booking_provider or self.display_name,
                "price_mode": booking_price_mode,
            }
            if best is None or self._candidate_sort_key(candidate) < self._candidate_sort_key(best):
                best = candidate
        return best


class MomondoScrapeClient(KayakScrapeClient):
    """Represent MomondoScrapeClient."""

    provider_id = "momondo"
    display_name = "Momondo Scrape"
    docs_url = "https://www.momondo.com/flight-search/"

    def __init__(
        self,
        host: str | None = None,
        poll_rounds: int | None = None,
        playwright_assisted: bool | None = None,
        playwright_assist_timeout_seconds: int | None = None,
        playwright_profile_root: str | None = None,
        playwright_browser_channel: str | None = None,
    ) -> None:
        """Initialize the MomondoScrapeClient.

        Args:
            host: Host name for the request.
            poll_rounds: Number of polling rounds to execute.
        """
        super().__init__(
            host=host if host is not None else MOMONDO_SCRAPE_HOST,
            poll_rounds=poll_rounds,
            playwright_assisted=playwright_assisted,
            playwright_assist_timeout_seconds=playwright_assist_timeout_seconds,
            playwright_profile_root=playwright_profile_root,
            playwright_browser_channel=playwright_browser_channel,
        )

    def _search_path_prefix(self) -> str:
        """Return the Momondo path prefix used by the search results page.

        Returns:
            str: Search page path prefix.
        """
        return "/flight-search"
