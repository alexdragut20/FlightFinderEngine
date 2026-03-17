from __future__ import annotations

import json
import logging
import os
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlsplit

import requests

from ..config import (
    ALLOW_PLAYWRIGHT_PROVIDERS,
    AUTO_HUB_CANDIDATES,
    DEFAULT_IO_WORKERS,
    DEFAULT_SEARCH_TIMEOUT_SECONDS,
    DESTINATION_NOTES,
    SKYSCANNER_SCRAPE_PLAYWRIGHT_FALLBACK,
)
from ..data.airports import AirportCoordinates
from ..data.resources import STATIC_DIR
from ..engine import SplitTripOptimizer
from ..providers import KiwiClient
from ..utils.constants import (
    API_PROVIDER_CONFIG_PATH,
    API_SEARCH_JOBS_PATH,
    API_SEARCH_PATH,
    DEFAULT_SERVER_HOST,
    DEFAULT_SERVER_PORT,
    ERROR_INVALID_JSON_BODY,
    ERROR_UNKNOWN_ENDPOINT,
    ERROR_UNKNOWN_SEARCH_JOB,
    HTTP_CACHE_CONTROL_NO_STORE,
    HTTP_EXPIRES_IMMEDIATELY,
    HTTP_PRAGMA_NO_CACHE,
    ROOT_INDEX_PATH,
    SERVER_READY_URL_TEMPLATE,
)
from ..utils.logging import log_event
from .search_jobs import SearchJobCapacityError, SearchJobStore


class AppHandler(SimpleHTTPRequestHandler):
    """HTTP request handler for the local FlightFinder web server."""

    optimizer = SplitTripOptimizer(KiwiClient(), AirportCoordinates())
    job_store = SearchJobStore()

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the AppHandler.

        Args:
            args: Positional arguments forwarded to the underlying implementation.
            kwargs: Keyword arguments forwarded to the underlying implementation.
        """
        super().__init__(*args, directory=str(STATIC_DIR), **kwargs)

    def end_headers(self) -> None:
        # Local app assets should always refresh so frontend changes do not
        # leave the browser running stale cached JS or CSS.
        """Send the final HTTP response headers."""
        self.send_header("Cache-Control", HTTP_CACHE_CONTROL_NO_STORE)
        self.send_header("Pragma", HTTP_PRAGMA_NO_CACHE)
        self.send_header("Expires", HTTP_EXPIRES_IMMEDIATELY)
        super().end_headers()

    def _send_json(self, status: int, payload: dict[str, Any]) -> None:
        """Send a JSON response payload.

        Args:
            status: HTTP status code to send.
            payload: JSON-serializable payload for the operation.
        """
        data = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        """Handle HTTP GET requests."""
        split = urlsplit(self.path)
        path = split.path
        query = parse_qs(split.query)
        if path.startswith(f"{API_SEARCH_JOBS_PATH}/"):
            job_id = path.rsplit("/", 1)[-1].strip()
            job = self.job_store.get_job(job_id)
            if job is None:
                self._send_json(
                    HTTPStatus.NOT_FOUND,
                    {"error": ERROR_UNKNOWN_SEARCH_JOB},
                )
                return
            raw_since_event_index = (query.get("since_event_index") or [None])[0]
            try:
                since_event_index = (
                    max(0, int(raw_since_event_index))
                    if raw_since_event_index is not None
                    else None
                )
            except (TypeError, ValueError):
                since_event_index = None
            self._send_json(
                HTTPStatus.OK,
                job.snapshot(since_event_index=since_event_index),
            )
            return

        if path == "/api/presets":
            self._send_json(
                HTTPStatus.OK,
                {
                    "origins": ["OTP"],
                    "auto_hub_candidates": AUTO_HUB_CANDIDATES,
                    "providers": self.optimizer.provider_catalog(),
                    "system_limits": self.optimizer.runtime_capabilities(),
                    "destinations": [
                        {
                            "code": code,
                            "name": meta.get("name", code),
                            "note": meta.get("note", ""),
                        }
                        for code, meta in DESTINATION_NOTES.items()
                    ],
                },
            )
            return

        if path == API_PROVIDER_CONFIG_PATH:
            self._send_json(
                HTTPStatus.OK,
                {
                    "runtime_provider_config": self.optimizer.runtime_provider_config_status(),
                    "providers": self.optimizer.provider_catalog(),
                    "system_limits": self.optimizer.runtime_capabilities(),
                },
            )
            return

        if path == "/":
            self.path = ROOT_INDEX_PATH
        return super().do_GET()

    def do_POST(self) -> None:
        """Handle HTTP POST requests."""
        path = self.path.split("?", 1)[0]
        if path not in {API_SEARCH_PATH, API_PROVIDER_CONFIG_PATH, API_SEARCH_JOBS_PATH}:
            self._send_json(
                HTTPStatus.NOT_FOUND,
                {"error": ERROR_UNKNOWN_ENDPOINT},
            )
            return

        try:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length)
            payload = json.loads(raw.decode("utf-8") or "{}")
        except Exception:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {"error": ERROR_INVALID_JSON_BODY},
            )
            return

        if path == API_PROVIDER_CONFIG_PATH:
            try:
                self.optimizer.update_runtime_provider_secrets(
                    payload if isinstance(payload, dict) else {}
                )
            except Exception as exc:
                self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                return
            self._send_json(
                HTTPStatus.OK,
                {
                    "ok": True,
                    "runtime_provider_config": self.optimizer.runtime_provider_config_status(),
                    "providers": self.optimizer.provider_catalog(),
                },
            )
            return

        if path == API_SEARCH_JOBS_PATH:
            log_event(
                logging.INFO,
                "http_search_job_request",
                client=str(getattr(self, "client_address", ("", ""))[0]),
            )
            try:
                config = self.optimizer.parse_search_config(payload)
                job = self.job_store.start_job(self.optimizer, config)
            except ValueError as exc:
                self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                return
            except SearchJobCapacityError as exc:
                self._send_json(HTTPStatus.TOO_MANY_REQUESTS, {"error": str(exc)})
                return
            except Exception as exc:
                self._send_json(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    {"error": f"Search failed to start: {exc}"},
                )
                return

            self._send_json(HTTPStatus.ACCEPTED, job.snapshot())
            return

        log_event(
            logging.INFO,
            "http_search_request",
            client=str(getattr(self, "client_address", ("", ""))[0]),
        )
        try:
            config = self.optimizer.parse_search_config(payload)
            result = self.optimizer.search(config)
        except ValueError as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
            return
        except requests.RequestException as exc:
            self._send_json(
                HTTPStatus.BAD_GATEWAY,
                {"error": f"Flight provider request failed: {exc}"},
            )
            return
        except TimeoutError as exc:
            self._send_json(
                HTTPStatus.GATEWAY_TIMEOUT,
                {"error": str(exc)},
            )
            return
        except Exception as exc:
            self._send_json(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {"error": f"Search failed: {exc}"},
            )
            return

        self._send_json(HTTPStatus.OK, result)


def run_server() -> None:
    """Start the local FlightFinder web server."""
    host = os.getenv("HOST", DEFAULT_SERVER_HOST)
    port = int(os.getenv("PORT", str(DEFAULT_SERVER_PORT)))
    log_event(
        logging.INFO,
        "server_starting",
        host=host,
        port=port,
        allow_playwright_providers=ALLOW_PLAYWRIGHT_PROVIDERS,
        skyscanner_playwright_fallback=SKYSCANNER_SCRAPE_PLAYWRIGHT_FALLBACK,
        default_search_timeout_seconds=DEFAULT_SEARCH_TIMEOUT_SECONDS,
        default_io_workers=DEFAULT_IO_WORKERS,
    )
    server = ThreadingHTTPServer((host, port), AppHandler)
    print(SERVER_READY_URL_TEMPLATE.format(host=host, port=port))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
