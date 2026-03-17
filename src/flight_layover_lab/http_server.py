from __future__ import annotations

import json
import logging
import os
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlsplit

import requests

from .airports import AirportCoordinates
from .config import (
    ALLOW_PLAYWRIGHT_PROVIDERS,
    AUTO_HUB_CANDIDATES,
    DEFAULT_IO_WORKERS,
    DEFAULT_SEARCH_TIMEOUT_SECONDS,
    DESTINATION_NOTES,
    SKYSCANNER_SCRAPE_PLAYWRIGHT_FALLBACK,
)
from .engine import SplitTripOptimizer
from .logging_utils import log_event
from .providers import KiwiClient
from .resources import STATIC_DIR
from .search_jobs import SearchJobCapacityError, SearchJobStore


class AppHandler(SimpleHTTPRequestHandler):
    optimizer = SplitTripOptimizer(KiwiClient(), AirportCoordinates())
    job_store = SearchJobStore()

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, directory=str(STATIC_DIR), **kwargs)

    def end_headers(self) -> None:
        # Local app assets should always refresh so frontend changes do not
        # leave the browser running stale cached JS or CSS.
        self.send_header("Cache-Control", "no-store, max-age=0, must-revalidate")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        super().end_headers()

    def _send_json(self, status: int, payload: dict[str, Any]) -> None:
        data = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        split = urlsplit(self.path)
        path = split.path
        query = parse_qs(split.query)
        if path.startswith("/api/search-jobs/"):
            job_id = path.rsplit("/", 1)[-1].strip()
            job = self.job_store.get_job(job_id)
            if job is None:
                self._send_json(
                    HTTPStatus.NOT_FOUND,
                    {"error": "Unknown search job"},
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

        if path == "/api/provider-config":
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
            self.path = "/index.html"
        return super().do_GET()

    def do_POST(self) -> None:
        path = self.path.split("?", 1)[0]
        if path not in {"/api/search", "/api/provider-config", "/api/search-jobs"}:
            self._send_json(
                HTTPStatus.NOT_FOUND,
                {"error": "Unknown endpoint"},
            )
            return

        try:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length)
            payload = json.loads(raw.decode("utf-8") or "{}")
        except Exception:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {"error": "Invalid JSON body"},
            )
            return

        if path == "/api/provider-config":
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

        if path == "/api/search-jobs":
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
    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "8000"))
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
    print(f"FlightFinder Engine running at http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
