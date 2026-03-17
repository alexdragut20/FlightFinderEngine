from __future__ import annotations

import io
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler
from types import SimpleNamespace

import requests

from flight_layover_lab.http_server import AppHandler
from flight_layover_lab.resources import STATIC_DIR
from flight_layover_lab.search_jobs import SearchJobCapacityError


def test_app_handler_end_headers_disables_caching(monkeypatch) -> None:
    handler = AppHandler.__new__(AppHandler)
    sent_headers: list[tuple[str, str]] = []
    super_called = {"value": False}

    def record_header(name: str, value: str) -> None:
        sent_headers.append((name, value))

    def fake_super_end_headers(_self: object) -> None:
        super_called["value"] = True

    handler.send_header = record_header  # type: ignore[attr-defined]
    monkeypatch.setattr(SimpleHTTPRequestHandler, "end_headers", fake_super_end_headers)

    AppHandler.end_headers(handler)

    assert ("Cache-Control", "no-store, max-age=0, must-revalidate") in sent_headers
    assert ("Pragma", "no-cache") in sent_headers
    assert ("Expires", "0") in sent_headers
    assert super_called["value"] is True


def test_index_html_uses_versioned_static_assets() -> None:
    index_html = (STATIC_DIR / "index.html").read_text(encoding="utf-8")

    assert "/styles.css?v=" in index_html
    assert "/app.js?v=" in index_html


def test_search_job_endpoint_returns_429_when_job_store_is_full(monkeypatch) -> None:
    handler = AppHandler.__new__(AppHandler)
    handler.path = "/api/search-jobs"
    handler.headers = {"Content-Length": "2"}
    handler.rfile = io.BytesIO(b"{}")
    handler.client_address = ("127.0.0.1", 12345)

    sent: dict[str, object] = {}

    class OptimizerStub:
        def parse_search_config(self, payload: object) -> object:
            return payload

    class JobStoreStub:
        def start_job(self, optimizer: object, config: object) -> object:
            raise SearchJobCapacityError("busy")

    def fake_send_json(status: int, payload: dict[str, object]) -> None:
        sent["status"] = status
        sent["payload"] = payload

    monkeypatch.setattr("flight_layover_lab.http_server.log_event", lambda *args, **kwargs: None)
    handler.optimizer = OptimizerStub()
    handler.job_store = JobStoreStub()
    handler._send_json = fake_send_json  # type: ignore[method-assign]

    AppHandler.do_POST(handler)

    assert sent["status"] == HTTPStatus.TOO_MANY_REQUESTS
    assert sent["payload"] == {"error": "busy"}


def test_search_job_get_passes_incremental_event_cursor() -> None:
    handler = AppHandler.__new__(AppHandler)
    handler.path = "/api/search-jobs/job-123?since_event_index=5"

    sent: dict[str, object] = {}

    class JobStub:
        def __init__(self) -> None:
            self.called_with: int | None = None

        def snapshot(self, *, since_event_index: int | None = None) -> dict[str, object]:
            self.called_with = since_event_index
            return {"job_id": "job-123", "status": "running", "progress": {"events": []}}

    job = JobStub()

    class JobStoreStub:
        def get_job(self, job_id: str) -> object | None:
            assert job_id == "job-123"
            return job

    def fake_send_json(status: int, payload: dict[str, object]) -> None:
        sent["status"] = status
        sent["payload"] = payload

    handler.job_store = JobStoreStub()
    handler._send_json = fake_send_json  # type: ignore[method-assign]

    AppHandler.do_GET(handler)

    assert sent["status"] == HTTPStatus.OK
    assert sent["payload"] == {"job_id": "job-123", "status": "running", "progress": {"events": []}}
    assert job.called_with == 5


def test_send_json_writes_response_body(monkeypatch) -> None:
    handler = AppHandler.__new__(AppHandler)
    handler.wfile = io.BytesIO()

    sent: list[tuple[str, object]] = []
    monkeypatch.setattr(handler, "send_response", lambda status: sent.append(("status", status)))
    monkeypatch.setattr(handler, "send_header", lambda name, value: sent.append((name, value)))
    monkeypatch.setattr(handler, "end_headers", lambda: sent.append(("ended", True)))

    AppHandler._send_json(handler, HTTPStatus.CREATED, {"ok": True})

    assert ("status", HTTPStatus.CREATED) in sent
    assert ("Content-Type", "application/json; charset=utf-8") in sent
    assert handler.wfile.getvalue() == b'{"ok": true}'


def test_get_presets_returns_catalog_and_limits() -> None:
    handler = AppHandler.__new__(AppHandler)
    handler.path = "/api/presets"
    sent: dict[str, object] = {}

    class OptimizerStub:
        def provider_catalog(self) -> list[dict[str, object]]:
            return [{"id": "kiwi"}]

        def runtime_capabilities(self) -> dict[str, object]:
            return {"cpu_workers": 20}

    handler.optimizer = OptimizerStub()
    handler._send_json = lambda status, payload: sent.update({"status": status, "payload": payload})  # type: ignore[method-assign]

    AppHandler.do_GET(handler)

    assert sent["status"] == HTTPStatus.OK
    payload = sent["payload"]
    assert isinstance(payload, dict)
    assert payload["providers"] == [{"id": "kiwi"}]
    assert payload["system_limits"] == {"cpu_workers": 20}
    assert payload["origins"] == ["OTP"]


def test_get_provider_config_returns_runtime_status() -> None:
    handler = AppHandler.__new__(AppHandler)
    handler.path = "/api/provider-config"
    sent: dict[str, object] = {}

    class OptimizerStub:
        def runtime_provider_config_status(self) -> dict[str, object]:
            return {"amadeus": {"configured": False}}

        def provider_catalog(self) -> list[dict[str, object]]:
            return [{"id": "kiwi"}]

        def runtime_capabilities(self) -> dict[str, object]:
            return {"cpu_workers": 20}

    handler.optimizer = OptimizerStub()
    handler._send_json = lambda status, payload: sent.update({"status": status, "payload": payload})  # type: ignore[method-assign]

    AppHandler.do_GET(handler)

    assert sent["status"] == HTTPStatus.OK
    payload = sent["payload"]
    assert isinstance(payload, dict)
    assert payload["runtime_provider_config"] == {"amadeus": {"configured": False}}


def test_get_unknown_search_job_returns_not_found() -> None:
    handler = AppHandler.__new__(AppHandler)
    handler.path = "/api/search-jobs/missing"
    handler.job_store = SimpleNamespace(get_job=lambda job_id: None)
    sent: dict[str, object] = {}
    handler._send_json = lambda status, payload: sent.update({"status": status, "payload": payload})  # type: ignore[method-assign]

    AppHandler.do_GET(handler)

    assert sent["status"] == HTTPStatus.NOT_FOUND
    assert sent["payload"] == {"error": "Unknown search job"}


def test_get_root_delegates_to_static_index(monkeypatch) -> None:
    handler = AppHandler.__new__(AppHandler)
    handler.path = "/"
    called = {"path": None}

    def fake_super_get(_self: object) -> str:
        called["path"] = handler.path
        return "ok"

    monkeypatch.setattr(SimpleHTTPRequestHandler, "do_GET", fake_super_get)

    result = AppHandler.do_GET(handler)

    assert result == "ok"
    assert called["path"] == "/index.html"


def test_post_invalid_endpoint_and_bad_json() -> None:
    missing = AppHandler.__new__(AppHandler)
    missing.path = "/api/unknown"
    missing.headers = {"Content-Length": "2"}
    missing.rfile = io.BytesIO(b"{}")
    sent_missing: dict[str, object] = {}
    missing._send_json = lambda status, payload: sent_missing.update(
        {"status": status, "payload": payload}
    )  # type: ignore[method-assign]

    AppHandler.do_POST(missing)

    assert sent_missing["status"] == HTTPStatus.NOT_FOUND

    bad_json = AppHandler.__new__(AppHandler)
    bad_json.path = "/api/search"
    bad_json.headers = {"Content-Length": "3"}
    bad_json.rfile = io.BytesIO(b"{]")
    sent_bad: dict[str, object] = {}
    bad_json._send_json = lambda status, payload: sent_bad.update(
        {"status": status, "payload": payload}
    )  # type: ignore[method-assign]

    AppHandler.do_POST(bad_json)

    assert sent_bad["status"] == HTTPStatus.BAD_REQUEST
    assert sent_bad["payload"] == {"error": "Invalid JSON body"}


def test_post_provider_config_handles_success_and_failure(monkeypatch) -> None:
    handler = AppHandler.__new__(AppHandler)
    handler.path = "/api/provider-config"
    handler.headers = {"Content-Length": "2"}
    handler.rfile = io.BytesIO(b"{}")
    sent: dict[str, object] = {}

    class OptimizerStub:
        def __init__(self) -> None:
            self.fail = False

        def update_runtime_provider_secrets(self, payload: dict[str, object]) -> None:
            if self.fail:
                raise RuntimeError("bad config")

        def runtime_provider_config_status(self) -> dict[str, object]:
            return {"serpapi": {"configured": True}}

        def provider_catalog(self) -> list[dict[str, object]]:
            return [{"id": "kiwi"}]

    optimizer = OptimizerStub()
    handler.optimizer = optimizer
    handler._send_json = lambda status, payload: sent.update({"status": status, "payload": payload})  # type: ignore[method-assign]

    AppHandler.do_POST(handler)
    assert sent["status"] == HTTPStatus.OK

    handler_fail = AppHandler.__new__(AppHandler)
    handler_fail.path = "/api/provider-config"
    handler_fail.headers = {"Content-Length": "2"}
    handler_fail.rfile = io.BytesIO(b"{}")
    optimizer.fail = True
    handler_fail.optimizer = optimizer
    sent_fail: dict[str, object] = {}
    handler_fail._send_json = lambda status, payload: sent_fail.update(
        {"status": status, "payload": payload}
    )  # type: ignore[method-assign]

    AppHandler.do_POST(handler_fail)
    assert sent_fail["status"] == HTTPStatus.BAD_REQUEST
    assert sent_fail["payload"] == {"error": "bad config"}


def test_post_search_handles_success_and_error_paths(monkeypatch) -> None:
    monkeypatch.setattr("flight_layover_lab.http_server.log_event", lambda *args, **kwargs: None)

    class OptimizerStub:
        def __init__(self, result: object) -> None:
            self.result = result

        def parse_search_config(self, payload: object) -> object:
            if self.result == "value-error":
                raise ValueError("invalid config")
            return {"parsed": True}

        def search(self, config: object) -> object:
            if self.result == "request-error":
                raise requests.RequestException("provider down")
            if self.result == "timeout":
                raise TimeoutError("took too long")
            if self.result == "boom":
                raise RuntimeError("boom")
            return {"ok": True}

    def _run_case(result: object) -> tuple[int, dict[str, object]]:
        handler = AppHandler.__new__(AppHandler)
        handler.path = "/api/search"
        handler.headers = {"Content-Length": "2"}
        handler.rfile = io.BytesIO(b"{}")
        handler.client_address = ("127.0.0.1", 12345)
        handler.optimizer = OptimizerStub(result)
        sent: dict[str, object] = {}
        handler._send_json = lambda status, payload: sent.update(
            {"status": status, "payload": payload}
        )  # type: ignore[method-assign]
        AppHandler.do_POST(handler)
        return int(sent["status"]), sent["payload"]  # type: ignore[return-value]

    assert _run_case("ok") == (HTTPStatus.OK, {"ok": True})
    assert _run_case("value-error") == (HTTPStatus.BAD_REQUEST, {"error": "invalid config"})
    assert _run_case("request-error") == (
        HTTPStatus.BAD_GATEWAY,
        {"error": "Flight provider request failed: provider down"},
    )
    assert _run_case("timeout") == (HTTPStatus.GATEWAY_TIMEOUT, {"error": "took too long"})
    assert _run_case("boom") == (
        HTTPStatus.INTERNAL_SERVER_ERROR,
        {"error": "Search failed: boom"},
    )
