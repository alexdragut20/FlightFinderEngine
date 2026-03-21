from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
import requests

from src.exceptions import ProviderBlockedError, ProviderNoResultError
from src.providers.amadeus import AmadeusClient
from src.providers.kiwi import KiwiClient
from src.providers.multi import MultiProviderClient
from src.providers.serpapi import SerpApiGoogleFlightsClient
from src.providers.travelpayouts import TravelpayoutsDataClient


class _FakeResponse:
    def __init__(
        self, payload: object, *, status_code: int = 200, headers: dict[str, str] | None = None
    ):
        self._payload = payload
        self.status_code = status_code
        self.headers = headers or {}

    def json(self) -> object:
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


class _FakeSession:
    def __init__(
        self,
        *,
        get_responses: list[_FakeResponse] | None = None,
        post_responses: list[_FakeResponse] | None = None,
    ) -> None:
        self.get_responses = list(get_responses or [])
        self.post_responses = list(post_responses or [])
        self.get_calls: list[tuple[str, dict[str, object], dict[str, object]]] = []
        self.post_calls: list[tuple[str, dict[str, object], dict[str, object]]] = []

    def get(self, url: str, **kwargs: object) -> _FakeResponse:
        self.get_calls.append((url, dict(kwargs), dict(kwargs.get("params") or {})))
        return self.get_responses.pop(0)

    def post(self, url: str, **kwargs: object) -> _FakeResponse:
        self.post_calls.append((url, dict(kwargs), dict(kwargs.get("data") or {})))
        return self.post_responses.pop(0)


class _StubProvider:
    def __init__(self, provider_id: str, *, supports_calendar: bool = True) -> None:
        self.provider_id = provider_id
        self.supports_calendar = supports_calendar


def _kiwi_segment(
    source: str,
    destination: str,
    depart_local: str,
    arrive_local: str,
    carrier: str,
) -> dict[str, object]:
    return {
        "segment": {
            "source": {
                "localTime": depart_local,
                "station": {"code": source, "name": source},
            },
            "destination": {
                "localTime": arrive_local,
                "station": {"code": destination, "name": destination},
            },
            "carrier": {"code": carrier, "name": carrier},
        }
    }


def test_kiwi_client_parses_calendar_and_best_itineraries() -> None:
    client = KiwiClient()
    client._post = lambda query, variables: {  # type: ignore[assignment]
        "data": {
            "itineraryPricesCalendar": {
                "calendar": [
                    {"date": "2026-03-10", "ratedPrice": {"price": {"amount": "123.7"}}},
                    {"date": "2026-03-11", "ratedPrice": {"price": {"amount": ""}}},
                    {"date": "2026-03-12", "ratedPrice": {"price": {"amount": "bad"}}},
                ]
            }
        }
    }

    prices = client.get_calendar_prices(
        "OTP",
        "MGA",
        "2026-03-10",
        "2026-03-12",
        "RON",
        2,
        2,
        1,
        0,
    )

    assert prices == {"2026-03-10": 123}
    assert client._passengers_payload(
        type("P", (), {"adults": 2, "hand_bags": 1, "hold_bags": 0})()
    ) == {  # type: ignore[arg-type]
        "adults": 2,
        "adultsHandBags": [1, 1],
        "adultsHoldBags": [0, 0],
    }


def test_kiwi_client_best_oneway_and_return_respect_caps() -> None:
    client = KiwiClient()
    payloads = {
        "oneway": {
            "data": {
                "onewayItineraries": {
                    "itineraries": [
                        {
                            "price": {
                                "amount": "500",
                                "formattedValue": "500 RON",
                                "currency": {"code": "RON"},
                            },
                            "bookingOptions": {"edges": [{"node": {"bookingUrl": "/deal-1"}}]},
                            "duration": 7200,
                            "sector": {
                                "sectorSegments": [
                                    _kiwi_segment(
                                        "OTP",
                                        "IST",
                                        "2026-03-10T08:00:00",
                                        "2026-03-10T09:00:00",
                                        "TK",
                                    ),
                                    _kiwi_segment(
                                        "IST",
                                        "MGA",
                                        "2026-03-10T09:45:00",
                                        "2026-03-10T13:00:00",
                                        "TK",
                                    ),
                                ]
                            },
                        },
                        {
                            "price": {
                                "amount": "500",
                                "formattedValue": "500 RON",
                                "currency": {"code": "RON"},
                            },
                            "bookingOptions": {"edges": [{"node": {"bookingUrl": "/deal-2"}}]},
                            "duration": 8200,
                            "sector": {
                                "sectorSegments": [
                                    _kiwi_segment(
                                        "OTP",
                                        "CDG",
                                        "2026-03-10T08:00:00",
                                        "2026-03-10T09:00:00",
                                        "AF",
                                    ),
                                    _kiwi_segment(
                                        "CDG",
                                        "MAD",
                                        "2026-03-10T10:00:00",
                                        "2026-03-10T11:00:00",
                                        "AF",
                                    ),
                                    _kiwi_segment(
                                        "MAD",
                                        "MGA",
                                        "2026-03-10T12:00:00",
                                        "2026-03-10T15:00:00",
                                        "AF",
                                    ),
                                ]
                            },
                        },
                    ]
                }
            }
        },
        "return": {
            "data": {
                "returnItineraries": {
                    "itineraries": [
                        {
                            "price": {
                                "amount": "900",
                                "formattedValue": "900 RON",
                                "currency": {"code": "RON"},
                            },
                            "bookingOptions": {"edges": [{"node": {"bookingUrl": "/return"}}]},
                            "duration": 15000,
                            "outbound": {
                                "duration": 7000,
                                "sectorSegments": [
                                    _kiwi_segment(
                                        "OTP",
                                        "IST",
                                        "2026-03-10T08:00:00",
                                        "2026-03-10T09:00:00",
                                        "TK",
                                    ),
                                    _kiwi_segment(
                                        "IST",
                                        "MGA",
                                        "2026-03-10T09:45:00",
                                        "2026-03-10T13:00:00",
                                        "TK",
                                    ),
                                ],
                            },
                            "inbound": {
                                "duration": 8000,
                                "sectorSegments": [
                                    _kiwi_segment(
                                        "MGA",
                                        "IST",
                                        "2026-03-24T08:00:00",
                                        "2026-03-24T09:00:00",
                                        "TK",
                                    ),
                                    _kiwi_segment(
                                        "IST",
                                        "OTP",
                                        "2026-03-24T10:00:00",
                                        "2026-03-24T12:30:00",
                                        "TK",
                                    ),
                                ],
                            },
                        }
                    ]
                }
            }
        },
    }
    client._post = lambda query, variables: payloads[
        "return" if "returnItineraries" in query else "oneway"
    ]  # type: ignore[assignment]

    best_oneway = client.get_best_oneway("OTP", "MGA", "2026-03-10", "RON", 2, 1, 0, 0)
    best_return = client.get_best_return(
        "OTP", "MGA", "2026-03-10", "2026-03-24", "RON", 2, 1, 0, 0
    )
    strict_cap = client.get_best_oneway(
        "OTP",
        "MGA",
        "2026-03-10",
        "RON",
        2,
        1,
        0,
        0,
        max_connection_layover_seconds=900,
    )

    assert best_oneway is not None
    assert best_oneway["price"] == 500
    assert best_oneway["stops"] == 1
    assert str(best_oneway["booking_url"]).endswith("/deal-1")
    assert best_return is not None
    assert best_return["price"] == 900
    assert best_return["outbound_stops"] == 1
    assert best_return["inbound_stops"] == 1
    assert strict_cap is None


def test_amadeus_token_and_get_paths(monkeypatch) -> None:
    client = AmadeusClient(client_id="id", client_secret="secret", base_url="https://amadeus.test")
    session = _FakeSession(
        post_responses=[
            _FakeResponse({"access_token": "token-1", "expires_in": 120}),
            _FakeResponse({"access_token": "token-2", "expires_in": 120}),
        ],
        get_responses=[
            _FakeResponse({}, status_code=401),
            _FakeResponse({"data": []}, status_code=200),
            _FakeResponse({"errors": [{"detail": "no flight offers found"}]}, status_code=400),
        ],
    )
    monkeypatch.setattr(client, "_session", lambda: session)
    monkeypatch.setattr(
        "src.providers.amadeus._capture_provider_response",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr("src.providers.amadeus.time.sleep", lambda *_args, **_kwargs: None)

    assert client._fetch_token() == "token-1"
    assert client._fetch_token() == "token-1"
    assert len(session.post_calls) == 1

    result = client._get("/v2/shopping/flight-offers", {"originLocationCode": "OTP"})
    assert result == {"data": []}
    assert len(session.post_calls) == 2

    with pytest.raises(ProviderNoResultError):
        client._get("/v2/shopping/flight-offers", {"originLocationCode": "OTP"})


def test_amadeus_best_calendar_oneway_and_return(monkeypatch) -> None:
    client = AmadeusClient(client_id="id", client_secret="secret")
    monkeypatch.setattr(client, "is_configured", lambda: True)
    calendar_payload = {
        "data": [
            {"departureDate": "2026-03-10", "price": {"total": "510"}},
            {"departureDate": "2026-03-10", "price": {"total": "490"}},
            {"departureDate": "2026-03-11", "price": {"total": "bad"}},
        ]
    }
    offer_payload = {
        "data": [
            {
                "price": {"grandTotal": "700", "currency": "EUR"},
                "itineraries": [
                    {
                        "duration": "PT5H",
                        "segments": [
                            {
                                "departure": {"iataCode": "OTP", "at": "2026-03-10T08:00:00"},
                                "arrival": {"iataCode": "IST", "at": "2026-03-10T09:00:00"},
                                "carrierCode": "TK",
                            },
                            {
                                "departure": {"iataCode": "IST", "at": "2026-03-10T10:00:00"},
                                "arrival": {"iataCode": "MGA", "at": "2026-03-10T13:00:00"},
                                "carrierCode": "TK",
                            },
                        ],
                    },
                    {
                        "duration": "PT6H",
                        "segments": [
                            {
                                "departure": {"iataCode": "MGA", "at": "2026-03-24T08:00:00"},
                                "arrival": {"iataCode": "IST", "at": "2026-03-24T10:00:00"},
                                "carrierCode": "TK",
                            },
                            {
                                "departure": {"iataCode": "IST", "at": "2026-03-24T11:00:00"},
                                "arrival": {"iataCode": "OTP", "at": "2026-03-24T13:00:00"},
                                "carrierCode": "TK",
                            },
                        ],
                    },
                ],
            },
            {
                "price": {"grandTotal": "650", "currency": "EUR"},
                "itineraries": [
                    {
                        "duration": "PT8H",
                        "segments": [
                            {
                                "departure": {"iataCode": "OTP", "at": "2026-03-10T08:00:00"},
                                "arrival": {"iataCode": "CDG", "at": "2026-03-10T10:00:00"},
                                "carrierCode": "AF",
                            },
                            {
                                "departure": {"iataCode": "CDG", "at": "2026-03-10T11:00:00"},
                                "arrival": {"iataCode": "MAD", "at": "2026-03-10T13:00:00"},
                                "carrierCode": "AF",
                            },
                            {
                                "departure": {"iataCode": "MAD", "at": "2026-03-10T14:00:00"},
                                "arrival": {"iataCode": "MGA", "at": "2026-03-10T18:00:00"},
                                "carrierCode": "AF",
                            },
                        ],
                    }
                ],
            },
        ]
    }

    monkeypatch.setattr(
        client,
        "_get",
        lambda path, params: calendar_payload if "flight-dates" in path else offer_payload,
    )

    prices = client.get_calendar_prices("OTP", "MGA", "2026-03-10", "2026-03-11", "EUR", 1, 1, 0, 0)
    best_oneway = client.get_best_oneway("OTP", "MGA", "2026-03-10", "EUR", 2, 1, 0, 0)
    best_return = client.get_best_return(
        "OTP", "MGA", "2026-03-10", "2026-03-24", "EUR", 2, 1, 0, 0
    )

    assert prices == {"2026-03-10": 490}
    assert best_oneway is not None
    assert best_oneway["price"] == 650
    assert best_oneway["stops"] == 2
    assert best_return is not None
    assert best_return["price"] == 700
    assert best_return["outbound_stops"] == 1
    assert best_return["inbound_stops"] == 1


def test_serpapi_search_and_best_flights(monkeypatch) -> None:
    client = SerpApiGoogleFlightsClient(api_key="key", search_url="https://serpapi.example")
    session = _FakeSession(
        get_responses=[
            _FakeResponse({"error": "bad request"}, status_code=400),
            _FakeResponse(
                {
                    "search_metadata": {"google_flights_url": "https://google.example"},
                    "best_flights": [],
                }
            ),
        ]
    )
    monkeypatch.setattr(client, "_session", lambda: session)
    monkeypatch.setattr(
        "src.providers.serpapi._capture_provider_response",
        lambda *args, **kwargs: None,
    )

    with pytest.raises(requests.HTTPError):
        client._search({"departure_id": "OTP"})

    payload = client._search({"departure_id": "OTP"})
    assert payload["search_metadata"]["google_flights_url"] == "https://google.example"


def test_serpapi_best_oneway_and_return(monkeypatch) -> None:
    client = SerpApiGoogleFlightsClient(api_key="key")
    payloads = [
        {
            "search_metadata": {"google_flights_url": "https://google.example/out"},
            "best_flights": [
                {
                    "price": 480,
                    "total_duration": 180,
                    "flights": [
                        {
                            "departure_airport": {
                                "id": "OTP",
                                "name": "OTP",
                                "time": "2026-03-10 08:00",
                            },
                            "arrival_airport": {
                                "id": "IST",
                                "name": "IST",
                                "time": "2026-03-10 09:00",
                            },
                            "airline": "TK",
                        },
                        {
                            "departure_airport": {
                                "id": "IST",
                                "name": "IST",
                                "time": "2026-03-10 10:00",
                            },
                            "arrival_airport": {
                                "id": "MGA",
                                "name": "MGA",
                                "time": "2026-03-10 13:00",
                            },
                            "airline": "TK",
                        },
                    ],
                }
            ],
        },
        {
            "search_metadata": {"google_flights_url": "https://google.example/base"},
            "best_flights": [
                {
                    "price": 900,
                    "departure_token": "token-123",
                    "total_duration": 180,
                    "flights": [
                        {
                            "departure_airport": {
                                "id": "OTP",
                                "name": "OTP",
                                "time": "2026-03-10 08:00",
                            },
                            "arrival_airport": {
                                "id": "IST",
                                "name": "IST",
                                "time": "2026-03-10 09:00",
                            },
                            "airline": "TK",
                        },
                        {
                            "departure_airport": {
                                "id": "IST",
                                "name": "IST",
                                "time": "2026-03-10 10:00",
                            },
                            "arrival_airport": {
                                "id": "MGA",
                                "name": "MGA",
                                "time": "2026-03-10 13:00",
                            },
                            "airline": "TK",
                        },
                    ],
                }
            ],
        },
        {
            "search_metadata": {"raw_html_file": "https://google.example/return"},
            "best_flights": [
                {
                    "price": 900,
                    "total_duration": 200,
                    "flights": [
                        {
                            "departure_airport": {
                                "id": "MGA",
                                "name": "MGA",
                                "time": "2026-03-24 08:00",
                            },
                            "arrival_airport": {
                                "id": "IST",
                                "name": "IST",
                                "time": "2026-03-24 10:00",
                            },
                            "airline": "TK",
                        },
                        {
                            "departure_airport": {
                                "id": "IST",
                                "name": "IST",
                                "time": "2026-03-24 11:00",
                            },
                            "arrival_airport": {
                                "id": "OTP",
                                "name": "OTP",
                                "time": "2026-03-24 13:00",
                            },
                            "airline": "TK",
                        },
                    ],
                }
            ],
        },
    ]
    monkeypatch.setattr(client, "_search", lambda params: payloads.pop(0))

    best_oneway = client.get_best_oneway("OTP", "MGA", "2026-03-10", "RON", 2, 1, 0, 0)
    best_return = client.get_best_return(
        "OTP", "MGA", "2026-03-10", "2026-03-24", "RON", 2, 1, 0, 0
    )

    assert best_oneway is not None
    assert best_oneway["price"] == 480
    assert best_oneway["stops"] == 1
    assert best_return is not None
    assert best_return["price"] == 900
    assert best_return["outbound_stops"] == 1
    assert best_return["inbound_stops"] == 1
    assert str(best_return["booking_url"]).startswith("https://google.example")


class _StubProvider:
    def __init__(
        self,
        provider_id: str,
        *,
        calendar: dict[str, int] | None = None,
        oneway: dict[str, object] | None = None,
        ret: dict[str, object] | None = None,
        supports_calendar: bool = True,
        exc: Exception | None = None,
    ) -> None:
        self.provider_id = provider_id
        self.supports_calendar = supports_calendar
        self._calendar = calendar or {}
        self._oneway = oneway
        self._return = ret
        self._exc = exc

    def get_calendar_prices(self, **kwargs: object) -> dict[str, int]:
        if self._exc:
            raise self._exc
        return self._calendar

    def get_best_oneway(self, **kwargs: object) -> dict[str, object] | None:
        if self._exc:
            raise self._exc
        return self._oneway

    def get_best_return(self, **kwargs: object) -> dict[str, object] | None:
        if self._exc:
            raise self._exc
        return self._return


def test_multi_provider_client_merges_prices_and_tracks_budgets(monkeypatch) -> None:
    cheap = _StubProvider(
        "kiwi",
        calendar={"2026-03-10": 400, "2026-03-11": 600},
        oneway={"price": 400, "stops": 1, "duration_seconds": 5000},
        ret={"price": 900, "outbound_stops": 1, "inbound_stops": 1, "duration_seconds": 12000},
    )
    expensive = _StubProvider(
        "amadeus",
        calendar={"2026-03-10": 450},
        oneway={"price": 450, "stops": 0, "duration_seconds": 4500},
        ret={"price": 950, "outbound_stops": 0, "inbound_stops": 0, "duration_seconds": 11000},
    )
    noisy = _StubProvider("serpapi", exc=OSError(24, "Too many open files"))
    client = MultiProviderClient(
        [cheap, expensive, noisy],
        max_total_calls=2,
        max_calls_by_provider={"amadeus": 1},
    )
    monkeypatch.setattr("src.providers.multi.log_event", lambda *args, **kwargs: None)

    prices = client.get_calendar_prices(
        "OTP",
        "MGA",
        "2026-03-10",
        "2026-03-11",
        "RON",
        2,
        1,
        0,
        0,
    )
    best_oneway = client.get_best_oneway("OTP", "MGA", "2026-03-10", "RON", 2, 1, 0, 0)
    best_return = client.get_best_return(
        "OTP", "MGA", "2026-03-10", "2026-03-24", "RON", 2, 1, 0, 0
    )
    stats = client.stats_snapshot()

    assert prices == {"2026-03-10": 400, "2026-03-11": 600}
    assert best_oneway is not None
    assert best_oneway["provider"] == "kiwi"
    assert best_return is not None
    assert best_return["provider"] == "kiwi"
    assert stats["calendar_errors"]["serpapi"] == 1
    assert stats["oneway_skipped_cooldown"]["serpapi"] == 1
    assert stats["return_skipped_budget"]["amadeus"] == 1


def test_travelpayouts_client_parses_calendar_and_exact_fares() -> None:
    session = _FakeSession(
        get_responses=[
            _FakeResponse(
                {
                    "success": True,
                    "data": [
                        {
                            "origin": "OTP",
                            "destination": "FCO",
                            "price": 150,
                            "airline": "W6",
                            "departure_at": "2026-04-10T08:00:00+03:00",
                            "transfers": 0,
                            "duration_to": 120,
                            "link": "/search/OTP1004FCO1",
                        },
                        {
                            "origin": "OTP",
                            "destination": "FCO",
                            "price": 170,
                            "airline": "FR",
                            "departure_at": "2026-04-11T09:00:00+03:00",
                            "transfers": 1,
                            "duration_to": 180,
                            "link": "/search/OTP1104FCO1",
                        },
                    ],
                }
            ),
            _FakeResponse(
                {
                    "success": True,
                    "data": [
                        {
                            "origin": "OTP",
                            "destination": "FCO",
                            "price": 310,
                            "airline": "AZ",
                            "departure_at": "2026-04-10T06:30:00+03:00",
                            "return_at": "2026-04-17T19:20:00+02:00",
                            "transfers": 1,
                            "return_transfers": 0,
                            "duration": 360,
                            "duration_to": 175,
                            "duration_back": 185,
                            "link": "/search/OTP1004FCO1704",
                        },
                        {
                            "origin": "OTP",
                            "destination": "FCO",
                            "price": 295,
                            "airline": "AZ",
                            "departure_at": "2026-04-12T06:30:00+03:00",
                            "return_at": "2026-04-19T19:20:00+02:00",
                            "transfers": 0,
                            "return_transfers": 0,
                            "duration": 300,
                            "duration_to": 150,
                            "duration_back": 150,
                            "link": "/search/OTP1204FCO1904",
                        },
                    ],
                }
            ),
        ]
    )
    client = TravelpayoutsDataClient(api_token="tp-token")
    client._session = lambda: session  # type: ignore[assignment]

    prices = client.get_calendar_prices(
        "OTP",
        "FCO",
        "2026-04-10",
        "2026-04-11",
        "EUR",
        1,
        1,
        1,
        0,
    )
    best_oneway = client.get_best_oneway("OTP", "FCO", "2026-04-10", "EUR", 1, 1, 1, 0)
    best_return = client.get_best_return(
        "OTP",
        "FCO",
        "2026-04-10",
        "2026-04-17",
        "EUR",
        1,
        1,
        1,
        0,
    )

    assert prices == {"2026-04-10": 150, "2026-04-11": 170}
    assert best_oneway is not None
    assert best_oneway["price"] == 150
    assert best_oneway["booking_url"] == "https://www.aviasales.com/search/OTP1004FCO1"
    assert best_return is not None
    assert best_return["price"] == 310
    assert best_return["outbound_stops"] == 1
    assert best_return["inbound_stops"] == 0
    assert best_return["booking_url"] == "https://www.aviasales.com/search/OTP1004FCO1704"
    assert len(session.get_calls) == 2


def test_multi_provider_client_internal_selection_pause_and_tiebreak_paths(monkeypatch) -> None:
    class _Provider:
        def __init__(
            self,
            provider_id: str,
            *,
            oneway: dict[str, object] | None = None,
            ret: dict[str, object] | None = None,
            error: Exception | None = None,
        ) -> None:
            self.provider_id = provider_id
            self.supports_calendar = True
            self._oneway = oneway
            self._return = ret
            self._error = error

        def get_calendar_prices(self, **kwargs: object) -> dict[str, int]:
            return {}

        def get_best_oneway(self, **kwargs: object) -> dict[str, object] | None:
            if self._error:
                raise self._error
            return self._oneway

        def get_best_return(self, **kwargs: object) -> dict[str, object] | None:
            if self._error:
                raise self._error
            return self._return

    fast = _Provider(
        "kiwi",
        oneway={"price": 500, "stops": 1, "duration_seconds": 4000},
        ret={"price": 900, "outbound_stops": 1, "inbound_stops": 1, "duration_seconds": 9000},
    )
    slow = _Provider(
        "amadeus",
        oneway={"price": 500, "stops": 1, "duration_seconds": 7000},
        ret={"price": 900, "outbound_stops": 2, "inbound_stops": 1, "duration_seconds": 9500},
    )
    broken = _Provider("serpapi", error=OSError(24, "Too many open files"))
    no_result = _Provider("googleflights", error=ProviderNoResultError("no result"))
    blocked = _Provider(
        "kayak",
        error=ProviderBlockedError(
            "Kayak blocked automated scraping (captcha/anti-bot challenge).",
            manual_search_url="https://www.kayak.com/flights/OTP-MGA/2026-03-10",
            cooldown_seconds=120,
        ),
    )
    client = MultiProviderClient(
        [fast, slow, broken, no_result, blocked],
        max_total_calls=3,
        max_calls_by_provider={
            "kiwi": 2,
            "amadeus": 1,
            "serpapi": 1,
            "googleflights": 1,
            "kayak": 1,
        },
    )

    now = {"value": 1000.0}
    logged: list[str] = []
    monkeypatch.setattr("src.providers.multi.time.time", lambda: now["value"])
    monkeypatch.setattr(
        "src.providers.multi.log_event",
        lambda *_args, **kwargs: logged.append(str(kwargs.get("provider_id") or "")),
    )

    assert client.active_provider_ids == ["kiwi", "amadeus", "serpapi", "googleflights", "kayak"]
    assert [
        provider.provider_id for provider in client._providers_for_selection(("kiwi", "amadeus"))
    ] == [
        "kiwi",
        "amadeus",
    ]
    assert client._providers_for_selection(tuple()) == client.providers
    assert client._is_better_oneway(fast._oneway or {}, slow._oneway or {})
    assert client._is_better_return(fast._return or {}, slow._return or {})

    first = client.get_best_oneway("OTP", "MGA", "2026-03-10", "RON", 2, 1, 0, 0)
    second = client.get_best_return("OTP", "MGA", "2026-03-10", "2026-03-24", "RON", 2, 1, 0, 0)
    assert first is not None
    assert first["provider"] == "kiwi"
    assert second is not None
    assert second["provider"] == "kiwi"
    assert "serpapi" in logged
    assert "kayak" in logged
    assert client._provider_pause_remaining_seconds("serpapi") > 0
    assert client._provider_pause_remaining_seconds("kayak") > 0

    now["value"] += 1
    assert (
        client.get_best_oneway(
            "OTP",
            "MGA",
            "2026-03-11",
            "RON",
            2,
            1,
            0,
            0,
            provider_ids=("serpapi", "googleflights", "kayak"),
        )
        is None
    )
    stats = client.stats_snapshot()
    assert stats["oneway_errors"]["serpapi"] == 1
    assert stats["oneway_no_result"]["googleflights"] == 1
    assert stats["oneway_blocked"]["kayak"] == 1
    assert stats["oneway_skipped_cooldown"]["kayak"] == 1


def test_multi_provider_client_remaining_calendar_budget_and_helper_paths(
    monkeypatch,
) -> None:
    class _Provider:
        def __init__(
            self,
            provider_id: str,
            *,
            calendar: dict[str, object] | None = None,
            supports_calendar: bool = True,
            calendar_error: Exception | None = None,
        ) -> None:
            self.provider_id = provider_id
            self.supports_calendar = supports_calendar
            self._calendar = calendar or {}
            self._calendar_error = calendar_error

        def get_calendar_prices(self, **kwargs: object) -> dict[str, object]:
            if self._calendar_error:
                raise self._calendar_error
            return self._calendar

        def get_best_oneway(self, **kwargs: object) -> dict[str, object] | None:
            return None

        def get_best_return(self, **kwargs: object) -> dict[str, object] | None:
            return None

    free_provider = _Provider(
        "kiwi",
        calendar={"2026-03-10": "bad", "2026-03-11": 500},
    )
    paid_provider = _Provider(
        "amadeus",
        calendar={"2026-03-11": 450, "2026-03-12": 700},
    )
    disabled_provider = _Provider(
        "kayak",
        calendar={"2026-03-11": 300},
        supports_calendar=False,
    )
    no_result_provider = _Provider(
        "serpapi",
        calendar_error=ProviderNoResultError("no calendar result"),
    )
    client = MultiProviderClient(
        [free_provider, paid_provider, disabled_provider, no_result_provider],
        max_total_calls=1,
        max_calls_by_provider={"kiwi": 0, "amadeus": 1, "serpapi": 1},
    )

    logged: list[dict[str, object]] = []
    monkeypatch.setattr(
        "src.providers.multi.log_event",
        lambda level, event, **fields: logged.append({"event": event, **fields}),
    )

    prices = client.get_calendar_prices(
        "OTP",
        "MGA",
        "2026-03-10",
        "2026-03-12",
        "RON",
        2,
        2,
        1,
        0,
        provider_ids=("", "kiwi", "amadeus", "serpapi"),
    )
    stats = client.stats_snapshot()

    assert prices == {"2026-03-11": 450, "2026-03-12": 700}
    assert client._providers_for_selection(("", "   ")) == ()
    assert client._provider_pause_remaining_seconds("") == 0
    client._pause_provider("", 60)
    client._register_provider_exception("kiwi", RuntimeError("plain error"))
    assert not logged

    assert client._is_better_oneway(
        {"price": 500, "stops": 1, "duration_seconds": 1000},
        {"price": 500, "stops": 1, "duration_seconds": None},
    )
    assert not client._is_better_oneway(
        {"price": 500, "stops": 1, "duration_seconds": None},
        {"price": 500, "stops": 1, "duration_seconds": 1000},
    )
    assert client._is_better_return(
        {
            "price": 900,
            "outbound_stops": 1,
            "inbound_stops": 1,
            "duration_seconds": 2000,
        },
        {
            "price": 900,
            "outbound_stops": 1,
            "inbound_stops": 1,
            "duration_seconds": None,
        },
    )
    assert not client._is_better_return(
        {
            "price": 900,
            "outbound_stops": 1,
            "inbound_stops": 1,
            "duration_seconds": None,
        },
        {
            "price": 900,
            "outbound_stops": 1,
            "inbound_stops": 1,
            "duration_seconds": 2000,
        },
    )

    assert stats["calendar_calls"]["kiwi"] == 1
    assert stats["calendar_calls"]["amadeus"] == 1
    assert "kayak" not in stats["calendar_calls"]
    assert stats["calendar_skipped_budget"]["serpapi"] == 1
    assert stats["calendar_selected"]["amadeus"] == 1
    assert stats["budget"]["used_total_calls"] == 1


def test_kiwi_client_internal_paths_cover_session_post_and_selection(monkeypatch) -> None:
    session = _FakeSession(
        post_responses=[
            _FakeResponse({"data": {"ok": True}}),
            _FakeResponse({"errors": [{"message": "GraphQL broken"}]}),
            _FakeResponse({}, status_code=502),
        ]
    )
    monkeypatch.setattr("src.providers.kiwi.requests.Session", lambda: session)

    client = KiwiClient()
    assert client._session() is session
    assert client._session() is session
    assert client._post("query", {"route": "otp-mga"}) == {"data": {"ok": True}}
    with pytest.raises(RuntimeError, match="GraphQL broken"):
        client._post("query", {"route": "otp-sez"})
    with pytest.raises(requests.HTTPError):
        client._post("query", {"route": "otp-mru"})

    parsed_segments = client._parse_sector_segments(
        {
            "sectorSegments": [
                {},
                {
                    "segment": {
                        "source": {
                            "localTime": "2026-03-10T08:00:00",
                            "station": {"code": "OTP", "name": "Bucharest"},
                        },
                        "destination": {
                            "localTime": "2026-03-10T10:00:00",
                            "station": {"code": "IST", "name": "Istanbul"},
                        },
                        "carrier": {"code": "TK", "name": "Turkish Airlines"},
                    }
                },
            ]
        }
    )
    assert parsed_segments[0]["from"] is None
    assert parsed_segments[1]["carrier_name"] == "Turkish Airlines"

    calendar_client = KiwiClient()
    calendar_client._post = lambda *_args, **_kwargs: {"data": {"itineraryPricesCalendar": []}}  # type: ignore[assignment]
    assert (
        calendar_client.get_calendar_prices(
            "OTP",
            "MGA",
            "2026-03-10",
            "2026-03-12",
            "RON",
            1,
            2,
            1,
            0,
        )
        == {}
    )

    def _segment(
        source: str,
        destination: str,
        depart_local: str,
        arrive_local: str,
    ) -> dict[str, object]:
        return {
            "segment": {
                "source": {"localTime": depart_local, "station": {"code": source, "name": source}},
                "destination": {
                    "localTime": arrive_local,
                    "station": {"code": destination, "name": destination},
                },
                "carrier": {"code": "TK", "name": "Turkish Airlines"},
            }
        }

    oneway_client = KiwiClient()
    oneway_client._post = lambda *_args, **_kwargs: {  # type: ignore[assignment]
        "data": {
            "onewayItineraries": {
                "itineraries": [
                    {"price": {"amount": ""}},
                    {
                        "price": {"amount": "bad"},
                        "sector": {
                            "sectorSegments": [
                                _segment("OTP", "IST", "2026-03-10T08:00:00", "2026-03-10T09:00:00")
                            ]
                        },
                    },
                    {
                        "price": {
                            "amount": "350",
                            "formattedValue": "350 RON",
                            "currency": {"code": "RON"},
                        },
                        "duration": 12000,
                        "bookingOptions": {"edges": [{"node": {"bookingUrl": "/too-many"}}]},
                        "sector": {
                            "sectorSegments": [
                                _segment(
                                    "OTP", "IST", "2026-03-10T08:00:00", "2026-03-10T09:00:00"
                                ),
                                _segment(
                                    "IST", "DOH", "2026-03-10T10:00:00", "2026-03-10T12:00:00"
                                ),
                                _segment(
                                    "DOH", "MGA", "2026-03-10T13:00:00", "2026-03-10T16:00:00"
                                ),
                            ]
                        },
                    },
                    {
                        "price": {
                            "amount": "400",
                            "formattedValue": "400 RON",
                            "currency": {"code": "RON"},
                        },
                        "duration": 14000,
                        "bookingOptions": {"edges": [{"node": {"bookingUrl": "/slow-layover"}}]},
                        "sector": {
                            "sectorSegments": [
                                _segment(
                                    "OTP", "IST", "2026-03-10T08:00:00", "2026-03-10T09:00:00"
                                ),
                                _segment(
                                    "IST", "MGA", "2026-03-10T12:30:00", "2026-03-10T17:00:00"
                                ),
                            ]
                        },
                    },
                    {
                        "price": {
                            "amount": "420",
                            "formattedValue": "420 RON",
                            "currency": {"code": "RON"},
                        },
                        "duration": 7000,
                        "bookingOptions": {"edges": [{"node": {"bookingUrl": "/not-best"}}]},
                        "sector": {
                            "sectorSegments": [
                                _segment("OTP", "MGA", "2026-03-10T08:00:00", "2026-03-10T12:00:00")
                            ]
                        },
                    },
                    {
                        "price": {
                            "amount": "400",
                            "formattedValue": "400 RON",
                            "currency": {"code": "RON"},
                        },
                        "duration": 6500,
                        "bookingOptions": {"edges": [{"node": {"bookingUrl": "/best-oneway"}}]},
                        "sector": {
                            "sectorSegments": [
                                _segment("OTP", "MGA", "2026-03-10T08:00:00", "2026-03-10T11:50:00")
                            ]
                        },
                    },
                ]
            }
        }
    }
    best_oneway = oneway_client.get_best_oneway(
        "OTP",
        "MGA",
        "2026-03-10",
        "RON",
        1,
        1,
        0,
        0,
        max_connection_layover_seconds=3600,
    )
    assert best_oneway is not None
    assert best_oneway["price"] == 400
    assert best_oneway["stops"] == 0
    assert str(best_oneway["booking_url"]).endswith("/best-oneway")

    empty_oneway_client = KiwiClient()
    empty_oneway_client._post = lambda *_args, **_kwargs: {"data": {"onewayItineraries": {}}}  # type: ignore[assignment]
    assert (
        empty_oneway_client.get_best_oneway("OTP", "MGA", "2026-03-11", "RON", 1, 1, 0, 0) is None
    )

    return_client = KiwiClient()
    return_client._post = lambda *_args, **_kwargs: {  # type: ignore[assignment]
        "data": {
            "returnItineraries": {
                "itineraries": [
                    {"price": {"amount": ""}},
                    {
                        "price": {
                            "amount": "850",
                            "formattedValue": "850 RON",
                            "currency": {"code": "RON"},
                        },
                        "duration": 18000,
                        "bookingOptions": {"edges": [{"node": {"bookingUrl": "/too-many-return"}}]},
                        "outbound": {
                            "duration": 8000,
                            "sectorSegments": [
                                _segment(
                                    "OTP", "IST", "2026-03-10T08:00:00", "2026-03-10T09:00:00"
                                ),
                                _segment(
                                    "IST", "DOH", "2026-03-10T10:00:00", "2026-03-10T12:00:00"
                                ),
                                _segment(
                                    "DOH", "MGA", "2026-03-10T13:00:00", "2026-03-10T16:00:00"
                                ),
                            ],
                        },
                        "inbound": {
                            "duration": 9000,
                            "sectorSegments": [
                                _segment(
                                    "MGA", "IST", "2026-03-24T08:00:00", "2026-03-24T12:00:00"
                                ),
                                _segment(
                                    "IST", "OTP", "2026-03-24T13:00:00", "2026-03-24T15:00:00"
                                ),
                            ],
                        },
                    },
                    {
                        "price": {
                            "amount": "900",
                            "formattedValue": "900 RON",
                            "currency": {"code": "RON"},
                        },
                        "duration": 17000,
                        "bookingOptions": {"edges": [{"node": {"bookingUrl": "/slow-return"}}]},
                        "outbound": {
                            "duration": 8000,
                            "sectorSegments": [
                                _segment(
                                    "OTP", "IST", "2026-03-10T08:00:00", "2026-03-10T09:00:00"
                                ),
                                _segment(
                                    "IST", "MGA", "2026-03-10T12:30:00", "2026-03-10T17:00:00"
                                ),
                            ],
                        },
                        "inbound": {
                            "duration": 9000,
                            "sectorSegments": [
                                _segment("MGA", "OTP", "2026-03-24T08:00:00", "2026-03-24T12:00:00")
                            ],
                        },
                    },
                    {
                        "price": {
                            "amount": "900",
                            "formattedValue": "900 RON",
                            "currency": {"code": "RON"},
                        },
                        "duration": 16000,
                        "bookingOptions": {"edges": [{"node": {"bookingUrl": "/best-return"}}]},
                        "outbound": {
                            "duration": 7000,
                            "sectorSegments": [
                                _segment(
                                    "OTP", "IST", "2026-03-10T08:00:00", "2026-03-10T09:00:00"
                                ),
                                _segment(
                                    "IST", "MGA", "2026-03-10T09:45:00", "2026-03-10T13:00:00"
                                ),
                            ],
                        },
                        "inbound": {
                            "duration": 8000,
                            "sectorSegments": [
                                _segment("MGA", "OTP", "2026-03-24T08:00:00", "2026-03-24T12:00:00")
                            ],
                        },
                    },
                ]
            }
        }
    }
    best_return = return_client.get_best_return(
        "OTP",
        "MGA",
        "2026-03-10",
        "2026-03-24",
        "RON",
        1,
        1,
        0,
        0,
        max_connection_layover_seconds=3600,
    )
    assert best_return is not None
    assert best_return["price"] == 900
    assert best_return["outbound_stops"] == 1
    assert best_return["inbound_stops"] == 0
    assert str(best_return["booking_url"]).endswith("/best-return")

    empty_return_client = KiwiClient()
    empty_return_client._post = lambda *_args, **_kwargs: {"data": {"returnItineraries": {}}}  # type: ignore[assignment]
    assert (
        empty_return_client.get_best_return(
            "OTP", "MGA", "2026-03-10", "2026-03-24", "RON", 1, 1, 0, 0
        )
        is None
    )


def test_serpapi_client_internal_paths_cover_search_and_selection(monkeypatch) -> None:
    unconfigured = SerpApiGoogleFlightsClient(api_key="")
    assert (
        unconfigured.get_calendar_prices(
            "OTP", "MGA", "2026-03-10", "2026-03-12", "RON", 1, 1, 0, 0
        )
        == {}
    )
    assert unconfigured.get_best_oneway("OTP", "MGA", "2026-03-10", "RON", 1, 1, 0, 0) is None
    assert (
        unconfigured.get_best_return("OTP", "MGA", "2026-03-10", "2026-03-24", "RON", 1, 1, 0, 0)
        is None
    )

    client = SerpApiGoogleFlightsClient(
        api_key="key",
        search_url="https://serpapi.example/",
        return_option_scan_limit=9,
    )
    assert client._search_url == "https://serpapi.example"
    assert client._return_option_scan_limit == 5

    session = _FakeSession(
        get_responses=[
            _FakeResponse([]),
            _FakeResponse({"error": "quota reached"}),
            _FakeResponse({"error": "provider down"}, status_code=400),
            _FakeResponse({"search_metadata": {"google_flights_url": "https://ok.example"}}),
        ]
    )
    monkeypatch.setattr(client, "_session", lambda: session)
    monkeypatch.setattr(
        "src.providers.serpapi._capture_provider_response",
        lambda *args, **kwargs: None,
    )

    assert client._search({"departure_id": "OTP"}) == {}
    with pytest.raises(RuntimeError, match="quota reached"):
        client._search({"departure_id": "OTP"})
    with pytest.raises(requests.HTTPError):
        client._search({"departure_id": "OTP"})
    assert client._search({"departure_id": "OTP"}) == {
        "search_metadata": {"google_flights_url": "https://ok.example"}
    }

    assert client._option_segments(
        {
            "flights": [
                None,
                {
                    "departure_airport": {"id": "OTP", "name": "OTP", "time": "2026-03-10 08:00"},
                    "arrival_airport": {"id": "MGA", "name": "MGA", "time": "2026-03-10 12:00"},
                    "airline": "TK",
                },
            ]
        }
    ) == [
        {
            "from": "OTP",
            "to": "MGA",
            "from_name": "OTP",
            "to_name": "MGA",
            "depart_local": "2026-03-10T08:00:00",
            "arrive_local": "2026-03-10T12:00:00",
            "carrier": "TK",
            "carrier_name": "TK",
        }
    ]

    def _flight(
        source: str,
        destination: str,
        depart_time: str,
        arrive_time: str,
    ) -> dict[str, object]:
        return {
            "departure_airport": {"id": source, "name": source, "time": depart_time},
            "arrival_airport": {"id": destination, "name": destination, "time": arrive_time},
            "airline": "TK",
        }

    oneway_payload = {
        "search_metadata": {"raw_html_file": "https://google.example/oneway"},
        "best_flights": [
            {"price": "bad"},
            {"price": 600, "flights": []},
            {
                "price": 500,
                "total_duration": "6h 0m",
                "flights": [
                    _flight("OTP", "IST", "2026-03-10 08:00", "2026-03-10 09:00"),
                    _flight("IST", "MGA", "2026-03-10 12:30", "2026-03-10 15:00"),
                ],
            },
            {
                "price": 450,
                "total_duration": 180,
                "flights": [_flight("OTP", "MGA", "2026-03-10 08:00", "2026-03-10 11:00")],
            },
            {
                "price": 450,
                "total_duration": 240,
                "flights": [
                    _flight("OTP", "IST", "2026-03-10 08:00", "2026-03-10 09:00"),
                    _flight("IST", "MGA", "2026-03-10 09:45", "2026-03-10 12:00"),
                ],
            },
        ],
    }
    monkeypatch.setattr(client, "_search", lambda params: oneway_payload)
    best_oneway = client.get_best_oneway(
        "OTP",
        "MGA",
        "2026-03-10",
        "EUR",
        1,
        1,
        0,
        0,
        max_connection_layover_seconds=3600,
    )
    assert best_oneway is not None
    assert best_oneway["price"] == 450
    assert best_oneway["stops"] == 0
    assert best_oneway["booking_url"] == "https://google.example/oneway"

    return_payloads = {
        "base": {
            "search_metadata": {},
            "best_flights": [
                {
                    "price": 700,
                    "flights": [_flight("OTP", "MGA", "2026-03-10 08:00", "2026-03-10 12:00")],
                },
                {
                    "price": 850,
                    "departure_token": "tok-empty",
                    "flights": [
                        _flight("OTP", "IST", "2026-03-10 08:00", "2026-03-10 09:00"),
                        _flight("IST", "MGA", "2026-03-10 09:45", "2026-03-10 12:00"),
                    ],
                },
                {
                    "price": 900,
                    "departure_token": "tok-best",
                    "total_duration": 180,
                    "flights": [_flight("OTP", "MGA", "2026-03-10 08:00", "2026-03-10 11:00")],
                },
                {
                    "price": 950,
                    "departure_token": "tok-slower",
                    "total_duration": 240,
                    "flights": [
                        _flight("OTP", "IST", "2026-03-10 08:00", "2026-03-10 09:00"),
                        _flight("IST", "MGA", "2026-03-10 09:45", "2026-03-10 12:00"),
                    ],
                },
            ],
        },
        "tok-empty": {"best_flights": []},
        "tok-best": {
            "search_metadata": {"raw_html_file": "https://google.example/return"},
            "best_flights": [
                {
                    "total_duration": 200,
                    "flights": [_flight("MGA", "OTP", "2026-03-24 08:00", "2026-03-24 11:20")],
                }
            ],
        },
        "tok-slower": {
            "best_flights": [
                {
                    "price": 950,
                    "total_duration": 300,
                    "flights": [
                        _flight("MGA", "IST", "2026-03-24 08:00", "2026-03-24 09:00"),
                        _flight("IST", "OTP", "2026-03-24 09:40", "2026-03-24 13:00"),
                    ],
                }
            ],
        },
    }

    def _return_search(params: dict[str, object]) -> dict[str, object]:
        departure_token = str(params.get("departure_token") or "")
        return return_payloads[departure_token or "base"]

    monkeypatch.setattr(client, "_search", _return_search)
    client._return_option_scan_limit = 3
    best_return = client.get_best_return(
        "OTP",
        "MGA",
        "2026-03-10",
        "2026-03-24",
        "EUR",
        1,
        1,
        0,
        0,
        max_connection_layover_seconds=3600,
    )
    assert best_return is not None
    assert best_return["price"] == 900
    assert best_return["outbound_stops"] == 0
    assert best_return["inbound_stops"] == 0
    assert best_return["booking_url"] == "https://google.example/return"


def test_amadeus_client_internal_paths_cover_auth_retries_and_selection(monkeypatch) -> None:
    unconfigured = AmadeusClient(client_id="", client_secret="")
    assert (
        unconfigured.get_calendar_prices(
            "OTP", "MGA", "2026-03-10", "2026-03-12", "EUR", 1, 1, 0, 0
        )
        == {}
    )
    assert unconfigured.get_best_oneway("OTP", "MGA", "2026-03-10", "EUR", 1, 1, 0, 0) is None
    assert (
        unconfigured.get_best_return("OTP", "MGA", "2026-03-10", "2026-03-24", "EUR", 1, 1, 0, 0)
        is None
    )
    with pytest.raises(RuntimeError, match="credentials are missing"):
        unconfigured._fetch_token()

    missing_token_client = AmadeusClient(client_id="id", client_secret="secret")
    missing_token_session = _FakeSession(post_responses=[_FakeResponse({"expires_in": 120})])
    monkeypatch.setattr(missing_token_client, "_session", lambda: missing_token_session)
    with pytest.raises(RuntimeError, match="missing access_token"):
        missing_token_client._fetch_token()

    retry_client = AmadeusClient(
        client_id="id", client_secret="secret", base_url="https://amadeus.test"
    )
    retry_session = _FakeSession(
        post_responses=[
            _FakeResponse({"access_token": "token-1", "expires_in": 120}),
            _FakeResponse({"access_token": "token-2", "expires_in": 120}),
        ],
        get_responses=[
            _FakeResponse({}, status_code=401),
            _FakeResponse({}, status_code=503, headers={"Retry-After": "0"}),
            _FakeResponse({"data": []}),
        ],
    )
    monkeypatch.setattr(retry_client, "_session", lambda: retry_session)
    monkeypatch.setattr(
        "src.providers.amadeus._capture_provider_response",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr("src.providers.amadeus.time.sleep", lambda *_args, **_kwargs: None)
    assert retry_client._get("/v2/shopping/flight-offers", {"originLocationCode": "OTP"}) == {
        "data": []
    }
    assert len(retry_session.post_calls) == 2

    no_result_client = AmadeusClient(
        client_id="id", client_secret="secret", base_url="https://amadeus.test"
    )
    no_result_session = _FakeSession(
        post_responses=[_FakeResponse({"access_token": "token-1", "expires_in": 120})],
        get_responses=[
            _FakeResponse({"errors": [{"detail": "No flights found"}]}, status_code=422)
        ],
    )
    monkeypatch.setattr(no_result_client, "_session", lambda: no_result_session)
    monkeypatch.setattr(
        "src.providers.amadeus._capture_provider_response",
        lambda *args, **kwargs: None,
    )
    with pytest.raises(ProviderNoResultError):
        no_result_client._get("/v2/shopping/flight-offers", {"originLocationCode": "OTP"})

    search_client = AmadeusClient(client_id="id", client_secret="secret")
    captured_calls: list[tuple[str, dict[str, object]]] = []

    calendar_payload = {
        "data": [
            {"departureDate": "2026-03-10", "price": {"total": "510"}},
            {"departureDate": "2026-03-10", "price": {"total": "490"}},
            {"departureDate": "2026-03-11", "price": {"total": "bad"}},
        ]
    }
    oneway_payload = {
        "data": [
            {"price": {"grandTotal": "700", "currency": "EUR"}},
            {
                "price": {"grandTotal": "650", "currency": "EUR"},
                "itineraries": [
                    {
                        "duration": "PT8H",
                        "segments": [
                            {
                                "departure": {"iataCode": "OTP", "at": "2026-03-10T08:00:00"},
                                "arrival": {"iataCode": "IST", "at": "2026-03-10T09:00:00"},
                                "carrierCode": "TK",
                            },
                            {
                                "departure": {"iataCode": "IST", "at": "2026-03-10T13:30:00"},
                                "arrival": {"iataCode": "MGA", "at": "2026-03-10T18:00:00"},
                                "carrierCode": "TK",
                            },
                        ],
                    }
                ],
            },
            {
                "price": {"grandTotal": "620", "currency": "EUR"},
                "itineraries": [
                    {
                        "duration": "PT4H",
                        "segments": [
                            {
                                "departure": {"iataCode": "OTP", "at": "2026-03-10T08:00:00"},
                                "arrival": {"iataCode": "MGA", "at": "2026-03-10T12:00:00"},
                                "carrierCode": "TK",
                            }
                        ],
                    }
                ],
            },
            {
                "price": {"grandTotal": "620", "currency": "EUR"},
                "itineraries": [
                    {
                        "duration": "PT5H",
                        "segments": [
                            {
                                "departure": {"iataCode": "OTP", "at": "2026-03-10T08:00:00"},
                                "arrival": {"iataCode": "IST", "at": "2026-03-10T09:00:00"},
                                "carrierCode": "TK",
                            },
                            {
                                "departure": {"iataCode": "IST", "at": "2026-03-10T09:45:00"},
                                "arrival": {"iataCode": "MGA", "at": "2026-03-10T12:00:00"},
                                "carrierCode": "TK",
                            },
                        ],
                    }
                ],
            },
        ]
    }
    return_payload = {
        "data": [
            {
                "price": {"grandTotal": "940", "currency": "EUR"},
                "itineraries": [
                    {
                        "duration": "PT5H",
                        "segments": [
                            {
                                "departure": {"iataCode": "OTP", "at": "2026-03-10T08:00:00"},
                                "arrival": {"iataCode": "MGA", "at": "2026-03-10T12:00:00"},
                                "carrierCode": "TK",
                            }
                        ],
                    }
                ],
            },
            {
                "price": {"grandTotal": "900", "currency": "EUR"},
                "itineraries": [
                    {
                        "duration": "PT4H",
                        "segments": [
                            {
                                "departure": {"iataCode": "OTP", "at": "2026-03-10T08:00:00"},
                                "arrival": {"iataCode": "MGA", "at": "2026-03-10T12:00:00"},
                                "carrierCode": "TK",
                            }
                        ],
                    },
                    {
                        "duration": "PT5H",
                        "segments": [
                            {
                                "departure": {"iataCode": "MGA", "at": "2026-03-24T08:00:00"},
                                "arrival": {"iataCode": "IST", "at": "2026-03-24T09:00:00"},
                                "carrierCode": "TK",
                            },
                            {
                                "departure": {"iataCode": "IST", "at": "2026-03-24T09:40:00"},
                                "arrival": {"iataCode": "OTP", "at": "2026-03-24T12:00:00"},
                                "carrierCode": "TK",
                            },
                        ],
                    },
                ],
            },
            {
                "price": {"grandTotal": "900", "currency": "EUR"},
                "itineraries": [
                    {
                        "duration": "PT5H",
                        "segments": [
                            {
                                "departure": {"iataCode": "OTP", "at": "2026-03-10T08:00:00"},
                                "arrival": {"iataCode": "IST", "at": "2026-03-10T09:00:00"},
                                "carrierCode": "TK",
                            },
                            {
                                "departure": {"iataCode": "IST", "at": "2026-03-10T12:30:00"},
                                "arrival": {"iataCode": "MGA", "at": "2026-03-10T15:00:00"},
                                "carrierCode": "TK",
                            },
                        ],
                    },
                    {
                        "duration": "PT4H",
                        "segments": [
                            {
                                "departure": {"iataCode": "MGA", "at": "2026-03-24T08:00:00"},
                                "arrival": {"iataCode": "OTP", "at": "2026-03-24T12:00:00"},
                                "carrierCode": "TK",
                            }
                        ],
                    },
                ],
            },
        ]
    }

    def _fake_get(path: str, params: dict[str, object]) -> dict[str, object]:
        captured_calls.append((path, dict(params)))
        if "flight-dates" in path:
            return calendar_payload
        if "returnDate" in params:
            return return_payload
        return oneway_payload

    monkeypatch.setattr(search_client, "_get", _fake_get)
    monkeypatch.setattr(search_client, "is_configured", lambda: True)
    prices = search_client.get_calendar_prices(
        "OTP", "MGA", "2026-03-10", "2026-03-11", "EUR", 0, 1, 0, 0
    )
    best_oneway = search_client.get_best_oneway(
        "OTP", "MGA", "2026-03-10", "EUR", 1, 1, 0, 0, max_connection_layover_seconds=3600
    )
    best_return = search_client.get_best_return(
        "OTP",
        "MGA",
        "2026-03-10",
        "2026-03-24",
        "EUR",
        1,
        1,
        0,
        0,
        max_connection_layover_seconds=3600,
    )

    assert prices == {"2026-03-10": 490}
    assert best_oneway is not None
    assert best_oneway["price"] == 620
    assert best_oneway["stops"] == 0
    assert best_return is not None
    assert best_return["price"] == 900
    assert best_return["outbound_stops"] == 0
    assert best_return["inbound_stops"] == 1
    assert any(
        path.endswith("/v1/shopping/flight-dates") and params.get("nonStop") == "true"
        for path, params in captured_calls
    )


def test_kiwi_client_internal_paths_cover_empty_results_invalid_amounts_and_tiebreaks() -> None:
    client = KiwiClient()
    assert client._session() is client._session()
    assert client._passengers_payload(
        type("P", (), {"adults": 0, "hand_bags": -1, "hold_bags": -2})()
    ) == {  # type: ignore[arg-type]
        "adults": 1,
        "adultsHandBags": [0],
        "adultsHoldBags": [0],
    }

    empty_oneway_client = KiwiClient()
    empty_oneway_payloads = iter(
        [
            {"data": {"onewayItineraries": []}},
            {"data": {"onewayItineraries": {"itineraries": []}}},
        ]
    )
    empty_oneway_client._post = lambda query, variables: next(empty_oneway_payloads)  # type: ignore[assignment]
    assert (
        empty_oneway_client.get_best_oneway("OTP", "MGA", "2026-03-10", "RON", 1, 1, 0, 0) is None
    )
    assert (
        empty_oneway_client.get_best_oneway("OTP", "MGA", "2026-03-11", "RON", 1, 1, 0, 0) is None
    )

    best_oneway_client = KiwiClient()
    best_oneway_client._post = lambda query, variables: {  # type: ignore[assignment]
        "data": {
            "onewayItineraries": {
                "itineraries": [
                    {"price": {"amount": None}},
                    {"price": {"amount": "bad"}},
                    {
                        "price": {
                            "amount": "500",
                            "formattedValue": "500 RON",
                            "currency": {"code": "RON"},
                        },
                        "duration": 2000,
                        "sector": {
                            "sectorSegments": [
                                _kiwi_segment(
                                    "OTP",
                                    "IST",
                                    "2026-03-10T08:00:00",
                                    "2026-03-10T09:00:00",
                                    "TK",
                                ),
                                _kiwi_segment(
                                    "IST",
                                    "MGA",
                                    "2026-03-10T09:30:00",
                                    "2026-03-10T12:00:00",
                                    "TK",
                                ),
                            ]
                        },
                    },
                    {
                        "price": {
                            "amount": "500",
                            "formattedValue": "500 RON",
                            "currency": {"code": "RON"},
                        },
                        "duration": 1800,
                        "sector": {
                            "sectorSegments": [
                                _kiwi_segment(
                                    "OTP",
                                    "MGA",
                                    "2026-03-10T08:00:00",
                                    "2026-03-10T10:00:00",
                                    "TK",
                                )
                            ]
                        },
                    },
                ]
            }
        }
    }
    best_oneway = best_oneway_client.get_best_oneway(
        "OTP",
        "MGA",
        "2026-03-10",
        "RON",
        1,
        1,
        0,
        0,
    )
    assert best_oneway is not None
    assert best_oneway["price"] == 500
    assert best_oneway["stops"] == 0

    empty_return_client = KiwiClient()
    empty_return_payloads = iter(
        [
            {"data": {"returnItineraries": []}},
            {"data": {"returnItineraries": {"itineraries": []}}},
        ]
    )
    empty_return_client._post = lambda query, variables: next(empty_return_payloads)  # type: ignore[assignment]
    assert (
        empty_return_client.get_best_return(
            "OTP", "MGA", "2026-03-10", "2026-03-24", "RON", 1, 1, 0, 0
        )
        is None
    )
    assert (
        empty_return_client.get_best_return(
            "OTP", "MGA", "2026-03-11", "2026-03-25", "RON", 1, 1, 0, 0
        )
        is None
    )

    best_return_client = KiwiClient()
    best_return_client._post = lambda query, variables: {  # type: ignore[assignment]
        "data": {
            "returnItineraries": {
                "itineraries": [
                    {"price": {"amount": None}},
                    {"price": {"amount": "bad"}},
                    {
                        "price": {
                            "amount": "900",
                            "formattedValue": "900 RON",
                            "currency": {"code": "RON"},
                        },
                        "duration": 4000,
                        "outbound": {
                            "duration": 2000,
                            "sectorSegments": [
                                _kiwi_segment(
                                    "OTP",
                                    "IST",
                                    "2026-03-10T08:00:00",
                                    "2026-03-10T09:00:00",
                                    "TK",
                                ),
                                _kiwi_segment(
                                    "IST",
                                    "MGA",
                                    "2026-03-10T09:30:00",
                                    "2026-03-10T12:00:00",
                                    "TK",
                                ),
                            ],
                        },
                        "inbound": {
                            "duration": 2000,
                            "sectorSegments": [
                                _kiwi_segment(
                                    "MGA",
                                    "IST",
                                    "2026-03-24T08:00:00",
                                    "2026-03-24T09:00:00",
                                    "TK",
                                ),
                                _kiwi_segment(
                                    "IST",
                                    "OTP",
                                    "2026-03-24T09:20:00",
                                    "2026-03-24T12:00:00",
                                    "TK",
                                ),
                            ],
                        },
                    },
                    {
                        "price": {
                            "amount": "900",
                            "formattedValue": "900 RON",
                            "currency": {"code": "RON"},
                        },
                        "duration": 3500,
                        "outbound": {
                            "duration": 1500,
                            "sectorSegments": [
                                _kiwi_segment(
                                    "OTP",
                                    "MGA",
                                    "2026-03-10T08:00:00",
                                    "2026-03-10T10:00:00",
                                    "TK",
                                )
                            ],
                        },
                        "inbound": {
                            "duration": 1500,
                            "sectorSegments": [
                                _kiwi_segment(
                                    "MGA",
                                    "OTP",
                                    "2026-03-24T08:00:00",
                                    "2026-03-24T10:00:00",
                                    "TK",
                                )
                            ],
                        },
                    },
                ]
            }
        }
    }
    best_return = best_return_client.get_best_return(
        "OTP",
        "MGA",
        "2026-03-10",
        "2026-03-24",
        "RON",
        1,
        1,
        0,
        0,
    )
    assert best_return is not None
    assert best_return["price"] == 900
    assert best_return["outbound_stops"] == 0
    assert best_return["inbound_stops"] == 0


def test_multi_provider_client_remaining_edge_paths_cover_calendar_filters_and_tiebreaks(
    monkeypatch,
) -> None:
    class _Provider:
        def __init__(
            self,
            provider_id: str,
            *,
            supports_calendar: bool = True,
            calendar_result: dict[str, object] | None = None,
            oneway_result: dict[str, object] | None = None,
            return_result: dict[str, object] | None = None,
            calendar_error: Exception | None = None,
        ) -> None:
            self.provider_id = provider_id
            self.supports_calendar = supports_calendar
            self._calendar_result = calendar_result or {}
            self._oneway_result = oneway_result
            self._return_result = return_result
            self._calendar_error = calendar_error

        def get_calendar_prices(self, **kwargs: object) -> dict[str, object]:
            if self._calendar_error is not None:
                raise self._calendar_error
            return dict(self._calendar_result)

        def get_best_oneway(self, **kwargs: object) -> dict[str, object] | None:
            return self._oneway_result

        def get_best_return(self, **kwargs: object) -> dict[str, object] | None:
            return self._return_result

    paused = _Provider("serpapi", calendar_result={"2026-03-10": 999})
    no_calendar = _Provider("googleflights", supports_calendar=False)
    no_result = _Provider(
        "amadeus",
        calendar_error=ProviderNoResultError("no calendar result"),
    )
    active = _Provider(
        "kiwi",
        calendar_result={"2026-03-10": "300", "2026-03-11": "bad"},
        oneway_result={"price": 500, "stops": 1, "duration_seconds": 6000},
        return_result={
            "price": 900,
            "outbound_stops": 1,
            "inbound_stops": 1,
            "duration_seconds": None,
        },
    )
    tie_breaker = _Provider(
        "kayak",
        oneway_result={"price": 500, "stops": 1, "duration_seconds": 5000},
        return_result={
            "price": 900,
            "outbound_stops": 1,
            "inbound_stops": 1,
            "duration_seconds": 7000,
        },
    )
    direct = _Provider(
        "momondo",
        oneway_result={"price": 500, "stops": 0, "duration_seconds": 9000},
        return_result={
            "price": 900,
            "outbound_stops": 0,
            "inbound_stops": 0,
            "duration_seconds": 8000,
        },
    )

    client = MultiProviderClient(
        [paused, no_calendar, no_result, active, tie_breaker, direct],
        max_total_calls=10,
        max_calls_by_provider={"kiwi": 2, "kayak": 2, "momondo": 2},
    )
    monkeypatch.setattr(
        client,
        "_provider_pause_remaining_seconds",
        lambda provider_id: 5 if provider_id == "serpapi" else 0,
    )
    monkeypatch.setattr("src.providers.multi.log_event", lambda *args, **kwargs: None)

    prices = client.get_calendar_prices(
        "OTP",
        "MGA",
        "2026-03-10",
        "2026-03-12",
        "EUR",
        1,
        1,
        0,
        0,
        provider_ids=("", "serpapi", "googleflights", "amadeus", "kiwi"),
    )
    assert prices == {"2026-03-10": 300}
    assert client._providers_for_selection(("", "   ")) == ()
    assert client._providers_for_selection((None,)) == client.providers
    assert client._is_better_oneway(
        {"price": 500, "stops": 1, "duration_seconds": 4000},
        {"price": 500, "stops": 1, "duration_seconds": 5000},
    )
    assert client._is_better_return(
        {"price": 900, "outbound_stops": 1, "inbound_stops": 1, "duration_seconds": 5000},
        {"price": 900, "outbound_stops": 1, "inbound_stops": 1, "duration_seconds": 7000},
    )

    best_oneway = client.get_best_oneway("OTP", "MGA", "2026-03-10", "EUR", 2, 1, 0, 0)
    best_return = client.get_best_return(
        "OTP",
        "MGA",
        "2026-03-10",
        "2026-03-24",
        "EUR",
        2,
        1,
        0,
        0,
    )
    assert best_oneway is not None
    assert best_oneway["provider"] == "momondo"
    assert best_return is not None
    assert best_return["provider"] == "momondo"


def test_multi_provider_health_snapshot_reports_selected_no_result_errors_and_listener() -> None:
    client = MultiProviderClient(
        providers=[
            _StubProvider("kiwi"),
            _StubProvider("kayak"),
            _StubProvider("momondo"),
            _StubProvider("googleflights"),
        ]
    )
    snapshots: list[dict[str, object]] = []
    client.set_stats_listener(lambda snapshot: snapshots.append(snapshot), min_interval_seconds=0.0)
    client._bump("calendar_selected", "kiwi", 2)
    client._bump("oneway_calls", "kiwi", 3)
    client._bump("oneway_selected", "kiwi", 1)
    client._bump("return_calls", "kayak", 2)
    client._bump("return_no_result", "kayak", 2)
    client._bump("calendar_calls", "momondo", 1)
    client._bump("calendar_errors", "momondo", 1)
    client._bump("calendar_skipped_budget", "momondo", 1)
    client._register_provider_block(
        "googleflights",
        ProviderBlockedError(
            "Google Flights returned a consent or challenge page instead of fares.",
            manual_search_url="https://www.google.com/travel/flights",
            cooldown_seconds=45,
        ),
    )
    client._bump("oneway_blocked", "googleflights", 2)

    snapshot = client.health_snapshot()

    assert snapshot["providers"]["kiwi"]["status"] == "selected"
    assert snapshot["providers"]["kiwi"]["selected"] == 3
    assert snapshot["providers"]["kiwi"]["calls"] == 3
    assert snapshot["providers"]["kayak"]["status"] == "no_result"
    assert snapshot["providers"]["kayak"]["no_result"] == 2
    assert snapshot["providers"]["momondo"]["status"] == "error"
    assert snapshot["providers"]["momondo"]["errors"] == 1
    assert snapshot["providers"]["momondo"]["skipped_budget"] == 1
    assert snapshot["providers"]["googleflights"]["status"] == "blocked"
    assert snapshot["providers"]["googleflights"]["blocked"] == 2
    assert (
        snapshot["providers"]["googleflights"]["manual_search_url"]
        == "https://www.google.com/travel/flights"
    )
    assert snapshot["providers"]["googleflights"]["cooldown_seconds"] > 0
    assert snapshots
    assert snapshots[-1]["providers"]["kiwi"]["selected"] == 3

    throttled_snapshots: list[dict[str, object]] = []
    client.set_stats_listener(
        lambda snapshot: throttled_snapshots.append(snapshot),
        min_interval_seconds=999.0,
    )
    first_count = len(throttled_snapshots)
    client._notify_stats_listener()
    assert len(throttled_snapshots) == first_count

    client.set_stats_listener(
        lambda _snapshot: (_ for _ in ()).throw(RuntimeError("listener boom")),
        min_interval_seconds=0.0,
    )
    client._notify_stats_listener(force=True)


def test_multi_provider_client_serializes_marked_provider_requests() -> None:
    state = {"active": 0, "max_active": 0}
    first_entered = threading.Event()
    release_first = threading.Event()

    class _SerializedProvider:
        provider_id = "kayak"
        supports_calendar = False
        serialized_requests = True
        request_interval_seconds = 0.0

        def get_best_oneway(self, **kwargs: object) -> dict[str, object] | None:
            state["active"] += 1
            state["max_active"] = max(state["max_active"], state["active"])
            departure_iso = str(kwargs.get("departure_iso") or "")
            try:
                if departure_iso == "2026-03-10":
                    first_entered.set()
                    release_first.wait(timeout=2.0)
                return {"price": 100, "stops": 0, "duration_seconds": 1000}
            finally:
                state["active"] -= 1

        def get_best_return(self, **kwargs: object) -> dict[str, object] | None:
            return None

    client = MultiProviderClient([_SerializedProvider()])

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(
            client.get_best_oneway,
            "OTP",
            "MGA",
            "2026-03-10",
            "RON",
            2,
            1,
            0,
            0,
        )
        assert first_entered.wait(timeout=1.0)
        second = executor.submit(
            client.get_best_oneway,
            "OTP",
            "MGA",
            "2026-03-11",
            "RON",
            2,
            1,
            0,
            0,
        )
        time.sleep(0.1)
        assert state["max_active"] == 1
        release_first.set()
        assert first.result(timeout=2.0) is not None
        assert second.result(timeout=2.0) is not None

    assert state["max_active"] == 1


def test_multi_provider_client_retries_serialized_provider_before_global_block_cooldown(
    monkeypatch,
) -> None:
    class _SerializedBlockingProvider:
        provider_id = "kayak"
        supports_calendar = False
        serialized_requests = True
        request_interval_seconds = 0.0

        def __init__(self) -> None:
            self.calls = 0

        def get_best_oneway(self, **kwargs: object) -> dict[str, object] | None:
            self.calls += 1
            if self.calls == 1:
                raise ProviderBlockedError(
                    "Kayak blocked automated scraping (captcha/anti-bot challenge).",
                    cooldown_seconds=120,
                )
            return {"price": 100, "stops": 0, "duration_seconds": 1000}

        def get_best_return(self, **kwargs: object) -> dict[str, object] | None:
            return None

    provider = _SerializedBlockingProvider()
    client = MultiProviderClient([provider])
    monkeypatch.setattr("src.providers.multi.log_event", lambda *args, **kwargs: None)

    first = client.get_best_oneway("OTP", "MGA", "2026-03-10", "RON", 2, 1, 0, 0)
    second = client.get_best_oneway("OTP", "MGA", "2026-03-11", "RON", 2, 1, 0, 0)
    stats = client.stats_snapshot()

    assert first is None
    assert second is not None
    assert second["provider"] == "kayak"
    assert provider.calls == 2
    assert client._provider_pause_remaining_seconds("kayak") == 0
    assert stats["oneway_calls"]["kayak"] == 2
    assert stats["oneway_blocked"]["kayak"] == 1
    assert stats["oneway_skipped_cooldown"].get("kayak", 0) == 0


def test_multi_provider_client_pauses_serialized_provider_after_repeated_blocks(
    monkeypatch,
) -> None:
    class _SerializedBlockingProvider:
        provider_id = "kayak"
        supports_calendar = False
        serialized_requests = True
        request_interval_seconds = 0.0

        def __init__(self) -> None:
            self.calls = 0

        def get_best_oneway(self, **kwargs: object) -> dict[str, object] | None:
            self.calls += 1
            raise ProviderBlockedError(
                "Kayak blocked automated scraping (captcha/anti-bot challenge).",
                cooldown_seconds=120,
            )

        def get_best_return(self, **kwargs: object) -> dict[str, object] | None:
            return None

    provider = _SerializedBlockingProvider()
    client = MultiProviderClient([provider])
    monkeypatch.setattr("src.providers.multi.log_event", lambda *args, **kwargs: None)

    for departure_iso in ("2026-03-10", "2026-03-11", "2026-03-12"):
        assert client.get_best_oneway("OTP", "MGA", departure_iso, "RON", 2, 1, 0, 0) is None

    assert provider.calls == 3
    assert client._provider_pause_remaining_seconds("kayak") > 0
    assert client.get_best_oneway("OTP", "MGA", "2026-03-13", "RON", 2, 1, 0, 0) is None

    stats = client.stats_snapshot()
    assert provider.calls == 3
    assert stats["oneway_blocked"]["kayak"] == 3
    assert stats["oneway_skipped_cooldown"]["kayak"] == 1
