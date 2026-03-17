from __future__ import annotations

import pytest
import requests

from flight_layover_lab.exceptions import ProviderNoResultError
from flight_layover_lab.providers.amadeus import AmadeusClient
from flight_layover_lab.providers.kiwi import KiwiClient
from flight_layover_lab.providers.multi import MultiProviderClient
from flight_layover_lab.providers.serpapi import SerpApiGoogleFlightsClient


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
        "flight_layover_lab.providers.amadeus._capture_provider_response",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "flight_layover_lab.providers.amadeus.time.sleep", lambda *_args, **_kwargs: None
    )

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
        "flight_layover_lab.providers.serpapi._capture_provider_response",
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
    monkeypatch.setattr(
        "flight_layover_lab.providers.multi.log_event", lambda *args, **kwargs: None
    )

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
