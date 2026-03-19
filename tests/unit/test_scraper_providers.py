import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import src.providers.kayak as kayak_provider_module  # noqa: E402
from server import (  # noqa: E402
    ALLOW_PLAYWRIGHT_PROVIDERS,
    GoogleFlightsLocalClient,
    KayakScrapeClient,
    MomondoScrapeClient,
    ProviderNoResultError,
    SkyscannerScrapeClient,
    parse_google_flights_text_datetime,
)


class GoogleFlightsClientTests(unittest.TestCase):
    def test_local_mode_is_downgraded_when_playwright_providers_disabled(self) -> None:
        client = GoogleFlightsLocalClient(fetch_mode="local")
        if ALLOW_PLAYWRIGHT_PROVIDERS:
            self.assertEqual(client._fetch_mode, "local")
        else:
            self.assertEqual(client._fetch_mode, "common")

    def test_parse_google_datetime_with_date_hint(self) -> None:
        parsed = parse_google_flights_text_datetime("5:50 AM on Thu, Mar 12", "2026-03-12")
        self.assertEqual(parsed, "2026-03-12T05:50:00")

    def test_get_best_oneway_prefers_cheaper_and_fewer_stops(self) -> None:
        client = GoogleFlightsLocalClient()
        client.is_configured = lambda: True  # type: ignore[assignment]
        client._fetch_flights = lambda **kwargs: [  # type: ignore[assignment]
            SimpleNamespace(price=2100, stops=0, duration="3h 30m", name="A"),
            SimpleNamespace(price=1900, stops=2, duration="14h 10m", name="B"),
            SimpleNamespace(price=1900, stops=1, duration="11h 45m", name="C"),
        ]

        best = client.get_best_oneway(
            source="OTP",
            destination="MGA",
            departure_iso="2026-03-10",
            currency="RON",
            max_stops_per_leg=2,
            adults=1,
            hand_bags=0,
            hold_bags=0,
        )

        self.assertIsNotNone(best)
        assert best is not None
        self.assertEqual(best["price"], 1900)
        self.assertEqual(best["stops"], 1)
        self.assertEqual(best["provider"], "googleflights")

    def test_get_best_return_sums_both_legs(self) -> None:
        client = GoogleFlightsLocalClient()
        client.is_configured = lambda: True  # type: ignore[assignment]

        def fake_best_oneway(**kwargs):  # type: ignore[no-untyped-def]
            source = kwargs["source"]
            destination = kwargs["destination"]
            if source == "OTP" and destination == "MGA":
                return {
                    "price": 2400,
                    "duration_seconds": 18_000,
                    "stops": 1,
                    "transfer_events": 1,
                    "segments": [{"from": "OTP", "to": "MGA"}],
                    "booking_url": "https://google.example/out",
                    "currency": "RON",
                    "provider": "googleflights",
                }
            return {
                "price": 2100,
                "duration_seconds": 16_000,
                "stops": 1,
                "transfer_events": 1,
                "segments": [{"from": "MGA", "to": "OTP"}],
                "booking_url": "https://google.example/in",
                "currency": "RON",
                "provider": "googleflights",
            }

        client.get_best_oneway = fake_best_oneway  # type: ignore[assignment]

        best = GoogleFlightsLocalClient.get_best_return(
            client,
            source="OTP",
            destination="MGA",
            outbound_iso="2026-03-10",
            inbound_iso="2026-03-24",
            currency="RON",
            max_stops_per_leg=2,
            adults=1,
            hand_bags=0,
            hold_bags=0,
        )

        self.assertIsNotNone(best)
        assert best is not None
        self.assertEqual(best["price"], 4500)
        self.assertEqual(best["outbound_stops"], 1)
        self.assertEqual(best["inbound_stops"], 1)


class SkyscannerScraperTests(unittest.TestCase):
    def setUp(self) -> None:
        SkyscannerScrapeClient._PROVIDER_COOLDOWN_UNTIL = 0.0
        SkyscannerScrapeClient._PLAYWRIGHT_COOLDOWN_UNTIL = 0.0

    def test_hosts_to_try_deduplicates_candidates(self) -> None:
        client = SkyscannerScrapeClient(
            host="www.skyscanner.com",
            host_candidates=["www.skyscanner.com", "www.skyscanner.net", "www.skyscanner.com"],
            playwright_fallback=False,
        )
        hosts = client._hosts_to_try()
        self.assertEqual(hosts[0], "www.skyscanner.com")
        self.assertIn("www.skyscanner.net", hosts)
        self.assertEqual(len(hosts), len(set(hosts)))

    def test_fetch_search_html_rotates_host_after_captcha(self) -> None:
        client = SkyscannerScrapeClient(
            host="www.skyscanner.com",
            host_candidates=["www.skyscanner.net"],
            http_retries=1,
            playwright_fallback=False,
        )
        calls: list[str] = []

        def fake_http(url: str, attempt_idx: int = 0):
            calls.append(url)
            if "skyscanner.com" in url:
                return "captcha challenge", url, 403
            return '{"rawPrice":3908,"stops":1}', url, 200

        client._http_fetch_search_html = fake_http  # type: ignore[assignment]

        best = client.get_best_oneway(
            source="OTP",
            destination="MRU",
            departure_iso="2026-04-21",
            currency="RON",
            max_stops_per_leg=2,
            adults=1,
            hand_bags=0,
            hold_bags=0,
        )

        self.assertIsNotNone(best)
        assert best is not None
        self.assertEqual(best["price"], 3908)
        self.assertTrue(any("skyscanner.net" in call for call in calls))

    def test_fetch_search_html_all_blocked_raises_no_result(self) -> None:
        client = SkyscannerScrapeClient(
            host="www.skyscanner.com",
            host_candidates=["www.skyscanner.net"],
            http_retries=1,
            playwright_fallback=False,
        )
        client._http_fetch_search_html = (  # type: ignore[assignment]
            lambda url, attempt_idx=0: ("captcha-v2", url, 429)
        )

        with self.assertRaises(ProviderNoResultError):
            client.get_best_return(
                source="OTP",
                destination="SEZ",
                outbound_iso="2026-04-20",
                inbound_iso="2026-04-27",
                currency="RON",
                max_stops_per_leg=2,
                adults=1,
                hand_bags=0,
                hold_bags=0,
            )

    def test_playwright_fallback_used_when_http_blocked(self) -> None:
        client = SkyscannerScrapeClient(
            host="www.skyscanner.com",
            host_candidates=["www.skyscanner.net"],
            http_retries=1,
            playwright_fallback=True,
        )
        client._http_fetch_search_html = (  # type: ignore[assignment]
            lambda url, attempt_idx=0: ("captcha", url, 403)
        )
        client._fetch_search_html_playwright = (  # type: ignore[assignment]
            lambda url: ('{"rawPrice":3550,"stops":0}', url)
        )

        best = client.get_best_return(
            source="OTP",
            destination="SEZ",
            outbound_iso="2026-04-20",
            inbound_iso="2026-04-27",
            currency="RON",
            max_stops_per_leg=2,
            adults=1,
            hand_bags=0,
            hold_bags=0,
        )

        self.assertIsNotNone(best)
        assert best is not None
        self.assertEqual(best["price"], 3550)
        self.assertEqual(best["outbound_stops"], 0)

    def test_block_sets_provider_cooldown_and_skips_network_temporarily(self) -> None:
        client = SkyscannerScrapeClient(
            host="www.skyscanner.com",
            host_candidates=["www.skyscanner.net"],
            http_retries=1,
            playwright_fallback=False,
        )
        call_counter = {"count": 0}

        def fake_http(url: str, attempt_idx: int = 0):
            call_counter["count"] += 1
            return "captcha challenge", url, 403

        client._http_fetch_search_html = fake_http  # type: ignore[assignment]

        with self.assertRaises(ProviderNoResultError):
            client.get_best_oneway(
                source="OTP",
                destination="MRU",
                departure_iso="2026-04-21",
                currency="RON",
                max_stops_per_leg=2,
                adults=1,
                hand_bags=0,
                hold_bags=0,
            )
        first_calls = call_counter["count"]
        self.assertGreater(first_calls, 0)

        # Second call should short-circuit because provider cooldown is active.
        with self.assertRaises(ProviderNoResultError):
            client.get_best_oneway(
                source="OTP",
                destination="MRU",
                departure_iso="2026-04-22",
                currency="RON",
                max_stops_per_leg=2,
                adults=1,
                hand_bags=0,
                hold_bags=0,
            )
        self.assertEqual(call_counter["count"], first_calls)

    def test_extract_offer_options_from_json_script(self) -> None:
        html = """
        <html><head></head><body>
          <script id="__NEXT_DATA__" type="application/json">
            {
              "props": {
                "pageProps": {
                  "offers": [
                    {
                      "agent": {"name": "Vayama"},
                      "pricing": {"rawPrice": 444, "currency": "EUR"},
                      "deeplink": "https://book.example/vayama"
                    },
                    {
                      "agent": {"name": "Mytrip"},
                      "pricing": {"rawPrice": 447, "currency": "EUR"},
                      "deeplink": "https://book.example/mytrip"
                    }
                  ]
                }
              }
            }
          </script>
        </body></html>
        """
        offers = SkyscannerScrapeClient._extract_offer_options(html)
        self.assertEqual(len(offers), 2)
        self.assertEqual(offers[0]["provider"], "Vayama")
        self.assertEqual(offers[0]["price"], 444)
        self.assertEqual(offers[0]["currency"], "EUR")
        self.assertEqual(offers[0]["booking_url"], "https://book.example/vayama")

    def test_get_best_oneway_prefers_cheapest_ota_offer(self) -> None:
        client = SkyscannerScrapeClient(
            host="www.skyscanner.com",
            host_candidates=["www.skyscanner.net"],
            http_retries=1,
            playwright_fallback=False,
        )
        client._http_fetch_search_html = (  # type: ignore[assignment]
            lambda url, attempt_idx=0: (
                """
                <script id=\"__NEXT_DATA__\" type=\"application/json\">
                {
                  "offers": [
                    {"providerName":"Booking.com","rawPrice":452,"currency":"EUR","deeplink":"https://book.example/booking"},
                    {"providerName":"Vayama","rawPrice":444,"currency":"EUR","deeplink":"https://book.example/vayama"},
                    {"providerName":"Mytrip","rawPrice":447,"currency":"EUR","deeplink":"https://book.example/mytrip"}
                  ],
                  "stops":1
                }
                </script>
                """,
                url,
                200,
            )
        )

        best = client.get_best_oneway(
            source="OTP",
            destination="MRU",
            departure_iso="2026-04-21",
            currency="EUR",
            max_stops_per_leg=2,
            adults=1,
            hand_bags=0,
            hold_bags=0,
        )

        self.assertIsNotNone(best)
        assert best is not None
        self.assertEqual(best["price"], 444)
        self.assertEqual(best["booking_provider"], "Vayama")
        self.assertEqual(best["booking_url"], "https://book.example/vayama")
        offers = best.get("offer_options") or []
        self.assertGreaterEqual(len(offers), 3)
        self.assertEqual(offers[0]["provider"], "Vayama")
        self.assertEqual(offers[1]["provider"], "Mytrip")
        self.assertEqual(offers[2]["provider"], "Booking.com")


class KayakAndMomondoScraperTests(unittest.TestCase):
    def setUp(self) -> None:
        KayakScrapeClient._BROWSER_ASSIST_COOKIES = {}
        MomondoScrapeClient._BROWSER_ASSIST_COOKIES = {}

    def _sample_payload(self, display_price: dict | None = None) -> dict:
        if display_price is None:
            display_price = {"price": "5521", "currency": "RON"}
        return {
            "status": "complete",
            "results": [
                {
                    "type": "core",
                    "legs": [{"id": "L1"}, {"id": "L2"}],
                    "bookingOptions": [
                        {
                            "providerCode": "OTA1",
                            "displayPrice": display_price,
                            "bookingUrl": {"url": "/book/deal-1"},
                        }
                    ],
                }
            ],
            "legs": {
                "L1": {"segments": ["S1", "S2", "S3"], "duration": 1270},
                "L2": {"segments": ["S4", "S5", "S6"], "duration": 1375},
            },
            "segments": {
                "S1": {
                    "origin": "OTP",
                    "destination": "CDG",
                    "departure": "2026-03-10T08:40:00",
                    "arrival": "2026-03-10T11:00:00",
                    "airline": "RO",
                },
                "S2": {
                    "origin": "CDG",
                    "destination": "PTY",
                    "departure": "2026-03-10T15:10:00",
                    "arrival": "2026-03-10T20:10:00",
                    "airline": "AF",
                },
                "S3": {
                    "origin": "PTY",
                    "destination": "MGA",
                    "departure": "2026-03-10T21:26:00",
                    "arrival": "2026-03-10T22:11:00",
                    "airline": "CM",
                },
                "S4": {
                    "origin": "MGA",
                    "destination": "PTY",
                    "departure": "2026-03-24T14:06:00",
                    "arrival": "2026-03-24T16:44:00",
                    "airline": "CM",
                },
                "S5": {
                    "origin": "PTY",
                    "destination": "AMS",
                    "departure": "2026-03-24T18:45:00",
                    "arrival": "2026-03-25T11:10:00",
                    "airline": "KL",
                },
                "S6": {
                    "origin": "AMS",
                    "destination": "OTP",
                    "departure": "2026-03-25T13:25:00",
                    "arrival": "2026-03-25T17:10:00",
                    "airline": "KL",
                },
            },
            "airports": {
                "OTP": {"displayName": "OTP"},
                "CDG": {"displayName": "CDG"},
                "PTY": {"displayName": "PTY"},
                "MGA": {"displayName": "MGA"},
                "AMS": {"displayName": "AMS"},
            },
            "airlines": {
                "RO": {"name": "TAROM"},
                "AF": {"name": "Air France"},
                "CM": {"name": "Copa"},
                "KL": {"name": "KLM"},
            },
            "providers": {
                "OTA1": {"displayName": "Example OTA"},
            },
        }

    def test_kayak_help_bots_url_is_detected_as_blocked(self) -> None:
        self.assertTrue(
            KayakScrapeClient._is_blocked_page(
                "<html><title>What is a bot?</title></html>",
                "https://www.kayak.com/help/bots.html",
                200,
            )
        )

    def test_kayak_browser_assisted_search_payload_recovers_from_bot_page(self) -> None:
        client = KayakScrapeClient(host="www.kayak.com", playwright_assisted=True)

        class _Response:
            def __init__(self, text: str, url: str, status_code: int) -> None:
                self.text = text
                self.url = url
                self.status_code = status_code

            def raise_for_status(self) -> None:
                return None

        session = requests.Session()
        session.headers["User-Agent"] = "UnitTestBrowser/1.0"
        session.get = lambda url, timeout=45: _Response(  # type: ignore[method-assign]
            "<html><title>What is a bot?</title></html>",
            "https://www.kayak.com/help/bots.html",
            200,
        )
        client._local.session = session

        assisted_html = (
            '<script id="jsonData_R9DataStorage">'
            '{"serverData":{"global":{"formtoken":"assist-token"}}}'
            "</script>"
        )

        def fake_assisted(url: str) -> tuple[str, str]:
            client._store_shared_browser_cookies(
                [
                    {
                        "name": "kayak_session",
                        "value": "verified",
                        "domain": ".kayak.com",
                        "path": "/",
                    }
                ]
            )
            client._apply_shared_browser_cookies(session)
            return assisted_html, url

        client._fetch_search_page_playwright_assisted = fake_assisted  # type: ignore[assignment]
        observed: dict[str, object] = {}

        def fake_post_poll(
            referer_url: str,
            csrf_token: str,
            payload: dict[str, object],
        ) -> dict[str, object]:
            observed["referer_url"] = referer_url
            observed["csrf_token"] = csrf_token
            observed["cookie_value"] = session.cookies.get("kayak_session")
            return {
                "status": "complete",
                "searchId": "assist-search",
                "results": [{"type": "core"}],
            }

        client._post_poll = fake_post_poll  # type: ignore[assignment]
        previous_allow_playwright = kayak_provider_module.ALLOW_PLAYWRIGHT_PROVIDERS
        kayak_provider_module.ALLOW_PLAYWRIGHT_PROVIDERS = True
        try:
            payload = client._search_payload(
                source="OTP",
                destination="FCO",
                outbound_iso="2026-04-18",
                inbound_iso=None,
                currency="RON",
                adults=1,
            )
        finally:
            kayak_provider_module.ALLOW_PLAYWRIGHT_PROVIDERS = previous_allow_playwright

        self.assertEqual(observed["csrf_token"], "assist-token")
        self.assertEqual(observed["cookie_value"], "verified")
        self.assertEqual(
            observed["referer_url"],
            "https://www.kayak.com/flights/OTP-FCO/2026-04-18?sort=price_a&adults=1&currency=RON",
        )
        self.assertEqual(payload["results"], [{"type": "core"}])

    def test_kayak_playwright_fallback_recovers_when_requests_poll_is_blocked(self) -> None:
        client = KayakScrapeClient(
            host="www.kayak.com",
            playwright_browser_channel="msedge",
        )

        class _Response:
            def __init__(self, text: str, url: str, status_code: int) -> None:
                self.text = text
                self.url = url
                self.status_code = status_code

            def raise_for_status(self) -> None:
                return None

        page_html = (
            '<script id="jsonData_R9DataStorage">'
            '{"serverData":{"global":{"formtoken":"csrf-token"}}}'
            "</script>"
        )
        session = requests.Session()
        session.get = lambda url, timeout=45: _Response(  # type: ignore[method-assign]
            page_html,
            url,
            200,
        )
        client._local.session = session
        client._post_poll = lambda **kwargs: (_ for _ in ()).throw(  # type: ignore[assignment]
            kayak_provider_module.ProviderBlockedError(
                "blocked by bot protection",
                manual_search_url=kwargs.get("referer_url"),
            )
        )
        client._search_payload_playwright_backed = lambda url: {  # type: ignore[assignment]
            "status": "complete",
            "searchId": "playwright-search",
            "results": [{"type": "core", "browser_url": url}],
        }
        previous_allow_playwright = kayak_provider_module.ALLOW_PLAYWRIGHT_PROVIDERS
        kayak_provider_module.ALLOW_PLAYWRIGHT_PROVIDERS = True
        try:
            payload = client._search_payload(
                source="OTP",
                destination="FCO",
                outbound_iso="2026-04-18",
                inbound_iso=None,
                currency="RON",
                adults=1,
            )
        finally:
            kayak_provider_module.ALLOW_PLAYWRIGHT_PROVIDERS = previous_allow_playwright

        self.assertEqual(payload["status"], "complete")
        self.assertEqual(payload["searchId"], "playwright-search")
        self.assertEqual(
            payload["results"],
            [
                {
                    "type": "core",
                    "browser_url": (
                        "https://www.kayak.com/flights/OTP-FCO/2026-04-18"
                        "?sort=price_a&adults=1&currency=RON"
                    ),
                }
            ],
        )

    def test_kayak_assisted_playwright_fallback_runs_after_headless_block(self) -> None:
        client = KayakScrapeClient(
            host="www.kayak.com",
            playwright_browser_channel="msedge",
            playwright_assisted=True,
        )

        class _Response:
            def __init__(self, text: str, url: str, status_code: int) -> None:
                self.text = text
                self.url = url
                self.status_code = status_code

            def raise_for_status(self) -> None:
                return None

        page_html = (
            '<script id="jsonData_R9DataStorage">'
            '{"serverData":{"global":{"formtoken":"csrf-token"}}}'
            "</script>"
        )
        session = requests.Session()
        session.get = lambda url, timeout=45: _Response(  # type: ignore[method-assign]
            page_html,
            url,
            200,
        )
        client._local.session = session
        client._post_poll = lambda **kwargs: (_ for _ in ()).throw(  # type: ignore[assignment]
            kayak_provider_module.ProviderBlockedError(
                "blocked by bot protection",
                manual_search_url=kwargs.get("referer_url"),
            )
        )
        client._search_payload_playwright_backed = lambda url: (_ for _ in ()).throw(  # type: ignore[assignment]
            kayak_provider_module.ProviderBlockedError(
                "still blocked in headless browser",
                manual_search_url=url,
            )
        )
        client._search_payload_playwright_assisted = lambda url: {  # type: ignore[assignment]
            "status": "complete",
            "searchId": "assisted-search",
            "results": [{"type": "core", "assisted_url": url}],
        }
        previous_allow_playwright = kayak_provider_module.ALLOW_PLAYWRIGHT_PROVIDERS
        kayak_provider_module.ALLOW_PLAYWRIGHT_PROVIDERS = True
        try:
            payload = client._search_payload(
                source="OTP",
                destination="FCO",
                outbound_iso="2026-04-18",
                inbound_iso="2026-04-25",
                currency="RON",
                adults=1,
            )
        finally:
            kayak_provider_module.ALLOW_PLAYWRIGHT_PROVIDERS = previous_allow_playwright

        self.assertEqual(payload["status"], "complete")
        self.assertEqual(payload["searchId"], "assisted-search")
        self.assertEqual(
            payload["results"],
            [
                {
                    "type": "core",
                    "assisted_url": (
                        "https://www.kayak.com/flights/OTP-FCO/2026-04-18/2026-04-25"
                        "?sort=price_a&adults=1&currency=RON"
                    ),
                }
            ],
        )

    def test_kayak_get_best_return_parses_full_itinerary(self) -> None:
        client = KayakScrapeClient(host="www.kayak.com")
        client._search_payload = lambda **kwargs: self._sample_payload()  # type: ignore[assignment]

        best = client.get_best_return(
            source="OTP",
            destination="MGA",
            outbound_iso="2026-03-10",
            inbound_iso="2026-03-24",
            currency="RON",
            max_stops_per_leg=2,
            adults=1,
            hand_bags=0,
            hold_bags=0,
        )

        self.assertIsNotNone(best)
        assert best is not None
        self.assertEqual(best["price"], 5521)
        self.assertEqual(best["outbound_stops"], 2)
        self.assertEqual(best["inbound_stops"], 2)
        self.assertEqual(best["provider"], "kayak")
        self.assertEqual(best["booking_provider"], "Example OTA")
        self.assertIn("kayak.com", str(best.get("booking_url") or ""))

    def test_kayak_scales_per_person_price_for_multiple_adults(self) -> None:
        client = KayakScrapeClient(host="www.kayak.com")
        client._search_payload = lambda **kwargs: self._sample_payload(  # type: ignore[assignment]
            {
                "price": "938",
                "currency": "USD",
                "priceMode": "perPerson",
            }
        )

        best = client.get_best_return(
            source="OTP",
            destination="MRU",
            outbound_iso="2026-04-28",
            inbound_iso="2026-05-06",
            currency="USD",
            max_stops_per_leg=2,
            adults=2,
            hand_bags=0,
            hold_bags=0,
        )

        self.assertIsNotNone(best)
        assert best is not None
        self.assertEqual(best["price"], 1876)
        self.assertEqual(best.get("price_mode"), "per_person_scaled")

    def test_kayak_prefers_explicit_total_when_present(self) -> None:
        client = KayakScrapeClient(host="www.kayak.com")
        client._search_payload = lambda **kwargs: self._sample_payload(  # type: ignore[assignment]
            {
                "price": "938",
                "currency": "USD",
                "priceMode": "perPerson",
                "totalPrice": "1876",
            }
        )

        best = client.get_best_return(
            source="OTP",
            destination="MRU",
            outbound_iso="2026-04-28",
            inbound_iso="2026-05-06",
            currency="USD",
            max_stops_per_leg=2,
            adults=2,
            hand_bags=0,
            hold_bags=0,
        )

        self.assertIsNotNone(best)
        assert best is not None
        self.assertEqual(best["price"], 1876)
        self.assertEqual(best.get("price_mode"), "explicit_total")

    def test_kayak_respects_stops_cap(self) -> None:
        client = KayakScrapeClient(host="www.kayak.com")
        client._search_payload = lambda **kwargs: self._sample_payload()  # type: ignore[assignment]

        best = client.get_best_oneway(
            source="OTP",
            destination="MGA",
            departure_iso="2026-03-10",
            currency="RON",
            max_stops_per_leg=1,
            adults=1,
            hand_bags=0,
            hold_bags=0,
        )
        self.assertIsNone(best)

    def test_momondo_provider_keeps_identity(self) -> None:
        client = MomondoScrapeClient(host="www.momondo.com")
        client._search_payload = lambda **kwargs: self._sample_payload()  # type: ignore[assignment]

        best = client.get_best_return(
            source="OTP",
            destination="MGA",
            outbound_iso="2026-03-10",
            inbound_iso="2026-03-24",
            currency="RON",
            max_stops_per_leg=2,
            adults=1,
            hand_bags=0,
            hold_bags=0,
        )

        self.assertIsNotNone(best)
        assert best is not None
        self.assertEqual(best["provider"], "momondo")
        self.assertIn("momondo.com", str(best.get("booking_url") or ""))

    def test_momondo_scales_per_person_price_for_multiple_adults(self) -> None:
        client = MomondoScrapeClient(host="www.momondo.com")
        client._search_payload = lambda **kwargs: self._sample_payload(  # type: ignore[assignment]
            {
                "price": "938",
                "currency": "USD",
                "priceMode": "perPerson",
            }
        )

        best = client.get_best_return(
            source="OTP",
            destination="MRU",
            outbound_iso="2026-04-28",
            inbound_iso="2026-05-06",
            currency="USD",
            max_stops_per_leg=2,
            adults=2,
            hand_bags=0,
            hold_bags=0,
        )

        self.assertIsNotNone(best)
        assert best is not None
        self.assertEqual(best["price"], 1876)
        self.assertEqual(best.get("price_mode"), "per_person_scaled")


if __name__ == "__main__":
    unittest.main()
