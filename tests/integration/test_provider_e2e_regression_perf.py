import datetime as dt
import sys
import time
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from server import (  # noqa: E402
    AirportCoordinates,
    KiwiClient,
    ProviderNoResultError,
    SplitTripOptimizer,
)


class SyntheticProvider:
    supports_calendar = True
    requires_credentials = False
    credential_env = ()
    docs_url = "https://example.test/flights"
    default_enabled = True

    def __init__(
        self,
        provider_id: str,
        fare_offset: int,
        *,
        delay_seconds: float = 0.0,
        anti_bot_oneway: bool = False,
        anti_bot_return: bool = False,
    ) -> None:
        self.provider_id = provider_id
        self.display_name = f"Synthetic {provider_id}"
        self.fare_offset = int(fare_offset)
        self.delay_seconds = max(0.0, float(delay_seconds))
        self.anti_bot_oneway = anti_bot_oneway
        self.anti_bot_return = anti_bot_return

    def is_configured(self) -> bool:
        return True

    @staticmethod
    def _seed(*parts: str) -> int:
        return sum(ord(ch) for part in parts for ch in str(part))

    @staticmethod
    def _date_range(date_start_iso: str, date_end_iso: str) -> list[str]:
        start = dt.date.fromisoformat(date_start_iso)
        end = dt.date.fromisoformat(date_end_iso)
        out: list[str] = []
        current = start
        while current <= end:
            out.append(current.isoformat())
            current += dt.timedelta(days=1)
        return out

    @staticmethod
    def _segment(source: str, destination: str, date_iso: str) -> dict:
        day = dt.date.fromisoformat(date_iso)
        depart = dt.datetime.combine(day, dt.time(hour=8, minute=0))
        arrive = depart + dt.timedelta(hours=3, minutes=20)
        return {
            "from": source,
            "to": destination,
            "from_name": source,
            "to_name": destination,
            "depart_local": depart.isoformat(timespec="seconds"),
            "arrive_local": arrive.isoformat(timespec="seconds"),
            "carrier": "SX",
            "carrier_name": f"Synthetic {source}-{destination}",
        }

    def get_calendar_prices(self, **kwargs):  # type: ignore[no-untyped-def]
        if self.delay_seconds > 0:
            time.sleep(self.delay_seconds)
        source = str(kwargs["source"])
        destination = str(kwargs["destination"])
        base = 900 + (self._seed(source, destination) % 120) + self.fare_offset
        prices: dict[str, int] = {}
        for idx, day in enumerate(
            self._date_range(kwargs["date_start_iso"], kwargs["date_end_iso"])
        ):
            prices[day] = base + idx
        return prices

    def get_best_oneway(self, **kwargs):  # type: ignore[no-untyped-def]
        if self.anti_bot_oneway:
            raise ProviderNoResultError("captcha challenge")
        if self.delay_seconds > 0:
            time.sleep(self.delay_seconds)

        source = str(kwargs["source"])
        destination = str(kwargs["destination"])
        departure_iso = str(kwargs["departure_iso"])
        currency = str(kwargs["currency"])
        base_price = 1400 + (self._seed(source, destination, departure_iso) % 220)
        price = base_price + self.fare_offset
        max_stops = int(kwargs.get("max_stops_per_leg") or 0)
        stops = 0 if max_stops <= 0 else 1
        segments = [self._segment(source, destination, departure_iso)]
        return {
            "price": price,
            "formatted_price": f"{price} {currency}",
            "currency": currency,
            "duration_seconds": 12_000,
            "stops": stops,
            "transfer_events": stops,
            "booking_url": (
                f"https://{self.provider_id}.example/oneway/{source}-{destination}/{departure_iso}"
            ),
            "segments": segments,
            "provider": self.provider_id,
            "booking_provider": self.display_name,
        }

    def get_best_return(self, **kwargs):  # type: ignore[no-untyped-def]
        if self.anti_bot_return:
            raise ProviderNoResultError("captcha challenge")
        if self.delay_seconds > 0:
            time.sleep(self.delay_seconds)

        source = str(kwargs["source"])
        destination = str(kwargs["destination"])
        outbound_iso = str(kwargs["outbound_iso"])
        inbound_iso = str(kwargs["inbound_iso"])
        currency = str(kwargs["currency"])

        base_price = 3300 + (self._seed(source, destination, outbound_iso, inbound_iso) % 380)
        price = base_price + self.fare_offset

        outbound_segments = [self._segment(source, destination, outbound_iso)]
        inbound_segments = [self._segment(destination, source, inbound_iso)]
        return {
            "price": price,
            "formatted_price": f"{price} {currency}",
            "currency": currency,
            "duration_seconds": 24_000,
            "outbound_duration_seconds": 12_000,
            "inbound_duration_seconds": 12_000,
            "outbound_stops": 0,
            "inbound_stops": 0,
            "outbound_transfer_events": 0,
            "inbound_transfer_events": 0,
            "booking_url": (
                f"https://{self.provider_id}.example/return/"
                f"{source}-{destination}/{outbound_iso}/{inbound_iso}"
            ),
            "outbound_segments": outbound_segments,
            "inbound_segments": inbound_segments,
            "provider": self.provider_id,
            "booking_provider": self.display_name,
        }


class ProviderE2ERegressionPerfTests(unittest.TestCase):
    def _optimizer_with_synthetic_providers(
        self,
        *,
        serpapi_antibot: bool = False,
        delay_seconds: float = 0.0,
    ) -> SplitTripOptimizer:
        optimizer = SplitTripOptimizer(KiwiClient(), AirportCoordinates())
        optimizer.providers = {
            "kiwi": SyntheticProvider("kiwi", 320, delay_seconds=delay_seconds),
            "kayak": SyntheticProvider("kayak", 260, delay_seconds=delay_seconds),
            "momondo": SyntheticProvider("momondo", 280, delay_seconds=delay_seconds),
            "googleflights": SyntheticProvider("googleflights", 240, delay_seconds=delay_seconds),
            "skyscanner": SyntheticProvider("skyscanner", 220, delay_seconds=delay_seconds),
            "amadeus": SyntheticProvider("amadeus", 180, delay_seconds=delay_seconds),
            "serpapi": SyntheticProvider(
                "serpapi",
                200,
                delay_seconds=delay_seconds,
                anti_bot_oneway=serpapi_antibot,
                anti_bot_return=serpapi_antibot,
            ),
        }
        return optimizer

    @staticmethod
    def _payload(provider_ids: list[str], *, io_workers: int = 24) -> dict:
        return {
            "origins": ["OTP"],
            "destinations": ["MGA"],
            "period_start": "2026-03-10",
            "period_end": "2026-03-12",
            "min_stay_days": 1,
            "max_stay_days": 1,
            "min_stopover_days": 0,
            "max_stopover_days": 0,
            "max_transfers_per_direction": 2,
            "currency": "RON",
            "objective": "cheapest",
            "providers": provider_ids,
            "top_results": 5,
            "validate_top_per_destination": 20,
            "estimated_pool_multiplier": 4,
            "auto_hubs_per_direction": 2,
            "hub_candidates": ["IST", "FRA"],
            "exhaustive_hub_scan": False,
            "max_connection_layover_hours": 0,
            "io_workers": io_workers,
            "cpu_workers": 1,
            "passengers": {"adults": 1, "hand_bags": 0, "hold_bags": 0},
            "use_beach_presets": False,
        }

    def test_e2e_provider_order_parity(self) -> None:
        optimizer = self._optimizer_with_synthetic_providers()

        config_a = optimizer.parse_search_config(
            self._payload(
                [
                    "kiwi",
                    "kayak",
                    "momondo",
                    "googleflights",
                    "skyscanner",
                    "amadeus",
                    "serpapi",
                ]
            )
        )
        config_b = optimizer.parse_search_config(
            self._payload(
                [
                    "serpapi",
                    "amadeus",
                    "skyscanner",
                    "googleflights",
                    "momondo",
                    "kayak",
                    "kiwi",
                ]
            )
        )

        result_a = optimizer.search(config_a)
        result_b = optimizer.search(config_b)

        prices_a = [int(item["total_price"]) for item in (result_a.get("results") or [])]
        prices_b = [int(item["total_price"]) for item in (result_b.get("results") or [])]

        self.assertTrue(prices_a)
        self.assertTrue(prices_b)
        self.assertEqual(min(prices_a), min(prices_b))

    def test_e2e_parity_survives_serpapi_antibot_failures(self) -> None:
        provider_ids = ["kiwi", "amadeus", "serpapi", "skyscanner"]

        optimizer_ok = self._optimizer_with_synthetic_providers(serpapi_antibot=False)
        result_ok = optimizer_ok.search(
            optimizer_ok.parse_search_config(self._payload(provider_ids))
        )

        optimizer_blocked = self._optimizer_with_synthetic_providers(serpapi_antibot=True)
        result_blocked = optimizer_blocked.search(
            optimizer_blocked.parse_search_config(self._payload(provider_ids))
        )

        self.assertTrue(result_ok.get("results"))
        self.assertTrue(result_blocked.get("results"))
        best_ok = min(int(item["total_price"]) for item in (result_ok.get("results") or []))
        best_blocked = min(
            int(item["total_price"]) for item in (result_blocked.get("results") or [])
        )

        self.assertEqual(best_ok, best_blocked)
        provider_stats = ((result_blocked.get("meta") or {}).get("engine") or {}).get(
            "provider_stats"
        ) or {}
        serpapi_blocked = int((provider_stats.get("oneway_blocked") or {}).get("serpapi", 0))
        serpapi_return_blocked = int((provider_stats.get("return_blocked") or {}).get("serpapi", 0))
        self.assertTrue((serpapi_blocked + serpapi_return_blocked) > 0)

    def test_perf_suite_multi_provider_finishes_under_budget(self) -> None:
        optimizer = self._optimizer_with_synthetic_providers(delay_seconds=0.01)
        config = optimizer.parse_search_config(
            self._payload(
                [
                    "kiwi",
                    "kayak",
                    "momondo",
                    "googleflights",
                    "skyscanner",
                    "amadeus",
                    "serpapi",
                ]
            )
        )

        start = time.perf_counter()
        result = optimizer.search(config)
        elapsed = time.perf_counter() - start

        self.assertTrue(result.get("results") or [])
        # Synthetic providers emulate network delay; this guards against accidental
        # sequential regressions in the async/threaded search flow.
        self.assertLess(elapsed, 10.0)


if __name__ == "__main__":
    unittest.main()
