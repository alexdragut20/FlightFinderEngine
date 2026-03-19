from __future__ import annotations

import os
import threading

from .data import hub_pool

AUTO_HUB_CANDIDATES = hub_pool.AUTO_HUB_CANDIDATES

GRAPHQL_ENDPOINT = "https://api.skypicker.com/umbrella/v2/graphql"
AIRPORTS_DATA_URL = (
    "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat"
)
ROUTES_DATA_URL = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat"
AMADEUS_BASE_URL = os.getenv("AMADEUS_BASE_URL", "https://test.api.amadeus.com").rstrip("/")
SERPAPI_SEARCH_URL = os.getenv("SERPAPI_SEARCH_URL", "https://serpapi.com/search.json").rstrip("/")
KAYAK_SCRAPE_HOST = (
    os.getenv("KAYAK_SCRAPE_HOST", "www.kayak.com").strip().lower() or "www.kayak.com"
)
MOMONDO_SCRAPE_HOST = (
    os.getenv("MOMONDO_SCRAPE_HOST", "www.momondo.com").strip().lower() or "www.momondo.com"
)
SKYSCANNER_SCRAPE_HOST = (
    os.getenv("SKYSCANNER_SCRAPE_HOST", "www.skyscanner.com").strip().lower()
    or "www.skyscanner.com"
)
KAYAK_SCRAPE_SCHEME = "https"

SUPPORTED_PROVIDER_IDS = (
    "kiwi",
    "kayak",
    "momondo",
    "googleflights",
    "skyscanner",
    "amadeus",
    "serpapi",
)
_FREE_PROVIDER_IDS = {"kiwi", "kayak", "momondo", "googleflights", "skyscanner"}


def _detect_playwright_browser_channel() -> str:
    """Return a preferred local browser channel for Playwright-backed providers."""
    channel_candidates = (
        (
            "msedge",
            (
                os.path.join(
                    os.getenv("ProgramFiles(x86)") or "",
                    "Microsoft",
                    "Edge",
                    "Application",
                    "msedge.exe",
                ),
                os.path.join(
                    os.getenv("ProgramFiles") or "",
                    "Microsoft",
                    "Edge",
                    "Application",
                    "msedge.exe",
                ),
            ),
        ),
        (
            "chrome",
            (
                os.path.join(
                    os.getenv("ProgramFiles") or "",
                    "Google",
                    "Chrome",
                    "Application",
                    "chrome.exe",
                ),
                os.path.join(
                    os.getenv("ProgramFiles(x86)") or "",
                    "Google",
                    "Chrome",
                    "Application",
                    "chrome.exe",
                ),
            ),
        ),
    )
    for channel, paths in channel_candidates:
        for candidate in paths:
            if candidate and os.path.exists(candidate):
                return channel
    return ""


try:
    _default_search_timeout_env = int(os.getenv("DEFAULT_SEARCH_TIMEOUT_SECONDS", "1500"))
except ValueError:
    _default_search_timeout_env = 1500
DEFAULT_SEARCH_TIMEOUT_SECONDS = max(60, min(7200, _default_search_timeout_env))
try:
    _provider_error_cooldown_env = int(os.getenv("PROVIDER_ERROR_COOLDOWN_SECONDS", "300"))
except ValueError:
    _provider_error_cooldown_env = 300
PROVIDER_ERROR_COOLDOWN_SECONDS = max(30, min(3600, _provider_error_cooldown_env))
ALLOW_PLAYWRIGHT_PROVIDERS = str(os.getenv("ALLOW_PLAYWRIGHT_PROVIDERS", "0")).strip().lower() in {
    "1",
    "true",
    "on",
    "yes",
}
_FX_CACHE_LOCK = threading.Lock()
_FX_CACHE_TTL_SECONDS = 12 * 3600
_FX_RATE_CACHE: dict[str, tuple[float, dict[str, float]]] = {}

DEFAULT_DESTINATIONS = ["MLE", "SEZ", "PUJ", "CUN", "DPS", "HKT", "MRU", "ZNZ"]

DEFAULT_IO_WORKERS = max(8, min(24, (os.cpu_count() or 4) * 2))
DEFAULT_CPU_WORKERS = max(1, min(8, (os.cpu_count() or 4) - 1))
DEFAULT_CALENDAR_HUBS_PREFETCH: int | None = None
DEFAULT_MAX_VALIDATE_ONEWAY_KEYS: int | None = None
DEFAULT_MAX_VALIDATE_RETURN_KEYS: int | None = None
DEFAULT_MAX_TOTAL_PROVIDER_CALLS: int | None = None
DEFAULT_MAX_CALLS_KIWI: int | None = None
DEFAULT_MAX_CALLS_AMADEUS: int | None = None
DEFAULT_MAX_CALLS_SERPAPI: int | None = None
DEFAULT_SERPAPI_PROBE_ONEWAY_KEYS = 80
DEFAULT_SERPAPI_PROBE_RETURN_KEYS = 16
try:
    _split_same_airport_minutes = int(os.getenv("MIN_SPLIT_CONNECTION_SAME_AIRPORT_MINUTES", "120"))
except ValueError:
    _split_same_airport_minutes = 120
try:
    _split_cross_airport_minutes = int(
        os.getenv("MIN_SPLIT_CONNECTION_CROSS_AIRPORT_MINUTES", "300")
    )
except ValueError:
    _split_cross_airport_minutes = 300
MIN_SPLIT_CONNECTION_SAME_AIRPORT_SECONDS = (
    max(
        0,
        min(1440, _split_same_airport_minutes),
    )
    * 60
)
MIN_SPLIT_CONNECTION_CROSS_AIRPORT_SECONDS = (
    max(
        MIN_SPLIT_CONNECTION_SAME_AIRPORT_SECONDS // 60,
        min(1440, _split_cross_airport_minutes),
    )
    * 60
)
try:
    _kiwi_scan_env = int(os.getenv("KIWI_ITINERARY_SCAN_LIMIT", "50"))
except ValueError:
    _kiwi_scan_env = 50
KIWI_ITINERARY_SCAN_LIMIT = max(1, min(50, _kiwi_scan_env))
AMADEUS_FLIGHT_OFFERS_MAX = 250
try:
    _serpapi_scan_env = int(os.getenv("SERPAPI_RETURN_OPTION_SCAN_LIMIT", "2"))
except ValueError:
    _serpapi_scan_env = 2
SERPAPI_RETURN_OPTION_SCAN_LIMIT = max(1, min(5, _serpapi_scan_env))
try:
    _kayak_poll_rounds_env = int(os.getenv("KAYAK_SCRAPE_POLL_ROUNDS", "2"))
except ValueError:
    _kayak_poll_rounds_env = 2
KAYAK_SCRAPE_POLL_ROUNDS = max(1, min(6, _kayak_poll_rounds_env))
_kayak_playwright_assisted_default = "1" if ALLOW_PLAYWRIGHT_PROVIDERS else "0"
_playwright_browser_channel_default = (
    _detect_playwright_browser_channel() if ALLOW_PLAYWRIGHT_PROVIDERS else ""
)
KAYAK_SCRAPE_PLAYWRIGHT_ASSISTED = ALLOW_PLAYWRIGHT_PROVIDERS and (
    str(
        os.getenv(
            "KAYAK_SCRAPE_PLAYWRIGHT_ASSISTED",
            _kayak_playwright_assisted_default,
        )
    )
    .strip()
    .lower()
    not in {"0", "false", "off", "no"}
)
KAYAK_PLAYWRIGHT_BROWSER_CHANNEL = (
    str(
        os.getenv(
            "KAYAK_PLAYWRIGHT_BROWSER_CHANNEL",
            _playwright_browser_channel_default,
        )
        or ""
    ).strip()
    or None
)
KAYAK_PLAYWRIGHT_PROFILE_ROOT = str(
    os.getenv("KAYAK_PLAYWRIGHT_PROFILE_ROOT", "") or ""
).strip() or os.path.join(
    os.getenv("LOCALAPPDATA") or os.path.expanduser("~"),
    "FlightFinderEngine",
    "playwright",
)
try:
    _kayak_playwright_assist_timeout_seconds = int(
        os.getenv("KAYAK_PLAYWRIGHT_ASSIST_TIMEOUT_SECONDS", "120")
    )
except ValueError:
    _kayak_playwright_assist_timeout_seconds = 120
KAYAK_PLAYWRIGHT_ASSIST_TIMEOUT_SECONDS = max(
    15,
    min(600, _kayak_playwright_assist_timeout_seconds),
)
try:
    _skyscanner_retry_env = int(os.getenv("SKYSCANNER_SCRAPE_HTTP_RETRIES", "2"))
except ValueError:
    _skyscanner_retry_env = 2
SKYSCANNER_SCRAPE_HTTP_RETRIES = max(1, min(6, _skyscanner_retry_env))
_skyscanner_playwright_fallback_default = "1" if ALLOW_PLAYWRIGHT_PROVIDERS else "0"
SKYSCANNER_SCRAPE_PLAYWRIGHT_FALLBACK = ALLOW_PLAYWRIGHT_PROVIDERS and (
    str(
        os.getenv(
            "SKYSCANNER_SCRAPE_PLAYWRIGHT_FALLBACK",
            _skyscanner_playwright_fallback_default,
        )
    )
    .strip()
    .lower()
    not in {"0", "false", "off", "no"}
)
_skyscanner_playwright_assisted_default = "1" if ALLOW_PLAYWRIGHT_PROVIDERS else "0"
SKYSCANNER_PLAYWRIGHT_ASSISTED = ALLOW_PLAYWRIGHT_PROVIDERS and (
    str(
        os.getenv(
            "SKYSCANNER_PLAYWRIGHT_ASSISTED",
            _skyscanner_playwright_assisted_default,
        )
    )
    .strip()
    .lower()
    not in {"0", "false", "off", "no"}
)
SKYSCANNER_PLAYWRIGHT_BROWSER_CHANNEL = (
    str(
        os.getenv(
            "SKYSCANNER_PLAYWRIGHT_BROWSER_CHANNEL",
            _playwright_browser_channel_default,
        )
        or ""
    ).strip()
    or None
)
SKYSCANNER_PLAYWRIGHT_PROFILE_DIR = str(
    os.getenv("SKYSCANNER_PLAYWRIGHT_PROFILE_DIR", "") or ""
).strip() or os.path.join(
    os.getenv("LOCALAPPDATA") or os.path.expanduser("~"),
    "FlightFinderEngine",
    "playwright",
    "skyscanner",
)
try:
    _skyscanner_playwright_max_concurrency = int(
        os.getenv("SKYSCANNER_PLAYWRIGHT_MAX_CONCURRENCY", "1")
    )
except ValueError:
    _skyscanner_playwright_max_concurrency = 1
SKYSCANNER_PLAYWRIGHT_MAX_CONCURRENCY = max(1, min(4, _skyscanner_playwright_max_concurrency))
try:
    _skyscanner_playwright_host_attempts = int(
        os.getenv("SKYSCANNER_PLAYWRIGHT_HOST_ATTEMPTS", "1")
    )
except ValueError:
    _skyscanner_playwright_host_attempts = 1
SKYSCANNER_PLAYWRIGHT_HOST_ATTEMPTS = max(1, min(3, _skyscanner_playwright_host_attempts))
try:
    _skyscanner_playwright_acquire_timeout_seconds = float(
        os.getenv("SKYSCANNER_PLAYWRIGHT_ACQUIRE_TIMEOUT_SECONDS", "6")
    )
except ValueError:
    _skyscanner_playwright_acquire_timeout_seconds = 6.0
SKYSCANNER_PLAYWRIGHT_ACQUIRE_TIMEOUT_SECONDS = max(
    1.0,
    min(30.0, _skyscanner_playwright_acquire_timeout_seconds),
)
try:
    _skyscanner_playwright_assist_timeout_seconds = int(
        os.getenv("SKYSCANNER_PLAYWRIGHT_ASSIST_TIMEOUT_SECONDS", "90")
    )
except ValueError:
    _skyscanner_playwright_assist_timeout_seconds = 90
SKYSCANNER_PLAYWRIGHT_ASSIST_TIMEOUT_SECONDS = max(
    15,
    min(300, _skyscanner_playwright_assist_timeout_seconds),
)
try:
    _skyscanner_waf_cooldown_seconds = int(os.getenv("SKYSCANNER_WAF_COOLDOWN_SECONDS", "900"))
except ValueError:
    _skyscanner_waf_cooldown_seconds = 900
SKYSCANNER_WAF_COOLDOWN_SECONDS = max(30, min(86400, _skyscanner_waf_cooldown_seconds))
try:
    _skyscanner_playwright_error_cooldown_seconds = int(
        os.getenv("SKYSCANNER_PLAYWRIGHT_ERROR_COOLDOWN_SECONDS", "300")
    )
except ValueError:
    _skyscanner_playwright_error_cooldown_seconds = 300
SKYSCANNER_PLAYWRIGHT_ERROR_COOLDOWN_SECONDS = max(
    30,
    min(3600, _skyscanner_playwright_error_cooldown_seconds),
)
_skyscanner_hosts_env = str(os.getenv("SKYSCANNER_SCRAPE_HOSTS", "") or "").strip()
if _skyscanner_hosts_env:
    SKYSCANNER_SCRAPE_HOSTS = [
        part.strip().lower() for part in _skyscanner_hosts_env.split(",") if part.strip()
    ]
else:
    SKYSCANNER_SCRAPE_HOSTS = [
        SKYSCANNER_SCRAPE_HOST,
        "www.skyscanner.net",
        "www.skyscanner.ro",
    ]

# Quick destination notes for April-May beach trips.
DESTINATION_NOTES: dict[str, dict[str, str]] = {
    "MLE": {
        "name": "Maldives (Male)",
        "note": "Hot year-round beach weather; May is warmer and more humid.",
    },
    "SEZ": {
        "name": "Seychelles",
        "note": "Warm tropical weather; April-May generally very beach-friendly.",
    },
    "PUJ": {
        "name": "Punta Cana",
        "note": "Hot Caribbean weather in April-May with beach temperatures.",
    },
    "CUN": {
        "name": "Cancun",
        "note": "Hot and sunny spring period with warm sea temperatures.",
    },
    "DPS": {
        "name": "Bali (Denpasar)",
        "note": "Transition to drier season around April-May; hot beach weather.",
    },
    "HKT": {
        "name": "Phuket",
        "note": "Hot tropical weather; shoulder season before deeper monsoon period.",
    },
    "MRU": {
        "name": "Mauritius",
        "note": "Warm and pleasant shoulder season in April-May.",
    },
    "ZNZ": {
        "name": "Zanzibar",
        "note": "Warm tropical temperatures; check rain patterns for exact dates.",
    },
}

# Fallback coordinates in case airport cache cannot be downloaded.
FALLBACK_COORDS = {
    "OTP": (44.5711, 26.0850),
    "BBU": (44.5032, 26.1021),
    "DXB": (25.2528, 55.3644),
    "DOH": (25.2731, 51.6081),
    "AUH": (24.4330, 54.6511),
    "IST": (41.2753, 28.7519),
    "SAW": (40.8986, 29.3092),
    "MAD": (40.4936, -3.5668),
    "LIS": (38.7742, -9.1342),
    "BCN": (41.2974, 2.0833),
    "FRA": (50.0379, 8.5622),
    "MUC": (48.3538, 11.7861),
    "AMS": (52.3105, 4.7683),
    "CDG": (49.0097, 2.5479),
    "LHR": (51.4700, -0.4543),
    "MXP": (45.6301, 8.7231),
    "FCO": (41.7999, 12.2462),
    "VIE": (48.1103, 16.5697),
    "WAW": (52.1657, 20.9671),
    "ATH": (37.9364, 23.9445),
    "BUD": (47.4390, 19.2610),
    "JFK": (40.6413, -73.7781),
    "EWR": (40.6895, -74.1745),
    "MIA": (25.7959, -80.2871),
    "DEL": (28.5562, 77.1000),
    "BOM": (19.0896, 72.8656),
    "SIN": (1.3644, 103.9915),
    "MLE": (4.1918, 73.5291),
    "SEZ": (-4.6743, 55.5218),
    "PUJ": (18.5674, -68.3634),
    "CUN": (21.0365, -86.8771),
    "DPS": (-8.7482, 115.1672),
    "HKT": (8.1132, 98.3169),
    "MRU": (-20.4302, 57.6836),
    "ZNZ": (-6.2220, 39.2249),
}

CALENDAR_QUERY = """
query Calendar(
  $search: SearchPricesCalendarInput,
  $filter: ItinerariesFilterInput,
  $options: ItinerariesOptionsInput
) {
  itineraryPricesCalendar(search: $search, filter: $filter, options: $options) {
    ... on AppError {
      code
      message
    }
    ... on ItineraryPricesCalendar {
      calendar {
        date
        ratedPrice {
          price {
            amount
            formattedValue
            currency {
              code
            }
          }
          rating
        }
      }
    }
  }
}
"""

ONEWAY_QUERY = """
query Oneway(
  $search: SearchOnewayInput,
  $filter: ItinerariesFilterInput,
  $options: ItinerariesOptionsInput
) {
  onewayItineraries(search: $search, filter: $filter, options: $options) {
    ... on AppError {
      code
      message
    }
    ... on Itineraries {
      itineraries {
        ... on ItineraryOneWay {
          price {
            amount
            formattedValue
            currency {
              code
            }
          }
          bookingOptions {
            edges {
              node {
                bookingUrl
              }
            }
          }
          duration
          sector {
            sectorSegments {
              segment {
                source {
                  localTime
                  station {
                    code
                    name
                  }
                }
                destination {
                  localTime
                  station {
                    code
                    name
                  }
                }
                carrier {
                  code
                  name
                }
              }
            }
          }
        }
      }
    }
  }
}
"""

RETURN_QUERY = """
query Return(
  $search: SearchReturnInput,
  $filter: ItinerariesFilterInput,
  $options: ItinerariesOptionsInput
) {
  returnItineraries(search: $search, filter: $filter, options: $options) {
    ... on AppError {
      code
      message
    }
    ... on Itineraries {
      itineraries {
        ... on ItineraryReturn {
          price {
            amount
            formattedValue
            currency {
              code
            }
          }
          bookingOptions {
            edges {
              node {
                bookingUrl
              }
            }
          }
          duration
          outbound {
            duration
            sectorSegments {
              segment {
                source {
                  localTime
                  station {
                    code
                    name
                  }
                }
                destination {
                  localTime
                  station {
                    code
                    name
                  }
                }
                carrier {
                  code
                  name
                }
              }
            }
          }
          inbound {
            duration
            sectorSegments {
              segment {
                source {
                  localTime
                  station {
                    code
                    name
                  }
                }
                destination {
                  localTime
                  station {
                    code
                    name
                  }
                }
                carrier {
                  code
                  name
                }
              }
            }
          }
        }
      }
    }
  }
}
"""
