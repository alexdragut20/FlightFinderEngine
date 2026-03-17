# FlightFinder Engine

[![CI](https://github.com/alexdragut20/FlightFinderEngine/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/alexdragut20/FlightFinderEngine/actions/workflows/ci.yml?query=branch%3Amain)
![Python](https://img.shields.io/badge/python-3.12+-3776AB?logo=python&logoColor=white)
![Tests](https://img.shields.io/badge/tests-pytest-0A9EDC?logo=pytest&logoColor=white)
![Coverage](https://img.shields.io/badge/coverage-95%25-brightgreen)
![Ruff](https://img.shields.io/badge/lint%20%26%20format-ruff-D7FF64?logo=ruff&logoColor=111111)
![License](https://img.shields.io/badge/license-MIT-green.svg)

FlightFinder Engine is a layover-first flight search engine focused on finding split-ticket opportunities, stopover strategies, and direct alternatives across multiple destinations in a single run.

It combines calendar seeding, route-graph hub discovery, live fare validation, and itinerary ranking to surface routes that normal round-trip searches often miss.

## Highlights

- Searches multiple destinations in one run.
- Compares direct round-trips with split-ticket and planned stopover itineraries.
- Auto-discovers hub candidates from route connectivity instead of relying only on static hub lists.
- Supports multiple provider adapters: Kiwi, Kayak, Momondo, Google Flights, Skyscanner, Amadeus, and SerpApi.
- Validates top estimated candidates with live one-way and round-trip fare lookups.
- Tracks long-running searches with asynchronous job progress, ETA, and full server-side logs.
- Applies provider budgets and validation caps to keep paid API usage under control.
- Includes a browser UI plus a Python entrypoint for local development.

## Why It Is Interesting

The project is structured around separable responsibilities:

- Provider adapters encapsulate external fare sources behind a common interface.
- The optimizer is responsible for candidate generation, fare validation, stitching, and ranking.
- The progress tracker and async job store isolate long-running workflow state from the search logic.
- The HTTP server is intentionally thin and delegates business rules to the engine layer.

That separation keeps the codebase easier to test, extend, and reason about when new providers or ranking strategies are added.

## Quick Start

### 1. Clone the repository

```bash
git clone https://github.com/alexdragut20/FlightFinderEngine.git
cd FlightFinderEngine
```

### 2. Create a virtual environment

macOS / Linux:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### 3. Install dependencies

Runtime only:

```bash
pip install -e .
```

Development tooling included:

```bash
pip install -e ".[dev]"
```

If you want browser-backed scrape providers:

```bash
pip install -e ".[dev,scrape]"
python -m playwright install chromium
```

### 4. Start the web app

```bash
python server.py
```

Open [http://127.0.0.1:8000](http://127.0.0.1:8000).

Alternative console entrypoint after install:

```bash
flightfinder-engine
```

## Using the App

The web UI lets you control:

- origin airports and destination lists
- travel date window
- main-destination stay window
- stopover stay window
- transfer / layover limits
- baggage profile
- ranking objective: `best`, `cheapest`, `fastest`, or `price_per_km`
- provider selection and runtime API keys
- validation and provider budget guardrails

During a search the UI shows:

- current phase
- completion percentage
- ETA
- rolling execution log

## Provider Configuration

Free-first flow:

- `kiwi`
- `kayak`
- `momondo`
- `googleflights`
- `skyscanner`

Credential-backed providers:

- `amadeus`
- `serpapi`

Environment variables supported by the engine include:

- `AMADEUS_CLIENT_ID`
- `AMADEUS_CLIENT_SECRET`
- `AMADEUS_BASE_URL`
- `SERPAPI_API_KEY`
- `SERPAPI_SEARCH_URL`
- `SERPAPI_RETURN_OPTION_SCAN_LIMIT`
- `KAYAK_SCRAPE_HOST`
- `KAYAK_SCRAPE_POLL_ROUNDS`
- `MOMONDO_SCRAPE_HOST`
- `SKYSCANNER_SCRAPE_HOST`
- `SKYSCANNER_SCRAPE_HOSTS`
- `SKYSCANNER_SCRAPE_HTTP_RETRIES`
- `SKYSCANNER_SCRAPE_PLAYWRIGHT_FALLBACK`
- `ALLOW_PLAYWRIGHT_PROVIDERS`
- `FLIGHT_LAYOVER_LAB_ROOT`

Runtime provider secrets can also be set through the UI without restarting the app.

## Quality Gates

Install local hooks:

```bash
pre-commit install
```

Run the main quality checks manually:

```bash
ruff check .
ruff format --check .
pytest --cov=src/flight_layover_lab --cov-report=term-missing
```

The repository includes:

- `pre-commit` hooks
- GitHub Actions CI
- `pytest` test suite
- `pytest-cov` coverage reporting
- `ruff` linting and formatting

## Benchmark

The repository includes a benchmark-oriented script for comparing split-ticket opportunities against standard pricing:

```bash
python benchmark_supreme_test.py
```

Output is written under `cache/`.

## Repository Layout

```text
.
|- src/flight_layover_lab/    Core engine, providers, HTTP server, progress tracking
|- static/                    Browser UI assets
|- tests/                     Unit and integration coverage
|- docs/                      Supporting research notes
|- scripts/                   Utility scripts such as local restart helpers
|- server.py                  Thin compatibility entrypoint
```

## Restart Helper

macOS / Linux restart helper:

```bash
zsh ./scripts/restart_server.zsh
```

This script is Unix-specific. On Windows, restart the server directly from PowerShell using the virtual environment Python executable.

## Notes and Tradeoffs

- This engine intentionally explores split-ticket strategies, so self-transfer and missed-connection risk still matters.
- Prices are live snapshots and can change quickly between searches.
- Baggage policies can differ across legs and providers.
- Some scrape providers may be rate-limited, blocked, or captcha-challenged depending on network conditions.

## License

MIT. See [LICENSE](LICENSE).
