# Flight API Research - March 21, 2026

This document summarizes the current provider research done for FlightFinderLab on March 21, 2026.

Account creation and spend:

- No paid plans were purchased.
- No third-party accounts were created during this pass.
- Reason: no user-owned email/identity or existing provider credentials were available locally, so only official documentation and token-free validation paths were used.

## APIs reviewed

### 1. Aviasales Data API via Travelpayouts

Official docs:

- https://support.travelpayouts.com/hc/en-us/articles/203956163-Aviasales-Data-API
- https://support.travelpayouts.com/hc/en-us/articles/4417975783314-Aviasales-GraphQL-for-access-to-Flights-Data-API
- https://support.travelpayouts.com/hc/en-us/articles/20384016664594-Brands-that-provide-access-to-APIs-and-data-feeds-for-Travelpayouts-partners

What stands out:

- Free after Travelpayouts signup and Aviasales program connection.
- REST data API returns cached fares, date matrices, nearest-place alternatives, and exact-date price lists.
- GraphQL version explicitly states it can return more data with fewer requests than REST.
- Good fit for discovery and cheap-date seeding.

What we implemented:

- Added a new optional `travelpayouts` provider in the engine.
- Uses month-level `prices_for_dates` queries and caches them per route/month.
- Supports `get_calendar_prices`, `get_best_oneway`, and `get_best_return`.
- Uses `TRAVELPAYOUTS_API_TOKEN` and optional `TRAVELPAYOUTS_MARKET`.

Tradeoffs:

- Fare data is cached from Aviasales search history, not guaranteed real-time.
- Better for discovery and broad coverage than last-mile exact booking validation.

### 2. Aviasales Flights Search API via Travelpayouts

Official docs:

- https://support.travelpayouts.com/hc/en-us/articles/30565016140434-Aviasales-Flights-Search-API-real-time-and-multi-city-search

What stands out:

- Real-time search API.
- Supports one-way, round-trip, and multi-city itineraries.
- Strong match for "many results per request" because it returns full result sets for complex itineraries.

Why it was not integrated in this pass:

- Official rules are strict:
  - every request must be user-initiated,
  - results must be shown to the user,
  - automatic buy-link generation is prohibited,
  - localhost requests are prohibited,
  - default limit is 100 requests per hour from a single user IP.
- It is better suited to a hosted metasearch flow than local background validation.

Recommended future use:

- Add it later as a hosted, user-driven provider for explicit search-result pages, not as a silent background validator.

### 3. Amadeus Self-Service APIs

Official docs:

- https://developers.amadeus.com/self-service/apis-docs/guides/developer-guides/faq/

Current status:

- Already integrated in FlightFinderLab.

Important limitations from the official FAQ:

- Self-Service flight search does not return data for American Airlines, Delta, British Airways, and low-cost carriers.
- It returns published GDS rates only.

Conclusion:

- Still useful, but not broad enough to be the only paid/credentialed provider.

### 4. Duffel API

Official docs:

- https://duffel.com/docs/api

Why it is interesting:

- Modern air-shopping model with rich offer requests and multi-slice support.
- Strong technical fit for exact shopping flows.

Why it was not chosen now:

- Requires a dashboard token/account.
- No credentials were available locally to validate against live or test mode.
- Better candidate for a future premium integration than for a free-first expansion.

### 5. aviationstack

Official docs:

- https://aviationstack.com/pricing
- https://docs.apilayer.com

Conclusion:

- Useful aviation data product, but it is not a fare-shopping API.
- Not selected for this engine because we need prices/offers, not just operational flight data.

### 6. AirLabs

Official docs/examples:

- https://airlabs.co/docs
- https://airlabs.co/aerogaviota-developer-api

Conclusion:

- Good for flight status, schedules, routes, airports, and airline operational data.
- Not selected because it does not solve airfare shopping.

### 7. AZair

Website:

- https://www.azair.eu/

What stands out:

- No account or API token is required for basic result pages.
- Exact-date and flexi-date searches return plain HTML that can be parsed without browser challenges in this environment.
- Flexi search can cover many departure dates in one response, which is useful for cheap-date seeding.

Tradeoffs:

- The site is positioned around Europe and the Middle East, so it is not a broad replacement for long-haul global coverage.
- Best fit is as an extra free discovery source for cheap feeder legs and regional budget fares, not as the only provider.

## Recommendation

Best results-for-money choice from this pass:

- `Travelpayouts / Aviasales Data API`

Why:

- Free after signup.
- Good coverage for cheap-date and cheap-route discovery.
- GraphQL path exists for future request consolidation.
- Easier to adopt than the real-time search API because it does not require the same metasearch-compliance flow.

Best future upgrade if hosted search compliance is acceptable:

- `Travelpayouts Aviasales Flights Search API`

Why:

- It is the strongest official multi-city, many-results-per-search option found in this pass.
- It should be added only when the app is running on a real host and the UX follows Travelpayouts' user-initiated search rules.
