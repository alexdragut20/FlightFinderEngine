# Provider Research (Live Fare Data)

## Summary

There is no reliable, legally safe, **unlimited free** API that provides broad, real-time fare-shopping across airlines and routes.

## Verified provider notes

- SerpApi Google Flights API
  - `deep_search=true` is recommended for results closer to browser Google Flights output.
  - Uses cached responses when possible (`no_cache=false`), and cached searches are not billed.
  - Source: https://serpapi.com/google-flights-api

- Google Flights direct scraping
  - Plain HTTP requests are commonly redirected to Google consent pages.
  - Local browser automation (Playwright) can work for some routes and return usable fare cards.
  - Practical recommendation: keep this provider optional/experimental and rate-limit it.

- Amadeus Self-Service
  - Flight Offers Search supports `max` and returns up to 250 offers.
  - Test environment has free but limited quota; production is real-time with paid overage after free threshold.
  - Source: https://developers.amadeus.com/self-service/apis-docs/guides/developer-guides/resources/flights/
  - Source: https://developers.amadeus.com/self-service/apis-docs/guides/developer-guides/test-data/
  - Source: https://developers.amadeus.com/support/faq/about-self-service-apis

- Skyscanner API
  - Access requires partnership approval and API key.
  - Source: https://developers.skyscanner.net/docs/getting-started/authentication

- Skyscanner direct scraping
  - Plain HTTP and headless browser flows are frequently redirected to captcha challenge pages (`/sttc/px/captcha-v2`).
  - Treat as best-effort experimental scrape path only.

- Kayak dynamic poll (scrape path)
  - No public free API key is required for basic polling, but this is still a scrape/integration path.
  - Requires anti-bot-safe request shaping (bootstrap + CSRF + dynamic poll payload).
  - Response includes full legs/segments and book-click URLs suitable for provider comparison.

- Momondo dynamic poll (scrape path)
  - Uses the same dynamic poll model as Kayak (bootstrap + CSRF + poll payload).
  - No public free API key is required, but still considered scraping with ToS/anti-bot risks.
  - Can return additional offers not present on Kiwi-only scans.

- Travelpayouts / Aviasales APIs
  - Explicit request limits exist; not unlimited.
  - Source: https://support.travelpayouts.com/hc/en-us/articles/4402565416594-API-rate-limits

- OpenSky / Aviationstack
  - Useful aviation/traffic data APIs, but not broad OTA fare-shopping replacements.
  - OpenSky is primarily air-traffic/state data; limits apply.
  - Aviationstack free plan is limited (100 requests/month).
  - Source: https://opensky-network.org/data/api
  - Source: https://opensky-network.org/about/faq
  - Source: https://aviationstack.com/pricing

## Practical conclusion

Best production approach:

1. Use Kiwi exhaustive as free baseline.
2. Use paid providers (Amadeus/SerpApi) as constrained validation probes.
3. Add caching and replay (already partly implemented) to reduce repeated paid calls.
4. Use scraping only as an explicit fallback, due ToS/anti-bot/legal and stability risks.
