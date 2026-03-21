# Provider Iteration Log - March 21, 2026

This log tracks the iteration-by-iteration attempts to improve FlightFinderLab's provider coverage on March 21, 2026. The goal was not just to add providers, but to measure whether each iteration increased result count or lowered prices on the same route combinations.

## Iteration 1 - Travelpayouts research

Hypothesis:

- A free or trial-backed API with calendar-style responses could improve broad discovery more efficiently than route-by-route scraping.

What happened:

- Researched Travelpayouts / Aviasales Data API and Aviasales Flights Search API.
- Integrated the free cached-data path as the `travelpayouts` provider.

Outcome:

- Success on research and integration.
- No live result benchmark was possible because no Travelpayouts token was available locally.

Notes:

- No account was created.
- No money was spent.

## Iteration 2 - AZair integration

Hypothesis:

- AZair could add cheap regional fares without credentials and strengthen low-cost discovery.

What happened:

- Implemented the `azair` provider with exact-date and flexi-date parsing.
- Verified live AZair responses for `OTP -> FCO`, including one-way and round-trip pricing.

Outcome:

- Technical success: provider worked and returned real prices.
- Commercially mixed result: on the sampled OTP Europe bundle, AZair reached parity with Kiwi but did not beat it.

Measured sample:

- `OTP -> FCO`
- one-way `2026-04-18`: `63 EUR`
- return `2026-04-18 / 2026-04-25`: `129 EUR`

## Iteration 3 - Wizz Air research

Hypothesis:

- Wizz Air public search pages might expose low-cost regional fares that Kiwi was missing.

What happened:

- Tested the public cheap-flights entry page.

Outcome:

- Failure in this environment.
- The entry page returned an AWS WAF human-verification flow instead of usable fare data.

Reason not pursued:

- The blocker was anti-bot gating, not a parser bug.

## Iteration 4 - easyJet research

Hypothesis:

- easyJet's public destination-guides app might expose route-pricing endpoints suitable for calendar fare harvesting.

What happened:

- Inspected the public page and its JavaScript bundle.
- Confirmed route-pricing API references and a live routes endpoint.

Outcome:

- Partial success.
- Route metadata was reachable, but the exact public fare endpoint shape was not resolved in this pass.

Reason not integrated yet:

- The integration would have been speculative without a verified fare payload.

## Iteration 5 - Ryanair integration

Hypothesis:

- Ryanair's official fare-finder pages might expose stable, public route and fare endpoints that are better aligned with low-cost direct markets than Kiwi.

What happened:

- Verified official airport, route, one-way fare, and round-trip fare endpoints.
- Implemented the `ryanair` provider with calendar, exact one-way, and exact return support.
- Benchmarked `kiwi` vs `kiwi + ryanair` on the same live OTP route bundle.

Outcome:

- Clear success.
- The combined engine matched Kiwi coverage and improved price competitiveness on the sampled bundle.

Measured bundle:

- Source: `OTP`
- Destinations tested: `AGP, AMM, BER, BGY, BHX, BLQ, BRS, BVA, CFU, CHQ, CIA, CRL, CTA, DUB, EDI, GDN, GOA, JSI, LBA, MAD`
- Outbound date: `2026-04-18`
- Return date: `2026-04-25`
- Passenger setup: `2 adults`, `1 hand bag`, `0 hold bags`

Measured result:

- One-way:
  - Kiwi-only results: `20/20`
  - Kiwi + Ryanair results: `20/20`
  - Cheaper combined result: `9/20`
  - Worse combined result: `0/20`
- Round-trip:
  - Kiwi-only results: `20/20`
  - Kiwi + Ryanair results: `20/20`
  - Cheaper combined result: `9/20`
  - Worse combined result: `0/20`

Examples where Ryanair beat Kiwi:

- `OTP -> BGY`
  - one-way: `25 EUR` vs `84 EUR`
  - return: `58 EUR` vs `190 EUR`
- `OTP -> BLQ`
  - one-way: `41 EUR` vs `117 EUR`
  - return: `83 EUR` vs `239 EUR`
- `OTP -> MAD`
  - one-way: `121 EUR` vs `200 EUR`
  - return: `194 EUR` vs `493 EUR`

## Current conclusion

The most productive free-provider iteration so far was Ryanair. AZair was a real working addition but did not move the benchmark enough. Wizz was blocked. easyJet remains a promising research lead but not yet a verified fare source. Travelpayouts remains the best future API expansion once a user-owned token is available.
