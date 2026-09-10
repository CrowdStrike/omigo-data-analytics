# Personalized Up-Front Pricing — The Quote as a Prediction

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one h2 + table per aspect, plus philosophy callouts and a summary table)
**HTML title tag:** Personalized Up-Front Pricing — The Quote as a Prediction

**Subtitle:** Two phones on the same corner request the same ride and may receive different quotes. The interesting part is the data science: what the phone emits, what gets derived from it, and what gets inferred by linking it over time.

## Callout (philosophy box, top)

**The question:** A ride quote used to be a formula over the trip. Up-front pricing turned it into a model output. What data feeds that model — and how does a raw phone signal become a pricing feature?

**The answer:** Signals are used at three levels: read *directly* from the device (location, timestamps, taps), *enriched* within a session (speed from GPS deltas, dwell time on the fare screen), and *linked* across sessions into behavioral estimates (trip regularity, price sensitivity). The model predicts two numbers per trip — the rider's likely acceptance ceiling and the driver's likely acceptance floor — and the platform's margin is the spread.

**An important caveat up front:** this is a best-guess reconstruction, in two senses. The models themselves output probabilistic estimates that are wrong on many individual trips. And the description of how they work is assembled from patents, paired-price experiments, and reporting — platforms dispute parts of it, and the practice (sometimes labeled "surveillance pricing" in policy debates) is not a settled, universally accepted industry standard.

## 1. From Formula to Prediction

**Obj-title:** The Rate Card Becomes a Model

The original taxi model was a formula anyone could verify: price was a function of the trip. Up-front pricing replaces the formula with a single quoted number, fixed before booking, produced by a model from the trip *and its context*.

Math-box:

**Rate card:**
`price = $2.50 + $0.30/min + $1.75/km × zone multiplier`
Same trip → same price, verifiable with a stopwatch.

**Up-front quote:**
`price = f(route, traffic, supply/demand, behavioral history, session context…)`
Same trip → a per-instance prediction.

- **The defining shift:** rider price and driver pay are decoupled — under a rate card they were the same number minus a fixed cut; here each is estimated independently
- **What changed for measurement:** a formula is auditable from a single receipt; a model output can only be studied statistically, across many quotes

### Visualization (canvas `canvas1`, 720×360)

Dot plot: rate-card era (all quotes identical) vs up-front era (quotes disperse), split by a vertical divider.

- **Layout:** origin at (70, 300), plot width 600, plot height 240. Axes `#1a5276`, width 2. Price scale $14–$38 on y.
- **Y ticks:** $15, $20, $25, $30, $35 in gray `#666` with `#eee` gridlines.
- **Axis labels:** x: "Same route, same moment — 24 request instances"; y (rotated): "Quoted price ($)" — `#1a5276`, 13px.
- **Divider:** dashed gray `#999` vertical line (dash 4/4) at the plot midpoint.
- **Left half:** green `#27ae60` horizontal rate-card line (width 2) at $24; 12 dots radius 5 in `rgba(39,174,96,0.8)` sitting exactly on the line, evenly spaced.
- **Right half:** 12 dispersed dots radius 5 in `rgba(26,82,118,0.7)`, prices p = 24 + (rand − 0.42)·16 using seeded PRNG mulberry32(11), evenly spaced.
- **Era labels (bold 12px, near top of plot):** green "rate-card era: every quote identical" over the left half; blue `#1a5276` "up-front era: one prediction per request" over the right half.
- **Title (bold 14px `#1a5276`, top center):** "From One Price per Trip to One Prediction per Request".

## 2. One Signal, Three Uses: Direct, Enriched, Linked

**Obj-title:** The Feature-Engineering Ladder

Follow a single GPS point up the ladder. Read directly, it is a position — which zone the request comes from. Enriched within the session, consecutive points yield speed, heading, and route geometry. Linked across sessions, the same origin five mornings a week becomes an inferred commute.

- **Direct:** fields consumed as-is — pickup location, destination entered, timestamps, product-tier taps, device state (OS, app version)
- **Enriched:** derived within one session — speed and heading from GPS deltas, route time estimates from map-matched traces, pickup difficulty, dwell time on the fare screen from tap timestamps
- **Linked (indirect):** inferred across sessions — origin–destination regularity → trip purpose; accept/abandon outcomes at past quotes → price-sensitivity estimate; tier-comparison behavior → urgency estimate

Math-box:

**One GPS point ≈ (lat, lon, t):**
Direct: `pickup zone` — joins to live supply/demand
Enriched: `Δposition/Δt = speed` — walking vs driving → pickup difficulty
Linked: `same origin, 8:45am, 5 days/week` → commute → schedule-constrained trip

- **Why the ladder exists:** raw primitives are compressed offline into compact features so the online model evaluates a small vector in milliseconds, at every fare-screen open
- **Confidence drops as you climb:** a position is a measurement; a speed is a derivation; a "commute" or an "urgency score" is an inference that can simply be wrong for this person today

### Visualization (canvas `canvas2`, 720×380)

Three-column box diagram: signal ladder from direct reading to cross-session inference.

- **Title (bold 14px `#1a5276`, top center):** "One Signal, Three Uses: Direct Reading → Session Enrichment → Cross-Session Inference".
- **Columns at x = 118, 360, 602.** Boxes 32px tall, width 190 (col 1–2) / 200 (col 3), fill = column color + 16 alpha hex, stroke = column color width 1.3, 12px `#2a2a2a` centered text; rows start at y=66, row gap 54 (col 3 offset +27).
  - Column 1 (purple `#8e44ad`): "GPS point stream", "tap & screen timestamps", "destination entered", "device state (OS, version)".
  - Column 2 (orange `#e67e22`): "speed & heading", "route time estimate", "fare-screen dwell time", "pickup difficulty".
  - Column 3 (blue `#1a5276`): "trip-purpose regularity", "price-sensitivity estimate", "urgency estimate".
- **Connections:** light gray `#bbb` lines, width 1 — all four rows connected 1:1 from column 1 to column 2; selective pairs from column 2 to column 3: (0→0), (1→0), (2→1), (2→2), (3→2).
- **Column headers (bold 12px, y=322):** purple "DIRECT — measured"; orange "ENRICHED — derived in session"; blue "LINKED — inferred over sessions".
- **Bottom note (12px `#999`, centered, y=352):** "Confidence falls left to right: a measurement → a derivation → a best guess about a person".

## 3. Two Predictions and a Spread

**Obj-title:** Ceiling, Floor, and the Gap

The features feed two estimation problems. Every displayed quote is a labeled data point — accepted or abandoned — that updates the rider's acceptance-ceiling estimate. Every driver accept/decline on a take-it-or-leave-it offer trains the acceptance-floor estimate. The two are modeled independently.

Math-box:

**Trip A (illustrative):** rider quoted `$31` (ceiling est. $33), driver offered `$17` (floor est. $16) → spread `$14 = 45% take`
**Trip B, same route next day:** rider quoted `$24`, driver offered `$19` → spread `$5 = 21% take`

Wide per-trip dispersion in the take rate is the statistical signature of independently estimated ceilings and floors.

- **Estimates, not facts:** both bands carry uncertainty — quote too close to the true ceiling and the booking is lost; offer too close to the true floor and the driver declines. The model prices that risk on every trip.
- **Strategic training data:** a driver who habitually declines low offers teaches the model their floor is high — the data is generated by agents responding to the model, not drawn from a fixed distribution

### Visualization (canvas `canvas3`, 720×360)

Band diagram: rider ceiling band on top, driver floor band below, double-headed spread arrow between.

- **Title (bold 14px `#1a5276`, top center):** "Per-Trip Estimation: Quote Near the Ceiling, Offer Near the Floor".
- **Bands:** horizontal band from x=170, width 430. Ceiling at y=100: fill `rgba(231,76,60,0.12)` 28px tall, center line `#e74c3c` width 3. Floor at y=250: fill `rgba(39,174,96,0.12)` 28px tall, center line `#27ae60` width 3.
- **Band labels (bold 13px, right-aligned left of bands):** red "rider ceiling"; green "driver floor"; beneath each in 11px `#999`: "(est. max accept ± uncertainty)" and "(est. min accept ± uncertainty)".
- **Spread arrow:** vertical blue `#1a5276` line width 2.5 at band midpoint x with filled arrowheads pointing at both bands; bold 13px blue label to its right: "spread = platform take".
- **Quote dot:** red filled circle radius 6 at 80% band width, just under the ceiling line; 12px label "quote set just under the ceiling".
- **Offer dot:** green filled circle radius 6 at 80% band width, just over the floor line; 12px label "offer set just over the floor".
- **Risk note (12px `#666`, centered, two lines at y=310/328):** "Both bands are best-guess estimates: quote too close to the true ceiling and the booking is lost;" / "offer too close to the true floor and the driver declines. The model prices that risk on every trip."

## 4. Measuring the System From Outside

**Obj-title:** One Draw per Observer

The platform computes a quote for every fare-screen open, so it observes the full distribution, including quotes shown but never booked. An outside observer — a rider, a researcher, a regulator — sees one realization per request. Estimating a per-instance pricing function from single draws is a genuinely hard inference problem.

Math-box:

**The standard outside instrument:** paired testing — identical requests from different accounts and devices at the same moment, then compare quotes.

Experiments with this design have measured different quotes for the same route at the same time, consistent with per-instance rather than per-route pricing.

- **Why conclusions stay tentative:** paired tests can show that quotes differ, but not *which* features drove the difference — route model noise, live market state, and personalization are confounded in a single number
- **The open debate:** platforms acknowledge route- and market-based variation but dispute person-level willingness-to-pay pricing; regulators are actively studying it. The honest summary is: dispersion is measured, its decomposition is contested.

### Visualization (canvas `canvas4`, 720×360)

Histogram: the quote distribution the platform sees, with the single observed draw highlighted.

- **Title (bold 14px `#1a5276`, top center):** "One Request Observes One Draw From the Quote Distribution".
- **Layout:** origin at (80, 290), plot width 580, plot height 220. Axes `#1a5276`, width 2.
- **Axis labels (13px `#1a5276`):** x: "Quoted price for the identical trip ($)"; y (rotated): "Share of quotes computed".
- **Bars (price, share):** [$18, 0.03], [$20, 0.08], [$22, 0.16], [$24, 0.24], [$26, 0.20], [$28, 0.13], [$30, 0.08], [$32, 0.05], [$34, 0.02], [$36, 0.01]. Bar width = plotWidth/11 minus 8px gap; heights = share × plotHeight × 3.2. Normal bars fill `rgba(26,82,118,0.35)` stroke `#1a5276`; the $30 bar highlighted fill `rgba(231,76,60,0.75)` stroke `#e74c3c`. Price labels "$18"… under each bar in 11px `#666`.
- **Highlight annotation:** red `#e74c3c` vertical pointer line above the $30 bar; bold 12px red "the one quote this request saw"; 11px red "one draw cannot separate route noise, market state, and personalization".
- **Bottom note (12px `#999`, centered):** "Paired testing — identical requests from different accounts at the same moment — is the standard outside probe".

## 5. The Complete Picture

Summary table (`.summary-table`, header row + 4 rows):

| Phone signal | Direct use | Enriched into | Linked across sessions |
|---|---|---|---|
| **GPS point stream** | Pickup zone, live supply/demand join | Speed, heading, route time, pickup difficulty | Origin–destination regularity → trip purpose |
| **Tap & screen timestamps** | Product tier selected | Fare-screen dwell time, tiers compared | Booking-vs-abandon history → price sensitivity |
| **Quotes shown & outcomes** | Conversion event | Quote-to-book latency | Acceptance-ceiling estimate |
| **Device state** | OS, app version | Session stability | Patents describe device-interaction signals as candidate features — public evidence of use is limited |

## Callout (philosophy box, bottom)

**One sentence:** An up-front quote is a layered estimation problem — signals read directly from the phone, enriched into derived features, and linked into behavioral inferences — and every layer up the ladder trades measurement for best guess, which is exactly why both the individual estimates and the external accounts of the system remain contested.

## Regeneration instructions

- **Layout:** detail page. h1, `.subtitle`, opening `.philosophy` callout, then per aspect: `<h2>N. Title</h2>` (h2 1.4em `#1a5276`, bottom border `2px solid #2980b9`) followed by a one-row `.obj-table` — left `<td>` (45%) holds `.obj-title`, paragraph, `.math-box`, bullets; right `<td>` (55%, centered) holds the canvas. Section 5 is a `.summary-table`; page closes with a `.philosophy` callout.
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` `#1a5276`; ul 0.9em `#333`. No nav bar, no back/home links.
- **Callout style:** `.philosophy` — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em.
- **Math box:** `.math-box` — background `#f8fafb`, border `1px solid #e0e0e0`, radius 6px, padding 16px 20px, 0.9em; `code` background `#eef2f7`.
- **Summary table:** `.summary-table` — 0.9em, th background `#f0f4f8` `#1a5276`, td padding 10px 14px, borders `1px solid #e0e0e0`.
- **Canvas:** intrinsic sizes 720×360 (canvases 1, 3, 4) and 720×380 (canvas 2); shared `setupCanvas(id, w, h)` scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Canvas 1 uses a seeded PRNG `mulberry32(11)` for reproducible dot dispersion. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, gray text `#666`/`#999`, accent `#2980b9`.
