# Up-Front & Personalized Pricing in Ride-Hailing

**Page type:** detail page (backlog kusto-style two-column layout: one `.lang-section` per numbered h2, text left ~45%, canvas right ~55%)
**HTML title tag:** Up-Front & Personalized Pricing in Ride-Hailing

**Subtitle:** Replacing a transparent rate card with per-trip price prediction

**Intro callout (blue-left-border box):** Case study: up-front pricing estimates what each side of the market will accept, from behavioral and contextual data. Rider price and driver pay are decoupled, and the spread between them becomes an optimization target.

## 1. From Rate Card to Prediction

The original model was a formula anyone could verify: **base + rate × time + rate × distance**, times the zone's surge multiplier. Price was a function of the trip.

Up-front pricing replaces the formula with a model output: a single quoted number, fixed before booking, produced from marketplace conditions, route features, and historical behavior. Price becomes a prediction of what this trip instance will clear at — a function of the trip *and its context*.

**Key point (red-left-border box):** The defining change for a data scientist: rider price and driver pay are decoupled. Under a rate card they were the same number minus a fixed commission. Under up-front pricing each is estimated independently, and the spread between them becomes an optimization target.

### Visualization (canvas `c1`, 720×340)

Two-box comparison: rate card formula vs up-front model, joined by an orange arrow.

- **Title (bold 14px `#1a5276`, top center):** "Rate card (verifiable formula) vs up-front quote (model output)".
- **Left box** at (40,55), 285×230: fill `rgba(39,174,96,0.08)`, stroke `#27ae60` width 2. Header (bold 13px `#27ae60`): "RATE CARD". Body (14px `#2c3e50`, centered lines): "price = base" / "+ rate × minutes" / "+ rate × km" / "× zone multiplier". Footer (12px `#666`): "same trip → same price" / "driver pay = price − fixed cut".
- **Arrow:** orange `#e67e22` width-3 horizontal line (338,170)→(388,170) with filled triangle head.
- **Right box** at (410,55), 285×230: fill `rgba(26,82,118,0.08)`, stroke `#1a5276` width 2. Header (bold 13px `#1a5276`): "UP-FRONT MODEL". Body (14px `#2c3e50`): "price = f( route, traffic," / "supply/demand, weather," / "rider history, session… )". Footer: 12px `#666` "same trip → prediction per instance"; then 12px `#e74c3c` "rider price and driver pay" / "estimated independently".
- **Caption (bottom center, 12px `#999`):** "The formula was auditable by anyone with a stopwatch; the model is auditable by no one outside"

## 2. Data Inputs — From Primitives to Features

- **Marketplace state:** live supply/demand per cell, traffic, weather — the same inputs surge uses, consumed here as model features.
- **Route geometry:** GPS traces aggregated into turn-by-turn time predictions, pickup difficulty, and likely tolls — richer than raw mileage.
- **Behavioral history:** past trips, booking-vs-abandon decisions at quoted prices, time-of-day patterns, origin/destination regularity (a repeated home→office pair is a commute; airport pickups signal travel).
- **Session context:** battery level, comparison behavior (checking several tiers vs booking instantly), time spent on the fare screen. Company patents describe device-interaction signals — typing speed, tap patterns, phone angle — as possible urgency indicators.

**Key point:** Feature engineering does the heavy lifting: raw primitives (GPS points, taps, timestamps) are compressed offline into features like "price sensitivity score" or "urgency estimate," so the online model only evaluates a compact vector at request time.

### Visualization (canvas `c2`, 720×340)

Three-column pipeline diagram: primitives → engineered features → per-trip predictions, connected by thin `#bbb` lines.

- **Title (bold 14px `#1a5276`, top center):** "Raw primitives → engineered features → per-trip predictions".
- **Column 1 (x=110, purple `#8e44ad`, boxes 180 wide):** "GPS points", "taps & timestamps", "quotes shown / booked", "device state", "trip records". Column footer (bold 12px `#8e44ad`): "PRIMITIVES (raw logs)".
- **Column 2 (x=360, orange `#e67e22`, boxes 195 wide):** "route time estimate", "pickup difficulty", "urgency estimate", "price sensitivity", "trip-pattern regularity". Footer (bold 12px `#e67e22`): "FEATURES (offline, cached)".
- **Column 3 (x=610, blue `#1a5276`, boxes 160 wide, vertically offset):** "rider ceiling", "driver floor", "match choice". Footer (bold 12px `#1a5276`): "PREDICTIONS (ms, online)".
- **Boxes:** 30px tall, fill = column color at ~8% alpha (hex + "14"), 1.2px colored stroke, 12px `#2c3e50` centered text; rows 48px apart starting y=62. Straight lines connect column 1 rows to column 2 rows; alternating (checkerboard) lines connect column 2 to column 3.

## 3. Two-Sided Estimation: Ceiling and Floor

The core modeling problem is estimating two quantities per trip:

- **Rider ceiling:** the highest quote this rider is likely to accept for this trip — learned from historical accept/abandon decisions across quoted prices (each quote shown is effectively an experiment).
- **Driver floor:** the lowest offer a nearby driver is likely to accept — learned from accept/decline history, current earnings, and local alternatives.

The platform's take is the spread between the two. Matching adds a third layer: choosing *which* driver sees the offer, based on predicted acceptance — "the right trip at the right price to the right driver."

**Key point:** Economists call this moving toward first-degree price discrimination — pricing each transaction near individual willingness to pay. Journalistic experiments have measured meaningfully different quotes for the same route at the same time, consistent with per-instance rather than per-route pricing; critics have labeled the practice "surveillance pricing," and it is an active regulatory question.

### Visualization (canvas `c3`, 720×340)

Band diagram: rider ceiling line above, driver floor line below, double-headed arrow marking the spread.

- **Title (bold 14px `#1a5276`, top center):** "Per-trip estimation: rider ceiling, driver floor, and the spread between".
- **Ceiling band:** horizontal band at y=95 (x=160, width 440): fill `rgba(231,76,60,0.15)` 32px tall, center line `#e74c3c` width 3. Left labels (right-aligned): bold 13px `#e74c3c` "rider ceiling", 11px `#999` "(estimated max accept)".
- **Floor band:** same geometry at y=235: fill `rgba(39,174,96,0.15)`, line `#27ae60` width 3. Labels: bold 13px `#27ae60` "driver floor", 11px `#999` "(estimated min accept)".
- **Spread arrow:** vertical `#1a5276` width-2.5 line at midpoint with arrowheads toward both bands; bold 13px label "spread = platform take".
- **Markers:** red 6px dot just under the ceiling at ~78% width, label "quote set just under ceiling"; green 6px dot just over the floor, label "offer set just over floor".
- **Caption (12px `#666`, two lines, bottom center):** "Both bands are model estimates with uncertainty — quoting too close to the true ceiling loses the booking;" / "offering too close to the true floor loses the driver. The models price that risk on every trip."

## 4. The Driver Side: Floor Estimation at Scale

Drivers see a take-it-or-leave-it offer with seconds to decide — pickup distance, trip length, and pay, but not the rider's quoted price. Every accept/decline at an offered price is a labeled training example for the floor model.

- **Acceptance modeling:** predicted probability a specific driver accepts a specific offer, conditioned on their history, current session earnings, and time since last trip.
- **Sequential dependence:** a driver who just declined two offers is more likely to accept the next — the floor is a moving target the model tracks within a session.
- **Take rate as outcome:** the platform's share of each fare varies per trip; reporting has documented spreads exceeding 50% on some trips, wide dispersion being the signature of independently estimated ceilings and floors.

**Key point:** A selection effect worth noting: drivers who systematically decline low offers teach the model their floor is high. The training data is generated by strategic agents responding to the model itself — feedback between model and data, not a fixed distribution.

### Visualization (canvas `c4`, 720×340)

Histogram comparison: fixed-cut spike vs wide up-front take-rate distribution.

- **Title (bold 14px `#1a5276`, top center):** "Take rate per trip — fixed cut vs independently estimated spread (illustrative)".
- **Axes:** origin x=90, baseline y=280, plot 540×210, stroke `#ccc`; x scale 0–70% with labels at 0/10/20/30/40/50/60/70%; x-axis caption (12px `#666`): "platform share of the fare (take rate) →".
- **Rate-card spike:** tall green bar (fill `rgba(39,174,96,0.5)`) from 24% to 27%, 90% plot height; two-line bold 12px `#27ae60` label above: "rate card era:" / "fixed ~25% cut".
- **Up-front histogram (fill `rgba(26,82,118,0.45)`, stroke `#1a5276`), heights as fraction of plot height per 5%-wide bin starting at take rate:** [0, 0.02], [5, 0.05], [10, 0.10], [15, 0.20], [20, 0.34], [25, 0.44], [30, 0.40], [35, 0.32], [40, 0.24], [45, 0.17], [50, 0.12], [55, 0.08], [60, 0.05], [65, 0.03]. Bold 12px `#1a5276` label: "up-front era: wide per-trip dispersion".
- **Tail annotation:** dashed red `#e74c3c` vertical line (4/4 dash) at 50%; 12px red text: "reported tail: >50%" / "on some trips".
- **Caption (bottom center, 12px `#999`):** "Dispersion is the signature: when ceiling and floor are estimated independently, the spread varies per trip"

## 5. Serving, Speed & Measurement

- **Precomputation:** baseline demand patterns and per-user features are computed offline and cached; the request-time model only merges them with live variables (traffic, current supply).
- **Low-latency inference:** the full quote — route prediction, ceiling estimate, spread decision — runs in milliseconds across distributed serving infrastructure, for every fare-screen open, not just bookings.
- **Continuous experimentation:** price variants are A/B tested at massive scale; conversion at each quoted price is the label that retrains the ceiling model. The system generates its own training data.
- **External auditability:** because inputs are personal and the model is private, the only outside measurement tool is paired testing — identical requests from different accounts/devices — which is exactly how journalists and researchers have studied it.

**Key point:** The measurement asymmetry is the study-worthy part: the platform observes every counterfactual quote, while riders, drivers, and auditors observe one draw each. Reasoning about such systems from the outside is a hard inference problem in its own right.

### Visualization (canvas `c5`, 720×340)

Scatter plot: paired-testing quotes for the same route/time, spread bracket at right.

- **Title (bold 14px `#1a5276`, top center):** "Paired testing: identical route & time, quotes across accounts (illustrative)".
- **Axes:** origin x=90, baseline y=280, plot 530×210, stroke `#ccc`; x label (12px `#666`): "request instances (different accounts / devices, same route & moment)"; y label (rotated): "quoted price".
- **Reference line:** dashed green `#27ae60` (4/4 dash, width 2) horizontal at 45% of plot height; bold 12px green label: "old rate-card price (single value)".
- **Points:** 26 dots, 6px radius, fill `rgba(26,82,118,0.55)`; positions generated by a seeded LCG (seed 7, multiplier 16807 mod 2147483647): x uniform over the plot with 30px margins, y deviating from the rate-card line by (rnd − 0.35) × 0.55 of plot height, clamped inside the plot — i.e. scattered mostly above the old price.
- **Spread bracket:** red `#e74c3c` bracket to the right of the plot spanning min to max point y; rotated bold 12px red label: "observed spread".
- **Caption (bottom center, 12px `#999`):** "Each auditor sees one draw; the platform sees the whole distribution — the measurement asymmetry"

## Regeneration instructions

- **Layout:** backlog detail-page style — h1 with `border-bottom: 2px solid #2980b9`, `.subtitle`, one `.intro` callout, then one `.lang-section` per numbered h2 (also blue-underlined), five sections total. Each section holds a `table.layout` (border-collapse, full width) with one `<tr>`: left `td.text-col` (45%) for paragraphs/bullets/`.key-point`, right `td.viz-col` (55%) for the canvas.
- **Page CSS:** body system-ui sans-serif, background `#fff`, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; h2 1.3rem `#1a5276`; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8` with `border-left: 3px solid #2980b9`; `.key-point` background `#f8f9fa` with `border-left: 3px solid #e74c3c`, 0.9rem; ul 0.92rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** all five canvases declare intrinsic `width="720" height="340"`; a shared `setupCanvas(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, plus `#8e44ad`, gray text `#666`/`#999`.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
