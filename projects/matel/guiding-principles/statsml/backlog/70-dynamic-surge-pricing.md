# Dynamic Surge Pricing in Ride-Hailing

**Page type:** detail page (backlog kusto-style two-column layout: one `.lang-section` per numbered h2, text left ~45%, canvas right ~55%)
**HTML title tag:** Dynamic Surge Pricing in Ride-Hailing

**Subtitle:** Price as a real-time control signal to balance a two-sided marketplace across space and time

**Intro callout (blue-left-border box):** Case study: a ride-hailing marketplace has perishable inventory and constantly mismatched demand and supply. Surge pricing treats price as a control variable — a closed-loop control system, not a static pricing formula.

## 1. What Is It?

A ride-hailing marketplace has perishable inventory: an idle driver-minute in one neighborhood cannot serve a rider two neighborhoods away. Demand and supply are constantly mismatched in space and time — concert endings, rain, rush hour, airport waves.

Surge pricing treats price as a control variable. When demand in a zone exceeds available supply, a multiplier raises the price there. This does two things at once: it reduces demand (some riders wait or choose alternatives) and it increases supply (nearby drivers reposition toward the higher-paying zone).

**Key point (red-left-border box):** The data science framing: this is a closed-loop control system, not a static pricing formula. The multiplier is the actuator; demand and supply telemetry are the sensors; market balance is the setpoint.

### Visualization (canvas `c1`, 720×340)

Hexagonal city-grid heatmap of surge multipliers with a legend and event marker.

- **Title (bold 14px `#1a5276`, top center):** "Surge multiplier by city zone (illustrative snapshot)".
- **Hex grid:** radius 34px, origin (110,85), odd rows offset; 5 rows × 7 columns of multiplier values:
  - Row 1: 1.0, 1.0, 1.1, 1.0, 1.0, 1.0, 1.0
  - Row 2: 1.0, 1.2, 1.5, 1.3, 1.0, 1.0, 1.1
  - Row 3: 1.1, 1.6, 2.4, 1.8, 1.2, 1.0, 1.0
  - Row 4: 1.0, 1.3, 1.7, 1.4, 1.0, 1.0, 1.2
  - Row 5: 1.0, 1.0, 1.1, 1.0, 1.0, 1.1, 1.4
- **Cell colors:** ≥2.0 → `rgba(231,76,60,0.85)`; ≥1.5 → `rgba(230,126,34,0.75)`; ≥1.2 → `rgba(241,196,15,0.6)`; else `rgba(26,82,118,0.15)`. White 2px hex borders. Cells above 1.0x show "N.Nx" in bold 13px (white text when ≥1.5, `#2c3e50` otherwise).
- **Event marker:** bold 13px `#e74c3c` text "◀ stadium letting out" beside the 2.4x cell.
- **Legend (right side, swatches with `#ccc` border):** "1.0x baseline" `rgba(26,82,118,0.15)`; "1.2–1.4x" `rgba(241,196,15,0.6)`; "1.5–1.9x" `rgba(230,126,34,0.75)`; "2.0x+" `rgba(231,76,60,0.85)`.
- **Caption (bottom center, 12px `#999`):** "Same multiplier for every rider in a cell — pricing the place, not the person"

## 2. What Data Feeds It — From Primitives Up

- **App opens & searches:** demand *intent* before any booking. An open that doesn't convert is still a demand signal.
- **Ride requests:** confirmed demand events with origin, destination, timestamp, product tier.
- **Driver status pings:** GPS location, availability state (idle / en-route / on-trip), heading — the supply side, refreshed every few seconds.
- **Trip telemetry:** pickup ETAs, completion times, cancellations — measures how strained the market actually is.
- **Exogenous context:** weather feeds, event calendars, historical demand curves for the same cell/hour/day-of-week.

Primitives are aggregated into **spatial cells** (hexagonal grid) and **short time windows** (1–5 min). Per cell-window the derived features are: open-app count, request count, idle drivers, ETA percentiles, and a short-horizon demand forecast.

**Key point:** Almost nothing here is rider-specific. Surge is a property of a *place and time*, computed from aggregate flows — every rider in the same cell sees the same multiplier.

### Visualization (canvas `c2`, 720×340)

Left-to-right pipeline flow diagram: primitives → aggregation → derived features → multiplier.

- **Title (bold 14px `#1a5276`, top center):** "From raw events to a per-cell multiplier".
- **Column 1 (primitives):** five boxes 140×34 at x=30 (fill `rgba(26,82,118,0.12)`, stroke `#2980b9`): "App opens", "Ride requests", "Driver GPS pings", "Trip completions", "Weather / events"; gray `#888` arrows converging to column 2.
- **Column 2 (aggregation):** box 155×80 at (225,115) (fill `rgba(39,174,96,0.15)`, stroke `#27ae60`), three lines: "Aggregate per" / "hex cell ×" / "1–5 min window"; arrows fan out to column 3.
- **Column 3 (derived features):** two boxes 160×62 (fill `rgba(230,126,34,0.15)`, stroke `#e67e22`): "Demand forecast" / "(next 10–15 min)" and "Effective supply" / "(idle + arriving)"; arrows converge to column 4.
- **Column 4 (multiplier):** box 110×58 at (600,125) (fill `rgba(231,76,60,0.15)`, stroke `#e74c3c`, text `#c0392b`): "Imbalance ratio" / "→ multiplier".
- **Note (12px `#666`, under column 4):** "smoothed over neighbors & time, quantized to steps".
- **Caption (bottom center, 12px `#999`):** "Sensors → aggregation → forecast → actuator: a control loop refreshed every few minutes"

## 3. How the Multiplier Is Computed

**Step 1 — imbalance:** per cell, compare forecast demand vs. effective supply (idle drivers + drivers finishing trips nearby soon). A ratio well above 1 means unmet demand.

**Step 2 — elasticity:** historical data gives two response curves: how demand falls as price rises (rider elasticity) and how supply grows as earnings rise (driver repositioning response).

**Step 3 — clearing:** pick the multiplier where the adjusted demand and adjusted supply curves roughly intersect — the price at which requested rides ≈ serviceable rides.

**Step 4 — smoothing:** raw multipliers would flicker. Values are smoothed across neighboring cells (spatial) and across windows (temporal), and quantized into steps so the displayed price is stable for the duration of a booking decision.

**Key point:** Elasticity estimation is the statistically hard part: price changes are not random experiments — they happen exactly when demand spikes. Naive regression of demand on price is confounded by the very signal that triggered the price. Platforms rely on switchback experiments and natural boundaries (adjacent cells, multiplier quantization steps) to identify the causal response.

### Visualization (canvas `c3`, 720×340)

Supply/demand curve chart with the clearing multiplier at the intersection.

- **Title (bold 14px `#1a5276`, top center):** "Picking the multiplier where the market clears (illustrative)".
- **Axes:** origin x=100, baseline y=285, plot 520×225, stroke `#999`; x label "price multiplier →", y label (rotated) "rides per window"; x ticks at 1.0x, 1.5x, 2.0x, 2.5x, 3.0x (12px `#666`).
- **Demand curve (red `#e74c3c`, width 3):** height = 210·exp(−0.75·(m−1.0)) px above baseline for m in [1.0, 3.0] — high at 1.0x, falling exponentially.
- **Supply curve (green `#27ae60`, width 3):** height = 60 + 128·(1 − exp(−1.1·(m−1.0))) px — low at 1.0x, rising and saturating.
- **Intersection:** at m★ ≈ 1.74 — dashed `#1a5276` vertical drop-line (5/5 dash), filled 6px `#1a5276` dot, bold 13px label "clearing multiplier".
- **Curve labels (bold 13px):** red "demand (riders willing to book)" near left top; green "supply (drivers responding)" near right.
- **Zone annotations (12px `#999`):** "left of the point: shortage" and "right: glut" near the baseline.
- **Caption (bottom center, 12px `#999`):** "Both curves are estimated from historical response — the confounded-elasticity problem lives here"

## 4. Feedback Dynamics & Failure Modes

Because the price changes behavior, and behavior changes the price, the system can oscillate:

- **Overshoot:** too many drivers converge on a surging zone, supply floods, multiplier collapses, drivers who repositioned earn nothing extra — and learn to distrust the signal.
- **Cobweb cycles:** demand and supply react with different lags (riders in seconds, drivers in minutes), so an aggressive controller ping-pongs between shortage and glut.
- **Forecast leakage:** if the demand forecast is trained on data that already includes surge suppression, the model learns dampened demand and under-forecasts true need.
- **Boundary artifacts:** hard cell edges create price cliffs — riders walk a block to cross a hexagon boundary, which shifts observed demand and distorts the next window's signal.

**Key point:** Classic control-theory lessons apply: dampen the actuator, forecast the lag, and validate the sensor. Most production incidents in dynamic pricing are feedback problems, not model-accuracy problems.

### Visualization (canvas `c4`, 720×340)

Time-series chart: demand spike, lagged supply response, and the multiplier tracking the gap.

- **Title (bold 14px `#1a5276`, top center):** "Event spike: demand leads, supply lags, multiplier bridges (illustrative)".
- **Axes:** origin x=80, baseline y=275, plot 580×215, stroke `#999`; x label "minutes after event ends →" with ticks at 0, 15, 30, 45, 60.
- **Demand curve (red `#e74c3c`, width 3):** height = 40 + 155·exp(−(t−8)²/180) (ramped in for t<2) — sharp Gaussian spike peaking at t=8 min.
- **Supply curve (green `#27ae60`, width 3):** height = 48 + 115·exp(−(t−22)²/320) — broader Gaussian peaking at t=22 min.
- **Multiplier curve (orange `#e67e22`, width 2.5, dashed 7/5):** height = 30 + max(0, demand − supply)·0.9 — tracks the demand-supply gap.
- **Labels (bold 13px):** red "demand" at t≈10; green "supply (drivers repositioning)" at t≈25; orange "multiplier (tracks the gap)" at t≈2.
- **Lag annotation:** dotted `#1a5276` vertical lines (3/3 dash) at t=8 and t=22 with centered 12px label "~14 min supply lag" between them.
- **Caption (bottom center, 12px `#999`):** "The controller must anticipate the lag — react too hard and the repositioned supply arrives into a collapsed price"

## Regeneration instructions

- **Layout:** backlog detail-page style — h1 with `border-bottom: 2px solid #2980b9`, `.subtitle`, one `.intro` callout, then one `.lang-section` per numbered h2 (also blue-underlined). Each section holds a `table.layout` (border-collapse, full width) with one `<tr>`: left `td.text-col` (45%) for paragraphs/bullets/`.key-point`, right `td.viz-col` (55%) for the canvas.
- **Page CSS:** body system-ui sans-serif, background `#fff`, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; h2 1.3rem `#1a5276`; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8` with `border-left: 3px solid #2980b9`; `.key-point` background `#f8f9fa` with `border-left: 3px solid #e74c3c`, 0.9rem; ul 0.92rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** all four canvases declare intrinsic `width="720" height="340"`; a shared `setupCanvas(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, plus `#2980b9`, yellow `rgba(241,196,15,0.6)` in the heatmap, gray text `#666`/`#999`.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
