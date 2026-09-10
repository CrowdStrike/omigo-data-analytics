# Self-Driving — Distribution Patterns

**Page type:** detail page (three-column obj-table layout: text left 38%, two canvases 31% each, one table per section)
**HTML title tag:** Self-Driving — Distribution Patterns

## Detection Confidence (The Dangerous Middle Zone)

**Pitfall label (uppercase, `#795548`):** BIMODAL BETA

Mass piles near 1.0 (confident detections) and near 0 (confident negatives), with a thin middle (0.3–0.7) holding ~10% of the mass in this simulation. The thinness of the middle works as a safety read: a fat middle means the car too often doesn't know what it's seeing.

- Mass near 1.0 (0.85–1.0) = confident correct detections
- Mass near 0 (0–0.15) = confident negatives (nothing there)
- Thin middle (0.3–0.7) = uncertainty zone (~10% here)
- Fat middle = frequent uncertainty; thin middle = decisive model

### Visualization (canvas `canvas1`, 420×340)

Histogram (shared `drawHistogram` utility): U-shaped bimodal confidence distribution.

- **Title (bold 13px, `#1a5276`, top center):** "Object Detection Confidence — Bimodal Beta".
- **Data (seeded RNG mulberry32(42)):** 3000 samples — 45% high-confidence cluster `1 - U^3 * 0.15` (near 0.85-1.0); 45% low-confidence cluster `U^3 * 0.15` (near 0-0.15); 10% uniform middle `0.3 + U*0.4`.
- **Bins:** 40; x range data min-max, tick labels 2 decimals at 6 positions; x-axis label "Confidence Score"; rotated y-axis label "Frequency".
- **Bars:** fill `rgba(52,73,94,0.4)`, stroke `#1a5276`.
- **Density line + SE band (standard for all histograms on this page):** Gaussian-smoothed counts (sigma 1.5 bins), line `#2c3e50` width 2, 95% band `rgba(52,73,94,0.2)` using effective N clamped to [30, 200].
- **Layout:** white background, gray `#999` L-axes, padding top 40 / right 20 / bottom 50 / left 50.

### Visualization (canvas `canvas1b`, 400×340)

Safety-zone strip diagram with proportional mass bars and a verdict box.

- **Title:** "Safety Zone Breakdown".
- **Zone strip (x 30, y 55, height 50, width = canvas−60):** three segments proportional to the confidence axis — 0-0.3 green `rgba(39,174,96,0.7)` labeled "SAFE / NEGATIVES"; 0.3-0.7 red `rgba(231,76,60,0.7)` labeled "DANGER / ZONE"; 0.7-1.0 blue `rgba(46,134,193,0.75)` labeled "CONFIDENT / DETECTIONS" (white bold 11px, two lines each). Scale labels 0.0 / 0.3 / 0.7 / 1.0 in `#999` 10px below the strip.
- **Mass bars (below strip, height 30):** segment widths proportional to actual sample fractions in each zone, same three colors at 0.6-0.65 alpha, with white bold 12px percentage labels (computed from the data, approx 45% / ~10% / 45%); left-aligned caption "Actual mass distribution ↑" in `#555` 11px.
- **Annotation:** red `#e74c3c` downward arrow into the danger zone with bold 10px label `"Car doesn't know"`.
- **Verdict box (full-width, 55px tall):** if middle-zone percent < 15 — fill `rgba(39,174,96,0.15)`, border `#27ae60`, bold 14px "VERDICT: SAFE MODEL"; else red equivalent "VERDICT: UNSAFE MODEL". Sub-line 11px `#333`: "Danger zone has only <mid%>% of detections".

## Disengagement Intervals (Weibull = Maturity)

**Pitfall label (uppercase, `#2980b9`):** WEIBULL (MATURITY METRIC)

Early software = exponential (k=1, memoryless random failures). Mature software = Weibull k>2 (failures cluster in specific conditions). A shift from exponential to high-k Weibull is consistent with graduating from random bugs to known hard scenarios — the shape parameter works as a maturity signal.

- Early: k=1 (exponential) = random failures everywhere
- Mature: k>2 (Weibull) = failures cluster in specific scenarios
- Shift from k=1 to k>2 = "random bugs" → "known hard cases"
- Shape parameter k = a usable maturity signal

### Visualization (canvas `canvas2`, 420×340)

Histogram: Weibull-distributed hours between disengagements.

- **Title:** "Disengagement Intervals — Weibull (k=3)".
- **Data:** 2000 samples from Weibull inverse CDF `50 * (-ln(1-u))^(1/3)` (k=3, λ=50 hours).
- **Bins:** 40; x-axis label "Hours Between Disengagements"; tick labels 2 decimals; y-axis label "Frequency".
- **Bars:** fill `rgba(230,126,34,0.35)`, stroke `#1a5276`. Standard density line + SE band.

### Visualization (canvas `canvas2b`, 400×340)

ECDF comparison: early (exponential) vs mature (Weibull k=3) systems.

- **Title:** "ECDF: Early (k=1) vs Mature (k=3)".
- **Data:** 500 samples each — early: exponential with λ=50 (`-50*ln(1-u)`); mature: Weibull `50*(-ln(1-u))^(1/3)`. X capped at 150 hours.
- **Curves:** early ECDF dashed `rgba(231,76,60,0.85)` width 2.5 (dash 6/4); mature ECDF solid `rgba(41,128,185,0.9)` width 2.5.
- **Shaded gap:** area between the analytic exponential CDF and Weibull CDF filled `rgba(230,126,34,0.15)`.
- **Annotation:** orange `#e67e22` vertical double-headed arrow between the curves at 25% of x-range, bold 10px label "Maturity gap".
- **Legend (top left, line samples):** dashed red "Early (k=1, random)"; solid blue "Mature (k=3, clustered)".
- **Bottom caption (bold 10px `#1a5276`, centered):** "Mature: failures concentrate at specific hours (S-curve steeper)".
- **Axes:** x ticks 0 decimals at 6 positions, label "Hours"; rotated y-axis label "Cumulative Probability"; padding top 42 / right 20 / bottom 50 / left 55.

## Pedestrian Prediction Error (No Safe Margin)

**Pitfall label (uppercase, `#27ae60`):** FAT-TAILED (CAUCHY)

Tight center for straight-walking pedestrians, extreme errors for sudden direction changes — simulated here as Cauchy. Tails this heavy have no finite variance, so a Gaussian error model underestimates the worst case, and under this shape any fixed safety margin is eventually exceeded.

- Tight center = predictable straight-line walkers
- Extreme tails = sudden direction changes (children, distracted)
- Cauchy tails never converge (no finite variance)
- Under this shape, any fixed safety margin is eventually exceeded

### Visualization (canvas `canvas3`, 420×340)

Histogram: Cauchy-distributed prediction errors.

- **Title:** "Pedestrian Prediction Error — Fat-Tailed (Cauchy)".
- **Data:** 4000 samples of `tan(π(u - 0.5)) * 0.3` (Cauchy, scale 0.3), rejection-resampled to keep only values in (−8, 8) meters.
- **Bins:** 60; x-axis label "Prediction Error (meters)"; y-axis label "Frequency".
- **Bars:** fill `rgba(231,76,60,0.35)`, stroke `#1a5276`. Standard density line + SE band.

### Visualization (canvas `canvas3b`, 400×340)

Radial safety-margin diagram: scatter of errors around a pedestrian icon with concentric range rings.

- **Title:** "Gaussian Safety Margin vs Reality (Cauchy)".
- **Center:** pedestrian emoji 🚶 (18px, `#1a5276`) at canvas center (offset +15px vertically).
- **Range rings:** dashed gray `#bbb` circles (dash 3/3) at 1m, 2m, 3m, 4m radii (scale = maxR/4.5), each labeled "1m"…"4m" in `#999` 9px.
- **Gaussian ring:** solid green `rgba(39,174,96,0.7)` circle at 2×0.3m (2-sigma of the Cauchy scale), width 3, interior tinted `rgba(39,174,96,0.08)`; two-line bold 10px green label "Gaussian 95% / safety margin".
- **Scatter:** up to 800 error samples placed at random angles with radius = |error|×scale — dots 1.8px; inside the Gaussian ring blue `rgba(41,128,185,0.35)`, outside red `rgba(231,76,60,0.8)`.
- **Annotation:** red arrow to an outlier region (upper right) with two-line bold 9px label "Cauchy outliers / OUTSIDE margin".
- **Bottom annotation box:** fill `rgba(231,76,60,0.1)`, border `#e74c3c`, bold 11px `#c0392b` centered text: "<pct>% of errors exceed Gaussian safety margin — no fixed bound works" (percentage computed from the data).

## Sensor Fusion Latency (Agreement vs Conflict)

**Pitfall label (uppercase, `#e74c3c`):** BIMODAL (FAST VS ARBITRATION)

Spike at 10–15ms (sensors agree, direct pipeline) and second mode centered ~65ms (sensors disagree, arbitration). The gap between modes = cost of conflict resolution. At 60mph, the 65ms arbitration mode ≈ 5.7 feet of travel blind. The shape quantifies when the car is flying blind.

- Spike at 10–15ms = sensors agree (fast path)
- Second mode ~45–85ms = sensors disagree (arbitration)
- Gap between modes = conflict resolution cost
- 65ms at 60mph ≈ 5.7 feet of blind travel

### Visualization (canvas `canvas4`, 420×340)

Histogram: bimodal fusion latency.

- **Title:** "Sensor Fusion Latency — Bimodal".
- **Data:** 2500 samples — 65% fast path `12 + N(0,1)*2` (kept if >3ms); 35% arbitration path `65 + N(0,1)*10` (kept if >30ms).
- **Bins:** 45; x-axis label "Latency (ms)"; y-axis label "Frequency".
- **Bars:** fill `rgba(41,128,185,0.35)`, stroke `#1a5276`. Standard density line + SE band.

### Visualization (canvas `canvas4b`, 400×340)

Grouped bar chart: blind travel distance by speed for both latency modes.

- **Title:** "Blind Travel Distance at Speed".
- **Data:** speeds 30, 45, 60, 75 mph; distance(feet) = speed × (5280/3600) × latency(ms)/1000 for fast path 12ms and arbitration 65ms. Max scale = 75mph at 65ms (~7.15 ft).
- **Bars per speed:** left blue `rgba(41,128,185,0.75)` (agreement) and right red `rgba(231,76,60,0.7)` (arbitration); white 9px distance labels (e.g. "5.7ft") inside tall-enough bars; bold 11px speed labels "30 mph"… below.
- **Gridlines:** dashed `#ddd` horizontal lines at quarter heights with `#999` 10px "ft" labels.
- **Legend (top left):** swatch "Agreement (12ms)" blue, "Arbitration (65ms)" red.
- **Annotation:** dark-red `#c0392b` leader line from the 75mph arbitration bar with bold 9px label "7+ feet BLIND".
- **Axis labels:** x "Vehicle Speed"; rotated y "Blind Distance (feet)"; padding top 45 / right 25 / bottom 55 / left 60.
- **Bottom insight strip:** fill `rgba(231,76,60,0.1)`, bold 10px `#c0392b` centered: "Arbitration = 5.4x the blind distance of agreement".

## Miles Between Scenarios (Most Miles Teach Nothing)

**Pitfall label (uppercase, `#8e44ad`):** RIGHT-SKEWED (LOG-NORMAL)

Extremely right-skewed — most miles are boring (straight highway, clear day) while interesting scenarios cluster together (construction zones, school zones). In this simulation, 80% of the scenarios sit in ~22% of the miles. One reading: random test driving is an inefficient scenario collector, which is why simulation is used to amplify the rare tail.

- Most miles = little learning value (boring, repetitive)
- Interesting scenarios cluster (not uniformly distributed)
- Random driving = inefficient scenario collection
- Simulation is one way to amplify the rare-scenario tail

### Visualization (canvas `canvas5`, 420×340)

Histogram: log-normal miles between edge cases.

- **Title:** "Miles Between Edge Cases — Log-Normal".
- **Data:** 2500 samples of `exp(6.5 + 1.8*N(0,1))` (median ~665 miles, very right-skewed), rejection-resampled to (0, 50000).
- **Bins:** 50; x-axis label "Miles Between Scenarios"; y-axis label "Frequency".
- **Bars:** fill `rgba(39,174,96,0.35)`, stroke `#1a5276`. Standard density line + SE band.

### Visualization (canvas `canvas5b`, 400×340)

Lorenz-style cumulative learning curve: % of miles driven vs % of scenarios found.

- **Title:** "Cumulative Learning: % Miles vs % Scenarios".
- **Curve:** data sorted ascending; x = cumulative miles fraction, y = cumulative scenario fraction (sampled every 10th point); stroke `rgba(39,174,96,0.9)` width 3; area under the curve up to the top-right corner filled `rgba(39,174,96,0.15)`.
- **Equality line:** dashed `#ccc` diagonal (dash 4/4) with rotated 9px `#aaa` label "uniform (ideal)".
- **Reference marker:** dashed orange `rgba(230,126,34,0.8)` L-shaped lines (dash 4/3) at the 80%-of-scenarios point, 5px orange `#e67e22` dot at the intersection, two-line bold 10px orange annotation: "80% of scenarios" / "in only <pct>% of miles" (~22%).
- **Region label:** lower right, "WASTED / MILES" in bold 11px `rgba(39,174,96,0.3)` with 9px `#666` sub-line "(zero learning)".
- **Axes:** both x and y tick labels 0% / 25% / 50% / 75% / 100%; x label "% of Total Miles Driven", rotated y label "% Scenarios Found"; padding top 42 / right 25 / bottom 50 / left 55.

## Regeneration instructions

- **Layout:** one `<table class="obj-table">` per section, each with a single `<tr>` of three `<td>`s — left (38%) holds `.pitfall-label` span + `<h3>` + paragraph + `<ul>`; middle (31%, centered) holds the primary 420×340 canvas; right (31%, centered) holds the insight 400×340 canvas. Head includes a responsive viewport meta tag.
- **Page style:** body system sans-serif, margin 20px, background `#f9f9f9`, text `#333`; h1 centered `#1a5276`; `.obj-table` full width, collapsed borders, cells `1px solid #2980b9` with 12px padding; h3 `#1a5276` 1.0em weight 700; paragraphs/bullets 14px, line-height 1.5-1.6; `.pitfall-label` inline-block bold 0.72em uppercase with 0.5px letter-spacing; `canvas { width: 100%; height: auto; }`.
- **Pitfall label colors:** assigned by a small script cycling `["#795548","#2980b9","#27ae60","#e74c3c","#8e44ad","#e67e22","#16a085","#d35400","#c0392b","#1abc9c"]` over all `.pitfall-label` elements in document order.
- **Data generation:** seeded RNG `mulberry32(42)` shared across all charts in document order; Box-Muller for normals; `randExp(lambda)` helper defined.
- **Shared histogram utility:** `drawHistogram(canvasId, data, bins, xlabel, title, color)` — white background, bold 13px `#1a5276` title, gray `#999` L-axes, bars normalized to max count, data-driven min/max range, x ticks 2 decimals at 6 positions, rotated "Frequency" y-label; every histogram also gets a Gaussian-smoothed density line `#2c3e50` width 2 with a 95% SE band `rgba(52,73,94,0.2)`.
- **Canvas scaling:** all canvases declare intrinsic width/height attributes and set `max-width` to the intrinsic width, size the backing store to the displayed width (`getBoundingClientRect().width`, falling back to the intrinsic width) × `window.devicePixelRatio`, and `ctx.scale` by that combined factor.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; slate `#2c3e50`/`rgba(52,73,94,…)`, secondary blues `#2980b9`/`rgba(41,128,185,…)`.
- No nav bar, no back/home links. In regenerated HTML any card links would use `.html` extensions (this page has none).
