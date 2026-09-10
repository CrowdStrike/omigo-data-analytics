# Climate & Weather — Distribution Patterns

**Page type:** detail page (three-column obj-table layout: text left 38%, main histogram canvas center 31%, insight canvas right 31%, one table per section)
**HTML title tag:** Climate & Weather — Distribution Patterns

## Daily Rainfall (Average Is Dangerously Misleading)

**Label:** ZERO-INFLATED GAMMA (color `#795548`)

60-70% of days are exactly 0mm (no rain). Among rainy days: gamma distribution (right-skewed, mode at light rain, tail to heavy events). "Average rainfall" is meaningless because it mixes dry and wet days. A city with 1000mm/year could get it in 50 storms or 200 drizzles — wildly different flood risk. Shape tells you drought-vs-deluge risk, mean tells you nothing.

- 60-70% of days = exactly 0mm (zero-inflated)
- Rainy days follow gamma (right-skewed)
- "Average daily rainfall" mixes two populations (dry + wet)
- Shape (zero ratio + tail weight) predicts flood risk better than mean

### Visualization (canvas `canvas1`, 420×340)

Zero-inflated gamma histogram.

- **Data:** seeded RNG mulberry32(42) shared across the page; 2000 draws: p=0.65 → 0 (dry day), else gamma(shape=2, scale=5) via Marsaglia-Tsang.
- **Bins/axes:** 50 bins over x 0–50; x ticks at 6 values (one decimal); y "Frequency" with 4 tick levels and `#eee` gridlines; axes `#ccc`; margins top 50 / right 30 / bottom 50 / left 60; white background.
- **Title (bold 14px, `#1a5276`):** "Daily Rainfall — Zero-Inflated Gamma". **X label:** "Rainfall (mm)".
- **Bars:** fill `rgba(26,188,156,0.35)`, stroke `#1a5276`.
- **Density overlay:** Gaussian-kernel smoothed counts (sigma 1.5 bins), teal line `#148f77` width 2 with SE band `rgba(26,188,156,0.18)` (1.96·smoothed/√effN, effN clamped 30–200).

### Visualization (canvas `canvas1b`, 400×340)

Exceedance curve.

- **Title (bold 13px, `#1a5276`):** "Exceedance Curve: P(Rainfall > x)". **Subtitle (11px `#666`):** "Reveals flood risk the histogram hides".
- **Curve:** exceedance P(X > x) from the sorted canvas1 data, red `#e74c3c` width 2.5, area filled `rgba(231,76,60,0.15)`; x 0–50mm, y 0–1.
- **Annotations:** dashed green `#27ae60` horizontal line at probability 0.35 with bold labels "65% days dry" / "(instant drop to 35%)"; dashed purple `#8e44ad` vertical line at 25mm with a purple arrowhead and bold label "Flood risk: <computed>% days > 25mm".
- **Axes:** y ticks 0.00–1.00 in quarters; x ticks every 10 labeled "0mm"…"50mm"; axis titles "Rainfall Threshold (mm)" and rotated "P(X > threshold)"; margins top 45 / right 25 / bottom 50 / left 55.

## Hurricane Peak Intensity (Fizzle or Explode)

**Label:** BIMODAL (PHASE TRANSITION) (color `#2980b9`)

Simulated lifetime-maximum intensity: cluster at Category 1-2, thin middle at Cat 3, second cluster at Cat 4-5. One reading: storms that undergo rapid intensification jump past the middle categories — they either fizzle or explode. Under that reading, warming would raise the probability of the jump rather than shifting every storm slightly upward.

- Cluster at Cat 1-2 (storms that fizzle)
- Thin middle at Cat 3 (few storms peak there)
- Cluster at Cat 4-5 (rapid intensifiers)
- If the phase-transition reading holds, warming shifts jump probability, not the whole curve

### Visualization (canvas `canvas2`, 420×340)

Bimodal histogram of hurricane category.

- **Data:** 1500 draws: p=0.55 → 1.2 + |randn()|·0.5 (Cat 1-2 cluster); else p=0.15 → 2.8 + randn()·0.3 (sparse Cat 3 transition); else 4.2 + randn()·0.5 (Cat 4-5 cluster).
- **Bins/axes:** 40 bins over x 0.5–5.5. **Title:** "Hurricane Peak Intensity — Bimodal". **X label:** "Category". **Y label:** "Frequency".
- **Bars:** fill `rgba(231,76,60,0.35)`, stroke `#1a5276`; same teal density overlay as canvas1.

### Visualization (canvas `canvas2b`, 400×340)

ECDF with phase-transition zone shading.

- **Title (bold 13px, `#1a5276`):** "ECDF: The Cat-3 Gap Shows as a Plateau". **Subtitle (11px `#666`):** "Flat region = gap where storms rarely exist".
- **Zone backgrounds (x range 0.5–5.5):** Cat 1.0–2.5 `rgba(46,204,113,0.12)`; Cat 2.5–3.5 `rgba(241,196,15,0.2)`; Cat 3.5–5.5 `rgba(231,76,60,0.1)`.
- **Zone labels (bold 10px, top):** "Cat 1-2" / "(Fizzle)" in `#27ae60`; "TRANSITION" / "ZONE" in `#f39c12`; "Cat 4-5" / "(Explode)" in `#e74c3c`.
- **Curve:** ECDF of the canvas2 data in dark red `#c0392b` width 2.5.
- **Annotation:** orange `#e67e22` vertical arrow from the top labels down to the plateau near Cat 3, with bold label "Flat = few storms".
- **Axes:** y 0.00–1.00 quarters; x ticks "Cat 1"…"Cat 5"; axis titles "Hurricane Category" and rotated "Cumulative Probability".

## Temperature Anomaly (Variance Change > Mean)

**Label:** VARIANCE EXPANSION (color `#27ae60`)

Simulated anomalies: each period approximately Gaussian, with the spread widening over decades — more extremes on BOTH ends. If variance expands, record colds keep happening even as the mean rises; a cold snap ("it was freezing!") doesn't contradict a warming mean under a wider distribution. For tail risk, the shape change (spread) can matter more than the location change (mean).

- Each period: approximately Gaussian anomaly
- Simulated: variance widening over decades (more extremes both ways)
- Record cold still happens under a wider distribution — no contradiction with a rising mean
- For extremes, spread change can outweigh mean shift

### Visualization (canvas `canvas3`, 420×340)

Overlaid past-vs-present anomaly histograms.

- **Data:** 2000 draws each. Past: randn()·0.8 (narrow, centered 0). Present: 0.3 + randn()·1.4 (wider, shifted).
- **Bins/axes:** 45 bins over x −5 to 5. **Title:** "Temperature Anomaly — Variance Expansion Over Decades". **X label:** "Temperature Anomaly (°C)". **Y label:** "Frequency".
- **Series:** main (past) fill `rgba(230,126,34,0.35)` stroke `#1a5276`, legend "Past (narrow)"; overlay (present, drawn behind) fill `rgba(231,76,60,0.3)` stroke `#e74c3c`, legend "Present (wider + shifted)". Legend swatches top-right. Teal density overlay on the main data.

### Visualization (canvas `canvas3b`, 400×340)

Variance-expansion timeline with widening sigma bands.

- **Title (bold 13px, `#1a5276`):** "Variance Expansion: The Real Story". **Subtitle (11px `#666`):** "Spread matters more than mean shift".
- **Data (7 decades):** labels `['1960s','1970s','1980s','1990s','2000s','2010s','2020s']`; mean shift `[0, 0.05, 0.1, 0.2, 0.35, 0.5, 0.7]` °C; sd `[0.6, 0.7, 0.8, 0.95, 1.1, 1.25, 1.4]`.
- **Bands:** ±2σ envelope filled `rgba(231,76,60,0.15)`; ±1σ band filled `rgba(230,126,34,0.25)`; mean line red `#e74c3c` width 2.5; dashed gray `#999` zero line.
- **Extreme points:** purple `rgba(142,68,173,0.7)` dots scattered per decade for simulated values beyond ±2σ (12 candidate draws per decade).
- **Annotations:** red "+2σ" and "-2σ" labels at the right edge of the envelope; purple 10px legend "• = extreme events" bottom-left.
- **Axes:** y −4.0 to +4.0 °C labeled in quarters; x labeled by decade; axis titles "Decade" and rotated "Anomaly (°C)"; margins top 45 / right 20 / bottom 50 / left 55.

## Wind Speed (Shape Parameter = Financial Model)

**Label:** WEIBULL (k≈2) (color `#e74c3c`)

Rises from zero, peaks at moderate speed, decays; never negative. Weibull fits wind-speed data well at many sites, so wind-farm revenue models integrate the fitted density (weighted by v³) above the turbine's cut-in speed. Because power scales with v³, small errors in the shape parameter k compound into large revenue errors over a turbine's multi-decade life. The shape parameter effectively IS the financial model.

- Weibull with k≈2 (Rayleigh special case) fits many sites
- Wind farm revenue = integral above cut-in speed
- Small error in k compounds via v³ into large revenue error
- Shape parameter IS the financial projection

### Visualization (canvas `canvas4`, 420×340)

Weibull wind-speed histogram.

- **Data:** 2000 draws from Weibull(k=2, λ=7) via inverse transform (Rayleigh).
- **Bins/axes:** 40 bins over x 0–20. **Title:** "Wind Speed — Weibull (k≈2, Rayleigh)". **X label:** "Wind Speed (m/s)". **Y label:** "Frequency".
- **Bars:** fill `rgba(41,128,185,0.35)`, stroke `#1a5276`; teal density overlay.

### Visualization (canvas `canvas4b`, 400×340)

Energy-density area chart with turbine thresholds.

- **Title (bold 13px, `#1a5276`):** "Wind Energy: Where the Money Is". **Subtitle (11px `#666`):** "Power ∝ v³ — most energy from moderate winds".
- **Curve:** energy density = Weibull(k=2, λ=7) PDF × v³ evaluated at 81 points over v 0–20 m/s, normalized to its peak; line `#2980b9` width 2.5 with area filled `rgba(41,128,185,0.3)`.
- **Capturable region:** area between cut-in 3.5 m/s and cut-out 25 m/s filled `rgba(39,174,96,0.25)`.
- **Threshold lines:** dashed red `#e74c3c` vertical at cut-in 3.5 m/s labeled "Cut-in" / "3.5 m/s"; dashed green `#27ae60` vertical at rated 12 m/s labeled "Rated" / "12 m/s".
- **Annotations:** purple `#8e44ad` dot and arrow at the energy peak with bold label "Peak energy @ <computed> m/s"; bold green centered label "REVENUE ZONE" between cut-in and rated near the baseline.
- **Axes:** y 0–100% (% of peak) at 25% steps; x ticks "0 m/s"…"20 m/s"; axis titles "Wind Speed (m/s)" and rotated "Energy Density (% of peak)".

## Earthquake Magnitude (No Maximum, Scale Invariant)

**Label:** POWER LAW (SCALE-INVARIANT) (color `#8e44ad`)

Near-linear relationship in log(frequency) vs magnitude: every +1 magnitude ≈ 10× fewer events, remarkably consistent across regions. Scale-invariant: the same physics produces M2 and M9, just at different rates. The fitted power law has no built-in maximum. And it says nothing about timing — the size distribution alone gives no support to "overdue for the big one" reasoning.

- Log-linear: each +1 magnitude ≈ 10× fewer events
- Scale-invariant (same physics at all scales)
- No built-in maximum in the fitted power law
- Size distribution says nothing about timing — "overdue" doesn't follow from it

### Visualization (canvas `canvas5`, 420×340)

Gutenberg-Richter magnitude histogram.

- **Data:** 3000 draws: m = 2 + exponential(λ=ln 10) (log-linear decay, b=1), kept if ≤ 9.5.
- **Bins/axes:** 35 bins over x 2–9.5. **Title:** "Earthquake Magnitude — Power Law (Gutenberg-Richter)". **X label:** "Magnitude". **Y label:** "Frequency (log-linear decay)".
- **Bars:** fill `rgba(52,73,94,0.4)`, stroke `#1a5276`; teal density overlay.

### Visualization (canvas `canvas5b`, 400×340)

Return-period bubble diagram on a log scale.

- **Title (bold 13px, `#1a5276`):** "Return Period: How Often Each Magnitude". **Subtitle (11px `#666`):** "Log scale reveals perfect linearity (scale invariance)".
- **Data:** magnitudes M2–M9; base rate 15,000 events/year ≥ M2 (illustrative), rate = 15000·10^−(M−2), return period T = 1/rate. Human-readable labels beside bubbles: "35 min", "5.8 hrs", "2.4 days", "24 days", "243 days", "6.7 yrs", "67 yrs", "667 yrs".
- **Bubbles:** radius 6 + M·2.5, alpha 0.7, colors by severity `['#27ae60','#2ecc71','#f1c40f','#e67e22','#e74c3c','#c0392b','#8e44ad','#6c3483']`, white bold "M2"…"M9" labels inside; dashed `#34495e` line through the points.
- **Axes:** y log10 return period from 10^−5 to 10^3 years, labels `['0.00001y','0.0001y','0.001y','0.01y','0.1y','1 yr','10 yr','100 yr','1000 yr']` with dashed `#eee` gridlines per decade; x ticks "M2"…"M9"; axis titles "Earthquake Magnitude" and rotated "Return Period (log scale)"; margins top 45 / right 25 / bottom 55 / left 60.
- **Annotation (bold 10px `#6c3483`, top-right):** "Fitted line has no" / "built-in maximum".

## Regeneration instructions

- **Layout:** one `.obj-table` (full-width, `border-collapse: collapse`) per pitfall, single `<tr>` with three `<td>`s: text 38%, main canvas 31% centered, insight canvas 31% centered. Cell borders `1px solid #2980b9`, padding 12px. Each text cell: `.pitfall-label` span, `<h3>`, one `<p>`, one 4-item `<ul>`.
- **Page style:** body system sans-serif, margin 20px, background `#f9f9f9`, text `#333`; h1 centered `#1a5276`; `.obj-table h3` 1.0em weight 700 `#1a5276`; p/li 14px, line-height 1.5–1.6; `.pitfall-label` inline-block bold 0.72em uppercase, letter-spacing 0.5px. `canvas { width: 100%; height: auto; }`. No nav bar, no back/home links.
- **Label colors:** assigned by section index from the palette `["#795548","#2980b9","#27ae60","#e74c3c","#8e44ad","#e67e22","#16a085","#d35400","#c0392b","#1abc9c"]` via a small script.
- **Canvases:** intrinsic sizes 420×340 (main) and 400×340 (insight); set `max-width` to the intrinsic width, size the backing store to the displayed width (`getBoundingClientRect().width`, falling back to the intrinsic width) × `window.devicePixelRatio`, and `ctx.scale` by that combined factor. Data generated with seeded mulberry32(42) RNG shared sequentially across all charts, plus Box-Muller `randn()`, `randExp(lambda)`, Marsaglia-Tsang `randGamma(shape, scale)`, and inverse-transform `randWeibull(k, lambda)` helpers.
- **Shared histogram helper:** margins {top 50, right 30, bottom 50, left 60}, white background, bold 14px `#1a5276` title, `#ccc` axes, `#666` tick text, `#eee` gridlines; optional overlay dataset drawn behind the main; smoothed density line `#148f77` with SE band `rgba(26,188,156,0.18)`; optional legend swatches top-right.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, secondary blue `#2980b9`, purple `#8e44ad`, teal accent `#148f77`/`rgba(26,188,156,0.35)`.
