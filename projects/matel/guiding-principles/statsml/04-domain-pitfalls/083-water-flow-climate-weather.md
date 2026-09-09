# Water/Climate - Domain-Specific Pitfalls

**Page type:** detail page (one h2 per pitfall, each followed by a two-column obj-table row: text left ~40%, canvas right ~60%)
**HTML title tag:** Water/Climate - Domain-Specific Pitfalls

**Subtitle:** Weather and climate data mix chaotic dynamics, sparse sensors, and shifting distributions — the statistical traps here break both forecasts and engineering standards.

## Chaotic Systems — The Butterfly Effect

**Small Initial Error → Completely Different Forecast After 10 Days**

- **Sensitivity:** A tiny error in the initial conditions grows exponentially, not linearly, with lead time.
- **Rapid divergence:** Two nearly identical starting states give completely different forecasts within days.
- **Hard limit:** Deterministic point prediction is physically impossible beyond roughly 2 weeks.
- **Not a compute problem:** No amount of model quality or compute pushes that horizon further out.
- **The fix:** Run probabilistic ensembles — many perturbed initial states — and report the spread.
- **False precision:** A single-path 10-day forecast is not a prediction, only one member of a wide fan.

### Visualization (canvas `c1`, 720×300)

Ensemble forecast fan: 12 member paths diverging from one starting point, with envelope and mean.

- **Title (bold 17px `#1a5276`, centered):** "Ensemble Forecast: One Start, Many Futures".
- **Axes:** L-shaped `#333` axes (width 1); margins left 60, right 30, top 35, bottom 45; x-axis label "Days from Now" with ticks 0–14 every 2 days; rotated y-axis label "Temp (°C)" (17px).
- **Data (deterministic pseudo-random via `seededRandom(seed) = frac(sin(seed)·10000)`):** 12 ensemble paths, 70 steps, starting at 15; each step adds drift 0.05·sin(step·0.1 + e) plus noise (seededRandom(e·1000 + step·7) − 0.5)·spread where spread grows linearly to 1.8 at the final step — a fan widening with lead time.
- **Envelope:** min/max band across members filled `rgba(41,128,185,0.3)`.
- **Members:** thin `#2980b9` lines (width 0.8, alpha 0.6).
- **Mean:** ensemble-mean line in `#1a5276`, width 2.5.
- **Annotation (bold 17px red `#e74c3c`, right-aligned near top):** "Day 14: spread swamps the mean".

## Sparse Station Networks

**One Station Every 50-500km — Interpolation Invents Smoothness**

- **Coverage gap:** Developed countries have a weather station every 50-100km, developing regions every 500km.
- **Ocean blind spot:** The open ocean has almost no fixed stations at all, over most of the planet's surface.
- **Smooth lies:** Interpolating between stations produces a smooth field that misses local topography.
- **Terrain detail lost:** A mountain and a valley 10km apart have completely different weather.
- **Invisible error:** The map looks plausible everywhere because it can only show what the stations saw.
- **Downstream risk:** Models trained on gridded "observations" inherit the artifacts as ground truth.

### Visualization (canvas `c2`, 720×300)

Jagged true field vs smooth interpolation through 5 sparse stations.

- **Title (bold 17px `#1a5276`, centered):** "Stations Every 100km: Interpolation Misses the Terrain".
- **True field:** red `#e74c3c` line (width 2) over x = 0–620 (left margin 50, baseline y=170): value = 40 + 18·sin(0.012x) + 22·sin(0.045x + 1.5) + 12·sin(0.09x + 0.7) — jagged topography-driven curve.
- **Stations:** blue `#2980b9` dots (radius 6) at x = 0, 155, 310, 465, 620 on the true field, labeled "S1"–"S5" (17px `#333`) below the baseline.
- **Interpolation:** dashed blue `#2980b9` straight segments (dash 6/4, width 2.5) connecting the 5 station points.
- **Legend (17px):** red "— True field (mountains, valleys)"; blue "- - Interpolated from 5 stations".
- **Captions (centered):** bold 17px red "Everything between stations is guessed. Peaks and valleys 10km apart: invisible."; 17px `#555` 'Ocean and developing regions: even sparser. Gridded "observations" inherit the guess.'

## Climate ≠ Weather Confusion

**Climate Is 30-Year Statistics; Weather Is Tomorrow**

- **Different objects:** Climate is the 30-year distribution of weather; weather is one draw from it.
- **Weak inference:** A claim about the distribution says little about any single draw, or the reverse.
- **Both directions wrong:** A climate projection says nothing about next Tuesday, nor Tuesday about the trend.
- **The classic error:** "It's cold today" does not disprove warming, it is one draw from a shifted distribution.
- **Fully consistent:** A single cold day sits comfortably inside a distribution with a rising mean.
- **Signal vs noise:** Daily variance far exceeds the trend slope, so the trend needs decades of averaging.

### Visualization (canvas `c3`, 720×300)

Noisy daily weather series around a slowly rising climate trend line, with a "cold today!" outlier circled.

- **Title (bold 17px `#1a5276`, centered):** "Weather = Noisy Draws. Climate = the Slow-Moving Mean."
- **Daily weather:** thin `rgba(41,128,185,0.6)` line (width 1) over x = 0–620 (left margin 50, baseline y=160): value = 50 + 0.045x + 0.6·(28·sin(0.35x + seededRandom(x)·2) + (seededRandom(13x) − 0.5)·30) — large high-frequency noise on a gentle upward trend. Labeled "daily weather" (17px blue) at lower left.
- **Trend line:** solid red `#e74c3c` (width 3) from (50, baseY−50) rising with slope 0.045 per px, labeled bold 17px red "30-year trend (climate)" at the right end.
- **Cold-day annotation:** orange `#e67e22` circle (radius 9, width 2) around the lowest weather point with x > 480, labeled bold 17px orange '"cold today!"' below it.
- **Captions (centered):** bold 17px red "One cold draw is fully consistent with a rising mean."; 17px `#555` "Daily variance >> trend slope — the trend only shows up after decades of averaging."

## Extreme Events From Limited History

**Estimating the 100-Year Flood From 50-70 Years of Records**

- **The definition:** A "100-year flood" means 1% annual probability of being equalled or exceeded.
- **Record too short:** Most gauges hold only 50-70 years of records, shorter than the return period itself.
- **Massive uncertainty:** A tail quantile from ~50 points has confidence intervals wide enough to be off 2×.
- **Cruel asymmetry:** The rarest events are the most consequential AND the least estimable statistically.
- **No data where needed:** Exactly where you need precision, you have almost no observations to fit on.
- **Hidden extrapolation:** The estimate depends heavily on which extreme-value distribution you assume.
- **Undecidable choice:** The 50 points of record cannot tell you which of those tail distributions is right.

### Visualization (canvas `c4`, 720×300)

Record-length comparison bars plus a point estimate with a very wide confidence interval.

- **Title (bold 17px `#1a5276`, centered):** "100-Year Flood, Estimated From 50 Years of Records".
- **Comparison bars (scale 3px/year, 18px tall, starting x=230):** "Records available:" — blue `#2980b9` bar 150px labeled "50 yrs"; "Return period target:" — red `#e74c3c` bar 300px labeled "100 yrs" (labels 17px `#333`).
- **Estimate axis:** horizontal `#333` line at y=170 from x=60 to x=660, labeled below in 17px `#555` "estimated 100-yr flood level (m³/s)".
- **CI band:** `rgba(41,128,185,0.3)` rectangle from x=160 to x=560 above the axis, with a `#2980b9` error-bar line (width 2) and end caps at both ends; a `#1a5276` point-estimate dot (radius 6) at x=310, labeled bold 17px "point estimate" above; 17px blue text "95% CI: off by up to 2×" above the right cap.
- **Captions (centered):** bold 17px red "Rarest events: most consequential AND least estimable."; 17px `#555` "The answer also changes with the assumed tail distribution — and 50 points can't pick one."

## Downscaling Artifacts

**100km Global Model → 1km Local Prediction Invents Detail**

- **Resolution gap:** Global climate models run on ~100km grids, but planning decisions need 1km predictions.
- **Detail from nowhere:** The missing fine structure has to come from somewhere, and it is not observations.
- **Method, not reality:** Statistical downscaling fills the gap from the method's assumptions, not the physics.
- **Bias correction backfires:** Adjusting output to match local observations can produce impossible scenarios.
- **Physically broken:** Those artifacts include negative rainfall and a broken surface energy balance.
- **False confidence:** The 1km map looks authoritative because its fine texture is fabricated deterministically.

### Visualization (canvas `c5`, 720×300)

Coarse 3×3 grid transformed by an arrow into a fabricated fine 12×12 grid.

- **Title (bold 17px `#1a5276`, centered):** 'Downscaling: 100km Grid → 1km "Detail" From Nowhere'.
- **Coarse grid (left, origin (70,45), 42px cells, white 1px cell borders):** 3×3 blue cells with alpha values `[[0.3,0.5,0.4],[0.5,0.7,0.6],[0.4,0.6,0.5]]` as `rgba(41,128,185,alpha)`; caption 17px `#333` "Global model: 100km cells".
- **Arrow:** orange `#e67e22` horizontal arrow (width 3, filled head) from x=250 to x=330 at mid-grid height, labeled two lines 17px "statistical" / "downscaling" above.
- **Fine grid (right, origin (360,45), 10.5px cells, orange 1px outer border):** 12×12 orange cells `rgba(230,126,34,alpha)` where alpha = parent coarse value + 0.35·sin(1.3c + 0.9r) + (seededRandom(37r + 11c) − 0.5)·0.3, clamped to [0.05, 0.95] — invented fine texture; caption 17px `#333` '"1km prediction"'.
- **Callout (bold 17px red, left-aligned at x=520, four lines):** "The fine texture is" / "fabricated by the" / "method — it was" / "never in the model."
- **Captions (centered):** bold 17px red "Bias correction on top can produce physically impossible scenarios."; 17px `#555` "The map looks authoritative because the invented detail is deterministic."

## Non-Stationarity From Climate Change

**The Past Is No Longer a Guide — Return Periods Drift**

- **Broken standards:** Engineering standards are set from the "historical 100-year flood level" on record.
- **Moving target:** Climate change shifts the underlying distribution those historical levels came from.
- **Wrong by construction:** Every statistical return period assumes a stationary distribution to be valid.
- **Stale on release:** If the distribution is shifting, the number is wrong the day it is published.
- **Quiet escalation:** A modest shift in the mean multiplies the tail probability disproportionately.
- **Return period drift:** Yesterday's 100-year event becomes a 20-year event under that shifted mean.
- **What to do:** Fit non-stationary extreme-value models with an explicit trend, not pooled history.

### Visualization (canvas `c6`, 720×300)

Two bell curves (historical vs shifted climate) against a fixed design-standard line, with the inflated tail shaded.

- **Title (bold 17px `#1a5276`, centered):** "Shifting Distribution: Yesterday's 100-Year Flood, Today's 20-Year Flood".
- **Curves** (x = 0–620 with left offset 50, baseline y=185, height = normal density `gauss(x, mu, sigma) = exp(−0.5((x−mu)/sigma)²)/(sigma·2.507)` scaled ×28000):
  - Historical: blue `#2980b9` line (width 2.5), mean 220, sd 80, labeled 17px blue "historical climate".
  - Shifted: red `#e74c3c` line (width 2.5), mean 320, sd 90, labeled 17px red "shifted climate".
- **Design standard:** dashed orange `#e67e22` vertical line (dash 7/5, width 2.5) at x=470 (the historical 1% level), labeled two lines bold 17px orange "design standard" / "(historical 1% level)" above.
- **Shaded tail:** the shifted curve's area beyond the design line filled `rgba(231,76,60,0.25)`, annotated bold 17px red "tail: 1% → 5%" to the right.
- **Captions (centered):** bold 17px red "Every stationary return period is wrong while the distribution is moving."; 17px `#555` "Fix: fit non-stationary extreme-value models with an explicit trend, not pooled history."

## Regeneration instructions

- **Layout:** domains detail-page style — h1, `.subtitle` paragraph, then one `<h2>` per pitfall (unnumbered, `border-bottom: 2px solid #2980b9`), each followed by a single-row `.obj-table`: left `<td>` (40%) with `.obj-title` div + `<ul>` of bold-labeled one-sentence bullets, right `<td>` (60%, centered) with the canvas. Even table rows have background `#fafcfe`. No thead, no nav, no cross-page links.
- **Page CSS:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276`; `.subtitle` `#666` 1.05em; `ul` 0.9em `#333`; `strong` `#1a5276`; `.obj-table td` border `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600 `#1a5276`; `.philosophy` callout style defined (background `#f0f4f8`, left border `4px solid #2980b9`) though unused.
- **Canvas:** each declares intrinsic `width="720" height="300"`; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Shared helpers: `gauss(x, mu, sigma)` (normal density) and `seededRandom(seed) = frac(sin(seed)·10000)` for deterministic noise. Charts use font `-apple-system` at 17px (bold 17px for titles/emphasis) and white (unfilled) backgrounds. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, accent blue `#2980b9` (band fill `rgba(41,128,185,0.3)`), green `#27ae60` (unused on this page's charts), red `#e74c3c`, orange `#e67e22`, grays `#555`/`#333`.
- Card/page links in regenerated HTML use `.html` extensions.
