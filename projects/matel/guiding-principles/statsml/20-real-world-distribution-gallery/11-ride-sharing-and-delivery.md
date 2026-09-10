# Ride-sharing & Delivery — Distribution Patterns

**Page type:** detail page (three-column obj-table layout: text left 38%, histogram canvas middle 31%, insight canvas right 31%, one table per section)
**HTML title tag:** Ride-sharing & Delivery — Distribution Patterns

## Surge Multiplier — A Spike at 1.0× and Discrete Price Steps

**Pitfall label:** SPIKE AT 1x, THEN STEP-LADDER BUMPS (color `#795548`)

Most rides have no surge — 60% of this simulated sample sits in a spike at 1.0×. When surge kicks in, it lands on distinct steps at 1.25×, 1.5×, and 2.0×, plus a thin tail above. A stepped shape like this looks like preset price tiers rather than a continuously clearing market price.

- Massive spike at 1.0× — most of the time, no surge at all
- Discrete bumps at 1.25×, 1.5×, 2.0× — with near-empty gaps between them
- A continuous supply-demand price would fill those gaps; the steps are consistent with a tiered pricing policy
- One interpretation: each tier is a tested acceptance point — plausible, but the histogram alone can't confirm it

### Visualization (canvas `canvas1`, 420×340)

Histogram of surge multipliers: spike at 1.0× with discrete steps.

- **Data:** page-level seeded RNG mulberry32(42). 3000 samples at `1.0 + 0.02·N(0,1)` (60%), 600 at `1.25 + 0.03·N(0,1)`, 400 at `1.5 + 0.03·N(0,1)`, 250 at `2.0 + 0.04·N(0,1)`, and 750 exponential tail `2.0 + Exp(λ=1.5)`.
- **Chart:** 60 bins over x range 0.8–5.0 (1 decimal). Bars filled `rgba(44,62,80,0.35)` stroked `#1a5276` (0.5px). Density overlay disabled (`density: false`) — a smoothed line would blur the discrete surge-tier step-ladder the chart exists to show. White background, `#333` axes with `#eee` horizontal gridlines at quarter y ticks; margins top 35 / right 20 / bottom 45 / left 55; rotated y-axis label.
- **Title (bold 13px `#1a5276`):** "Surge Pricing Multiplier"
- **X-axis label:** "Surge Multiplier (×)"; **Y-axis label:** "Count".

### Visualization (canvas `canvas1b`, 400×340)

Waterfall bar chart of rider acceptance rate by surge tier.

- **Title:** "Rider Acceptance Rate by Surge Tier"; subtitle (10px `#666`): "Illustrative waterfall: demand drop-off at each price step".
- **Data (fixed):** tiers "1.0×", "1.25×", "1.5×", "2.0×", "2.5×", "3.0×+" with acceptance 95%, 82%, 64%, 41%, 22%, 9%.
- **Bar colors (left to right):** `rgba(39,174,96,0.75)`, `rgba(39,174,96,0.65)`, `rgba(230,126,34,0.7)`, `rgba(231,76,60,0.7)`, `rgba(231,76,60,0.8)`, `rgba(192,57,43,0.85)`; white bold 13px percentage inside each bar top; between consecutive bars a red down arrow with bold 10px `-{drop}%` label (-13%, -18%, -23%, -19%, -13%).
- **Annotation (italic 10px `#1a5276`, lower right):** "Biggest drop: 1.5× → 2.0×".
- **Axes:** rotated y-axis label "Acceptance %"; tier labels below bars.

## ETA Error — Almost Never Early, Often Late (Right-Skewed)

**Pitfall label:** LOPSIDED — LATE MUCH MORE THAN EARLY (color `#2980b9`)

The error distribution is lopsided: a peak near zero, only ~5% early, and a long late tail — in this simulation the median arrival is ~2 minutes late and 1 in 5 is 5+ minutes late. One explanation is deliberately optimistic ETAs (shorter promises win more orders). But right-skew is also what delays look like physically: traffic can add 20 minutes, it can rarely subtract them.

- Peak at 0 (on time) — but only ~5% of simulated rides arrive early
- Heavy late tail — median ~2 min late, and a fifth of rides run 5-15 minutes behind
- Symmetric error is the wrong baseline here: lateness is unbounded, earliness isn't
- Deliberate over-promising would produce this same shape — the histogram alone can't separate bias from natural skew

### Visualization (canvas `canvas2`, 420×340)

Histogram of ETA error in minutes late, right-skewed.

- **Data:** 5000 samples of `Exp(λ=0.3)` minutes late; with 5% probability replaced by a small early arrival `-|N(0,1)|`.
- **Chart:** 55 bins over x range -5 to 30 (0 decimals). Bars `rgba(231,76,60,0.35)` stroked `#1a5276`. Smoothed density line `#1a252f` + SE band `rgba(44,62,80,0.18)` (shared helper).
- **Title:** "ETA Error (Estimated vs Actual)"
- **X-axis label:** "ETA Error (minutes late)"; **Y-axis label:** "Count".

### Visualization (canvas `canvas2b`, 400×340)

ECDF of lateness with percentile markers.

- **Title:** "ECDF: What % of rides arrive by minute X?"; subtitle: "Empirical Cumulative Distribution Function".
- **Curve:** ECDF over x range -5 to 25 min, stroke `#e74c3c` width 2.5, area under curve filled `rgba(231,76,60,0.2)`.
- **Percentile markers** at 50% (`#27ae60`), 75% (`#e67e22`), 90% (`#8e44ad`): dashed elbow guides (dash 4/3) from the y-axis to the curve and down to the x-axis, 4px dot, bold 10px label "{P}% at {value} min".
- **Axes:** x ticks -5 to 25 in steps of 5, title "Minutes Late"; y 0%–100% by 25%; rotated y-axis label "Cumulative %".

## Driver Idle Time — Two Humps, Almost Nothing Between

**Pitfall label:** TWO HUMPS WITH A GAP (color `#27ae60`)

In this simulated data, drivers either get their next ride within about a minute (40% — matched while still finishing the previous trip) or wait in a broad hill around 10-20 minutes. The 1-5 minute range holds under half a percent of gaps. A hole like that suggests two distinct dispatch paths rather than one queue with a smooth wait distribution.

- Spike near 0 = pre-matched while still finishing the last ride (~40% of gaps here)
- Dead zone at 1-5 minutes — only ~0.4% of simulated gaps land there
- Broad hill at 10-20 min = waiting in the general queue
- Two separate experiences: instant dispatch vs. a long wait — the middle is nearly empty

### Visualization (canvas `canvas3`, 420×340)

Bimodal histogram of driver idle time.

- **Data:** 2000 samples of `|0.3·N(0,1)|` minutes (pre-matched spike near 0) plus 3000 samples of `15 + 4·N(0,1)` minutes (queue hill), keeping only values > 3 for the second group.
- **Chart:** 60 bins over x range 0–30 (0 decimals). Bars `rgba(41,128,185,0.35)` stroked `#1a5276`. Smoothed density line `#1a252f` + SE band `rgba(44,62,80,0.18)` (shared helper).
- **Title:** "Driver Idle Time Between Rides"
- **X-axis label:** "Idle Time (minutes)"; **Y-axis label:** "Count".

### Visualization (canvas `canvas3b`, 400×340)

Annotated state diagram of the two dispatch paths (no data plot).

- **Title:** "Dispatch Algorithm: Two Paths"; subtitle: "One explanation for the bimodal idle time".
- **Decision node:** blue diamond (`rgba(41,128,185,0.8)` filled, `#1a5276` 2px stroke) at top center with white bold 10px text "DISPATCH" / "DECISION".
- **Left branch:** green arrow (`#27ae60`, 2.5px) to a rounded green box (`rgba(39,174,96,0.7)`) reading in white: bold "PRE-MATCHED", "0-30 sec idle", "~40% of rides".
- **Right branch:** red arrow (`#e74c3c`, 2.5px) to a rounded red box (`rgba(231,76,60,0.65)`) reading in white: bold "GENERAL QUEUE", "10-20 min idle", "~60% of rides".
- **Dead zone callout:** dashed orange box (`#e67e22`, dash 4/3, fill `rgba(230,126,34,0.15)`) below the branches: bold 11px "DEAD ZONE: 1-5 min" and 10px "Nearly empty in the simulated data".
- **Bottom note (italic 10px `#1a5276`, centered):** "Binary dispatch would produce bimodal idle time (hypothesis)".

## Trip Distance — Two Hills Suggest Two Different Products

**Pitfall label:** TWO HILLS — TWO DIFFERENT PRODUCTS (color `#e74c3c`)

A tall cluster at 1-3 miles, a dip around 5-6 miles, then a second, lower and broader bump spanning roughly 7-12 miles. One reading: short hops (errands, bar hops) and planned longer trips (airports, cross-town commutes) are different products with different customers. That's an interpretation — what the histogram itself shows is a mixture of two populations with a valley between them.

- First hill at 1-3 miles — the bulk of simulated trips
- Second, flatter hill around 7-12 miles — one reading: planned trips where riders weigh alternatives
- Valley near 5-6 miles — a distance few simulated trips occupy
- A bimodal mixture like this makes the "average trip distance" describe almost nobody

### Visualization (canvas `canvas4`, 420×340)

Bimodal histogram of trip distances (two log-normals).

- **Data:** 3000 short trips `exp(0.7 + 0.5·N(0,1))` miles (mode ~2) plus 2000 long trips `exp(2.3 + 0.4·N(0,1))` miles (mode ~10).
- **Chart:** 60 bins over x range 0–30 (0 decimals). Bars `rgba(39,174,96,0.35)` stroked `#1a5276`. Smoothed density line `#1a252f` + SE band `rgba(44,62,80,0.18)` (shared helper).
- **Title:** "Trip Distance Distribution"
- **X-axis label:** "Trip Distance (miles)"; **Y-axis label:** "Count".

### Visualization (canvas `canvas4b`, 400×340)

Bubble scatter of the two trip clusters vs price sensitivity.

- **Title:** "Two Products: Distance vs Price Sensitivity"; subtitle: "Illustrative — bubble size = trip volume; Y = price sensitivity".
- **Bubbles:** 40 short-trip points (x = 1–4.5 mi, y = 15–40 sensitivity, radius 3–7) in `rgba(39,174,96,0.7)` stroked `#1e8449`; 30 long-trip points (x = 7–17 mi, y = 55–90, radius 3–8) in `rgba(41,128,185,0.7)` stroked `#1a5276`. Axis ranges x 0–20 mi, y 0–100.
- **Cluster halos:** dashed ellipses (dash 5/3) around each cluster — green `rgba(39,174,96,0.4)` stroke with `rgba(39,174,96,0.08)` fill, blue `rgba(41,128,185,0.4)` with `rgba(41,128,185,0.08)`.
- **Cluster labels (top):** bold 11px `#1e8449` "SHORT TRIPS" with 9px "Impulse / Convenience"; bold 11px `#1a5276` "LONG TRIPS" with 9px "Planned / Airport".
- **Valley band:** vertical band from 4.5 to 6.5 mi tinted `rgba(230,126,34,0.12)` with dashed orange edges, labeled in `#e67e22`: bold 9px "VALLEY" and 8px "\"Should I" / "drive?\"".
- **Axes:** x ticks "0 mi"–"20 mi" in 5-mile steps, title "Trip Distance"; y annotated "Low" (bottom) and "High" (top) with rotated title "Price Sensitivity"; light `#eee` horizontal gridlines.

## Tip Amount — Spikes Exactly Where the Buttons Are

**Pitfall label:** SPIKES WHERE THE BUTTONS ARE (color `#8e44ad`)

About a third of simulated rides leave no tip. The rest cluster sharply at ~$3, $4, and $5 — exactly 15%, 20%, and 25% of the simulated $20 fare — with almost nothing in between. A comb-shaped distribution like this points at the preset buttons, not at riders doing arithmetic.

- ~33% at $0 = no tip at all
- Sharp spikes at 15%, 20%, 25% of the fare — the three preset buttons
- Almost nothing between spikes — few type a custom amount (~5% here)
- Consistent with default effects: move the presets and you'd expect the spikes to move with them

### Visualization (canvas `canvas5`, 420×340)

Comb-shaped histogram of tip amounts (average fare $20).

- **Data:** 1500 samples at `|0.1·N(0,1)|` (no tip, ~30%); 1200 at `$3 + 0.15·N(0,1)` (15% of $20); 1000 at `$4 + 0.15·N(0,1)` (20%); 600 at `$5 + 0.15·N(0,1)` (25%); 200 uniform custom amounts in $0–8.
- **Chart:** 60 bins over x range -0.5 to 8 (1 decimal). Bars `rgba(230,126,34,0.35)` stroked `#1a5276`. Density overlay disabled (`density: false`) — smoothing would fill in the empty gaps between the $0/$1/$2/$5 preset-tip spikes.
- **Title:** "Tip Amount Distribution"
- **X-axis label:** "Tip Amount ($)"; **Y-axis label:** "Count".

### Visualization (canvas `canvas5b`, 400×340)

Illustration of a phone tip screen with connected horizontal share bars.

- **Title:** "UI Controls Behavior: Button Taps vs Custom"; subtitle: "Preset buttons shape the tip distribution (illustrative)".
- **Phone mockup (left):** rounded rectangle outline (`#333`, 2px, `#f8f8f8` fill, ~130px wide) headed bold 9px "Add a tip?", containing five rounded buttons with white bold 11px labels:
  - "$0" — `rgba(149,165,166,0.7)`
  - "15%" — `rgba(230,126,34,0.75)`
  - "20%" — `rgba(230,126,34,0.85)`
  - "25%" — `rgba(211,84,0,0.8)`
  - "Custom" — `rgba(127,140,141,0.5)`
- **Share bars (right):** horizontal bars connected to each button by dashed `#bbb` lines, lengths proportional to 33%, 27%, 22%, 13%, 5% (scale max 35%), in matching colors, each with bold 11px `#333` percentage label at bar end.
- **Annotation (bottom, with upward red arrow):** bold 11px `#e74c3c` "95% tap a preset button" and 10px `#555` "Only 5% type a custom amount".

## Regeneration instructions

- **Layout:** one `.obj-table` (full-width, border-collapse) per section, single `<tr>` with three `<td>`: left 38% text (`.pitfall-label` span, `h3`, paragraph, `ul`), middle 31% centered canvas (420×340), right 31% centered insight canvas (400×340). Cell borders `1px solid #2980b9`, padding 12px.
- **Page style:** body system sans-serif, margin 20px, background `#f9f9f9`, text `#333`; h1 `#1a5276` centered; `h3` in cells `#1a5276` 1.0em weight 700; paragraphs/bullets 14px, line-height 1.5-1.6; `.pitfall-label` inline-block bold 0.72em uppercase with 0.5px letter-spacing. Canvas CSS `width: 100%; height: auto`. No nav bar, no back/home links.
- **Pitfall label colors:** assigned by a small script cycling through `["#795548","#2980b9","#27ae60","#e74c3c","#8e44ad","#e67e22","#16a085","#d35400","#c0392b","#1abc9c"]` in document order.
- **Data generation:** single page-level seeded RNG `mulberry32(42)` shared across charts (consumed in document order), Box-Muller `randn()`, inverse-CDF `randExp(lambda)`.
- **Histogram helper:** shared `drawHistogram(canvasId, data, options)` — options bins/xLabel/yLabel/title/color/xMin/xMax/xDecimals; stroke always `#1a5276`; white plot background; margins top 35 / right 20 / bottom 45 / left 55; title bold 13px `#1a5276` centered; `#333` axes with tick marks, 5 x ticks, quarter y ticks with `#eee` gridlines; rotated y-axis label; plus a Gaussian-smoothed density line (`#1a252f`, sigma 1.5 bins) with a 95% SE band (`rgba(44,62,80,0.18)`, effective n clamped to [30, 200]); the density/SE overlay is skipped when `density: false` is passed (surge and tip charts).
- **Canvas scaling:** all canvases set `max-width` to the intrinsic width, size the backing store to the displayed width (`getBoundingClientRect().width`, falling back to the intrinsic width) × `window.devicePixelRatio`, and `ctx.scale` by that combined factor.
- **Palette:** primary blue `#1a5276`, green `#27ae60`/`#1e8449`, red `#e74c3c`, orange `#e67e22`/`#d35400`, accent blue `#2980b9`, dark navy `#1a252f`/`rgba(44,62,80,…)`, dark red `#c0392b`, grays `#95a5a6`/`#7f8c8d`, gray text `#555`/`#666`/`#333`.
- Note: regenerated HTML pages link nowhere (detail page); any grid page linking here uses the `.html` extension.
