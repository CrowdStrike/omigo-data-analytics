# Foot Traffic & In-Store Conversion — Distribution Patterns

**Page type:** detail page (three-column obj-table layout: text left ~38%, histogram canvas middle ~31%, insight canvas right ~31%, one table per section)
**HTML title tag:** Foot Traffic & In-Store Conversion — Distribution Patterns

**Subtitle:** Simulated distribution shapes from physical retail foot traffic and in-store conversion

## Entry→Purchase (Geometric Decay)

**Label:** COMPOUND LOSS (color `#795548`)

Each stage loses 50% of traffic. The histogram of "stage reached" shows geometric decay: losses compound multiplicatively, so doubling any single stage's pass-through doubles final conversions.

- Stage 1 (Enter): 1000 customers
- Stage 2 (Browse): 500 (−50%)
- Stage 3 (Try): 250 (−50%)
- Stage 4 (Buy): 125 (−50%) = 12.5% final conversion

### Visualization (canvas `canvas1`, 420×340)

Histogram of funnel stage reached (geometric decay).

- **Title (bold 13px, `#1a5276`, top center):** "Funnel Stage Reached (Geometric Decay)".
- **Data:** 3000 simulated customers, each contributing one histogram entry per stage they reach: every customer reaches stage 1, and each later stage passes with probability 0.5 (seeded RNG mulberry32, seed 42). Expected bin counts ~3000 / 1500 / 750 / 375 — strict geometric decay (an earlier "stage stopped at" version left stages 3 and 4 with equal mass).
- **Bins/axes:** 4 bins over x range 0.5-4.5; x labels rounded integers; x-axis label "Stage (1=Enter, 2=Browse, 3=Try, 4=Buy)".
- **Bars:** fill `rgba(22,160,133,0.5)`, border `#16a085`; Gaussian-smoothed (sigma 1.5 bins) density line `#1a5276` width 2 with 95% SE band filled `rgba(230,126,34,0.22)`.

### Visualization (canvas `canvas1b`, 400×340)

Funnel diagram of compound conversion loss.

- **Title (bold 13px, `#16a085`, top center):** "Conversion Funnel — Compound Loss".
- **Stages (4 stacked trapezoids, widths proportional to count / 1000, max width 85% of plot, `#333` 1px outline, white bold 12px name label + 10px percent label inside):**
  - ENTER (1000), 100%, fill `rgba(22,160,133,0.8)`.
  - BROWSE (500), 50%, fill `rgba(41,128,185,0.8)`.
  - TRY (250), 25%, fill `rgba(230,126,34,0.8)`.
  - BUY (125), 12.5%, fill `rgba(231,76,60,0.8)` (bottom width 60% of its top width).
- **Between stages:** bold 11px `#e74c3c` "-50%" label with a small red downward arrow at the right edge of each transition (3 transitions).
- **Bottom caption (bold 11px, `#c0392b`, centered):** "Losses compound -- double any stage, double the bottom".

## Time in Store (Bimodal)

**Label:** NOBODY IS AVERAGE (color `#2980b9`)

Two distinct populations: quick-trip shoppers (returns, pickup — exponential mean 4 min) and serious shoppers (normal, mean 35 min). The mixture average of ~16 min lands in the valley and describes almost nobody.

- Quick trip: N=1200, exponential mean=4 min
- Serious shop: N=800, normal mean=35, sd=8
- The ~16-min "average" falls between the two modes
- Suggests two service models, not one

### Visualization (canvas `canvas2`, 420×340)

Bimodal histogram of time spent in store.

- **Title:** "Time Spent in Store (minutes)".
- **Data:** 1200 samples Exponential(mean 4) rejected/retried if ≥ 60; 800 samples Normal(mean 35, sd 8) rejected/retried if outside (0,60).
- **Bins/axes:** 35 bins over x range 0-60; x labels formatted "Nm"; x-axis label "Minutes".
- **Bars:** fill `rgba(41,128,185,0.5)`, border `#2980b9`; standard smoothed density line + SE band overlay.

### Visualization (canvas `canvas2b`, 400×340)

Two analytic density curves with the useless mixture mean marked.

- **Title (bold 13px, `#2980b9`, top center):** "Two Populations — \"Average\" Describes Nobody".
- **Curves (120 points over 0-60 min, normalized to shared max, each stroked width 3 and area-filled to the baseline):**
  - Exponential density `1200 × (1/4)·exp(−x/4)` in `#2980b9`, fill `rgba(41,128,185,0.15)` — peak labeled bold 11px "Quick trip" / "(returns, pickup)" at upper left.
  - Normal density `800 × N(35, 8)` in `#27ae60`, fill `rgba(39,174,96,0.15)` — labeled bold 11px "Serious shop" / "(browsing, trying)" at upper right.
- **Average marker:** vertical dashed (6/4) `#e74c3c` width 2 line at x = 16.4 min with a bold red 3px "X" drawn through it at mid-height; bold 10px `#e74c3c` labels above/below the X: "\"Average\" = 16min" / "DESCRIBES NOBODY".
- **Axis:** gray `#999` baseline only; x labels 10px `#555`: "0m", "15m", "30m", "45m", "60m"; padding top 35, right 15, bottom 40, left 45.

## Conversion by Hour (Two Peaks)

**Label:** STAFF TO THE CURVE (color `#27ae60`)

Conversion peaks at lunch (12pm) and after-work (5:30pm) — two overlapping normals. A flat staffing line (dashed) misses both peaks: understaffed at the rushes, overstaffed in between.

- Peak 1: lunch rush, mean=12, sd=1.5
- Peak 2: after-work, mean=17.5, sd=1.2
- Flat schedule misses both peaks
- Understaffed peaks = lost conversions

### Visualization (canvas `canvas3`, 420×340)

Two-peak histogram of conversion events by hour.

- **Title:** "Conversion Events by Hour".
- **Data:** 1200 samples Normal(mean 12, sd 1.5) and 800 samples Normal(mean 17.5, sd 1.2), each rejected/retried if outside [8,22].
- **Bins/axes:** 24 bins over x range 8-22; x labels formatted as am/pm hours (e.g. "8am", "12pm", "5pm"); x-axis label "Hour of Day".
- **Bars:** fill `rgba(230,126,34,0.5)`, border `#e67e22`; standard smoothed density line + SE band overlay.

### Visualization (canvas `canvas3b`, 400×340)

Overlay chart: staffing line vs two-peak demand curve with understaffed gaps shaded.

- **Title (bold 13px, `#e67e22`, top center):** "Staff Scheduled vs Conversion Demand".
- **Demand curve:** 28 half-hour points over 8am-10pm; `exp(−0.5·((t−12)/1.5)²) + 0.7·exp(−0.5·((t−17.5)/1.2)²)`; solid `#27ae60` width 3 with area filled `rgba(39,174,96,0.2)`; scaled to 90% of plot height.
- **Staff curve:** `0.45 + 0.15·exp(−0.5·((t−14)/2.5)²)` (flat with a mild hump at 2pm shift overlap); dashed (6/4) `#2980b9` width 2.5.
- **Understaffed zones:** wherever demand exceeds staff by more than 0.1, the vertical gap between the curves is filled `rgba(231,76,60,0.2)`.
- **Legend (upper left, bold 10px):** `#27ae60` "-- Conversion rate"; `#2980b9` "-- Staff scheduled"; at ~55% width in `#e74c3c`: "   Revenue lost here" with a red 2px arrow pointing down-left into the lunch-peak gap.
- **Axis:** gray `#999` baseline; x labels 10px `#555`: "8am", "12pm", "3pm", "5:30pm", "10pm"; padding top 38, right 15, bottom 40, left 45.

## Group Size (Monotone Decreasing)

**Label:** DIFFUSION OF RESPONSIBILITY (color `#e74c3c`)

In this simulation, solo shoppers convert at 38% and each added person lowers the rate — pairs 28%, triples 18%, groups of 6 just 5%. Consistent with diffusion of responsibility: "someone else will decide."

- Solo = 38% conversion (highest)
- Pair = 28%, Triple = 18%
- Group of 4 = 12%, 5 = 8%, 6 = 5%
- One read: tailor the sales approach to group size

### Visualization (canvas `canvas4`, 420×340)

Histogram of conversions by group size.

- **Title:** "Conversions by Group Size".
- **Data:** for group sizes 1-6, conversion rates `[0.38, 0.28, 0.18, 0.12, 0.08, 0.05]` × traffic populations `[500, 400, 300, 200, 150, 100]` × 5 (visibility scaling) samples per size — i.e. bar heights proportional to 950/560/270/120/60/25.
- **Bins/axes:** 6 bins over x range 0.5-6.5; x labels rounded integers; x-axis label "Group Size".
- **Bars:** fill `rgba(142,68,173,0.5)`, border `#8e44ad`; standard smoothed density line + SE band overlay.

### Visualization (canvas `canvas4b`, 400×340)

Line chart of conversion rate by group size.

- **Title (bold 13px, `#8e44ad`, top center):** "Conversion Rate by Group Size".
- **Data:** rates `[38, 28, 18, 12, 8, 5]`% at x labels `["Solo", "Pair", "Triple", "4", "5", "6"]`.
- **Axes:** L-shaped gray `#999` axes; y scale 0-45% with 10px `#555` labels every 10% (0%-40%) and light `#eee` horizontal gridlines; padding top 38, right 25, bottom 40, left 50.
- **Series:** connected line `#8e44ad` width 3; 6px-radius `#8e44ad` dots with white 2px outline; bold 11px `#333` value label ("38%" etc.) above each point; 10px `#555` category label below the axis.
- **Annotations:** at the Solo point, bold 10px `#27ae60` two-line label "HIGHEST" / "CONVERSION" with a short green arrow; near groups 5-6, bold 9px `#e74c3c` two-line label "\"Someone else" / "will decide\"".
- **Bottom caption (bold 10px, `#333`, centered):** "One read: tailor the approach to group size".

## Weather → Traffic Shift (Conditional)

**Label:** STRONGEST PREDICTOR (color `#8e44ad`)

Rain days center at 180 visitors/hr, sunny days at 120 — a +50% shift with zero marketing spend. One implication: control for weather before crediting any promo for a traffic bump.

- Rain day: mall traffic UP, normal mean=180, sd=30
- Sunny day: mall traffic DOWN, normal mean=120, sd=25
- Street retail plausibly moves the opposite way (not simulated)
- An uncontrolled variable bigger than most promo lifts

### Visualization (canvas `canvas5`, 420×340)

Overlapping-bimodal histogram of mall foot traffic on rain vs sun days.

- **Title:** "Mall Foot Traffic (Rain vs Sun Days)".
- **Data:** 1000 samples Normal(mean 180, sd 30) (rain days) and 1000 samples Normal(mean 120, sd 25) (sunny days), each rejected/retried if outside (50,280).
- **Bins/axes:** 30 bins over x range 50-280; x labels rounded integers; x-axis label "Visitors per Hour".
- **Bars:** fill `rgba(39,174,96,0.5)`, border `#27ae60`; standard smoothed density line + SE band overlay.

### Visualization (canvas `canvas5b`, 400×340)

Overlaid per-condition distributions decomposing the canvas5 mixture (computed from the same rainData/sunData samples).

- **Title (bold 13px, `#27ae60`, top center):** "Rain vs Sun — The Whole Distribution Shifts".
- **Series:** step-outline histograms (30 bins over 50-280, shared y scale) — sunny filled `rgba(241,196,15,0.35)` stroked `#f39c12`; rain filled `rgba(41,128,185,0.30)` stroked `#2980b9`.
- **Mean markers:** dashed (5/4) vertical lines at each computed sample mean, labeled below the axis bold 11px in the series color: "SUNNY <mean>/hr", "RAIN <mean>/hr".
- **Shift arrow:** horizontal `#1a5276` 2px arrow from the sunny mean to the rain mean near the top, labeled bold 11px with the computed lift "+<pct>%".
- **X axis:** gray `#999` baseline with 10px `#555` label "Visitors per Hour".
- **Bottom caption (bold 11px, `#1a5276`, centered, two lines):** "The whole distribution shifts with zero marketing spend --" / "control for weather before crediting the promo."

## Regeneration instructions

- **Layout:** one `.obj-table` per section (full-width, border-collapse), each with a single `<tr>` of three `<td>`s: first 38% (text: `.pitfall-label` span, `<h3>` title, `<p>` paragraph, `<ul>` bullets), second 31% centered (histogram canvas), third 31% centered (insight canvas). Section order as above.
- **Page style:** body system sans-serif, margin 20px, background `#f9f9f9`, text `#333`; h1 `#1a5276` centered; `.subtitle` centered `#666` 0.95em; table cell borders `1px solid #2980b9`, padding 12px; h3 `#1a5276` 1.0em weight 700; paragraphs/bullets 14px, line-height 1.5-1.6; `.pitfall-label` inline-block bold 0.72em uppercase, letter-spacing 0.5px. No nav bar, no back/home links.
- **Pitfall label colors:** assigned by index from the cycling palette `["#795548","#2980b9","#27ae60","#e74c3c","#8e44ad","#e67e22","#16a085","#d35400","#c0392b","#1abc9c"]` via a small script that sets each `.pitfall-label`'s color.
- **Canvases:** intrinsic sizes as given (420×340 histograms, 400×340 insight charts), CSS `width: 100%; height: auto`; every canvas scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Shared histogram helper:** white background, centered bold 13px `#1a5276` title, gray `#999` L axes (margins 35/20/40/50), per-bin bars with 1px gap, Gaussian-smoothed (sigma 1.5 bins) density line `#1a5276` width 2 over a 95% SE band filled `rgba(230,126,34,0.22)`, 6 x-tick labels 11px `#555` with optional 12px `#333` x-axis label. Data generated with seeded mulberry32(42) RNG and Box-Muller normal sampler.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`/`#f39c12`, purple `#8e44ad`, teal `#16a085`.
