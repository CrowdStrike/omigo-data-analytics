# Area Under a Curve

**Page type:** detail page (tutorial layout: 4 card-sections; all sections use a two-column table.layout 50%/50%, section 3 stacks two canvases in its viz cell)
**HTML title tag:** Area Under a Curve

**Subtitle:** Adding up thin slices to get a total — from distance traveled to probability to the AUC metric

## A Broken Odometer, a Working Speedometer

**Tags:** `core idea` (blue), `running example` (green), `accumulation` (blue)

- **The fix** — a delivery van's odometer dies; only the speedometer works for the next hour
- **One reading** — 60 km/h held for 10 minutes covers 60 × (1/6) = 10 km: speed × time
- **A whole trip** — speed keeps changing, so slice the hour thin and total the little pieces
- **The picture** — each piece is a skinny rectangle under the speed curve; the total is the area
- **The name** — adding infinitely thin slices is called integration; the answer is the integral

*Example:* For this hour of driving, the shaded area under the speed curve is 45 — the van went 45 km.

**Key point:** Height is the rate at one instant; area is the total accumulated over time. Area under speed = distance — no odometer needed.

### Visualization (canvas `c1`, 720×300)

Area chart: one hour's speed curve with the area beneath shaded.

- **Title (bold 15px, `#1a5276`, top center):** "One Hour on the Speedometer — the Shaded Area Is the Distance".
- **Data:** piecewise-linear speed profile through readings at minutes [0, 10, 20, 30, 40, 50, 60] of [30, 45, 60, 60, 45, 30, 30] km/h, sampled every 2 minutes.
- **Axes:** x 0–60 minutes (tick labels every 10), y 0–75 with labels 15, 30, 45, 60, 75 (gray `#444` 12px); gray `#999` L-shaped axes; padding top 50 / bottom 48 / left 62 / right 30. Axis titles: "minutes into the trip" bottom center, "speed (km/h)" rotated on left.
- **Fill:** area under the curve shaded `rgba(42,120,214,0.22)`; curve stroked blue `#2a78d6` 3px.
- **Annotations (centered at x=30 min):** bold 16px `#1a5276` "area = 45 km traveled" inside the shaded region; bold orange `#d95926` 13px above the curve: "height = speed now; area = distance so far".

## Six Strips of Ten Minutes

**Tags:** `worked example` (green), `integration` (blue)

- **The readings** — every 10 min the speedometer shows 30, 45, 60, 60, 45, 30 km/h
- **Strip width** — 10 minutes = 1/6 hour; each strip's area = speed × 1/6
- **Strip areas** — 5, 7.5, 10, 10, 7.5, 5 km — six rectangles you can check by hand
- **Total** — 5 + 7.5 + 10 + 10 + 7.5 + 5 = 45 km for the hour
- **Thinner is truer** — 1-minute strips catch the speed changes the 10-minute strips smooth over

*Example:* Check the third strip: 60 km/h for 1/6 of an hour is 60 / 6 = 10 km.

**Key point:** Integration is nothing exotic — multiply, add, repeat. Calculus just pushes the strip width toward zero so no wiggle is missed.

### Visualization (canvas `c2`, 720×300)

Riemann-strip chart: six 10-minute rectangles with the smooth curve overlaid.

- **Title (bold 15px, `#1a5276`, top center):** "Six 10-Minute Strips: 5 + 7.5 + 10 + 10 + 7.5 + 5 = 45 km".
- **Data:** strip heights (readings) [30, 45, 60, 60, 45, 30] km/h over minutes 0–60; strip area labels ['5', '7.5', '10', '10', '7.5', '5'] km.
- **Strips:** rectangles filled `rgba(25,158,112,0.25)`, stroked aqua `#199e70` 1.5px; area labels bold 13px `#1a5276` centered inside each strip ("5 km" etc.); reading value in gray `#444` 12px just above each strip top.
- **Overlay:** the same piecewise-linear speed curve as c1, blue `#2a78d6` 2.5px.
- **Axes:** same frame and scales as c1 (x 0–60, y max 75); x tick labels every 10; x-axis caption: "minutes (strip width 1/6 h; bar height = speedometer reading)".
- **Annotation (bold orange `#d95926` 13px, left-aligned near x=41 min, high on the plot):** "thinner strips hug" / "the curve tighter".

## The Same Trick Prices Risk: Probability and AUC

**Tags:** `where it's used` (blue), `probability` (blue), `AUC` (blue)

- **Probability** — chance = area under the probability curve; the whole curve holds area 1.0
- **Reading it** — P(delivery takes 25–35 min) is the shaded slice, about 0.68 here
- **Never height** — a single point on a density has zero width, so zero probability
- **AUC metric** — "area under the ROC curve" grades a classifier; this one scores 0.81
- **Meaning** — AUC 0.81: a random spam email outranks a random good one 81% of the time
- **Without it** — you would compare classifiers threshold by threshold; one area settles it

*Example:* Both shaded regions are the delivery-van trick again: total a quantity by shading under a curve (illustrative data).

**Key point:** Distance, probability, and AUC are one idea wearing three outfits — a total computed as area under a curve.

### Visualization (canvas `c3a`, 420×300)

Probability density curve with a shaded central slice.

- **Title (bold 15px, `#1a5276`):** "Probability = Area"; subtitle (12px gray `#6b7280`): "delivery times, illustrative".
- **Density:** Gaussian-like curve dens(x) = 0.0798·exp(−(x−30)²/50) over x 10–50 minutes, stroked violet `#4a3aa7` 3px; y max 0.09; padding top 56 / bottom 52 / left 34 / right 20.
- **Shaded slice:** region 25–35 min filled `rgba(213,81,129,0.25)`.
- **Axis:** gray `#999` baseline; x tick labels at 10, 20, 25, 30, 35, 40, 50; axis caption "delivery time (minutes)" (gray `#444` 12px).
- **Annotations:** bold magenta `#d55181` 14px centered below the peak: "P(25–35 min)" / "≈ 0.68"; bold 12px `#1a5276` above the peak: "whole curve: area = 1.0".

### Visualization (canvas `c3b`, 420×300)

ROC curve with the area beneath shaded.

- **Title (bold 15px, `#1a5276`):** "AUC = Area Too"; subtitle (12px gray `#6b7280`): "spam classifier ROC, illustrative".
- **Data:** fpr = [0, 0.05, 0.1, 0.2, 0.3, 0.4, 0.6, 0.8, 1]; tpr = [0, 0.35, 0.52, 0.68, 0.78, 0.85, 0.93, 0.98, 1].
- **Curve:** aqua `#199e70` 3px polyline; area under it filled `rgba(25,158,112,0.22)`; padding top 52 / bottom 52 / left 46 / right 18.
- **Diagonal:** dashed gray `#6b7280` (dash 5/4, 1.5px) from (0,0) to (1,1).
- **Axes:** gray `#999` L-shape; tick labels "0" and "1" on x; axis captions "false alarm rate" (bottom) and "true catch rate" (rotated left), gray `#444` 12px.
- **Annotations:** bold green `#008300` 16px at ~(0.55, 0.42): "AUC = 0.81"; bold gray `#6b7280` 12px near the diagonal: "diagonal = 0.5" / "(coin flip)".

## Tall Curve or Flat Curve — the Area Decides

**Tags:** `common mistake` (red), `rule of thumb` (blue)

- **The mix-up** — people read the peak ("hit 75 km/h!") when the question needs the area
- **Van A** — steady 45 km/h for the full hour: area = 45 × 1 = 45 km
- **Van B** — 75 km/h for 24 min, then 25 km/h for 36 min: 30 + 15 = 45 km
- **Same total** — wildly different curves, identical area — both vans arrive 45 km away
- **Rule** — ask "rate right now?" read the height; ask "how much in total?" read the area

*Example:* Van B's driver brags about the 75 km/h peak, but the slow second leg gives back everything the sprint gained.

**Common mistake:** Judging totals by peaks. A short spike over a thin slice adds little area — a modest rate held a long time often accumulates more.

### Visualization (canvas `c4`, 720×300)

Two side-by-side step-profile panels with equal shaded areas, split by a dashed divider at x=368.

- **Title (bold 15px, `#1a5276`, top center):** "Two Very Different Hours, the Same 45 km".
- **Panels** (each 265px wide, baseline y=236, chart height 165, y max 85; segment bars shaded at 25% alpha of the panel color with a 3px top stroke; speed labels in gray `#444` 12px above each segment; x labels "0" and "60 min"):
  - Left at x=62 — "Van A: steady" (blue `#2a78d6`): one segment 45 km/h from 0–60 min; note in bold blue 14px: "area = 45 × 1 h = 45 km".
  - Right at x=408 — "Van B: sprint then crawl" (orange `#d95926`): segments 75 km/h 0–24 min and 25 km/h 24–60 min; note: "area = 30 + 15 = 45 km".
- **Bottom caption (bold magenta `#d55181` 13px, centered):** "the peak is higher on the right — the area is identical".

## Regeneration instructions

- **Template:** tutorials topic page (see `tutorials/CLAUDE.md`): `<h1>` (no index number) with 2px `#2980b9` bottom border, `.subtitle` gray one-liner, then 4 `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout`. Every section uses `td.text-col` (50%) + `td.viz-col` (50%); section 3's viz cell stacks two canvases, c3a (420×300) and c3b (420×300, `margin-top:12px`).
- **Text column structure:** `.tags` row of colored pill spans (0.72rem bold, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`); then a `<ul>` of one-line bullets each opening with `<b>` in `#1a5276`; one italic `.example` line (`#555`); one `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, 0.9rem) starting with `<strong>Key point:</strong>` or `<strong>Common mistake:</strong>`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; canvases `width:100%` with `1px solid #e0e0e0` border, 4px radius.
- **Canvases:** intrinsic 720×300 default; the `setup(id, lw, lh)` helper accepts overrides (c3a 420×300, c3b 420×300). Scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Chart JS palette object: blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Shared `speedAt(m)` piecewise-linear interpolator over speedT/speedV used by c1 and c2. All data arrays hardcoded (no Math.random). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Site palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links would use `.html` extensions.
