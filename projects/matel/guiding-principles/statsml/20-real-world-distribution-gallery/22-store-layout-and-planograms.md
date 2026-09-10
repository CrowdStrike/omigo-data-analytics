# Store Layout & Planograms — Distribution Patterns

**Page type:** detail page (3-column obj-table layout: text left 38%, histogram canvas center 31%, insight canvas right 31%, one table per pattern)
**HTML title tag:** Store Layout & Planograms — Distribution Patterns

**Subtitle:** 5 distribution shapes in how shoppers move through, look at, and spend across store space

## Zone Traffic (Power Law)

**Label:** SPATIAL POWER LAW (color `#795548`)

Simulated foot traffic follows a power law — the busiest zones (entrances, endcaps) see 5-10x the visits of a typical mid-aisle spot. A small slice of floor space draws a disproportionate share of eyeballs — one reason placement is fought over in retail.

- Pareto(alpha=1.5, xmin=10) — heavy right tail
- Top 5% of zones average ~8x the median zone
- Busiest 20% of floor space draws ~half of all traffic
- Flat rent-per-sq-ft valuation ignores this skew

### Visualization (canvas `canvas1`, 420×340)

Histogram (shared `drawHistogram` helper, see Regeneration instructions).

- **Data:** 3000 draws from Pareto(alpha=1.5, xmin=10) via inverse CDF `x = 10 / u^(1/1.5)`, values > 500 discarded; seeded RNG mulberry32(42).
- **Bins/range:** 40 bins, x from 0 to 500.
- **Title:** "Zone Traffic (visits/hour)". **X label:** "Visits per Hour". X tick format: integer.
- **Colors:** bar fill `rgba(230,126,34,0.5)`, bar border `#e67e22`.

### Visualization (canvas `canvas1b`, 400×340)

Lorenz curve of floor space vs traffic, computed from the same simulated data (sorted ascending, ~50 sample points plus endpoint (1,1)).

- **Title (bold, `#e67e22`, top center):** "Lorenz Curve — Floor Space vs Attention".
- **Axes:** gray `#999` L-shaped axes, padding top 35 / right 20 / bottom 45 / left 50. X label "Cumulative % of Floor Space" (bottom center); Y label "Cumulative % of Traffic" (rotated vertical, left).
- **Equality diagonal:** dashed `#aaa` line (dash 5/4, width 1.5) from bottom-left to top-right, with rotated gray `#888` 10px label "Perfect equality" along it at ~25% width, 60% height.
- **Lorenz curve:** `#e67e22`, width 3; Gini area between diagonal and curve shaded `rgba(230,126,34,0.2)`.
- **80/50 annotation:** dashed `#c0392b` guides (dash 3/3, width 1.5) — vertical at x=80% of floor up to y=49.5% of traffic, horizontal from y-axis to that point. Bold red `#c0392b` 11px labels: "80% of floor" below the x-axis at the vertical guide; "~50%" / "traffic" (two lines) left of the y-axis at the horizontal guide.
- **Gini label (`#e67e22`, centered at ~55% width, 45% height):** bold 12px "GINI AREA", then 10px "20% of space gets" / "half the eyeballs".

## Eye-Level Shelf (Normal 120-150cm)

**Label:** EYE LEVEL = BUY LEVEL (color `#2980b9`)

Simulated gaze fixation follows a tight normal centered at 135cm — the "eye level" band. In this illustration, eye-level placement converts ~2.5x the knee or top-shelf positions — consistent with why brands compete for this narrow band.

- Normal(135, 12) — tight concentration at eye height
- Eye level (120-150cm) holds ~78% of fixations
- Illustrative conversion: eye level 2.5x other shelves
- Retail lore: top shelf = aspirational, floor = kids' eye level

### Visualization (canvas `canvas2`, 420×340)

Histogram (shared helper).

- **Data:** 2000 draws from Normal(135, 12) (Box-Muller on the shared seeded RNG).
- **Bins/range:** 30 bins, x from 80 to 190.
- **Title:** "Gaze Fixation Height (cm)". **X label:** "Shelf Height (cm)". X tick format: integer + "cm".
- **Colors:** bar fill `rgba(41,128,185,0.5)`, bar border `#2980b9`.

### Visualization (canvas `canvas2b`, 400×340)

Vertical shelf-zone diagram with a conversion heat bar per zone.

- **Title (bold, `#2980b9`, top center):** "Shelf Zone Conversion Heatmap".
- **Zones (top to bottom, each a rectangle ~40% of width on the left plus a horizontal heat bar to the right whose length is proportional to conversion):**
  - Top Shelf, "170-190cm", conversion bar length factor 0.4, fill `rgba(41,128,185,0.2)`, text `#2980b9`, value label "1.0x"
  - Eye Level, "120-150cm", factor 1.0, fill `rgba(231,76,60,0.6)`, text `#c0392b`, value label "2.5x"
  - Knee Level, "70-100cm", factor 0.35, fill `rgba(230,126,34,0.25)`, text `#e67e22`, value label "0.9x"
  - Floor Level, "0-50cm", factor 0.25, fill `rgba(149,165,166,0.3)`, text `#7f8c8d`, value label "0.6x"
- Each shelf rectangle shows the zone name (bold 12px) over the height range (10px), stroked in its text color.
- **Arrow:** red `#c0392b` arrow (width 2.5) from left margin pointing to the Eye Level zone, annotated in `#c0392b` with bold 14px "2.5x" and 9px "conv." above the arrow start.

## Dwell Time by Aisle (Bimodal)

**Label:** GRAB VS BROWSE (color `#27ae60`)

Simulated dwell time is bimodal — quick-grab visits (exponential, mean 15s) and browsing visits (normal, mean 200s). The "average dwell time" of ~90s describes almost nobody — a planogram tuned to the average fits neither mode.

- Mode 1: Grab shoppers — exponential(mean=15s)
- Mode 2: Browse shoppers — normal(200s, sd=50)
- Grocery aisles skew grab; electronics skews browse
- Mean dwell time (90s) describes neither population

### Visualization (canvas `canvas3`, 420×340)

Histogram (shared helper).

- **Data:** mixture — 1200 draws from Exponential(mean=15s) (`-15·ln(u)`, values > 400 discarded) plus 800 draws from Normal(200, 50) (kept in (0, 400]).
- **Bins/range:** 40 bins, x from 0 to 400.
- **Title:** "Dwell Time per Aisle Visit (seconds)". **X label:** "Seconds". X tick format: integer + "s".
- **Colors:** bar fill `rgba(142,68,173,0.5)`, bar border `#8e44ad`.

### Visualization (canvas `canvas3b`, 400×340)

Two overlaid analytic density curves over x in [0, 400], 200 points each, scaled to a common max.

- **Title (bold, `#8e44ad`, top center):** "Two Populations — Grab vs Browse".
- **Grab curve:** exponential density `(1/15)·exp(-x/15)·1200` — line `#2980b9` width 2.5, fill under `rgba(41,128,185,0.35)`.
- **Browse curve:** normal density `N(200, 50)·800` — line `#8e44ad` width 2.5, fill under `rgba(142,68,173,0.3)`.
- **Gap zone:** gray band `rgba(149,165,166,0.2)` from x=60s to x=130s, with 9px `#95a5a6` label "GAP ZONE" near the bottom of the band.
- **Corner labels:** top-left in `#2980b9` — bold 12px "GRAB", then 10px "Grocery aisle" / "peak ~15s"; top-right in `#8e44ad` — bold 12px "BROWSE", then 10px "Electronics aisle" / "peak ~200s".
- **Axes:** gray `#999`; x ticks "0s", "100s", "200s", "300s", "400s"; x-axis title "Dwell Time (seconds)" in `#333`.

## Fresh Perimeter vs Center (Gradient)

**Label:** PATH ENGINEERING (color `#e74c3c`)

Simulated spending per minute is bimodal — perimeter zones (fresh/high-margin) average $8/min while center aisles (packaged goods) average $3/min. Classic grocery layouts route shoppers along the perimeter first — consistent with essentials sitting at the back.

- Perimeter: Normal(8, 2) — fresh, deli, bakery
- Center: Normal(3, 1.5) — packaged, canned, dry
- Layouts often route the long way through high-margin zones
- Essentials (milk, eggs) typically at the back

### Visualization (canvas `canvas4`, 420×340)

Histogram (shared helper).

- **Data:** mixture — 1500 draws from Normal(8, 2) plus 800 draws from Normal(3, 1.5), positive values only.
- **Bins/range:** 30 bins, x from 0 to 15.
- **Title:** "Spending per Minute by Zone ($/min)". **X label:** "$/min". X tick format: "$" + integer.
- **Colors:** bar fill `rgba(39,174,96,0.5)`, bar border `#27ae60`.

### Visualization (canvas `canvas4b`, 400×340)

Top-down store map diagram.

- **Title (bold, `#27ae60`, top center):** "Store Map — Perimeter-First Path".
- **Store:** outlined rectangle (`#333`, width 2). Perimeter band 35px wide on all four sides filled `rgba(39,174,96,0.35)` (high-margin); center block filled `rgba(149,165,166,0.25)` (packaged).
- **Entrance:** red `#e74c3c` marker (40×8px) centered on the bottom wall, with bold 10px label "ENTRANCE" below.
- **Path:** red `#e74c3c` dashed polyline (dash 4/3, width 2) from the entrance along the bottom perimeter to the right perimeter, up to the top, across to the left, down to mid-height, then into the center; arrowhead at the end.
- **Zone labels (bold 10px `#27ae60`):** "FRESH" (top band center), "DELI" (right band middle), "BAKERY" (left band middle), "PRODUCE" (bottom band center). Center labels in `#7f8c8d` 10px: "PACKAGED" / "CENTER". At top center in `#c0392b`: bold 9px "MILK/EGGS" and 8px "(typically at back)".
- **Annotation (bold 10px `#333`, bottom-left):** "Long route through" / "high-margin zones first".
- **Legend (bottom-right):** green swatch `rgba(39,174,96,0.35)` "High-margin" (text `#27ae60`); gray swatch `rgba(149,165,166,0.4)` "Packaged" (text `#7f8c8d`).

## End-Cap Conversion Lift (Log-Normal)

**Label:** MOST EXPENSIVE REAL ESTATE (color `#8e44ad`)

Simulated end-cap conversion lift follows a log-normal — about half of endcaps land in a modest 1.5-3x lift, but the right tail stretches past 8x. That fat tail is one explanation for the premium fees prime endcap slots command.

- Log-normal: exp(Normal(0.7, 0.5))
- Median lift ~2x; tail stretches past 8x
- Illustration: new-product + seasonal endcaps top the chart
- The fat tail is what premium slotting fees are buying

### Visualization (canvas `canvas5`, 420×340)

Histogram (shared helper).

- **Data:** 1500 draws from exp(Normal(0.7, 0.5)), values > 10 discarded.
- **Bins/range:** 35 bins, x from 0 to 10.
- **Title:** "End-Cap Conversion Lift (multiplier vs shelf)". **X label:** "Conversion Multiplier (x)". X tick format: one decimal + "x".
- **Colors:** bar fill `rgba(231,76,60,0.5)`, bar border `#e74c3c`.

### Visualization (canvas `canvas5b`, 400×340)

Horizontal bar chart of end-cap types.

- **Title (bold, `#e74c3c`, top center):** "End-Cap Type — Conversion Lift".
- **Bars (top to bottom, scale max 9x, left padding 150px for labels; each bar filled with a left-to-right gradient from its color to its color at 53% alpha, stroked in its color):**
  - New Product Launch — 8.2x — `#c0392b`
  - Seasonal Display — 5.4x — `#e74c3c`
  - Price Promo — 3.1x — `#e67e22`
  - Brand Awareness — 2.0x — `#f39c12`
  - Regular Shelf — 1.0x — `#95a5a6`
- Bar name labels right-aligned in `#333` 11px left of bars; value labels ("8.2x" etc.) bold 13px in the bar's color right of each bar.
- **Arrow:** vertical `#c0392b` arrow (width 2) dropping from beside the top bar's end downward with arrowhead.
- **Caption (bold 10px `#555`, bottom center):** "The fat tail is what premium slotting fees buy".

## Regeneration instructions

- **Layout:** one `<table class="obj-table">` per pattern, single `<tr>` with three `<td>`: left 38% text (`.pitfall-label` span + `<h3>` + `<p>` + `<ul>`), center 31% (histogram canvas 420×340), right 31% (insight canvas 400×340), both canvas cells centered. Table cell borders `1px solid #2980b9`, padding 12px, `border-collapse: collapse`.
- **Page CSS:** body system sans-serif (-apple-system stack), margin 20px, background `#f9f9f9`, text `#333`; h1 `#1a5276` centered; `.subtitle` centered `#666` 0.95em; h3 `#1a5276` 1.0em weight 700; p/li 14px, line-height 1.5-1.6; `.pitfall-label` inline-block bold 0.72em uppercase, letter-spacing 0.5px; `canvas { width: 100%; height: auto; }`.
- **Pitfall label colors:** assigned by a trailing script from the cyclic palette `["#795548","#2980b9","#27ae60","#e74c3c","#8e44ad","#e67e22","#16a085","#d35400","#c0392b","#1abc9c"]` in document order (labels 1-5 use the first five).
- **Shared histogram helper (`drawHistogram`):** white plot background; bold 13px `#1a5276` centered title at y=18; gray `#999` L-axes; margins top 35 / right 20 / bottom 40 / left 50; bars normalized to max bin count; overlaid Gaussian-smoothed density line in `#1a5276` (width 2, sigma 1.5 bins) with a 95% SE band filled `rgba(230,126,34,0.22)` (effective N clamped to [30, 200]); 6 x-tick labels in `#555` 11px; optional x-axis label in `#333` 12px. Data simulated with seeded RNG mulberry32(42) and a Box-Muller `randNormal(mean, std)` helper shared across all charts on the page.
- **Canvas scaling:** all canvases declare intrinsic width/height attributes and scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; secondary `#2980b9`, `#8e44ad`, `#c0392b`, `#95a5a6`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions (this page has none).
