# Real Estate — Distribution Patterns

**Page type:** detail page (three-column obj-table layout: text left 38%, two canvases 31% each, one table per section)
**HTML title tag:** Real Estate — Distribution Patterns

## Home Prices (School District Shatters Aggregate)

**Pitfall label (uppercase, `#795548`):** MIXTURE OF NORMALS

Aggregate looks log-normal (smooth right skew). Split by school rating and it shatters into four tight normals with different means and variances. One reading: "the housing market" is really several parallel markets stacked. In this simulation, otherwise similar homes on opposite sides of a school-band boundary differ by $120-170K with zero physical change.

- Aggregate: looks log-normal (misleading smooth curve)
- By school district: shatters into four tight normals
- "The housing market" behaves like parallel markets stacked
- Simulated boundary effect: $120-170K gap between adjacent bands, zero physical change

### Visualization (canvas `canvas1`, 420×340)

Histogram with per-bin coloring by dominant mixture component.

- **Title (bold 13px, `#1a5276`, top center):** "Home Prices — Mixture of 4 Normals (colored by school district)".
- **Data (seeded RNG mulberry32(42), Box-Muller normals):** 400 samples of `N(220000, 30000)` (low-rated schools); 500 of `N(340000, 35000)` (mid); 350 of `N(480000, 40000)` (high); 150 of `N(650000, 50000)` (elite).
- **Bins/range:** 50 bins, x $100K-$850K, tick format "$NK"; x-axis label "Price ($)".
- **Bar colors (each bin colored by whichever weighted component density dominates at its center):** low `rgba(231,76,60,0.45)` red; mid `rgba(230,126,34,0.45)` orange; high `rgba(39,174,96,0.45)` green; elite `rgba(26,82,118,0.45)` blue.
- **Density line + SE band (standard for histograms drawn via the shared utility):** Gaussian-smoothed counts (sigma 1.5 bins), line `#5d4037` width 2, 95% band `rgba(121,85,72,0.18)` using effective N clamped to [30, 200].
- **Layout:** white background, gray `#999` L-axes, y count labels at 5 positions, margins top 40 / right 20 / bottom 45 / left 50.

### Visualization (canvas `canvas1b`, 400×340)

Box plot of price by school-rating band.

- **Title (bold 12px):** "Box Plot — 4 Hidden Markets by School Rating".
- **Boxes (per component, whiskers at P5/P95, box Q1-Q3, white median line):** "Low (3-4)" `rgba(231,76,60,0.7)`; "Mid (5-6)" `rgba(230,126,34,0.7)`; "High (7-8)" `rgba(39,174,96,0.7)`; "Elite (9-10)" `rgba(26,82,118,0.7)`. Whisker lines/caps `#555`.
- **Y-axis:** $100K-$850K with "$NK" labels at 6 positions and `#eee` gridlines; x-axis label "School Rating Band".
- **Annotation:** dashed red `#e74c3c` vertical double line between the High and Elite boxes with arrowhead and bold 10px label "Gap!".

## Lot Size (Fossil Record of Zoning Codes)

**Pitfall label (uppercase, `#2980b9`):** DISCRETE SPIKES

Not continuous at all. Needle spikes at 0.12, 0.15, 0.25, 0.33, 0.5, 1.0 and 2.0 acres. Almost nothing between. Lots come in whatever the developer platted in that era — in this simulated market: pre-war estates = 2.0 acres, 1950s = 1.0, 1960s = 0.5, 1970s = 0.33, 1980s = 0.25, 1990s = 0.15, 2010s infill = 0.12. The distribution reads like a fossil record of zoning codes.

- Needle spikes at platted sizes from 0.12 to 2.0 acres
- Almost nothing between (not continuous)
- Each spike = a developer/era's standard plat
- Can date neighborhood construction from spike positions

### Visualization (canvas `canvas2`, 420×340 — drawn at 480×340 internally)

Hand-drawn needle-spike chart (not the shared histogram utility).

- **Title:** "Lot Size — Discrete Spikes (Developer Platting Eras)".
- **Spikes (6px-wide bars, fill `rgba(26,82,118,0.6)`, stroke `#1a5276`, bold 10px value label above each):**

| Acres | Count |
|-------|-------|
| 0.12 | 180 |
| 0.15 | 320 |
| 0.25 | 550 |
| 0.33 | 120 |
| 0.50 | 400 |
| 1.00 | 250 |
| 2.00 | 60 |

- **Noise floor:** 80 faint random 3px bars in `rgba(26,82,118,0.12)` at heights up to ~30% of a 15-count level.
- **Axes:** x 0-2.2 acres, ticks 1 decimal at 5 positions, label "Acres"; y count labels 0-550 at 5 positions.

### Visualization (canvas `canvas2b`, 400×340)

Bubble timeline: lot size by construction era.

- **Title (bold 12px):** "Lot Size = Fossil Record of Zoning Eras"; subtitle 10px `#666`: "(bubble size = number of lots)".
- **Timeline:** horizontal `#aaa` axis with an arrowhead at the right end; bubbles placed left-to-right by decade, vertical offset proportional to lot size; dashed `#ccc` connector from each bubble down to the axis; decade labels below.
- **Bubbles (radius 8-28px scaled by count; white bold acre value inside):**

| Decade | Acres | Count | Color |
|--------|-------|-------|-------|
| 1950s | 1.00 | 250 | rgba(26,82,118,0.75) |
| 1960s | 0.50 | 400 | rgba(39,174,96,0.75) |
| 1970s | 0.33 | 120 | rgba(230,126,34,0.75) |
| 1980s | 0.25 | 550 | rgba(142,68,173,0.75) |
| 1990s | 0.15 | 320 | rgba(231,76,60,0.75) |
| 2010s | 0.12 | 180 | rgba(44,62,80,0.75) |

- **Annotations:** rotated y-axis label "Lot Size (acres)"; dashed red `#e74c3c` trend arrow from the 1950s bubble down to the 2010s bubble labeled bold 10px "Shrinking over time".

## Price/SqFt (Land-Value vs Structure-Value)

**Pitfall label (uppercase, `#27ae60`):** BIMODAL (TWO REGIMES)

One mode at $150-$250/sqft, a second at $800-$1500/sqft, sparse valley between. One explanation: in the left mode you're paying for the structure (renovation adds value); in the right mode you're paying for the land (teardowns sell near renovated prices). The valley marks where that logic flips.

- Left mode ($150-250): structure dominates (renovation adds value)
- Right mode ($800-1500): land dominates (teardown ≈ renovated price)
- Valley between = geographic line where logic flips
- "Price/sqft" means two different things depending on mode

### Visualization (canvas `canvas3`, 420×340)

Histogram: bimodal price per square foot.

- **Title:** "Price/SqFt — Bimodal (Structure vs Land Value Regimes)".
- **Data:** 700 suburb samples of `N(200, 35)`; 350 urban-core samples of `N(1050, 180)`; 50 valley samples of `N(450, 80)`.
- **Bins/range:** 50 bins, x $50-$1600, tick format "$N"; x-axis label "$/sqft".
- **Bars:** fill `rgba(231,76,60,0.35)`, stroke `#c0392b`. Standard density line + SE band.

### Visualization (canvas `canvas3b`, 400×340)

ECDF with regime shading and flip point.

- **Title (bold 12px):** "ECDF — Two Plateaus Reveal the Regimes".
- **Curve:** ECDF of the same data, line `#1a5276` width 2.5, x $50-$1600, y 0-1 (labels 0.00-1.00 at 5 positions).
- **Regimes:** left of $450 tinted `rgba(39,174,96,0.1)` with two-line bold 11px green label "STRUCTURE / dominates"; right tinted `rgba(231,76,60,0.1)` with red label "LAND / dominates"; dashed red `#e74c3c` vertical flip line at $450 (dash 5/4, width 2).
- **Annotation:** orange `#e67e22` horizontal arrow across the plateau at ~67% cumulative height, labeled bold 10px "Flat = valley".
- **Axes:** x ticks "$50"…"$1600" at 5 positions, label "$/sqft"; margins top 40 / right 20 / bottom 50 / left 55.

## Days on Market (Spike = Market Thermometer)

**Pitfall label (uppercase, `#e74c3c`):** EXPONENTIAL + SPIKE

Spike at 0-3 days (sold almost immediately, often over asking), then exponential decay for the rest. The spike mass acts as a market thermometer: this simulated hot market puts 40% of sales inside 72 hours; in a cold market the spike shrinks toward single digits. One use: the spike mass can shift before headline price metrics react.

- Spike at 0-3 days = sold almost immediately
- Exponential tail for rest of market
- Spike mass = a compact market-heat metric
- Can shift before headline price metrics react

### Visualization (canvas `canvas4`, 420×340)

Histogram: spike plus exponential decay.

- **Title:** "Days on Market — Spike at 0-3 Days + Exponential Decay".
- **Data:** 400 spike samples of `U*3` days; 600 tail samples of `3 + Exp(0.04)`; all clamped to ≤180 days.
- **Bins/range:** 50 bins, x 0-180, 6 ticks with format "Nd"; x-axis label "Days".
- **Bars:** fill `rgba(39,174,96,0.35)`, stroke `#27ae60`. Density overlay disabled (`density: false`) — smoothing would smear the 0-3 day spike into the exponential tail.

### Visualization (canvas `canvas4b`, 400×340)

Market thermometer: stacked speed bar plus temperature dial.

- **Title (bold 12px):** "Market Thermometer — Where Do Sales Land?"; subtitle 10px `#666`: "Stacked bar shows sale speed distribution".
- **Stacked horizontal bar (50px tall, white 2px separators, bold white percentage inside segments wider than 30px; percentages computed from the data):** "0-3d (Bidding War)" `rgba(231,76,60,0.8)`; "4-7d" `rgba(230,126,34,0.8)`; "8-14d" `rgba(241,196,15,0.8)`; "15-30d" `rgba(39,174,96,0.7)`; "30d+ (Stale)" `rgba(26,82,118,0.6)`.
- **Legend:** swatch + label row below the bar.
- **Temperature dial (bottom center, 45px radius semicircle):** gray `#eee` background arc, four colored quadrant arcs `#27ae60` / `#f1c40f` / `#e67e22` / `#e74c3c` (cold→hot), blue `#1a5276` needle angled by spike fraction with center dot; "COLD" label (green, left) and "HOT" (red, right); bold 11px `#1a5276` caption below needle: "<pct>% sell in 72hrs" (~40%).

## School Rating Premium (Binary, Not Linear)

**Pitfall label (uppercase, `#8e44ad`):** STEP FUNCTION

Simulated premium is flat from rating 1-6, jumps ~$80K at rating 7, then flat again 7-10. Consistent with buyers paying for a threshold, not a gradient: below the line reads as "bad schools", above as "good schools", with little differentiation inside each band.

- Flat from 1-6 (no premium difference within "bad")
- Vertical jump at 7 (the threshold)
- Flat from 7-10 (undifferentiated within "good")
- Step shape is consistent with a binary buyer heuristic

### Visualization (canvas `canvas5`, 420×340 — drawn at 480×340 internally)

Scatter plot with step-function overlay (hand-drawn).

- **Title:** "School Rating vs Price Premium — Step Function".
- **Data:** 200 points — integer rating `1 + floor(U*10)` (what buyers see), displayed with ±0.15 x jitter; premium `5000 + N(0,1)*8000` for ratings 1-6, `85000 + N(0,1)*10000` for ratings 7-10. Dots 3px in `rgba(26,82,118,0.3)`.
- **Step line:** red `#e74c3c` width 3 — flat at $5K from rating 1 to 6.5, vertical jump to $85K drawn at 6.5 (between ratings 6 and 7), flat to rating 10.
- **Annotation (bold 11px red, right of the jump):** "threshold = 7" / "+$80K jump".
- **Axes:** x ticks 1-10, label "School Rating"; y −$20K to $120K with "$NK" labels at 5 positions; margins top 40 / right 20 / bottom 45 / left 60.

### Visualization (canvas `canvas5b`, 400×340)

Waterfall of incremental premium per rating point.

- **Title (bold 12px):** "Waterfall — Where Does the $80K Come From?"; subtitle 10px `#666`: "Incremental gain per rating point".
- **Bars:** average premium per integer rating bucket 1-10 (computed from the scatter data), drawn as a cumulative waterfall with dashed `#ccc` connectors; small gains fill `rgba(26,82,118,0.5)` stroke `#1a5276`; the big jump (|gain| > $15K, at rating 7) fill `rgba(231,76,60,0.75)` stroke `#c0392b` with bold 10px red "+$NK" label above.
- **Annotation:** red downward arrow at the jump column labeled bold 9px "THE CLIFF".
- **Axes:** x rating labels 1-10, label "School Rating"; y −$10K to $110K with "$NK" labels at 5 positions; margins top 40 / right 15 / bottom 50 / left 55.

## Regeneration instructions

- **Layout:** one `<table class="obj-table">` per section, each with a single `<tr>` of three `<td>`s — left (38%) holds `.pitfall-label` span + `<h3>` + paragraph + `<ul>`; middle (31%, centered) holds the primary 420×340 canvas; right (31%, centered) holds the insight 400×340 canvas.
- **Page style:** body system sans-serif, margin 20px, background `#f9f9f9`, text `#333`; h1 centered `#1a5276`; `.obj-table` full width, collapsed borders, cells `1px solid #2980b9` with 12px padding; h3 `#1a5276` 1.0em weight 700; paragraphs/bullets 14px, line-height 1.5-1.6; `.pitfall-label` inline-block bold 0.72em uppercase with 0.5px letter-spacing; `canvas { width: 100%; height: auto; }`.
- **Pitfall label colors:** assigned by a small script cycling `["#795548","#2980b9","#27ae60","#e74c3c","#8e44ad","#e67e22","#16a085","#d35400","#c0392b","#1abc9c"]` over all `.pitfall-label` elements in document order.
- **Data generation:** seeded RNG `mulberry32(42)` shared across all charts in document order; Box-Muller for normals; `randExp(lambda)` for exponentials.
- **Shared histogram utility:** `drawHistogram(canvasId, data, options)` — white background, bold 13px `#1a5276` title, gray `#999` L-axes, bars normalized to max count, optional per-bin `colors` array, optional `xFormat`/`xTicks`, y count labels at 5 positions; every utility histogram also gets a Gaussian-smoothed density line `#5d4037` width 2 with a 95% SE band `rgba(121,85,72,0.18)`, skipped when `density: false` is passed (canvas 4). Canvases 2 and 5 are hand-drawn instead and set their drawing size to 480×340 in JS (overriding the 420×340 HTML attribute).
- **Canvas scaling:** all canvases set `max-width` to the intrinsic width, size the backing store to the displayed width (`getBoundingClientRect().width`, falling back to the intrinsic width) × `window.devicePixelRatio`, and `ctx.scale` by that combined factor.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; brown density line `#5d4037`/`rgba(121,85,72,…)`, purple `rgba(142,68,173,…)`, yellow `#f1c40f`, slate `rgba(44,62,80,…)`.
- No nav bar, no back/home links. In regenerated HTML any card links would use `.html` extensions (this page has none).
