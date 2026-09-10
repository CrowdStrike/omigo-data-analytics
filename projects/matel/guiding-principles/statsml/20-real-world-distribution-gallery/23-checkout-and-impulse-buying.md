# Checkout & Impulse Buying — Distribution Patterns

**Page type:** detail page (3-column obj-table layout: text left 38%, histogram canvas center 31%, insight canvas right 31%, one table per pattern)
**HTML title tag:** Checkout & Impulse Buying — Distribution Patterns

**Subtitle:** 5 distribution shapes in queue patience, impulse grabs, and checkout behavior

## Items Added in Queue (Geometric Decay)

**Label:** GEOMETRIC DECAY (color `#795548`)

Most shoppers add zero impulse items while waiting in the checkout queue. In this simulation each next item is added with only 30% probability — a geometric decay that looks like a chain of "one more?" decisions with a constant stop rate.

- P(0 items) = 70% — most resist entirely
- P(1 item) = 21% — one grab is common
- P(2+ items) = 9% — rare multi-grabbers
- Consistent with independent add decisions at a constant rate

### Visualization (canvas `canvas1`, 420×340)

Histogram (shared `drawHistogram` helper, see Regeneration instructions).

- **Data:** 3000 geometric draws — count of successive `rng() < 0.3` successes (capped at 20); seeded RNG mulberry32(42).
- **Bins/range:** 8 bins, x from 0 to 8 — integer-aligned (bin width 1) so each integer count gets its own bin; no false gaps.
- **Density overlay:** suppressed (`density: false`) — the smoothed density + SE band is misleading on discrete/staircase counts.
- **Title:** "Items Added While in Queue". **X label:** "Number of Items". X tick format: integer.
- **Colors:** bar fill `rgba(142,68,173,0.5)`, bar border `#8e44ad`.

### Visualization (canvas `canvas1b`, 400×340)

Waterfall bar chart of survival probabilities with multiplier arrows.

- **Title (bold, `#8e44ad`, top center):** "Geometric Decay — Each Extra Item ×0.3".
- **Bars (40px wide, scale max 35%, alpha 0.8 fill with solid stroke width 2):**
  - "≥1 item" — 30% — `#8e44ad`
  - "≥2 items" — 9% — `#a569bd`
  - "≥3 items" — 3% — `#c39bd3`
  - "≥4 items" — 1% — `#d7bde2`
- Percentage labels ("30%" etc.) bold 13px `#333` above each bar; item labels 11px `#555` below the axis.
- **Multiplier arrows:** red `#e74c3c` horizontal arrows (width 2) between consecutive bars, each labeled "×0.3" in bold 10px red above the arrow.
- **Conclusion (bold 12px `#8e44ad`, bottom center):** "Each next grab is only 30% as likely".
- **Axes:** gray `#999` L-axes; y-axis label "P(%)" in 10px `#555` at top-left.

## Impulse Item Price (Concentrated < $5)

**Label:** PAIN THRESHOLD (color `#2980b9`)

Simulated impulse prices cluster hard below $5 — about 90% of items. One explanation: small prices skip deliberation entirely, while $10+ purchases turn deliberate rather than impulsive. Read this way, the price distribution sketches a pain-threshold map.

- $0-5: ~90% of impulse items
- $5-10: thin hesitation zone (~9%)
- $10+: ~1% — rare
- One interpretation: ~$5 is where impulse becomes decision

### Visualization (canvas `canvas2`, 420×340)

Histogram (shared helper).

- **Data:** 2000 draws from shifted Exponential — `-ln(1-u)/0.5 + 0.5` (lambda=0.5, shift $0.50), values > 15 discarded.
- **Bins/range:** 30 bins, x from 0 to 15.
- **Title:** "Impulse Item Price Distribution". **X label:** "Price ($)". X tick format: "$" + integer.
- **Colors:** bar fill `rgba(230,126,34,0.5)`, bar border `#e67e22`.

### Visualization (canvas `canvas2b`, 400×340)

Grab-probability-vs-price chart computed from the generation model (mirrors the canvas5b decay-curve-with-annotations approach).

- **Title (bold, `#e67e22`, top center):** "Grab Probability vs Price — Band Masses".
- **Model:** density `f(p) = lambda·exp(-lambda·(p-shift))` with lambda=0.5, shift=$0.50 (the same parameters that generate the histogram data), drawn for p in [0, 15] — curve `#e67e22` width 3, y-scale max `1.1·lambda`.
- **Bands (area under the curve filled at alpha 0.22):** $0-5 green `#27ae60` "auto-grab", $5-10 orange `#e67e22` "hesitation", $10+ red `#e74c3c` "deliberate" (last band takes the full tail).
- **Band mass labels:** computed from the model as `S(a) − S(b)` with `S(p) = exp(-lambda·(p-shift))` — NOT hard-coded (seeded run renders 89% / 10% / 1%). Bold 14px percentage in the band color, with 10px `#555` band name and price range beneath; the $0-5 label sits above the curve at the band midpoint, the others near the bottom of the plot.
- **$5 boundary:** dashed red `#e74c3c` vertical line (dash 4/3, width 2) at p=$5.
- **Conclusion (bold 11px `#e74c3c`, bottom center, computed):** "~" + round($0-5 mass·100) + "% of impulse mass sits below $5".
- **Axes:** gray `#999` L-axes; x ticks "$0"/"$5"/"$10"/"$15"; label "Grab density" in 10px `#555` at top-left. Margins top 40 / right 20 / bottom 55 / left 55.

## Queue Abandon Time (Exponential + Cliff)

**Label:** PATIENCE CLIFF (color `#27ae60`)

Simulated abandonment decays exponentially — memoryless, constant hazard rate — until minute 4, where a wall appears: nearly everyone still in line quits at once. Smooth, then catastrophic.

- Exponential decay (lambda=0.3) for 0-4 min
- ~30% still waiting at 4 min — then the cliff
- Memoryless property holds until the cliff
- Only ~3% of customers outlast the cliff

### Visualization (canvas `canvas3`, 420×340)

Histogram (shared helper).

- **Data:** 2000 draws from Exponential(lambda=0.3); draws above 4 minutes are collapsed to the wall — 10% of them leak past as `4 + u·1.5`, the rest quit at `4 + u·0.15`.
- **Bins/range:** 30 bins, x from 0 to 6.
- **Title:** "Queue Abandon Time (Minutes)". **X label:** "Minutes Waiting". X tick format: one decimal + "m".
- **Colors:** bar fill `rgba(41,128,185,0.5)`, bar border `#2980b9`.

### Visualization (canvas `canvas3b`, 400×340)

Survival curve with a cliff.

- **Title (bold, `#2980b9`, top center):** "Survival Curve — P(still waiting) vs Minutes".
- **Curve:** `S(t) = exp(-0.3t)` for t ≤ 4, sharp drop over t in (4, 4.1] down to a floor of 0.03 through t=6 — line `#2980b9` width 3, area under filled `rgba(41,128,185,0.15)`.
- **Cliff marker:** dashed red `#e74c3c` vertical line (dash 4/3, width 2) at t=4; bold 12px red labels "ABANDON" / "CLIFF" to the right of the pre-cliff level; a thick red down-arrow (width 3) from the pre-cliff level to the floor.
- **Label (bold 11px `#8e44ad`, bottom center):** "Memoryless until catastrophic".
- **Axes:** gray `#999`; y ticks "100%", "50%", "0%"; x ticks "0", "2 min", "4 min", "6 min".

## Self-Checkout (Bimodal at ~12)

**Label:** NATURAL SEGMENTATION (color `#e74c3c`)

Simulated cart sizes are bimodal — self-checkout trips cluster around 5 items, staffed-lane trips around 22, with a valley near 12. One explanation: around a dozen items, self-scanning stops saving time.

- Self-checkout peak: mean=5, sd=2 (quick trips)
- Staffed lane peak: mean=22, sd=6 (full shops)
- Valley at ~12 items separates the two populations
- 60% of trips are self-checkout, 40% staffed

### Visualization (canvas `canvas4`, 420×340)

Histogram (shared helper).

- **Data:** mixture — 1200 draws from Normal(5, 2) plus 800 draws from Normal(22, 6), positive values only.
- **Bins/range:** 35 bins, x from 0 to 40.
- **Title:** "Cart Size Distribution (Items)". **X label:** "Number of Items". X tick format: integer.
- **Colors:** bar fill `rgba(231,76,60,0.5)`, bar border `#e74c3c`.

### Visualization (canvas `canvas4b`, 400×340)

Split-lane diagram.

- **Title (bold, `#e74c3c`, top center):** "Natural Segmentation at 12 Items".
- **Left box (self-checkout):** rectangle ~40% width, 60% height, fill `rgba(41,128,185,0.15)`, stroke `#2980b9` width 2. Text in `#2980b9`: bold 14px "SELF-CHECKOUT", 11px "≤ 12 items", bold 24px "60%"; in `#333` 11px: "of customers", "mean = 5 items".
- **Right box (staffed):** same geometry at ~55% x-offset, fill `rgba(230,126,34,0.15)`, stroke `#e67e22`. Text in `#e67e22`: bold 14px "STAFFED LANE", 11px "> 12 items", bold 24px "40%"; in `#333` 11px: "of customers", "mean = 22 items".
- **Divider:** dashed red `#e74c3c` vertical line (dash 5/4, width 3) between the boxes at ~48% width, with rotated bold 10px red label "NATURAL BOUNDARY".
- **Bottom annotation (bold 11px `#333`, centered):** "One explanation: past ~12 items, self-scanning stops saving time".

## Distance from Register (Exponential Decay)

**Label:** EVERY FOOT HALVES CONVERSION (color `#8e44ad`)

In this simulation the grab rate halves with every foot of distance from the register — 20% at the register down to ~1% at 4 feet. At that gradient, where an item sits matters about as much as what it costs.

- 1 foot: ~10% grab rate
- 2 feet: ~5% grab rate
- 3 feet: ~2.5% grab rate
- 4+ feet: ~1% — effectively invisible to impulse buyers

### Visualization (canvas `canvas5`, 420×340)

Histogram (shared helper).

- **Data:** 2000 draws from Exponential(rate=ln 2) — `-ln(1-u)/ln 2` (grab rate halves per foot), values > 8 discarded.
- **Bins/range:** 25 bins, x from 0 to 8.
- **Title:** "Distance from Register When Item Grabbed (ft)". **X label:** "Distance (feet)". X tick format: one decimal + "ft".
- **Colors:** bar fill `rgba(39,174,96,0.5)`, bar border `#27ae60`.

### Visualization (canvas `canvas5b`, 400×340)

Exponential decay line chart of conversion vs distance.

- **Title (bold, `#27ae60`, top center):** "Conversion Rate vs Distance from Register".
- **Curve:** `conv(d) = 20·exp(-ln2·d)` for d in [0, 6] ft (20% at the register, halving per foot; y-scale max 22%) — line `#27ae60` width 3, area under filled `rgba(39,174,96,0.15)`.
- **Annotated points (green `#27ae60` dots radius 5 with white rim, bold 11px `#333` labels to the right):** "1ft = 10% grab", "2ft = 5%", "3ft = 2.5%", "4ft = 1.25%".
- **Arrow:** red `#e74c3c` arrow (width 2) sloping from below the 1ft point to below the 2ft point.
- **Conclusion (bold 11px `#e74c3c`, bottom center):** "Every foot = 50% less likely to pick up".
- **Axes:** gray `#999`; y ticks "20%", "10%", "0%"; x ticks "0ft" through "6ft" per foot; label "Conversion %" in 10px `#555` at top-left.

## Regeneration instructions

- **Layout:** one `<table class="obj-table">` per pattern, single `<tr>` with three `<td>`: left 38% text (`.pitfall-label` span + `<h3>` + `<p>` + `<ul>`), center 31% (histogram canvas 420×340), right 31% (insight canvas 400×340), both canvas cells centered. Table cell borders `1px solid #2980b9`, padding 12px, `border-collapse: collapse`.
- **Page CSS:** body system sans-serif (-apple-system stack), margin 20px, background `#f9f9f9`, text `#333`; h1 `#1a5276` centered; `.subtitle` centered `#666` 0.95em; h3 `#1a5276` 1.0em weight 700; p/li 14px, line-height 1.5-1.6; `.pitfall-label` inline-block bold 0.72em uppercase, letter-spacing 0.5px; `canvas { width: 100%; height: auto; }`.
- **Pitfall label colors:** assigned by a trailing script from the cyclic palette `["#795548","#2980b9","#27ae60","#e74c3c","#8e44ad","#e67e22","#16a085","#d35400","#c0392b","#1abc9c"]` in document order (labels 1-5 use the first five).
- **Shared histogram helper (`drawHistogram`):** white plot background; bold 13px `#1a5276` centered title at y=18; gray `#999` L-axes; margins top 35 / right 20 / bottom 40 / left 50; bars normalized to max bin count; overlaid Gaussian-smoothed density line in `#1a5276` (width 2, sigma 1.5 bins) with a 95% SE band filled `rgba(230,126,34,0.22)` (effective N clamped to [30, 200]) — the density block is guarded by `opts.density !== false` so discrete/staircase histograms can pass `density: false` to suppress it (canvas1 does); 6 x-tick labels in `#555` 11px; optional x-axis label in `#333` 12px. Data simulated with seeded RNG mulberry32(42) and a Box-Muller `randNormal(mean, std)` helper shared across all charts on the page.
- **Canvas scaling:** all canvases declare intrinsic width/height attributes and scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; secondary `#2980b9`, `#8e44ad`, `#c0392b`, `#95a5a6`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions (this page has none).
