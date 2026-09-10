# Fashion Retail — Distribution Patterns

**Page type:** detail page (3-column obj-table layout: text left 38%, histogram canvas center 31%, insight canvas right 31%, one table per pattern)
**HTML title tag:** Fashion Retail — Distribution Patterns

**Subtitle:** 5 distributions that expose how pricing tiers, inventory curves, and trend lifecycles shape fashion revenue

## Markdown Staircase (Step Function)

**Label:** PRICE TIER UNLOCK (color `#795548`)

Simulated purchase prices cluster at discrete tiers: full price, 20% off, 40% off, 70% off. The staircase shape is consistent with each markdown reaching a new buyer segment — continuous pricing models miss these cliffs entirely.

- Full price ($100) = early adopters, brand-loyal
- 20% off ($80) = "deal" threshold reaches mainstream
- 40% off ($60) = largest population — value shoppers
- 70% off ($30) = clearance hunters

### Visualization (canvas `canvas1`, 420×340)

Histogram (shared `drawHistogram` helper, see Regeneration instructions).

- **Data:** four normal clusters — 500 draws from Normal(100, 2), 600 from Normal(80, 2), 700 from Normal(60, 2), 400 from Normal(30, 2); seeded RNG mulberry32(42).
- **Bins/range:** 40 bins, x from 10 to 110.
- **Title:** "Purchase Price Distribution ($)". **X label:** "Price ($)". X tick format: "$" + integer.
- **Colors:** bar fill `rgba(211,84,0,0.5)`, bar border `#d35400`.
- **Density overlay disabled** (`density: false`) — a smoothed line would blur the four discrete markdown-tier spikes the staircase story depends on.

### Visualization (canvas `canvas1b`, 400×340)

Descending staircase diagram of markdown tiers.

- **Title (bold, `#d35400`, top center):** "Markdown Staircase — Population Unlocks".
- **Steps (28px-tall rectangles descending left-to-right, each starting further right and narrower; fill at 25% alpha, stroke width 2 in tier color; bold 11px label on each step):**
  - "$100 (0% off)" — `#1a5276` — at 15% height
  - "$80 (20% off)" — `#2980b9` — at 38% height
  - "$60 (40% off)" — `#e67e22` — at 61% height
  - "$30 (70% off)" — `#27ae60` — at 84% height
- **Arrows between tiers:** red `#e74c3c` vertical arrows (width 2) to the right of the steps, each labeled in 9px red: "New population" / "unlocked".
- **Summary (centered, below):** bold 11px `#333` "Each step = new buyer segment"; 10px "Continuous pricing misses the cliffs".

## Size Curve Mismatch

**Label:** STOCKOUT SIGNAL (color `#2980b9`)

Demand peaks sharply at M/L but orders follow a flatter distribution. The mismatch creates simultaneous stockouts (M/L) and overstock (XS/XXL). The shape gap between demand and supply IS the lost revenue.

- Demand = normal centered at M/L (sizes 3-4)
- Orders = flatter, over-indexing on extremes
- M/L stockout = lost sales in highest-demand segment
- XS/XXL overstock = markdowns eating margin

### Visualization (canvas `canvas2`, 420×340)

Histogram (shared helper).

- **Data:** 1500 draws from Normal(3.5, 1.5) kept in [0.5, 6.5] (size index scale XS=1 … XXL=6).
- **Bins/range:** 6 bins, x from 0.5 to 6.5.
- **Title:** "Size Demand Distribution (XS=1 ... XXL=6)". **X label:** "Size Index". X tick format: nearest size label from `['XS','S','M','L','XL','XXL']`.
- **Colors:** bar fill `rgba(41,128,185,0.5)`, bar border `#2980b9`.

### Visualization (canvas `canvas2b`, 400×340)

Grouped bar chart: demand vs ordered per size.

- **Title (bold, `#2980b9`, top center):** "Demand vs Ordered — The Mismatch".
- **Data (y-scale max 35):** sizes `['XS','S','M','L','XL','XXL']`; demand `[8, 18, 32, 28, 12, 5]` (peaked at M/L); ordered `[16, 17, 18, 18, 17, 16]` (flatter).
- **Demand bars:** fill `rgba(41,128,185,0.7)`, stroke `#2980b9` width 1.5 (left of each pair). **Ordered bars:** fill `rgba(231,76,60,0.2)`, dashed stroke `#e74c3c` (dash 4/3, width 2) (right of each pair). Size labels bold 11px `#333` below.
- **Stockout markers:** at M and L, bold 10px red `#e74c3c` "STOCKOUT" at the top with a small red down-arrow.
- **Overstock markers:** at XS and XXL, bold 9px orange `#e67e22` "overstock" near the baseline.
- **Legend (below axis):** blue swatch "DEMAND"; dashed-red swatch "ORDERED" (labels 10px `#333`).
- **Callout (bold 10px `#c0392b`, right-aligned below):** "Lost revenue in the middle".
- **Axes:** gray `#999` L-axes.

## Bundle Attachment Rate (Power Law)

**Label:** GOLDEN BUNDLES (color `#27ae60`)

Simulated attach rates follow a power law — ~97% of product pairs sit under 2%, while the top pairs reach 7-12%. A handful of bundles tower over thousands of near-zero pairs, so the "average" attach rate describes none of them.

- Power law: ~97% of pairs < 2% attach rate
- Top 5 pairs reach ~7-12% — several times the rest
- Mean attach rate is skewed by the mass of near-zero pairs
- "Average bundle performance" describes no real bundle

### Visualization (canvas `canvas3`, 420×340)

Histogram (shared helper).

- **Data:** 3000 draws from Pareto(alpha=2.5, xmin=0.5) via inverse CDF `0.5 / (1-u)^(1/2.5)`.
- **Bins/range:** 40 bins, x from 0 to 15.
- **Title:** "Bundle Attach Rate (%)". **X label:** "Attach Rate (%)". X tick format: integer + "%".
- **Colors:** bar fill `rgba(142,68,173,0.5)`, bar border `#8e44ad`.

### Visualization (canvas `canvas3b`, 400×340)

Horizontal bar chart of top-5 bundles.

- **Title (bold, `#8e44ad`, top center):** "Top 5 Golden Bundles".
- **Bars (30px tall, 10px gap, scale max 15%, fill `rgba(142,68,173,0.6)`, stroke `#8e44ad` width 1.5; name labels bold 11px `#333` right-aligned left of bars; rate labels bold 12px `#8e44ad` right of bars):**
  - Shirt + Tie — 12.5%
  - Dress + Belt — 9.2%
  - Suit + Shirt — 8.3%
  - Jeans + Top — 7.3%
  - Coat + Scarf — 7.2%
- **Separator:** dotted gray `#999` horizontal line (dash 4/4) below the bars.
- **Below separator (11px `#666`, centered):** "...2,995 other pairs — 97% below 2%".
- **Arrow + insight:** purple `#8e44ad` right-pointing arrow (width 2), then bold 11px purple centered: "A few pairs dominate attach revenue".

## Return Rate by Price (U-Shaped)

**Label:** U-SHAPED RETURNS (color `#e74c3c`)

Returns are high at both ends of the price range — consistent with "gamble" purchases at the cheap end ("try it, return if bad") and buyer's remorse at the expensive end. The valley at $60-100 is where returns bottom out. Linear return models miss both tails.

- Left peak: cheap items = gamble-and-return behavior
- Right peak: expensive items = buyer's remorse
- $60-100 valley = lowest returns (commitment sweet spot)
- U-shape means "average return rate" hides two problems

### Visualization (canvas `canvas4`, 420×340)

Bar chart of observed return rate per price band (custom-drawn, not the shared histogram helper).

- **Data:** 5000 items with price uniform in [10, 200]; return rate per price `rr = 0.3/(1+exp(-(30-price)/10)) + 0.3/(1+exp(-(price-150)/15))` (first sigmoid falls after $30 as gamble returns fade, second rises after $150 as remorse returns grow; minimum near $80). Each item is sampled as returned with probability rr; items are binned into 19 price bands of $10 ($10-$200) and each bar shows returned/bought for its band — the empirical U that the smooth model curve in canvas4b predicts.
- **Y axis:** 0-30% with `#eee` gridlines and `#555` labels at 0/10/20/30%.
- **Title:** "Observed Return Rate by Price Band". **X label:** "Price ($)" with "$" + integer ticks at 6 positions ($10-$200).
- **Colors:** bar fill `rgba(231,76,60,0.5)`, bar border `#e74c3c`; layout matches the shared helper (white background, bold 13px `#1a5276` title, gray `#999` L-axes, margins top 35 / right 20 / bottom 40 / left 50).

### Visualization (canvas `canvas4b`, 400×340)

U-shape line chart of return rate vs price.

- **Title (bold, `#e74c3c`, top center):** "Return Rate vs Price — The U-Shape".
- **Curve:** the same double-sigmoid rr formula ×100, evaluated for price $10-200 step $2 (y-scale max 35%) — line `#e74c3c` width 3, area under filled `rgba(231,76,60,0.12)`.
- **Left peak annotation (at ~$30, in `#c0392b`):** bold 12px "GAMBLE", 9px "(cheap, try-and-return)", with a small down-arrow.
- **Right peak annotation (at ~$165, in `#8e44ad`):** bold 12px "REMORSE", 9px "(expensive, regret)", with a small down-arrow.
- **Valley annotation (at ~$80, in `#27ae60`):** bold 11px "SWEET SPOT", 9px "$60-100 lowest returns"; the $60-100 band shaded `rgba(39,174,96,0.12)` full height.
- **Axes:** gray `#999`; x ticks "$10", "$60", "$100", "$150", "$200"; x-axis title "Price ($)" in `#333`; y-axis label "Return %" at top-left.

## Trend Lifecycle (Weibull Decay)

**Label:** FAST FASHION HALF-LIFE (color `#8e44ad`)

The simulated trend follows Weibull(k=3, lambda=6) — accelerating adoption, a sharp peak around week 5, then aggressive decay. Half of lifetime sales land by week ~5, so inventory arriving after the peak risks becoming dead stock. The shape IS the markdown calendar.

- Launch phase (weeks 1-3): building momentum
- Peak phase (weeks 4-7): maximum sell-through
- Decay phase (week 8+): aggressive drop-off
- Half of sales by week ~5 — reorder cycles need to beat this

### Visualization (canvas `canvas5`, 420×340)

Histogram (shared helper).

- **Data:** 2000 draws from Weibull(k=3, lambda=6) via inverse CDF `6·(-ln(1-u))^(1/3)`.
- **Bins/range:** 30 bins, x from 0 to 14.
- **Title:** "Trend Sales by Week (Weibull Lifecycle)". **X label:** "Week". X tick format: "W" + integer.
- **Colors:** bar fill `rgba(39,174,96,0.5)`, bar border `#27ae60`.

### Visualization (canvas `canvas5b`, 400×340)

Cumulative sell-through (Weibull CDF) curve with phase bands.

- **Title (bold, `#27ae60`, top center):** "Cumulative Sell-Through — Half Gone by Week ~5".
- **Curve:** Weibull(k=3, lambda=6) CDF `F(t) = 1 - exp(-(t/6)^3)` over weeks 0-14 (step 0.1), scaled to 90% of plot height — line `#1a5276` width 3, area under filled `rgba(26,82,118,0.12)`. Complements the canvas5 histogram (weekly sales) with the cumulative view instead of duplicating its shape.
- **Phase bands (full-height background rectangles with bold 11px labels near the bottom):**
  - LAUNCH: weeks 0-3.5, fill `rgba(39,174,96,0.15)`, label `#27ae60`
  - PEAK: weeks 3.5-7, fill `rgba(241,196,15,0.15)`, label `#d4ac0d`
  - DECAY: weeks 7-14, fill `rgba(231,76,60,0.1)`, label `#e74c3c`
- **Half-life crosshair:** dashed red `#e74c3c` lines (dash 5/4, width 2) — vertical from the axis up to the curve at week `6·(ln 2)^(1/3) ≈ 5.3` (median of Weibull(3, 6)) and horizontal from the y-axis to the same point at the 50% level — labeled bold 10px red "50% SOLD" / "Week ~5".
- **Y axis labels:** "0%", "50%", "100%" in `#555` 10px, right-aligned at the axis.
- **Summary arrow:** blue `#1a5276` right-pointing arrow (width 2) below the axis, then bold 10px centered: "Half the sales are gone by week ~5".
- **Axes:** gray `#999`; x ticks "W0" through "W14" every 2 weeks.

## Regeneration instructions

- **Layout:** one `<table class="obj-table">` per pattern, single `<tr>` with three `<td>`: left 38% text (`.pitfall-label` span + `<h3>` + `<p>` + `<ul>`), center 31% (histogram canvas 420×340), right 31% (insight canvas 400×340), both canvas cells centered. Table cell borders `1px solid #2980b9`, padding 12px, `border-collapse: collapse`.
- **Page CSS:** body system sans-serif (-apple-system stack), margin 20px, background `#f9f9f9`, text `#333`; h1 `#1a5276` centered; `.subtitle` centered `#666` 0.95em; h3 `#1a5276` 1.0em weight 700; p/li 14px, line-height 1.5-1.6; `.pitfall-label` inline-block bold 0.72em uppercase, letter-spacing 0.5px; `canvas { width: 100%; height: auto; }`.
- **Pitfall label colors:** assigned by a trailing script from the cyclic palette `["#795548","#2980b9","#27ae60","#e74c3c","#8e44ad","#e67e22","#16a085","#d35400","#c0392b","#1abc9c"]` in document order (labels 1-5 use the first five).
- **Shared histogram helper (`drawHistogram`):** white plot background; bold 13px `#1a5276` centered title at y=18; gray `#999` L-axes; margins top 35 / right 20 / bottom 40 / left 50; bars normalized to max bin count; overlaid Gaussian-smoothed density line in `#1a5276` (width 2, sigma 1.5 bins) with a 95% SE band filled `rgba(230,126,34,0.22)` (effective N clamped to [30, 200]), skipped when `density: false` is passed (canvas 1); 6 x-tick labels in `#555` 11px; optional x-axis label in `#333` 12px. Data simulated with seeded RNG mulberry32(42) and a Box-Muller `randNormal(mean, std)` helper shared across all charts on the page.
- **Canvas scaling:** all canvases declare intrinsic width/height attributes and scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; secondary `#2980b9`, `#8e44ad`, `#c0392b`, `#d35400`, `#f39c12`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions (this page has none).
