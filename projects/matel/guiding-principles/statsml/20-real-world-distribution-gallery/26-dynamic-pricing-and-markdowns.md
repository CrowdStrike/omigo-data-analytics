# Dynamic Pricing & Markdowns — Distribution Patterns

**Page type:** detail page (3-column obj-table layout: text left 38%, histogram canvas center 31%, insight canvas right 31%, one table per pattern)
**HTML title tag:** Dynamic Pricing &amp; Markdowns — Distribution Patterns

**Subtitle:** 5 distributions that expose the mechanics behind price manipulation, elasticity traps, and clearance cascades

## Pre-Discount Markup (Manufactured Anchor)

**Label:** MANUFACTURED ANCHOR (color `#795548`)

Days before a sale when the "original price" was set. Most anchors appear 3-7 days out, with a thin uniform tail 10-30 days out. One reading of the spike: the markup exists to create a reference price that makes the "discount" feel real — the sale price was the price a week earlier.

- Spike at 3-7 days before the sale (~60% of anchors)
- Uniform tail 10-30 days = longer-running anchors
- If the anchor is manufactured, the "discount" is theater

### Visualization (canvas `canvas1`, 420×340)

Histogram (shared `drawHistogram` helper, see Regeneration instructions).

- **Data:** mixture — 1500 draws from Normal(5, 1.5) kept in (0, 30), plus 500 draws uniform on [10, 30]; seeded RNG mulberry32(42).
- **Bins/range:** 30 bins, x from 0 to 30.
- **Title:** "Days Before Sale When \"Original Price\" Set". **X label:** "Days Before Sale". X tick format: integer + "d".
- **Colors:** bar fill `rgba(231,76,60,0.5)`, bar border `#e74c3c`.

### Visualization (canvas `canvas1b`, 400×340)

Price timeline diagram of the manufactured anchor.

- **Title (bold, `#e74c3c`, top center):** "The Manufactured Anchor — Timeline".
- **Timeline:** horizontal `#555` baseline (width 2) at ~45% height.
- **Real price segment:** flat blue `#2980b9` line (width 3) 40px above the baseline over the first ~60% of width, labeled bold 11px blue "Real price = $70".
- **Markup segment:** red `#e74c3c` line (width 3) jumping vertically up (with upward arrowhead) to 100px above the baseline and running flat from ~60% to ~78% of width; centered labels bold 12px red "MARKUP" and 10px "3-7 days before"; to the right in `#c0392b`: bold 11px '"Was $100"' and 9px "(manufactured)".
- **Sale segment:** green `#27ae60` line (width 3) dropping back to the real-price level (downward arrowhead) and running flat to the right edge; centered below in green bold 12px '"Now $70!"' and 9px `#555` "(the real price all along)"; bold 14px green "SALE!" near the drop.
- **Bottom annotation:** bold 11px red centered "The \"discount\" is theater", with a small red up-arrow pointing at the diagram.

## Price Elasticity (Bimodal Across SKUs)

**Label:** MOSTLY IMMOVABLE (color `#2980b9`)

Bimodal elasticity: a large inelastic bulk (elasticity ~0.15) where discounts give up margin with little volume gain, and a thinner hyper-elastic mode (elasticity ~2.5) where every 1% price cut yields ~2.5% volume. The average (~0.7) describes neither group.

- 75% of SKUs = inelastic (discounts barely move volume)
- 25% = hyper-elastic (discounts move volume a lot)
- "Average elasticity" lands in the empty valley between modes
- Blanket promos spend most of their budget on the inelastic 75%

### Visualization (canvas `canvas2`, 420×340)

Histogram (shared helper).

- **Data:** mixture — 1500 draws from Normal(0.15, 0.05) plus 500 draws from Normal(2.5, 0.8), positive values only.
- **Bins/range:** 35 bins, x from 0 to 4.
- **Title:** "Price Elasticity by SKU". **X label:** "Elasticity". X tick format: one decimal.
- **Colors:** bar fill `rgba(230,126,34,0.5)`, bar border `#e67e22`.

### Visualization (canvas `canvas2b`, 400×340)

Pie chart of inelastic vs hyper-elastic SKUs.

- **Title (bold, `#e67e22`, top center):** "Where Discounts Actually Move Volume".
- **Pie (radius 85, centered slightly left, starting at 12 o'clock):** 75% inelastic slice filled `rgba(41,128,185,0.7)` stroked `#2980b9`; 25% elastic slice filled `rgba(230,126,34,0.8)` stroked `#e67e22`.
- **On-pie labels (white):** bold 14px "75%" with 10px "INELASTIC" in the blue slice; bold 12px "25%" in the orange slice.
- **Arrow:** orange `#e67e22` arrow (width 2) from an annotation at lower right to the 25% slice; annotation bold 11px orange: "Discounts move" / "volume here".
- **Legend (bottom-left, 10px `#333`):** blue swatch "Discounts barely move volume"; orange swatch "Discounts move volume a lot".

## Days to Markdown (Weibull Trigger)

**Label:** SYNCHRONIZED CLEARANCE (color `#27ae60`)

Automated markdown triggers cluster tightly: half of all first markdowns land between day 15 and 23 (Weibull shape, median ~19). If many retailers run similar algorithms with similar parameters, clearance waves would synchronize — stores marking down the same week, flooding the market together.

- Weibull(k=3.5, lambda=21) — half of markdowns in days 15-23
- Similar algorithms + similar parameters = similar timing
- Synchronized markdowns compete with each other
- One implication: moving a few days early avoids the wave

### Visualization (canvas `canvas3`, 420×340)

Histogram (shared helper).

- **Data:** 2000 draws from Weibull(k=3.5, lambda=21) via inverse CDF `21·(-ln(1-u))^(1/3.5)`, values > 40 discarded.
- **Bins/range:** 30 bins, x from 0 to 40.
- **Title:** "Days on Shelf Before First Markdown". **X label:** "Days". X tick format: integer + "d".
- **Colors:** bar fill `rgba(41,128,185,0.5)`, bar border `#2980b9`.

### Visualization (canvas `canvas3b`, 400×340)

Step survival curve with markdown trigger lines.

- **Title (bold, `#2980b9`, top center):** "Algorithmic Markdown Triggers — Survival Curve".
- **Curve:** step function over days 0-40 — flat between triggers, vertical drop at each markdown day: 100% until day 14, 85% until day 21, 50% until day 28, 15% after. Line `#2980b9` width 3, area under the steps filled `rgba(41,128,185,0.12)`.
- **Trigger markers (dashed red `#e74c3c` full-height vertical lines, dash 4/3, width 2, each with a red down-arrow beside the drop spanning its actual height):**
  - Day 14 — label "-20%" (drops to 85% still at full price)
  - Day 21 — label "-40%" (drops to 50%)
  - Day 28 — label "-60%" (drops to 15%)
- Drop labels bold 12px red below the axis, with 9px `#555` "Day 14" / "Day 21" / "Day 28" beneath.
- **Annotation (upper right):** bold 10px `#c0392b` "Similar triggers create" / "synchronized waves"; 9px `#555` "Many stores mark down the same week".
- **Axes:** gray `#999`; y ticks "100%", "0%"; label "% Still at Full Price" at top-left.

## Competitor Price Gap (Fat-Tailed Normal)

**Label:** PRICE WAR SIGNAL (color `#e74c3c`)

Most items sit at price parity (normal center at 0), but fat tails from Cauchy contamination reveal extreme gaps. Left tail = we're cheaper (margin leak). Right tail = they're cheaper (losing customers). The tails are where all strategic action lives.

- Center "parity zone" = boring, no action needed
- Left tail (we're cheaper) = unnecessary margin sacrifice
- Right tail (they're cheaper) = customer defection risk
- Fat tails mean outliers are far more common than normal assumes

### Visualization (canvas `canvas4`, 420×340)

Histogram (shared helper).

- **Data:** 1900 draws from Normal(0, 2) plus 100 Cauchy draws `tan(pi·(u - 0.5))` kept in (-15, 15) (5% fat-tail contamination).
- **Bins/range:** 40 bins, x from -15 to 15.
- **Title:** "Competitor Price Gap (% Difference)". **X label:** "% Price Difference (Negative = We're Cheaper)". X tick format: integer + "%".
- **Colors:** bar fill `rgba(142,68,173,0.5)`, bar border `#8e44ad`.

### Visualization (canvas `canvas4b`, 400×340)

Annotated fat-tailed density with strategy zones.

- **Title (bold, `#8e44ad`, top center):** "Price Gap Zones — Where Strategy Lives".
- **Density:** mixture curve `Normal(0,2) PDF + 0.05·Cauchy(0,1) PDF` over x in [-15, 15] (step 0.3), normalized to 85% of plot height — line `#8e44ad` width 2.5.
- **Zone fills under the curve:** left tail (x < -5) `rgba(231,76,60,0.3)`; center (-5 to 5) `rgba(150,150,150,0.25)`; right tail (x > 5) `rgba(41,128,185,0.3)`.
- **Left tail labels (centered at x≈-10, `#e74c3c`):** bold 11px "MARGIN" / "LEAK", 9px "We're cheaper"; below, a red down-arrow and 9px "RAISE PRICES".
- **Center labels (top center):** bold 11px `#888` "PARITY ZONE"; 9px `#999` "(boring — no action)".
- **Right tail labels (centered at x≈10, `#2980b9`):** bold 11px "LOSING" / "CUSTOMERS", 9px "They're cheaper"; below, a blue down-arrow and 9px "MATCH or DIFFERENTIATE".
- **X axis:** gray `#999` baseline; ticks "-15%", "0%", "+15%"; title "Price gap (our price - their price)".

## Coupons (Zero-Inflated + Spike)

**Label:** DEADLINE EFFECT (color `#8e44ad`)

In this simulated program, 85% of coupons are never redeemed, and of the 15% that are, 60% are redeemed on the last day. Days 1-29 are nearly flat at near-zero — consistent with the deadline, not the discount, driving the timing. The distribution reads like a revenue model, not a usage pattern.

- 85% zeros = breakage (never redeemed)
- 60% of redemptions = last day only
- Days 1-29 are nearly flat at near-zero
- One reading: the deadline is doing most of the work

### Visualization (canvas `canvas5`, 420×340)

Histogram (shared helper).

- **Data:** 3000 coupons — 2550 zeros (day 0 = never redeemed); of 450 redeemed, 270 (60%) at day 30 and 180 spread uniform on [1, 29].
- **Bins/range:** 35 bins, x from 0 to 31.
- **Title:** "Coupon Redemption Day (0 = Never)". **X label:** "Day of Redemption". X tick format: integer.
- **Colors:** bar fill `rgba(39,174,96,0.5)`, bar border `#27ae60`.
- **Density overlay disabled** (`density: false`) — a smoothed line would bridge the empty days between the zero pile and the deadline spike.
- **Clipping:** y scale capped at count 300 (`clipCount: 300`) so the day-30 deadline spike is readable; the day-0 bar (count 2550) is clipped with white break marks near its top and labeled bold 10px `#333` "85% never redeem (bar clipped)".

### Visualization (canvas `canvas5b`, 400×340)

Redemption timeline bar chart with breakage zone.

- **Title (bold, `#27ae60`, top center):** "Redemption Timeline — The Deadline Effect".
- **Bars (days 1-30, width w/32, scale max 270):** days 1-29 at count ~6 each, fill `rgba(39,174,96,0.35)`; day 30 at count 270, fill `rgba(39,174,96,0.85)`.
- **Breakage zone:** bottom 60% of plot shaded `rgba(231,76,60,0.08)`, topped by a dashed red `#e74c3c` horizontal line (dash 4/3, width 1); centered labels bold 12px red "85% NEVER USED" and 10px "(breakage = pure profit)".
- **Deadline spike annotation (green `#27ae60`, placed left of the day-30 bar, right-aligned, with a down-right arrow to the bar top):** bold 11px "DEADLINE" / "SPIKE"; 9px `#555` "60% of all" / "redemptions".
- **Boxed callout:** rectangle stroked `#c0392b` width 2 (155×30) with bold 10px red text "Breakage revenue" / "= pure profit".
- **Bottom annotation (bold 11px `#c0392b`, centered):** "Deadline drives the timing".
- **X axis:** gray `#999` baseline; ticks "Day 1", "Day 15", "Day 30".

## Regeneration instructions

- **Layout:** one `<table class="obj-table">` per pattern, single `<tr>` with three `<td>`: left 38% text (`.pitfall-label` span + `<h3>` + `<p>` + `<ul>`), center 31% (histogram canvas 420×340), right 31% (insight canvas 400×340), both canvas cells centered. Table cell borders `1px solid #2980b9`, padding 12px, `border-collapse: collapse`.
- **Page CSS:** body system sans-serif (-apple-system stack), margin 20px, background `#f9f9f9`, text `#333`; h1 `#1a5276` centered; `.subtitle` centered `#666` 0.95em; h3 `#1a5276` 1.0em weight 700; p/li 14px, line-height 1.5-1.6; `.pitfall-label` inline-block bold 0.72em uppercase, letter-spacing 0.5px; `canvas { width: 100%; height: auto; }`.
- **Pitfall label colors:** assigned by a trailing script from the cyclic palette `["#795548","#2980b9","#27ae60","#e74c3c","#8e44ad","#e67e22","#16a085","#d35400","#c0392b","#1abc9c"]` in document order (labels 1-5 use the first five).
- **Shared histogram helper (`drawHistogram`):** white plot background; bold 13px `#1a5276` centered title at y=18; gray `#999` L-axes; margins top 35 / right 20 / bottom 40 / left 50; bars normalized to max bin count; overlaid Gaussian-smoothed density line in `#1a5276` (width 2, sigma 1.5 bins) with a 95% SE band filled `rgba(230,126,34,0.22)` (effective N clamped to [30, 200]), skipped when `density: false` is passed; optional `clipCount` caps the y scale and marks taller bars with white break lines plus an optional `clipLabel`; 6 x-tick labels in `#555` 11px; optional x-axis label in `#333` 12px. Data simulated with seeded RNG mulberry32(42) and a Box-Muller `randNormal(mean, std)` helper shared across all charts on the page.
- **Canvas scaling:** all canvases declare intrinsic width/height attributes and scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; secondary `#2980b9`, `#8e44ad`, `#c0392b`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions (this page has none).
