# Grocery & Market Basket — Distribution Patterns

**Page type:** detail page (three-column obj-table layout: text left 38%, main histogram canvas center 31%, insight canvas right 31%, one table per section)
**HTML title tag:** Grocery & Market Basket — Distribution Patterns

**Subtitle:** Distributions that reveal how shopping missions, habits, and product lifecycles shape every grocery decision

## Basket Size (Quick-Trip vs Stock-Up)

**Label:** BIMODAL MISSIONS (color `#795548`)

Basket sizes are bimodal: quick-trip shoppers grab 1-5 items, stock-up shoppers fill carts with 20-40. The empty "dead zone" at 5-15 items is the tell — the shape points to two distinct shopping missions, not one continuous behavior.

- Quick-trip: 1-5 items (milk, bread, forgot something)
- Stock-up: 20-40 items (weekly haul)
- Dead zone at 5-15 items — nobody shops here
- "Average basket ≈ 13 items" describes nobody

### Visualization (canvas `canvas1`, 420×340)

Bimodal basket-size histogram.

- **Data:** seeded RNG mulberry32(42) shared across the page; 1500 quick-trip draws normal(2.5, 0.8) clamped to [1, 5] + 1000 stock-up draws normal(28, 6) clamped to [15, 50].
- **Bins/axes:** 40 bins over x 0–50; integer x tick labels; margins top 35 / right 20 / bottom 40 / left 50; axes `#999`; white background.
- **Title (bold 13px, `#1a5276`):** "Basket Size (Number of Items)". **X label:** "Items in Basket".
- **Bars:** fill `rgba(39,174,96,0.5)`, stroke `#27ae60`.
- **Density overlay:** Gaussian-kernel smoothed counts (sigma 1.5 bins), line `#1a5276` width 2 with SE band `rgba(230,126,34,0.22)` (1.96·smoothed/√effN, effN clamped 30–200).

### Visualization (canvas `canvas1b`, 400×340)

Two-population scatter with a dead-zone band.

- **Title (bold 13px, `#27ae60`):** "Two Populations — The Dead Zone".
- **Scales:** x items 0–50, y spend $0–60; margins top 40 / right 20 / bottom 45 / left 50; L-frame axes `#999`.
- **Dead zone:** band from x=5 to x=15 filled `rgba(150,150,150,0.15)` with dashed `#999` borders; bold 12px `#777` centered labels "DEAD" / "ZONE" and bold 10px `#555` "Nobody shops" / "for 8 items" below.
- **Points:** 120 quick-trip dots `rgba(41,128,185,0.6)` (items uniform 1–5, spend = items·(2 + 3·rng())); 100 stock-up dots `rgba(230,126,34,0.6)` (items uniform 20–40, spend = items·(1.5 + 1.5·rng())); radius 3.
- **Corner labels:** bold 11px blue `#2980b9` "QUICK TRIP" with 9px "1-5 items" top-left; bold 11px orange `#e67e22` "STOCK-UP" with 9px "20-40 items" top-right.
- **X ticks:** 0, 10, 25, 40, 50 with title "Items".

## Co-Purchase Lift (Long Tail of Associations)

**Label:** POWER LAW LIFT (color `#2980b9`)

Association rule lift follows a power law: the median pair sits at lift ≈1.0 (no association) and ~94% fall below 2x, but a rare tail reaches 5-8x. Those few strong pairs carry the cross-promotion signal — the fat tail is where the value hides.

- Most pairs: lift ≈1.0 (median 1.0, no real association)
- ~6% exceed 2x; only ~0.5% exceed 5x
- The mean (≈1.2) says nothing about the tail that matters
- Cross-promo value concentrates in the extreme tail

### Visualization (canvas `canvas2`, 420×340)

Power-law lift histogram.

- **Data:** 5000 draws from Pareto(alpha=3, xmin=0.8): val = 0.8·(1−rng())^(−1/3).
- **Bins/axes:** 50 bins over x 0.5–8; x ticks formatted "0.5x"…"8.0x".
- **Title:** "Co-Purchase Lift Distribution". **X label:** "Lift Value".
- **Bars:** fill `rgba(41,128,185,0.5)`, stroke `#2980b9`; same density overlay as canvas1.

### Visualization (canvas `canvas2b`, 400×340)

Horizontal bar chart of the top pairs.

- **Title (bold 13px, `#2980b9`):** "Top 5 Co-Purchase Pairs by Lift".
- **Pairs (name, lift, color):** "bread + butter" 7.2x `#e74c3c`; "chips + salsa" 6.1x `#e67e22`; "pasta + sauce" 5.8x `#f39c12`; "beer + snacks" 5.3x `#27ae60`; "coffee + cream" 4.9x `#2980b9`. Bar length scaled to max 8; alpha 0.7 fill with 1.5px matching stroke; pair names right-aligned 12px `#333` left of the bars; bold white "7.2x" etc. inside each bar; margins top 40 / right 30 / bottom 50 / left 120.
- **Annotation (bold 10px `#c0392b`, centered below bars, with small up-arrow):** "~0.5% of pairs exceed 5x —" / "the tail carries the signal".

## Inter-Visit Interval (Weekly Habit Cycle)

**Label:** HABIT SIGNATURE (color `#27ae60`)

Days between grocery visits spike at 7 and 14 days, with a fainter echo at 21 and valleys between — consistent with ingrained weekly routines rather than memoryless shopping. A memoryless (exponential) model would fit this shape terribly.

- Primary spike at 7 days (weekly shoppers)
- Secondary spike at 14 days, faint echo at 21
- Exponential decay in the tail (lapsed shoppers)
- Coupon timing could align to these cycles

### Visualization (canvas `canvas3`, 420×340)

Multi-spike inter-visit histogram.

- **Data (mixture, values kept if 0 < v < 30):** 1200 draws normal(7, 1.2); 600 draws 14 + 0.3·exponential(λ=0.1); 200 draws normal(14, 2); 200 draws normal(21, 1.0) (faint three-week echo).
- **Bins/axes:** 35 bins over x 0–30; integer x tick labels.
- **Title:** "Days Between Grocery Visits". **X label:** "Days".
- **Bars:** fill `rgba(230,126,34,0.5)`, stroke `#e67e22`; same density overlay.

### Visualization (canvas `canvas3b`, 400×340)

Timeline with periodic habit spikes.

- **Title (bold 13px, `#e67e22`):** "Weekly Habit Cycle — Periodicity".
- **Timeline:** horizontal `#999` baseline at 65% plot height; 10px `#555` markers "Day 0", "Day 7", "Day 14", "Day 21", "Day 28"; margins top 40 / right 20 / bottom 40 / left 45.
- **Spikes:** 16px-wide bars at days 7, 14, 21 with heights 50%, 35%, 25% of plot height, fill `rgba(230,126,34,0.7)`, stroke `#e67e22` 2px; bold 11px red `#e74c3c` "HABIT" label above each.
- **Overlay:** dashed blue `#2980b9` sine wave with 7-day period (amplitude 20% of plot height) riding above the baseline, labeled "7-day periodicity" 10px right-aligned near the top.
- **X axis label:** "Days Since Last Visit" (11px `#333`, centered).

## Price Elasticity by Category

**Label:** STAPLE VS DISCRETIONARY (color `#e74c3c`)

Price elasticities are bimodal: staples (milk, eggs, bread) cluster near 0.2 (inelastic — people buy regardless), while discretionary items (wine, snacks) cluster near 2.2 (elastic — discount drives volume). One pricing strategy cannot serve both.

- Staples: elasticity 0.1-0.3 (can't move them)
- Discretionary: elasticity 1.5-3.0 (discount gold)
- Gap at 0.5-1.0 — few items live here
- Discounting milk wastes margin; discounting wine drives trips

### Visualization (canvas `canvas4`, 420×340)

Bimodal elasticity histogram.

- **Data:** 1000 staple draws normal(0.2, 0.08) kept if > 0 + 800 discretionary draws normal(2.2, 0.6) kept if in (0, 4).
- **Bins/axes:** 35 bins over x 0–4; one-decimal x tick labels.
- **Title:** "Price Elasticity by Category". **X label:** "Elasticity".
- **Bars:** fill `rgba(142,68,173,0.5)`, stroke `#8e44ad`; same density overlay.

### Visualization (canvas `canvas4b`, 400×340)

Lollipop chart split around e = 1.0.

- **Title (bold 13px, `#8e44ad`):** "Staples vs Discretionary — Elasticity".
- **Center line:** vertical `#333` 2px line at 40% of plot width labeled bold "e = 1.0" below; headers bold 11px: "CAN'T MOVE" in `#2980b9` left of the line, "CAN DISCOUNT" in `#e74c3c` right of the line; margins top 45 / right 25 / bottom 35 / left 25.
- **Staples (left side, blue `#2980b9`, rows 1-3):** Milk 0.15, Eggs 0.18, Bread 0.22 — 2px stem from the center line to a 6px dot, label "Milk 0.15" etc. right-aligned beyond the dot.
- **Discretionary (right side, red `#e74c3c`, rows 5-7):** Wine 2.8, Snacks 2.1, Cheese 1.9 — stems rightward to 6px dots, labels "Wine 2.8" etc. left-aligned beyond the dot. Values scaled against max elasticity 3.0.

## Fresh Produce Waste (Shelf-Life Decay)

**Label:** WEIBULL DECAY (color `#8e44ad`)

Days-to-waste follows a Weibull distribution (k=2.5): initially low failure rate that accelerates after day 3. The shape parameter can drive markdown policy — the markdown schedule for perishables can be read straight off the survival curve.

- Day 0-3: full price (low spoilage risk)
- Day 3-5: markdown zone (accelerating decay)
- Day 5+: waste zone (unsellable)
- Policy = distribution shape, not guesswork

### Visualization (canvas `canvas5`, 420×340)

Weibull days-to-spoilage histogram.

- **Data:** 2000 draws from Weibull(k=2.5, λ=4) via inverse transform: val = 4·(−ln(1−rng()))^(1/2.5).
- **Bins/axes:** 30 bins over x 0–12; integer x tick labels.
- **Title:** "Days to Spoilage (Fresh Produce)". **X label:** "Days".
- **Bars:** fill `rgba(231,76,60,0.5)`, stroke `#e74c3c`; same density overlay.

### Visualization (canvas `canvas5b`, 400×340)

Weibull survival curve with policy zones.

- **Title (bold 13px, `#e74c3c`):** "Survival Curve — P(still sellable) vs Days".
- **Curve:** S(t) = exp(−(t/4)^2.5) over days 0–10, line `#1a5276` width 3 with area under filled `rgba(26,82,118,0.1)`.
- **Policy zones:** day 0–3 shaded `rgba(39,174,96,0.15)`; day 3–5 `rgba(241,196,15,0.15)`; day 5+ `rgba(231,76,60,0.12)`; dashed `#555` vertical lines at day 3 and day 5; bold 10px zone labels at top: "Full Price" `#27ae60`, "Markdown" `#f39c12`, "Waste" `#e74c3c`.
- **Annotation:** bold 11px `#c0392b` right-aligned "Policy = distribution shape" with a red arrow pointing to the curve knee near day 3.5.
- **Axes:** L-frame `#999`; y labels "100%", "50%", "0%"; x ticks "0d", "2d", … "10d" with title "Days on Shelf"; margins top 40 / right 20 / bottom 45 / left 50.

## Regeneration instructions

- **Layout:** h1 + `.subtitle` paragraph, then one `.obj-table` (full-width, `border-collapse: collapse`) per pitfall, single `<tr>` with three `<td>`s: text 38%, main canvas 31% centered, insight canvas 31% centered. Cell borders `1px solid #2980b9`, padding 12px. Each text cell: `.pitfall-label` span, `<h3>`, one `<p>`, one 4-item `<ul>`.
- **Page style:** body system sans-serif, margin 20px, background `#f9f9f9`, text `#333`; h1 centered `#1a5276`; `.subtitle` centered `#666` 0.95em; `.obj-table h3` 1.0em weight 700 `#1a5276`; p/li 14px, line-height 1.5–1.6; `.pitfall-label` inline-block bold 0.72em uppercase, letter-spacing 0.5px. `canvas { width: 100%; height: auto; }`. No nav bar, no back/home links.
- **Label colors:** assigned by section index from the palette `["#795548","#2980b9","#27ae60","#e74c3c","#8e44ad","#e67e22","#16a085","#d35400","#c0392b","#1abc9c"]` via a small script.
- **Canvases:** intrinsic sizes 420×340 (main) and 400×340 (insight); backing store scaled by `window.devicePixelRatio`, CSS size fixed, `ctx.scale` back to logical coordinates. Data generated with seeded mulberry32(42) RNG shared sequentially across all charts plus a Box-Muller `randNormal(mean, std)` helper; Pareto and Weibull draws via inverse transform.
- **Shared histogram helper:** margins {top 35, right 20, bottom 40, left 50}, white background, bold 13px `#1a5276` title, `#999` axes, `#555` tick text; smoothed density line `#1a5276` width 2 with SE band `rgba(230,126,34,0.22)`; optional `xFormat` tick formatter and `min`/`max` range.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, secondary blue `#2980b9`, purple `#8e44ad`, dark red `#c0392b`, yellow accents `#f1c40f`/`#f39c12`.
