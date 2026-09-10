# E-Commerce & Marketplace — How Your Data Actually Looks

**Page type:** detail page (three-column obj-table layout: text left 38%, primary canvas 31%, insight canvas 31%; one table per pitfall)
**HTML title tag:** E-Commerce & Marketplace — Distribution Patterns

**Subtitle:** Data shapes that show up in online-store numbers — and why they matter

Each section is its own `.obj-table` with one row: a colored uppercase `.pitfall-label` tag, an h3 title, a lead paragraph, a bullet list, then two canvases. Pitfall label colors are assigned in order from the palette `["#795548", "#2980b9", "#27ae60", "#e74c3c", "#8e44ad", "#e67e22", "#16a085", "#d35400", "#c0392b", "#1abc9c"]` (label 1 = `#795548`, label 2 = `#2980b9`, ... cycling).

All simulated data uses a seeded RNG — mulberry32 with seed 42, shared sequentially across all charts in document order — plus a Box–Muller `randNormal(mean, std)` built on it. Two shared drawing utilities:

- `drawHistogram(canvasId, data, opts)`: white background; bold 13px `#1a5276` centered title at y=18; `#999` 1px L-axes; margins {top:35, right:20, bottom:40, left:50}; bars normalized to max bin count, fill/stroke from opts; a Gaussian-smoothed density line (sigma 1.5 bins, `#1a5276`, width 2) with a 95% SE band filled `rgba(230,126,34,0.22)` (effective n clamped to [30, 200]) — the density line + band are skipped when `opts.density === false` (used for zero-inflated data where a smoothed line over the spike is meaningless); 5 x-ticks formatted per opts, 5 y-ticks (rounded counts), 11px `#555` tick labels, 12px `#333` x-label.
- `drawBarChart(canvasId, labels, values, opts)`: same styling; one bar per label with 15% padding; 5 y-ticks formatted per opts.

## Section 1: Where People Click in Search Results (Geometric Decay)

**Pitfall label:** DROPS OFF A CLIFF (`#795548`)

The first result gets ~30% of clicks, the second gets about half that, and by the fifth spot almost nobody is clicking. The decay is strikingly regular: each position gets roughly half the clicks of the one above it. One explanation: users scan top-down with a fixed give-up probability — like a hallway with doors where about half the remaining people stop at each door.

- At each position, about half the remaining clicks disappear
- The smoothness is consistent with users trusting the ranking rather than weighing each item
- Similar cliff shapes are widely reported on search engines, marketplaces, and other ranked lists
- If you're A/B testing search results, the position effect drowns out the content effect

### Visualization (canvas `canvas1`, 420×340)

Bar chart via `drawBarChart`.

- **Title:** "Click-Through Rate by Search Position"; **x-label:** "Position".
- **Data:** positions #1–#10 with CTR% following geometric decay: start p=30%, multiply by 0.52 per position (30, 15.6, 8.11, 4.22, 2.19, 1.14, 0.59, 0.31, 0.16, 0.08).
- **Bars:** fill `rgba(26,82,118,0.55)`, border `#1a5276`. Y-ticks formatted as whole percents ("N%").

### Visualization (canvas `canvas1b`, 400×340)

Cumulative click share area/line chart.

- **Title (bold 13px `#1a5276`, centered):** "Cumulative Click Share".
- **Data:** cumulative share (%) of the ten CTR values above, computed at each position; area under the curve filled `rgba(41,128,185,0.3)`, line `#2980b9` width 3, 5px dots at each point — first 3 dots red `#e74c3c`, the rest `#2980b9`.
- **Top-3 line:** horizontal dashed red `#e74c3c` line (width 2, dash 5/4) at the cumulative value of position 3 (≈86%), with red bold 12px label: "Top 3 = 86% of clicks" (value computed from data, rounded).
- **Axes:** margins {35,20,40,50}; x labels #1–#10; y labels 0–100% in 25% steps.

## Section 2: Shopping Cart Totals — Free Shipping Changes Behavior (Log-Normal + Spikes)

**Pitfall label:** SMOOTH HILL WITH SPIKES (`#2980b9`)

Most cart totals form a smooth right-skewed hill (median around $35), like you'd expect. But then there are sharp spikes at exactly $25, $35, $49, and $75 — the free-shipping thresholds. That's consistent with shoppers adding filler items to hit the magic number. You can reverse-engineer a company's shipping policy just by looking at the spike locations.

- The sharp spikes sit exactly at the free-shipping cutoffs
- The smooth hill in the middle is what unconstrained spending looks like
- The spikes mess up models that assume spending changes smoothly
- When the company changes its free-shipping threshold, the spikes move — instantly visible in the data

### Visualization (canvas `canvas2`, 420×340)

Histogram via `drawHistogram`, 60 bins, range $0–$130.

- **Title:** "Cart Value Distribution ($)"; **x-label:** "Cart Value ($)"; x-ticks formatted "$N".
- **Data:** 3000 log-normal draws `exp(N(3.5, 0.55))` kept if 0 < v < 150, plus injected spikes at thresholds [25, 35, 49, 75] with counts [180, 250, 200, 120], each spike value = threshold + uniform(−0.75, +0.75).
- **Bars:** fill `rgba(39,174,96,0.5)`, border `#27ae60`.

### Visualization (canvas `canvas2b`, 400×340)

Annotated spike diagram over a smooth density.

- **Title (bold 13px `#27ae60`, centered):** "Threshold Scars in Cart Values".
- **Background density:** approximate log-normal density curve (mu 3.5, sigma 0.55) over $0–$130, filled `rgba(39,174,96,0.15)`, stroked `rgba(39,174,96,0.6)` width 1.5.
- **Spikes:** thick vertical lines (width 3) at $25, $35, $49, $75; heights proportional to spike counts [180, 250, 200, 120] scaled so max = 85% of plot height; colors `#e74c3c`, `#e67e22`, `#8e44ad`, `#2980b9` respectively. Each spike topped with a diamond marker, a small downward arrow above it, a bold 11px price label ("$25" etc.) and a 9px "FREE SHIP" label above that, all in the spike color.
- **X labels:** $0–$130 in 6 ticks.
- **Annotation box (bottom right, 125×40):** fill `rgba(231,76,60,0.1)`, border `#e74c3c` 1px; bold 10px `#c0392b` "Policy → Distribution"; 9px "Spikes = company decisions".

## Section 3: How Long Until Someone Buys — Impulse vs. Researchers (Bimodal)

**Pitfall label:** TWO HUMPS, GAP IN THE MIDDLE (`#27ae60`)

There are two totally different kinds of buyers hiding in this data. Group A buys within a day or two — consistent with impulse purchases. Group B takes about two weeks — consistent with researchers who compare options and come back later. Days 3-10? Almost nobody buys then. The "average" of about 5 days lands in that gap and describes literally no one.

- The dead zone (days 3-10) is where nobody converts — the "average" time lands right here and describes no real customer
- Two completely different buyer types: same-day impulse shoppers and two-week comparison researchers
- If your marketing doesn't know which group it's talking to, it will fail at reaching both
- Two humps = two different decision-making processes happening in your data

### Visualization (canvas `canvas3`, 420×340)

Histogram via `drawHistogram`, 40 bins, range 0–21 days.

- **Title:** "Time to First Purchase (Days)"; **x-label:** "Days Since First Visit"; x-ticks formatted "Nd".
- **Data:** impulse buyers — 1200 draws of |N(0.5, 0.6)| kept in [0, 2]; dead zone — 50 uniform draws in [3, 10]; researcher buyers — 600 draws of N(13, 1.8) kept in [10, 20].
- **Bars:** fill `rgba(230,126,34,0.5)`, border `#e67e22`.

### Visualization (canvas `canvas3b`, 400×340)

Two-population jittered scatter (strip plot) over days 0–21.

- **Title (bold 13px `#e67e22`, centered):** "Two Hidden Populations".
- **Dead zone band:** gray rectangle `rgba(189,195,199,0.3)` spanning days 3–10 full plot height, with centered bold 11px `#7f8c8d` label "DEAD ZONE" and 9px "nobody converts here".
- **Dots:** every data point from canvas3, x = day mapped to 0–21, y = random vertical jitter; radius 2.5; color by population — day ≤ 2: blue `rgba(41,128,185,0.7)` (impulse), day ≥ 10: orange `rgba(230,126,34,0.7)` (researchers), otherwise gray `rgba(149,165,166,0.5)`.
- **Legend (below axis, 10px `#2c3e50`):** blue dot "Impulse (day 0-1)"; orange dot "Researchers (day 12-14)".
- **Axes:** bottom x-axis only (`#999`), x labels 0d–21d in steps of 3.

## Section 4: Star Ratings — Why You See So Many 5s and 1s (J-Curve)

**Pitfall label:** LOVE IT OR HATE IT (`#e74c3c`)

Most products have tons of 5-star reviews, a bunch of 1-star reviews, and almost nothing in between. One explanation: mostly delighted or angry people bother to write reviews, while the 3-star "it was fine" crowd stays silent. On that reading, the shape reflects motivation to review, not product quality.

- Silence in the middle — the least-represented opinions may be the most common ones
- The shape tells you who is motivated enough to leave feedback, not how good the product is
- The "average rating" of 3.6 stars describes nobody — it's a blend of lovers and haters
- Where platforms prompt every buyer to review, you'd expect the middle to fill in and the shape to change

### Visualization (canvas `canvas4`, 420×340)

Bar chart via `drawBarChart`.

- **Title:** "Star Rating Distribution (J-Curve)"; **x-label:** "Rating".
- **Data:** labels [1★, 2★, 3★, 4★, 5★], counts `[220, 60, 80, 150, 490]` (high at 5, elevated at 1, desert in the middle).
- **Bars:** fill `rgba(231,76,60,0.5)`, border `#e74c3c`; y-ticks as rounded counts.

### Visualization (canvas `canvas4b`, 400×340)

Radar/polar chart of the same rating counts.

- **Title (bold 13px `#c0392b`, centered):** "Sentiment Polarity — Extremes Win".
- **Layout:** center at (w/2, h/2+10), max radius = min(w,h)/2 − 50; 4 concentric grid rings and 5 spokes in `rgba(189,195,199,0.5)` (0.5px); first axis points up, spokes every 72°.
- **Data polygon:** counts `[220, 60, 80, 150, 490]` normalized to max; fill `rgba(231,76,60,0.2)`, stroke `#e74c3c` width 2.5.
- **Vertex dots:** radius 6, white 2px border, colored per rating: `#e74c3c`, `#e67e22`, `#f39c12`, `#27ae60`, `#2ecc71`; axis labels "1★"–"5★" in matching colors (bold 12px) placed 20px beyond max radius.
- **Caption (bottom center, 10px `#7f8c8d`):** "\"Average\" = 3.6★ describes nobody" (average computed from the plotted values, one decimal).

## Section 5: Revenue Per User — The "Two Universes" Problem (Zero-Inflated with a Long Tail)

**Pitfall label:** MOST PAY NOTHING, FEW PAY A LOT (`#8e44ad`)

70% of users spend exactly $0. Among the rest, spending follows a heavy-tailed power law: the top 1% of all users generate roughly a third of total revenue. "Average revenue per user" is meaningless here — it blends two completely separate populations that need separate treatment.

- 70% at exactly $0 — you're averaging two different universes together
- Among people who do spend: a small group of whales generates a large share of the money
- Standard "compare the averages" tests give you nonsense on data shaped like this
- You need to ask two separate questions: "Who converts?" and then "Among converters, who spends big?"

### Visualization (canvas `canvas5`, 420×340)

Histogram via `drawHistogram`, 50 bins, range $0–$300.

- **Title:** "Revenue Per User (Zero-Inflated Pareto)"; **x-label:** "Revenue ($)"; x-ticks formatted "$N".
- **Data:** 7000 users at exactly $0; 3000 spenders drawn Pareto via `5 · (1−U)^(−1/1.2)` (x_min $5, alpha 1.2), capped at $500 for display.
- **Bars:** fill `rgba(142,68,173,0.5)`, border `#8e44ad`.

### Visualization (canvas `canvas5b`, 400×340)

Lorenz-style ECDF of revenue concentration.

- **Title (bold 13px `#8e44ad`, centered):** "ECDF — Revenue Concentration".
- **Curve:** users sorted ascending by spend on x (0–100%), cumulative revenue share on y (0–100%); line `#8e44ad` width 2.5, area under curve filled `rgba(142,68,173,0.15)`.
- **Equality line:** dashed diagonal `#bdc3c7` (width 1.5, dash 4/4) from (0%,0%) to (100%,100%).
- **Concentration markers:** dashed red `#e74c3c` lines (width 1.5, dash 3/3) — horizontal at the curve height of the bottom 95% of users and vertical at x=95%; red bold 11px labels: "5% of users →" below the axis near x=95% and "N% of revenue" at left (N = top-5% revenue share computed from the data, roughly 60–70%).
- **Insight label (bold 12px `#8e44ad`, centered at ~35% width near the bottom of the plot):** "70% contribute $0".
- **X labels:** "0%", "Users (sorted by spend)", "100%"; **Y labels:** 0%, 50%, 100%.

## Section 6: Return Rates — Why the Blended Average Is Misleading (Category Split)

**Pitfall label:** DIFFERENT CATEGORIES, DIFFERENT WORLDS (`#e67e22`)

In this data, clothing returns cluster around 25% (commonly attributed to sizing being a guessing game online), while electronics cluster around 4%. Mash them together and you get an overall return rate of about 17% — a number that describes neither category. Worse: 17% in electronics would be a five-alarm fire, while 17% in clothing is better than typical.

- Clothing: clusters around 25% returns, spread wide
- Electronics: clusters around 4% returns, tightly packed near zero
- The blended average is a mirage — it matches neither category's typical value
- Spotting a policy change: watch the whole shape shift, not just the average number

### Visualization (canvas `canvas6`, 420×340)

Histogram via `drawHistogram`, 35 bins, range 0–50%.

- **Title:** "Return Rate Distribution (All Categories Mixed)"; **x-label:** "Return Rate (%)"; x-ticks formatted "N%".
- **Data:** 1200 apparel draws from Beta(5,15)·100 plus 800 electronics draws from Beta(2,30)·100 (beta variates generated via Marsaglia–Tsang gamma method on the seeded RNG).
- **Bars:** fill `rgba(142,68,173,0.5)`, border `#8e44ad`.

### Visualization (canvas `canvas6b`, 400×340)

Two overlaid beta PDF curves with an aggregate-mean marker.

- **Title (bold 13px `#8e44ad`, centered):** "Two Hidden Populations with Different Beta Shapes".
- **Curves over 0–50%** (200 points, unnormalized beta PDFs scaled to the shared max, drawn to 90% of plot height):
  - Apparel Beta(5,15): fill `rgba(142,68,173,0.25)`, stroke `#8e44ad` width 2.5.
  - Electronics Beta(2,30): fill `rgba(41,128,185,0.25)`, stroke `#2980b9` width 2.5.
- **Aggregate mean marker:** dashed red `#e74c3c` vertical line (width 2, dash 6/4) at the mixed sample's mean (~17%); red annotations to its right: bold 10px "Aggregate mean 17%" (value computed from the sample) and 9px lines "matches neither" / "category's peak".
- **Legend (below axis, bold 11px):** purple line swatch "Apparel beta(5,15)" (`#8e44ad`); blue line swatch "Electronics beta(2,30)" (`#2980b9`).
- **X labels:** 0%–50% in 6 ticks.

## Section 7: How Many Pages Before Buying — Two Types of Shoppers (Funnel with Spike)

**Pitfall label:** QUICK DROP, THEN A SURPRISE BUMP (`#16a085`)

Most people either buy on the first page or two, or drop off steadily page by page. But there's a surprising bump around pages 9-12 — a second population that views many options before deciding. One interpretation: fast buyers who already knew what they wanted, plus deliberate comparison shoppers. "Average pages viewed" hides both groups.

- 40% buy on page 1 or 2 — consistent with arriving via an ad or recommendation
- The steady drop-off in the middle: at each page, about 30% of remaining people stop
- The bump at pages 9-12 breaks the decay — a distinct second population, not noise
- Making page 1 better vs. keeping page 8+ visitors engaged are completely different design problems

### Visualization (canvas `canvas7`, 420×340)

Histogram via `drawHistogram`, 20 bins, range 0–20.

- **Title:** "Pages Viewed Before Purchase"; **x-label:** "Product Pages Viewed"; x-ticks as whole numbers.
- **Data:** 800 impulse buyers at page 1 (60%) or 2 (40%); 900 geometric(p=0.3) draws starting at page 3 (max 20); 300 comparison shoppers from round(N(10,2)) clamped to [7,15].
- **Bars:** fill `rgba(230,126,34,0.5)`, border `#e67e22`.

### Visualization (canvas `canvas7b`, 400×340)

Segment-colored bar chart of conversion share by page depth, derived from the canvas7 data (not hardcoded).

- **Title (bold 13px `#e67e22`, centered):** "Where Conversions Happen, by Page Depth".
- **Data:** computed from the simulated sessions — each session converts at its final page, so the share at page i is the fraction of all sessions whose page count rounds to i (pages 1-15).
- **Bar colors by segment:** pages 1–2 blue `rgba(41,128,185,0.6)`/`#2980b9` (impulse); pages 9–12 green `rgba(39,174,96,0.6)`/`#27ae60` (comparison shoppers); all others orange `rgba(230,126,34,0.4)`/`#e67e22` (geometric decay).
- **Segment brackets (top of plot):** blue bracket over pages 1–2 labeled "IMPULSE" (bold 9px `#2980b9`); green bracket over pages 9–12 labeled "COMPARISON SHOPPERS" (bold 9px `#27ae60`).
- **Axes:** x labels 1–15, x-label "Page Number"; y-ticks 0 to the computed max share in quarters, formatted "N%".
- **Legend (below axis, 9px `#2c3e50`):** blue swatch "Impulse (ad/rec)"; green swatch "Comparison shoppers"; orange swatch "Geometric decay".

## Section 8: How Many Items in One Session — The Basket Isn't the Entry Item (Long-Tail Count)

**Pitfall label:** ONE SESSION, MANY ITEMS (`#d35400`)

Session analysis usually anchors on the item that started the session — the thing searched for or clicked from an ad. But sessions routinely end with more than one item: accessories, cross-category adds, filler to reach free shipping. Single-item sessions are the most common, yet the multi-item tail carries a large share of revenue — and most of that revenue comes from items the session was never "about."

- The count distribution is right-skewed: one item is the mode, but the tail runs long
- In larger baskets, the entry item is a minority of basket value — the adds dominate
- Attributing the whole basket to the entry item (or its ad) inflates that item's measured demand
- Restricting analysis to the searched item throws away the co-purchase structure that drives recommendations

### Visualization (canvas `canvas8`, 420×340)

Histogram via `drawHistogram`, 12 bins, range 1–13.

- **Title:** "Items Bought per Purchase Session"; **x-label:** "Items in Session"; x-ticks as whole numbers.
- **Data:** 2600 geometric-ish draws — start at 1 item, keep adding while rng() > 0.52, max 12; plus 120 stock-up sessions from round(N(9, 1.2)) clamped to [7,12].
- **Bars:** fill `rgba(26,82,118,0.5)`, border `#1a5276`.

### Visualization (canvas `canvas8b`, 400×340)

100%-stacked bar chart: entry item's share of basket value by basket size.

- **Title (bold 13px `#1a5276`, centered):** "Entry Item's Share of Basket Value".
- **Data (basket sizes ['1','2','3','4','5','6+'], entry-item share %):** `[100, 61, 46, 37, 30, 24]` (illustrative); remainder of each 100% bar = items added during session.
- **Bars:** bottom segment (entry item) fill `rgba(26,82,118,0.55)`, stroke `#1a5276`; top segment (added items) fill `rgba(39,174,96,0.55)`, stroke `#27ae60`; bold 10px `#1a5276` percentage label ("100%", "61%", ...) just inside the top of each entry segment.
- **Axes:** x labels 1–6+, x-label "Items in Basket"; y labels 0%–100% in 25% steps.
- **Legend (below axis, 9px `#2c3e50`):** blue swatch "Entry item"; green swatch "Items added during session".

## Regeneration instructions

- **Layout:** one `.obj-table` per pitfall (8 tables), each a single `<tr>` with three `<td>`s: text (38%), primary chart canvas (31%, centered), insight chart canvas (31%, centered). Text cell = `.pitfall-label` span, h3 title, lead paragraph, `<ul>` bullets.
- **Page style:** body system sans-serif, margin 20px, background `#f9f9f9`, text `#333`; h1 centered `#1a5276`; `.subtitle` centered `#555` 14px; table cell borders `1px solid #2980b9`, padding 12px, vertical-align top; h3 `#1a5276` 1.0em weight 700; paragraphs/bullets 14px, line-height 1.5–1.6; `.pitfall-label` inline-block bold 0.72em uppercase with 0.5px letter-spacing. No nav bar, no back/home links.
- **Pitfall label colors:** applied by a small script cycling `["#795548","#2980b9","#27ae60","#e74c3c","#8e44ad","#e67e22","#16a085","#d35400","#c0392b","#1abc9c"]` over `.pitfall-label` elements in document order.
- **Canvas:** intrinsic sizes 420×340 (primary) and 400×340 (insight); CSS `canvas { width: 100%; height: auto; }`; each draw routine sets `max-width` to the intrinsic width, sizes the backing store to the displayed width (`getBoundingClientRect().width`, falling back to the intrinsic width) × `window.devicePixelRatio`, and calls `ctx.scale(scale, scale)` with that combined scale so charts stay sharp at the rendered size.
- **Data:** all simulated data is generated with mulberry32(seed 42) shared across charts in document order, Box–Muller normals, Marsaglia–Tsang gamma for beta variates; derived annotations (top-3 share, average rating, top-5% revenue share, aggregate mean) are computed from the generated data, not hardcoded.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`; bar fills at ~0.5 alpha of their stroke color.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
