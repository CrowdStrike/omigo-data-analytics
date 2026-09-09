# Dissecting Popular Metrics

**Page type:** detail page (backlog-style two-column layout: text left 45%, canvas right 55%, one `.lang-section` per metric; BACKLOG status badge in h1)
**HTML title tag:** Dissecting Popular Metrics

**Status badge:** BACKLOG (inline in h1)

**Subtitle:** What each metric measures, how to read it correctly, and how trivial inflation undermines it.

**Intro callout:** Each metric below is a compressed summary of behavior: a formula, what it actually measures, the correct way to read it, and the cheapest way to inflate it. Any metric that drives decisions will eventually be gamed through its inflation channel.

## 1. CTR — Click-Through Rate

- **Formula:** clicks / impressions
- **Measures:** Whether the creative/title/snippet matched user intent at that moment.
- **Correct read:** Higher CTR = better match between impression and click. Does not mean the destination satisfied them.
- **Mistakes:** Comparing CTR across positions (position 1 always wins). Ignoring post-click behavior.

**Inflation callout (orange left-border box):** ⚠️ **Inflation:** Sensational headlines, misleading thumbnails, curiosity gaps. CTR ↑, satisfaction ↓.

### Visualization (canvas `c1`, 720×300)

Histogram with Gaussian-smoothed density line and SE band (right-skewed CTR distribution).

- **Title (bold 13px, `#1a5276`, top center):** "Typical CTR Distribution".
- **Axes:** L-shaped axes in `#95a5a6` (1.2 width); x label "CTR %", y label "density" (rotated), both 11px `#6b7b8d`; plot area padding px=50, py=34, pw=w−80, ph=h−74.
- **Bars (14 bins, 0.5 alpha, fill `#1a5276`):** `[0.12, 0.38, 0.72, 0.88, 0.95, 0.82, 0.6, 0.4, 0.22, 0.12, 0.07, 0.04, 0.02, 0.01]`, normalized to max.
- **Density overlay:** Gaussian kernel smoothing (sigma 1.2, kernel radius 3σ, winsorized at 2× bin value), center line `#1a5276` width 2, 95% SE band (effN=100) filled `rgba(230,126,34,0.25)`.
- **Bin labels (10px `#888`, centered):** "0%" under bin 0, "~2%" under bin 4, "10%+" under bin 13.

## 2. Dwell Time

- **Formula:** time(next action) − time(page load)
- **Measures:** How long content held attention before user navigated away.
- **Correct read:** Useful as relative signal within same content type. Longer ≠ always better.
- **Mistakes:** Treating all dwell as positive. Not separating active engagement from idle/background tabs.

**Inflation callout:** ⚠️ **Inflation:** Confusion, rage-reading, slow-loading pages, auto-play, infinite scroll.

### Visualization (canvas `c2`, 720×300)

Bimodal histogram with density line + SE band (same drawHistWithDensity style as c1).

- **Title:** "Dwell Time — Bimodal".
- **Axes:** x label "seconds", y label "density"; same layout as c1.
- **Bars (14 bins, fill `#1a5276`):** `[0.92, 0.55, 0.3, 0.18, 0.14, 0.2, 0.35, 0.55, 0.7, 0.62, 0.42, 0.25, 0.12, 0.05]`.
- **Annotations (10px):** "← bounces" in red `#e74c3c`, left-aligned at top-left of plot; "engaged →" in blue `#1a5276`, right-aligned at top-right of plot.

## 3. View Count

- **Formula:** count of render/play events exceeding platform threshold (3s–30s).
- **Measures:** Reach / exposure. Not quality, not completion.
- **Correct read:** A view ≠ watched. Must pair with completion rate to mean anything about engagement.
- **Mistakes:** Equating views with engagement. Treating 3s autoplay same as full intentional watch.

**Inflation callout:** ⚠️ **Inflation:** Autoplay in feeds, bot farms, refresh loops, playlist padding.

### Visualization (canvas `c3`, 720×300)

Power-law histogram with density line + SE band.

- **Title:** "View Count Distribution".
- **Axes:** x label "views (log scale)", y label "density".
- **Bars (14 bins, fill `#1a5276`):** `[0.95, 0.78, 0.58, 0.4, 0.27, 0.18, 0.12, 0.08, 0.05, 0.03, 0.02, 0.015, 0.01, 0.008]`.
- **Annotation (10px purple `#8e44ad`, left-aligned at ~15% plot width, near top):** "power law: most content gets near-zero views".

## 4. Likes

- **Formula:** count of binary positive taps on content.
- **Measures:** Momentary positive reaction. Low-cost, one-tap signal.
- **Correct read:** Biased toward emotional peaks, humor, controversy. Not a measure of long-term utility. "Not liking" ≠ disliking.
- **Mistakes:** Using likes as quality signal for ranking. Ignoring that most satisfaction is silent.

**Inflation callout:** ⚠️ **Inflation:** Outrage bait, engagement pods, like-for-like exchanges, asking for likes.

### Visualization (canvas `c4`, 720×300)

Extreme right-skew histogram with density line + SE band.

- **Title:** "Like Distribution".
- **Axes:** x label "likes per post", y label "density".
- **Bars (14 bins, fill `#8e44ad`):** `[0.98, 0.7, 0.42, 0.22, 0.12, 0.06, 0.03, 0.018, 0.01, 0.006, 0.004, 0.003, 0.002, 0.001]`.
- **Annotations (10px `#888`, left-aligned):** "~70% get 0-2 likes" at ~25% plot width, ~40% plot height; "viral tail →" at ~70% plot width, ~85% plot height.

## 5. MRR — Mean Reciprocal Rank

- **Formula:** (1/N) × Σ (1/rank_i) for first relevant result per query.
- **Measures:** How quickly user hits the first useful result. 1.0 = always rank 1. 0.5 = average rank 2.
- **Correct read:** Offline evaluation metric for ranking. Good for single-answer tasks (voice, QA).
- **Mistakes:** Ignores everything after first hit. 1 good + 9 garbage = same score as 10 good results.

**Inflation callout:** ⚠️ **Inflation:** Conservative rankers surfacing the obvious popular answer at rank 1 while failing on novel/tail queries.

### Visualization (canvas `c5`, 720×300)

Bar chart of 1/rank weight decay with density line + SE band overlay.

- **Title:** "MRR: 1/rank Weight Decay".
- **Axes:** x label "rank position", y label "1/rank contribution".
- **Bars (10 bins, fill `#1a5276`):** `[1, 0.5, 0.33, 0.25, 0.2, 0.167, 0.143, 0.125, 0.111, 0.1]`.
- **X tick labels (9px `#555`, centered):** "1" through "10" under each bar.
- **Annotation (10px red `#e74c3c`, left-aligned at ~35% plot width, near top):** "rank 1 dominates the score".

## 6. NDCG — Normalized Discounted Cumulative Gain

- **Formula:** DCG / idealDCG, where DCG = Σ gain(i) / log₂(i+1).
- **Measures:** Ranked list quality weighted by position. 1.0 = perfect ordering.
- **Correct read:** Captures relevance × position. Great at rank 5 counts less than good at rank 1.
- **Mistakes:** Assuming log discount matches user patience for all query types. Truncating at @10 hides tail behavior.

**Inflation callout:** ⚠️ **Inflation:** Over-optimizing top-1 at expense of diversity. Subjective relevance labels inflate scores.

### Visualization (canvas `c6`, 720×300)

Bar chart of NDCG log discount curve with density line + SE band.

- **Title:** "NDCG Position Discount".
- **Axes:** x label "rank position", y label "discount weight".
- **Bars (14 bins, fill `#1a5276`):** computed as 1/log₂(i+1) for i = 1..14, i.e. approximately `[1.0, 0.631, 0.5, 0.431, 0.387, 0.356, 0.333, 0.315, 0.301, 0.289, 0.279, 0.270, 0.263, 0.256]`.
- **Annotation (10px `#888`, left-aligned at ~25% plot width, near top):** "1/log₂(rank+1) — diminishing weight".

## 7. GMV — Gross Merchandise Value

- **Formula:** Σ (item price × quantity) across all transactions in period.
- **Measures:** Total dollar volume flowing through platform. Not platform revenue (keep only 10-25% take rate).
- **Correct read:** Scale indicator. Must compare with same take-rate peers. Subtract returns/cancellations for reality.
- **Mistakes:** Treating GMV as revenue. Not deducting returns, fraud. Comparing across different take rates.

**Inflation callout:** ⚠️ **Inflation:** Heavy discounting, counting cancelled orders, bundling fees into price.

### Visualization (canvas `c7`, 720×300)

Right-skewed transaction-value histogram with density line + SE band.

- **Title:** "GMV: Transaction Value Distribution".
- **Axes:** x label "transaction value ($)", y label "density".
- **Bars (14 bins, fill `#27ae60`):** `[0.15, 0.45, 0.82, 0.95, 0.78, 0.52, 0.32, 0.18, 0.1, 0.06, 0.04, 0.025, 0.015, 0.01]`.
- **Annotations (10px):** "bulk of txns" in green `#27ae60`, left-aligned at ~10% plot width, near top; "high-value tail drives GMV →" in `#888`, right-aligned at plot right edge, near top.

## 8. Bought Items (Units Sold)

- **Formula:** count of items transacted, regardless of price.
- **Measures:** Transaction frequency and breadth. Useful for logistics/supply chain.
- **Correct read:** Says nothing about revenue quality. $1 accessory = $500 electronics in this metric.
- **Mistakes:** Treating unit surge as growth signal without value weighting.

**Inflation callout:** ⚠️ **Inflation:** Buy-3-get-1 promos, filler add-ons, splitting bundles into SKUs, free samples as "sold."

### Visualization (canvas `c8`, 720×300)

Paired dual-bar chart: units share vs revenue share by price tier, with connecting line per series.

- **Title:** "Units vs Revenue by Price Tier".
- **Axes:** x label "price tier", y label "% share".
- **Data (5 tiers):**
  - units share: `[0.45, 0.28, 0.15, 0.08, 0.04]` — red bars, fill `#e74c3c` at 0.5 alpha.
  - revenue share: `[0.05, 0.12, 0.20, 0.25, 0.38]` — green bars, fill `#27ae60` at 0.5 alpha.
  - tier labels (9px `#555`, centered under each pair): `<$5`, `$5-20`, `$20-50`, `$50-100`, `$100+`.
- **Bar geometry:** each tier gets a side-by-side pair (units left, revenue right, 2px apart); bar heights scaled at value × plot height × 2.
- **Overlay lines:** polyline through units bar centers in `#e74c3c` width 2; polyline through revenue bar centers in `#27ae60` width 2.
- **Legend (10px, top-left):** "units" in `#e74c3c`, "revenue" in `#27ae60`.

## 9. Churn Rate

- **Formula:** lost customers / total customers at period start.
- **Measures:** Retention failure. Must pair with cohort vintage.
- **Correct read:** 5% monthly churn in month-1 users ≠ 5% in year-3 users. Context-dependent.
- **Mistakes:** Mixing monthly/annual subs. Counting "paused" as retained. Reactivations erasing history.

**Inflation callout:** ⚠️ **Deflation:** Redefining "active" loosely, pause features, forced annual contracts delaying recognition.

### Visualization (canvas `c9`, 720×300)

Monthly churn histogram (red bars) with SE band + smoothed density line, plus green survival-curve overlay.

- **Title:** "Churn: Monthly Loss + Survival".
- **Axes:** x label "month", y label "rate".
- **Churn bars (12 bins, fill `#e74c3c` at 0.5 alpha, normalized to max 0.22):** `[0.22, 0.15, 0.12, 0.1, 0.08, 0.07, 0.06, 0.055, 0.05, 0.048, 0.045, 0.043]`.
- **Density overlay:** same Gaussian smoothing + SE band spec as c1 applied to the churn bars.
- **Survival line (`#27ae60`, width 2.5):** cumulative product retained = Π(1 − churn_i) plotted at bar centers, i.e. approximately `[0.78, 0.663, 0.583, 0.525, 0.483, 0.449, 0.422, 0.399, 0.379, 0.361, 0.345, 0.330]`, scaled 0-1 to full plot height.
- **Legend (10px, top of plot):** "monthly churn %" in `#e74c3c` (left), "survival curve" in `#27ae60` (at ~55% plot width).

## 10. By Sector — What They Actually Optimize

Comparison table (`table.compare`, blue header row):

| Sector | Primary Engagement Signal | Primary Business Metric |
|--------|---------------------------|-------------------------|
| Web Search | CTR on results | Ad revenue per query |
| Long-form Video | Watch time | Ad revenue (CPM × impressions) |
| Short-form Video | Dwell time per video | Ad revenue per user |
| Social Feed | Time spent in feed | Ad revenue per user |
| Microblogging | Impressions | Ad revenue |
| Streaming (video) | Hours streamed | Subscriber revenue |
| Streaming (audio) | Listening hours | Subscription revenue |
| E-commerce | Purchase rate | GMV |
| Ride-hailing | Rides completed | Gross bookings |
| Vacation Rental | Nights booked | Gross bookings |
| Professional Network | Feed sessions | Job ad + recruiter revenue |
| Team Messaging | Messages sent | Paid seat count |
| Dating | Matches | Subscription revenue |
| Visual Discovery | Pins saved | Ad revenue |

## Regeneration instructions

- **Template/layout:** backlog kusto-style detail page. `<h1>` with inline `<span class="status">BACKLOG</span>` badge, `.subtitle` paragraph, `.intro` callout, then one `.lang-section` per metric. Sections 1–9 each contain an `<h2>` ("N. Title") and a `table.layout` with one row: left `td.text-col` (45%) with `.field` paragraphs (`<strong>` label + text) and one `.inflation` callout; right `td.viz-col` (55%) with the canvas. Section 10 contains only a `table.compare`.
- **Field structure:** each text cell has four `.field` paragraphs labeled Formula / Measures / Correct read / Mistakes (bold labels in `#1a5276`), then the `.inflation` box.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border. h2 1.3rem `#1a5276`, 2px `#2980b9` bottom border. `.subtitle` `#666` 0.95rem. `.intro` background `#f0f4f8`, left border 3px `#2980b9`. `.status` badge: background `#fef9e7`, border 1px `#f39c12`, text `#b7950b`, radius 4px. `.inflation` background `#fef9e7`, left border 3px `#e67e22`, 0.86rem. `.field` 0.9rem with `strong` in `#1a5276`. `table.compare`: header background `#1a5276` white text, rows bordered `#eee`, even rows `#f8fafb`, first column bold. Canvases `width: 100%`, border 1px `#e0e0e0`, radius 4px.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange; also `#8e44ad` purple and `#2980b9` accent blue.
- **Canvas rendering:** all canvases declare intrinsic width/height and are scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper. Shared helpers: `drawAxes` (L-shaped gray axes with x/y labels), `drawTitle` (bold 13px `#1a5276` centered), `drawBand` (Gaussian kernel smoothing sigma 1.2, winsorized, 95% SE band with effN=100 filled `rgba(230,126,34,0.25)`, center line `#1a5276` width 2), `drawHistWithDensity` (0.5-alpha bars + band).
- Note: in regenerated HTML any card/page links use `.html` extensions (this page has none).
