# Loyalty & Credit Card Analytics — Distribution Patterns

**Page type:** detail page (3-column obj-table layout: text left 38%, histogram canvas center 31%, insight canvas right 31%, one table per pattern)
**HTML title tag:** Loyalty & Credit Card Analytics — Distribution Patterns

**Subtitle:** 5 distributions that expose how customer behavior clusters, concentrates, and fractures across loyalty programs

## Purchase Frequency (Negative Binomial)

**Label:** HEAVY BUYER DOMINANCE (color `#795548`)

Half of cardholders visit three times a year or fewer, while a small tail visits 20+ times. The heavy tail drags the mean well above the typical customer — the "average" member is rarer than the number suggests.

- 50% of customers visit 3x/year or fewer
- Top 5% visit 20+ times/year
- Mean (~6) is roughly double the median (3)
- Negative binomial: overdispersed count data

### Visualization (canvas `canvas1`, 420×340)

Histogram (shared `drawHistogram` helper, see Regeneration instructions).

- **Data:** 3000 overdispersed counts — `floor(-ln(u)/0.3) · (1 + floor(u'·3))`, capped at 100; seeded RNG mulberry32(42).
- **Bins/range:** 40 bins, x from 0 to 60.
- **Title:** "Purchase Frequency (visits/year)". **X label:** "Visits per Year". X tick format: integer.
- **Colors:** bar fill `rgba(26,82,118,0.5)`, bar border `#1a5276`.

### Visualization (canvas `canvas1b`, 400×340)

ECDF step-line of the same data.

- **Title (bold, `#1a5276`, top center):** "ECDF — Cumulative Purchase Frequency".
- **Line:** ECDF of the sorted simulated data, x clamped to 60 — `#1a5276`, width 2.5.
- **Median annotation:** dashed red `#e74c3c` guides (dash 5/3, width 1.5) — horizontal from y-axis at 50% to the curve, vertical down to the x-axis; bold 10px red labels "50% of customers" / "visit ≤3x/year".
- **P95 annotation:** dashed green `#27ae60` guides at the 95th percentile; bold 10px green label "Top 5% visit 20+ times".
- **Axes:** gray `#999`; x ticks "0", "15", "30", "45", "60" with title "Visits/Year"; y ticks "0%", "50%", "100%".

## Customer Lifetime Value (Pareto)

**Label:** WHALE CONCENTRATION (color `#2980b9`)

Revenue follows a Pareto shape — the top 5% of customers generate nearly 40% of total revenue. A whale spends about 20x a typical bottom-half customer, a split the average completely hides.

- Pareto(alpha=1.2): extreme right tail
- Top 5% = ~38% of revenue (Gini ~0.55)
- Losing 1 whale ≈ losing 20 casuals
- Mean CLV (~2x the median) is a poor guide for resource allocation

### Visualization (canvas `canvas2`, 420×340)

Histogram (shared helper).

- **Data:** 3000 draws from Pareto(alpha=1.2, xmin=50) via inverse CDF `50 / u^(1/1.2)`, capped at 5000.
- **Bins/range:** 50 bins, x from 0 to 3000.
- **Title:** "Customer Lifetime Value ($)". **X label:** "CLV ($)". X tick format: "$" + integer.
- **Colors:** bar fill `rgba(41,128,185,0.5)`, bar border `#2980b9`.

### Visualization (canvas `canvas2b`, 400×340)

Lorenz curve of revenue concentration, computed from the same simulated data (sorted ascending, ~100 sample points).

- **Title (bold, `#2980b9`, top center):** "Lorenz Curve — Revenue Concentration".
- **Equality diagonal:** dashed gray `#999` line (dash 5/4, width 1.5); Gini area between diagonal and curve shaded `rgba(41,128,185,0.2)`.
- **Lorenz curve:** `#2980b9`, width 3.
- **Top-5% marker:** dashed red `#e74c3c` vertical line (dash 3/3, width 1.5) at x=95%; bold 11px red label near top-left: "Top 5% =" / "~38% of revenue".
- **Bold annotation (`#c0392b` 10px, centered near bottom):** "Losing 1 whale =" / "losing ~20 casuals".
- **Gini readout:** bold 12px `#2980b9`, right-aligned: "Gini = <computed value to 3 decimals>" (computed from the sample; ~0.55).
- **Axes:** gray `#999`; x ticks "0%", "50%", "100%" with title "% of Customers (sorted by CLV)"; y ticks "0%", "100%" and "% Rev" mid-axis.

## Churn Hazard (Bathtub Curve)

**Label:** BATHTUB CURVE (color `#27ae60`)

Churn is NOT uniform over time. It follows the classic bathtub shape: high early, low and stable in the middle, then rising again near the two-year mark — consistent with three distinct failure modes.

- Month 1-2: early exits (consistent with onboarding failure)
- Month 3-22: stable retention
- Month 22+: rising again (one explanation: program fatigue)
- One churn model for three regimes = wrong

### Visualization (canvas `canvas3`, 420×340)

Histogram (shared helper).

- **Data:** mixture — 500 draws from Exponential(lambda=2) in months (early churn), 1500 draws uniform on [1, 24] (stable middle), 500 draws from Normal(26, 3) (late churn).
- **Bins/range:** 35 bins, x from 0 to 30.
- **Title:** "Churn Timing (Months Since Enrollment)". **X label:** "Months". X tick format: integer + "mo".
- **Colors:** bar fill `rgba(231,76,60,0.5)`, bar border `#e74c3c`.

### Visualization (canvas `canvas3b`, 400×340)

Bathtub hazard-rate curve with colored regime zones.

- **Title (bold, `#e74c3c`, top center):** "Hazard Rate — The Bathtub Curve".
- **Hazard series (30 monthly points, floor 0.05):** months 0-2: `0.9 - 0.22·m` (high, decaying); months 3-21: `0.2 ± small noise` (flat); months 22+: `0.2 + 0.12·(m-22)` (rising). Line `#c0392b`, width 3, normalized to the series max.
- **Zone bands (full height):** months 0-3 fill `rgba(231,76,60,0.12)`; months 3-22 fill `rgba(39,174,96,0.08)`; months 22-30 fill `rgba(230,126,34,0.12)`.
- **Zone labels (bold 11px near the bottom):** "ONBOARDING" / "FAIL" in `#e74c3c` (early); "STABLE" in `#27ae60` (middle); "FATIGUE" in `#e67e22` (late).
- **Danger arrows:** small diagonal arrows with bold 9px "DANGER" labels — red `#e74c3c` pointing at month 1 on the curve, orange `#e67e22` pointing at month 26.
- **Axes:** gray `#999`; x ticks "0", "6mo", "12mo", "18mo", "24mo", "30mo" with title "Months Since Enrollment"; y-axis label "Churn Rate" at top-left.

## Reward Redemption Timing (Bimodal)

**Label:** TWO PSYCHOLOGIES (color `#e74c3c`)

Redemption points pile up in two camps — small redemptions near 50 points and large ones near 500 — with almost nothing between. Consistent with two redemption styles in one program: instant redeemers and point hoarders. The mean describes neither group.

- 60% redeem at ~50 points (instant gratification)
- 40% save to ~500 points (goal setters)
- Valley at 150-300 points (no man's land)
- One reward tier structure fits neither psychology

### Visualization (canvas `canvas4`, 420×340)

Histogram (shared helper).

- **Data:** mixture — 1200 draws from Normal(50, 15) plus 800 draws from Normal(500, 80), positive values only.
- **Bins/range:** 35 bins, x from 0 to 700.
- **Title:** "Reward Redemption (Points at Redemption)". **X label:** "Points". X tick format: integer.
- **Colors:** bar fill `rgba(142,68,173,0.5)`, bar border `#8e44ad`.

### Visualization (canvas `canvas4b`, 400×340)

Two-persona diagram.

- **Title (bold, `#8e44ad`, top center):** "Two Psychologies — Same Program".
- **Decomposition (computed from the canvas4 data, split at the 150-300 valley midpoint 275):** two rows on a shared 0-700 "Points at Redemption" axis (y=215, ticks every 100). Each row is a p10-p90 range bar (6px, round caps) with a 7px median dot (white 1.5px ring): row 1 (y=100) orange `#e67e22` "Instant gratification"; row 2 (y=175) blue `#2980b9` "Goal setters". Above each row: bold 12px "<name> — <computed pct>% of redemptions" and 10px `#555` "median <computed> pts, p10-p90 <computed>-<computed>".
- **Valley band:** gray `rgba(120,120,120,0.10)` rectangle from x=150 to x=300 (dashed `#999` edges) labeled 9px `#888` "no man's land".
- **Bottom callout box:** rectangle filled `rgba(142,68,173,0.08)`, stroked `#8e44ad` width 1.5; bold 11px purple "One reward tier structure fits neither psychology."; 10px `#555` "Valley at 150-300 points = no man's land".

## Transaction Amount (Log-Normal + Spikes)

**Label:** ROUND-NUMBER SCARS (color `#8e44ad`)

Transaction amounts follow a log-normal base (most purchases small, long right tail) — but with visible spikes at $20, $50, and $100. One explanation: policy scars, such as gift card denominations, authorization limits, and round-number pricing.

- Base shape: log-normal (median ~$33)
- $20 spike — consistent with gift card denominations
- $50 spike — consistent with pre-authorization limits
- $100 spike — consistent with round-number pricing

### Visualization (canvas `canvas5`, 420×340)

Histogram (shared helper).

- **Data:** 3000 draws from exp(Normal(3.5, 0.8)); then with probability 6% the value is replaced by `20 + Normal(0, 0.5)`, next 4% by `50 + Normal(0, 0.5)`, next 3% by `100 + Normal(0, 0.5)`; negatives set to 1.
- **Bins/range:** 50 bins, x from 0 to 200.
- **Title:** "Transaction Amount ($)". **X label:** "Amount ($)". X tick format: "$" + integer.
- **Density overlay disabled** (`density: false`) — smoothing would blur the $20/$50/$100 policy-scar spikes into the log-normal base.
- **Colors:** bar fill `rgba(39,174,96,0.5)`, bar border `#27ae60`.

### Visualization (canvas `canvas5b`, 400×340)

Annotated log-normal curve with policy-scar spikes.

- **Title (bold, `#27ae60`, top center):** "Policy Scars in the Distribution".
- **Base curve:** log-normal PDF (mu=3.5, sigma=0.8) over $0-200, 100 points, scaled to 85% of plot height — line `#27ae60` width 2.5, area under filled `rgba(39,174,96,0.15)`.
- **Spikes (6px-wide vertical bars at 60% alpha, each with a down-arrow and a bold 10px label above in its color):**
  - $20 — height factor 0.7 — `#e74c3c` — label "$20 = gift cards"
  - $50 — height factor 0.55 — `#e67e22` — label "$50 = auth limits"
  - $100 — height factor 0.45 — `#8e44ad` — label "$100 = round-number"
- **Annotation box (lower right):** rectangle filled `rgba(26,82,118,0.06)`, stroked `#1a5276` width 1.5, with bold 10px blue text "Policy scars readable" / "from the shape".
- **Axes:** gray `#999`; x ticks "$0", "$50", "$100", "$150", "$200" with title "Transaction Amount".

## Regeneration instructions

- **Layout:** one `<table class="obj-table">` per pattern, single `<tr>` with three `<td>`: left 38% text (`.pitfall-label` span + `<h3>` + `<p>` + `<ul>`), center 31% (histogram canvas 420×340), right 31% (insight canvas 400×340), both canvas cells centered. Table cell borders `1px solid #2980b9`, padding 12px, `border-collapse: collapse`.
- **Page CSS:** body system sans-serif (-apple-system stack), margin 20px, background `#f9f9f9`, text `#333`; h1 `#1a5276` centered; `.subtitle` centered `#666` 0.95em; h3 `#1a5276` 1.0em weight 700; p/li 14px, line-height 1.5-1.6; `.pitfall-label` inline-block bold 0.72em uppercase, letter-spacing 0.5px; `canvas { width: 100%; height: auto; }`.
- **Pitfall label colors:** assigned by a trailing script from the cyclic palette `["#795548","#2980b9","#27ae60","#e74c3c","#8e44ad","#e67e22","#16a085","#d35400","#c0392b","#1abc9c"]` in document order (labels 1-5 use the first five).
- **Shared histogram helper (`drawHistogram`):** white plot background; bold 13px `#1a5276` centered title at y=18; gray `#999` L-axes; margins top 35 / right 20 / bottom 40 / left 50; bars normalized to max bin count; overlaid Gaussian-smoothed density line in `#1a5276` (width 2, sigma 1.5 bins) with a 95% SE band filled `rgba(230,126,34,0.22)` (effective N clamped to [30, 200]), skipped when `density: false` is passed (canvas 5); 6 x-tick labels in `#555` 11px; optional x-axis label in `#333` 12px. Data simulated with seeded RNG mulberry32(42) and a Box-Muller `randNormal(mean, std)` helper shared across all charts on the page.
- **Canvas scaling:** all canvases declare intrinsic width/height attributes and scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; secondary `#2980b9`, `#8e44ad`, `#c0392b`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions (this page has none).
