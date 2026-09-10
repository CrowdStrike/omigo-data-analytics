# Small-Sample Rate Extremes — Lucky Sale & Low-N Metrics

**Page type:** detail page (h2 section headings, each followed by a two-column obj-table row: text left 40%, canvas right 60%)
**HTML title tag:** Small-Sample Rate Extremes — Lucky Sale & Low-N Metrics

**Subtitle:** When items have few impressions, rate metrics (CTR, conversion rate, sales/impression) become noise masquerading as signal. One lucky event creates an extreme rate that the system treats as truth.

## 1. The Lucky Sale — 1 Impression, 100% Conversion

**Someone buys a cheap item on first view — sales/impression = 1.0**

- **The scenario:** Item shown once → user buys it (often cheap, impulse). Conversion rate = 100%. System sees this as the highest-performing item in the catalog.
- **Why it's wrong:** The "rate" is meaningless at n=1. The 95% confidence interval for a 1/1 success is [2.5%, 100%]. The point estimate tells you nothing.
- **The damage:** If the system uses raw conversion rate to rank → this item gets boosted → gets more impressions → now its true rate (maybe 3%) emerges → rankings oscillate wildly.
- **Where it happens:** E-commerce conversion optimization. Ad CTR for new creatives. App install rates for new campaigns. Any rate metric on fresh items.

**Fix:** Bayesian shrinkage (beta-binomial prior). Minimum impression threshold before rate is trusted. Wilson score interval instead of point estimate. Blend toward population mean at low n.

### Visualization (canvas `ca1`, 720×300)

Scatter plot: conversion rate vs impressions on a log-x scale, showing low-n extremes converging to the true rate.

- **Title (bold 14px `#1a5276`, top center):** "Conversion Rate vs Impressions — Low-N Extremes".
- **Data points (impressions, conversion %):** (1, 100, labeled "Lucky sale"), (2, 50), (3, 66), (5, 60), (8, 37), (12, 25), (20, 15), (45, 8), (100, 5.2), (200, 4.8), (500, 4.1), (1000, 3.9), (2000, 4.0), (5000, 3.8), (10000, 4.2).
- **Scales:** x = log10(impressions) over log10(10000); y = 0–100%. Margins left 70, right 40, top 50, bottom 45.
- **Point style:** radius 8 for imp<10, 5 for imp<100, else 3; fill `rgba(231,76,60,0.7)` (red) for imp<10, else `rgba(26,82,118,0.4)` (blue). The n=1 point carries a bold 11px red label "Lucky sale" to its right.
- **True rate line:** dashed green `#27ae60` (dash 5/3, width 2) horizontal at 4%, right-aligned 11px green label "True population rate ~4%".
- **Axis labels (11px `#666`):** "Impressions (log scale)" bottom center; "Conversion Rate %" rotated on the left.
- **Caption (bold 11px red `#e74c3c`, above the x-axis label):** "Low-N items dominate leaderboards. Noise looks like signal."

## 2. Confidence Interval Width at Low N

**At n=5, your 95% CI is wider than the entire metric range**

- **The math:** Observed 2/5 = 40% CTR. Wilson interval: [11.8%, 73.6%]. The item's "true CTR" could be anywhere from terrible to excellent. You literally know nothing.
- **At n=50:** Same observed rate (20/50 = 40%). Wilson interval: [27.0%, 54.4%]. Starting to be informative.
- **At n=500:** 200/500 = 40%. Wilson interval: [35.7%, 44.4%]. Now you can trust it.
- **The mistake:** Teams treat the point estimate the same regardless of sample size. "40% conversion" at n=5 looks identical to "40% conversion" at n=5000 in a dashboard.

**Fix:** Always display confidence intervals alongside rates. Gray out / flag metrics below minimum n. Sort by lower confidence bound (pessimistic estimate), not point estimate.

### Visualization (canvas `ca2`, 720×300)

Horizontal CI-width bars: 95% Wilson interval per sample size, all at observed rate 40%.

- **Title (bold 14px `#1a5276`, top center):** "95% Confidence Interval Width at Observed Rate = 40%".
- **Data (n / lower / upper):** 5 / 11.8 / 73.6; 10 / 16.8 / 67.7; 20 / 21.3 / 61.3; 50 / 27.0 / 54.4; 100 / 30.5 / 50.1; 500 / 35.7 / 44.4; 2000 / 37.9 / 42.2.
- **Layout:** margins left 100, right 60, top 55, bottom 40; one 28px-tall bar per n, 8px gap; x scaled 0–100%.
- **Bar colors by n:** n<20 → fill `rgba(231,76,60,0.3)` / stroke `#e74c3c`; n<100 → fill `rgba(230,126,34,0.3)` / stroke `#e67e22`; else fill `rgba(39,174,96,0.3)` / stroke `#27ae60`. Stroke width 1.5.
- **Center line:** vertical blue `#1a5276` (width 2) through each bar at 40%, with bold blue "40%" label above the column.
- **Labels (11px `#333`):** "n=5" etc. right-aligned left of each bar; "11.8%–73.6%" etc. to the right of each bar.
- **Caption (bold 11px red `#e74c3c`, bottom center):** "At n=5: "40% CTR" actually means "somewhere between 12% and 74%"".

## 3. Bayesian Shrinkage — The Correct Response

**Blend extreme observations toward the population mean proportional to uncertainty**

- **The principle:** At n=1, your best guess is the population average (prior). At n=10000, your best guess is the observed rate (data dominates). In between, it's a weighted blend.
- **Beta-binomial:** Prior: Beta(α, β) from population. Posterior: Beta(α + successes, β + failures). Point estimate = (α + successes) / (α + β + n). Shrinks extreme rates toward the mean.
- **Effect:** The "lucky sale" item with 1/1 gets pulled from 100% down toward ~5% (population average). An item with 500/1000 stays near 50% because data dominates.

**Fix:** Use empirical Bayes (estimate prior from catalog-wide rates). Apply to all rate metrics before ranking. The stronger your prior, the more data you need to override it.

### Visualization (canvas `ca3`, 720×300)

Paired horizontal bars: raw rate vs Bayesian-shrunk estimate per item.

- **Title (bold 14px `#1a5276`, top center):** "Raw Rate vs Bayesian Shrinkage Estimate".
- **Data (label / raw % / shrunk % / n):** "1/1 (lucky sale)" / 100 / 8.3 / 1; "2/3" / 66.7 / 12.5 / 3; "5/10" / 50 / 18.8 / 10; "20/50" / 40 / 28.6 / 50; "200/500" / 40 / 37.5 / 500; "2000/5000" / 40 / 39.6 / 5000.
- **Layout:** margins left 120, right 60, top 50, bottom 35; each item is a 30px-tall pair (top half raw, bottom half shrunk) with 10px gap; x scaled 0–100%.
- **Bar colors:** raw `rgba(231,76,60,0.4)` (red); shrunk `rgba(39,174,96,0.5)` (green). Value labels (10px) at each bar end: raw in red (integer %), shrunk in green (one decimal %). Item labels right-aligned 11px `#333`.
- **Prior mean line:** vertical dashed gray `#666` (dash 3/3, width 1) at 4%, labeled "prior mean (4%)" in 10px gray above.
- **Legend (bottom, bold 11px):** red "■ Raw rate"; green "■ Shrunk estimate"; gray 11px note "(At high n, shrinkage vanishes — data dominates)".

## Regeneration instructions

- **Layout:** each section is an `<h2>` heading ("1. …", "2. …", "3. …", 1.3em `#1a5276` with 2px `#2980b9` bottom border) followed by a single-row `.obj-table`: full-width, border-collapse, one `<tr>`; left `<td>` (40%) holds `.obj-title` div + `<ul>` bullets + a `<p>` Fix line, right `<td>` (60%, centered) holds the canvas. Even rows background `#fafcfe`.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; `ul` 0.9em `#333`, li margin 4px 0; `strong` `#1a5276`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600 `#1a5276`. No nav bar, no back/home links.
- **Canvas:** intrinsic `width="720" height="300"` per chart; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`, gray text `#666`/`#333`.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
