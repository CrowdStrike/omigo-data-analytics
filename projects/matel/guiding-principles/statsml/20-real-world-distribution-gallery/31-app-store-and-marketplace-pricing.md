# App Store & Marketplace Pricing — Distribution Patterns

**Page type:** detail page (three-column obj-table layout: text left ~38%, histogram canvas middle ~31%, insight canvas right ~31%, one table per section)
**HTML title tag:** App Store & Marketplace Pricing — Distribution Patterns

**Subtitle:** 5 simulated distributions from digital marketplaces — fixed price tiers, freemium funnels, whale concentration, subscription churn

## App Price Distribution (Zero-Inflated, Free Wins)

**Label:** FREE DOMINATES (color `#795548`)

90% of the apps in this simulated catalog are free; the paid 10% cluster at psychological price tiers ($0.99, $2.99, $4.99, $9.99). This is zero-inflated — no continuous distribution fits because the spike at $0 is a structural wall, not a tail.

- 90% at $0 — free-to-download with IAP/ads
- Paid apps cluster at marketplace's fixed tiers
- Gap between $0 and $0.99 is impossible (minimum tier)
- No tier below $0.99 exists — the floor is the platform's choice, not the market's

### Visualization (canvas `canvas1`, 420×340)

Zero-inflated histogram of app prices.

- **Title (bold 13px, `#1a5276`, top center):** "App Price Distribution (N=3000)".
- **Data:** 2700 samples at exactly 0 (free apps); 120 samples at 0.99 + Normal(0, 0.02) (40% of paid); 75 at 2.99 + Normal(0, 0.02) (25%); 60 at 4.99 + Normal(0, 0.02) (20%); 45 at 9.99 + Normal(0, 0.05) (15%). Seeded RNG (mulberry32, seed 42).
- **Bins/axes:** 30 bins over x range −0.5 to 15; x labels formatted "$N.N"; x-axis label "Price ($)".
- **Bars:** fill `rgba(41,128,185,0.5)`, border `#2980b9`; Gaussian-smoothed (sigma 1.5 bins) density line `#1a5276` width 2 with 95% SE band filled `rgba(230,126,34,0.22)`.

### Visualization (canvas `canvas1b`, 400×340)

Donut chart of free vs paid plus a zoomed paid-tier bar breakdown.

- **Title (bold 13px, `#2980b9`, top center):** "Free vs Paid — The Zero Wall".
- **Donut (left, center ~(100,145), outer radius 65, inner 35):** 90% slice filled `rgba(41,128,185,0.8)` stroked `#2980b9` with bold 14px `#1a5276` center label "90%" / "FREE"; 10% slice filled `rgba(231,76,60,0.8)` stroked `#c0392b` with bold 11px `#e74c3c` label "10%" / "PAID" placed at the slice's mid-angle 22px outside the ring, connected by a short `#c0392b` tick line.
- **Zoomed panel (right, 155×180 `#999`-outlined box titled bold 11px `#333` "Paid Tier Breakdown", connected from the paid slice by a red `#e74c3c` arrow):** 4 horizontal bars at 70% alpha with matching strokes, tier label bold 11px `#333` at left and percentage 11px `#555` at right:
  - "$0.99" — 40%, `#3498db`.
  - "$2.99" — 25%, `#2ecc71`.
  - "$4.99" — 20%, `#f39c12`.
  - "$9.99+" — 15%, `#e74c3c`.
- **Bottom caption (bold 10px, `#c0392b`, centered, two lines, with a small upward arrow above it):** "Fixed tiers: nothing can exist" / "between $0 and $0.99".

## Paywall Conversion Funnel (Geometric Decay)

**Label:** CONVERSION CLIFF (color `#2980b9`)

Each funnel stage loses a massive fraction. Download to subscribe is 1.5% — geometric decay at each gate. The "average user" never pays; the distribution of stage-reached is a collapsing staircase.

- 10K downloads → 8K use → 5K hit paywall
- 5K hit wall → 3K view price → 150 subscribe
- 98.5% never pay a cent
- 40% quit at the paywall step; 95% of price-viewers walk away

### Visualization (canvas `canvas2`, 420×340)

Histogram of maximum funnel stage reached.

- **Title:** "Funnel Stage Reached (N per stage)".
- **Data:** stage totals `[10000, 8000, 5000, 3000, 150]`; users reaching exactly stage i = consecutive difference (2000, 3000, 2000, 2850) with stage 5 = 150 subscribers; downsampled by 3000/10000 to histogram samples at values 1-5.
- **Bins/axes:** 5 bins over x range 0.5-5.5; x tick labels mapped to "DL", "Use", "Wall", "Price", "Sub"; x-axis label "Stage".
- **Bars:** fill `rgba(230,126,34,0.5)`, border `#e67e22`; standard smoothed density line + SE band overlay.

### Visualization (canvas `canvas2b`, 400×340)

Centered funnel diagram of the paywall conversion collapse.

- **Title (bold 13px, `#e67e22`, top center):** "Conversion Funnel — 98.5% Never Pay".
- **Stages (5 centered horizontal bars, widths proportional to n / 10000, max width 70% of canvas, white bold 12px labels inside):**
  - "Download — 10,000", fill `rgba(41,128,185,0.7)`.
  - "Free Use — 8,000", fill `rgba(52,152,219,0.7)`.
  - "Hit Paywall — 5,000", fill `rgba(230,126,34,0.7)`.
  - "View Price — 3,000", fill `rgba(231,76,60,0.7)`.
  - "Subscribe — 150", fill `rgba(192,57,43,0.9)`.
- **Drop labels:** between consecutive stages, bold 10px `#e74c3c` "-20%", "-38%", "-40%", "-95%" at the right edge with short red tick arrows.
- **Left annotation at the paywall stage (bold 9px, `#e74c3c`, right-aligned, two lines with a red arrow):** "40% quit" / "at this step".
- **Bottom caption (bold 12px, `#c0392b`, centered):** "98.5% NEVER PAY A CENT".

## Marketplace 30% Cut (Price Tier Distortion)

**Label:** PLATFORM TAX DISTORTION (color `#27ae60`)

Developers cannot set arbitrary prices — marketplace owners force fixed tiers ($0.99, $1.99, $2.99...). After the 30% cut, $0.99 becomes $0.69. The platform decided your price floor AND your margin. The distribution is discretized by fiat.

- Fixed tiers — no price between $0 and $0.99
- 30% cut on every transaction
- $0.99 tier → developer gets $0.69
- Below $0.99 is impossible — no lower tier exists

### Visualization (canvas `canvas3`, 420×340)

Comb-like histogram of app prices clustered at fixed tiers.

- **Title:** "App Prices Clustered at Marketplace Tiers (N=2000)".
- **Data:** spikes at tiers `[0.99, 1.99, 2.99, 3.99, 4.99, 5.99, 6.99, 7.99, 8.99, 9.99]` with weights `[500, 350, 300, 200, 200, 150, 100, 80, 60, 60]`, each sample = tier + Normal(0, 0.01).
- **Bins/axes:** 20 bins over x range 0-12; x labels formatted "$N.N"; x-axis label "Price ($)".
- **Bars:** fill `rgba(142,68,173,0.5)`, border `#8e44ad`; standard smoothed density line + SE band overlay.

### Visualization (canvas `canvas3b`, 400×340)

Grouped bar chart: customer pays vs developer gets at each tier.

- **Title (bold 13px, `#8e44ad`, top center):** "Customer Pays vs Developer Gets".
- **Groups (6 tiers `[0.99, 1.99, 2.99, 4.99, 6.99, 9.99]`, y scale 0-$10):** per tier a blue left bar (customer price, fill `rgba(41,128,185,0.75)`, stroke `#2980b9`) and a red right bar (developer take = 70% of tier, fill `rgba(231,76,60,0.75)`, stroke `#c0392b`); the gap between the two heights over the red bar is shaded `rgba(149,165,166,0.35)` (the tax). Below each group: 10px `#333` tier price (e.g. "$0.99") and 9px `#e74c3c` developer amount (e.g. "$0.69").
- **Legend (swatches above the plot):** blue "Customer pays", red "Developer gets", gray `#7f8c8d` "30% TAX".
- **Bottom caption (bold 10px, `#c0392b`, centered, with a small arrow under the first group):** "$0.99 is really $0.69 — the platform decided your price floor".
- **Axes:** L-shaped gray `#999` axes; padding top 40, right 15, bottom 55, left 40.

## In-App Purchase Revenue (Whale Concentration)

**Label:** WHALE ECONOMY (color `#e74c3c`)

95% spend $0. Of the 5% who pay, spending follows Pareto(alpha=1.1) — extreme right tail. In this simulation the top 0.5% of users generate ~63% of IAP revenue. The revenue lives in a microscopic fraction of users — one explanation for design choices that court them.

- 95% of users = $0 revenue
- Top 0.5% of users = ~63% of all IAP revenue
- Top spender ($208) out-spends ~120 median payers ($1.71 each)
- An incentive to design for the 0.5%, not the 99.5%

### Visualization (canvas `canvas4`, 420×340)

Extreme right-tailed histogram of IAP spend.

- **Title:** "IAP Spend Distribution (N=3000)".
- **Data:** 2850 samples at exactly 0; 150 payer samples from Pareto(alpha=1.1, xmin=1) via inverse CDF `1/(1−U)^(1/1.1)`, capped at 500.
- **Bins/axes:** 50 bins over x range 0-200; x labels formatted "$N"; x-axis label "Spend ($)".
- **Bars:** fill `rgba(231,76,60,0.5)`, border `#e74c3c`; standard smoothed density line + SE band overlay.

### Visualization (canvas `canvas4b`, 400×340)

Lorenz curve of revenue concentration over ALL users (zeros included).

- **Title (bold 13px, `#e74c3c`, top center):** "Lorenz Curve — Revenue Concentration".
- **Equality line:** dashed (5/4) `#bbb` 1.5px diagonal from bottom-left to top-right.
- **Lorenz curve:** computed from ALL 3000 sorted spends including the ~95% zeros (cumulative users vs cumulative revenue), so the curve stays flat at $0 until ~95% of users, then rockets — matching the "% of Users" x-axis label; stroked `#e74c3c` width 3; the area between the curve and the diagonal filled `rgba(231,76,60,0.2)` and labeled bold 14px `rgba(231,76,60,0.6)` "GINI GAP" with 11px `#555` "(extreme inequality)" beneath.
- **Annotations (computed from the simulated data, not hardcoded):** bold 11px `#c0392b` right-aligned near the top: "Top 0.5% of users = <computed>% of revenue" (≈63%); a `#c0392b` downward arrow pointing at the takeoff elbow where the curve leaves zero (~95% of users); white box outlined `#c0392b` 2px at lower left containing bold 10px `#c0392b` "Top whale ≈ <computed> median payers" (≈120) and 10px `#555` "Revenue concentrates in the 0.5%".
- **Axes:** L-shaped gray `#999` axes; padding top 38, right 20, bottom 45, left 50; captions 10px `#555`: "% of Users (sorted by spend)" below, rotated "% of Revenue" on the left, "0%" and "100%" at the x extremes.

## Subscription Fatigue (Survival Decay)

**Label:** SUBSCRIPTION DECAY (color `#8e44ad`)

Free trial cliff at month 1 (40% quit), then Weibull(k=0.8) — decreasing hazard means survivors get stickier over time. Most who pass month 3 are still subscribed at year 2. One explanation for the retention tactics concentrated in the cliff zone.

- Month 1: free trial cliff — 40% churn
- Months 2-6: evaluation zone — gradual decline
- Month 7+: habit formed — plateau
- Cancel button hidden, "are you sure?" x 5

### Visualization (canvas `canvas5`, 420×340)

Spike-plus-decay histogram of time until unsubscribe.

- **Title:** "Time Until Unsubscribe (Months, N=2000)".
- **Data:** 800 samples at Normal(1, 0.3) floored at 0.5 (the month-1 trial cliff, 40%); 1200 samples from Weibull(k=0.8, λ=8) via `8·(−ln(1−U))^(1/0.8)`, resampled until in (1.5, 24].
- **Bins/axes:** 35 bins over x range 0-24; x labels formatted "Nmo"; x-axis label "Months".
- **Bars:** fill `rgba(39,174,96,0.5)`, border `#27ae60`; standard smoothed density line + SE band overlay.

### Visualization (canvas `canvas5b`, 400×340)

Survival curve with three phase zones.

- **Title (bold 13px, `#27ae60`, top center):** "Survival Curve — Three Phases".
- **Survival function (over months 0-24, `#1a5276` width 3):** months 0-1: linear cliff `1 − 0.4m` (100% → 60%); months 1-6: `0.6·exp(−0.08·(m−1))`; months 7+: plateau `0.6·exp(−0.4)·exp(−0.01·(m−6))`.
- **Phase zones:** month 0-1 shaded `rgba(231,76,60,0.12)` labeled bold 10px `#e74c3c` "FREE TRIAL" / "CLIFF" + 9px "40% quit"; months 1-6 shaded `rgba(241,196,15,0.12)` labeled `#f39c12` "EVALUATION" + "gradual"; months 7-24 shaded `rgba(39,174,96,0.1)` labeled `#27ae60` "LOCKED IN" + "habit formed".
- **Annotation:** `#1a5276` 2px arrow from the curve at month 3 up-right to bold 9px two-line text "Survive month 3" / "→ most stay 2 yrs".
- **Dark-patterns box (white, `#e74c3c` 2px border, centered near the bottom):** bold 9px `#c0392b` "Dark patterns keep you past the cliff:" and 9px `#555` "cancel button hidden, \"are you sure?\" x 5".
- **Axes:** L-shaped gray `#999` axes; padding top 38, right 15, bottom 40, left 50; x labels 10px `#555`: "0", "6mo", "12mo", "24mo"; y labels "100%" (top) and "0%" (bottom).

## Regeneration instructions

- **Layout:** one `.obj-table` per section (full-width, border-collapse), each with a single `<tr>` of three `<td>`s: first 38% (text: `.pitfall-label` span, `<h3>` title, `<p>` paragraph, `<ul>` bullets), second 31% centered (histogram canvas), third 31% centered (insight canvas). Section order as above.
- **Page style:** body system sans-serif, margin 20px, background `#f9f9f9`, text `#333`; h1 `#1a5276` centered; `.subtitle` centered `#666` 0.95em; table cell borders `1px solid #2980b9`, padding 12px; h3 `#1a5276` 1.0em weight 700; paragraphs/bullets 14px, line-height 1.5-1.6; `.pitfall-label` inline-block bold 0.72em uppercase, letter-spacing 0.5px. No nav bar, no back/home links.
- **Pitfall label colors:** assigned by index from the cycling palette `["#795548","#2980b9","#27ae60","#e74c3c","#8e44ad","#e67e22","#16a085","#d35400","#c0392b","#1abc9c"]` via a small script that sets each `.pitfall-label`'s color.
- **Canvases:** intrinsic sizes as given (420×340 histograms, 400×340 insight charts), CSS `width: 100%; height: auto`; every canvas scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Shared histogram helper:** white background, centered bold 13px `#1a5276` title, gray `#999` L axes (margins 35/20/40/50), per-bin bars with 1px gap, Gaussian-smoothed (sigma 1.5 bins) density line `#1a5276` width 2 over a 95% SE band filled `rgba(230,126,34,0.22)`, 6 x-tick labels 11px `#555` with optional 12px `#333` x-axis label. Data generated with seeded mulberry32(42) RNG and Box-Muller normal sampler.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`/`#3498db`, green `#27ae60`/`#2ecc71`, red `#e74c3c`/`#c0392b`, orange `#e67e22`/`#f39c12`, purple `#8e44ad`, gray `#95a5a6`/`#7f8c8d`.
