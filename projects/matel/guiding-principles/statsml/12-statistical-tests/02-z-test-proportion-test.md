# z-test / Proportion test

**Page type:** detail page (card-sections: Overview two-column layout table with text 45% / canvas 55%, full-width Real-World Examples canvas + callout boxes, Quick Decision table)
**HTML title tag:** z-test / Proportion test — Statistical Tests Reference

**Subtitle:** Tests whether an observed proportion differs from a hypothesized value

Note: the source HTML has a top back link "← Statistical Tests Reference" to `../12-statistical-tests.html`; per project convention (no back/home links) it is not part of the regenerated page.

## Overview

**What it measures**

Whether an observed proportion differs significantly from a hypothesized value (or two proportions differ).

**Key assumptions**

- np ≥ 10 and n(1-p) ≥ 10 (normal approximation to binomial)
- Independent observations (no clustering)
- Fixed sample size (not sequential)
- Simple random sampling

**What breaks when violated**

- Small n: confidence intervals are too wide OR too narrow (asymmetric binomial)
- np < 10: normal approximation fails; actual coverage of CI drops below nominal
- Clustering: effective n is smaller than apparent n, inflating significance

**Failure box (monospace, red):**

Bucket purity test: 12 out of 15 positive (80%).
z-test CI: [0.60, 1.00] — upper bound is 1.002, just past the impossible >1.0 boundary
Exact binomial CI: [0.52, 0.96] — valid bounds
np = 15 * 0.5 = 7.5 < 10: normal approximation invalid.

**Alt-note box (green):**

**Use instead:** Exact binomial test (Fisher) when n < 30 or np < 10. Wilson score interval for confidence intervals at any n. Bayesian posterior (Beta-Binomial) for sequential updates.

### Visualization (canvas `c2`, 960×460)

Horizontal CI comparison chart: three sample-size scenarios, each showing z-test CI vs exact binomial CI on a 0–1 proportion scale.

- **Title (bold 13px `#1a5276`, top center):** "Confidence Intervals: z-test vs Exact Binomial"
- **Scale:** proportion axis from 0.0 to 1.0, tick labels every 0.2 (17px `#666` at y=40) with faint `#eee` vertical gridlines; scale spans x=140 to width−40.
- **Boundary line:** vertical dashed red (`#e74c3c`, dash 4/3, width 3) at p=1.0, labeled "max=1.0" (17px red).
- **Scenarios (rows of two stacked CI bars, 50px row height, 20px gap, starting y=50):**
  - `n=15, 12/15=80%` — z CI [0.598, 1.002] (clipped at 1.1), exact CI [0.52, 0.96], invalid (np < 10)
  - `n=50, 40/50=80%` — z CI [0.69, 0.91], exact CI [0.66, 0.90], valid (np >= 10)
  - `n=200, 160/200=80%` — z CI [0.74, 0.86], exact CI [0.74, 0.85], valid (np >= 10)
- **Row labels (right-aligned, 17px):** scenario label in `#1a5276`, plus "np >= 10" in green `#27ae60` or "np < 10" in red `#e74c3c` beneath.
- **z-test CI bar (upper, 10px tall):** fill `rgba(41,128,185,0.4)` / stroke `#2980b9` when valid; fill `rgba(231,76,60,0.4)` / stroke `#e74c3c` when invalid. For the invalid row, a red 14px annotation above the bar's right end: "upper = 1.002 > 1 (impossible)".
- **Exact binomial CI bar (lower, 10px tall):** fill `rgba(39,174,96,0.4)`, stroke `#27ae60`.
- **Point estimate:** filled `#1a5276` dot (radius 4) at p̂ = k/n between the two bars.
- **Legend (bottom left, 17px `#666`):** red swatch "z-test CI"; green swatch "Exact binomial CI".

## Real-World Examples

### Visualization (canvas `c2r`, 960×300)

Time-series line chart of observed crash rate over four hourly checkpoints, showing the z-test false alarm at small n.

- **Title (bold 16px `#1a5276`, top center):** "🚀 Crash Rate Monitoring: z-test Triggers False Alarm at Small n"
- **Data points:** `1hr (n=20)` rate 10.0% (2 crashes, z p=0.005, exact p=0.06); `2hr (n=55)` 3.6% (2 crashes, z p=0.19, exact p=0.30); `3hr (n=120)` 2.5% (3 crashes, z p=0.35, exact p=0.43); `4hr (n=200)` 2.5% (5 crashes, z p=0.31, exact p=0.37).
- **Axes:** plot x=100 to width−60, y=40 to height−50; light `#ddd` L axes; y scale 0–20% (rates plotted as rate/0.20 of plot height); x labels "1hr (n=20)" etc. 14px `#333` under each point, evenly spaced.
- **Reference lines (dashed 4/3, width 1.5):** green `#27ae60` at 2% labeled "Baseline: 2%" (14px, right of plot); red `#e74c3c` at 5% labeled "Rollback trigger".
- **Series:** connected `#1a5276` line, width 3; each point a filled dot radius 6 — red `#e74c3c` when z p<0.05 (only the 1hr point), otherwise blue `#1a5276`. Rate value label (e.g. "10.0%") 14px `#333` above each point.
- **Alarm annotations at the 1hr point:** bold red 14px "⚠ ROLLBACK" above; below the point in 13px: red "z: p=0.005" and green "exact: p=0.06".
- **Caption (14px `#666`, bottom center):** "By hour 4 the rate settled at 2.5% (normal) — unnecessary rollback avoided with exact test".

**Real-world box 1 (🚀 Feature launch: Crash rate monitoring):**

After shipping a mobile app update, the crash team sees 2 crashes out of 20 sessions in the first hour (10%). They run a z-test against the baseline rate of 2% — it says p=0.005 (one-sided), triggering a rollback. But np=20×0.02=0.4, far below the threshold of 10, so the z-test overstates the evidence. The exact binomial test gives p=0.06 with a Wilson CI of [3%, 30%]. The "crisis" was just small-sample noise — by hour 4 (n=200, 5 crashes) the rate settled at 2.5%, indistinguishable from baseline.

**Real-world box 2 (📊 Survey research: Voter preference polling):**

A political poll of 25 likely voters in a rural district shows 72% favor candidate A. The z-test CI is [54%, 90%]. But with np=18 and n(1-p)=7 < 10, the normal approximation is invalid for the minority. Wilson score CI: [52%, 86%] — note the asymmetry that z-test misses. The 2-point difference at the lower bound means the race could actually be competitive, which the symmetric z-interval obscures.

## Quick Decision

| Data Situation | If Assumptions Met | If Violated | Universal Fallback |
|---|---|---|---|
| One proportion vs hypothesized | z-test for proportions | np < 10: CI exceeds [0,1] | Exact binomial / Wilson score |

## Regeneration instructions

- **Layout:** h1 + `.subtitle`, then three `.card-section` blocks each with an h2 (1.3rem `#1a5276`, 2px `#2980b9` bottom border): "Overview" (a `table.layout` row: `td.text-col` 45% with `.obj-title` headings, bullets, `.failure`, `.alt-note`; `td.viz-col` 55% with canvas `c2`), "Real-World Examples" (full-width canvas `c2r` followed by two `.real-world` boxes), "Quick Decision" (`.decision-table`).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px solid `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` `#1a5276`; `code` on `#e8f0f8`; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius.
- **Callout boxes:** `.failure` — background `#fdedec`, left border 3px `#e74c3c`, monospace ('SF Mono'/'Fira Code'), color `#922`, 0.85rem. `.alt-note` — background `#eafaf1`, left border 3px `#27ae60`, color `#1a5276`, 0.85rem. `.real-world` — background `#fef9e7`, left border 4px `#e67e22`, 0.88rem; `.domain` heading weight 600 `#7d6608`; `strong` inside `#e67e22`.
- **Decision table:** `.decision-table` — th background `#1a5276` white text; td 1px `#e0e0e0` border; even rows `#fafcfe`; column 3 text `#e74c3c`, column 4 text `#27ae60` weight 500.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, accent blue `#2980b9`.
- **Canvas:** intrinsic `width`/`height` attributes as given; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper. No nav bar, no back/home links.
