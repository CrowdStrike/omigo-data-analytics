# Insurance Domain: Data Pitfalls

**Page type:** detail page (one h2 per pitfall, each with a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** Insurance Domain - Data Pitfalls in Statistical ML

**Subtitle:** Selection bias, long-tail claim development, correlated catastrophes, and tail risk break the standard statistical assumptions behind insurance models.

## Adverse Selection

**Obj-title:** Adverse Selection

- People who buy insurance are systematically riskier than the general population
- The insured pool is NOT a random sample - it's self-selected
- Models trained on general population data will underestimate risk for actual policyholders
- Pricing models must account for this selection bias or face losses
- Information asymmetry: applicants know their own risk better than the insurer

**Example:** A life insurer uses population mortality tables to price policies. But healthy people are less likely to buy coverage - the actual policyholder pool has 15-30% higher mortality than the general population at the same age.

### Visualization (canvas `canvas1`, 720×240)

Two overlapping gaussian density curves showing the insured pool shifted toward higher risk.

- **Curves:** gaussian densities over x from 0.5 to 10.5 (201 points); "General Population" mean 4, std 1.2 — stroke green `#27ae60` width 2.5, fill `rgba(39,174,96,0.15)`; "Insured Population" mean 6, std 1.3 — stroke dark red `#c0392b` width 2.5, fill `rgba(192,57,43,0.15)`. Density scaled by ×450 above a baseline at y=200.
- **Axes:** L-shaped axes `#333` (x from 60 to 680 at y=200, y up to 20); x-axis label "Risk Level" centered, rotated y-axis label "Density".
- **Curve labels (17px):** "General Population" in green at upper left (~100, 45); "Insured Population" in dark red at upper right (~420, 45).
- **Shift annotation:** rightward arrow `#333` from (300, 75) to (400, 75), labeled above in `#555` 13px: "Selection Bias".

## Long-Tail Claims

**Obj-title:** Long-Tail Claims

- Claims can take 5-10+ years to fully develop and settle
- Initial reserve estimate is often wildly inaccurate for complex claims
- Reserve must be updated as information emerges - creating moving targets
- Predicting ultimate cost at filing time is the hardest problem in insurance
- Inflation, legal changes, and medical advances change claim trajectories

**Example:** A workers' comp claim for a back injury is filed with a $50K initial reserve. Over 8 years, surgery, rehabilitation, legal fees, and permanent disability push the ultimate cost to $1.2M - a 24x development factor.

### Visualization (canvas `canvas2`, 720×240)

Area/line chart of a single claim's reserve growing over 8 years vs the flat initial estimate.

- **Data:** x labels `['Filed','Yr 1','Yr 2','Yr 3','Yr 4','Yr 5','Yr 6','Yr 7','Settled']`; cumulative reserve ($K) `[50, 120, 280, 450, 620, 780, 950, 1100, 1200]`.
- **Series:** reserve line dark red `#c0392b` width 3 with 4px dots, shaded area under it `rgba(231,76,60,0.2)`; horizontal dashed green `#27ae60` line (dash 5/5, width 2) at $50K for the initial estimate.
- **Axes:** y-axis $0K–$1200K with labels every $300K and `#eee` gridlines; x labels under each point.
- **Legend (17px, upper left area):** dark red "Actual Reserve Development"; green "Initial Estimate ($50K)".
- **Annotation (`#555` 14px, right side):** "24x development factor".

## Catastrophic Correlation

**Obj-title:** Catastrophic Correlation

- Insurance assumes claims are largely independent - catastrophes violate this
- A single event (hurricane, earthquake) triggers thousands of correlated claims simultaneously
- Normal-year data cannot predict catastrophe-year losses
- Portfolio diversification fails when correlation spikes to 1.0

**Example:** Hurricane Andrew (1992) caused $27B in insured losses. Models based on prior decades predicted max annual hurricane loss of $8B. Eleven insurers went bankrupt - their "independent" homeowner policies were 100% correlated in a Cat-5 event.

### Visualization (canvas `canvas3`, 720×240)

Monthly claims bar chart: 24 months of normal noise with one massive catastrophe spike.

- **Data (24 monthly values, $M):** `[45, 52, 38, 61, 48, 55, 42, 67, 51, 44, 58, 47, 53, 41, 62, 49, 56, 43, 850, 120, 65, 48, 52, 44]`; y scale max 900.
- **Bars:** width 22, spacing 26; month 19 (value 850) in dark red `#c0392b`, month 20 (value 120, aftermath) in orange `#e67e22`, all others blue `#3498db`.
- **Reference:** horizontal dashed green `#27ae60` line (dash 5/5, width 2) at 50, labeled in green 13px above it at the left: "Expected claims level".
- **Annotations:** centered dark red 17px label "Hurricane Event" above the spike with a short dashed leader line; x-axis label "Months" centered; y-axis label "Claims ($M)" top left.

## IBNR (Incurred But Not Reported)

**Obj-title:** IBNR (Incurred But Not Reported)

- Claims that have occurred but haven't been reported yet create hidden liability
- Recent periods ALWAYS appear to have fewer claims - they haven't all been reported yet
- Development triangles show how each accident period "fills in" over time
- Naive trend analysis on raw data shows false improvement in recent periods
- IBNR reserves can be 20-40% of total reserves for long-tail lines

**Example:** A liability insurer sees reported claims for 2024 are 30% below 2023. Leadership celebrates improvement. But actuaries know: at 12 months of development, only 60% of ultimate claims have been reported. The true 2024 number will likely exceed 2023 once fully developed.

### Visualization (canvas `canvas4`, 720×240)

Development-triangle line chart: cumulative percent of ultimate claims reported per accident year, with recent years cut off earlier.

- **Chart title (17px `#1a5276`, top left area):** "Claims Development by Accident Year".
- **Data (cumulative % reported at development months 12/24/36/48/60/72/84/96, one line per accident year, blues from dark to light):**
  - 2019: `[0.45, 0.62, 0.74, 0.83, 0.90, 0.95, 0.98, 1.00]` — `#1a5276`.
  - 2020: `[0.43, 0.60, 0.72, 0.81, 0.89, 0.94, 0.97]` — `#2471a3`.
  - 2021: `[0.44, 0.61, 0.73, 0.82, 0.88, 0.93]` — `#2e86c1`.
  - 2022: `[0.42, 0.59, 0.71, 0.80, 0.87]` — `#3498db`.
  - 2023: `[0.44, 0.60, 0.72, 0.81]` — `#5dade2`.
  - 2024: `[0.41, 0.58, 0.70]` — `#85c1e9`.
- **Series style:** line width 2.5, 4px end-point dot with the accident-year label in the line color to its right.
- **Axes:** y-axis 0%–100% with labels every 25% and `#eee` gridlines; x-axis labels "12mo" through "96mo" spaced 70px apart.
- **Annotation (dark red `#c0392b` 13px):** "Recent years appear lower - IBNR not yet reported!" with a short dark red arrow pointing up-left toward the gap between short and long lines.

## Rare Events Matter Most

**Obj-title:** Rare Events Matter Most

- Insurance is fundamentally about the tail of the distribution - not the mean
- A 1-in-100-year event cannot be reliably estimated from 30 years of data
- The largest single loss often exceeds the sum of all other losses in a year
- Standard statistical methods assume enough data in the tails - insurance violates this
- Extreme Value Theory needed but requires assumptions that may not hold

**Example:** A flood insurer has 30 years of loss data for a river basin. The worst observed flood caused $500M in losses. But geological evidence suggests a 1-in-200-year flood would cause $4B. You literally cannot estimate this from your data window.

### Visualization (canvas `canvas5`, 720×240)

Return-period plot: 30 years of observed flood losses with a dashed extrapolation into the unobserved tail.

- **Observed data (sorted annual max losses, $M):** `[12, 18, 22, 28, 35, 38, 42, 45, 48, 52, 55, 58, 62, 65, 70, 75, 80, 85, 92, 100, 110, 125, 140, 160, 185, 210, 250, 300, 380, 500]`; plotted as 4px blue `#2980b9` dots using the Weibull plotting position rp = (n+1)/(n−i) on a piecewise log-like x scale.
- **X-axis:** return-period ticks `1yr, 2yr, 5yr, 10yr, 20yr, 50yr, 100yr, 200yr` at x positions `[70, 140, 240, 330, 420, 530, 600, 660]`, with vertical `#eee` gridlines; axis caption "Return Period" bottom center.
- **Y-axis:** labeled "$0" at bottom, "$4.5B" at top (scale max 4500).
- **Fitted line:** solid blue `#2980b9` width 2 through the observed range (from loss 12 at 1yr to loss 500 at ~50yr position).
- **Extrapolation:** dashed dark red `#c0392b` line (dash 6/4, width 2) continuing from 500 at the 50yr boundary to 4000 at 200yr, with a widening translucent confidence band `rgba(192,57,43,0.1)` in the extrapolation zone.
- **Data boundary:** vertical dashed orange `#e67e22` line (dash 4/4, width 2) at the 30-year data limit (x≈540), captioned above in orange 13px: "Data ends here".
- **Labels (17px):** blue "Observed (30 years)" top left; dark red "Extrapolated (huge uncertainty)" upper middle-right.

## Regeneration instructions

- **Layout:** standard detail-page structure: h1, `.subtitle` paragraph, then per pitfall an `<h2>` (1.4em `#1a5276`, 2px solid `#2980b9` bottom border) followed by a `.obj-table` with a single `<tr>`: left `<td>` (40%) contains `.obj-title` + `<ul>` bullets + `.example` callout, right `<td>` (60%, centered) contains the canvas. Even table rows have background `#fafcfe`.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em; bullets 0.9em `#333`; `strong` in `#1a5276`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`. No nav bar, no back/home links.
- **Example callout:** on this page `.example` is styled differently from the other domain pages — background `#eaf2f8`, padding 10px, border-radius 5px, italic, 0.9em (no left border). (A `.philosophy` class also exists in the stylesheet but is unused on this page.)
- **Canvas:** all canvases 720×240, declared with intrinsic `width`/`height` attributes and scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates), one IIFE per chart. Chart headline text uses a 17px -apple-system font.
- **Palette:** primary blue `#1a5276`, chart blues `#2980b9`/`#3498db` (plus development-triangle ramp `#2471a3`, `#2e86c1`, `#5dade2`, `#85c1e9`), green `#27ae60`, red `#e74c3c` (area fill), dark red `#c0392b`, orange `#e67e22`, text `#333`/`#555`/`#666`.
