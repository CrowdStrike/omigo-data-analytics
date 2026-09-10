# Pitfall: Outlier Handling Before Understanding

**Page type:** detail page (card-section layout: one h2 section per block, two-column table text left 45% / canvas right 55%)
**HTML title tag:** Outlier Handling Before Understanding

**Subtitle:** Removing outliers because they're 'weird' before asking why.

## Section 1: The Problem

**Tags:** `the trap` (red pill), `outliers` (blue pill)

- **Blind cleaning** — z-score > 3 or IQR filters remove points labeled weird without asking why
- **Not noise** — outliers are often the most informative cases, not errors to be scrubbed
- **High-value customers** — big spenders sit in the tail; removed, the model cannot predict them
- **Fraud and failures** — fraudulent transactions and failing sensors are outliers by definition
- **Medical crises** — emergencies produce outlier vitals, exactly the cases a severity model needs

*Example:* A churn model drops users with usage 3 std above the mean — the power users who churned — and misses 80% of high-value churn.

**Impact:** Outliers are often the positive class, so "cleaning" them trains the model on sanitized data that fails on real-world extremes.

### Visualization (canvas `c1`, 720×300)

Two scatter strips (before/after) showing outlier removal deleting the positive class.

- **Title (bold 14px, `#1a5276`, top center):** "Outlier Removal Destroys Signal".
- **Data (seeded, deterministic):** a 200-point main population and a 50-point outlier population, jitter drawn from a seeded Park-Miller LCG (`lcg(20250915)`). A main point is a positive when its index mod 40 equals 17 (5 positives); the first 45 of the 50 outliers are positives. Total positives = 50, of which 45 (90%) sit beyond the threshold.
- **"Before Outlier Removal:" strip** (bold 11px blue label at left, x=100, y=60): the 200 main dots spread over the left 75% of a 550px-wide band above baseline y=200 (heights up to 72px = jitter × 0.6 × 120); then the 50 outlier dots in the rightmost 25% of the band (heights up to 120px). Positives are red `rgba(231,76,60,0.7)` at 2.5px radius, negatives blue `rgba(52,152,219,0.4)` at 2px.
- **Threshold line:** vertical dashed orange `#e67e22` (dash 6/4, width 2) at 75% of the band width, labeled "Outlier" / "threshold" in 10px orange above; counts to the right are computed from the arrays — red "45 positives", blue `#3498db` "5 negatives".
- **"After Outlier Removal:" strip** (bold 11px blue label at y=240): the same 200 main dots only, spread over 90% of the band above baseline y=280 (heights up to 48px) — the outlier region is gone, so the 5 in-range positives remain visible in red.
- **Caption (bold 12px red `#e74c3c`, centered at y=260):** "Removed 50 outliers. Lost 90% of positive class (45 of 50)!" — the count, percentage, and numerator/denominator are all computed from the plotted points at render time.

## Section 2: Why It Happens

**Tags:** `root cause` (orange pill), `imbalance` (blue pill)

- **Taught as routine** — courses present outlier removal as a standard preprocessing step
- **One-line tools** — z-score and IQR filters apply in one line of code, so removal feels free
- **Wrong assumption** — z-score tests assume Gaussian data; real distributions rarely comply
- **Two kinds of extreme** — automated rules cannot tell entry errors from valid rare events
- **Imbalance overlap** — minority-class points look outlier-like, so filters delete the positives
- **Convenient win** — removal shrinks linear-model variance, though trees split around extremes

*Example:* Incomes above $500K are removed as outliers (z > 4), yet those earners default at 8% versus the 2% baseline.

**Root Cause:** The filtered points are only 2% of rows but carry a 34% positive rate versus 4% elsewhere — removal deletes 8.5x-concentrated signal.

### Visualization (canvas `c2`, 720×300)

Two-bar comparison of positive rate in non-outliers vs outliers.

- **Title (bold 14px, `#1a5276`, top center):** "Positive Rate: Outliers vs. Non-Outliers".
- **Inputs (all labels derived from these):** non-outliers n = 7,350 at a 4% positive rate; outliers n = 150 at a 34% positive rate. Total = 7,500, so the shares are exactly 98% and 2%, and positives are 294 and 51.
- **Bars:** 140px wide, baseline y=240, bar height = rate ÷ 0.40 × 150 (so a 40% axis maximum fills the 150px plot height and the 34% bar stays on canvas).
  - Left bar centered at x=200: fill `rgba(52,152,219,0.6)`, stroke `#3498db` width 2; bold 16px blue value "4%" above; labels below: "Non-Outliers" (12px `#333`), "n = 7,350 (98%)" and "positives = 294" (10px `#666`).
  - Right bar centered at x=520: fill `rgba(231,76,60,0.6)`, stroke `#e74c3c` width 2; bold 16px red value "34%"; labels: "Outliers", "n = 150 (2%)", "positives = 51".
- **Baseline:** dashed gray `#999` line (dash 4/3) from x=100 to x=650 at y=240.
- **Caption (bold 12px red `#e74c3c`, bottom center):** "Outliers are 8.5x more likely to be positive. They ARE the signal!" — the multiple is computed as 34% ÷ 4% at render time.

## Section 3: The Correct Approach

**Tags:** `the fix` (green pill), `robust methods` (blue pill)

- **Inspect first** — plot and examine outliers to separate errors from real extremes
- **Check the target** — compare positive rates in outliers vs the rest; a higher rate means signal
- **Split by cause** — remove confirmed errors, keep valid extremes as a distinct segment
- **Winsorize or transform** — cap at a percentile or compress the range with log or sqrt
- **Tolerant models** — tree methods like XGBoost split around extremes instead of distorting
- **Surgical removal** — if you must remove, do it per feature; never delete whole rows

*Example:* Transactions above $50K are 8% of rows but 35% of revenue with a 12% vs 6% positive rate, so they are winsorized at the 99th percentile, not removed.

**Fix:** Investigate outliers before removing; if they predict the target, keep them and handle with winsorization, transforms, or tree models.

### Visualization (canvas `c3`, 720×300)

Flowchart: outlier handling decision tree.

- **Title (bold 14px, `#1a5276`, top center):** "Outlier Handling Decision Tree".
- **Start box:** white 140×40 rectangle centered at top (x = w/2−70, y=50), stroke `#2980b9` width 2, blue 11px text "Outliers detected"; blue arrow down to the first decision.
- **Decision 1 box:** white 200×40 rectangle centered at y=110, stroke `#e67e22` width 2; bold orange 11px "Are they data errors?" with 10px line "(typos, sensor failures)".
- **Left branch (Yes):** red `#e74c3c` connector from the decision's left edge down to a white 140×50 box at (80, 170) stroked red; bold red 10px "Yes: Errors" with lines "Remove or" / "correct them". Small red 9px "Yes" label on the connector.
- **Right branch (No):** green `#27ae60` connector to **Decision 2 box** — white 180×40 rectangle at (480, 160), stroke `#e67e22`; bold orange 10px "Higher target rate" / "than non-outliers?". Small green 9px "No" label on the connector.
- **Decision 2 Yes:** green connector down to a white 160×50 box at (490, 230), stroke `#27ae60`; bold green 10px "Yes: KEEP THEM!" with 9px lines "Use tree models or" / "winsorize, don't remove". Small green 9px "Yes" label.
- **Caption (bold 11px `#1a5276`, bottom center):** "Don't remove outliers by default. Understand them first."

## Regeneration instructions

- **Layout:** three `.card-section` blocks, each with an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` with one row: left `td.text-col` (45%) holding `.tags` pills, `<ul>` bullets, `.example` italic paragraph, and `.key-point` callout; right `td.viz-col` (55%) holding the canvas.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 0.95rem; `li b` in `#1a5276`; bullets 0.92rem.
- **Tag pills:** inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px — blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem.
- **Canvas:** intrinsic 720×300, CSS `width: 100%`, 1px `#e0e0e0` border with 4px radius; scale via a shared `setup(id)` helper using `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Randomness:** never `Math.random()`. Scatter jitter comes from a seeded Park-Miller LCG helper, `function lcg(seed) { var s = seed; return function () { s = (s * 16807) % 2147483647; return s / 2147483647; }; }`, instantiated per chart with a fixed seed (c1 uses 20250915) so the figure and every label it prints are identical on each load. Charts c2 and c3 use no random data.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, scatter blue `#3498db`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, grays `#666`/`#444`/`#333`. No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
