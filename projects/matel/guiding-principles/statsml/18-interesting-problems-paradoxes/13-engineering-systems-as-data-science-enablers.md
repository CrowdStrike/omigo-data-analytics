# Engineering Systems as Data Science Enablers — the Storage Layer Decides the Questions

**Page type:** detail page (two-column obj-table layout: text left 50%, canvas right 50%, one h2 + table per aspect, plus philosophy callouts and a summary table)
**HTML title tag:** Engineering Systems as Data Science Enablers — the Storage Layer Decides the Questions

**Subtitle:** Almost every method a modern analyst uses predates the modern analyst. What changed was not inference — it was reach. Treat the pipeline as the thing that fixes the question space, and the last twenty years reads differently.

## Callout (philosophy box, top)

**The question:** If the statistics were already invented, what exactly happened? Logistic regression, the bootstrap, and gradient boosting were all published before the phrase "data science" was in common use. So why did the field arrive when it did?

**The answer:** The storage and serving layer moved, not the mathematics. A sorted, distributed key-value store made a full scan affordable; a columnar layout made a full scan cheap; and affordability is what makes a question *askable*. That reframes infrastructure design as a quietly epistemic act — the schema, the grain, and the retention window decide which hypotheses exist before an analyst is hired.

## 1. The Claim, Stated Precisely

**Obj-title:** Method Was Never the Bottleneck

The inference toolkit was largely complete decades before it was widely used. Logistic regression as a fitted model dates to the 1950s, the bootstrap to 1979, boosting to the mid-1990s, and gradient boosting to around 2001. None of them waited on a theorem. They waited on a machine that could read the rows.

Math-box:

**Two timelines, same axis (published dates):**

Methods: `1958` logistic regression · `1967` k-means · `1977` EM · `1979` bootstrap · `1984` CART · `1986` backpropagation · `1995` AdaBoost · `1996` lasso · `2001` random forests

Systems: `1970` relational model · `1996` LSM-tree · `2004` MapReduce · `2006` distributed sorted key-value store · `2010` interactive columnar query · `2013` open columnar file formats · `2015` storage/compute separation

Median method year is `1984`; median system year is `2006`. The gap is `22 years` — a generation in which the answer was computable in principle and unaffordable in practice. Both medians are computed in the chart from the plotted points.

- **Not a math shortage:** the estimator for a click-through model existed long before the click log was storable
- **What a sorted store buys:** keys adjacent in value land adjacent on disk, so a range is one sequential read
- **What a columnar layout buys:** a 3-column query touches 3 columns, not 300, so cost tracks the question's width
- **Append-only structures:** the LSM-tree traded read amplification for write throughput, which is what event logging needs
- **Compute you can rent:** separating storage from compute made an expensive scan a budget line, not a capacity plan
- **The honest reading:** methods got better too, but the discontinuity in *what got analyzed* lines up with the systems track

### Visualization (canvas `canvas1`, 720×360)

Two-track timeline: statistical/ML methods on the upper track, storage and serving systems on the lower track, with computed medians marked.

- **Layout:** x axis from year 1955 to 2020, plot area x from 70 to 690. Upper track baseline y = 130, lower track baseline y = 250.
- **Data (hardcoded literal arrays):**
  - Methods: `[[1958,'logistic regression'],[1967,'k-means'],[1977,'EM'],[1979,'bootstrap'],[1984,'CART'],[1986,'backprop'],[1995,'AdaBoost'],[1996,'lasso'],[2001,'random forests']]`
  - Systems: `[[1970,'relational model'],[1996,'LSM-tree'],[2004,'MapReduce'],[2006,'sorted distributed KV'],[2010,'interactive columnar'],[2013,'columnar file formats'],[2015,'storage/compute split']]`
- **Tracks:** two horizontal lines `#ccc` width 1.5 across the full plot width at each baseline.
- **Markers:** methods = filled circles radius 5 in `#1a5276`; systems = filled circles radius 5 in `#27ae60`.
- **Marker labels:** 10px, alternating above/below its own track (odd index offset further) to avoid collision; methods labels `#1a5276`, systems labels `#27ae60`, rotated 0°, `textAlign` center.
- **Track titles (bold 12px):** blue "Statistical / ML methods" at x=70 y=104; green "Storage & serving systems" at x=70 y=224.
- **Medians:** compute the median year of each array in JS at render time. Draw a dashed (5/5) vertical line per median spanning its own track ±26px — blue for methods, green for systems — and print the computed year as a bold 12px label above each line.
- **Gap annotation:** orange `#e67e22` horizontal double-headed segment at y = 190 between the two median x positions, with a bold 12px orange centered label printing the computed gap, e.g. `"22-year gap"` — the integer must come from the computed medians, not a literal.
- **X ticks:** 1960, 1970, 1980, 1990, 2000, 2010, 2020 in `#666` 11px at y = 312, with light `#eee` vertical gridlines from y=90 to y=300.
- **Axis label:** "Year first published" centered at y = 338, `#1a5276` 13px.
- **Title (bold 14px `#1a5276`, top center):** "The Methods Are Old — the Access Is New".

## 2. Latency as an Epistemic Boundary

**Obj-title:** Your Hypothesis Budget Is a Division

A query that takes a week gets asked once, and it gets asked about the thing the analyst was already fairly sure of. A query that takes a second gets asked a thousand times, most of them stupidly, and that is exactly where findings come from — iteration, not the first draft. So latency is not a convenience metric. It sets a hard ceiling on the number of hypotheses a person can physically test.

Math-box:

**Illustrative Example — one analyst-week of query time:** `40 h = 144,000 s`

Hypotheses testable `≈ time budget ÷ query latency`

| Storage tier | Latency | Hypotheses / week |
|---|---|---|
| Nightly batch scan of raw logs | `14,400 s` (4 h) | `10` |
| Distributed row-oriented scan | `600 s` (10 min) | `240` |
| Sorted key-value store, range scan | `20 s` | `7,200` |
| Columnar + cached aggregates | `2 s` | `72,000` |

Top to bottom the latency improves `7,200x`, and the hypothesis budget improves by exactly the same `7,200x` — `10 → 72,000`. Every figure in the table is `144,000 ÷ latency`, computed at render time. The latency values are illustrative tier stand-ins, not measurements of any product.

- **Ten questions is a hypothesis, not a search:** at 4 hours a query you spend your week confirming a prior
- **The first draft is usually wrong:** a real finding is the tenth version of a query, so version count is the asset
- **Interactive is a threshold, not a gradient:** below roughly ten seconds the analyst stops context-switching away
- **Exploration needs to be cheap enough to waste:** most queries must be allowed to return nothing useful
- **Latency compounds with breadth:** cheap scans also widen each query, so you test bigger hypotheses too
- **The uncomfortable corollary:** budget scales with speed whether or not the hypotheses deserve testing

### Visualization (canvas `canvas2`, 720×360)

Horizontal bar chart on a log x-axis: hypotheses testable per analyst-week for each storage tier, values computed as 144000 / latency.

- **Layout:** plot x from 250 to 660, four bars height 34 with 22px gaps, first bar top y = 70.
- **Data (hardcoded literal array of latencies in seconds):** `[14400, 600, 20, 2]` with tier labels `['Nightly batch scan','Distributed row scan','Sorted KV range scan','Columnar + cache']` and latency captions `['4 h','10 min','20 s','2 s']`.
- **Values:** `n = 144000 / latency` computed in JS → 10, 240, 7200, 72000. Never hardcode these.
- **Scale:** log10 from 1 to 100000 mapped across the plot width.
- **Bars:** fill `rgba(26,82,118,0.35)`, stroke `#1a5276` width 1.5.
- **Row labels:** tier name bold 12px `#1a5276` right-aligned at x=240; latency caption 11px `#666` on the line below.
- **Value labels:** bold 12px `#1a5276` just right of each bar end, printing the computed count with thousands separators via `toLocaleString()`.
- **Gridlines:** at 1, 10, 100, 1,000, 10,000, 100,000 — `#eee` verticals, `#666` 10px labels at y = 300.
- **Speedup annotation:** orange `#e67e22` dashed (4/4) bracket linking the first and last bar ends, with a bold 12px orange label printing the computed ratio (last count ÷ first count) as `"7,200x more hypotheses"` — computed, not literal.
- **Axis label:** "Hypotheses testable per analyst-week (log scale)" centered at y = 330, `#1a5276` 13px.
- **Footnote (10px `#999`, bottom left):** "Illustrative Example — tier latencies are stand-ins, not measured products."
- **Title (bold 14px `#1a5276`, top center):** "Time Budget ÷ Latency = Questions You Can Actually Ask".

## 3. What the Schema Forbids

**Obj-title:** The Question Space Is Fixed Before You Arrive

This is the strongest form of the claim. If events are written as one row per user per day, no question about within-day ordering can ever be answered — not by a better model, not by a better statistician, not ever. The information is not noisy, it is absent. The engineer who chose the grain closed off a region of the hypothesis space, usually to save storage, usually without knowing.

Math-box:

**Illustrative Example — grain destroys resolution, retention truncates horizons.**

A day rolled up to one row collapses `1,440` minute-level slots into `1`. Ordering, gaps, and session structure are unrecoverable from the aggregate — there is no inverse.

Of five common questions, a raw event log answers `5/5` (`100%`), an hourly rollup `2/5` (`40%`), a daily rollup `1/5` (`20%`). These fractions are computed in the chart from the plotted matrix.

**Retention is a second hard bound.** With a `90-day` window, a 6-month (`183-day`) cohort curve is only `90 ÷ 183 = 49%` observable. The remaining 51% is not underpowered — it does not exist.

- **Aggregation is lossy and irreversible:** a sum has no inverse, so within-bucket questions die at write time
- **The grain sets the finest testable unit:** daily rows cannot support a claim about the first five minutes
- **Retention bounds every longitudinal claim:** the longest measurable horizon equals the window, full stop
- **Absence looks like a modelling problem:** analysts spend months on questions the schema already answered "no"
- **The decision is made by cost:** grain and retention are usually chosen from a storage bill, not a research plan
- **Backfill does not rescue you:** you can widen the schema going forward, never for the months already rolled up

### Visualization (canvas `canvas3`, 720×360)

Matrix chart: five question types (rows) against three storage grains (columns), with green checks and red crosses, plus a computed answerable-fraction footer per column.

- **Layout:** row label column x from 20 to 330; three grain columns of width 110 starting at x = 340. Header row baseline y = 84; five data rows of height 40 starting y = 100.
- **Rows (hardcoded literal, `[question, rawOK, hourlyOK, dailyOK]`):**
  - `['Daily active users', true, true, true]`
  - `['Peak hour of the day', true, true, false]`
  - `['Did the click precede the purchase?', true, false, false]`
  - `['Seconds between two events', true, false, false]`
  - `['Sessions split on a 30-min idle gap', true, false, false]`
- **Columns:** `['Raw event log','Hourly rollup','Daily rollup']`, header bold 12px `#1a5276` centered, with a light `#f0f4f8` header band.
- **Cell marks:** green `#27ae60` check glyph drawn as two strokes (width 2.5) when answerable; red `#e74c3c` cross (two strokes, width 2.5) when not. Cell centered, arm length 7px.
- **Row separators:** `#e0e0e0` horizontal lines width 1 between rows; column separators `#e0e0e0` verticals.
- **Row labels:** 12px `#333`, left-aligned at x = 24.
- **Footer per column:** compute `answerable / 5` in JS and print bold 12px — green if the fraction is 1, orange `#e67e22` otherwise — as e.g. `"5/5 · 100%"`, `"2/5 · 40%"`, `"1/5 · 20%"` at y = 322. Values must be derived from the matrix.
- **Footnote (10px `#999`, bottom left at y = 348):** "Illustrative Example — a representative question set, not an exhaustive one."
- **Title (bold 14px `#1a5276`, top center):** "The Grain Decides Which Questions Are Even Expressible".

## 4. Sampling Decided by Engineering

**Obj-title:** A Buffer Size Became a Sampling Frame

A log that drops events when its buffer fills has a sampling mechanism, and that mechanism is correlated with load — which is precisely the condition you most wanted data about. Nobody designed this sampling frame. It emerged from a queue depth chosen for memory headroom. The result is a dataset that is thinnest exactly where the interesting behaviour lives.

Math-box:

**Illustrative Example — a logger draining `10,000 events/s`.**

| Period | Hours | Arrival rate | Retained | True error rate |
|---|---|---|---|---|
| Normal | `20 h` | `4,000/s` | `100%` | `0.5%` |
| Peak | `4 h` | `25,000/s` | `40%` | `5.0%` |

Events: normal `4,000 × 20 × 3,600 = 288,000,000`; peak `25,000 × 4 × 3,600 = 360,000,000`.

True rate `= (288M × 0.005 + 360M × 0.05) ÷ 648M = 19.44M ÷ 648M = 3.0%`

Logged rate `= (1.44M + 0.4 × 18M) ÷ (288M + 144M) = 8.64M ÷ 432M = 2.0%`

The dashboard reports `2.0%` against a truth of `3.0%` — it understates by a third, and it does so worst during the incident. All four quantities are computed in the chart from the two literal rows.

- **Load-correlated loss is the worst kind:** the missingness depends on the very variable under study
- **It is not missing at random:** dropped rows carry a higher error rate, so the mean shifts, not just the variance
- **The bias is directional and predictable:** healthy traffic is over-represented, so every rate looks better
- **Nobody wrote it down:** the sampling probability is a side effect of buffer depth and drain rate
- **Reweighting needs the drop count:** recoverable only if the logger records what it discarded, and most do not
- **Same shape, other causes:** client-side ad blockers, timeouts, and retry storms all sample on the outcome

### Visualization (canvas `canvas4`, 720×360)

Two-panel chart: left panel bars for arrival rate versus logger capacity with the dropped fraction shaded; right panel two bars comparing computed true rate and computed logged rate.

- **Layout:** left panel x from 70 to 380, right panel x from 470 to 660. Shared baseline y = 280, plot height 200.
- **Data (hardcoded literal rows):** `[{name:'Normal', hours:20, rate:4000, err:0.005},{name:'Peak', hours:4, rate:25000, err:0.05}]`, capacity `10000`.
- **Left panel:** one bar per period, width 90, value = arrival rate, y-scale 0 to 26,000. Retained portion (up to `min(rate, capacity)`) filled `rgba(26,82,118,0.35)` with `#1a5276` stroke; the dropped portion above capacity filled `rgba(231,76,60,0.15)` with a dashed (4/4) `#e74c3c` outline.
- **Capacity line:** solid green `#27ae60` width 2 horizontal at 10,000 across the left panel, labeled bold 11px green "logger capacity 10,000/s" above its right end.
- **Left labels:** period name bold 12px `#1a5276` under each bar; arrival rate 11px `#666` below that; retained percentage computed in JS (`min(rate,capacity)/rate`) printed bold 11px orange `#e67e22` inside the bar near its base — expect 100% and 40%.
- **Right panel:** two bars width 60. Bar A = computed true error rate, stroke/fill green `#27ae60` at 0.35 alpha; bar B = computed logged error rate, stroke/fill red `#e74c3c` at 0.35 alpha. Y-scale 0 to 4% shared, drawn from the same baseline.
- **Right labels:** bold 12px value labels above each bar printing the computed percentages to one decimal (`3.0%`, `2.0%`); 11px `#666` captions "truth" and "as logged" below each bar.
- **Understatement annotation:** orange `#e67e22` bold 12px, two lines to the right of the panel or under it, printing the computed shortfall as a percentage of truth, e.g. `"understates by 33%"` — computed as `(true − logged)/true`.
- **Panel headings (bold 12px `#1a5276`):** "Arrival rate vs capacity" over the left panel, "Error rate estimate" over the right.
- **Y gridlines:** left panel at 0, 10,000, 20,000 in `#eee` with `#666` 10px labels; right panel at 0%, 2%, 4%.
- **Footnote (10px `#999`, bottom left):** "Illustrative Example — constructed rates chosen to make the bias arithmetic exact."
- **Title (bold 14px `#1a5276`, top center):** "Drops Under Load Are a Sampling Design Nobody Chose".

## 5. The Counter-Argument, Taken Seriously

**Obj-title:** Necessary, Not Sufficient — and It Ships a Debt

Infrastructure is a precondition, not an answer. The same cheap query that lets you find a real effect on the tenth attempt lets you find a fake one on the two-hundredth, and the p-value threshold has no idea how many attempts preceded it. A 7,200x hypothesis budget at a fixed `α = 0.05` is a 7,200x false-positive budget. That is the bill the engineering win runs up.

Math-box:

**Illustrative Example — expected spurious findings under a true null, `α = 0.05`.**

`E[false positives] = N × α`

| Hypotheses tested | Expected false positives at α = 0.05 |
|---|---|
| `10` | `0.5` |
| `240` | `12` |
| `7,200` | `360` |
| `72,000` | `3,600` |

Same N values as section 2, so the two sections reconcile. To hold the family-wise error at `0.05` across `72,000` tests, Bonferroni needs a per-test `α = 0.05 ÷ 72,000 = 6.9 × 10⁻⁷`, a two-sided threshold of `|z| > 4.96` instead of `1.96`.

That is not free. At 80% power the required sample per test scales as `(z_{α/2} + z_β)²`, so `(4.96 + 0.84)² ÷ (1.96 + 0.84)² = 33.6 ÷ 7.84 ≈ 4.3x` the sample. Fortunately, cheap scans are also what make 4.3x the sample affordable — the engineering win partly pays its own bill.

- **The threshold is blind to attempt count:** 0.05 means one-in-twenty per test, never one-in-twenty overall
- **Iteration is the same act as p-hacking:** the only difference is whether the attempts were declared
- **Garden of forking paths:** informal choices of filter, window, and cohort are untracked tests too
- **Correction is the statistical debt:** the engineering gain must be repaid in threshold or in preregistration
- **Bonferroni is the crude repayment:** FDR control is usually the right instrument for a wide screen
- **The win is real but conditional:** more data helps only if the analyst counts the questions asked

### Visualization (canvas `canvas5`, 720×360)

Log-log line chart: expected false positives against number of tests, with an uncorrected line and a flat Bonferroni-controlled line.

- **Layout:** origin at (80, 290), plot width 570, plot height 220. Axes `#1a5276` width 2.
- **Data:** hardcoded N array `[10, 240, 7200, 72000]`; uncorrected series computed in JS as `N × 0.05`; corrected series is constant `0.05` (family-wise error held fixed).
- **Scales:** x log10 from 10 to 100,000; y log10 from 0.01 to 10,000.
- **Uncorrected line:** red `#e74c3c` width 2.5 with filled circles radius 5 at each N; each point labeled bold 11px red above it with the computed value (`0.5`, `12`, `360`, `3,600`) via a formatter that drops trailing zeros.
- **Corrected line:** green `#27ae60` width 2.5, dashed (6/4), horizontal at 0.05, labeled bold 11px green "family-wise error held at 0.05" above its left end.
- **Gridlines:** x at 10, 100, 1,000, 10,000, 100,000 and y at 0.01, 0.1, 1, 10, 100, 1,000, 10,000 — `#eee`, labels `#666` 10px.
- **Shaded region:** between the two lines filled `rgba(231,76,60,0.10)` to make the growing gap visible.
- **Threshold annotation (11px `#666`, two lines, upper left inside the plot):** print the computed Bonferroni α for the largest N in exponential form and the corresponding `|z|` threshold, both derived in JS from `0.05 / Nmax` — expect `α = 6.9e-7` and `|z| > 4.96` (use a rational inverse-normal approximation, then round to two decimals).
- **Axis labels:** x "Hypotheses tested (log scale)" at y = 322; y (rotated) "Expected false positives (log scale)" — both `#1a5276` 13px.
- **Footnote (10px `#999`, bottom left):** "Illustrative Example — N values match the latency tiers in the hypothesis-budget table."
- **Title (bold 14px `#1a5276`, top center):** "A 7,200x Hypothesis Budget Is a 7,200x False-Positive Budget".

## 6. The Complete Picture

Summary table (`.summary-table`, header row + 6 rows):

| Layer decision | Looks like | Is actually | Consequence for inference |
|---|---|---|---|
| **Storage layout** | A cost optimisation | Which scans are affordable | Sets how wide a question can be |
| **Query latency** | A developer-experience metric | The hypothesis budget | Budget ≈ time ÷ latency, 10 vs 72,000 |
| **Aggregation grain** | A compression choice | The finest testable unit | Daily rows forbid within-day ordering, permanently |
| **Retention window** | A compliance or bill decision | The longest measurable horizon | A 90-day window sees 49% of a 183-day curve |
| **Buffer / drop policy** | A reliability tuning knob | An undocumented sampling frame | Load-correlated loss biases rates downward (3.0% → 2.0%) |
| **Cheap iteration** | Pure upside | A multiplicity problem | 72,000 tests at α=0.05 expect 3,600 false positives |

## Callout (philosophy box, bottom)

**One sentence:** The pipeline is an epistemic instrument — it fixes the question space before any analyst arrives, so grain, retention, and drop policy deserve the scrutiny normally reserved for the model, and the cheap query it delivers must be paid for with an honest count of how many questions were asked.

## Regeneration instructions

- **Layout:** detail page. h1 (no index number), `.subtitle`, opening `.philosophy` callout, then per aspect: `<h2>N. Title</h2>` (h2 1.4em `#1a5276`, bottom border `2px solid #2980b9`, padding-bottom 8px) followed by a one-row `.obj-table` — left `<td>` (50%) holds `.obj-title`, paragraph, `.math-box`, bullets; right `<td>` (50%, centered) holds the canvas. Section 6 is a `.summary-table`; page closes with a `.philosophy` callout.
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em; table cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` `#1a5276`; ul 0.9em `#333`. No nav bar, no back/home links, no cross-reference links.
- **Callout style:** `.philosophy` — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em.
- **Math box:** `.math-box` — background `#f8fafb`, border `1px solid #e0e0e0`, radius 6px, padding 16px 20px, 0.9em; `code` background `#eef2f7`, padding 2px 6px, radius 3px. Inner tables use `.mini-table` (0.85em, `#e0e0e0` borders, `#f0f4f8` header).
- **Summary table:** `.summary-table` — 0.9em, th background `#f0f4f8` `#1a5276` padding 10px 14px left-aligned, td padding 10px 14px, borders `1px solid #e0e0e0`.
- **Canvas:** intrinsic 720×360 each; a shared `setupCanvas(id, w, h)` sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Data rule:** all chart data is hardcoded literal arrays — the counts and dates carry the lesson, so no PRNG is needed. If any future chart on this page generates data, it must use an inline seeded Park-Miller LCG with its own fixed seed, never `Math.random()`. Every statistic printed beside chart data (medians, the year gap, hypothesis counts, the speedup ratio, answerable fractions, error rates, the understatement percentage, false-positive counts, the Bonferroni α and z) is computed in JS at render time from the plotted values.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`, gray text `#666`/`#999`, accent `#2980b9`.
