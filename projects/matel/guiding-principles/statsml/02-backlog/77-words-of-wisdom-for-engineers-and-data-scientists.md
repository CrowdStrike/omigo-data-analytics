# Words of Wisdom for Engineers and Data Scientists

**Page type:** detail page (backlog-style two-column layout: text left 50%, canvas right 50%, one `.lang-section` per saying; h1 carries a BACKLOG status pill)
**HTML title tag:** Words of Wisdom for Engineers and Data Scientists

**Subtitle:** Each saying is decomposed into its hidden conditioning variable, the regime where it holds, and the regime where it inverts.

**Intro callout:** A saying that fires unconditionally cannot be wrong, and a rule that cannot be wrong carries no information. Every durable piece of engineering wisdom is a conditional statement with the condition stripped off. Recovering the condition is what turns a slogan back into advice. All figures on this page are constructed illustrations, not measurements.

## 1. Slow Is Smooth, Smooth Is Fast

The rifle-range version says slow is steady and steady is fast — both compress the same claim.

- **The hidden variable** — the probability that a fast attempt has to be thrown away and redone.
- **What it really says** — expected total time, not single-pass time, is the quantity being minimized.
- **Where it holds** — irreversible work: migrations, deletes, deploys, anything with expensive rework.
- **Where it inverts** — cheap-feedback work where the fast attempt teaches you what to build.
- **The arithmetic** — a careful pass costing 10 units with a 5% redo rate expects 10.5 units total.
- **The crossover** — a 4-unit fast pass wins until its failure rate passes 62%, then careful wins.
- **The real failure mode** — "slow" gets used to license unbounded deliberation with no rework risk to justify it.

**Key point:** Deliberation is only cheap insurance when the thing you might have to undo is expensive.

### Visualization (canvas `c1`, 720×340)

Line chart: expected total time to a correct result, careful pass vs fast pass, as the fast pass's failure rate rises.

- **Title (bold 16px, `#1a5276`, top center):** "Expected Total Time, Not Single-Pass Time".
- **Plot area:** x=76, y=52, width = canvas−150, height = canvas−120; L-shaped axes `#95a5a6` (1.4px).
- **Model:** expected total time = single-pass cost ÷ (1 − failure rate), retrying until success.
- **Constants:** careful pass cost 10 with failure rate 0.05; fast pass cost 4 with failure rate `p` on the x-axis.
- **Scales:** y from 0 to 40 (time units), ticks every 10 (12px `#5a6875`, right-aligned); x spans `p` = 0 to 0.9, ticks at 0, 0.2, 0.4, 0.6, 0.8; x axis label "Failure rate of the fast pass" (13px `#4a5866`, centered below).
- **Careful series (stroke `#27ae60`, 3px):** horizontal line at 10 / 0.95 = 10.526, drawn across the full x range.
- **Fast series (stroke `#e74c3c`, 3px):** 4 / (1 − p), sampled p = 0 → 0.9 in 0.005 steps.
- **Crossover marker:** computed at render time as `p* = 1 − 4·0.95/10`; vertical dashed `#e67e22` line (dash 5/4, 2px) at `p*`, label "crossover at " + (100·p*).toFixed(0) + "%" (13px `#e67e22`, left-aligned near the top).
- **Series labels (13px, left-aligned):** green "careful pass" just above the flat line at p = 0.08; red "fast pass, retried" at p = 0.30, offset above the curve.

## 2. Premature Optimization Is the Root of All Evil

Knuth's sentence is a conditional and the condition is the word "premature".

- **The hidden variable** — what share of runtime the code you are about to tune actually holds.
- **The original claim** — measure first, then optimize the small slice that dominates the profile.
- **Where it holds** — leaf-level micro-tuning of code whose profile share is unmeasured or tiny.
- **Where it inverts** — decisions you cannot revisit: data layout, schema, indexing, algorithmic order.
- **The asymmetry** — a slow function is a patch later; a wrong data model is a migration later.
- **In the profile shown** — two functions hold 69% of runtime and the last eight hold 14% combined.
- **The misuse** — quoting the slogan to skip the profiling step that the slogan is built on.

**Key point:** The quote forbids tuning without a profile, not thinking about performance.

### Visualization (canvas `c2`, 720×340)

Bar chart: runtime share per function, sorted descending, with the cumulative share of the top two computed at render time.

- **Title (bold 16px, `#1a5276`, top center):** "Where the Runtime Actually Is".
- **Data (percent of total runtime, 12 functions labeled f1…f12):** `[41, 28, 11, 6, 4, 3, 2, 2, 1, 1, 0.6, 0.4]` — sums to exactly 100.
- **Plot area:** x=66, y=72, width = canvas−120, height = canvas−140; scale max 45; L-shaped axes `#95a5a6` (1.4px).
- **Bars:** 12 slots, bar width 0.55·slot; fill `rgba(26,82,118,0.35)`, stroke `#1a5276` 1.2px; the first two bars stroked `#e74c3c` 1.6px with fill `rgba(231,76,60,0.40)`.
- **Y ticks:** every 15 with a "%" suffix (12px `#5a6875`, right-aligned).
- **X labels:** f1…f12 (11px `#4a5866`) centered under each slot; axis label "Functions, sorted by runtime share" centered below.
- **Computed annotations (13px, both derived from the array at render time, never hardcoded):** red `#e74c3c` "top 2 = " + sum of first two + "% of runtime" above the second bar; gray `#5a6875` "last 8 = " + sum of the final eight + "% combined" above the tail, right-aligned.
- **Footnote (11px `#7f8c8d`, bottom right of the canvas):** "Illustrative Example".

## 3. Make It Work, Make It Right, Make It Fast

The ordering assumes "works" and "right" are separable observations. In modelling they are not.

- **The hidden variable** — whether "it works" can be observed without already knowing "it is right".
- **Where it holds** — deterministic software: the failing case is visible, so working code is checkable.
- **Where it inverts** — supervised learning, where fitting the data you have looks exactly like working.
- **The training-set trap** — error on data the model saw is not evidence about data it has not seen.
- **In the curve shown** — training error falls to 0.5% while held-out error bottoms at epoch 7 and then climbs.
- **The second inversion** — when latency is in the contract, "fast" is part of "right", not a later phase.
- **What survives** — the ordering is about not polishing throwaways, not about deferring correctness.

**Key point:** In modelling, "make it work" and "make it right" are the same step and must share one holdout.

### Visualization (canvas `c3`, 720×340)

Line chart: training error vs held-out error across epochs, with the held-out minimum located by argmin at render time.

- **Title (bold 16px, `#1a5276`, top center):** "\"It Works\" and \"It's Right\" Are Not the Same Curve".
- **Plot area:** x=76, y=52, width = canvas−150, height = canvas−120; L-shaped axes `#95a5a6` (1.4px).
- **Data (epochs 1–14):**
  - training error %: `[40, 28, 20, 15, 11, 8, 6, 4, 3, 2, 1.5, 1, 0.8, 0.5]`
  - held-out error %: `[42, 31, 24, 19, 16, 14, 13.5, 14, 15.5, 17, 19, 21, 23, 25]`
- **Scales:** y from 0 to 45 (error %), ticks every 15 with "%" suffix (12px `#5a6875`, right-aligned); x = epoch 1 to 14, ticks at 1, 4, 7, 10, 14; axis label "Training epoch" (13px `#4a5866`, centered below).
- **Training series (stroke `#27ae60`, 3px):** straight segments through the 14 points.
- **Held-out series (stroke `#e74c3c`, 3px):** straight segments through the 14 points.
- **Minimum marker:** the index of the held-out minimum is computed with argmin at render time; draw a filled `#e67e22` circle (radius 5) at that point and a vertical dashed `#e67e22` line (dash 4/4, 1.6px) down to the axis.
- **Computed label (13px `#e67e22`, centered above the marker):** "best holdout: epoch " + (argmin+1) + " at " + value + "%".
- **Series labels (13px, left-aligned):** green "training error" near epoch 9 below its curve; red "held-out error" near epoch 11 above its curve.
- **Footnote (11px `#7f8c8d`, bottom right of the canvas):** "Illustrative Example".

## 4. If It Isn't Tested, It's Broken

True as a warning, false as a coverage target — the two readings differ in what "tested" refers to.

- **The hidden variable** — whether your tests live where your defects live, which coverage cannot report.
- **Where it holds** — logic inside a single function, where a unit test is a direct check of the claim.
- **Where it inverts** — contracts, schemas, ordering, and configuration, which unit tests barely touch.
- **The data-team version** — the transform is tested; the data flowing through it never is.
- **In the mix shown** — per-class catch rates weight to an overall 43% of defects caught by unit tests.
- **The converse is also false** — a passing suite is evidence about the suite, not proof of correctness.
- **The useful form** — untested code is unverified, and unverified is not the same as broken.

**Key point:** Coverage measures which lines ran, never whether the defect classes you have are checked.

### Visualization (canvas `c4`, 720×340)

Bar chart: unit-test catch rate per defect class, with the defect-mix-weighted average drawn as a computed reference line.

- **Title (bold 16px, `#1a5276`, top center):** "Catch Rate by Defect Class".
- **Data (6 classes; mix share % and unit-test catch rate %):**
  - in-function logic — share 22, catch 88
  - wrong branch — share 18, catch 76
  - integration contract — share 20, catch 34
  - data schema drift — share 17, catch 9
  - concurrency — share 12, catch 12
  - config / environment — share 11, catch 6
- **Share column sums to 100 (22+18+20+17+12+11).**
- **Plot area:** x=66, y=72, width = canvas−120, height = canvas−148; scale max 100; L-shaped axes `#95a5a6` (1.4px).
- **Bars:** 6 slots, bar width 0.42·slot; catch rate ≥ 50 uses fill `rgba(39,174,96,0.50)` stroke `#27ae60`, below 50 uses fill `rgba(231,76,60,0.50)` stroke `#e74c3c` (1.4px).
- **Y ticks:** 0, 25, 50, 75, 100 with "%" suffix (12px `#5a6875`, right-aligned).
- **X labels:** two lines per slot — class name (11px `#4a5866`) then "share " + share + "%" (10px `#7f8c8d`); axis label "Defect class" centered below.
- **Weighted average line:** computed at render time as Σ(share·catch)/100 = 43.47; horizontal dashed `#1a5276` line (dash 6/4, 2px) at that value, label "mix-weighted catch rate " + value.toFixed(0) + "%" (13px `#1a5276`, left-aligned at the right end, above the line).
- **Bar value labels (11px `#4a5866`, centered just above each bar):** the catch rate with a "%" suffix.
- **Footnote (11px `#7f8c8d`, bottom right of the canvas):** "Illustrative Example".

## 5. You Can't Manage What You Don't Measure

The one saying here that is simply false, and it is usually credited to the person who called it a myth.

- **The attribution is wrong** — Deming's writing rejects this claim outright rather than asserting it.
- **The hidden variable** — whether the outcome's real drivers are measurable at acceptable cost.
- **Where a weaker version holds** — for drivers already instrumented, measurement beats intuition.
- **Where it inverts** — design quality, morale, and tacit domain knowledge are managed by judgment.
- **The damage it does** — it licenses substituting a measurable proxy for the unmeasurable driver.
- **In the plot shown** — 57% of total driver importance sits on the hard-to-measure side of the line.
- **The honest restatement** — measure what you can, and keep managing the rest deliberately.

**Key point:** Declaring the unmeasurable unmanageable does not remove it from the causal path.

### Visualization (canvas `c5`, 720×340)

Quadrant scatter: outcome drivers plotted by how measurable they are against how much they matter.

- **Title (bold 16px, `#1a5276`, top center):** "Importance vs Measurability".
- **Plot area:** x=76, y=56, width = canvas−150, height = canvas−124; full box frame `#95a5a6` (1.4px).
- **Scales:** x = measurability 0–100, y = importance 0–100; ticks every 25 on both axes (12px `#5a6875`); axis labels "How measurable" (below, centered) and "How much it drives the outcome" (13px `#4a5866`, above the y ticks, left-aligned at the top-left of the plot).
- **Quadrant lines:** dashed `#bdc3c7` (dash 4/4, 1.2px) at x = 50 and y = 50.
- **Points (measurability, importance) — radius 6 filled circles:**
  - incident rate (92, 55) — `#27ae60`
  - p99 latency (88, 60) — `#27ae60`
  - test coverage (95, 30) — `#27ae60`
  - code churn (80, 35) — `#27ae60`
  - design quality (25, 85) — `#e74c3c`
  - team morale (20, 80) — `#e74c3c`
  - tacit domain knowledge (15, 78) — `#e74c3c`
- **Point labels (11px `#4a5866`):** name placed right of green points and left of red points, vertically centered.
- **Computed annotation (13px `#e74c3c`, top-left inside the plot, two lines):** "hard to measure, high impact" and the share of total importance with measurability < 50, computed at render time as 243/423 → "57% of total importance".
- **Quadrant caption (12px `#7f8c8d`, bottom-right inside the plot):** "easy to measure, low impact".
- **Footnote (11px `#7f8c8d`, bottom right of the canvas):** "Illustrative Example".

## 6. Don't Repeat Yourself

DRY is a claim about duplicated decisions; it gets applied to duplicated characters.

- **The hidden variable** — whether the two copies must change together, or only happen to look alike.
- **Where it holds** — one rule expressed twice, where a single edit must reach both call sites.
- **Where it inverts** — coincidental similarity, where sharing couples callers that will diverge later.
- **The tell** — every new caller adds a flag, and the shared helper grows a branch per caller.
- **The data version** — merging two similar metrics quietly merges two different business definitions.
- **The tradeoff shape** — duplication cost falls with sharing while coupling cost rises faster than linearly.
- **In the curve shown** — total change cost is minimized near 58% shared, not at 0% and not at 100%.

**Key point:** The cost curve has an interior minimum, so maximal sharing is as wrong as none.

### Visualization (canvas `c6`, 720×340)

Line chart: duplication cost, coupling cost, and their sum against how much of the logic is shared, with the minimum located numerically.

- **Title (bold 16px, `#1a5276`, top center):** "Total Change Cost Has an Interior Minimum".
- **Plot area:** x=76, y=52, width = canvas−150, height = canvas−120; L-shaped axes `#95a5a6` (1.4px).
- **Model (s = fraction of the logic factored into one place, 0 to 1):**
  - duplication cost = 30·(1 − s)
  - coupling cost = 4 + 26·s^2.2
  - total = the sum of the two
- **Scales:** y from 0 to 36 (relative cost of one future change), ticks every 12 (12px `#5a6875`, right-aligned); x = s from 0 to 1 shown as a percentage, ticks at 0, 25, 50, 75, 100; axis label "Share of the logic factored into one place" (13px `#4a5866`, centered below).
- **Series (sampled s = 0 → 1 in 0.005 steps):** duplication cost stroke `#e67e22` 2px; coupling cost stroke `#2980b9` 2px; total stroke `#1a5276` 3.2px.
- **Minimum marker:** scan the sampled total for its minimum at render time; filled `#e74c3c` circle (radius 5) at that point plus a vertical dashed `#e74c3c` line (dash 4/4, 1.6px) to the axis.
- **Computed label (13px `#e74c3c`, centered above the marker):** "minimum at " + (100·s*).toFixed(0) + "% shared".
- **Series labels (12px, left-aligned):** orange "duplication cost" near s = 0.10 above its line; blue "coupling cost" near s = 0.80 left of its line; dark blue "total cost" near s = 0.30 above the total curve.
- **Footnote (11px `#7f8c8d`, bottom right of the canvas):** "Illustrative Example".

## Regeneration instructions

- **Layout:** backlog detail page. `h1` (2rem `#1a5276`, bottom border `2px solid #2980b9`) with inline `.status` pill "BACKLOG" (background `#fef9e7`, border `1px solid #f39c12`, text `#b7950b`, 4px radius, 0.8rem); `.subtitle` (`#666`, 0.95rem); `.intro` callout (background `#f0f4f8`, left border `3px solid #2980b9`, 8px 12px padding, 0.9rem). One `.lang-section` per numbered h2 (1.3rem `#1a5276`, bottom border `2px solid #2980b9`); inside each, `table.layout` with `td.text-col` 50% and `td.viz-col` 50%, both `vertical-align: top`, 12px padding. No index number in the h1 or the title tag.
- **Text blocks:** intro `<p>`, `<ul>` bullets (0.92rem) with `<strong>` lead-ins, `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, 0.9rem).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. Canvases `width: 100%`, `1px solid #e0e0e0` border, 4px radius, `height: auto`.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange, `rgba(26,82,118,0.35)` bar fill; secondary `#2980b9`; gray labels `#5a6875`/`#4a5866`/`#7f8c8d`, axes `#95a5a6`.
- **Canvas:** intrinsic 720×340 each; a shared `setupCanvas(id)` caches the intrinsic size on the element, caps display width via `style.maxWidth`, sizes the backing store to rendered width × `window.devicePixelRatio`, and resets the transform so drawing stays in logical coordinates. Every chart registers its draw function and all of them re-run on `window resize`.
- **No randomness anywhere:** all series are literal arrays or closed-form functions, so no PRNG is required; `Math.random()` must never appear. Every statistic printed beside a series is derived from that series at render time.
- **No cross-page links:** this page contains no navigation, back, or reference links.
