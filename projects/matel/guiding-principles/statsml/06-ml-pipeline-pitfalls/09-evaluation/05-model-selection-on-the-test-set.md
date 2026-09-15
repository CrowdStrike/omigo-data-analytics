# Pitfall: Model Selection on the Test Set

**Page type:** detail page (card-section layout: h2 per section, two-column table with text left 50% / canvas right 50%)
**HTML title tag:** Model Selection on the Test Set

**Subtitle:** The test set is consulted once per experiment and reported once at the end — the number you report is a maximum, and a maximum is biased upward.

## The Simulation Behind Every Number on This Page

All three canvases share one seeded construction, so every printed statistic reconciles to the digit.

- **True accuracy** of every candidate: `p = 0.85`. All 40 configurations are *genuinely equally good* — there is no real winner.
- **Test set size:** `n = 2000`. The binomial standard error of an accuracy estimate is `sqrt(p*(1-p)/n) = sqrt(0.85*0.15/2000)`, computed as **0.7984 percentage points**. That is the noise scale, so the simulation is defensible rather than arbitrary.
- **Observed score** for configuration *i*: `p + se * z_i`, where `z_i` is a standard normal drawn by Box–Muller from a seeded Park-Miller LCG with fixed seed `8675309` (two uniforms per draw).
- **Computed results** (verified by running the exact draw code): mean of the 40 observed scores = **84.97%**, max (the "winner", configuration **31**) = **86.66%**, so the optimism gap = max − true = **1.66 percentage points**.
- **Maxima over the first N of the same series** — a strict, real rise: N=5 → **85.36%**, N=10 → **86.23%**, N=20 → **86.32%**, N=40 → **86.66%**.
- Both chart functions instantiate `lcg(8675309)` and regenerate the identical series, then compute mean, maxima and gaps from the plotted points at render time. Nothing is hardcoded as a label.

## The Problem

**Tags:** `the trap` (red), `adaptive overfitting` (blue)

- **Every peek spends information** — each test evaluation transfers a little of the holdout into your choices
- **The analyst is a slow optimizer** — a human running experiments is gradient descent on the test score
- **The test set becomes the objective** — whatever you select on, you fit, whether or not you wrote a loss
- **Max-of-N is biased upward** — with 40 equally good candidates, pure noise still produces a "winner"
- **Everything gets adjudicated there** — architecture, hyperparameters, early-stopping rounds, preprocessing variants
- **Early stopping on test** — halting at the best test epoch fits the stopping round to the holdout, silently
- **The reported number is a maximum** — a maximum over noisy estimates is not an unbiased estimate of anything
- **The gap shows up in production** — deployed accuracy lands near the true 85%, not the reported 86.66%

*Example:* 40 configurations all truly 85% accurate on n = 2000 give a mean observed score of 84.97%, but the winner reads 86.66% — 1.66 points of pure selection bias.

**Impact:** The reported score exceeds the deployable score by the selection bias, and nothing in the training logs reveals the gap until production traffic arrives.

### Visualization (canvas `c1`, 720×300)

Scatter of the 40 simulated configuration scores against the true accuracy, with the noise-driven winner highlighted. Conventional chart. All statistics computed at render time from the plotted points.

- **Seeded generator (verbatim, inside this draw function):**
  ```js
  // Seeded Park-Miller LCG — deterministic, never Math.random()
  function lcg(seed) { var s = seed; return function () { s = (s * 16807) % 2147483647; return s / 2147483647; }; }
  ```
  `var rnd = lcg(8675309); var P_TRUE = 0.85, N_TEST = 2000; var se = Math.sqrt(P_TRUE*(1-P_TRUE)/N_TEST);` then 40 draws of `P_TRUE + se * z` with `z = Math.sqrt(-2*Math.log(rnd()))*Math.cos(2*Math.PI*rnd())`.
- **Title (bold 14px `#1a5276`, centered, y=20):** "40 Configurations, All Truly 85% — Noise Picks a Winner (Illustrative Example)".
- **Subtitle (10px `#666`, centered, y=36):** "test set n = 2000, binomial standard error = " + (100*se).toFixed(2) + " pp".
- **Plot area:** left=62, right=700, top=52, bottom=214. Y scale spans 82.4% to 87.0% (fixed domain that contains the computed min 82.86 and max 86.66); gray `#999` L-shaped axes; y tick labels every 1 pp (83, 84, 85, 86, 87) in 9px `#666`.
- **True-accuracy line:** solid 1.5px `#27ae60` horizontal line at 85.0%, right-aligned green 9px label above it: "true accuracy = 85.00% (identical for all 40)".
- **Mean line:** dashed (5,3) 1.5px `#2980b9` at the computed mean, right-aligned blue 9px label below it: "mean observed = " + mean.toFixed(2) + "%".
- **Points:** 40 dots radius 3.2 at x = left + (i + 0.5) * (right-left)/40; fill `rgba(26,82,118,0.35)` with 1px `#1a5276` stroke, except the argmax point which is radius 5.5, fill `rgba(231,76,60,0.7)`, 2px `#e74c3c` stroke.
- **Winner annotation (bold 10px `#e74c3c`, centered above the winning dot, two lines):** "winner: config #" + (argmax+1) + " @ " + (100*max).toFixed(2) + "%" / "same model quality, luckier test split".
- **X label (10px `#666`, centered, y=230):** "configuration index (ordered by the sequence they were tried)".
- **Bottom callout (bold 12px `#e74c3c`, centered, y=254):** "Selection bias = max − true = " + gapPP.toFixed(2) + " pp of optimism from nothing but noise".
- **Bottom note (10px `#666`, centered, y=274 and y=290):** "Every configuration has the same true accuracy — there is no better model here to find." / "Illustrative Example — scores drawn from a seeded generator at the computed binomial standard error.".

## Why It Happens

**Tags:** `root cause` (orange), `search size` (blue)

- **No single script is wrong** — the leak is spread across a project's history, not inside any one file
- **Review sees one run** — a code reviewer reads the final notebook and finds a clean, correct split
- **"We only looked a few times"** — an informal peek count is always an undercount of the real search
- **Abandoned ideas were peeks too** — every discarded experiment already read the test score once
- **The search size is never recorded** — nobody logs how many configurations were compared before the winner
- **Bias grows with the number of tried configurations** — 5 attempts cost 0.36 pp here, 40 cost 1.66 pp
- **Shared leaderboards multiply it** — one public holdout scored by many teams is adaptively overfit collectively
- **Validation feels wasteful** — carving out a third split looks like throwing away scarce training rows

*Example:* The same seeded series gives maxima of 85.36%, 86.23%, 86.32% and 86.66% after 5, 10, 20 and 40 configurations — the optimism rises with the search, monotonically.

**Root Cause:** Selection bias is a property of how many times the holdout was consulted, and that count is neither logged nor visible in the code, so nothing in review can price it.

### Visualization (canvas `c2`, 720×300)

Step chart of the running maximum over the *same* seeded series, showing computed maxima at N = 5, 10, 20, 40. Conventional chart. The rise is real in the data, not drawn by hand.

- **Seeded generator:** identical `lcg(8675309)`, same 40 draws as canvas `c1`, so the two charts agree to the digit.
- **Title (bold 14px `#1a5276`, centered, y=20):** "Optimism Grows With the Number of Configurations Tried (Illustrative Example)".
- **Subtitle (10px `#666`, centered, y=36):** "running maximum of the same 40 scores — true accuracy 85.00%, standard error " + (100*se).toFixed(2) + " pp".
- **Plot area:** left=62, right=620, top=54, bottom=212. Y domain 84.8% to 87.0%; X domain configuration 1 to 40 (linear). Gray `#999` axes; y ticks every 0.5 pp labeled to one decimal (85.0, 85.5, 86.0, 86.5, 87.0) in 9px `#666`; x ticks at 1, 5, 10, 20, 30, 40.
- **True-accuracy line:** solid 1.5px `#27ae60` at 85.0%, green 9px label at the right edge: "true = 85.00%".
- **Running-maximum staircase:** 2.5px `#e74c3c` step-after path of `max(scores[0..i])` for i = 0..39.
- **Milestone markers:** filled `#e74c3c` dots radius 4.5 at N = 5, 10, 20, 40 with bold 10px `#e74c3c` labels above each: "N=5" / value, "N=10" / value, "N=20" / value, "N=40" / value, each value printed as `(100*maxOf(N)).toFixed(2) + '%'` computed from the series slice.
- **Gap bracket:** thin 1px `#e67e22` vertical line at N=40 from the true line up to the N=40 maximum, with a rotated-free 9px orange label to its right: "+" + gap40.toFixed(2) + " pp".
- **Right-hand legend block (9px, x=634, right edge free):** three stacked lines — green "true", red "max seen", blue "mean".
- **Mean line:** dashed (5,3) 1.5px `#2980b9` at the computed mean of all 40, blue 9px label: "mean " + mean.toFixed(2) + "%".
- **Bottom callout (bold 11px `#e67e22`, centered, y=246):** "Maxima: N=5 → " + m5 + "%,  N=10 → " + m10 + "%,  N=20 → " + m20 + "%,  N=40 → " + m40 + "% — strictly rising".
- **Bottom note (10px `#666`, centered, y=266 and y=284):** "The running maximum can only rise, so more experiments always buy more apparent accuracy." / "Illustrative Example — one seeded series; the milestone values are computed from the plotted points.".

## The Correct Approach

**Tags:** `the fix` (green), `locked holdout` (blue)

- **Three-way split** — training fits, validation decides *everything*, test is read exactly once at the end
- **All selection on validation** — architecture, hyperparameters, early-stopping round, preprocessing variant
- **Nested cross-validation** — when data is scarce, an inner loop selects and an outer loop estimates
- **Locked holdout with an access log** — record who evaluated on test, when, and for which configuration
- **Pre-register the metric and the budget** — write down the selection metric and how many configurations before starting
- **Report both numbers** — publish the validation-based estimate beside the single final test number
- **A second test read needs a fresh holdout** — re-evaluating on the same test set restarts the bias accumulation
- **Never report a maximum bare** — state the search size next to the score so the optimism can be priced

*Example:* Selecting all 40 configurations on validation and reading test once returns roughly the true 85%, instead of the 86.66% a test-set search reports.

**Fix:** Let validation absorb every comparison, keep the test set sealed behind a written access log, and report the search size alongside the one final test number.

### Visualization (canvas `c3`, 720×300)

Conceptual diagram: a three-way split where validation does all the selection work while the test set sits behind glass with a single-use seal, plus an access log of experiment rows stamped as withdrawals. No simulated statistics — labels are structural, and the two accuracy figures shown come from the same computed constants used in `c1` (true 85.00%, biased 86.66%).

- **Title (bold 14px `#1a5276`, centered, y=20):** "Validation Does the Work; the Test Set Has One Seal".
- **Split bar:** x from 40 to 680, y=44, height 46, three segments by width share — TRAIN 70% fill `rgba(26,82,118,0.7)` stroke `#1a5276`, VALIDATION 15% fill `rgba(230,126,34,0.7)` stroke `#e67e22`, TEST 15% fill `rgba(39,174,96,0.18)` stroke `#27ae60` 2.5px. Bold 11px white centered labels "TRAIN (70%)" and "VALIDATION (15%)"; the test segment carries a bold 11px `#27ae60` label "TEST (15%)".
- **Glass seal on the test segment:** two diagonal 1px `rgba(39,174,96,0.55)` hatch lines across it plus a small 54×16 white box centered on its lower edge, 1.5px `#27ae60` border, bold 8px `#27ae60` text "BREAK ONCE".
- **Captions under the bar (10px, y=104):** orange under VALIDATION — "all 40 comparisons happen here"; green under TEST — "read exactly once, at the end".
- **Access log panel:** 300×150 box at (40, 122), 1.5px `#1a5276` border, header bold 10px `#1a5276` at the top-left inside: "HOLDOUT ACCESS LOG". Column headers 9px `#666`: "experiment", "set", "status". Six rows, pitch 19px, alternate row fill `rgba(26,82,118,0.04)`:
  1. "arch sweep A–F" / "validation" / green "allowed"
  2. "learning-rate grid" / "validation" / green "allowed"
  3. "early-stopping round" / "validation" / green "allowed"
  4. "scaler vs quantile" / "validation" / green "allowed"
  5. "abandoned idea #7" / "validation" / green "allowed"
  6. "FINAL model" / "test" / bold red "SEAL BROKEN"
  Statuses in bold 9px, `#27ae60` for allowed and `#e74c3c` for the last row.
- **Comparison panel:** 320×150 box at (360, 122), 1.5px `#2980b9` border, header bold 10px `#1a5276`: "WHAT GETS REPORTED".
  - Green row: bold 11px `#27ae60` "85.00%" then 10px `#333` "test read once, after validation selection".
  - Red row: bold 11px `#e74c3c` "86.66%" then 10px `#333` "best of 40 test evaluations (biased)".
  - Orange bracket between the two values: 1px `#e67e22` vertical line with bold 9px orange label "1.66 pp of search, not skill" (value printed from the same computed constants).
  - Footer line inside the panel, 9px `#666`: "Always state the number of configurations compared.".
- **Bottom note (10px `#666`, centered, y=292):** "Illustrative Example — figures carried over from the seeded simulation above (n = 2000, standard error 0.80 pp).".

## Regeneration instructions

- **Layout:** `.card-section` per section: `<h2>` with 2px `#2980b9` bottom border, then `table.layout` (border-collapse, full width) with one `<tr>`: `td.text-col` (**50%**) holding `.tags` pills + `<ul>` bullets + `.example` italic paragraph + `.key-point` callout; `td.viz-col` (**50%**) holding the canvas. Shrink a visualization via canvas `max-width` / `max-height`, never by narrowing the column.
- **Sections:** exactly three — "The Problem", "Why It Happens", "The Correct Approach". The simulation-constants section above is spec metadata, not a rendered `<h2>`; its numbers appear only inside the canvases and the prose.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; h2 1.3rem `#1a5276`. No nav bar, no back/home/see-also links — this is a leaf page.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem. `.example` italic `#555` 0.9rem. `li b` colored `#1a5276`.
- **Canvas:** intrinsic 720×300, CSS `width: 100%`, 1px `#e0e0e0` border, 4px radius; scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Randomness:** never `Math.random()`. Each of `c1` and `c2` declares its own `lcg` generator seeded with the fixed value `8675309`, reproducing the identical 40-score series; `c3` uses no generator. Every statistic printed beside generated data is computed from the plotted points at render time.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, accent `#2980b9`, bar fill `rgba(26,82,118,0.35)`.
- In regenerated HTML, any card links use `.html` extensions.
