# Unqualified Percentage Reporting

**Page type:** detail page (two-column obj-table layout: text left 40%, canvas right 60%, one row per section)
**HTML title tag:** Unqualified Percentage Reporting — Common Bad Practices

**Subtitle:** Publishing normalized rates (accuracy, conversion, improvement) without sample size, confidence intervals, or dataset provenance — making results on 100 examples indistinguishable from results on 100,000.

## The Practice

- Collect or generate a dataset — often small, often convenient, often unrepresentative.
- Compute a rate metric: accuracy, precision, conversion, satisfaction score.
- Normalize to percentage. Publish the percentage alone.
- Omit: sample size (n), confidence interval, dataset description, selection criteria.
- Result: the reader cannot distinguish a rigorous finding from a favorable accident.

**Why it persists:** Percentage normalization is a lossy format. Once you divide by n and multiply by 100, the denominator is gone. The format itself looks complete — "99% accuracy" appears to be a self-contained fact requiring no footnote. The reader has no instinct to ask "out of how many?"

### Visualization (canvas `c1`, 720×300)

Funnel diagram: two evidence boxes collapse into one identical published claim ("Same Published Number — Different Underlying Evidence").

- **Title (bold 15px, top center, `#1a5276`):** "Same Published Number — Different Underlying Evidence".
- **Left box (weak evidence):** at y=55, height 80, fill `rgba(231,76,60,0.08)`, stroke `#e74c3c` width 2. Bold 28px red "99%", 13px `#333` lines "n = 100" and "CI: [94.6%, 99.9%]".
- **Right box (strong evidence):** same size, fill `rgba(39,174,96,0.08)`, stroke `#27ae60` width 2. Bold 28px green "99%", 13px `#333` lines "n = 100,000" and "CI: [98.94%, 99.06%]".
- **Published box:** centered below (240×50), fill `rgba(230,126,34,0.08)`, stroke `#e67e22` width 2; bold 22px orange text: "\"99% accuracy\"".
- **Connectors:** thin gray `#999` lines from the bottom center of each evidence box to the published box.
- **Bottom annotation (italic 13px `#666`, centered, two lines):** "Once normalized to percentage, both produce the identical published claim." / "The reader cannot recover which one they are looking at."

## The Incentive Structure

- **Proper evaluation requires sufficient data** — that means effort, cost, and timeline.
- Publishing on whatever data currently exists is faster and cheaper.
- Smaller datasets produce higher variance — more likely to land a favorable number by chance.
- Smaller datasets are easier to curate (intentionally or inadvertently) toward clean, easy examples.
- Percentage normalization erases all of the above from the published result.

**The perverse alignment:** less effort → faster delivery, more extreme (favorable) rates, and a format that hides both. The honest path (collect more data, report with CI) takes longer AND produces a less impressive-looking number (regression toward the true mean + visible uncertainty). Rigor is punished on both axes: timeline and optics.

### Visualization (canvas `c2`, 720×340)

Strip/dot plot: published percentage claims plotted against the denominator behind them ("Published Percentage Claims vs the Denominator Behind Them").

- **Title (bold 15px, top center, `#1a5276`):** "Published Percentage Claims vs the Denominator Behind Them".
- **Chart area:** left 70, right w−40, top 50, bottom 280; x = sample size n on log10 scale from 10 to 100,000, gridlines `#eee` with 11px `#999` labels at 10, 100, 1k, 10k, 100k; y = published rate mapped 88%–101%, gridlines `#eee` at 90/95/100 with 11px `#999` percent labels; 11px `#666` x-axis label "sample size n behind the claim (log scale)".
- **Headline zone:** light red band `rgba(231,76,60,0.06)` covering y ≥ 95% across the chart, 11px `#e74c3c` right-aligned label "headline zone (≥ 95%)".
- **True-rate line:** dashed green `#27ae60` (dash 4/3, width 1.5) horizontal at 90%, 11px green label "true rate ≈ 90%".
- **Headline claims (red `#e74c3c` dots, radius 5, fill `rgba(231,76,60,0.75)`), (n, published %):** (30, 100), (50, 99), (60, 98), (80, 97.5), (100, 99), (120, 96), (150, 98), (200, 95.5).
- **Routine claims (blue `#1a5276` dots, radius 4, fill `rgba(26,82,118,0.55)`), (n, published %):** (400, 93.5), (700, 92), (1000, 92.5), (1500, 91), (2500, 90.8), (4000, 90.4), (8000, 90.6), (15000, 90.2), (30000, 89.9), (60000, 90.1), (100000, 90.0).
- **Insight annotation (bold 13px red, near the small-n cluster):** "Every claim above 95% rests on n ≤ 200".
- **Caption (italic 12px `#666`, bottom center):** *"Each dot is one published claim. Extreme percentages are a small-denominator artifact — and the format hides the denominator."*

## What a Percentage Without Context Destroys

- **Confidence interval:** at n=100 with 99% observed, the 95% CI is [94.6%, 99.9%]. At n=100,000 with 99% observed, CI is [98.94%, 99.06%]. Same percentage, entirely different knowledge.
- **Statistical significance:** you cannot compute p-values or determine whether the result differs from baseline without n.
- **Reproducibility:** without dataset description, nobody can replicate, verify, or stress-test the claim.
- **Comparability:** "99% accuracy" on curated examples vs "94% accuracy" on production data — the 94% is the stronger result, but looks weaker to a reader who lacks context.

### Visualization (canvas `c3`, 720×300)

Horizontal CI-width chart by sample size ("95% Confidence Interval Width at Observed 99% — by Sample Size").

- **Title (bold 15px, top center, `#1a5276`):** "95% Confidence Interval Width at Observed 99% — by Sample Size".
- **Data (one row per n, CI [lo, hi] in %):**
  - n=50: [91.1, 99.9]
  - n=100: [94.6, 99.9]
  - n=500: [97.6, 99.6]
  - n=1000: [98.2, 99.5]
  - n=5000: [98.7, 99.2]
  - n=100,000: [98.94, 99.06]
- **Chart area:** left 130, right w−40, top 50, bottom 250; x scale maps 88%–100%; vertical gridlines `#eee` at 90–100 step 2 with 11px `#999` percent labels below.
- **Reference line:** dashed `#1a5276` (dash 4/3, width 1.5) vertical at 99%.
- **CI bars:** 10px tall red bars per row, fill `rgba(231,76,60,alpha)` where alpha scales with CI width (0.2 + width/10 × 0.6, capped), stroke `#e74c3c`; blue `#1a5276` 4px point-estimate dot at 99% on each row; right-aligned 12px `#333` row labels "n=50" … "n=100,000".
- **Caption (italic 12px `#666`, bottom center):** "All report \"99%.\" The CI tells you how much you actually know."

## Real-World Manifestations

**ML Model Announcement** (example box)
"Our model achieves 99% accuracy on the benchmark." Benchmark: 100 hand-selected examples covering only the easy cases. Production accuracy on the full distribution: 71%. The published number is technically correct but practically meaningless.

**Customer Satisfaction** (example box)
"95% customer satisfaction." Survey sent to 47 users who opted in after a positive support interaction. Non-respondents (the dissatisfied majority) excluded by design. n=47, CI: [84%, 99%].

**A/B Test Result** (example box)
"3× conversion improvement." Two-week test on 200 users with no holdout, no power analysis, no correction for multiple comparisons. Point estimate unstable; a different two-week window would yield 0.8× just as easily.

**Startup Pitch Deck** (example box)
"98% detection rate." Tested on a curated set of 50 positive examples chosen to match the model's training distribution. False positive rate on real-world data: unreported because the denominator (millions of negatives) would make the false positive count visible.

### Visualization (canvas `c4`, 720×380)

Grouped bar chart: published curated numbers vs production reality ("Curated Eval vs Production Reality").

- **Title (bold 15px, top center, `#1a5276`):** "Curated Eval vs Production Reality".
- **Chart area:** left 100, right w−60, top 55, bottom 300; four groups, bar width 35, curated/production side by side.
- **Groups 1-3 (percent scale, max 100):**
  - "ML paper" / "(100 hand-selected)": curated 99%, production 71%.
  - "Satisfaction survey" / "(47 opt-in)": curated 95%, production 62%.
  - "Detection system" / "(50 curated positives)": curated 98%, production 79%.
- **Group 4 (multiplier scale, drawn with fixed bar heights 180 and 60):** "A/B test" / "(200 users, 2 weeks)": curated "3×", production "1.08×".
- **Bar styles:** curated fill `rgba(231,76,60,0.6)` stroke `#c0392b`; production fill `rgba(26,82,118,0.35)` stroke `#1a5276`. Bold 12px value labels above each bar in the bar's stroke color; 11px `#333` two-line group labels below.
- **Legend (top left, 14px swatches):** "Published (curated eval)" (red), "Production reality" (blue).
- **Caption (italic 12px `#666`, bottom center):** "The published percentage is technically correct. It is also practically meaningless."

## What Responsible Reporting Requires

- **Sample size (n):** the absolute minimum. Without it, no downstream analysis is possible.
- **Confidence interval or standard error:** quantifies the uncertainty around the point estimate.
- **Dataset provenance:** how examples were selected, what population they represent, known exclusions.
- **Baseline comparison:** what does random/naive/previous-best achieve on the same data?
- **Effect size in context:** is the improvement practically meaningful, not just statistically present?

**Minimum credibility threshold:** if a published result omits all five, the percentage is not a finding — it is a claim without supporting evidence, regardless of how precise the number appears.

### Visualization (canvas `c5`, 720×300)

Audit-style paired horizontal bars: each headline claim vs what it becomes once the denominator is disclosed ("The Audit: Reported Claim vs What the Evidence Supports").

- **Title (bold 15px, top center, `#1a5276`):** "The Audit: Reported Claim vs What the Evidence Supports".
- **Legend (top left, 14px swatches, 12px `#333` text):** "As reported" (red), "95% CI floor once n is disclosed" (green).
- **Chart area:** left 190, right w−50, top 60, bottom 245; x scale 75%–100%, vertical gridlines `#eee` at 75–100 step 5 with 11px `#999` percent labels below.
- **Data rows (claim, n, reported %, audited floor % = 95% CI lower bound):**
  - "99% accuracy", n=100: 99 → 94.6
  - "98% detection", n=50: 98 → 89.4
  - "95% satisfaction", n=47: 95 → 84.0
  - "96% precision", n=25: 96 → 79.7
  - "100% pass rate", n=20: 100 → 83.2
- **Bars:** two 11px horizontal bars per row (reported on top: fill `rgba(231,76,60,0.55)`, stroke `#c0392b`; audited floor below: fill `rgba(39,174,96,0.45)`, stroke `#27ae60`), both anchored at 75%; bold 11px value labels at bar ends in the bar's stroke color; left-aligned row labels: bold 12px `#333` claim over 11px `#666` "n = …".
- **Insight annotation (bold 13px red, centered below axis):** "The smaller the n, the more of the headline evaporates under audit".
- **Caption (italic 12px `#666`, bottom center):** *"Same claims, audited: disclosing the denominator turns each headline into its evidence-supported floor."*

## How It Differs From Neighbors

- **Denominator Trick (#17):** deliberately shrinks the denominator to inflate the rate. Here, the denominator was always small — the practice is publishing without revealing that fact.
- **Cherry-Picking (#14 Dashboard):** selects favorable time windows or segments. Here, the selection is at the dataset level — favorable examples, not favorable slices of a larger dataset.
- **Small-Sample Rate Extremes (metrics/):** addresses automated systems acting on low-n rates. Here, the audience is human — readers, stakeholders, investors — who lack the statistical literacy to demand the missing context.
- **Overconfidence in Small Samples (cognitive biases):** a cognitive error by the consumer. Here, the focus is on the producer who exploits that error by choosing a format that triggers it.

### Visualization (canvas `c6`, 720×300)

Scatter plot: the five related practices positioned by where in the reporting pipeline the distortion happens vs how deliberate it is ("Where This Sits Among Related Practices").

- **Title (bold 15px, top center, `#1a5276`):** "Where This Sits Among Related Practices".
- **Chart area:** left 70, right w−50, top 50, bottom 225; x = four evenly spaced pipeline-stage columns with 11px `#666` labels below ("Data selection", "Computation", "Publication format", "Reader cognition"); y = deliberateness of the distortion 0–10, horizontal gridlines `#eee` at 0/2/4/6/8/10 with 11px `#999` tick labels; 11px `#666` y-axis end labels "deliberate" (top) and "accidental" (bottom).
- **Highlight band:** light red vertical band `rgba(231,76,60,0.05)` behind the "Publication format" column.
- **Points (practice, stage, deliberateness, color; bold 12px name in point color above dot, 11px `#555` one-line descriptor below dot):**
  - "Dashboard Cherry-Pick", Data selection, 8, orange `#e67e22`: "picks favorable slices".
  - "Denominator Trick", Computation, 9, purple `#8e44ad`: "shrinks the denominator".
  - "Small-Sample Extremes", Computation, 2, blue `#2980b9`: "automated low-n rates".
  - "Unqualified %", Publication format, 6, red `#e74c3c`: "hides that n was small" — radius 8 with outer ring (others radius 6).
  - "Overconfidence Bias", Reader cognition, 1, green `#27ae60`: "reader misjudges small n".
- **Insight annotation (bold 13px red, centered below stage labels):** "Only Unqualified % distorts at the publication step — the number is true; the format deceives".
- **Caption (italic 12px `#666`, bottom center):** *"Each exploits a different stage: data selection, computation, publication format, or reader cognition."*

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` per section (6 rows); left `<td>` (40%) holds `.obj-title` + bullets/paragraphs or `.example-box` divs, right `<td>` (60%, centered) holds the canvas. Section 4 uses four `.example-box` divs (background `#f8fafb`, border `1px solid #e0e0e0`, radius 6px, padding 12px 16px, 0.88em; `.ex-title` bold 700 `#1a5276`).
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; subtitle `#666` 1.0em; ul 0.9em `#333`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c` (dark `#c0392b`), orange `#e67e22`, purple `#8e44ad`, gray text `#666`/`#333`.
