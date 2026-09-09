# Research Publications Domain: Data Pitfalls

**Page type:** detail page (one h2 per pitfall, each followed by a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** Research Publications Domain: Data Pitfalls

**Subtitle:** How publication incentives — selective publishing, p-hacking, citation gaming, and novelty chasing — systematically distort the scientific record.

## Publication Bias / File Drawer Problem

- Journals preferentially publish positive/significant results (p < 0.05)
- Negative results (null findings) stay in researchers' file drawers, never published
- Published literature is a systematically biased sample of all research conducted
- Meta-analyses based on published studies overestimate true effect sizes by 30-50%
- Funnel plot asymmetry reveals missing negative studies but correction is imperfect
- Estimated that for every published positive result, 4-5 negative replications go unpublished

**Example:** In antidepressant trials, 94% of published studies showed positive results. When FDA obtained ALL registered trials (published + unpublished), only 51% were positive. The published literature created a false impression of universal efficacy.

### Visualization (canvas `canvas1`, 720×300)

Funnel plot scatter: effect size (x) vs study size (y), with the lower-left quadrant conspicuously empty.

- **Title (17px `#1a5276`, centered):** "Funnel Plot: Publication Bias Visible".
- **Plot area:** left=80, right=w-40, top=35, bottom=h-30; solid axes `#2c3e50` width 1.5.
- **Axis labels (12px `#2c3e50`):** "Effect Size" centered below x-axis; "Study Size (n)" rotated on y-axis. X-axis markers: "-0.5" at 10% width, "0" at center, "0.3" at 65% width, "0.8" near right.
- **Funnel outline:** dashed gray triangle (`#7f8c8d`, dash 5/4, width 1.5) with apex at x-fraction 0.65 near the top (true effect) and base spanning x-fractions 0.2 to 0.95 near the bottom.
- **Large studies (blue `#2980b9` dots, radius 4, clustered near true effect at top)** — (x-fraction, y-fraction) points: [0.60, 0.85], [0.63, 0.90], [0.67, 0.88], [0.65, 0.92], [0.62, 0.80], [0.68, 0.82], [0.64, 0.95].
- **Small studies, right side only (green `#27ae60` dots, radius 4 — positive effects published):** [0.70, 0.20], [0.75, 0.25], [0.80, 0.15], [0.72, 0.30], [0.78, 0.18], [0.85, 0.22], [0.68, 0.35], [0.73, 0.12], [0.82, 0.28], [0.76, 0.38], [0.90, 0.10], [0.69, 0.42].
- **Missing-region label (11px red `#e74c3c`, centered at x-fraction 0.28 in the lower-left):** two lines "Missing negative studies" / "(file drawer)".

## P-Hacking in Published Results

- Researchers try multiple analyses (different subgroups, covariates, outcomes, exclusion criteria)
- Report only the one that achieves p < 0.05 — "researcher degrees of freedom"
- With 20 tests at alpha=0.05, probability of at least one false positive = 64%
- Garden of forking paths: each analytical choice multiplies the false positive rate
- ~60% of psychology findings failed replication in the Reproducibility Project (2015) — only ~36% replicated
- HARKing: Hypothesizing After Results are Known — presenting exploratory findings as confirmatory

**Example:** A nutrition study tests the effect of chocolate on 20 health outcomes. One hits p=0.03 (weight loss). Paper title: "Chocolate Accelerates Weight Loss" with no mention of the 19 null results. This is exactly how a real sting operation fooled major media outlets.

### Visualization (canvas `canvas2`, 720×300)

Branching decision-tree diagram: raw data forking into 3, then 6, then 12 analysis paths, with the two "significant" leaves highlighted.

- **Title (17px `#1a5276`, centered):** "Garden of Forking Paths → Cherry-picked p < 0.05".
- **Start node:** blue circle (`#2980b9`, radius 14) at x=50, mid-height+10, with white 9px two-line label "Raw" / "Data".
- **Level 1 (x = start+130):** 3 purple nodes (`#8e44ad`, radius 8) at mid-height -55/0/+55, connected by blue lines (`#2980b9`, width 1.5); stage label above (10px `#1a5276`): "Exclude outliers?".
- **Level 2 (x = +110 further):** 6 orange nodes (`#e67e22`, radius 6), 2 per level-1 node at ±20px offsets, connected by purple lines width 1; stage label: "Which covariates?".
- **Level 3 (x = +110 further):** 12 terminal nodes (radius 6), 2 per level-2 node at ±12px offsets, connected by orange lines width 1; stage label: "Which outcome?". Terminal nodes at indices 3 and 7 are green `#27ae60` with a 9px green label "p<.05" to their right; the other 10 are gray `#7f8c8d`.
- **Callout:** red arrow (`#e74c3c`, width 2, with filled arrowhead) pointing at the first significant node, with 11px red label "Report this one!".

## Citation Gaming / Manipulation

- Citation rings: groups of researchers systematically cite each other to inflate metrics
- Self-citation can account for 10-35% of total citations for some authors
- Journal impact factor manipulated by editorial practices (review articles, forced citations)
- H-index gameable through salami-slicing (splitting one study into many papers)
- Citation count ≠ quality or impact; highly-cited papers include many later-retracted ones
- Coercive citation: journals requiring authors to add citations to that same journal

**Example:** In 2020, a citation ring of 16 researchers in Saudi Arabia was discovered — they had collectively cited each other 700+ times, inflating their h-indices from ~5 to ~35. None were sanctioned for over a year because the gaming was technically within journal rules.

### Visualization (canvas `canvas3`, 720×300)

Network diagram: a six-node citation ring with mutual citation arrows, plus an actual-vs-gamed h-index comparison on the right.

- **Title (17px `#1a5276`, centered):** "Citation Ring: Mutual Inflation Network".
- **Ring:** 6 blue nodes (`#2980b9`, radius 16, white 12px labels A–F) evenly spaced on a circle of radius 60 centered at (0.4w, h/2+15), starting at top.
- **Ring arrows:** solid red arrows (`#e74c3c`, width 2.5, with arrowheads) from each node to the next around the ring; plus 3 dashed red cross-ring lines (dash 3/3, width 1.5) linking opposite nodes: A–D, B–E, C–F.
- **Center text (10px red, two lines):** "700+ mutual" / "citations".
- **Right comparison (starting x = 0.72w):** green `#27ae60` 13px "Actual impact:" over 17px "h = 5"; red 13px "Gamed impact:" over 17px "h = 35"; red vertical arrow between them with 14px label "↑ 7x".

## Reproducibility Crisis

- Code not shared (or runs only on author's machine with unlisted dependencies)
- Data not shared (proprietary, lost, or "available upon request" but never provided)
- Random seeds not fixed → stochastic results differ between runs
- Compute environment differences (GPU versions, library versions, floating point)
- 70% of researchers have tried and failed to reproduce another scientist's experiments (Nature 2016 survey)
- "Same experiment" produces different results → which run was published? The best one.

**Example:** A landmark deep learning paper claimed SOTA results. 5 independent labs tried to reproduce it over 6 months. Results ranged from -15% to +3% of claimed performance. The original authors eventually revealed they reported the best of 200 training runs without mentioning the other 199.

### Visualization (canvas `canvas4`, 720×300)

Horizontal dot plot: the original claim vs five labs' reproduction results on a 75–100% scale.

- **Title (17px `#1a5276`, centered):** "Reproduction Attempts: Same Paper, Different Results".
- **Plot area:** left=120, right=w-50, top=38, bottom=h-25; x-axis from 75 to 100 with "%" labels every 5 (10px `#2c3e50`).
- **Rows (right-aligned 12px `#2c3e50` labels, light `#ecf0f1` horizontal gridlines):** "Original claim" 94, "Lab A" 82, "Lab B" 79, "Lab C" 88, "Lab D" 85, "Lab E" 97.
- **Markers:** the original claim is a red `#e74c3c` star ("★", 20px); labs are blue `#2980b9` filled circles radius 6.
- **Reference line:** vertical dashed red line (dash 5/4, width 2) at 94% (the original claim).
- **Gap annotation:** orange `#e67e22` 11px label "Reproducibility gap" centered at 87% near the top, with a double-headed orange arrow spanning 80% to 93%.

## Retraction Lag and Citation Persistence

- Average time from publication to retraction: 2-3 years (some take 10+ years)
- Fraudulent/erroneous papers accumulate hundreds of citations before retraction
- Post-retraction citations continue for years — papers keep citing retracted work
- Only 4% of post-retraction citations acknowledge the retraction
- Knowledge built on retracted papers persists in textbooks, reviews, and downstream papers
- Retraction notices are poorly propagated — not surfaced by academic search engines or many databases

**Example:** The Lancet MMR-autism paper (Wakefield 1998) was cited 900+ times before retraction in 2010. It continues to be cited 40+ times per year AFTER retraction. The anti-vaccine movement it spawned persists decades later despite the foundational paper being fraudulent.

### Visualization (canvas `canvas5`, 720×300)

Line chart: cumulative citations over 15 years, still rising after a retraction line at year 8.

- **Title (17px `#1a5276`, centered):** "Citations Continue Long After Retraction".
- **Plot area:** left=70, right=w-40, top=40, bottom=h-30; solid axes `#2c3e50` width 1.5.
- **Axis labels (11px `#2c3e50`):** "Years" centered below; "Cumulative Citations" rotated on y-axis. X-axis ticks 0, 3, 6, 9, 12, 15 with light vertical gridlines `#ecf0f1`.
- **Curve (blue `#2980b9`, width 2.5, sampled every 0.5 yr):** for yr ≤ 8, citations = 500·(1 − e^(−0.4·yr)); after yr 8, citations = 500·(1 − e^(−3.2)) + (yr − 8)·28. Y scaled to max 700.
- **Retraction line:** vertical dashed red (`#e74c3c`, dash 6/4, width 2) at year 8, labeled above in 12px red: "RETRACTION".
- **Annotations (11px):** dark blue `#1a5276` two lines at left: "500 citations before" / "retraction"; orange `#e67e22` two lines right of the retraction line: "200+ citations AFTER" / "retraction"; gray `#7f8c8d` 10px right-aligned near bottom: "Only 4% note the retraction".

## Novelty Bias vs Incremental Truth

- Journals strongly reward "novel" and "surprising" findings over confirmatory replications
- Researchers overstate novelty and frame results as paradigm-shifting to get published
- Field lurches between contradictory "breakthroughs" instead of gradually converging on truth
- Boring but important confirmatory studies have ~3x lower acceptance rates
- Incentive structure: career advancement requires high-impact "first" discoveries
- Results in contradictory literature: coffee causes/prevents cancer (depending on which paper)

**Example:** Nutritional epidemiology has produced contradictory findings on virtually every food. Wine, eggs, coffee, butter, chocolate — each has been declared both harmful and beneficial in high-impact journals. The field advances through contradictory headlines rather than converging on nuanced dose-response truths.

### Visualization (canvas `canvas6`, 720×300)

Zigzag line chart: published "breakthrough" findings oscillating between beneficial and harmful, against a flat dashed truth line.

- **Title (17px `#1a5276`, centered):** "Novelty Bias: Published 'Breakthroughs' vs Stable Truth".
- **Plot area:** left=80, right=w-30, top=40, bottom=h-25; solid axes `#2c3e50` width 1.5.
- **Y-axis labels (10px, right-aligned):** "Beneficial" at top, "Neutral" at middle, "Harmful" at bottom. **X-axis labels:** years 2000, 2004, 2008, 2012, 2016, 2020, 2024 evenly spaced.
- **Truth line:** horizontal dashed gray (`#7f8c8d`, dash 4/3, width 2) just above mid-height, labeled in 10px gray: "Actual truth (nuanced, boring)".
- **Zigzag line (red `#e74c3c`, width 2.5)** through (x-fraction, y-fraction where 1=beneficial top): [0, 0.3], [0.08, 0.8], [0.16, 0.2], [0.25, 0.85], [0.33, 0.15], [0.42, 0.75], [0.50, 0.25], [0.58, 0.9], [0.67, 0.1], [0.75, 0.7], [0.83, 0.3], [0.92, 0.8], [1.0, 0.2].
- **Peak/valley labels (9px red, offset above peaks / below valleys):** "Breakthrough!" at (0.08, 0.8); "New finding!" at (0.25, 0.85); "Breakthrough!" at (0.58, 0.9); "New finding!" at (0.33, 0.15); "Reversal!" at (0.67, 0.1).
- **Topic label (11px purple `#8e44ad`, top-left of plot):** "Coffee & Health".
- **Bottom annotation (10px `#1a5276`, centered):** "Headlines oscillate; truth is stable but unpublishable".

## Regeneration instructions

- **Layout:** per pitfall, an `<h2>` section heading followed by a single-row `.obj-table`: full-width table, left `<td>` (40%) with `.obj-title` (repeating the h2 text), a `<ul>` of bullets, and an `.example` callout div (`<strong>Example:</strong>` + text); right `<td>` (60%, centered) holds one canvas 720×300.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px solid `#2980b9` bottom border; subtitle `#666` 1.05em; table cell borders `1px solid #e0e0e0`, padding 20px 24px, even rows `#fafcfe`; `.obj-title` 1.05em weight 600 `#1a5276`; bullets 0.9em `#333`; `strong` `#1a5276`; `.example` background `#eaf2f8`, padding 10px 14px, radius 6px, 0.92em. A `.philosophy` style (background `#f0f4f8`, left border `4px solid #2980b9`) is defined but unused. No nav bar, no back/home links.
- **Canvas:** each canvas declares intrinsic `width="720" height="300"`; scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper. Chart titles are 17px (not bold) and centered with `textAlign='center'`.
- **Palette:** primary blue `#1a5276`, mid blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, dark text `#2c3e50`, gray `#7f8c8d`, gridlines `#ecf0f1`.
