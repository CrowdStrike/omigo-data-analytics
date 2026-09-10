# Beat the Metric

**Page type:** detail page (two-column obj-table layout: text left 50%, canvas right 50%, one row per section)
**HTML title tag:** Beat the Metric — Common Bad Practices

**Subtitle:** Metric Manipulation — Engineering, science, and design optimized to pass the test rather than improve the system. Goodhart's Law weaponized: "When a measure becomes a target, it ceases to be a good measure."

## Section 1: The Practice

- System has a defined evaluation metric (accuracy, emissions limit, test score, latency SLA).
- Instead of improving the underlying system, engineer specifically to pass the test condition.
- System performs well on the metric, poorly on everything the metric was supposed to proxy for.
- The metric stops measuring what it was designed to measure — but nobody updates it because "we're hitting targets."

### Visualization (canvas `c1`, 720×320)

Dual line chart: metric score rising while actual system quality diverges downward.

- **Title (bold 15px, `#1a5276`, top center):** "Metric Score vs Actual System Quality".
- **Plot area:** left 80, right w-60, bottom 260, top 45; `#ccc` L-shaped axes width 1.
- **X labels (13px `#666`):** v1…v8, evenly spaced.
- **Data (y scale 0–1):** metric = `[0.60, 0.68, 0.75, 0.82, 0.88, 0.92, 0.94, 0.96]`; quality = `[0.58, 0.65, 0.70, 0.72, 0.71, 0.68, 0.65, 0.62]`.
- **Divergence area:** region between the two lines filled `rgba(231,76,60,0.12)`.
- **Metric line:** green `#27ae60`, width 3, 4px green dots. **Quality line:** red `#e74c3c`, width 3, 4px red dots.
- **Y labels (right-aligned `#666`):** "0%", "50%", "100%".
- **Legend (top-right):** 14×14 green swatch + 13px `#333` "Test metric score"; 14×14 red swatch + "Actual system quality".
- **Annotation (italic 12px red, centered):** "← divergence grows →" between the lines at x-step 6.5.
- **Caption (italic 13px `#666`, bottom center):** "Each iteration optimizes for the test. System quality degrades after v3."

## Section 2: Real-World Examples

**Volkswagen Dieselgate (2015)** (example box)
Software detected EPA test conditions (wheels spinning, steering idle) and switched to low-emission mode. Passed the test at 40x actual real-world NOx emissions. Cars "beat the metric" while poisoning air for 11 million drivers.

**Teaching to the Test** (example box)
Schools drill students on specific test patterns to raise standardized scores. Scores rise, but students can't apply knowledge to novel problems. The metric (test score) improves; the thing it measures (learning) doesn't.

**Wells Fargo Fake Accounts (2016)** (example box)
Target: accounts opened per employee. Solution: open 3.5 million fake accounts without customer consent. Metric soared. Actual customer value: negative. $3B in fines.

**Cobra Effect — Colonial India** (example box)
British government offered bounties for dead cobras. People bred cobras to kill them for the bounty. When the program was cancelled, breeders released their stock. Cobra population increased.

**Public Ratings & Rankings** (example box)
University rankings score selectivity, alumni giving, and spending per student — so schools game those inputs (reject more applicants, reclassify spending) rather than improving education. The product stays the same; the score improves.

**Tokenmaxxing (AI Products, 2024–)** (example box)
"AI adoption" gets measured by token consumption, so teams run agents that re-scan codebases and regenerate context just to pump the count. Millions of tokens burned; the dashboard leads while the shipped code is unchanged.

**Pentesting as Security Theater** (example box)
An annual pentest covers a defined scope, and only those findings get fixed. The metric ("pentest findings resolved") goes to zero while everything outside the tested surface stays untested and unknown.

**Code Review as Rubber-Stamping** (example box)
Compliance requires 100% of code reviewed before merge, so approvals arrive in under 60 seconds on 500-line diffs. Coverage reads 100%; the defect-catching benefit approaches zero.

**Unit Test Coverage Targets** (example box)
A 100% line-coverage target invites tests that execute every line but assert nothing meaningful. The report is green while the critical paths go untested.

### Visualization (canvas `c2`, 720×340)

Two-bar comparison: VW test-mode vs real-world emissions against the EPA limit line.

- **Title (bold 15px, `#1a5276`, top center):** "Volkswagen: Test Mode vs Real-World Emissions".
- **Chart area:** left 100, right w-60, bottom 280, top 50; y scale max 1.5 g/mile.
- **Left bar ("Test Mode"):** value 0.043 g/mi, centered at x = w/2 − 160, width 120, fill `#27ae60`, stroke `#1f8c4e` width 1.5. Below (centered): bold 14px green "Test Mode"; 13px `#666` "0.043 g/mi"; "✓ PASSES".
- **Right bar ("Real Driving"):** value 1.3 g/mi, centered at x = w/2 + 160, width 120, fill `#e74c3c`, stroke `#c0392b` width 1.5. Below: bold 14px red "Real Driving"; 13px `#666` "1.3 g/mi (≈30x limit)"; "✗ TOXIC".
- **EPA limit line:** horizontal dashed (6/4) orange `#e67e22` width 2 at 0.07 g/mile, spanning slightly past the chart; bold 13px orange label "EPA Limit: 0.07 g/mi" at the left end (clear of the bars), 10px above the line.
- **Center annotation (14px `#333`, three stacked centered lines at ~35% chart height):** "Same car." / "Software detects test." / "Switches behavior."
- **Center ratio (bold 22px red, ~70% chart height):** "~30×".
- **Caption (italic 13px `#666`, bottom center):** "The car was engineered to beat the test, not to reduce emissions."

## Section 3: In ML/Data Science

- **Overfitting to the test set:** Model tuned until it scores 95% on held-out eval — but eval set doesn't represent production distribution. Deployed model fails on real data.
- **Optimizing proxy metrics:** Maximize click-through rate → get clickbait. Maximize engagement → get outrage content. Maximize watch time → get addictive loops. The proxy diverges from the goal.
- **Adversarial test gaming:** Know the test suite? Engineer features that exploit test data patterns. Model performs well on benchmark, collapses on slightly different inputs.
- **Benchmark hacking in research:** Tune architecture and hyperparams on the "held-out" test set of a public benchmark. Paper reports SOTA. Model doesn't generalize to any other dataset.

### Visualization (canvas `c2b`, 720×300)

Grouped bar chart summarizing all example boxes: reported metric vs actual goal, per example.

- **Title (bold 15px, `#1a5276`, top center):** "Same Shape Everywhere: The Metric Rises, the Goal Does Not".
- **Groups (6, two-line 12px `#333` x labels):** Emissions/test, Test/scores, Accounts/opened, Cobra/bounties, Token/count, Test/coverage.
- **Bars (width 30 per pair):** reported metric fill `rgba(26,82,118,0.75)` values `[95, 85, 100, 90, 100, 100]`; actual goal fill `rgba(231,76,60,0.8)` values `[15, 30, 5, 10, 20, 15]` (percent of plot height; baseline y=236, top y=62).
- **Legend (top, inline):** blue swatch "Reported metric"; red swatch "Actual goal" (13px `#333`).
- **Caption (italic 13px `#666`, bottom center):** "Illustrative magnitudes — each example reports success while the underlying goal stays flat."

### Visualization (canvas `c3`, 720×320)

Grouped bar chart: benchmark score vs real-world accuracy for five models.

- **Title (bold 15px, `#1a5276`, top center):** "ML: Benchmark Score vs Real-World Generalization".
- **Plot area:** left 80, right w-60, bottom 260, top 50; horizontal grid lines `#eee` at 0.5–1.0 every 0.1; y scale maps 0.4–1.0 onto plot height.
- **Models (x labels, 12px `#333`, two-line for last):** "Baseline", "Model A", "Model B", "Model C", "Model D" / "(SOTA)".
- **Benchmark bars (left of each pair, width 35):** fill `rgba(39,174,96,0.7)`, stroke `#1f8c4e`; values `[0.72, 0.78, 0.84, 0.89, 0.93]`.
- **Real bars (right of each pair, width 35):** fill `rgba(231,76,60,0.7)`, stroke `#c0392b`; values `[0.70, 0.74, 0.73, 0.69, 0.61]`.
- **Y labels (12px `#666`, right-aligned):** "50%", "60%", "70%", "80%", "90%", "100%".
- **Legend (top-left, inside plot area):** green swatch "Benchmark score"; red swatch "Real-world accuracy" (13px `#333`).
- **Annotation on Model D (italic 11px red, centered, two stacked lines above its benchmark bar):** '"SOTA" on paper' / "worst in practice".
- **Caption (italic 13px `#666`, bottom center):** "More iterations tuning to the benchmark = worse generalization."

## Section 4: Why It Persists

- **Incentive alignment:** Person is rewarded for metric, not outcome. Rational to optimize the reward signal.
- **Measurement lag:** Real outcome takes months/years to observe. Metric available now. Short-term metric wins attention.
- **Complexity of root cause:** Actually improving the system is hard, uncertain, multi-quarter. Gaming the metric is fast, predictable, promotable.
- **Audit theater:** Auditors check the metric, not the system. If number green, ship it.

### Visualization (canvas `c4`, 720×280)

Staircase timeline: three metric lifecycles over 12 quarters — each cycle the metric is introduced, optimized to its target, then redefined and reset — while the real goal line sinks underneath.

- **Title (bold 15px, `#1a5276`, top center):** "The Beat-the-Metric Treadmill".
- **Plot area:** left 65, right w-50, bottom 212, top 45; `#ccc` L-shaped axes width 1; horizontal `#eee` gridlines at y-values 50 and 100.
- **X labels (12px `#666`):** Q1…Q12, evenly spaced (12 points). Below them (bold 12px `#666`, centered over each 4-quarter span): "Cycle 1", "Cycle 2", "Cycle 3".
- **Y labels (12px `#666`, right-aligned):** "0", "50", "100" (y scale 0–100).
- **Data (deterministic, hardcoded):**
  - gamed metric = `[50, 65, 78, 80, 58, 72, 85, 90, 66, 80, 92, 95]` — three rising segments (quarters 1–4, 5–8, 9–12), each ending exactly on its cycle target;
  - real goal = `[48, 49, 50, 50, 49, 48, 47, 46, 45, 44, 43, 42]` — flat then steadily declining;
  - cycle targets = `[80, 90, 95]`.
- **Target segments:** per cycle, horizontal dashed (5/4) orange `#e67e22` width 1.5 line at the target value spanning that cycle's four quarters; 11px orange label "target 80"/"target 90"/"target 95" centered 6px above each segment.
- **Gamed metric line:** green `#27ae60`, width 3, drawn as three separate 4-point segments (one per cycle) with 4px green dots — the peaks step up cycle over cycle.
- **Reset connectors:** dashed (4/4) `#999` width 1.5 diagonal from each cycle's peak down to the next cycle's start; italic 11px `#999` label "redefined" right-aligned just left of each connector midpoint.
- **Real goal line:** red `#e74c3c`, width 3, continuous across all 12 quarters with 3.5px red dots.
- **Legend (inline row under the title):** 14×12 green swatch + 13px `#333` "Gamed metric"; 14×12 red swatch + "Real goal".
- **Insight annotation (bold 13px `#e74c3c`, two centered lines in the gap between the lines over quarters 9–11):** "Every target hit —" / "the real goal only falls".
- **Caption (italic 13px `#666`, bottom center):** "Each redefinition resets the game: the gamed line climbs the staircase, the goal line sinks."

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table with `border-collapse: collapse`, one `<tr>` per section; left `<td>` (50%) holds `.obj-title` + bullets or `.example-box` divs, right `<td>` (50%, centered) holds the canvas(es). Section 2's left cell uses nine `.example-box` divs, each with a `.ex-title` heading line; its right cell stacks `c2` and `c2b`.
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#666` 1.0em; `ul` 0.9em `#333`; `strong` `#1a5276`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `.example-box` background `#f8fafb`, border `1px solid #e0e0e0`, radius 6px, padding 12px 16px, margin 10px 0, 0.88em; `.ex-title` weight 700 `#1a5276`. Inline `code` in the Unit Test Coverage box. No nav bar, no back/home links.
- **Canvas:** intrinsic `width`/`height` attributes as given per chart; `canvas { display: block; margin: 0 auto; width: 100%; height: auto; }`. Sharp-rendering pattern: shared `setup(id)` helper caps display at the logical width (`style.maxWidth = w px`), sizes the backing store to rendered CSS width × `devicePixelRatio`, and `ctx.scale`s accordingly; chart draw functions are pushed into a `__charts` array, run on load, and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60` (dark stroke `#1f8c4e`), red `#e74c3c` (dark stroke `#c0392b`), orange `#e67e22`, purple `#8e44ad`, gray text `#666`/`#333`.
- Card links elsewhere pointing to this page use the `.html` extension in regenerated HTML.
