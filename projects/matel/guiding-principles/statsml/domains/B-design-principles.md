# Design Principles from Domain Failures

**Page type:** other (single-page long doc: h2 per pattern, each with 2-3 green rule callout boxes followed by one canvas; no tables, no cards)
**HTML title tag:** Design Principles from Domain Failures

**Subtitle:** Each pattern from doc 33 → a concrete engineering rule for system design. Not "be aware" — but "build it THIS way."

## From Pattern 1: Measurement Creates Reality

**DESIGN RULE** — **Never use your own system's output as a training signal without a randomized holdout.**
If your ranking creates the clicks you train on, you can't learn true relevance. Reserve 5-10% of traffic for random ranking to measure organic signal.

**DESIGN RULE** — **Log the POSITION/CONTEXT alongside every engagement event.**
A click at position 1 means less than a click at position 10. Without position, you can't debias. Store: what you showed, where, when — not just "they clicked."

**DESIGN RULE** — **For any predictive model that triggers action: measure outcomes for a held-out group where you DON'T act.**
If "predicted churn → intervention → no churn" is your only data, you can't tell if the model prevented churn or created loyalty theater. Holdout proves causation.

### Visualization (canvas `dp1`, 720×160 as drawn; HTML attribute 720×260)

Two-panel diagram: contaminated loop vs clean loop with holdout, split by a vertical gray divider at x=360.

- **Left panel:** heading (17px `#1a5276`): "Contaminated Loop". A red (`#e74c3c`, 3px) circle centered (120, 90), radius 40, with a small arrow and labels "Train" (top) and "Serve" (bottom) in red; a thick 4px red X drawn across the circle.
- **Right panel:** heading: "Clean Loop with Holdout". Green (`#27ae60`, 3px) broken loop (two half-arcs joined by verticals) centered around (500, 90); labels "Train" / "Serve" in `#1a5276`. A solid green rect (580, 60, 80×50) with white 14px text: "5% Random" / "Holdout".

## From Pattern 2: Survivorship Bias

**DESIGN RULE** — **Before any analysis, answer: "What data DOESN'T exist because it failed/died/left before being recorded?"**
If your dataset is filtered by survival (active customers, existing funds, sold properties), your conclusions only apply to survivors — not the population you want to serve.

**DESIGN RULE** — **Archive data from churned/failed/deleted entities BEFORE removing them from production tables.**
Once a customer churns, their data gets purged. Now you can't study WHY they churned. Maintain a "graveyard" table of departed entities with pre-departure features.

### Visualization (canvas `dp2`, 720×160 as drawn; HTML attribute 720×260)

Two-table diagram: production table feeding a graveyard archive.

- **Left:** heading (17px `#1a5276`): "Production Table"; `#1a5276` 2px rect (50, 40, 200×90) containing a 3×4 grid of green (`#27ae60`) 35×18 cells; two red (`#e74c3c`) cells in the bottom row marked "X" (departed entities).
- **Arrow:** orange `#e67e22` 3px arrow from (270, 80) to (360, 80), labeled above in orange: "Archive FIRST".
- **Right:** heading: "Graveyard Archive"; orange 2px rect (400, 40, 240×90) containing a 2×4 grid of red 45×25 cells with white 12px labels "churn" and "data".

## From Pattern 3: Metric Gaming (Goodhart's Law)

**DESIGN RULE** — **Use at least 3 independent metrics. If one improves while others degrade: gaming detected.**
Any single metric can be gamed. But gaming one at the expense of others creates a detectable signature. Monitor metric CORRELATION — when it breaks, someone is gaming.

**DESIGN RULE** — **Before choosing a metric, ask: "If someone optimized ONLY this number, what would break?"**
"Minimize handle time" → agents hang up on customers. "Maximize accuracy" → model predicts majority class. Pre-mortem the metric before deploying it.

**DESIGN RULE** — **Track a counter-metric for every primary metric.**
Primary: conversion rate. Counter: return rate. Primary: model accuracy. Counter: confidence calibration. If primary improves and counter degrades → the improvement is an illusion.

### Visualization (canvas `dp3`, 720×160 as drawn; HTML attribute 720×260)

Three semicircular gauges side by side.

- **Header (15px `#e74c3c`, top center):** "Gaming Detected: Primary up, Counter down".
- **Gauge 1** (center (120, 100), radius 45, 6px arc): red `#e74c3c`, needle pointing up-right; labels: "Primary" (`#1a5276`), "95% ⚠" (red).
- **Gauge 2** (center (360, 100)): orange `#e67e22`, needle pointing up-left; labels: "Counter" (`#1a5276`), "35% ↓" (orange).
- **Gauge 3** (center (600, 100)): green `#27ae60`, needle pointing up-right; labels: "Secondary" (`#1a5276`), "82% ✓" (green).

## From Pattern 4: Absence = Signal

**DESIGN RULE** — **For every column with missing values: create an "is_missing" binary feature. NEVER impute without testing if missingness itself is predictive.**
In healthcare, "lab not ordered" = doctor thinks patient is healthy. Imputing with mean makes healthy look sick. The is_missing feature may be your strongest predictor.

**DESIGN RULE** — **Log failed/dropped/abandoned interactions, not just successful completions.**
IVR dropout, search with no click, page load timeout, app crash — all carry intent information that "success-only" logging misses. The frustrated majority is your biggest opportunity.

### Visualization (canvas `dp4`, 720×160 as drawn; HTML attribute 720×260)

Vertical bar chart of feature importances with is_missing on top.

- **Title (17px `#1a5276`, top center):** "Feature Importance".
- **Bars** (width 80, spacing 120, starting x=60, baseline y=140, height = value×200; value printed above each bar in 16px of its color, name below in 14px `#333`):
  - is_missing — 0.42 — `#e67e22`
  - income — 0.28 — `#1a5276`
  - age — 0.15 — `#1a5276`
  - location — 0.10 — `#1a5276`
  - gender — 0.05 — `#1a5276`
- **Annotation:** orange 3px arrow rising from the is_missing bar, labeled (15px `#e67e22`): "Missingness #1!".

## From Pattern 5: Marginal → Whole Extrapolation

**DESIGN RULE** — **Always report the N backing any estimate. A "90% accuracy" from n=10 is not a measurement — it's an anecdote.**
Confidence interval from n=10: [55%, 99%]. From n=1000: [88%, 92%]. The number alone is meaningless without its sample size. Build CI display into every metric dashboard.

**DESIGN RULE** — **For any aggregate metric (average, total, market cap): compute "what would happen if 10× the current volume tried to realize this value?"**
Market cap assumes all shares sell at current price. Average assumes representative sample. Stress-test: would the metric survive if participation increased? If not, it's a marginal observation, not a stable property.

### Visualization (canvas `dp5`, 720×160 as drawn; HTML attribute 720×260)

Confidence-interval comparison plot: same point estimate, two very different interval widths.

- **Title (17px `#1a5276`, top center):** "Confidence Intervals: Same 85% Estimate".
- **Scale:** vertical `#999` 1px gridlines at 50%, 60%, 70%, 80%, 90%, 100% (labels 12px `#999` above), mapped at 400px per unit proportion starting at 55% = x=180.
- **n=10 interval (y=90):** red `#e74c3c` 3px horizontal bar with end caps spanning [55%, 99%], red 6px dot at the 85% estimate; row label "n=10" (`#1a5276`); value label "[55%, 99%]" (14px red).
- **n=1000 interval (y=130):** green `#27ae60` 3px bar spanning [82%, 88%], green dot at 85%; row label "n=1000"; value label "[82%, 88%]" (14px green).

## From Pattern 6: Adversary Adapts

**DESIGN RULE** — **In adversarial domains: retrain weekly minimum. Model performance = decaying asset.**
Spam, fraud, cybersecurity, SEO — adversaries study your defenses and evolve. A 6-month-old model in these domains is already significantly degraded. Build continuous retraining into the system, not as an afterthought.

**DESIGN RULE** — **Use behavioral features (what IS this entity doing?) over signature features (does this match known bad?).**
Signatures are trivially evaded (change one byte → new hash). Behavior is harder to fake (attacker using PowerShell must still MOVE laterally, which looks different from admin usage regardless of tool).

**DESIGN RULE** — **Before deploying a detection rule, ask: "If the adversary KNEW this rule, how easily could they evade it?"**
If evasion requires one minor change → rule will be defeated within days. If evasion requires fundamentally changing attack economics → durable rule. Prefer features the adversary CAN'T cheaply change.

### Visualization (canvas `dp6`, 720×160 as drawn; HTML attribute 720×260)

Timeline comparing retrain cadence to attack evolution cadence.

- **Title (17px `#1a5276`, top center):** "Retrain vs Attack Evolution Speed".
- **Timeline:** `#999` 2px horizontal line at y=80 from x=50 to x=670, with week tick marks labeled "W1"…"W5" (13px `#666`) every 155px.
- **Retrain events:** five solid green (`#27ae60`) 12px circles on the timeline at each week mark, each with a short green stem below; legend (15px green): "Model Retrain (weekly)".
- **Attack evolution:** 28 thin red (`#e74c3c`) 4×25 ticks above the timeline every 22px; legend (15px red): "Attack Evolution (daily)".
- **Annotation (16px `#e67e22`, bottom right):** "Attacker adapts 7× faster than defense!".

## From Pattern 7: Feedback Loops

**DESIGN RULE** — **Inject 5-10% exploration (random/diverse exposure) to break recommendation loops.**
Without exploration, you only observe the effect of your OWN recommendations — never what would happen with different content. The system converges to a local optimum that may be globally terrible.

**DESIGN RULE** — **Monitor for "model confidence increasing over time without external validation" — this is a feedback loop symptom.**
If your model gets more confident each retraining cycle without new ground truth, it's learning from its own predictions. Confidence should be VALIDATED against holdout, not derived from consistency with past predictions.

### Visualization (canvas `dp7`, 720×160 as drawn; HTML attribute 720×260)

Flow diagram: model output split into a model-driven path and a random exploration path.

- **Title (17px `#1a5276`, top center):** "Exploration Injection".
- **Model box:** solid `#1a5276` rect (80, 60, 100×50) with white 16px label "Model", connected by a `#1a5276` 3px line to a split point at (250, 85).
- **90% path:** red `#e74c3c` 4px arrow branching up-right, labeled in red: "90%" and "Model-driven".
- **10% path:** green `#27ae60` 4px arrow branching down-right, labeled in green: "10%" and "Random/Diverse".
- **Symbol:** thick green X at approximately (470, 130), labeled (15px green): "Breaks Loop".

## From Pattern 8: Temporal Contamination

**DESIGN RULE** — **For every feature: document "what is the latest event used in this computation?" If answer > prediction time: it's leakage.**
The universal test. Apply to every feature, every transform, every aggregation. If ANY component uses information from after the prediction point, the entire feature is contaminated.

**DESIGN RULE** — **Use point-in-time databases. Store what was KNOWN at each date, not what is known NOW.**
Financial restatements, diagnosis code updates, address changes — the "current" value overwrites history. You need to query "what did we know on March 15th?" not "what is true today about March 15th?"

**DESIGN RULE** — **Always use temporal split (not random) for time-ordered data. Train on past, test on future.**
Random split on time-series puts future data in training. Model "predicts" the past using future knowledge. The only honest evaluation: train on [t₀, t₁], test on [t₁, t₂].

### Visualization (canvas `dp8`, 720×160 as drawn; HTML attribute 720×260)

Two-row query comparison: point-in-time query vs current-state query.

- **Title (17px `#1a5276`, top center):** "Point-in-Time vs Current Query".
- **Top row (correct):** green (`#27ae60`) box (80, 50, 180×40) with white 15px text 'SELECT * WHERE' / 'date = "Mar 15"'; green label "✓ Correct"; green arrow to a green box (440, 50, 220×40) with white 14px text "What was KNOWN" / "on March 15".
- **Bottom row (wrong):** red (`#e74c3c`) box (80, 110, 180×40) with white text "SELECT * FROM" / "current_state"; red label "✗ Leakage"; red arrow to a red box (440, 110, 220×40) with white text "What is TRUE NOW" / "(includes future!)".

## From Pattern 9: Rare But Fatal Events

**DESIGN RULE** — **Don't optimize average-case. Optimize worst-case for safety-critical systems.**
Self-driving: 99.99% accuracy means 1 in 10,000 failure. At 1M decisions/day = 100 failures/day = people die. Standard ML optimization (minimize average loss) is exactly wrong here. Use minimax, not minimum-expected.

**DESIGN RULE** — **For rare classes: use rank-based tests (Mann-Whitney), not per-bucket analysis.**
0.01% positive rate + 20 buckets = 12 positives per bucket = unvalidatable. Rank tests don't require bucketing and work at any class ratio. Skip fine-grained bucketing when the rare class can't support it.

**DESIGN RULE** — **Report tail risk separately from average performance. Include "worst 1% of cases" as a standard metric.**
p50 latency = 5ms means nothing if p99 = 2000ms. Average accuracy = 95% means nothing if accuracy on rare-but-important cases = 30%. The tail IS the product for many systems.

### Visualization (canvas `dp9`, 720×160 as drawn; HTML attribute 720×260)

Two loss curves over cases, with the tail region highlighted.

- **Title (17px `#1a5276`, top center):** "Optimization Strategy".
- **Axes:** `#999` 2px L-shape, origin (60, 140) to (660, 140) and up to (60, 50); axis captions (13px `#666`): "Cases" (x), "Loss" (rotated, y).
- **Avg-case curve:** red `#e74c3c` 3px, `y = 80 + t^0.3 · 40` (rises steeply then flattens), labeled "Avg-case" (red, right side).
- **Worst-case curve:** green `#27ae60` 3px, `y = 95 + t · 20` (flatter, handles tail), labeled "Worst-case" (green, right side).
- **Tail highlight:** `rgba(231,76,60,0.15)` rect (550, 50, 110×90), labeled (14px `#e67e22`): "Tail!".

## From Pattern 10: Confidently Wrong

**DESIGN RULE** — **Monitor for "correctness" not just "availability." A system that's fast + wrong is worse than one that's slow + right.**
Stale caches, hallucinating agents, misleading dashboards — all serve answers confidently. Add periodic ground-truth checks: sample outputs and verify against reality. Alert on CORRECTNESS, not just uptime.

**DESIGN RULE** — **Include staleness/age metadata on every served prediction. Let consumers decide if it's fresh enough.**
"This prediction was computed 3 minutes ago" lets the consumer decide if that's acceptable. Without this metadata, stale predictions look identical to fresh ones.

### Visualization (canvas `dp10`, 720×160 as drawn; HTML attribute 720×260)

Staleness gauge with the needle in the warning zone.

- **Title (17px `#1a5276`, top center):** "Staleness Meter".
- **Gauge:** semicircular 12px arc centered (360, 110), radius 70, in three segments — green `#27ae60` (π to 1.4π), orange `#e67e22` (1.4π to 1.7π), red `#e74c3c` (1.7π to 2π); dark `#333` 4px needle at angle 1.65π with a 10px hub dot.
- **Zone labels (14px):** "Fresh" (green, left), "Stale" (red, right).
- **Readout:** orange 18px "Age: 18 min" below the hub; gray 14px "Threshold: 15 min" to the right.

## From Pattern 11: Population Mismatch

**DESIGN RULE** — **Report model performance PER SUBGROUP. If overall = 90% but one group = 60%: you have a problem you can't see in the aggregate.**
Average hides disparities. Skin cancer detection overall: 92%. On dark skin: 65%. The aggregate masks harm. Segment evaluation by every available demographic/group variable.

**DESIGN RULE** — **Compare training data demographics to deployment population. If they differ: expect degraded performance. Quantify the gap.**
Trained on US data, deployed in India? Trained on 2019 behavior, deployed in 2024? The gap = expected performance loss. If you can't close it (more data): at minimum document it and set expectations.

### Visualization (canvas `dp11`, 720×160 as drawn; HTML attribute 720×260)

Per-subgroup performance bar chart with a disparity call-out.

- **Title (17px `#1a5276`, top center):** "Per-Subgroup Performance".
- **Gridlines:** `#ddd` 1px horizontals at 0%, 20%, 40%, 60%, 80%, 100% with 12px `#999` labels; baseline y=140, 100px full height.
- **Bars** (width 100, spacing 150, starting x=70; percent printed above each in 16px of its color, name below in 14px `#333`):
  - Overall — 90% — `#1a5276`
  - Group A — 60% — `#e74c3c`
  - Group B — 95% — `#27ae60`
  - Group C — 88% — `#1a5276`
- **Annotation:** red 3px arrow pointing at the Group A bar, labeled (14px `#e74c3c`): "Disparity!".

## From Pattern 12: Resolution Hides Truth

**DESIGN RULE** — **Profile at multiple resolutions (doc 13). A signal visible at 1-min but invisible at 5-min is REAL — your resolution is just too coarse.**
Electricity spikes, network microbursts, CPU bimodality — all invisible at standard polling intervals. Multi-resolution profiling catches what single-resolution misses.

**DESIGN RULE** — **Use meta-distribution Gini (doc 28) to detect when aggregation is hiding structure.**
If Gini of bin heights > 0.7: your data is extremely concentrated. Standard equal-width analysis will miss the structure. Adaptive bins, percentile-width encoding, or log-scale needed.

**DESIGN RULE** — **Never report an average without the distribution. If the distribution is bimodal, the average represents a state NO entity actually occupies.**
"Average CPU = 50%" when it's bimodal (0% and 100%) describes a state that never exists. The average is a lie. Report the SHAPE, not just the summary statistic.

### Visualization (canvas `dp12`, 720×160 as drawn; HTML attribute 720×260)

Three stacked traces of the same signal at decreasing resolution.

- **Title (17px `#1a5276`, top center):** "Multi-Resolution: Same Signal".
- **1s poll (row baseline y=50):** green `#27ae60` 2px spiky line — `base = 30 + sin(6t)·12` plus `sin(40t)·8` high-frequency noise, sampled every 5px; right label (14px green): "1s poll".
- **5s poll (y=90):** orange `#e67e22` 2px line — same base wave without the noise term, sampled every 10px; right label: "5s poll".
- **60s poll (y=130):** red `#e74c3c` 2px flat line at base 30, sampled every 30px — all detail lost; right label: "60s poll".
- **Annotation:** dashed green (dash 5/3) vertical line at x=200 with 13px green label "Spike!" — the spike visible only in the 1s trace.

## Regeneration instructions

- **Layout:** single long page: h1, `.subtitle`, then 12 `<h2>` sections ("From Pattern N: …"). Each section is a stack of 2-3 `.rule` boxes followed by one standalone `<canvas>` (`width="720" height="260"` attributes, inline style `display:block; margin:15px auto 20px;`). No tables, no nav, no cross-links.
- **Rule box structure:** `.rule` — background `#f0f8f0`, border `1px solid #27ae60`, left border `5px solid #27ae60`, radius `0 8px 8px 0`, padding 14px 18px. Inside: `.label` ("Design Rule" — 0.8em, `#27ae60`, uppercase, letter-spacing 1px, weight 700), `.text` (the rule — 0.95em, `#1a5276`, weight 700), `.why` (rationale — 0.88em, `#555`, margin-top 6px).
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.3em `#1a5276` with 2px solid `#2980b9` bottom border, margin 35px 0 12px; subtitle `#666` 1.05em; p 0.95em `#333`; `strong` `#1a5276`.
- **Canvas:** the drawing script resizes each canvas to 720×160 CSS pixels (overriding the 260px height attribute) and scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates); default font 17px system sans-serif.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, secondary blue `#2980b9`, gray text `#666`/`#999`/`#333`.
