# Pitfall: Label Noise (Incorrect Ground Truth)

**Page type:** detail page (card-section layout: one `.card-section` per h2 with a two-column table — text left 45%, canvas right 55%)
**HTML title tag:** Label Noise (Incorrect Ground Truth)

**Subtitle:** When the target variable itself is wrong for 5-20% of records, capping model performance and misleading evaluation.

## The Problem

**Tags:** `the trap` (red), `label noise` (blue)

- **Wrong ground truth** — 5-20% of training and evaluation labels are simply incorrect
- **Annotator disagreement** — medical diagnosis labels show 10-15% inter-rater disagreement
- **Proxy labels** — "clicked ad" stands in for purchase intent and only noisily tracks it
- **Pipeline bugs** — join errors, off-by-one, and timezone issues corrupt around 5% of labels
- **Evaluation lies** — metrics computed against noisy labels misstate the model's true quality

*Example:* A fraud model labeled on "transaction_refunded" trains on 20% legitimate returns and learns to flag expensive electronics buyers.

**Impact:** With 15% of labels wrong, performance hits a ceiling around 85% AUC — the model learns the noise, not the signal.

### Visualization (canvas `c1`, 720×300)

Block diagram mapping true labels (unknown) to noisy labels (what the model sees), with mislabeled bands.

- **Title (bold 14px `#1a5276`, top center):** "Label Noise: Ground Truth Contains Errors".
- **Left column heading (12px `#444`, centered at x=140, y=55):** "TRUE LABELS (unknown)". Two filled blocks at x=60, 160 wide (row height 22): green `#27ae60` block (4 rows tall, from y=75) with white 11px text "True Positive Examples"; red `#e74c3c` block (4 rows tall, starting 4.5 rows down) with white text "True Negative Examples".
- **Middle arrow:** gray `#666` 2px arrow from x=240 to x=320 at y=150 with a filled arrowhead; bold 10px `#e67e22` two-line label above it: "Annotation / pipeline" / "introduces errors".
- **Right column heading (centered at x=520, y=55):** "NOISY LABELS (what model sees)". Stacked blocks at x=440, 140 wide: green block (3 rows) "Labeled Positive"; orange `#e67e22` band (1 row) bold 10px white "MISLABELED (should be neg)"; second orange band (1 row) "MISLABELED (should be pos)"; red block (3 rows) "Labeled Negative".
- **Bottom impact (centered):** bold 11px `#e74c3c` "15% label noise → performance ceiling at ~85% AUC" (y=250); 10px red "Model cannot distinguish signal from noise in the labels themselves" (y=268).

## Why It Happens

**Tags:** `root cause` (orange), `ground truth` (blue)

- **Unearned authority** — once labels land in a training table, nobody questions their origin
- **Fallible producers** — labels come from humans, proxies, or pipelines, each an error source
- **Subjective tasks** — sentiment, toxicity, and diagnosis carry 15-30% annotator disagreement
- **Systematic proxies** — "refunded" as fraud mislabels 10-20% of cases in a consistent direction
- **Definitions drift** — fraud criteria and content policies evolve, so 2022 labels fail 2024 rules
- **Unvalidated joins** — timezone and off-by-one bugs silently corrupt 5-10% of labels unchecked

**Root Cause:** Teams assume labels are ground truth, but "ground truth" is often a noisy approximation nobody has measured.

### Visualization (canvas `c2`, 720×300)

Annotator disagreement matrix (3 annotators × 5 examples) plus a Cohen's kappa scale.

- **Title (bold 14px `#1a5276`, top center):** "Why It Happens: Annotator Disagreement".
- **Matrix:** header row bold 11px `#444`: "Example", "#1", "#2", "#3", "#4", "#5" (columns at x = 130, 190, 250, 310, 370; row labels at x=60). Rows (bold 11px `#1a5276` names, 35px pitch from y=75):
  - Annotator A: + + − + −
  - Annotator B: + − − + −
  - Annotator C: + + + − −
  - Labels drawn bold 14px, "+" in `#27ae60`, "−" in `#e74c3c`. Disagreement columns #2, #3, #4 get a `rgba(230,126,34,0.15)` background highlight behind each cell, and a bold 10px `#e67e22` "⚠" marker below the matrix (y=185).
- **Kappa panel (right side):** bold 12px `#1a5276` "Cohen's kappa = 0.45" at (450,80); 11px `#e67e22` "(moderate agreement)". A 200px horizontal `#ccc` scale line at y=115 from x=450 with 9px `#666` tick labels 0.0, 0.2, 0.4, 0.6, 0.8, 1.0; an orange `#e67e22` 5px-radius dot at the 0.45 position; 8px zone labels below: "Poor" (red), "Moderate" (orange), "Good" (green), "Excellent" (green).
- **Bottom text (centered):** 12px `#444` "3 out of 5 examples have annotator disagreement (60%)" (y=210); bold 13px `#e74c3c` "Which label is \"ground truth\"?" (y=240); 11px `#666` "With kappa = 0.45, ~15-20% of labels are effectively random" (y=265) and "No model architecture can recover from fundamentally ambiguous supervision" (y=282).

## The Correct Approach

**Tags:** `the fix` (green), `label quality` (blue)

- **Measure first** — quantify label trust before training instead of pretending noise is zero
- **Estimate noise rate** — compute inter-annotator agreement; kappa below ~0.6 signals trouble
- **Expert audit** — relabel ~500 samples; noise above ~15% means fix labels before training
- **Noise-robust losses** — symmetric cross-entropy or confident learning down-weight bad labels
- **Multiple annotations** — collect 3-5 labels per example; use majority vote or soft labels
- **Ongoing detection** — check for impossible values and compare predictions to expert review

**Fix:** Treat label quality as a measurable property with acceptance criteria — no model can overcome garbage labels.

### Visualization (canvas `c3`, 720×300)

Four-stage label quality pipeline with stage details and a final output box.

- **Title (bold 14px `#1a5276`, top center):** "Correct Approach: Label Quality Pipeline".
- **Pipeline stages** (stroked boxes at y=55, height 65, each with a green 14px "✓" in the top-right corner, bold 11px stage label in the stage color, 10px `#444` description; connected by gray `#666` 1.5px arrows):
  - "Raw Labels" (x=30, 110 wide, `#1a5276`) — "N = 10,000"
  - "Quality Audit" (x=170, 120 wide, `#1a5276`) — "Sample 500"
  - "Noise Estimation" (x=320, 140 wide, `#e67e22`) — "Flip rate: 12%"
  - "Noise-Robust" (x=490, 130 wide, `#27ae60`) — "Adjusted loss"
- **Quality Audit detail (left, bold 11px `#1a5276` heading at (40,140), 10px `#444` bullets):** "• Sample 500 random examples", "• 3 expert annotators relabel independently", "• Compute Cohen's kappa: 0.72 (acceptable)".
- **Noise Estimation detail (right, bold 11px `#e67e22` heading at (380,140)):** "• Estimated flip rate: 12%", "• Confident learning identifies suspect labels", "• 1,200 labels flagged for review".
- **Final output box:** fill `rgba(39,174,96,0.08)` with 1.5px `#27ae60` stroke at (80,225) 560×55; bold 12px `#27ae60` "Model aware of label uncertainty"; 10px `#444` "Symmetric cross-entropy loss: down-weights high-loss examples (likely mislabeled)" and "Result: Model trained on effective 88% clean labels → robust predictions despite 12% noise".

## Regeneration instructions

- **Layout:** three `.card-section` blocks, each with an `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) and a `table.layout` (width 100%, border-collapse) containing one `<tr>`: left `td.text-col` (45%) with `.tags` pills, a `<ul>` of labeled bullets, optional `.example` italic paragraph, and a `.key-point` callout; right `td.viz-col` (55%) with one canvas.
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.example` italic `#555` 0.9rem. Bullets 0.92rem with `<b>` labels in `#1a5276`.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300 each, CSS `width: 100%`, 1px `#e0e0e0` border, 4px radius; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#444`/`#666`.
- **Links:** none on this page; in regenerated HTML any card links elsewhere use `.html` extensions.
