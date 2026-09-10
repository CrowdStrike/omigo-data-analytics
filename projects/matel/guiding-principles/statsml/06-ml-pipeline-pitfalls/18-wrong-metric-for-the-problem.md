# Pitfall: Wrong Metric for the Problem

**Page type:** detail page (card-section layout: one h2 section per block, two-column table text left 45% / canvas right 55%)
**HTML title tag:** Wrong Metric for the Problem

**Subtitle:** Optimizing a metric that doesn't align with business value.

## Section 1: The Problem

**Tags:** `the trap` (red pill), `metrics` (blue pill)

- **The trap** — a model can score well on AUC, accuracy, or F1 yet still deliver no business value
- **Fraud** — AUC 0.93, yet only 12% precision at the threshold that catches 80% of fraud
- **Churn** — 92% accuracy loses to always predicting "no churn", which scores 95% at a 5% base rate
- **Medical** — a diagnosis model optimizes F1 though a false negative costs 100x a false positive
- **Ranking** — a recommender minimizes RMSE when only the top three results need to be relevant

*Example:* A loan model optimizing accuracy under a 15% default rate learns to approve everyone for an easy 85% accuracy.

**Impact:** The model maximizes a number that does not correspond to the real decision, so high scores coexist with wasted investigator time, missed fraud, and irrelevant recommendations.

### Visualization (canvas `c1`, 720×300)

ROC curve with a marked operating point, plus a confusion matrix at that threshold.

- **Title (bold 14px, `#1a5276`, top center):** "AUC = 0.93 (Excellent) but Precision @ Operating Point = 12% (Terrible)".
- **ROC plot:** origin at (100, 250), plot area 200×200; `#444` axes width 2; 11px axis labels "False Positive Rate" (below) and "True Positive Rate" (rotated, left). Curve in `#2980b9` width 3, drawn as tpr = fpr^0.07 for fpr 0→1 (concave, AUC = 1/1.07 ≈ 0.93). Dashed light-gray `#ccc` diagonal (dash 4/4) for random.
- **Operating point:** at FPR 0.044, TPR 0.80 (on the curve: 0.044^0.07 = 0.80); filled red `#e74c3c` dot 6px with a 10px-radius red ring; bold 10px red label "Operating Point" and 9px "(to catch 80% of fraud)".
- **Confusion matrix (right, 70px cells at cmX=400, cmY=80):** heading bold 11px blue "At Operating Threshold (10,000 txns, 75 fraud):".
  - TN 9,485 — fill `rgba(39,174,96,0.3)`, stroke `#27ae60` width 2, bold 16px green value, 9px "TN".
  - FP 440 — fill `rgba(231,76,60,0.4)`, stroke `#e74c3c` width 3 (emphasized), red value, "FP".
  - FN 15 — fill `rgba(230,126,34,0.3)`, stroke `#e67e22` width 2, orange value, "FN".
  - TP 60 — fill `rgba(39,174,96,0.3)`, stroke `#27ae60` width 2, green value, "TP".
- **Calculations below matrix:** bold 12px red "Precision = TP / (TP + FP)" and "= 60 / (60 + 440) = 0.12 (12%)"; 10px green "Accuracy = 9,545 / 10,000 = 95.5% — looks great"; 10px `#444` "Recall = 60/75 = 80%, but 88% of flags are false alarms".

## Section 2: Why It Happens

**Tags:** `root cause` (orange pill), `textbook defaults` (blue pill)

- **Defaults** — textbook metrics are easy to compute, compare, and report, so they win by habit
- **Threshold blindness** — AUC averages ranking quality over all thresholds, not the one you deploy
- **Symmetric errors** — accuracy weighs each error the same while real costs are wildly unequal
- **Fixed balance** — F1 weights precision and recall equally even when the business values one more
- **Missing context** — standard metrics ignore base rates, costs, and the decision being driven

*Example:* A spam filter optimizing F1 reaches 0.91 but ships an 8% false-positive rate that sends good email to spam.

**Root Cause:** The metric defines the objective the model optimizes, so AUC = 0.93 proves good average ranking but says nothing about precision at the threshold you will actually use.

### Visualization (canvas `c2`, 720×300)

Cost-asymmetry table plus explanatory text.

- **Title (bold 14px, `#1a5276`, top center):** "Metric vs Business Reality: Cost Asymmetry Ignored".
- **Table (headers bold 11px blue at y=72, columns 150px wide starting x=160):** "Error Type" / "Model Weight" / "Business Cost" / "Ratio".
  - Row 1 (orange `#e67e22`, 1px outline box): "False Positive" / "1" / "$50" / "1×".
  - Row 2 (red `#e74c3c`, 2px outline box): "False Negative" / "1" / "$5,000" / "100×".
- **Problem statement (centered red):** bold 12px "F1 and Accuracy treat both errors equally (weight = 1)" and 11px "But business loses 100× more on false negatives!".
- **Example block (10px `#444`, left-aligned):** "Example: Medical diagnosis for serious condition"; "• False Positive (FP): Unnecessary test → $50 cost, minor inconvenience"; "• False Negative (FN): Missed diagnosis → $5,000 treatment delay, patient harm"; "• Model optimizing F1 minimizes FP + FN equally, ignoring 100× cost difference".

## Section 3: The Correct Approach

**Tags:** `the fix` (green pill), `alignment` (blue pill)

- **Fixed threshold** — evaluate precision and recall at the deployed threshold instead of AUC
- **Asymmetric costs** — train with a cost-weighted loss or optimize expected value directly
- **Top-K decisions** — when the product shows the top K items, optimize precision@K or NDCG@K
- **Imbalanced classes** — for rare positives, use the precision-recall curve rather than accuracy
- **Business anchor** — pick a model metric that demonstrably tracks the true business outcome
- **Detection** — a metric win that does not improve past business outcomes signals misalignment

*Example:* A fraud team reviewing 500 flags daily retrains for precision@500 and lifts it from 12% to 68%, so 340 of the 500 flags are real fraud instead of 60.

**Fix:** Before training, ask what decision the model drives, then optimize a metric measured at that decision boundary, with weights taken from your cost structure rather than convention.

### Visualization (canvas `c3`, 720×300)

Four wrong→correct metric mapping rows with scenario context.

- **Title (bold 14px, `#1a5276`, top center):** "Fix: Optimize Metric Aligned with Business Decision".
- **Rows (at y = 60, 110, 160, 210):** each has a white 140×35 box at x=60 stroked red `#e74c3c` with bold 10px red "Wrong: <metric>", an orange `#e67e22` arrow with filled head, a white 170×35 box at x=250 stroked green `#27ae60` with bold 10px green "Correct: <metric>", and 9px `#444` context text at x=430 in the form "<problem> — <reason>".
  1. Wrong: AUC → Correct: Precision@K — "Fraud Detection — Review top K cases daily".
  2. Wrong: Accuracy → Correct: PR-AUC — "Imbalanced (5% positive) — Baseline predicts all negative".
  3. Wrong: F1 → Correct: Cost-weighted loss — "Asymmetric costs — FN costs 100× FP".
  4. Wrong: RMSE → Correct: NDCG@3 — "Ranking (top-3 shown) — Only top results matter".
- **Caption (bold 11px green `#27ae60`, bottom center):** "Choose metric that measures performance at the decision boundary".

## Regeneration instructions

- **Layout:** three `.card-section` blocks, each with an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` with one row: left `td.text-col` (45%) holding `.tags` pills, `<ul>` bullets, `.example` italic paragraph, and `.key-point` callout; right `td.viz-col` (55%) holding the canvas.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 0.95rem; `li b` in `#1a5276`; bullets 0.92rem.
- **Tag pills:** inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px — blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem.
- **Canvas:** intrinsic 720×300, CSS `width: 100%`, 1px `#e0e0e0` border with 4px radius; scale via a shared `setup(id)` helper using `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, grays `#666`/`#444`/`#333`. No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
