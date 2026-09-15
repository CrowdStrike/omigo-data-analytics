# Pitfall: Class Imbalance Mishandling

**Page type:** detail page (sectioned card layout: per section an h2, then a two-column table — text left ~45% with tag pills/bullets/example/key-point, canvas right ~55%)
**HTML title tag:** Class Imbalance Mishandling

**Subtitle:** Treating 95/5 class ratio naively → model predicts majority class always

## The Problem

Tags: `the trap` (red), `imbalance` (blue)

- **Trivial classifier** — an all-negative model on a 95/5 dataset scores 95% accuracy with 0% recall
- **Rewarded laziness** — the standard workflow quietly rewards a model that ignores the rare class
- **Meaningless accuracy** — on skewed data it mostly measures the class ratio, not detection skill
- **Contaminated tests** — oversampling before the split copies minority rows into the test set
- **Inflated scores** — evaluating on duplicated minority samples overstates minority performance

*Example:* On fraud data with 2% positives, always predicting "no fraud" scores 98% accuracy and catches nothing.

**Impact:** A model that detects none of the rare events ships behind an impressive accuracy number, and the failure surfaces only as the missed fraud, churn, or defects it was built to catch.

### Visualization (canvas `c1`, 720×300)

Left panel: stacked class-distribution bar; right panel: confusion matrix of the all-negative model.

- **Left title (bold 13px, `#1a5276`, centered):** "Class Distribution". Horizontal stacked bar at x=70, y=50, 220×120: negative segment 95% of width in `#1a5276` (fill alpha 0.7, 2px stroke) labeled "95%" in white bold 14px centered; positive segment 5% in `#e74c3c` labeled "5%" in bold red 11px to its right. Legend below: blue swatch "Negative", red swatch "Positive".
- **Right title (bold 13px, centered):** "Confusion Matrix (Predicts All Negative)". 2×2 grid, cells 90×55 starting at x=435, y=55. Header "Predicted" with column labels "Neg"/"Pos"; rotated row header "Actual" with row labels "Neg"/"Pos".
  - TN (top-left): green `#27ae60` (fill alpha 0.25, 2px stroke), value "950" bold 16px green.
  - FP (top-right): light blue `#1a5276` (fill alpha 0.1, 1px stroke), value "0" bold blue.
  - FN (bottom-left): red `#e74c3c` (fill alpha 0.25, 2px stroke), value "50" bold red.
  - TP (bottom-right): light blue, value "0" bold blue.
- **Metrics below matrix (12px, left-aligned):** "Accuracy: 95%" in green, "Recall (positive): 0%" in red, "Precision (positive): N/A" in orange `#e67e22`.
- **Bottom warning (bold 13px red, centered):** "Useless despite high accuracy — model learns to ignore the minority class"

## Why It Happens

Tags: `root cause` (orange), `defaults` (blue)

- **Default metric** — accuracy feels natural, and nothing in the workflow flags it as misleading
- **Majority dominance** — at a 95/5 ratio, accuracy says almost nothing about minority detection
- **Unlucky folds** — a naive random split can leave a validation fold with zero minority samples
- **Default threshold** — the 0.5 cutoff assumes balance, so rare positives may never cross it
- **Trivial optimum** — the loss gain from 95% easy negatives swamps the 5% misclassified positives
- **Global oversampling** — resampling before the split lets the model memorize test-set positives

*Example:* On fraud data with 0.2% positives, the model scores every transaction below 0.03, so at threshold 0.5 it flags nothing.

**Root Cause:** Gradient descent finds the trivial "always predict negative" solution because the loss from 5% missed positives is overwhelmed by the reward from 95% correct negatives.

### Visualization (canvas `c2`, 720×300)

Grouped bar comparison of Smart Model vs Lazy Model on three metrics.

- **Title (bold 14px, `#1a5276`, centered):** "Why Accuracy Fails Under Imbalance".
- **Headers:** left at x=185 — bold green "Smart Model" with `#555` subtitle "(some minority correct)"; right at x=535 — bold red "Lazy Model" with subtitle "(predicts all majority)". Dashed `#ccc` vertical divider at mid-width.
- **Data:** metrics [Accuracy, Precision, Recall]; Smart values [93, 60, 45] (%), Lazy values [95, 0, 0] (%).
- **Bars:** width 30, max height 130 (scale 0-100%), chart top y=80; group spacing 115. Smart bars green `#27ae60` (fill alpha 0.7, 1.5px stroke) starting at x=80; Lazy bars starting at x=430 — Accuracy bar orange `#e67e22`, Precision/Recall bars red `#e74c3c` (zero bars drawn at minimum 2px height). Bold value labels ("93%", "60%", "45%", "95%", "0%", "0%") above each bar in the bar color; metric labels below in `#555` 10px.
- **Verdict (centered):** bold red 12px "Lazy model WINS on accuracy despite being useless!" then blue 11px "Precision & Recall reveal the truth: the lazy model detects nothing."

## The Correct Approach

Tags: `the fix` (green), `evaluation` (blue)

- **Natural test set** — evaluate at the real class distribution, the way the business will use it
- **Stratified splits** — StratifiedKFold keeps minority samples in every fold at the true ratio
- **Right metrics** — judge the minority class with precision, recall, and F1, not overall accuracy
- **PR over ROC** — with rare positives, ROC looks deceptively good; prefer PR curves and PR-AUC
- **Threshold tuning** — set the cutoff on validation using the cost of misses vs false alarms
- **SMOTE hygiene** — oversample only inside the training fold, never validation or test data

*Example:* With stratified 5-fold CV and a threshold tuned to 0.08, the fraud model reaches precision 0.45 and recall 0.72.

**Fix:** Replace accuracy with F1 or PR-AUC, use StratifiedKFold, and tune the threshold on validation against the business cost of false positives vs false negatives.

### Visualization (canvas `c3`, 720×300)

Pipeline flow, PR-curve sketch, and threshold-tuning slider.

- **Title (bold 14px, `#1a5276`, centered):** "Correct: Stratified Split + Proper Metrics".
- **Pipeline (rounded boxes, height 28, white bold 10px labels, green arrows between):** "Data" (blue, x=20 w=70) → "StratifiedKFold" (green, x=115 w=110) → "Train (+SMOTE)" (orange, x=250 w=110) → "Evaluate (P/R/F1)" (green, x=385 w=130), at y=50.
- **PR curve sketch (bottom-left):** 180×140 box at x=30, y=105 with `#ccc` border; x-axis label "Recall", rotated y-axis label "Precision" (`#555` 10px); bold blue title "PR Curve" above; blue `#1a5276` bezier curve (width 2.5) descending from top-left toward bottom-right.
- **Threshold tuning (bottom-right, from x=260):** bold blue label "Threshold Tuning"; gray `#e0e0e0` slider track 350×6 with orange `#e67e22` handle (radius 8) at 35% carrying white bold "0.08"; scale labels "0.0" and "1.0".
- **Tradeoff lines (11px):** green "Low threshold (0.08):" with `#555` "Recall=0.72, Precision=0.45"; red "High threshold (0.5):" with "Recall=0.05, Precision=0.90"; orange "Default (0.5):" with "Assumes balanced — wrong for imbalanced data".
- **Bottom note (bold green 11px, centered):** "Tune threshold on validation set using business cost function"

## Regeneration instructions

- **Template/layout:** ml-pipeline-pitfalls detail page. h1 + `.subtitle`, then three `.card-section` blocks ("The Problem", "Why It Happens", "The Correct Approach"). Each section: `h2` with 2px `#2980b9` bottom border, then a `table.layout` (border-collapse, full width) with one row — `td.text-col` (45%) and `td.viz-col` (55%), both top-aligned, 12px padding.
- **Text column structure:** `.tags` div of pill spans, then `ul` of bullets with `<b>` lead-ins (bold `#1a5276`), then italic `.example` paragraph, then `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, 8px 12px padding, 0.9rem) whose `<strong>` label is Impact/Root Cause/Fix.
- **Tag pills:** inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors: blue `rgba(26,82,118,0.12)`/`#1a5276`; green `rgba(39,174,96,0.15)`/`#27ae60`; red `rgba(231,76,60,0.12)`/`#e74c3c`; orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 0.95rem; canvas `width:100%`, 1px `#e0e0e0` border, 4px radius; ul 0.92rem. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, secondary blue `#2980b9`, bar fill `rgba(26,82,118,0.35)`.
- **Canvas:** each canvas declares intrinsic width=720 height=300 and is drawn via a shared `setup(id)` helper that sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- In regenerated HTML, any card links use `.html` extensions.
