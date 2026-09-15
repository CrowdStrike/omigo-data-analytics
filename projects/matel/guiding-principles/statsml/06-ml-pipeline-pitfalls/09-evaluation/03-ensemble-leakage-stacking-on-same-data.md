# Pitfall: Ensemble Leakage (Stacking on Same Data)

**Page type:** detail page (three `.card-section` blocks, each a two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** Ensemble Leakage (Stacking on Same Data)

**Subtitle:** Meta-model trained on base predictions from same data.

## The Problem

Tags: `the trap` (red), `stacking` (blue)

- **Stacking setup** — a meta-model learns to combine predictions from several base models
- **In-sample preds** — base models predict on the same data they trained on, inflating accuracy
- **Contaminated features** — the meta-model treats those inflated outputs as genuine skill
- **Amplified overfit** — the stacker learns training-only error patterns, compounding the overfit
- **Worse than base** — the leaked stacker often tests below its best base model

*Example:* Three base models predict on the same 10k samples they trained on; the stacker hits 0.95 train AUC but only 0.80 on test, below the best base model at 0.83.

**Impact:** The meta-model learns to correct base-model errors that exist only in the training data, so its corrections are useless or harmful in production.

### Visualization (canvas `c1`, 720×300)

Flow diagram of incorrect stacking: training data feeding three base models whose in-sample predictions feed the meta-model.

- **Title (bold 14px `#1a5276`, top center):** "INCORRECT Stacking: Meta-Model Trained on In-Sample Base Predictions".
- **Training Data box:** white 120×50 at (80, 60), stroke `#2980b9` width 2, bold blue label "Training Data".
- **Base model boxes:** three white 60×30 boxes at x=50, y = 140/175/210, stroke `#27ae60`, green bold labels "RF", "GBM", "LogReg"; solid blue arrows from the Training Data box down to each.
- **In-sample prediction boxes:** for each base, a dashed red arrow (`#e74c3c`, dash 4/4) rightward with a small red "(overfit)" annotation above it, into a white 100×30 box at x=220 stroked `#e74c3c` labeled "In-sample preds" in red.
- **Meta-model box:** white 120×50 at (420, 150), stroke `#e67e22` width 3, orange labels bold "META-MODEL" and "(Stacker)"; solid orange arrows from each prediction box converge into it.
- **Bottom annotation (centered, `#e74c3c`):** bold 11px "PROBLEM: Meta-model learns from overfit base predictions", then 10px "Meta-model overfits to base models' training-specific errors".

## Why It Happens

Tags: `root cause` (orange), `inflated inputs` (blue)

- **Systematic inflation** — every base model predicts better in-sample than out-of-sample
- **Inflated inputs** — a 0.88-train, 0.82-test base feeds the stacker the inflated 0.88 level
- **Miscalibrated weights** — the stacker overweights bases whose in-sample accuracy won't repeat
- **Noise correlations** — error-target correlations in the training set vanish on new data
- **Second-level overfit** — a stacker fed contaminated features overfits to the overfit

*Example:* When the RF predicts 0.95 and the GBM 0.40 on the same training sample with true label 1, the stacker learns a noise rule that misfires on test data.

**Root Cause:** The meta-model's input features are contaminated by base-model overfitting, so it exploits error-target correlations that exist only in the training data.

### Visualization (canvas `c2`, 720×300)

Grouped train/test bar chart showing the leaked stacker underperforming its base models on test.

- **Title (bold 14px `#1a5276`, top center):** "Incorrect Stacking Performance".
- **Data (baseline y=240, max bar height 160, paired bars ~40px wide each):**
  - "RF (base)", x=140: train 0.88, test 0.82
  - "GBM (base)", x=280: train 0.86, test 0.83
  - "Stacker (WRONG)", x=420: train 0.95, test 0.80 — highlighted
- **Colors:** train bars fill `rgba(52,152,219,0.5)` stroke `#3498db`; test bars green `#27ae60` at 0.7 alpha (stroke width 2), except the Stacker's test bar in red `#e74c3c` (stroke width 3) with a dashed red rectangle (dash 4/4) drawn around it. Bold value labels above each bar (train × 100 and test × 100, e.g. 88/82, 86/83, 95/80); two-line labels in `#444` below the baseline.
- **Legend (right side, 10px):** blue square "Train", green square "Test".
- **Annotation (bold 11px `#e74c3c`, centered near top):** "Stacker performs WORSE than best base model!".

## The Correct Approach

Tags: `the fix` (green), `out-of-fold` (blue)

- **Out-of-fold** — train bases on K−1 folds, predict the held-out fold; K = 5 is common, not a rule
- **Unbiased features** — every sample gets a base prediction from models that never saw it
- **Meta training** — fit the stacker on these out-of-fold predictions, not in-sample ones
- **Test scoring** — retrain bases on the full training set; their predictions feed the stacker
- **Holdout variant** — fit bases on a train split and the meta on a separate validation split
- **Detection** — a stacker testing worse than its best base model signals in-sample leakage

*Example:* With 5-fold CV stacking on 1,000 training samples, the meta-model trains on 1,000 out-of-fold predictions and reaches 0.84 test AUC, edging out the best base model at 0.83.

**Fix:** Generate base predictions with K-fold cross-validation so each sample's prediction comes from models that never saw it, then train the meta-model on those out-of-fold predictions.

### Visualization (canvas `c3`, 720×300)

Two-step diagram of correct CV stacking: a 5-fold strip, then out-of-fold predictions feeding the meta-model, with a result comparison.

- **Title (bold 14px `#1a5276`, top center):** "CORRECT Stacking: Meta-Model Trained on Out-of-Fold Predictions".
- **Step 1 label (11px `#444`, left):** "1. K-Fold Cross-Validation for Base Models:".
- **Fold strip:** five 100×30 boxes starting at x=80, y=70, 10px apart; fold 3 is the test fold — fill `rgba(231,76,60,0.3)` stroke `#e74c3c` labeled "TEST"; the other four fill `rgba(39,174,96,0.3)` stroke `#27ae60` labeled "TRAIN"; each with a small "Fold N" sublabel.
- **Explanatory lines (10px `#444`):** "Train base model on green folds → predict red fold (out-of-fold)" and "Repeat for all folds → collect unbiased predictions for all samples".
- **Step 2 label (11px `#444`):** "2. Train Meta-Model on Out-of-Fold Predictions:".
- **Boxes:** white 180×40 box at (80, 175) stroked `#27ae60`, green bold label "Out-of-Fold Base Predictions" with sublabel "(unbiased, realistic performance)"; solid green arrow with filled arrowhead to a white 140×50 box at (320, 170) stroked `#27ae60` width 3, labels bold "META-MODEL" and "(trained correctly)".
- **Result block (bottom center):** bold 11px `#1a5276` "Result (Correct CV Stacking):"; 10px `#444` "Best base model: Test AUC = 0.83"; bold 11px `#27ae60` "Stacker (CV): Test AUC = 0.84 — IMPROVEMENT!".

## Regeneration instructions

- **Layout:** three `.card-section` divs, each with an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` (width 100%, border-collapse, td padding 12px, vertical-align top): left `td.text-col` 45% holds `.tags` pills + `<ul>` bullets + `.example` + `.key-point`; right `td.viz-col` 55% holds one canvas.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. No nav bar, no back/home links.
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px; blue = `rgba(26,82,118,0.12)`/`#1a5276`, green = `rgba(39,174,96,0.15)`/`#27ae60`, red = `rgba(231,76,60,0.12)`/`#e74c3c`, orange = `rgba(230,126,34,0.15)`/`#e67e22`.
- **Text blocks:** `<ul>` 0.92rem with `<b>` lead words in `#1a5276`; `.example` italic `#555` 0.9rem; `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem, with a `<strong>` lead ("Impact:", "Root Cause:", "Fix:").
- **Canvas:** each 720×300 intrinsic, CSS `width: 100%`, 1px `#e0e0e0` border, 4px radius; scaled via a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`/`#3498db`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#444`/`#666`.
