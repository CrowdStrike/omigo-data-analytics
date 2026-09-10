# Gradient Boosting Machines (GBM)

**Page type:** detail page (two-column obj-table layout: text left 42%, canvas right 58%, one row per assumption)
**HTML title tag:** Gradient Boosting - ML Assumptions

**Subtitle:** Sequential ensemble of shallow regression trees — each tree corrects the errors of all previous trees combined.

## Section 0: What It Does

Builds an additive model of many shallow regression trees. Tree₁ fits the target. Tree₂ fits the residuals of Tree₁. Tree₃ fits the residuals of (Tree₁ + Tree₂). Each tree is a weak learner (depth 3–6); their sum converges to a strong predictor. Learning rate (η) shrinks each tree's contribution to prevent overshoot.

- **Best For:** Tabular data competitions (XGBoost/LightGBM), click-through rate, insurance pricing, ranking systems
- **Data:** Structured/tabular features, moderate-to-large n. Handles mixed types, missing values (LightGBM). State-of-the-art for non-image, non-text tasks.
- **Key Params:** n_estimators (num trees), learning_rate (η), max_depth, subsample, colsample. Early stopping on validation loss.

### Visualization (canvas `c0`, 720×300)

Three mini regression trees summed into a final stepped function approximating a smooth curve.

- **Title (bold 14px `#1a5276`, centered):** "GBM: Sum of Shallow Regression Trees".
- **Mini trees:** three 130×130 boxes (stroke `#ddd`) starting at x=30, y=45, gap 20, each with a bold `#1a5276` label above: "Tree₁ (target)", "Tree₂ (residual₁)", "Tree₃ (residual₂)". Each tree: a root circle (radius 12, fill `rgba(26,82,118,0.65)`) with white split text ("x<5", "x<3", "x<7" respectively), branching to two 40×20 leaf rectangles with white bold values — Tree₁: "+80" (green fill `rgba(39,174,96,0.7)`) and "+20" (red fill `rgba(231,76,60,0.7)`); Tree₂: "+25" and "-10"; Tree₃: "+8" and "-5". Bold orange 20px "+" between trees and "+  ...  =" after the third.
- **Final panel (right, 150×120):** L-shaped gray axes; dashed blue `#1a5276` (dash 4/3, width 1.5) smooth true curve: height fraction 0.2 + 0.6·sin(t·π); solid green `#27ae60` width 2.5 stepped GBM approximation with step positions t = 0, 0.15, 0.3, 0.45, 0.6, 0.75, 0.9, 1.0 and heights 0.22, 0.48, 0.7, 0.8, 0.78, 0.58, 0.35, 0.22. Labels: "x" below in `#1a5276`, bold green "Final" above.
- **Bottom explanation (13px `#333`, centered):** "F(x) = η·Tree₁(x) + η·Tree₂(x) + η·Tree₃(x) + ...    (η = learning rate)".

## Section 1: Noise Amplification (Hard Example Focus)

Boosting works by **focusing on examples the current ensemble gets wrong**. Each new tree corrects the residuals from previous trees. Problem: "hard examples" are often hard because they're mislabeled, noisy, or outliers — not because they represent learnable patterns. Later trees fit noise with increasing confidence.

- **Breaks:** 5% of labels are wrong. By tree 50, these mislabeled points dominate the residuals. Trees 50–300 exist solely to memorize these noise points, hurting generalization on clean data.
- **Verify:** Inspect highest-residual examples — are they genuinely hard or mislabeled?
- **Fix:** Early stopping, lower learning rate, subsampling (stochastic GB), label cleaning

### Visualization (canvas `c1`, 720×300)

Bubble scatter where bubble size = sample weight after 50 boosting rounds; mislabeled points have huge bubbles.

- **Title (bold `#1a5276`, centered):** "Sample weights after 50 boosting rounds".
- **Decision boundary:** dashed gray (`#888`, dash 4/4, width 1.5) vertical line at x=310, labeled "boundary" at the bottom in `#555`.
- **Clean samples (small bubbles):** class 0 blue `rgba(41,128,185,0.7)` at (x, y, radius): (80,180,3), (120,170,4), (160,190,3), (100,210,2), (200,165,3), (140,220,4); class 1 green `rgba(39,174,96,0.7)` at (450,80,3), (500,100,4), (530,70,2), (480,120,3), (560,90,3), (510,60,4).
- **Mislabeled/noisy samples (huge bubbles, fill `rgba(231,76,60,0.5)` with `#e74c3c` outline width 2):** (460,170, r=22), (150,90, r=20), (490,200, r=18).
- **Annotation (bold red, left-aligned at x=505):** "Mislabeled — weight 50×" / "normal after 50 rounds".
- **Legend:** "Bubble size = sample weight" in `#555` bottom-left; top right: small blue dot "Clean (low weight)"; larger red-outlined dot "Noisy (huge weight)".

## Section 2: Overfitting (Train/Validation Divergence)

Unlike Random Forest which converges, gradient boosting can **overfit indefinitely** — training loss keeps dropping while validation loss climbs. Each added tree reduces bias but increases variance. Without explicit stopping, the model achieves near-perfect training accuracy by memorizing every residual pattern, including noise.

- **Breaks:** 500 samples, 200 features. Training AUC=0.99, validation AUC=0.61. Trees 50–300 memorize residual noise. The model is confident and wrong.
- **Verify:** Learning curves (train vs. val), early stopping rounds, train-val gap
- **Fix:** Early stopping (patience=10–20), max_depth limits, L1/L2 regularization, min_child_weight

### Visualization (canvas `c2`, 720×300)

Waterfall chart of each tree block's contribution to validation AUC.

- **Title (bold `#1a5276`, centered):** "What each block of trees contributes to validation AUC".
- **Baseline:** dashed gray line at AUC 0.5 with right-aligned label "AUC 0.5" in `#555`; scale 400 px per AUC unit, baseline y = h−60; bars 80 wide with 20px gaps starting at x=85.
- **Blocks (label, gain, style):**
  - "Trees 1-10", +0.18, good — fill `rgba(39,174,96,0.75)`, stroke `#27ae60`
  - "Trees 11-30", +0.06, good
  - "Trees 31-50", +0.02, ok — fill `rgba(230,126,34,0.3)`, stroke `#e67e22`
  - "Trees 51-100", −0.01, bad — fill `rgba(231,76,60,0.5)`, stroke `#e74c3c`
  - "Trees 101-200", −0.04, bad
  - "Trees 201-500", −0.07, bad
- Each bar starts where the previous ended (running cumulative level from 0.5); signed value label bold above each bar (green if positive, red if negative); two-line block labels below the baseline; dashed gray connectors between consecutive bars.
- **Annotations (bold red 13px):** "← Signal" near the left bars and "Memorization →" near the right bars, at y=45.

## Section 3: Stationarity Assumption (No Concept Drift)

Gradient boosting assumes **future data comes from the same distribution** as training data. If the relationship between features and outcome shifts over time (concept drift), the model's predictions become increasingly wrong — but its confidence remains unchanged. Tree models don't extrapolate; they repeat whatever leaf pattern was learned from stale data.

- **Breaks:** Model trained on 2019 spending patterns. Post-2020, customer behavior shifted dramatically. Model still predicts based on old patterns — high confidence, systematically wrong predictions.
- **Verify:** Monitor prediction distributions over time, PSI (Population Stability Index), feature drift tracking
- **Fix:** Retrain periodically, sliding windows, online boosting, drift detection triggers

### Visualization (canvas `c3`, 720×300)

Overlaid training vs deployment feature distributions with a stale model split point.

- **Title (bold `#1a5276`, centered):** "Feature distribution: training vs. deployment (6 months later)".
- **Axes:** L-shaped gray (`#bbb`) axes, padding 50; x-axis "Spending Amount ($)" mapping 0–500.
- **Training distribution (2019):** Gaussian 0.7·exp(−(x−200)²/4000), stroke `#2980b9` width 2.5, fill `rgba(41,128,185,0.15)`.
- **Deployment distribution (2020):** mixture 0.5·exp(−(x−320)²/5000) + 0.15·exp(−(x−150)²/1500), stroke `#e74c3c` width 2.5, fill `rgba(231,76,60,0.12)` — shifted right with a secondary bump.
- **Model split:** dashed orange `#e67e22` (dash 5/4, width 2) vertical line at x=250 with bold orange label "Model split (stale)" above.
- **Legend (top right):** blue line "Training (2019)"; red line "Deployment (2020)".

## Section 4: Sufficient Data (Residuals Need Signal)

Each tree fits **residuals from the previous ensemble**. With small n, residuals become pure noise after just a few trees — there's no remaining signal to learn from. The model continues fitting anyway, creating complex rules for random patterns. Boosting is MORE prone to overfitting than RF on small datasets because it doesn't have bagging's variance protection.

- **Breaks:** n=100, 50 features. After 5 trees, training residuals are dominated by noise. Trees 6–500 build elaborate memorization structures. Ensemble is much worse than a single regularized tree.
- **Verify:** Residual analysis (are residuals structured or random?), OOF performance by tree count
- **Fix:** Very low learning rate + very few trees, or switch to simpler model (logistic, single tree)

### Visualization (canvas `c4`, 720×300)

Heatmap grid of validation AUC by sample size × tree count.

- **Title (bold `#1a5276`, centered):** "Validation AUC by (n_samples × n_trees)".
- **Grid:** rows = "n=50", "n=200", "n=1000", "n=5000"; columns = tree counts "5", "20", "50", "100", "200", "500". Cell values (bold 13px, dark `#333`, or `#c0392b` when < 0.60):
  - n=50: 0.62, 0.64, 0.58, 0.52, 0.48, 0.44
  - n=200: 0.65, 0.71, 0.72, 0.70, 0.67, 0.63
  - n=1000: 0.67, 0.74, 0.78, 0.80, 0.80, 0.79
  - n=5000: 0.68, 0.76, 0.81, 0.84, 0.86, 0.86
- **Cell colors by AUC:** ≥0.78 `rgba(39,174,96,0.75)`; ≥0.70 `rgba(39,174,96,0.2)`; ≥0.62 `rgba(230,126,34,0.2)`; else `rgba(231,76,60,0.25)`. Cell borders `#ddd`.
- **Labels:** row labels right-aligned to the left of the grid; column labels below; "Number of trees →" centered at the bottom, all in `#555` 13px.
- **Highlights:** green `#27ae60` (width 2.5) rectangle around the sweet spot (columns 50–200 × rows n=1000/n=5000); red `#e74c3c` (width 2.5) rectangle around the danger zone (columns 100–500 × row n=50).

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width `border-collapse` table, one `<tr>` per section; left `<td>` (42%) holds `.obj-title` + `.obj-desc` paragraph + `.obj-detail` lines, right `<td>` (58%, centered) holds the canvas. Even rows have background `#fafcfe`. Row 0 has an extra "Key Params:" `.obj-detail` line.
- **Detail-line markup:** "Breaks:" uses `<span class="bad">` (red `#e74c3c`, weight 600); "Verify:" uses `<span class="tag tag-check">` (pill: background `#eafaf1`, color `#1e8449`); "Fix:" uses `<span class="tag tag-fix">` (pill: background `#fef9e7`, color `#b7950b`). Tag pills: inline-block, 0.75em, padding 2px 8px, radius 4px. Also defined: `.good` `#27ae60`, `.warn` `#e67e22`, `.tag-break` (background `#fdeaea`, color `#c0392b`).
- **Page style:** global reset (`* { margin:0; padding:0; box-sizing:border-box }`); body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px solid `#2980b9` bottom border and 8px bottom padding; `.subtitle` `#666` 0.95rem, 32px bottom margin; table cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; `.obj-title` 1.05em weight 600 `#1a5276`; `.obj-desc` 0.9em `#333`; `.obj-detail` 0.85em `#555`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** each canvas declares intrinsic `width="720" height="300"`; `canvas { display:block; margin:0 auto; }`; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper; charts drawn with vanilla canvas 2D in IIFEs.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#555`/`#333`.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
