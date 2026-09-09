# Handling Skewed Classes

**Page type:** detail page (backlog-style two-column layout: text left 45%, canvas right 55%, one `.lang-section` per topic; BACKLOG status badge in h1)
**HTML title tag:** Handling Skewed Classes

**Status badge:** BACKLOG (inline in h1)

**Subtitle:** Workflow for imbalanced problems — generating controlled skew, choosing between resampling and cost-sensitive learning, and testing without accuracy illusions.

**Intro callout:** At high class imbalance, accuracy rewards models that do nothing. The workflow here: generate datasets with controlled skew, choose between resampling and cost-sensitive learning, and validate without falling for accuracy illusions.

## 1. The Trivial Baseline

At 100:1 imbalance, a model that always predicts the majority class scores 99% accuracy. The number rises with skew while the model gets less useful.

- Any accuracy figure has to be read against the majority-class rate, not against zero.
- Skew alone is not the hard part. Skew *plus class overlap* is — a well-separated 1000:1 problem is easier than an overlapping 10:1 one.

**Key point (red left-border box):** Report the majority-class baseline next to any accuracy number, or the reader has no way to tell whether the model did anything.

### Visualization (canvas `c1`, 720×310)

Bar chart: majority-class accuracy climbing with imbalance ratio.

- **Title (bold 16px, `#1a5276`, centered at y=24):** "Accuracy of a Model That Predicts Nothing".
- **Axes:** L-shaped axes in `#95a5a6` width 1.4; plot area plotX=80, plotY=52, plotW=w−150, plotH=h−116.
- **Bars (fill `rgba(231,76,60,0.50)`, stroke `#e74c3c` width 1.6; height scaled as (pct−40)/60 of plot height; each bar occupies 52% of its slot, offset 24% into the slot):**
  | Ratio label | Accuracy % (bold 13px `#e74c3c` above bar, formatted to one decimal) |
  |-------------|------------------------------------------------------------------------|
  | 1:1 | 50.0% |
  | 4:1 | 80.0% |
  | 10:1 | 90.9% |
  | 100:1 | 99.0% |
  | 1000:1 | 99.9% |
- **X tick labels (13px `#4a5866`, centered under bars):** the ratio labels; x-axis caption below (13px `#4a5866`, centered): "majority : minority".
- **Annotation (13px orange `#e67e22`, centered above the plot):** "the metric improves as the problem gets harder".

## 2. Resample or Reweight

Two routes to the same goal, with different costs. Both change the decision boundary; only one changes the data.

- **Resampling** alters the training distribution. Synthetic points may not lie on the real data manifold, and oversampling before the split leaks across folds.
- **Class weights + threshold tuning** leaves the data alone and moves the operating point. Usually the first thing to try, and often sufficient.
- Either way the test set stays untouched at the true prevalence — resampling it invalidates every metric computed on it.

**Philosophy callout (blue left-border box):** Resampling and reweighting are both statements about relative error cost. Prefer stating that cost directly.

### Visualization (canvas `c2`, 720×310)

Two overlapping Gaussian score-distribution curves with default and tuned decision thresholds.

- **Title (bold 16px, `#1a5276`, centered at y=24):** "Score Distributions and Where You Cut".
- **Axes:** horizontal baseline only in `#95a5a6` width 1.4; plot area plotX=70, plotY=52, plotW=w−130, plotH=h−116.
- **Curves (200-point Gaussian bells over t∈[0,1], filled to baseline and stroked width 2.6):**
  - Majority: mean 0.36, sd 0.13, peak scale 0.92 — stroke `#2980b9`, fill `rgba(41,128,185,0.22)`; label "majority" (13px `#2980b9`) at 30% plot width near top.
  - Minority: mean 0.66, sd 0.11, peak scale 0.30 — stroke `#e74c3c`, fill `rgba(231,76,60,0.35)`; label "minority" (13px `#e74c3c`) at 75% plot width, above its peak.
- **Default threshold:** vertical dashed gray line (`#95a5a6`, dash 5/4, width 2) at 50% plot width; label "0.5 default" (12px `#5a6875`) above.
- **Tuned threshold:** solid green line (`#27ae60`, width 2.6) at 56% plot width; label "tuned to cost" (bold 13px `#27ae60`) to its right near the top.
- **X-axis caption (13px `#4a5866`, centered):** "model score →".
- **Bottom annotation (13px orange `#e67e22`, centered):** "moving the cut needs no resampling and no retraining".

## 3. To Cover

Comparison table (`table.compare`, blue header row):

| Area | Items |
|------|-------|
| Dataset generation | Controlled ratios at 10:1 / 100:1 / 1000:1; class overlap varied independently of ratio; multimodal minority; noise features; ratio drift over time |
| Sampling | Random over/under; SMOTE and variants with their interpolation assumptions; hybrids (SMOTE+Tomek, SMOTE+ENN); ensemble undersampling (EasyEnsemble, BalanceCascade) |
| Cost-sensitive | Class weights; explicit cost matrix; post-hoc threshold selection on a validation set |
| Validation | Stratified k-fold; resample inside the fold only; McNemar's test for paired model comparison; permutation tests when accuracy misleads |
| Calibration | Probabilities skew after resampling; Platt scaling and isotonic regression as post-hoc corrections |
| Metrics | AUC-PR over AUC-ROC; F-beta with beta from costs; MCC; expected cost = FP·c_FP + FN·c_FN (subscripts rendered with `<sub>`); lift for top-k triage |

## 4. Open Questions

- At what ratio does each sampling method start producing artifacts rather than help?
- How do you check that SMOTE points respect the true manifold instead of bridging across a gap?
- When does resampling genuinely beat class weights plus a tuned threshold?
- Minimum minority-class count for a metric estimate with a usable bootstrap interval?

## Regeneration instructions

- **Template/layout:** backlog kusto-style detail page. `<h1>` with inline `<span class="status">BACKLOG</span>` badge, `.subtitle` paragraph, `.intro` callout, then four `.lang-section` blocks. Sections 1 and 2 use `table.layout` with one row: left `td.text-col` (45%) with intro paragraph, `<ul>`, and a `.key-point` (section 1) or `.philosophy` (section 2) callout; right `td.viz-col` (55%) with the canvas. Section 3 contains only a `table.compare`; section 4 is a plain `<ul>` (no table, no canvas).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border. h2 1.3rem `#1a5276`, 2px `#2980b9` bottom border. `.subtitle` `#666` 0.95rem. `.intro` and `.philosophy` background `#f0f4f8`, left border 3px `#2980b9`, 0.9rem. `.key-point` background `#f8f9fa`, left border 3px `#e74c3c`, 0.9rem. `.status` badge: background `#fef9e7`, border 1px `#f39c12`, text `#b7950b`, radius 4px. `table.compare`: th background `#1a5276` white text padding 8px 12px, rows bordered `#eee`, even rows `#f8fafb`, first column bold. `ul` 0.92rem. Canvases `width: 100%`, border 1px `#e0e0e0`, radius 4px.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange; also `#2980b9` accent blue.
- **Canvas rendering:** canvases declare intrinsic width/height and are scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper; fonts are -apple-system sans-serif.
- Note: in regenerated HTML any card/page links use `.html` extensions (this page has none).
