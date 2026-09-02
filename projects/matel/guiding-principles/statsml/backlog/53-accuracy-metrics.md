# Accuracy Metrics

**Page type:** detail page (backlog-style sections: kusto 2-col text/viz layout table per section, plus two full-width comparison tables; BACKLOG status badge next to h1)
**HTML title tag:** Accuracy Metrics

**Status badge (in h1):** BACKLOG

**Subtitle:** Every classification metric is a different summary of the same four counts. Choosing one means choosing which of them you are willing to ignore.

**Intro callout:** Every classification metric is a ratio over the same confusion matrix — the differences lie in which cells each one ignores. Picking a metric is picking which error you are willing not to measure.

## 1. One Table, Many Summaries

Precision, recall, accuracy and specificity are all ratios drawn from the confusion matrix. What separates them is which cell they leave out.

- **Precision** ignores TN — it never asks how many negatives you got right.
- **Recall** ignores both TN and FP — you can reach 1.0 by predicting everything positive.
- **Accuracy** uses all four, which is exactly why it hides imbalance: TN dominates the sum.

**Key point:** No single ratio is complete. Report at least one metric that includes FP and one that includes FN, or state which error you decided not to measure.

### Visualization (canvas `c1`, 720×320)

Confusion-matrix diagram plus metric-coverage strip rows showing which cells each metric reads.

- **Title (bold 16px, `#1a5276`, top center):** "Which Cells Each Metric Reads".
- **Confusion matrix:** 2×2 grid of cells, each 116×62 px, grid origin at x=250, y=84. Cells: TP (row 0, col 0, green `#27ae60`), FN (row 0, col 1, red `#e74c3c`), FP (row 1, col 0, orange `#e67e22`), TN (row 1, col 1, blue `#2980b9`). Each cell: fill is the cell color at ~20% alpha (`color + '33'`), 2px stroke in the cell color, bold 20px centered label in the cell color.
- **Axis labels (13px, `#4a5866`):** "actual +" and "actual −" centered above the two columns; "predicted +" and "predicted −" right-aligned to the left of the two rows.
- **Metric coverage rows** (starting 34px below the matrix, 26px row spacing), one row per metric with the metric name right-aligned at x=gx−10 in 13px `#2c3e50`, then four 50×16 px chips at 56px x-spacing labeled TP / FN / FP / TN:
  - Precision (color `#e67e22`) reads cells TP, FP.
  - Recall (color `#e74c3c`) reads cells TP, FN.
  - Accuracy (color `#2980b9`) reads all four cells: TP, FN, FP, TN.
  - Chips the metric reads: filled and stroked in the metric color with white 12px centered text. Chips it cannot see: fill `#eef1f3`, stroke `#d5dbdf` (1.2px), text `#95a5a6`.
- **Legend note (13px, `#4a5866`, left-aligned at x=gx+236, 38px below the rows' start):** "greyed = cell the metric cannot see".

## 2. Why ROC Flatters Imbalanced Data

The chart is computed from one model at 1% prevalence — same scores, same thresholds, both curves.

- ROC plots TPR against `FPR = FP/(FP+TN)`. When negatives outnumber positives 99:1, TN is enormous, so FPR stays near zero even as false positives pile up.
- Precision uses `TP/(TP+FP)` with no TN term, so it registers the same false positives immediately.
- A respectable AUC-ROC and a poor AUC-PR are routinely the same model. Neither number is wrong; they answer different questions.

**Philosophy callout:** Under imbalance, ask what fraction of your positive predictions are correct — not what fraction of a vast negative class you avoided.

### Visualization (canvas `c2`, 720×330)

Two side-by-side line-chart panels: ROC curve and Precision-Recall curve computed from the same simulated model.

- **Title (bold 16px, `#1a5276`, top center):** "Same Model, 1% Prevalence".
- **Data generation:** binormal model with separation d=1.6 and prevalence π=0.01. For 401 thresholds t sweeping 6 → −6: TPR = 1 − Φ(t − d), FPR = 1 − Φ(t), precision = (TPR·π) / (TPR·π + FPR·(1−π)), where Φ is the normal CDF (implemented via an erf approximation). AUCs computed by trapezoid rule from the generated points and printed with 2 decimals (AUC-ROC ≈ 0.87, AUC-PR much lower — values are computed, not hardcoded).
- **Panels:** each 236 wide × 196 high, top at y=54; left panel x=62, right panel x = w−236−44. L-shaped axes in `#95a5a6` (1.4px). Panel title 13px `#1a5276` centered above; x-axis label centered below; y-axis label rotated −90° at 34px left of the panel; tick labels "1" and "0" on y, "1" at x-max (12px, `#5a6875`).
  - Left panel: title "ROC", x-axis "FPR", y-axis "TPR"; dashed (5/4) gray `#95a5a6` chance diagonal from bottom-left to top-right; curve of (FPR, TPR) points, 3px stroke in green `#27ae60`.
  - Right panel: title "Precision-Recall", x-axis "Recall", y-axis "Precision"; dashed horizontal baseline at precision = prevalence (0.01) near the bottom; curve of (Recall, Precision) points, 3px stroke in red `#e74c3c`.
- **AUC labels (bold 14px, centered under each panel, 52px below the axis):** "AUC {value}" — green under ROC, red under PR.
- **Captions (13px, `#4a5866`, centered, 72px below the axis):** "looks strong" under ROC; "the honest view" under PR.

## 3. Classification Metrics

Full-width comparison table (`table.compare`), columns Metric / Use when / Fails when:

| Metric | Use when | Fails when |
|---|---|---|
| Accuracy | Balanced classes, equal misclassification cost | Any imbalance — the majority-class baseline already scores high |
| Precision | False positives are expensive (spam, ad spend, recommendations) | Used alone — silent about how much of the minority class you missed |
| Recall | False negatives are expensive (screening, fraud, safety) | Used alone — predicting all-positive gives 1.0 |
| F1 | One number balancing precision and recall, moderate imbalance | FP and FN costs differ — use F-beta |
| F-beta | Asymmetric costs; beta > 1 favours recall, beta < 1 favours precision | Beta is usually picked by feel, not derived from costs |
| AUC-ROC | Threshold-free model comparison, balanced or mild imbalance | Severe imbalance — optimistic because TN dominates FPR |
| AUC-PR | Severe imbalance; minority-class retrieval quality | Balanced data — adds complexity over ROC without new insight |
| MCC | Balanced single summary at any class ratio; binary problems | Multiclass — extensions exist but lose interpretability |
| Cohen's Kappa | Agreement above chance; inter-annotator reliability | Extreme prevalence — can read low despite high accuracy |
| Log loss | Calibrated probabilities matter, not just the ordering | Only the hard decision is consumed downstream |
| Brier score | Overall probability accuracy; proper scoring rule | Extreme imbalance — dominated by majority-class calibration |
| Lift / gain | Top-k operational triage: campaigns, prioritised review queues | You need whole-population performance |

## 4. Regression Metrics

Full-width comparison table (`table.compare`), columns Metric / Use when / Fails when:

| Metric | Use when | Fails when |
|---|---|---|
| RMSE / MSE | Large errors are disproportionately costly | Outliers dominate; MSE units are squared and hard to talk about |
| MAE | All errors cost the same per unit; robust to outliers | Large errors genuinely matter more |
| MAPE | Relative error matters; targets span different scales | Actuals near zero blow it up; over-prediction is penalised harder than under-prediction, so it quietly rewards forecasting low |
| R² | Variance-explained summary across models on the same data | Cross-dataset comparison; says nothing about bias direction |
| Quantile loss | Asymmetric costs; prediction intervals | Symmetric costs — unnecessary machinery |

## 5. Choosing One

Four questions settle most cases, in this order:

- **Are FP and FN equally bad?** If not, the metric must be asymmetric.
- **How skewed are the classes?** Skewed pushes you off accuracy and ROC toward PR and MCC.
- **Threshold or ranking?** A fixed operating point needs point metrics; triage needs AUC or lift.
- **Do probabilities get consumed?** If a human or a downstream cost model reads them, add log loss or Brier.

**Key point:** Optimising log loss and optimising F1 select different models, not just different scores. Pick the metric before tuning, not after.

### Visualization (canvas `c3`, 720×320)

Vertical decision-flow diagram of four question boxes with "no" branches to metric recommendations.

- **Title (bold 16px, `#1a5276`, top center):** "Four Questions, In Order".
- **Question boxes:** four boxes 320×44 px at x=40, starting y=52, 18px vertical gap; fill `#f4f7f9`, stroke `#95a5a6` (1.6px); question text 13px `#2c3e50` left-aligned with 14px inset. In order:
  1. "FP and FN cost the same?" → no-branch: "F-beta, cost-sensitive" (red `#e74c3c`)
  2. "Classes badly skewed?" → no-branch: "AUC-PR, MCC" (orange `#e67e22`)
  3. "Ranking rather than a fixed cut?" → no-branch: "AUC, lift, gain" (purple `#8e44ad`)
  4. "Probabilities consumed downstream?" → no-branch: "log loss, Brier" (blue `#2980b9`)
- **No-branches:** horizontal 44px arrow (2px line + filled triangle head) from each box's right edge, colored per step; recommendation text bold 13px in the step color 10px right of the arrowhead; small "no" label (12px, `#5a6875`) above the arrow start.
- **Yes-path:** vertical connector between consecutive boxes at x=64 (gray `#95a5a6`, 1.6px) with "yes" label (12px, `#5a6875`) beside it.
- **Terminal line (13px, green `#27ae60`, left-aligned at x=64 below the last box):** "all yes → accuracy or F1 is defensible".

## 6. Open Questions

- Is F1 ever the right optimisation target, or should you always optimise a proper scoring rule and set the threshold afterwards?
- What test-set size makes a metric estimate meaningful — how wide is the confidence interval at n = 500 versus n = 50,000?
- Which two or three metrics should always be reported together to close the single-metric blind spot?

## Regeneration instructions

- **Template/layout:** backlog detail page. Body: h1 with inline `.status` badge, `.subtitle` paragraph, `.intro` callout, then `.lang-section` blocks each with an h2 (blue, 2px `#2980b9` bottom border) and either (a) a `table.layout` with one `<tr>`: left `td.text-col` (45%) holding paragraphs/bullets/callouts, right `td.viz-col` (55%) holding one canvas, or (b) a full-width `table.compare` (sections 3–4), or (c) a plain `<ul>` (section 6, Open Questions).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; h2 1.3rem `#1a5276`. `.intro`/`.philosophy`: background `#f0f4f8`, left border 3px `#2980b9`. `.key-point`: background `#f8f9fa`, left border 3px `#e74c3c`. `.status` badge: background `#fef9e7`, border 1px `#f39c12`, text `#b7950b`, radius 4px. `code`: background `#e8f0f8`, color `#1a5276`. `table.compare`: th background `#1a5276` white text, td 8px 12px padding with `#eee` bottom border, even rows `#f8fafb`, first column bold. Canvases have 1px `#e0e0e0` border, 4px radius, `width: 100%`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, secondary blue `#2980b9`, purple `#8e44ad`, amber `#f39c12`.
- **Canvas:** each canvas declares intrinsic `width`/`height` attributes as specified; a shared `setupCanvas(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates.
- No nav bar, no back/home links. This page has no outbound card links; any regenerated links elsewhere use `.html` extensions.
