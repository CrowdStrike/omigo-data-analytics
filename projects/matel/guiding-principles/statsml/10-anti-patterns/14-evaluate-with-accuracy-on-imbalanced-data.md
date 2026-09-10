# Evaluate with Accuracy on Imbalanced Data

**Page type:** detail page (two card-sections, each an h2 + two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** Evaluate with Accuracy on Imbalanced Data

**Subtitle:** '95% accuracy!' on 95/5 data = predicting majority class always. Recall on rare class = 0%.

## The Anti-Pattern

On imbalanced data, a model that always predicts the majority class achieves high accuracy while catching zero rare-class events. The metric is misleading because it rewards ignoring the class you care about most.

- Fraud detection — 0.1% positive rate
- Cancer screening — 1% prevalence
- Cybersecurity intrusion — 0.001% of traffic
- Equipment failure — rare but catastrophic

**Key point (red left border):** 95% accuracy means nothing if the 5% you missed are the only cases that matter.

### Visualization (canvas `c1`, 720×300)

Confusion matrix (2×2) for an always-predict-negative model, with a crossed-out accuracy badge.

- **Matrix:** cells 160×80, matrix origin at x=180, y=50. Column headers (bold 13px `#1a5276`, centered): "Predicted: Negative", "Predicted: Positive"; rotated left label "Actual".
- **Cells:**
  - TN (top-left): fill `rgba(26,82,118,0.15)`, stroke `#1a5276` 1px, bold 24px `#1a5276` text "TN = 950".
  - FP (top-right): fill `#f8f8f8`, stroke `#ccc`, 16px `#999` text "FP = 0".
  - FN (bottom-left): fill `rgba(231,76,60,0.15)`, stroke `#e74c3c` 2px, bold 24px `#e74c3c` text "FN = 50".
  - TP (bottom-right): fill `#f8f8f8`, stroke `#ccc` 1px, 16px `#999` text "TP = 0".
- **Recall label (bold 14px red `#e74c3c`, centered below matrix):** "Recall = 0% — catches ZERO fraud!"
- **Accuracy badge (bottom center, y ≈ h-25):** bold 18px green `#27ae60` at 35% opacity: "95% Accuracy", struck through with a red `#e74c3c` 2px X (two crossing diagonal lines spanning ±65px).

## The Design Pattern

Evaluate with precision, recall, and F1 on the minority class. Report performance at multiple thresholds using a precision-recall curve. Let the business decide the operating point based on the cost of false positives vs. false negatives.

- Precision: of predictions flagged positive, how many are correct?
- Recall: of actual positives, how many did the model catch?
- Plot the full PR curve — every threshold tells a different story
- Business picks the threshold based on cost tradeoff

**Key point (red left border):** The right metric depends on what's more expensive: missing a fraud case (FN) or flagging a legit transaction (FP).

### Visualization (canvas `c2`, 720×300)

Precision-recall curve with a highlighted operating point.

- **Title (bold 13px `#1a5276`, centered above plot):** "Precision-Recall Curve (minority class)"
- **Plot area:** left 80, right w-40, top 30, bottom h-50; L-shaped axes in `#2c3e50` width 1.5. Axis labels 12px `#2c3e50`: "Recall →" centered below, rotated "Precision →" on the left. Tick labels 10px `#666` at 0%, 20%, 40%, 60%, 80%, 100% on both axes.
- **PR curve data (recall, precision):** `[0, 0.98], [0.1, 0.95], [0.2, 0.91], [0.3, 0.86], [0.4, 0.80], [0.5, 0.73], [0.6, 0.64], [0.7, 0.53], [0.8, 0.40], [0.9, 0.28], [1.0, 0.15]` — connected line `#1a5276`, width 2.5.
- **Operating point:** at (recall=0.5, precision=0.73), orange `#e67e22` filled circle radius 7 with 2px orange stroke; labels to its right: bold 11px orange "chosen threshold" and 10px "(recall=50%, precision=73%)".

## Regeneration instructions

- **Layout:** two `.card-section` blocks ("The Anti-Pattern", "The Design Pattern"), each with an `h2` and a `table.layout` (width 100%, border-collapse) containing one row: `td.text-col` (45%) with paragraph + `ul` + `.key-point`, `td.viz-col` (55%) with the canvas. (On this page the `.key-point` comes after the bullet list, and there is no `.example` label.)
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px solid `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border; canvas `width: 100%`, 1px `#e0e0e0` border, 4px radius; `.key-point` background `#f8f9fa`, 3px red `#e74c3c` left border, padding 8px 12px, 0.9rem; `ul` 0.92rem. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Canvas:** intrinsic 720×300, scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; CSS width 100%. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- In regenerated HTML, any card links use `.html` extensions.
