# Cross-entropy

**Page type:** detail page (card-section layout: Overview two-column table with text left 45% / canvas right 55%, then a full-width Real-World Examples section with canvas + callouts, then a Quick Decision Guide table)
**HTML title tag:** Cross-entropy — Statistical Tests Reference

**Back link (top of page):** "← Statistical Tests Reference" pointing to `../12-statistical-tests.md` (in regenerated HTML: `../12-statistical-tests.html`), styled `color:#2980b9`, no underline, 0.9em.

**Subtitle:** Measures average bits needed to identify an event using predicted vs true distribution

## Overview

**What it measures**

The average number of bits needed to identify an event from a set, using a predicted distribution q instead of the true distribution p. Used as both a loss function and comparison metric.

**Key assumptions**

- Predicted probabilities are calibrated (reflect true likelihoods)
- Classes are mutually exclusive (for categorical CE)
- No predicted probability is exactly 0 or 1 (log undefined)
- Training labels are correct (label noise amplifies loss)

**What breaks when violated**

- Overconfident wrong predictions: predicting p=0.99 when true label is 0 gives loss = -log(0.01) = 4.6 (catastrophic penalty)
- Poorly calibrated: low CE does NOT mean good predictions — model can have low loss but terrible calibration
- Relationship to KL divergence: CE(p,q) = H(p) + D_KL(p||q). Minimizing CE = minimizing KL divergence from true distribution.

**Failure box (`.failure`, monospace, red-left-border):**

Model A: predicts [0.99, 0.01] for true class 0. Loss = 0.01.
Model B: predicts [0.51, 0.49] for true class 0. Loss = 0.67.
But Model A on a WRONG prediction: [0.99, 0.01] true class 1. Loss = 4.6!
One overconfident mistake costs more than 400 uncertain-but-correct predictions.

**Alternative box (`.alt-note`, green-left-border):**

**Use instead:** Label smoothing to prevent overconfidence. Focal loss for class imbalance. Brier score when calibration matters more than discrimination. Temperature scaling post-hoc.

### Visualization (canvas `c5`, 960×460)

Horizontal bar chart: cross-entropy loss for five prediction scenarios, showing the overconfidence penalty.

- **Title (bold 13px, `#1a5276`, top center):** "Cross-Entropy Loss: Overconfidence Penalty".
- **Data (one horizontal bar per row, top to bottom):**
  - "Correct, confident" — p=0.99, actual 1, loss 0.01, green `#27ae60`
  - "Correct, uncertain" — p=0.70, actual 1, loss 0.36, green `#27ae60`
  - "Correct, barely" — p=0.51, actual 1, loss 0.67, green `#27ae60`
  - "Wrong, uncertain" — p=0.70, actual 0, loss 1.20, orange `#e67e22`
  - "WRONG, confident" — p=0.99, actual 0, loss 4.60, red `#e74c3c` (catastrophic, loss > 4)
- **Geometry:** bars start at x=220, max width 350px scaled to maxLoss=5.0, height 32px, 8px gap, first bar at y=42. Scenario label right-aligned to the left of each bar (17px, `#333`); loss value "loss = X.XX" (bold 12px, red `#e74c3c` if catastrophic else `#1a5276`) to the right of the bar; "p=0.XX" in white 10px SF Mono inside the bar when bar width > 50px.
- **Annotation (bottom center, 17px red `#e74c3c`):** "One overconfident mistake (4.6) > 400 correct predictions (400 x 0.01)".
- **Scale:** thin gray `#ccc` horizontal axis line at y = h-30 spanning the bar area, tick labels 0–5 in gray `#999` centered under it.

## Real-World Examples

### Visualization (canvas `c5r`, 960×300)

Two-panel figure: left, a calibration plot; right, text/bar summary of the biopsy impact.

- **Title (bold 16px, `#1a5276`, top center):** "🏥 Radiology AI: Low Loss Hides Dangerous Miscalibration".
- **Left panel (calibration plot):** plot area from x=100 to w/2-30, y from 40 to h-50.
  - Dashed gray (`#bbb`, dash 4/4, width 1.5) diagonal from bottom-left to top-right, labeled "Perfect calibration" (12px `#999`) near the top-right.
  - Model calibration curve: red `#e74c3c` line (width 2.5) with 4px-radius red dots through bins (predicted confidence → actual accuracy): (0.55, 0.58), (0.65, 0.62), (0.75, 0.70), (0.85, 0.78), (0.95, 0.84) — the last bin is overconfident.
  - Annotation near the last bin, three lines of 13px red text: "95% confident" / "but 84% accurate" / "→ 11% false positive!".
  - Axis labels (13px `#666`): x "Predicted Confidence" centered below; y "Actual Accuracy" rotated vertical on the left.
- **Right panel (starting at x = w/2+40):**
  - Loss comparison (15px `#1a5276`): "Before: CE loss = 0.08" then "After temp scaling: CE loss = 0.12".
  - Then 14px `#666`: '"Worse" loss, but calibrated =' followed by bold 14px green `#27ae60`: "fewer unnecessary biopsies".
  - Patients affected (14px `#333`): '200 "malignant" predictions at >95% conf:'; then red `#e74c3c`: "Before: 11 are benign (5.5% false positive)" / "= 11 unnecessary biopsies"; then green `#27ae60`: "After: 3 are benign (1.5% false positive)" / "= 8 biopsies avoided".
  - Mini bars: red `#e74c3c` bar 110×14 labeled "11 FP (before)" in white; green `#27ae60` bar 30×14 labeled "3" in white with "(after)" in `#333` beside it.

**Real-world callout 1 (`.real-world`, orange-left-border), domain line "🏥 Medical imaging: Cancer detection model":**

A radiology AI model achieves cross-entropy loss of 0.08 on the validation set — looks excellent. But examining calibration: on 200 scans where it predicts "malignant" with confidence >95%, 11 are actually benign. Those 11 overconfident false positives each contribute loss ~3.0 (vs 0.05 for correct-and-confident). The low average loss masked dangerous miscalibration: patients would get unnecessary biopsies. Temperature scaling (T=1.4) raised the loss to 0.12 but produced well-calibrated confidence — an actual improvement despite the "worse" metric.

**Real-world callout 2 (`.real-world`), domain line "📧 Spam classification: Imbalanced production traffic":**

A spam filter trained with standard cross-entropy achieves 0.04 loss. But in production, 0.1% of emails are spam. The model learns to predict "not spam" with p=0.998 for everything — loss is tiny because it's almost always right. The 50 actual spam emails per day get predicted at p=0.7 spam — not confident enough to filter. Switching to focal loss (γ=2) forces the model to work harder on the rare spam cases, raising average loss to 0.09 but catching 47/50 spam emails instead of 31/50.

## Quick Decision Guide

| Data Situation | If Assumptions Met | If Violated | Universal Fallback |
|---|---|---|---|
| Classification loss function | Cross-entropy (calibrated model) | Overconfident: single mistake dominates | Label smoothing / Focal loss |

## Regeneration instructions

- **Layout:** three `.card-section` blocks (Overview, Real-World Examples, Quick Decision Guide), each with an `h2` underlined by `2px solid #2980b9`. Overview uses `table.layout` with `td.text-col` (45%) holding `.obj-title` headings + paragraphs/lists/callouts and `td.viz-col` (55%) holding canvas `c5`. Real-World Examples is full width: canvas `c5r` then two `.real-world` divs. Quick Decision Guide holds the `.decision-table`.
- **Back link:** page opens with `<a href="../12-statistical-tests.html">← Statistical Tests Reference</a>` before the h1 (color `#2980b9`, no underline, 0.9em).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`; subtitle `#666` 0.95rem; `strong` and `code` in `#1a5276`, code on `#e8f0f8`; `.obj-title` 1.05em weight 600 `#1a5276`.
- **Callout styles:** `.failure` — background `#fdedec`, left border `3px solid #e74c3c`, monospace ('SF Mono'/'Fira Code'), color `#922`, 0.85rem. `.alt-note` — background `#eafaf1`, left border `3px solid #27ae60`, color `#1a5276`, 0.85rem. `.real-world` — background `#fef9e7`, left border `4px solid #e67e22`, 0.88rem, `.domain` line weight 600 color `#7d6608`, `strong` in `#e67e22`.
- **Decision table:** `.decision-table` — header row background `#1a5276` white text; cell borders `1px solid #e0e0e0`; even rows `#fafcfe`; 3rd column text `#e74c3c`, 4th column `#27ae60` weight 500.
- **Canvas:** intrinsic `width`/`height` attributes as given (`c5` 960×460, `c5r` 960×300), CSS `width:100%` with `1px solid #e0e0e0` border and 4px radius; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, accent blue `#2980b9`, gray text `#666`/`#333`/`#999`.
