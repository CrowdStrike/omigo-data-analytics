# Pitfall: Label Shift (Train ≠ Deploy Distribution)

**Page type:** detail page (card-section layout: h2 per section, two-column table with text left 45% / canvas right 55%)
**HTML title tag:** Label Shift (Train ≠ Deploy Distribution)

**Subtitle:** Class proportions in production differ from training

## The Problem

**Tags:** `the trap` (red), `base rate` (blue)

- **The setup** — the model trains on a balanced 50/50 sample but scores a 99/1 population
- **Ranking survives** — the ordering of scores holds up, but probabilities and thresholds do not
- **Miscalibrated scores** — a 50/50-trained model over-predicts the rare class at 99/1
- **Stale threshold** — a 0.5 cutoff picked on balanced data is far too high at 1% prevalence
- **No immunity** — even a well-calibrated model breaks at a different deployment prevalence

*Example:* A disease model trained on 50% cases flags 30% of a 1%-prevalence population at threshold 0.5.

**Impact:** The deployed model flags 30% of the population when true prevalence is 1%, swamping downstream review capacity and destroying trust in the scores.

### Visualization (canvas `c1`, 720×300)

Two-panel stacked-bar comparison of class proportions in training vs production, with the same threshold overlaid on both.

- **Title (bold 14px, `#1a5276`, centered):** "Label Shift: Training vs Production Class Distribution".
- **Left panel — "Training Distribution":** 300px-wide horizontal bar at y=80, height 120, split 50/50: left half Negative (fill `#1a5276` at 0.6 alpha, 1.5px `#1a5276` border) labeled "Negative" / "50%"; right half Positive (fill `#27ae60` at 0.6 alpha, green border) labeled "Positive" / "50%".
  - Dashed orange (`#e67e22`, 2.5px, dash 6/3) vertical threshold line at the 50% midpoint, labeled above in bold: "threshold = 0.5".
  - Below the bar, green bold annotation: "✓ Correct threshold".
- **Right panel — "Production Distribution":** same-size bar split 99/1: Negative 99% (fill `#1a5276` at 0.6 alpha) labeled "Negative 99%"; Positive a 1% sliver (fill `#27ae60` at 0.8 alpha) labeled outside with "← 1%" (10px green).
  - Same dashed orange threshold line at the 50% midpoint, labeled "threshold = 0.5".
  - Region from threshold to right end shaded `#e74c3c` at 0.2 alpha, with a red bracket below and red bold annotations: "✗ Flags 30% as positive" / "True positive rate: 1%".
- **Bottom summary (bold 12px red, centered):** "Threshold calibrated on balanced data fails under label shift".

## Why It Happens

**Tags:** `root cause` (orange), `prevalence` (blue)

- **Balanced sampling** — training at 50/50 is sensible for discrimination but hides prevalence
- **Case-control design** — equal cases and controls aid efficiency; deployment sees natural mix
- **Base-rate anchoring** — output probabilities are calibrated to the training base rate
- **Carried unchanged** — those probabilities and thresholds go to production untouched
- **Threshold mismatch** — 0.5 fits 50/50 data but is catastrophically wrong at 99/1

*Example:* A patient scored P(disease)=0.6 by the 50/50-trained model has a true probability near 0.012 at 1% prevalence.

**Root Cause:** The model learns P(Y=1|X) under training P(Y=1)=0.5, so by Bayes' theorem the same score maps to a much lower true probability when production P(Y=1)=0.01.

### Visualization (canvas `c2`, 720×300)

Two-panel overlapping class-conditional density plot showing the same threshold cutting a balanced vs a 99/1 mixture.

- **Title (bold 13px, `#1a5276`, centered):** "How Training Base Rate Distorts Production Predictions".
- **Left panel** (300px wide, at x=40) headed "Training: P(Y=1) = 0.50":
  - Gray x-axis line. Two overlapping Gaussian curves (each spanning 70% of the panel width, peak formula exp(−((x−μ)·4)²/2)): Class 0 centered left, peak 0.8 of the 100px curve height, fill `rgba(26,82,118,0.3)`, stroke `#1a5276`; Class 1 offset right by 30% of the width, same peak 0.8, fill `rgba(39,174,96,0.3)`, stroke `#27ae60`.
  - Dashed orange threshold (2.5px, dash 5/3) at the panel midpoint labeled "t = 0.5".
  - Axis labels: "Class 0" (blue) and "Class 1" (green).
  - Green bold annotation below: "✓ Balanced: threshold cuts evenly".
- **Right panel** (at x=390) headed "Production: P(Y=1) = 0.01":
  - Same two curves, but Class 0 peak is 0.95 (dominant, tall) and Class 1 peak is 0.08 (tiny, barely visible); same fills/strokes.
  - Same dashed orange threshold at the midpoint labeled "t = 0.5".
  - Region right of the threshold shaded `rgba(231,76,60,0.2)`.
  - Axis labels: "Class 0 (99%)" (blue), "Class 1 (1%)" (green).
  - Red bold annotation below: "✗ Same threshold → massive false positives".
- **Bottom line (bold 11px red, centered):** "Score 0.5 in training → true P(Y=1) ≈ 0.01 in production (Bayes shift)".

## The Correct Approach

**Tags:** `the fix` (green), `recalibration` (blue)

- **Recalibrate, not retrain** — keep the ranking, remap scores to deployment prevalence
- **Measure prevalence** — estimate the production base rate P(Y=1) before setting anything
- **Platt scaling** — fit a logistic map from raw scores to production-calibrated probabilities
- **Isotonic regression** — use non-parametric calibration when the score map is not logistic
- **Threshold adjustment** — solve the cutoff from the production base rate and cost matrix
- **Monitoring** — divergence of predicted vs observed positive rate signals a new shift

*Example:* Platt scaling on a production-representative set moves the threshold to 0.03, flagging 5% of the population instead of 30% while catching 70% of true positives.

**Fix:** Train on balanced data if useful, but calibrate on production-prevalence data, set the threshold from the production base rate and cost ratio, and monitor predicted vs observed rates.

### Visualization (canvas `c3`, 720×300)

Recalibration pipeline diagram plus a before/after threshold-shift comparison.

- **Title (bold 13px, `#1a5276`, centered):** "Correct: Recalibrate for Deployment Prevalence".
- **Pipeline row** (5 boxes at y=50, height 55, connected by gray `#666` arrows), each with a bold label and a 9px sub-label:
  1. "Train" / "(balanced 50/50)" — blue `#1a5276`, fill `rgba(26,82,118,0.12)`, width 100.
  2. "Raw Scores" / "(0.0 - 1.0)" — orange `#e67e22`, fill `rgba(230,126,34,0.12)`, width 100.
  3. "Platt Scaling" / "(prod. prevalence)" — green `#27ae60`, fill `rgba(39,174,96,0.12)`, width 120.
  4. "Calibrated P" / "(true probs)" — green, width 100.
  5. "Threshold" / "(adjusted)" — blue, width 110.
- **"Threshold Adjustment" section** (bold 11px `#1a5276`, centered heading):
  - Before bar: 250×25 at x=80, fill `rgba(231,76,60,0.1)`, 1px red border; red 2.5px threshold marker at 50% labeled "t = 0.5"; right-side label "Before: 30% flagged"; "0" and "1" endpoints in gray.
  - After bar: same size below, fill `rgba(39,174,96,0.1)`, 1px green border; green 2.5px threshold marker at 3% labeled "t = 0.03"; right-side label "After: 5% flagged, 70% recall"; "0"/"1" endpoints.
  - Dashed orange arrow (2px, dash 4/3) connecting the two threshold markers, labeled in bold orange: "prevalence shift".
- **Bottom comparison:** bold 11px, left in red at 30% width: "Before: t=0.5, flags 30%, useless precision"; right in green at 70% width: "After: t=0.03, flags 5%, 70% true positive recall".
- **Final line (bold 12px green, centered):** "✓ Practical and deployable".

## Regeneration instructions

- **Layout:** `.card-section` per section: `<h2>` with 2px `#2980b9` bottom border, then `table.layout` (border-collapse, full width) with one `<tr>`: `td.text-col` (45%) holding `.tags` pills + `<ul>` bullets + `.example` italic paragraph + `.key-point` callout; `td.viz-col` (55%) holding the canvas.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; h2 1.3rem `#1a5276`. No nav bar, no back/home links.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem. `.example` italic `#555` 0.9rem. `li b` colored `#1a5276`.
- **Canvas:** intrinsic 720×300, CSS `width: 100%`, 1px `#e0e0e0` border, 4px radius; scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, accent `#2980b9`, bar fill `rgba(26,82,118,0.35)`.
- In regenerated HTML, any card links use `.html` extensions.
