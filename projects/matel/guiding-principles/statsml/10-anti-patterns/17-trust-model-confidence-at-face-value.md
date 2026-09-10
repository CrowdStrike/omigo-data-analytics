# Trust Model Confidence at Face Value

**Page type:** detail page (two `.card-section` blocks — The Anti-Pattern / The Design Pattern — each a two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** Trust Model Confidence at Face Value

**Subtitle:** Model says '85% probability' but was never calibrated — actual rate could be 50% or 95%

## The Anti-Pattern

Model outputs a score that users treat as a calibrated probability. Without verification, "when the model says 80%, does the event actually happen 80% of the time?" is unknown. Most models are overconfident by default.

- All probabilistic classifiers (logistic regression, random forests, neural nets)
- Risk scores in healthcare and finance
- Churn prediction confidence
- Medical diagnosis probability estimates

**Key point (callout):** A probability that isn't calibrated is just a ranking score wearing a lab coat.

### Visualization (canvas `c1`, 720×300)

Reliability diagram: overconfident uncalibrated model curve bowing below the perfect-calibration diagonal.

- **Title (bold 13px red `#e74c3c`, centered above plot):** "Uncalibrated: overconfident model".
- **Axes:** L-shaped `#2c3e50` axes (width 1.5); plot area left=80, right=w−50, top=40, bottom=h−50. Axis labels (11px `#2c3e50`): "Predicted Probability" centered below x-axis, rotated "Actual Frequency" on the left. Ticks (10px `#666`) every 20% on both axes: 0%, 20%, 40%, 60%, 80%, 100%.
- **Perfect-calibration diagonal:** gray dashed line (`#aaa`, width 1, dash 5/4) from bottom-left to top-right, with a rotated 10px `#999` label "Perfect calibration" placed along it at ~(0.3, 0.35).
- **Uncalibrated curve:** red line (`#e74c3c`, width 2.5) through (predicted, actual) points: `[0,0.05], [0.1,0.04], [0.2,0.08], [0.3,0.12], [0.4,0.18], [0.5,0.25], [0.6,0.30], [0.7,0.40], [0.8,0.55], [0.9,0.70], [1.0,0.85]`.
- **Annotations:** 4px red dot at (0.8, 0.55) with bold 10px red label "Says 80%, Actual: 55!"; 4px red dot at (0.6, 0.30) with label "Says 60%, Actual: 30!".
- **Legend (bottom left, 11px):** short red line segment (width 2) followed by red text "Model output (uncalibrated)".

## The Design Pattern

Apply Platt scaling or isotonic regression after training to calibrate outputs. Plot the calibration curve (reliability diagram) to verify. If not calibrated, treat scores as ordinal (ranking) not cardinal (true probabilities).

- Platt scaling: fit sigmoid on held-out set logits
- Isotonic regression: non-parametric monotonic calibration
- Plot reliability diagram: predicted vs. observed frequency
- If calibration is impossible, clearly label outputs as "scores" not "probabilities"

**Key point (callout):** Calibrated probabilities enable rational decision-making. Uncalibrated scores only support ranking.

### Visualization (canvas `c2`, 720×300)

Reliability diagram: calibrated model curve hugging the perfect-calibration diagonal.

- **Title (bold 13px green `#27ae60`, centered above plot):** "Calibrated: predictions match reality".
- **Axes, labels, ticks, and gray dashed diagonal:** identical to c1 (plot area left=80, right=w−50, top=40, bottom=h−50; "Predicted Probability" / "Actual Frequency"; 20% ticks; `#aaa` dashed diagonal, no diagonal label).
- **Calibrated curve:** green line (`#27ae60`, width 2.5) through points: `[0,0.02], [0.1,0.09], [0.2,0.19], [0.3,0.28], [0.4,0.38], [0.5,0.49], [0.6,0.58], [0.7,0.69], [0.8,0.79], [0.9,0.88], [1.0,0.97]`.
- **Annotation:** 4px green dot at (0.8, 0.79) with bold 11px green label "80% predicted ≈ 80% actual ✓".
- **Method label:** 12px green centered text "Platt scaling / isotonic regression" inside the plot near the top-left (at 0.4 of plot width, 20px below the top).
- **Legend (bottom left, 11px):** short green line segment (width 2) followed by green text "Calibrated model output".

## Regeneration instructions

- **Layout:** two `.card-section` divs, each with an `<h2>` ("The Anti-Pattern", "The Design Pattern", 1.3rem `#1a5276`, bottom border `2px solid #2980b9`) followed by a `table.layout` (width 100%, border-collapse) with one `<tr>`: left `td.text-col` (45%) holding a paragraph, `<ul>` bullets, and a `.key-point` callout; right `td.viz-col` (55%) holding the canvas.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with bottom border `2px solid #2980b9`; `.subtitle` `#666` 0.95rem; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `ul` 0.92rem. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300 per chart, CSS `width: 100%` with `1px solid #e0e0e0` border and 4px radius; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)` family.
- Any card links in regenerated HTML use `.html` extensions.
