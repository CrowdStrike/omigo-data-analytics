# Cargo Cult Statistics

**Page type:** detail page (two-column obj-table layout: text left ~40%, two stacked canvases right ~60%, single row)
**HTML title tag:** Cargo Cult Statistics — Pseudoscience in Data Analysis

**Subtitle:** Statistical rituals performed without understanding their requirements

## The Rituals of Science Without the Substance

- **p-value without understanding:** "p=0.04, it's significant!" — but what was the hypothesis, was it pre-registered, how many tests were run, and what are the effect size and CI? Reporting p<0.05 without that context is a ritual, not reasoning.
- **R² without validation:** "Our model has R²=0.92!" — on training data, test data, or new data? R²=0.92 on training with 100 features and 100 data points is memorization, not learning; the high number feels like science but is overfitting.
- **Confidence intervals without calibration:** Reporting a 95% CI without ever checking whether the true value actually falls inside 95% of the time. Uncalibrated intervals are theater — they look rigorous but aren't.
- **"Controlled for confounders":** Twenty variables thrown into a regression — but were unmeasured confounders addressed, and were mediators distinguished from confounders? Throwing variables into a regression is not controlling for them; it's a ritual that looks like careful analysis.

**Richard Feynman's "Cargo Cult Science" (1974):** Pacific islanders built runways, control towers, and wooden headphones hoping planes would land — the form of an airport without the function. Researchers likewise use the form of statistics (p-values, CIs, regression) without the function of actually validating claims; the rituals look like science from outside but produce nothing.

### Visualization (canvas `c1`, 720×340)

Two-column text diagram: ritual performed vs what is missing.

- **Title (bold 17px `#1a5276`, top center):** "Cargo Cult: Has the FORM of Science, None of the FUNCTION"
- **Rows (one per 45px starting y≈57; ritual in bold orange `#e67e22` left-aligned at x=50, missing part in red `#e74c3c` prefixed with "→ " at x=230):**
  - "Report p-value" → "No pre-registration, no effect size, no multiple testing correction"
  - "Say \"controlled for\"" → "Didn't check unmeasured confounders, mediators vs confounders"
  - "Show R²=0.92" → "On training data only. No test set. No cross-validation."
  - "Cite \"the literature\"" → "Cherry-picked supportive papers. Ignored contradicting ones."
- **Bottom line (bold 17px `#555`, centered, y=h-8):** "Looks scientific from outside. Produces nothing on closer inspection."

### Visualization (canvas `c2`, 720×300)

Two-column checklist table: form (done, green check) vs function (skipped, red X).

- **Title (bold 17px `#1a5276`, top center):** "Checklist: Form vs Function"
- **Column headers (bold 16px, centered at 25% and 70% width, y=38):** "Form (they do)" in `#27ae60`; "Function (they skip)" in `#e74c3c`.
- **Rows (28px tall starting y≈65, alternating row background `rgba(26,82,118,0.04)` on even rows; each row has a green `#27ae60` "✓" before the left text and a red `#e74c3c` "✗" before the right text, texts in `#2c3e50` 17px centered on their columns):**
  - "Report p-value" / "Understand what it means"
  - "Say \"controlled for\"" / "Actually controlled"
  - "Show confidence interval" / "Calibrated interval"
  - "Cite literature" / "Read the papers"
  - "Use big words" / "Know their meaning"
- **Divider:** vertical dashed gray line (`#bdc3c7`, dash 3/3, width 1) at x=w/2 spanning the rows.

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` with left `<td>` (40%) holding `.obj-title`, a `<ul>` of bullets, and a closing `<p>`; right `<td>` (60%, centered) holding the two canvases stacked.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#666` 1.0em; `p` 0.95em `#333`; `ul` 0.9em `#333`, `li` margin 6px 0; `strong` `#1a5276`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, grays `#555`/`#999`/`#bdc3c7`, dark slate `#2c3e50`.
- In regenerated HTML, any card links use `.html` extensions.
