# Invented Probabilities

**Page type:** detail page (two-column obj-table layout: text left 40%, canvas right 60%, one row)
**HTML title tag:** Invented Probabilities — Pseudoscience in Data Analysis

**Subtitle:** Assigning Probabilistic Relationships Without Proving They Exist

## Section 1: Assigning Probabilistic Relationships Without Proving They Exist

- **The 50-50 fallacy (Young Sheldon):** "Either I find a million dollars under my bed or I don't — 50-50!" Two outcomes do not imply equal probability; you need a **probability space with a defined measure**, not a count of outcomes — this confuses the sample space with a uniform distribution over it.
- **Macro correlation ≠ micro determinism:** "People who shower get dressed afterward" (true) slides into "the shower determines shirt color" (false). The macro event correlates with showering, but the micro attributes (color, material, style) carry zero information from the upstream event.
- **Temporal sequence ≠ causal probability:** A repeatedly preceding B proves nothing if both are driven by a common factor (time of day, routine). Correlation requires covariation, not co-occurrence in time.
- **Possibility inflated to probability:** "It's possible gene X causes disease Y" slides into "gene X probably causes Y" with no evidence updating the prior. Possible means P > 0 (could be 10⁻⁹); probable means meaningfully likely given data.

**Why it's pseudoscience:** Probability is grounded in the Kolmogorov axioms, frequency data, or Bayesian updating — not intuition about "could happen." Without observed covariation, a defined probability space, or a causal mechanism, P(B|A) is not a probabilistic relationship — it's a story in mathematical clothing.

### Visualization (canvas `c1`, 720×360)

Two-column comparison diagram: invented vs real probabilistic relationships, split by a vertical divider.

- **Background:** full-canvas light gray `#f9f9f9`.
- **Title (600 16px, `#1a5276`, left-aligned at x=20, y=30):** "Invented vs Real Probabilistic Relationships".
- **Vertical divider:** 1px `#ccc` line from (360, 60) to (360, 340).
- **Left column — INVENTED (wrong):**
  - Header (600 14px, red `#e74c3c`, at x=40, y=70): "INVENTED (no evidence)".
  - Four bullet lines (13px, `#333`, x=40, y=100/125/150/175):
    - "• Two outcomes → 50-50 (ignores distribution)"
    - "• Temporal sequence → causal link"
    - "• Macro correlation → micro determinism"
    - "• \"Possible\" → \"Probable\""
  - Fake mini bar chart: red `#e74c3c` axes (x-axis from (60,210) to (300,210), y-axis from (60,210) to (60,300), width 2); two bars filled `rgba(231,76,60,0.3)` — bar A at (80,250) 50×50, bar B at (160,240) 50×60; red 11px labels "A" at (100,330), "B" at (180,330), and "P(B|A) = ???" at (120,220).
- **Right column — REAL (correct):**
  - Header (600 14px, green `#27ae60`, at x=400, y=70): "REAL (evidence-based)".
  - Four bullet lines (13px, `#333`, x=400, y=100/125/150/175):
    - "• Defined probability space (set theory)"
    - "• Observed covariation (frequency data)"
    - "• Causal mechanism (intervention test)"
    - "• Bayesian update from prior"
  - Real mini bar chart: green `#27ae60` axes (x-axis from (420,210) to (660,210), y-axis from (420,210) to (420,300), width 2); two bars filled `rgba(39,174,96,0.3)` — bar A at (440,260) 50×40, bar B at (520,240) 50×60; green 11px labels "A" at (460,330), "B" at (540,330), and "P(B|A) = 0.73" at (480,220).
  - Green arrow line (width 1.5) from (500,260) to (540,240), with label "(from data)" at (550,230).

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table with one `<tr>`; left `<td>` (40%) holds `.obj-title` + bullet list + closing paragraph, right `<td>` (60%, centered) holds the canvas.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#666` 1.0em; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`; `ul` 0.9em `#333`. No nav bar, no back/home links.
- **Canvas:** intrinsic `width`/`height` attributes as given; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#333`.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
