# Weak Baseline Selection

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvases right ~60%, single row)
**HTML title tag:** Weak Baseline Selection — Common Bad Practices

**Subtitle:** Manufactured Win — Choose a straw-man baseline so your method looks revolutionary.

## Section: The Practice

- Compare your deep learning model against "random" or a deliberately hobbled heuristic. Your model: 87% accuracy. Published baseline: "random = 50%." Delta: +37%! Impressive! But tuned XGBoost on same features = 85%. Real delta: +2%.
- **In industry:** "Our new recommendation engine improved CTR by 200%!" Baseline: the broken/untuned version nobody would ship. Honest baseline: last quarter's production model. Real improvement: 5%.
- **In research:** Compare against a 2015 paper instead of current SOTA. Or: use SOTA but with default hyperparameters (no tuning), guaranteeing it underperforms.
- **Variant — test vs no-feature:** Compare "model with feature X" against "no model at all" instead of "model without feature X." The feature gets credit for the entire model's performance, not just its marginal contribution.

**Why it persists:** Nobody checks what the baseline IS. "Beat baseline by X%" is the headline. The asterisk (which baseline?) is in the appendix nobody reads. Reviewers/managers see the delta, not the denominator.

**The tell:** Ask "what's the STRONGEST baseline you compared against?" If they get defensive or say "that's not fair" — they know their improvement is marginal.

### Visualization (canvas `c1`, 720×340)

Three-bar chart with claimed vs real improvement arrows.

- **Bars** (80px wide, 100px gap, centered as a group; baseline at h−30, full scale = 100% over h−60 px; 17px labels below each bar and value labels "N%" above each bar in `#2a2a2a`):
  | Label | Value | Fill |
  |---|---|---|
  | Random | 50% | `#e74c3c` |
  | Your Model | 87% | `#27ae60` |
  | Honest Baseline | 85% | `#2980b9` |
- **Claimed arrow:** dashed red (`#e74c3c`, dash 4/3, width 2) horizontal arrow from the Random bar toward the Your Model bar at 20px above the Random bar top, with bold 18px red label "+37% (what they claim)" above it.
- **Real arrow:** solid dark blue (`#1a5276`, width 2) horizontal arrow from the Honest Baseline bar back to the Your Model bar at 15px below the Your Model bar top, with bold 18px `#1a5276` label "+2% (real improvement)" above it.

### Visualization (canvas `c2`, 720×300)

Three side-by-side scenario panels: same model bar over three different baseline bars.

- **Title (bold 17px, top center, `#1a5276`):** "Baseline Selection Changes Perception".
- **Three panels** (starting x=40, 240px apart, y=50; horizontal bars 200px full scale, 20px tall):
  - In each panel, top bar = "Your Model: 87%" — width 87% of scale, fill `rgba(39,174,96,0.3)`, stroke `#27ae60`, bold 18px `#27ae60` label inside.
  - Bottom bar (30px lower) = the baseline — fill `rgba(231,76,60,0.2)`, stroke `#e74c3c`, 16px `#e74c3c` label inside.
  - A `#1a5276` width-2 bracket line between bar ends, with bold 18px `#1a5276` centered label "Gap: {label}" below.
  | Panel | Baseline | Baseline value | Gap label |
  |---|---|---|---|
  | 1 | Random (50%) | 50 | huge |
  | 2 | Default XGBoost (82%) | 82 | moderate |
  | 3 | Tuned XGBoost (85%) | 85 | tiny |
- **Takeaway (bold 16px `#e74c3c`, centered, y=185):** "Same model. Three different \"improvements\" depending on what you compare against."

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>`; left `<td>` (40%) holds `.obj-title` ("The Practice") + `ul` bullets + two `<p><strong>…</strong></p>` paragraphs, right `<td>` (60%, centered) holds the two canvases stacked.
- **Page style:** body `-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif`, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; subtitle `#666` 1.0em; table cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`; `ul` 0.9em; canvases `display: block; margin: 0 auto`.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart text uses `-apple-system`/sans-serif at 16–18px. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, gray text `#666`.
- No nav bar, no back/home links. (In regenerated HTML, any card links elsewhere use `.html` extensions; this detail page has no links.)
