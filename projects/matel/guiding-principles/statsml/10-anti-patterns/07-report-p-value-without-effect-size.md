# Report P-Value Without Effect Size

**Page type:** detail page (anti-pattern/design-pattern pair: two `.card-section` blocks, each a two-column layout table — text left 45%, canvas right 55%)
**HTML title tag:** Report P-Value Without Effect Size

**Subtitle:** At n=100,000: enrichment 1.006× is "significant" (p<0.001) but useless for classification

## The Anti-Pattern

Statistical significance ≠ practical significance. Large n makes trivially small effects "significant".

**Key point (red-left-border callout):** With enough data, any non-zero effect becomes statistically significant—even effects too small to act on.

*Domain examples:*

- Large datasets (n > 10k) where noise becomes "significant"
- Clinical trials with massive enrollment
- Any high-n analysis where effect size is not reported

### Visualization (canvas `c1`, 720×300)

Single-bar enrichment chart on a zoomed-in scale showing a "significant" but negligible 1.006× effect, with a red REJECT verdict.

- **Background:** full-canvas `#fefefe` fill. Chart area: left 120, right w−80, top 50, bottom h−60.
- **Title (bold 14px `#2c3e50`, top center):** "n = 100,000 — Statistically Significant, Practically Useless".
- **Y-axis:** enrichment 0.9 to 1.1; tick labels "0.90x", "0.95x", "1.00x", "1.05x", "1.10x" in 11px `#666`, right-aligned, with `#eee` horizontal gridlines; L-shaped axes in `#bbb`.
- **Baseline:** dashed `#1a5276` line (dash 6/4, width 2) at y=1.0, labeled "Baseline (1.0x)" in bold 11px `#1a5276` to the right of the plot.
- **Bar:** 80px wide, centered horizontally. Sliver from 1.0 to 1.006 filled `rgba(231, 76, 60, 0.3)` and outlined `#e74c3c` width 2; the remainder of the bar from 1.0 down to the chart bottom filled `rgba(26, 82, 118, 0.15)` with stroke `rgba(26, 82, 118, 0.4)` width 1.
- **Labels:** "Enrichment: 1.006x" bold 13px red 30px above the bar top, with a small red down-arrow (stroke width 1.5, with chevron head) pointing to the tiny gap; "p < 0.001" bold 12px red centered 20px below the chart bottom.
- **Verdict (right side, vertically centered at x = w−55):** "✗" bold 40px `#e74c3c` with "REJECT" bold 13px beneath.
- **Caption (bottom center, italic 11px `#999`):** "\"Significant\" but effect is 0.6% above baseline".

## The Design Pattern

Require BOTH p < threshold AND enrichment > minimum practical level. Gate: p < 0.05/n_tests (Bonferroni) AND enrichment > 1.5×. Report effect size FIRST, p-value second.

**Key point (green-left-border callout):** Practical significance gates prevent trivial effects from polluting results.

- Compute effect size (enrichment, Cohen's d, odds ratio)
- Apply Bonferroni correction: p < 0.05 / n_tests
- Require enrichment > 1.5× minimum practical threshold
- Report effect size first, p-value second
- Reject results that pass p-value but fail effect size gate

### Visualization (canvas `c2`, 720×300)

Single-bar enrichment chart with baseline and orange threshold lines, a strong 3.2× effect passing both gates, and a green ACCEPT verdict.

- **Background:** full-canvas `#fefefe` fill. Chart area: left 120, right w−80, top 50, bottom h−60.
- **Title (bold 14px `#2c3e50`, top center):** "Dual Gate: Effect Size + Statistical Significance".
- **Y-axis:** enrichment 0 to 4.0; tick labels "0.0x", "1.0x", "1.5x", "2.0x", "3.0x", "4.0x" in 11px `#666` with `#eee` gridlines (width 0.5); L-shaped axes in `#bbb`.
- **Baseline:** dashed `#1a5276` line (dash 4/3, width 1.5) at y=1.0, labeled "Baseline (1.0x)" in 10px `#1a5276` to the right.
- **Threshold:** dashed orange `#e67e22` line (dash 8/4, width 2.5) at y=1.5, labeled "Threshold (1.5x)" in bold 11px `#e67e22` to the right.
- **Bar:** 80px wide, centered; full bar from 0 to 3.2 filled `rgba(39, 174, 96, 0.25)` outlined `#27ae60` width 2; the portion above the 1.5× threshold overlaid with a darker fill `rgba(39, 174, 96, 0.4)`.
- **Labels:** "Enrichment: 3.2x" bold 14px green 12px above the bar top; "p < 0.001" bold 12px `#1a5276` centered 20px below the chart bottom.
- **Verdict (right side, vertically centered at x = w−55):** "✓" bold 40px `#27ae60` with "ACCEPT" bold 13px beneath.
- **Caption (bottom center, italic 11px `#555`):** "Passes both gates: p < 0.001 AND enrichment 3.2x > 1.5x threshold".

## Regeneration instructions

- **Layout:** anti-pattern-pairs detail page. h1 with 2px `#2980b9` bottom border, `.subtitle` paragraph, then two `.card-section` divs ("The Anti-Pattern", "The Design Pattern"). Each section: h2 (1.3rem `#1a5276`, 2px `#2980b9` bottom border) followed by a full-width `table.layout` with one row — `td.text-col` (45%) holding the paragraph, `.key-point` callout (design-pattern one overrides border to `#27ae60` via inline style), optional `.example` italic lead-in and `<ul>`; `td.viz-col` (55%) holding one `<canvas width="720" height="300">`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; `.subtitle` `#666` 0.95rem; table cells padding 12px, vertical-align top; canvas `width:100%`, border `1px solid #e0e0e0`, radius 4px; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem; ul 0.92rem. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`.
- In regenerated HTML, any card links use `.html` extensions.
