# Report a Single Summary Statistic

**Page type:** detail page (anti-pattern/design-pattern pair: two `.card-section` blocks, each a two-column layout table — text left 45%, canvas right 55%)
**HTML title tag:** Report a Single Summary Statistic

**Subtitle:** If bimodal with peaks at 20 and 80, mean=50 represents a state that NEVER occurs

## The Anti-Pattern

Report "mean = 50" as the summary. Hides multi-modality, spikes, tails.

**Key point (red-left-border callout):** A single number collapses the entire distribution shape into one point — erasing the very structure that matters for decisions.

*Domain examples where this misleads:*

- **CPU utilization** — idle at 5% or pegged at 95%, mean says "50% used"
- **Income** — clusters at $30k and $120k, mean suggests $75k (nobody earns that)
- **Latency** — cache hit 2ms vs cache miss 200ms, mean of 50ms is fictional

### Visualization (canvas `c1`, 720×300)

Bimodal density curve with a misleading dashed mean line dropped in the empty valley.

- **Plot area:** baseline at y = h−40; margins left/right 40. Gray x-axis line `#ccc` along the baseline; tick labels 0, 20, 40, 60, 80, 100 in 11px `#666` with 4px tick marks, x mapped linearly 0–100 across the plot width.
- **Humps:** two smooth Gaussian-style humps (`exp(-3t²)` profile over t∈[−1,1], 60 steps), fill `rgba(26,82,118,0.35)`, stroke `#1a5276` width 1.5:
  - Left hump centered at x=20, peak y=60 (tall), spread 12% of plot width.
  - Right hump centered at x=80, peak y=50, spread 13% of plot width.
- **Mean line:** vertical dashed red line (`#e74c3c`, dash 6/4, width 2) at x=50, from y=30 down to the baseline.
- **Labels:** "mean = 50" bold 13px red centered above the line (y=22); "(no data here!)" italic 12px red centered in the valley (baseline − 30).
- **Title (top-left, bold 12px `#1a5276`):** "Bimodal distribution — mean is misleading".

## The Design Pattern

Always report shape class alongside summary stats (bell / skew / bimodal / spike).

**Key point (red-left-border callout):** Include: shape, skewness, Gini, max/mean ratio. Bimodal with mean 50? Report the two peaks, not the fictional mean.

- **Shape class** — bell, skew-right, bimodal, spike, uniform
- **Skewness** — direction and magnitude of asymmetry
- **Gini coefficient** — inequality / concentration measure
- **Max/mean ratio** — flags heavy tails or outlier dominance

*Bimodal with mean 50? Report "peak₁=20, peak₂=72, valley at 45" — describe the reality, not the fiction.*

### Visualization (canvas `c2`, 720×300)

Same bimodal density curve, properly annotated with peak markers and a stats box.

- **Plot area:** identical axes/tick setup as canvas c1 (baseline y = h−40, margins 40, ticks 0–100 by 20 in `#666`).
- **Humps:** same hump style as c1 (fill `rgba(26,82,118,0.35)`, stroke `#1a5276`): left centered at x=20, peak y=60, spread 12%; right centered at x=72, peak y=50, spread 13%.
- **Peak lines:** vertical dashed green lines (`#27ae60`, dash 6/4, width 2) at x=20 and x=72, from y=40 down to the baseline.
- **Peak labels:** " peak₁=20" and " peak₂=72" bold 12px green, centered above each line (y=34).
- **Stats box (top-right):** 210×56 rectangle at x = w−40−220, y=10; fill `#f8f9fa`, stroke `#1a5276` width 1.5. Three left-aligned lines of 11px `#1a5276` text: "shape: bimodal", "skew: 0.3", "gini: 0.41".
- **Title (top-left, bold 12px `#1a5276`):** "Bimodal distribution — properly described".

## Regeneration instructions

- **Layout:** anti-pattern-pairs detail page. h1 with 2px `#2980b9` bottom border, `.subtitle` paragraph, then two `.card-section` divs ("The Anti-Pattern", "The Design Pattern"). Each section: h2 (1.3rem `#1a5276`, 2px `#2980b9` bottom border) followed by a full-width `table.layout` with one row — `td.text-col` (45%) holding the paragraph, `.key-point` callout, optional `.example` italic text and `<ul>`; `td.viz-col` (55%) holding one `<canvas width="720" height="300">`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; `.subtitle` `#666` 0.95rem; table cells padding 12px, vertical-align top; canvas `width:100%`, border `1px solid #e0e0e0`, radius 4px; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem; ul 0.92rem. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. A shared `drawHump(ctx, centerX, peakY, baseY, spread, fillColor, strokeColor)` helper draws each Gaussian hump. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`, gray text `#666`.
- In regenerated HTML, any card links use `.html` extensions.
