# Deploy Model and Check Results Quarterly

**Page type:** detail page (two `.card-section` blocks — The Anti-Pattern / The Design Pattern — each a two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** Deploy Model and Check Results Quarterly

**Subtitle:** Model silently degrades for 3 months — thousands of wrong predictions served

## The Anti-Pattern

Without continuous monitoring, model degradation goes undetected for months. Upstream data changes, concept drift, and population shifts silently erode performance while the system serves increasingly wrong predictions.

- Schema changes in upstream data pipelines
- Feature store staleness or missing values
- Concept drift — relationship between features and target shifts
- Population shift — new user segments not seen in training

**Key point (callout):** A model without monitoring is a model without accountability. You won't know it's broken until someone complains.

### Visualization (canvas `c1`, 720×300)

Line chart: model performance silently degrading over 3 months with no monitoring.

- **Title (bold 13px red `#e74c3c`, centered, y=20):** "Silent degradation — discovered too late".
- **Axes:** L-shaped axes in `#2c3e50` (width 1.5); plot area left=70, right=w−40, top=40, bottom=h−50. Rotated 11px y-axis label "Performance" in `#2c3e50`. X-axis labels (11px `#666`): "Month 1", "Month 2", "Month 3" centered at thirds of the plot width.
- **Performance line:** `#1a5276`, width 2.5, 31 points over t=0..1: performance 0.92 flat for t<0.2, then declining linearly to 0.86 at t=0.4 (slope −0.3), then declining faster (slope −0.8) to about 0.38 at t=1. Plotted as y = bottom − plotH×perf.
- **Shaded region:** `rgba(231,76,60,0.08)` rectangle covering the plot from t=0.2 to t=1.0, with semi-transparent (globalAlpha 0.5) bold 14px red centered text "No monitoring!" near the top of the region.
- **End marker:** thick red X (`#e74c3c`, width 3, arms ±10px) at the end of the line (x=right−20), with bold 11px red label "Discovered!" to its right.

## The Design Pattern

Validate data on every input (schema, range, null rate, KS test vs. reference). Monitor distribution of every output (prediction distribution vs. expected). Alert before serving if inputs shift beyond tolerance.

- Input validation: schema checks, range bounds, null rate thresholds
- Distribution monitoring: KS test on features vs. last week
- Output monitoring: prediction distribution stability
- Alert fires immediately — fix within hours, not months

**Key point (callout):** Monitoring is not optional. If you can't detect degradation within 24 hours, you shouldn't be in production.

### Visualization (canvas `c2`, 720×300)

Line chart: performance drifts, alert fires at day 8, fast recovery within a day.

- **Title (bold 13px green `#27ae60`, centered, y=20):** "Continuous monitoring — detect and recover fast".
- **Axes:** same L-shaped `#2c3e50` axes and plot area as c1 (left=70, right=w−40, top=40, bottom=h−50); rotated 11px y-axis label "Performance". X-axis labels (10px `#666`), evenly spaced: "Day 1", "Day 3", "Day 5", "Day 7", "Day 8", "Day 10", "Day 12", "Day 14".
- **Threshold line:** horizontal red dashed line (`#e74c3c`, width 1.5, dash 6/4) at 0.75 of plot height, with 10px right-aligned red label "threshold" just left of the axis.
- **Stable segment:** blue line (`#1a5276`, width 2.5) flat at performance 0.92 from t=0 to t=0.43.
- **Drift segment:** red line (`#e74c3c`, width 2.5) from (t=0.43, 0.92) down to (t=0.57, 0.72).
- **Alert marker at t=0.57:** filled orange triangle (`#e67e22`, apex 18px above the point) containing a bold 10px white "!", with bold 11px orange label "ALERT!" above it.
- **Recovery segment:** green line (`#27ae60`, width 2.5) from the alert point up to (t=0.71, 0.90), then to (t=1.0, 0.92); bold 11px green label "Fixed within 1 day" centered above the recovered line at t≈0.78.

## Regeneration instructions

- **Layout:** two `.card-section` divs, each with an `<h2>` ("The Anti-Pattern", "The Design Pattern", 1.3rem `#1a5276`, bottom border `2px solid #2980b9`) followed by a `table.layout` (width 100%, border-collapse) with one `<tr>`: left `td.text-col` (45%) holding a paragraph, `<ul>` bullets, and a `.key-point` callout; right `td.viz-col` (55%) holding the canvas.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with bottom border `2px solid #2980b9`; `.subtitle` `#666` 0.95rem; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `ul` 0.92rem. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300 per chart, CSS `width: 100%` with `1px solid #e0e0e0` border and 4px radius; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)` family.
- Any card links in regenerated HTML use `.html` extensions.
