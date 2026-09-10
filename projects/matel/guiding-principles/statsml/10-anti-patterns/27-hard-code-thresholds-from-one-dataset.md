# Hard-Code Thresholds from One Dataset

**Page type:** detail page (anti-pattern-pair layout: two card-sections, each a two-column table — text left 45%, canvas right 55%)
**HTML title tag:** Hard-Code Thresholds from One Dataset

**Subtitle:** "Anomaly if > 1000" works on THIS dataset. Deploy to new client: normal values are 5000+

## The Anti-Pattern

Magic numbers derived from one context. The threshold becomes meaningless in a new deployment. Requires manual tuning per client.

**Key point (red-left-border callout):** A hard-coded threshold encodes the *specific distribution* of one dataset as a universal truth.

*Domain examples:*

- Alerting systems
- Anomaly detection
- Data validation

### Visualization (canvas `c1`, 720×300)

Number-line diagram: a fixed threshold at 1000 against two datasets' value ranges — fits Dataset A, flags all of Dataset B.

- **Title (bold 14px `#1a5276`, left-aligned at (20, 28)):** "Fixed Threshold = 1000 (hard-coded)".
- **Axis:** 1px `#999` horizontal number line at y=h−50 from x=60 to x=w−30, linear scale 0→8000 mapped across the width; tick marks every 1000 with 11px `#666` centered labels "0", "1000", …, "8000".
- **Threshold line:** 3px vertical `#e74c3c` line at value 1000 from y=45 down to the axis; bold 12px `#e74c3c` centered label above it: "threshold = 1000".
- **Dataset A band:** filled rectangle from value 200 to 800, 30px tall centered at y=100, fill `rgba(26,82,118,0.35)`, stroke `#1a5276` 2px; 12px `#1a5276` label above-left: "Dataset A: values 200-800"; bold 11px `#27ae60` label to the right of the band: "Works fine here".
- **Dataset B band:** filled rectangle from value 3000 to 7000, 30px tall centered at y=190, fill `rgba(231,76,60,0.2)`, stroke `#e74c3c` 2px; 12px `#e74c3c` label above-left: "Dataset B: values 3000-7000"; bold 11px `#e74c3c` label to the right: "ALL flagged as anomalies!".
- **Warning label:** bold 14px `#e74c3c` centered at (w/2, axisY−15): "Useless in new context!".

## The Design Pattern

Derive thresholds from data statistics (percentiles, mean + 3 sigma). Self-calibrating across populations.

**Key point (red-left-border callout):** Let each deployment discover its own boundary from its own data.

*Steps:*

- Collect baseline sample from the deployment
- Compute percentile (e.g., p99) or mean + k*sigma
- Set threshold = computed statistic
- Re-calibrate periodically as data evolves

### Visualization (canvas `c2`, 720×300)

Side-by-side distribution curves, each with its own p99 threshold line derived from its own data.

- **Title (bold 14px `#1a5276`, left-aligned at (20, 28)):** "Percentile-Based Thresholds (p99 per dataset)".
- **Layout:** canvas split into two equal regions (width (w−60)/2 each, starting at x=60), separated by a dashed (dash 3/3) 1px `#ddd` vertical divider from y=45 to y=h−40.
- **Distribution shapes:** each drawn as a filled bell-ish curve (quadratic curves from base up to a peak 30px above baseline y=110, half-width ≈ 38% of the region), with a 1.5px stroke in the solid version of the fill color.
  - Left: Dataset A — fill `rgba(26,82,118,0.25)`, stroke `#1a5276`, centered at 45% of its region; 12px `#2c3e50` label above-left: "Dataset A (values 200-800)".
  - Right: Dataset B — fill `rgba(230,126,34,0.25)`, stroke `#e67e22`, centered at 45% of its region; label: "Dataset B (values 3000-7000)".
- **Threshold lines:** dashed (dash 5/3) 2.5px vertical `#27ae60` line at 82% of each region's width, spanning ±35px around the baseline; bold 11px `#27ae60` centered label below each: "p99 = 950" (left), "p99 = 7200" (right).
- **Success message:** bold 14px `#27ae60` centered at (w/2, h−20): "Self-adapting per deployment".
- **Annotation (11px `#555`, centered, two lines at y=180/195):** "Each dataset gets its own threshold" / "derived from its own distribution".

## Regeneration instructions

- **Template/layout:** anti-pattern-pair detail page. h1 with `border-bottom: 2px solid #2980b9`, `.subtitle` paragraph, then two `.card-section` divs ("The Anti-Pattern", "The Design Pattern"). Each section: `h2` (1.3rem, `#1a5276`, 2px `#2980b9` bottom border) followed by a `table.layout` with one `<tr>`: left `td.text-col` (45%) holding paragraph + `.key-point` callout + `.example` label ("Domain examples:" / "Steps:") + `<ul>`; right `td.viz-col` (55%) holding one `<canvas>`.
- **Page CSS:** universal reset; body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; `.subtitle` `#666` 0.95rem, margin-bottom 32px; `.card-section` margin-bottom 40px; table cells `vertical-align: top`, padding 12px; canvas `width: 100%`, `1px solid #e0e0e0` border, radius 4px; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem; ul 0.92rem with 20px left margin. No nav bar, no back/home links.
- **Canvas:** each canvas drawn at intrinsic 720×300 and scaled via `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) through a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; band/curve fills use rgba variants including `rgba(26,82,118,0.35)`.
- **Links:** none on this page; in regenerated HTML any card links elsewhere use `.html` extensions.
