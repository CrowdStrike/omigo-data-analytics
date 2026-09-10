# Train on Batch-Computed Features, Serve with Real-Time Features

**Page type:** detail page (two `.card-section` blocks — The Anti-Pattern / The Design Pattern — each a two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** Train on Batch-Computed Features, Serve with Real-Time Features

**Subtitle:** Batch SQL '30-day average' ≠ streaming pipeline rolling average — subtle differences compound

## The Anti-Pattern

Training uses batch-computed features (perfect SQL queries), but serving uses a streaming pipeline with different edge cases, NULL handling, and timestamp logic. The same feature name produces subtly different values in each context.

- Feature stores with separate batch and real-time compute paths
- Real-time ML serving with streaming aggregations
- Any system where batch-train and online-serve compute features differently
- Different NULL handling: SQL COALESCE vs. stream skip

**Key point (callout):** Training-serving skew is invisible at deployment time and only shows up as silent accuracy loss in production.

### Visualization (canvas `c1`, 720×300)

Two pipeline boxes with a big "≠" between them, plus diverging feature-value lines below.

- **Batch box (left):** 260×100 at y=30 (two boxes 260 wide with a 60px gap, centered as a pair); fill `rgba(26,82,118,0.08)`, stroke `#1a5276` width 2. Bold 12px centered heading "BATCH (SQL)" in `#1a5276`, then three 11px `#2c3e50` lines: "AVG(last 30d)", "NULL → 0", "timestamp: end_of_day".
- **Serving box (right):** 260×100; fill `rgba(230,126,34,0.08)`, stroke `#e67e22` width 2. Bold 12px centered heading "SERVING (stream)" in `#e67e22`, then three 11px `#2c3e50` lines: "rolling_avg(30d)", "NULL → skip", "timestamp: event_time".
- **Not-equal sign:** bold 36px red `#e74c3c` "≠" centered in the gap between the boxes.
- **Diverging lines below (from x=100 to x=w−100, baseline lineY=boxY+boxH+40, y = lineY+60 − value×80):**
  - Batch line, blue `#1a5276` width 2, 10 values: `[0.5, 0.52, 0.48, 0.51, 0.50, 0.53, 0.49, 0.52, 0.50, 0.51]` (stays flat).
  - Serving line, orange `#e67e22` width 2, 10 values: `[0.5, 0.49, 0.45, 0.43, 0.40, 0.38, 0.35, 0.33, 0.30, 0.28]` (drifts down).
  - Right-end 10px labels: blue "batch feature value" and orange "serving feature value" at each line's final y.
- **Bottom caption:** bold 11px red centered "Subtle differences compound over time".

## The Design Pattern

Use a unified feature pipeline: the same code computes features for both training and serving. If impossible, log serving-time features and compare against batch offline. Alert on any systematic deviation.

- Single compute_feature() function used by both paths
- If dual paths unavoidable: log serving features, replay against batch
- Parity monitoring: alert if |batch - serving| > epsilon
- Feature store with point-in-time correctness guarantees

**Key point (callout):** Same name, same code, same result. If you can't guarantee this, you must monitor for drift between the two paths.

### Visualization (canvas `c2`, 720×300)

Fan-out diagram: one shared compute function feeding both training and serving, plus a parity-monitoring box.

- **Title (bold 13px green `#27ae60`, centered, y=20):** "Unified feature pipeline — same code, both paths".
- **Compute box:** 220×50 centered at y=40; fill `rgba(39,174,96,0.1)`, stroke `#27ae60` width 2; bold 13px green centered label "compute_feature()".
- **Arrows:** blue `#1a5276` arrow (width 2, filled arrowhead) from the box's lower-left down-left to a point 120px left of center; orange `#e67e22` arrow from the lower-right down-right to a point 120px right of center (both ending 60px below the box).
- **Endpoint labels (bold 13px, centered):** blue "Training" on the left, orange "Serving" on the right; between them a bold 24px green "=" at center.
- **Parity monitoring box:** 440×45 centered, 50px below the endpoints; fill `rgba(39,174,96,0.06)`, stroke `#27ae60` width 1.5; centered 12px green text "+ parity monitoring: alert if |batch − serving| > ε".
- **Bottom note:** 11px `#666` centered "One source of truth eliminates training-serving skew".

## Regeneration instructions

- **Layout:** two `.card-section` divs, each with an `<h2>` ("The Anti-Pattern", "The Design Pattern", 1.3rem `#1a5276`, bottom border `2px solid #2980b9`) followed by a `table.layout` (width 100%, border-collapse) with one `<tr>`: left `td.text-col` (45%) holding a paragraph, `<ul>` bullets, and a `.key-point` callout; right `td.viz-col` (55%) holding the canvas.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with bottom border `2px solid #2980b9`; `.subtitle` `#666` 0.95rem; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `ul` 0.92rem. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300 per chart, CSS `width: 100%` with `1px solid #e0e0e0` border and 4px radius; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)` family.
- Any card links in regenerated HTML use `.html` extensions.
