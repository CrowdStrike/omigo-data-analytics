# Deep-Dive Analysis of Popular Statistical Tests

**Page type:** detail page (backlog 2-col layout: text left 45%, canvas right 55%, one `table.layout` row per section)
**HTML title tag:** Deep-Dive Analysis of Popular Statistical Tests — Discussion Backlog

**Subtitle:** How popular tests behave on real data — edge cases, violations, silent failures

**Intro callout:** The assumptions reference covers what each test needs. But we lack analysis of HOW each test behaves on real data — edge cases, sensitivity to violations, power curves, when they silently give wrong answers.

## 1. Tests Needing Deep-Dive

- **t-test:** How robust really? At what skew × n does it break?
- **z-test (proportions):** np ≥ 10 rule — what happens at np=8?
- **Proportion test:** Bucket purity testing — when is "75% positive" significant?
- **Chi-squared:** Expected count < 5 rule — how wrong do p-values get?
- **KS test:** Conservative with ties — how much power lost on discrete data?
- **Cross-entropy:** As loss function AND comparison metric
- **Gini impurity:** vs entropy for splits — do they differ?
- **Information gain:** Bias toward high-cardinality features
- **Entropy:** As a shape descriptor

### Visualization (canvas `c1`, 720×300)

Horizontal stacked bar chart: per-test assumption regions (valid / caution / violated).

- **Title (bold 16px, `#1a5276`, top center):** "Test Assumption Regions: Valid / Caution / Violated".
- **Legend (14px, below title, starting at left margin + 10):** green swatch `#27ae60` "Valid", orange swatch `#e67e22` "Caution", red swatch `#e74c3c` "Violated" (12×10 filled squares, labels `#2c3e50`).
- **Data (fractions valid/caution/danger per test):**
  - t-test: 0.7 / 0.15 / 0.15
  - z-test (prop): 0.5 / 0.2 / 0.3
  - Proportion: 0.6 / 0.2 / 0.2
  - Chi-squared: 0.55 / 0.25 / 0.2
  - KS test: 0.4 / 0.3 / 0.3
  - Cross-entropy: 0.65 / 0.2 / 0.15
  - Gini: 0.75 / 0.15 / 0.1
  - Info gain: 0.45 / 0.25 / 0.3
  - Entropy: 0.7 / 0.2 / 0.1
- **Layout:** margins left 100, right 30, top 35, bottom 30; one horizontal bar per test (row height = plot height/9 minus 4px gap); test names right-aligned in 14px `#2c3e50` at the left margin.
- **Segments:** solid fills — valid `#27ae60`, caution `#e67e22`, danger `#e74c3c` — spanning the full plot width proportionally.
- **Border:** thin `#2980b9` rectangle (width 0.5) around the whole plot area.

## 2. What Each Deep-Dive Needs

- Math intuition with visualization
- Real data examples
- Boundary conditions
- Comparison with alternatives

**Key-point callout (red left border):**
**Key Questions:**
(1) Individual docs or consolidated?
(2) Tie-back to multi-candidate framework?
(3) Include simulation results?

### Visualization (canvas `c2`, 720×300)

Multi-series line chart: power curve degradation as assumptions are violated.

- **Title (bold 16px, `#1a5276`, top center):** "Power Curve Degradation as Assumptions are Violated".
- **Axes:** margins left 60, right 30, top 35, bottom 45; L-shaped axes in `#2c3e50` width 1. Rotated y-axis label "Statistical Power" (14px); x-axis label "Assumption Violation Severity" (14px, bottom center). Y ticks 0.0–1.0 in steps of 0.2 (13px labels) with light `#ecf0f1` gridlines. X ticks: "None", "Mild", "Moderate", "Severe", "Extreme" (13px).
- **Series (line width 2.5, one point per x tick):**
  - t-test (n=30), `#1a5276`: `[0.85, 0.82, 0.75, 0.60, 0.45]`
  - t-test (n=100), `#27ae60`: `[0.95, 0.93, 0.88, 0.78, 0.65]`
  - KS test, `#e74c3c`: `[0.70, 0.55, 0.40, 0.28, 0.18]`
  - Chi-squared, `#e67e22`: `[0.80, 0.72, 0.55, 0.35, 0.20]`
- **Legend (13px, upper right of plot, ~130px from the right edge):** short colored line samples with series names in `#2c3e50`, stacked 16px apart.
- **Threshold line:** horizontal dashed gray line (`#95a5a6`, dash 4/3, width 1) at power 0.8, labeled "0.8 threshold" (13px, `#95a5a6`, left-aligned just above the line).

## Regeneration instructions

- **Template:** backlog detail page (kusto-style 2-col layout). Structure: h1, `.subtitle` paragraph, `.intro` callout, then one `.lang-section` per section, each with an `<h2>` and a `table.layout` single `<tr>`: left `<td class="text-col">` (45%) with bullets/key-point, right `<td class="viz-col">` (55%) with the canvas. No index number in the h1.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px solid `#2980b9` bottom border; h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border. `.subtitle` `#666` 0.95rem. `.intro` background `#f0f4f8`, left border 3px solid `#2980b9`, padding 8px 12px, 0.9rem. `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem. `.example` italic `#555` 0.9rem. `ul` 0.92rem. Canvas `width: 100%`, border `1px solid #e0e0e0`, radius 4px. No nav bar, no back/home links.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange, threshold gray `#95a5a6`.
- **Canvas:** intrinsic 720×300 attributes; scale by `window.devicePixelRatio` via a shared `setupCanvas(id)` helper (backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- In regenerated HTML, any card links use `.html` extensions.
