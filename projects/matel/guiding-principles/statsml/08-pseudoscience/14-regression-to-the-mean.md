# Regression to the Mean Misattribution

**Page type:** detail page (single-row obj-table layout: text left ~40%, two stacked canvases right ~60%)
**HTML title tag:** Regression to the Mean Misattribution — Pseudoscience in Data Analysis

**Subtitle:** Extreme Values Always Drift Back to Average

## Section: "Treatment Worked!" — No, Extreme Values Always Drift Back to Average

- **Sports Illustrated curse:** "Athletes perform worse after being on the cover!" They made the cover because of an extreme performance, and extreme performances are followed by average ones. The decline was already statistically inevitable — regression to the mean, not a curse.
- **Speed cameras:** "Cameras at accident hotspots → accidents dropped 30%!" Hotspots were identified by unusually high accident counts, which return to normal with or without cameras. The cameras took credit for regression to the mean.
- **Remedial programs:** "Students below the 20th percentile took our program → scores improved 10 points!" Bottom scorers tend to score higher next time anyway (measurement noise plus mean reversion). The program takes credit for what would have happened without it.
- **Sick patients:** "Patients at their worst visited the healer → felt better!" People seek healers at their lowest point, from which the natural direction is back toward baseline. The healer takes credit for natural recovery.

**Why it's pseudoscience:** Any intervention applied at an extreme value appears to work, because extremes regress toward the mean regardless; without a control group starting from the same extreme, "treatment worked" is indistinguishable from ordinary math.

### Visualization (canvas `c1`, 720×340)

Zigzag time series around a dashed mean line, with the extreme peak and trough marked as "intervention" points.

- **Title (bold 17px, top center, `#1a5276`):** "Extreme Values ALWAYS Drift Back — With or Without Intervention".
- **Plot margins:** left 60, right 40, top 50, bottom 30.
- **Mean line:** dashed blue horizontal line (`#2980b9`, dash 4/4, width 1) at the vertical midpoint of the plot, labeled "Mean" in blue 17px to its right.
- **Series:** dark line (`#333`, width 2) through 13 evenly spaced points with normalized values `[0.5, 0.6, 0.9, 0.55, 0.4, 0.1, 0.45, 0.55, 0.85, 0.5, 0.4, 0.6, 0.5]` (0 = plot bottom, 1 = plot top).
- **Extreme high marker:** red dot (`#e74c3c`, radius 7) at point index 2 (value 0.9), with bold 17px centered label above: "Peak! → \"Intervene here\"".
- **Extreme low marker:** red dot (radius 7) at point index 5 (value 0.1), with bold 17px centered label below: "Trough! → \"Treat here\"".
- **Caption (bold gray `#555` 17px, bottom center):** "Both return to mean regardless of intervention. The \"cure\" takes credit for math.".

### Visualization (canvas `c2`, 720×300)

Two overlaid decay curves — with and without intervention — following the same trajectory back to the mean.

- **Title (bold 16px `#2c3e50`, centered at y=18):** "Identical outcome with or without intervention. The \"cure\" took credit for math.".
- **Mean line:** dashed gray horizontal line (`#95a5a6`, dash 4/4, width 1) at y = height/2+10 from x=60 to x=width−40, labeled "Mean" in gray 16px to its left.
- **WITH intervention curve:** solid red (`#e74c3c`, width 2) quadratic curve from (100, 50) through control point (250, 50) down to the mean line at x=400, then flat along the mean to x=width−60. Bold red 18px label at (100, 42): "WITH intervention". Orange (`#e67e22`) 16px marker text "Intervention applied here" at (220, 70) with a short vertical orange tick at x=250. Red 16px right-aligned label just above the mean line at the right end: "\"It worked!\"".
- **WITHOUT intervention curve:** the identical trajectory drawn in blue (`#1a5276`, width 2) — first solid along the same path, then repeated dashed (dash 6/3) offset 5px below to remain distinguishable. Bold blue 18px label at (100, height−25): "WITHOUT intervention (same trajectory)". Blue 16px right-aligned label just below the mean line at the right end: "Same result".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>`; left `<td>` (40%) holds `.obj-title`, bullets, and the "Why it's pseudoscience" paragraph; right `<td>` (60%, centered) holds two stacked canvases (`c1` 720×340, `c2` 720×300).
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#666` 1.0em; paragraphs `#333` 0.95em; `ul` 0.9em `#333` with 6px item spacing; table cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; `canvas { display: block; margin: 0 auto; }`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, accent blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, grays `#555`/`#95a5a6`, dark slate `#2c3e50`.
- In regenerated HTML, any card links use `.html` extensions.
