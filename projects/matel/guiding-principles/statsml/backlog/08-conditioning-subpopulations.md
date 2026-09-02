# 1-Level Conditioning to Reveal Hidden Subpopulations

**Page type:** detail page (backlog 2-col layout: text left 45%, canvas right 55%, one `table.layout` row per section)
**HTML title tag:** 1-Level Conditioning to Reveal Hidden Subpopulations — Discussion Backlog

**Subtitle:** A noisy mountain may be two clean bells hiding behind a categorical variable

**Intro callout:** A feature that looks unimodal (noisy mountain) may actually be a mixture of clean subpopulations hidden by a categorical variable. Conditioning on one low-cardinality feature at a time can reveal the mixture.

## 1. Method

- For each continuous feature X, split by each low-cardinality feature Z (binary/categorical with 5 or fewer levels)
- Classify X|Z=z for each level separately

*Example (italic):* The combined distribution of weight over all people is a wide, bumpy mountain — a shape that resists any single-distribution label.

### Visualization (canvas `c1`, 720×300)

Histogram: combined "noisy mountain" weight distribution (mixture of two gaussians).

- **Title (bold 17px, `#1a5276`, top center):** "Combined Distribution: Weight (all people)".
- **Data:** 30 bins over x range 45kg–105kg; bin height at x is the sum of two gaussians: female peak `gaussian(mean=65, std=6, amplitude=40)` + male peak `gaussian(mean=80, std=7, amplitude=38)`. Y scale max = 1.1 × max bin value.
- **Margins:** top 40, right 30, bottom 50, left 50.
- **Bars:** fill `rgba(26,82,118,0.35)`, stroke `#1a5276` width 0.5, 1px gap between bars.
- **Axes:** L-shaped axes (left + bottom) in `#2c3e50`, width 1. X-axis labels "45kg" (left), "75kg" (center), "105kg" (right) in 13px `#2c3e50`. Rotated y-axis label "Count" (14px, `#2c3e50`) on the left.
- **Shape label (italic 12px, `#e67e22`, centered below plot):** 'Shape: "noisy mountain" — wide, bumpy'.
- **Arrow:** orange (`#e67e22`) right-pointing arrow at far right edge, vertically centered (line width 3 with filled triangular head), labeled "split by Z" (13px) above it.

## 2. Signal After Splitting

- If X is "noisy bell" but X|Z=male and X|Z=female are both "clean bell" with different means, then Z explains the shape
- **Example:** Weight looks like a wide mountain with bumps. Split by gender yields two tight bells at 65kg and 80kg
- **Extensions:** Works for any latent grouping: profession, region, age-band, treatment/control
- Automatically detects Simpson's paradox scenarios

**Key-point callout (red left border):**
**Key Questions:**
(1) Only low-cardinality splits?
(2) How to measure "cleaner after split"?
(3) Minimum n per subgroup?

### Visualization (canvas `c2`, 720×300)

Overlaid histograms: same weight axis split by gender into two clean bells.

- **Title (bold 17px, `#1a5276`, top center):** "After Split: Weight | Gender".
- **Data:** 30 bins over 45kg–105kg. Female bins: `gaussian(mean=65, std=5, amplitude=38)`; male bins: `gaussian(mean=80, std=5.5, amplitude=36)`. Fixed y scale max = 42. Bars below height 1px are skipped.
- **Margins:** top 40, right 30, bottom 50, left 50; same L-shaped `#2c3e50` axes.
- **Female bars:** fill `rgba(231,76,60,0.3)`, stroke `#e74c3c` width 0.8. **Male bars:** fill `rgba(26,82,118,0.3)`, stroke `#1a5276` width 0.8.
- **Peak labels (bold 15px):** "Female ~65kg" in `#e74c3c` at the female peak (x = left margin + (20/60)·plot width, near top); "Male ~80kg" in `#1a5276` at the male peak (x = left margin + (35/60)·plot width + 50, near top).
- **Shape labels (italic 11px, `#27ae60`, below plot):** '"clean bell"' under each peak position.
- **X-axis labels:** "45kg", "75kg", "105kg" in 13px `#2c3e50`.
- **Verdict (bold 15px, `#27ae60`, right-aligned bottom):** "Z=Gender EXPLAINS the shape".

## Regeneration instructions

- **Template:** backlog detail page (kusto-style 2-col layout). Structure: h1, `.subtitle` paragraph, `.intro` callout, then one `.lang-section` per section, each containing an `<h2>` and a `table.layout` with a single `<tr>`: left `<td class="text-col">` (45%) holds bullets/example/key-point, right `<td class="viz-col">` (55%) holds the canvas. No index number in the h1.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px solid `#2980b9` bottom border; h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border. `.subtitle` `#666` 0.95rem. `.intro` background `#f0f4f8`, left border 3px solid `#2980b9`, padding 8px 12px, 0.9rem. `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem. `.example` italic `#555` 0.9rem. `ul` 0.92rem. Canvas `width: 100%`, border `1px solid #e0e0e0`, radius 4px. No nav bar, no back/home links.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange, bar fill `rgba(26,82,118,0.35)`.
- **Canvas:** intrinsic 720×300 attributes; scale by `window.devicePixelRatio` via a shared `setupCanvas(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates); a shared `gaussianY(x, mean, std, amplitude)` helper generates the bell shapes.
- In regenerated HTML, any card links use `.html` extensions.
