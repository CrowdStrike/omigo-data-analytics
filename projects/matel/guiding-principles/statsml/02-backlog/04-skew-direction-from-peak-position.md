# Post-Processing: Skew Direction from Peak Position

**Page type:** detail page (backlog kusto-style: intro callout, numbered h2 sections, each a 2-col table with text left 45% / canvas right 55%)
**HTML title tag:** Post-Processing: Skew Direction from Peak Position — Discussion Backlog

**Subtitle:** Simple arithmetic on the mass distribution relative to the peak

**Intro callout:** After the CNN classifies a histogram as "mountain," determine skew direction through simple arithmetic on the mass distribution relative to the peak.

## 1. Mass-Split Algorithm

- **Step 1:** Find peak bin position
- **Step 2:** Compute mass LEFT vs RIGHT of peak
- **Step 3:** Apply significance threshold:
  - ~50/50 → `bell`
  - 60%+ on one side → `skewed`

Need `> 0.5 + 2/sqrt(n)` for 2-sigma significance:

- n=100 → need 60% on one side
- n=1000 → need 53% on one side
- n=10000 → need 52% on one side

### Visualization (canvas `c1`, 720×300)

Right-skewed histogram with a dashed vertical peak line and mass percentages labeled left and right of the peak.

- **Title (bold 16px, `#1a5276`, top center):** "Right-Skewed: Mass Split at Peak".
- **Margins:** top 30, right 30, bottom 45, left 50.
- **Data:** 24 bins, chi-squared-like right-skewed shape; value at bin i (x = (i+0.5)/24): `max((x*4)^1.5 * exp(-x*5) * 3, 0.01)`. Peak found as the argmax bin (peaks early, long right tail). Y scale = data max.
- **Bars color-coded by side of peak:** bins ≤ peak — fill `rgba(39,174,96,0.35)`, stroke `#27ae60`; bins > peak — fill `rgba(231,76,60,0.35)`, stroke `#e74c3c`; stroke width 0.8.
- **Peak line:** vertical dashed (dash 5/3) line at the peak bin center, `#1a5276`, width 2; bold blue label "PEAK" centered below it on the axis.
- **Mass labels:** left/right mass computed as sums of bin values on each side divided by total. Left side: bold 19px green (`#27ae60`) percentage (rounds to 68%) centered over the left region with 14px "LEFT mass" beneath. Right side: bold 19px red (`#e74c3c`) percentage (32%) with 14px "RIGHT mass" beneath.
- **Threshold annotation (orange `#e67e22`, 14px, right-aligned below axis):** "Threshold (n=100): need 60% for significance".
- **X-axis:** horizontal baseline in `#2c3e50`, width 1.
- **Caption (italic, `.example`):** Right-skewed: LEFT mass 68% vs RIGHT mass 32%

## 2. Band Enhancement

Compare band widths on each side of peak. Wider band = fewer samples = sparser tail. This gives a second confirmation signal.

**Key Question:** Does the generative sigmoid already handle this? It outputs right_skew=87%, bell=45% — but those are independent scores, not a LEFT/RIGHT decomposition.

### Visualization (canvas `c2`, 720×300)

Same right-skewed histogram with a dashed SE band whose width varies with density, color-tinted by side of peak.

- **Title (bold 16px, `#1a5276`, top center):** "Band-Width Asymmetry: Narrow (Dense) vs Wide (Sparse Tail)".
- **Margins:** top 30, right 30, bottom 45, left 50.
- **Data:** same 24-bin right-skewed shape as c1.
- **Bars:** fill `rgba(26,82,118,0.3)`, stroke `#1a5276` width 0.5.
- **SE band:** per-bin half-width `1.96 * sqrt(p*(1-p)/200) * maxVal + 0.015 * maxVal` where p = data[i]/maxVal; upper and lower boundary polylines through bin centers in `#e67e22`, width 2, dashed 4/3, clipped to [0, max].
- **Band fill (per-bin rectangles between upper and lower):** `rgba(39,174,96,0.15)` for bins ≤ peak (dense side), `rgba(231,76,60,0.15)` for bins > peak (sparse side).
- **Vertical width markers:** solid 2px vertical line spanning the band at bin 3 in `#27ae60` (dense side) and at bin 18 in `#e74c3c` (sparse side).
- **Annotations (14px, centered, below axis):** green (`#27ae60`) two lines near bin 3: "narrow band" / "= dense (many samples)"; red (`#e74c3c`) two lines near bin 18: "wide band" / "= sparse (few samples)".
- **X-axis:** horizontal baseline in `#2c3e50`, width 1.
- **Caption (italic, `.example`):** Band-width asymmetry confirms skew direction

## Regeneration instructions

- **Layout:** backlog detail page (kusto-style). Structure: `<h1>` with bottom border `2px solid #2980b9`, `.subtitle` paragraph, one `.intro` callout, then one `.lang-section` per numbered section. Each section: `<h2>` ("N. Title", bottom border `2px solid #2980b9`), then a `table.layout` (width 100%, border-collapse) with one `<tr>`: left `td.text-col` (45%) holding bullets/paragraphs and optional `.key-point` callout, right `td.viz-col` (55%) holding the canvas plus an italic `.example` caption. No index number in the h1/title.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276`; h2 1.3rem `#1a5276`; subtitle `#666` 0.95rem. `.intro`: background `#f0f4f8`, left border `3px solid #2980b9`, padding 8px 12px, 0.9rem. `.key-point`: background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.example`: italic, `#555`, 0.9rem. `ul` 0.92rem, nested `ul` for sub-bullets. `code`: background `#e8f0f8`, color `#1a5276`, padding 2px 6px, radius 3px. Canvas: `width: 100%`, border `1px solid #e0e0e0`, radius 4px. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`.
- **Canvas:** intrinsic size 720×300 per chart; a shared `setupCanvas(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates.
- In regenerated HTML, any card/page links use `.html` extensions.
