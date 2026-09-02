# Second-Level CNN for Mountain Sub-Classification

**Page type:** detail page (backlog kusto-style: intro callout, numbered h2 sections, each a 2-col table with text left 45% / canvas right 55%)
**HTML title tag:** Second-Level CNN for Mountain Sub-Classification — Discussion Backlog

**Subtitle:** A specialized CNN that only sees mountains and outputs continuous skew scores

**Intro callout:** Instead of threshold-based skew detection, train a specialized CNN that only sees "mountain" histograms and outputs continuous bell/left_skew/right_skew scores.

## 1. Band-Enriched Input

- **Encoding:** 64x64 image with 3 intensity levels
  - Bars = 1.0 (white)
  - Band region = 0.5 (gray)
  - Background = 0.0 (black)
- **Key insight:** Band asymmetry is a strong skew signal the first CNN misses entirely
- **Advantage:** Continuous confidence output vs binary threshold decision

### Visualization (canvas `c1`, 720×300)

Side-by-side comparison of two simulated CNN input images (white histogram on black square), standard vs band-enriched.

- **Title (bold 16px, `#1a5276`, top center):** "Band-Enriched CNN Input (64x64 encoding)".
- **Layout:** two black squares (`#000`, size = min(plotHeight−10, 200) ≈ 200px) side by side, horizontally centered; left square starts at (w − size×2.5)/2, right square offset by 1.5× the square size.
- **Data:** 32 bins, right-skewed mountain; value at bin i (x = (i+0.5)/32): `max((x*4)^1.8 * exp(-x*5.5) * 3.5, 0.01)`. SE band half-width per bin: `1.96 * sqrt(p*(1-p)/150) * maxVal + 0.02 * maxVal` where p = data[i]/maxVal.
- **Left square (standard):** white (`#fff`) bars only, drawn bottom-up on black; caption below in `#2c3e50` 14px: "Standard (bars only)".
- **Right square (band-enriched):** gray (`#808080`, the 0.5 intensity) band rectangles drawn first from lower to upper band bound per bin, then white bars on top; caption below: "Band-enriched (bars + band)".
- **Arrow:** horizontal orange (`#e67e22`) 2px arrow with filled triangular head pointing from the left square to the right square at mid-height.
- **Annotations (13px, orange `#e67e22`) on the right square:** "narrow band (dense)" bottom-left inside, "wide band (sparse)" top-right inside.
- **Caption (italic, `.example`):** Band-enriched input: bars=white, band=gray, bg=black

## 2. Comparison Strategy

- Implement BOTH arithmetic mass-split AND CNN
- Compare calibration on held-out mountain set
- If CNN wins: use it. If tie: prefer arithmetic (simpler)
- If CNN wins only on edge cases: use arithmetic + CNN fallback

**Key Question:** Does the CNN add value over simple mass-split arithmetic? The band-enriched input gives it more info, but the question is whether that info needs nonlinear extraction.

### Visualization (canvas `c2`, 720×300)

Flow diagram of the sub-classification decision process, drawn with rounded boxes and gray arrows.

- **Title (bold 16px, `#1a5276`, top center):** "Decision Flow: Mountain Sub-Classification".
- **Boxes** (rounded rect radius 6, stroke width 2, bold 14px multi-line centered text in the stroke color):
  - Top center (140×35, fill `#e8f4fd`, stroke `#1a5276`): "Mountain Detected" / "(CNN Level 1)".
  - Path A, left of center (130×50, fill `#e8f8f0`, stroke `#27ae60`): "Arithmetic" / "Mass Split" / "L=68% R=32%".
  - Path B, right of center (130×50, fill `#fef5e7`, stroke `#e67e22`): "CNN Level 2" / "band-enriched" / "right_skew=91%".
  - Convergence box (180×40, fill `#fdf2f2`, stroke `#e74c3c`): "Compare Calibration" / "Arithmetic vs CNN on held-out".
  - Three result boxes on the bottom row: (120×30, fill `#e8f8f0`, stroke `#27ae60`) "CNN wins → use it"; (100×30, fill `#e8f4fd`, stroke `#1a5276`) "Tie → arithmetic"; (130×30, fill `#fef5e7`, stroke `#e67e22`) "Edge only → fallback".
- **Arrows:** 1.5px gray (`#7f8c8d`) lines with filled `#555` arrowheads: top box splits down-left to Path A and down-right to Path B; both paths converge down to the comparison box; comparison box fans out to the three result boxes.
- **Caption (italic, `.example`):** Decision flow: arithmetic vs CNN sub-classifier

## Regeneration instructions

- **Layout:** backlog detail page (kusto-style). Structure: `<h1>` with bottom border `2px solid #2980b9`, `.subtitle` paragraph, one `.intro` callout, then one `.lang-section` per numbered section. Each section: `<h2>` ("N. Title", bottom border `2px solid #2980b9`), then a `table.layout` (width 100%, border-collapse) with one `<tr>`: left `td.text-col` (45%) holding bullets (with nested sub-bullets) and optional `.key-point` callout, right `td.viz-col` (55%) holding the canvas plus an italic `.example` caption. No index number in the h1/title.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276`; h2 1.3rem `#1a5276`; subtitle `#666` 0.95rem. `.intro`: background `#f0f4f8`, left border `3px solid #2980b9`, padding 8px 12px, 0.9rem. `.key-point`: background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.example`: italic, `#555`, 0.9rem. `ul` 0.92rem. `code`: background `#e8f0f8`, color `#1a5276`, padding 2px 6px, radius 3px. Canvas: `width: 100%`, border `1px solid #e0e0e0`, radius 4px. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`.
- **Canvas:** intrinsic size 720×300 per chart; a shared `setupCanvas(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates.
- In regenerated HTML, any card/page links use `.html` extensions.
