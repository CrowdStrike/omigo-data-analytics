# Using Band Information to Define Shape

**Page type:** detail page (backlog-style two-column layout table: text left 45%, viz right 55%, one `.lang-section` per section)
**HTML title tag:** Using Band Information to Define Shape — Discussion Backlog

**Subtitle:** The SE band as a rigorous threshold for real structure vs sampling noise

## Callout (intro box)

A "shape" entirely within the uncertainty band isn't a shape — it's noise. The SE band provides a rigorous threshold for distinguishing real structure from sampling variation.

## Section 1: Shape Significance Rules

- **Bimodal:** valley must be BELOW lower band of both peaks
- **Spike:** peak must rise ABOVE upper band of surrounding bins
- **Heavy tail:** tail bins ABOVE upper band of body
- **Bell:** density fits Gaussian template within band everywhere

### Visualization (canvas `c1`, 720×300)

Histogram of a mildly bimodal distribution with a wide SE band that swallows the valley (not significant).

- **Title (bold 16px, top center, red `#e74c3c`):** "NOT Significant — Valley Within Noise Band".
- **Data (procedural, 20 bins):** for x = i/(bins−1), value = `0.5·exp(−((x−0.3)/0.15)²) + 0.45·exp(−((x−0.7)/0.15)²) + 0.1` — mild bimodal with peaks at 0.3 and 0.7 and a shallow valley; normalized to max.
- **SE band (wide, simulating small n=50):** per-bin half-width = `1.96·sqrt(data[i]·(1 − data[i]/max))/sqrt(50)·max·0.8 + 0.06·max`.
- **Margins:** top 30, right 20, bottom 40, left 50.
- **Bars:** fill `rgba(26,82,118,0.35)`, 0.5px `#1a5276` stroke, drawn per bin over the plot area.
- **Band:** upper and lower boundary lines through bin centers in orange `#e67e22`, 2px, dashed 4/3 (lower clamped at the baseline); band region filled `rgba(230,126,34,0.15)`.
- **Annotation:** at the valley bin (index 10), vertical red arrow (`#e74c3c`, 2px) pointing down toward the valley top, with 14px red label above it: "valley inside band".
- **Axis:** horizontal baseline in `#2c3e50`, 1px.
- **Legend (top right):** 12×12 orange `#e67e22` swatch with 14px `#2c3e50` text "SE Band (±1.96σ)".

**Caption (italic, `.example`):** Valley within band — NOT significant (noise)

## Section 2: Implication

This means "confidently bimodal" requires the valley to drop below the noise floor — not just be lower than the peaks. Many apparent bimodals at small n are just noise.

**Key Question:** Does this replace the CNN or become a confidence layer on top? The CNN detects candidate shapes; the band validates them.

### Visualization (canvas `c2`, 720×300)

Histogram of a clearly bimodal distribution with a narrow SE band; the valley drops below the peaks' lower band (significant).

- **Title (bold 16px, top center, green `#27ae60`):** "IS Significant — Valley Below Both Peaks' Lower Band".
- **Data (procedural, 20 bins):** for x = i/(bins−1), value = `0.7·exp(−((x−0.25)/0.1)²) + 0.65·exp(−((x−0.75)/0.1)²) + 0.05` — sharp bimodal with peaks at 0.25 and 0.75 and a deep valley; normalized to max.
- **SE band (narrower, larger n=200):** per-bin half-width = `1.96·sqrt(data[i]·(1 − data[i]/max))/sqrt(200)·max·0.5 + 0.025·max`.
- **Margins:** top 30, right 20, bottom 40, left 50.
- **Bars:** fill `rgba(26,82,118,0.35)`, 0.5px `#1a5276` stroke.
- **Band:** upper and lower boundary lines in green `#27ae60`, 2px, dashed 4/3; band region filled `rgba(39,174,96,0.12)`.
- **Reference line:** horizontal dashed blue line (`#1a5276`, 1.5px, dash 6/4) across the plot at the higher of the two peaks' lower-band levels (peak bins at indices 5 and 15); right-aligned 14px blue label above it: "peaks' lower band".
- **Annotation:** horizontal green arrow (`#27ae60`, 2px) extending right from the valley bin (index 10) at valley height, with 14px green label: "valley BELOW peak bands".
- **Axis:** horizontal baseline in `#2c3e50`, 1px.
- **Legend (top left):** 12×12 green `#27ae60` swatch with 14px `#2c3e50` text "SE Band (±1.96σ)".

**Caption (italic, `.example`):** Valley below band — IS significant (real bimodal)

## Regeneration instructions

- **Layout:** backlog detail page (kusto-style 2-col). h1 (no index number) + `.subtitle` paragraph + `.intro` callout, then one `.lang-section` per section: `<h2>N. Title</h2>` followed by `<table class="layout">` with one `<tr>`: left `<td class="text-col">` (45%) holding bullets/paragraphs and optional `.key-point` div, right `<td class="viz-col">` (55%) holding the canvas plus an italic `.example` caption paragraph.
- **Page CSS:** body `system-ui, -apple-system, sans-serif`, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`; h2 1.3rem `#1a5276` with `border-bottom: 2px solid #2980b9`; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8`, left border `3px solid #2980b9`, padding 8px 12px, 0.9rem; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem; `ul` 0.92rem; canvas `width: 100%`, 1px `#e0e0e0` border, 4px radius; `code` background `#e8f0f8`, `#1a5276`. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, accent blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`.
- **Canvas:** intrinsic 720×300; shared `setupCanvas(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; CSS scales the canvas to 100% of the viz cell.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
