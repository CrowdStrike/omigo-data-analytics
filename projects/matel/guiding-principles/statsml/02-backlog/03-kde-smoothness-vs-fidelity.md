# KDE Smoothness vs Fidelity

**Page type:** detail page (backlog kusto-style: intro callout, numbered h2 sections, each a 2-col table with text left 45% / canvas right 55%)
**HTML title tag:** KDE Smoothness vs Fidelity — Discussion Backlog

**Subtitle:** Smooth enough for humans to read, faithful enough to not lie about the data

**Intro callout:** Fixed-bandwidth KDE bleeds spikes into neighbors, distorting structure. The tension: smooth enough for humans to read, faithful enough to not lie about the data.

## 1. Candidate Approaches

- **A. Log-space smoothing** — compress spikes before smoothing, decompress after
- **B. Adaptive bandwidth** — Abramson's `h(x) ~ 1/sqrt(f(x))` — narrow at peaks, wide in tails
- **C. Diffusion-based KDE** — iterative convolution that respects boundaries
- **D. Spike + Continuous mixture** — model spikes as point masses + smooth remainder
- **E. Drop the curve** — just bars + SE band (what the CNN actually sees)

### Visualization (canvas `c1`, 720×300)

Histogram with a spike, overlaid with two KDE curves (fixed vs adaptive bandwidth).

- **Title (bold 16px, `#1a5276`, top center):** "Spike Distortion: Fixed KDE vs Adaptive KDE".
- **Margins:** top 30, right 20, bottom 40, left 50.
- **Histogram data:** 24 bins; value at bin i (x = i/23): `0.4 * exp(-((x-0.5)/0.2)^2) + 0.05`, except bin 8 which is a spike at 0.95. Scale max 1.0.
- **Bars:** fill `rgba(26,82,118,0.25)`, stroke `#1a5276` width 0.5, bar width = plotWidth/24 − 2.
- **Fixed KDE curve (red `#e74c3c`, width 2.5):** Gaussian-kernel sum over the 24 bins with wide fixed bandwidth 3.0/bins, evaluated at 100 points, normalized by `fixedBW/bins * sqrt(2π) * bins * 0.18`, clipped at 1.0 — visibly bleeds the spike into neighbors.
- **Adaptive KDE curve (green `#27ae60`, width 2.5):** same kernel sum but local bandwidth `(3.0/bins) / sqrt(max(data[i],0.1)*3)` (inversely proportional to sqrt of density), normalized by `bins * 1.8`, clipped at 1.0 — narrow at the spike, wide elsewhere.
- **Annotations:** red text "bleed into neighbors" at spike x + 30, y = top + 25, with a thin red pointer line down toward the bleed area; bold blue (`#1a5276`) label "SPIKE" centered under bin 8 on the x-axis.
- **X-axis:** single horizontal baseline in `#2c3e50`, width 1.
- **Legend (top right):** short 2.5px line segments with labels in `#2c3e50` 14px: red segment "Fixed BW KDE", green segment "Adaptive BW KDE".
- **Caption (italic, `.example`):** Fixed KDE (red) bleeds spike vs adaptive KDE (green) preserves it

## 2. The Real Question

The CNN sees raw 32-bin histograms. The KDE curve is purely for human interpretation. If it misleads humans about what the CNN sees, it's harmful.

**Key Question:** Is the curve for humans or the CNN? If just humans, option E (bars + band) might be the most honest representation.

### Visualization (canvas `c2`, 720×300)

Bars-only histogram with a dashed SE (standard error) band — the honest "option E" representation.

- **Title (bold 16px, `#1a5276`, top center):** "Option E: Bars + SE Band (What CNN Actually Sees)".
- **Margins:** top 30, right 20, bottom 40, left 50.
- **Data:** identical to canvas c1 (24 bins, Gaussian body + spike 0.95 at bin 8, max 1.0).
- **Bars:** fill `rgba(26,82,118,0.35)`, stroke `#1a5276` width 0.5.
- **SE band:** per-bin half-width `1.96 * sqrt(data[i]*(1-data[i])) / sqrt(150) * 0.8 + 0.02`; upper and lower boundary polylines through bin centers in `#e67e22`, width 1.5, dashed 4/3, clipped to [0, 1]; band interior filled `rgba(230,126,34,0.12)`.
- **Annotations:** green (`#27ae60`) centered text under the axis: "No distortion — spike is spike, body is body"; orange (`#e67e22`) right-aligned text near top right: "SE Band shows uncertainty".
- **X-axis:** horizontal baseline in `#2c3e50`, width 1.
- **Caption (italic, `.example`):** Bars-only with SE band — what the CNN actually sees

## Regeneration instructions

- **Layout:** backlog detail page (kusto-style). Structure: `<h1>` with bottom border `2px solid #2980b9`, `.subtitle` paragraph, one `.intro` callout, then one `.lang-section` per numbered section. Each section: `<h2>` ("N. Title", bottom border `2px solid #2980b9`), then a `table.layout` (width 100%, border-collapse) with one `<tr>`: left `td.text-col` (45%) holding bullets/paragraphs and optional `.key-point` callout, right `td.viz-col` (55%) holding the canvas plus an italic `.example` caption. No index number in the h1/title.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276`; h2 1.3rem `#1a5276`; subtitle `#666` 0.95rem. `.intro`: background `#f0f4f8`, left border `3px solid #2980b9`, padding 8px 12px, 0.9rem. `.key-point`: background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.example`: italic, `#555`, 0.9rem. `ul` 0.92rem. `code`: background `#e8f0f8`, color `#1a5276`, padding 2px 6px, radius 3px. Canvas: `width: 100%`, border `1px solid #e0e0e0`, radius 4px. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`.
- **Canvas:** intrinsic size 720×300 per chart; a shared `setupCanvas(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates.
- In regenerated HTML, any card/page links use `.html` extensions.
