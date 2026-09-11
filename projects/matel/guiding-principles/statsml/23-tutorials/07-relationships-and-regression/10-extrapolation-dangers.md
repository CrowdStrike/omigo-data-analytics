# Extrapolation Dangers

**Page type:** detail page (tutorial page: `.card-section` blocks, each h2 + two-column `table.layout` — text left with tag pills / bullets / example / key-point, canvas right)
**HTML title tag:** Extrapolation Dangers

**Subtitle:** The rent line was fitted on 30–80 m² flats — ask it about a 300 m² penthouse and it answers confidently about a world it has never seen

## Asking the line about a penthouse

**Tags:** `core idea` (blue), `worked example` (green)

- **The fit** — rent ≈ 400 + 18 × size, learned from 15 flats between 30 and 80 m²
- **The question** — what does a 300 m² penthouse rent for?
- **The line answers** — 400 + 18 × 300 = $5,800; it will always give a number
- **The gap** — 300 m² sits 220 m² beyond the biggest flat the line ever saw
- **Reality check** — luxury units price by a different logic; $14,000 is plausible (illustrative)

*Example:* The line treats a penthouse as "a 60 m² flat, five times over" — but penthouse buyers, supply, and finishes are a different market.

**Key point:** A model never refuses to answer — outside the data it saw, the confident number is a guess wearing a suit.

### Visualization (canvas `c1`, 720×300)

Wide-range scatter showing the fitted range vs the 300 m² question.

- **Title (bold 15px, `#1a5276`, top center):** "Fitted on 30–80 m² — asked about 300 m²".
- **Data:** sizes `[30,34,38,41,45,48,52,55,58,61,65,68,72,76,80]` vs rents `[1140,842,994,1258,1190,1124,1586,1360,1194,1328,1750,1694,1946,1528,1880]`, plotted as a small cluster at the left.
- **Axes:** x from 0 to 320 (ticks 0, 50, 100, 150, 200, 250, 300; label "size (m²)"), y from 0 to 15,000 (labels $5k, $10k, $15k); padding top 42 / bottom 46 / left 66 / right 26; axis `#999`, labels 12px `#6b7280`.
- **Data-range band:** vertical shaded region from x=30 to x=80 in `rgba(0,131,0,0.08)`.
- **Line:** green `#008300`; solid width 3 from (30, 940) to (80, 1840) inside the data; dashed (7/5, width 2) continuation from (80, 1840) out to x=305.
- **Points:** blue `#2a78d6` circles, radius 3.
- **Markers at x=300:** green filled circle radius 6 at $5,800 (the line's answer); magenta `#d55181` filled circle radius 6 at $14,000 (plausible market point); vertical magenta gap arrow (width 2, filled triangular head) connecting them.
- **Annotations (bold 12px, right-aligned):** green "the line says $5,800"; magenta "luxury market ~$14,000 (illustrative)"; bold 13px magenta "off by ~$8,000" mid-gap. Bold 12px green, left-aligned near the cluster: "all 15 flats live here".

## Why the line breaks out there

**Tags:** `core idea` (blue), `common mistake` (red)

- **Local truth** — "straight with slope 18" was only ever checked between 30 and 80 m²
- **Many futures** — a straight line, an upward curve, and a plateau all fit the 15 points equally well
- **They disagree wildly** — at 300 m² they say $5,800, $14,500, and $3,600 (illustrative)
- **No penalty** — least squares only counts misses at the data; nonsense beyond it costs nothing
- **Distance hurts** — the further from the data, the faster candidate models drift apart

*Example:* Inside 30–80 m² the three candidate curves are visually identical — the data cannot tell them apart, so it cannot pick your extrapolation.

**Key point:** Fitting well where you have data says nothing about the shape where you don't — the data can't referee a region it never visited.

### Visualization (canvas `c2`, 720×300)

Three candidate models agreeing inside the data band and diverging outside it.

- **Title (bold 15px, `#1a5276`):** "Three models that fit the 15 flats equally well".
- **Axes:** x from 0 to 320 (ticks 0, 50, 100, 150, 200, 250, 300; label "size (m²)"), y from 0 to 16,000 (labels $5k, $10k, $15k); padding top 42 / bottom 46 / left 66 / right 26.
- **Data-range band:** x=30 to x=80 shaded `rgba(0,131,0,0.08)`.
- **Curves (each width 2.5, drawn x=30 to 305 in 5-unit steps):**
  - straight line: 400 + 18x, green `#008300`
  - curve up: same line for x ≤ 80, then + 0.18 × (x−80)² beyond, violet `#4a3aa7`
  - plateau: same line for x ≤ 80, then 1840 + 8 × (x−80) beyond, yellow `#c98500`
- **Points:** the 15 listings, blue `#2a78d6`, radius 3.
- **Labels at right (bold 12px, right-aligned):** violet "curve up → $14,500"; green "straight → $5,800"; yellow "plateau → $3,600".
- **Annotations (bold 13px):** ink `#1a5276`, left-aligned "inside the band: indistinguishable"; magenta `#d55181` "out here the data can't pick — answers span 4x (illustrative)".

## Where a data scientist gets burned

**Tags:** `where it's used` (orange), `rule of thumb` (blue)

- **Forecasts** — projecting next year from three months of history is time extrapolation
- **Pricing** — scoring a deal 4x larger than anything in the training data
- **ML too** — every model, however fancy, extrapolates when inputs leave the training range
- **The guard** — record the training range; flag any prediction request outside it
- **Interpolation is fine** — a 63 m² flat sits inside 30–80, and the line handles it well

*Example:* A 55 m² query: trust it. A 90 m² query: caution, just past the edge. A 300 m² query: the model has nothing to say.

**Key point:** Models are only valid where they saw data — ship the training range with the model and check every input against it.

### Visualization (canvas `c3`, 720×300)

Horizontal number-line diagram: check the query against the training range.

- **Title (bold 15px, `#1a5276`):** "Check the range before you predict".
- **Axis:** single horizontal line at y=190 spanning x 0–320 (ticks 0, 50, 100, 150, 200, 250, 300; label "requested size (m²)"); line `#999` width 1.5, labels 12px `#6b7280`.
- **Training band:** rectangle over x=30–80 (68px tall, centered on the axis) filled `rgba(0,131,0,0.14)` with green `#008300` 2px border; bold 12px green labels "training range" above and "30–80 m²" below.
- **Caution zone:** rectangle over x=80–110 filled `rgba(201,133,0,0.12)`.
- **Query markers (filled circle radius 7 on the axis, dashed leader line to a two-line label — bold 13px mark+title, 12px sub-label):**
  - x=55, green `#008300`, above: "✓ 55 m²: in range" / "interpolation — trust it"
  - x=90, yellow `#c98500`, below: "! 90 m²: past the edge" / "caution"
  - x=300, red `#e74c3c`, above: "✗ 300 m²: far outside" / "extrapolation — don't trust"
- **Caption (bold 13px red `#e74c3c`, bottom center):** "a model is a summary of the data it saw — nothing more".

## Regeneration instructions

- **Template/layout:** tutorial topic page (see `tutorials/CLAUDE.md`; skeleton copied from `most-powerful-signals/07-social-graph-connections.html`). h1 + `.subtitle`, then three `.card-section` blocks each with an `<h2>` (bottom border `2px solid #2980b9`) and a `table.layout` with two columns: `.text-col` 50% / `.viz-col` 50%. All three canvases are 720×300.
- **Left column structure per section:** `.tags` row of colored pill spans (`.tag.blue` bg rgba(26,82,118,0.12) text `#1a5276`; `.tag.green` bg rgba(39,174,96,0.15) text `#27ae60`; `.tag.red` bg rgba(231,76,60,0.12) text `#e74c3c`; `.tag.orange` bg rgba(230,126,34,0.15) text `#e67e22`; 0.72rem, weight 600, radius 10px), then a `<ul>` of one-line bullets each opening with `<b>` in `#1a5276`, an italic `.example` paragraph (`#555`, 0.9rem), and a `.key-point` callout (bg `#f8f9fa`, left border `3px solid #e74c3c`, 0.9rem).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with bottom border `2px solid #2980b9`; `.subtitle` `#666` 0.95rem; table cells padding 12px, no borders; canvases `width:100%` with border `1px solid #e0e0e0`, radius 4px. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange (used in tag pills, key-point border, and the c3 "don't trust" query marker).
- In regenerated HTML, any card links use `.html` extensions (this page has no links).
