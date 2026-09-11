# Fitting a Line

**Page type:** detail page (tutorial page: `.card-section` blocks, each h2 + two-column `table.layout` — text left with tag pills / bullets / example / key-point, canvas right; one section uses a 3-column 38/31/31 layout)
**HTML title tag:** Fitting a Line

**Subtitle:** Fifteen apartment listings, one straight line through the scatter — the line that misses the points by the least, read out in plain words

## Fifteen apartments, one trend

**Tags:** `core idea` (blue), `worked example` (green)

- **The data** — 15 rental listings: size in m² and monthly rent in dollars
- **The pattern** — bigger flats rent for more, but the points don't sit on a neat curve
- **The line** — one straight line summarizes the whole cloud: rent ≈ 400 + 18 × size
- **The scatter** — every point misses the line a little; that is normal, not a failure
- **The payoff** — 15 messy numbers collapse into two: a slope and a starting level

*Example:* The 52 m² flat rents for $1,586 while the bigger 55 m² one is only $1,360 — the trend is real, the points are noisy.

**Key point:** A fitted line is a summary of a cloud of points — not a claim that every apartment obeys it.

### Visualization (canvas `c1`, 720×300)

Scatter plot with fitted line over 15 apartment listings.

- **Title (bold 15px, `#1a5276`, top center):** "15 listings: apartment size vs monthly rent".
- **Data:** sizes `[30,34,38,41,45,48,52,55,58,61,65,68,72,76,80]` (m²) vs rents `[1140,842,994,1258,1190,1124,1586,1360,1194,1328,1750,1694,1946,1528,1880]` ($).
- **Axes:** x from 25 to 85 (ticks 30, 40, 50, 60, 70, 80), y from 700 to 2100 (horizontal gridlines `#e5e9ef` with labels $800, $1200, $1600, $2000); padding top 42 / bottom 46 / left 62 / right 22; axis lines `#999`; tick labels 12px `#6b7280`; x-axis label "size (m²)" centered at bottom.
- **Fitted line:** rent = 400 + 18 × size drawn from x=27 to x=83 in green `#008300`, width 3.
- **Points:** blue `#2a78d6` filled circles, radius 4.5.
- **Annotation:** bold 13px green `#008300` text "rent ≈ $400 + $18 × size" at approximately (x=30, y=$1,880).

## Why this line and not another

**Tags:** `worked example` (green), `rule of thumb` (blue)

- **Miss** — actual rent minus the line's guess: the 52 m² flat misses by 1,586 − 1,336 = +$250
- **Square it** — 250² = 62,500, so misses above and below both count against a line
- **Add them up** — the line 400 + 18×size totals 425,200 over all 15 squared misses
- **Try rivals** — 300 + 20×size totals 440,132; 200 + 22×size totals 484,928 — both worse
- **Worst case** — a flat line at the mean rent ($1,388) totals 1,519,586

*Example:* Check one by hand: the 45 m² flat — line says 400 + 18 × 45 = $1,210, actual is $1,190, miss = −$20, squared = 400.

**Key point:** "Least squares" means exactly this — of all straight lines, the fitted one has the smallest total of squared misses.

### Visualization (canvas `c2a`, 420×300)

Scatter with three candidate lines over the same 15 points.

- **Title (bold 15px, `#1a5276`):** "Three candidate lines".
- **Axes:** x 25–85 (ticks 30, 50, 70; label "size (m²)"), y 600–2200; padding top 42 / bottom 44 / left 52 / right 14; axis `#999`, tick labels 12px `#6b7280`.
- **Lines (drawn from x=27 to x=83):** orange `#d95926` dashed (6/4, width 2) for 300 + 20×s; magenta `#d55181` dashed (6/4, width 2) for 200 + 22×s; green `#008300` solid width 3 for 400 + 18×s (the fitted line).
- **Points:** blue `#2a78d6` circles, radius 3.5 (same sizes/rents data as c1).
- **Labels (bold 12px):** green "400 + 18×s (fitted)" near (28, 2060); orange "300 + 20×s" near (63, 1500); magenta "200 + 22×s" near (56, 870).

### Visualization (canvas `c2b`, 400×300)

Bar chart of total squared misses per candidate line.

- **Title (bold 15px, `#1a5276`):** "Total of squared misses".
- **Bars (4, width 56px):** labels `['400+18×s', '300+20×s', '200+22×s', 'flat $1,388']`, values `[425200, 440132, 484928, 1519586]`, displayed value labels `['425k', '440k', '485k', '1,520k']` (bold 12px `#2c3e50` above each bar), colors `[#008300, #d95926, #d55181, #6b7280]`; the first (fitted) bar full opacity, the other three at 0.55 alpha; y scale max 1,600,000; padding top 42 / bottom 60 / left 56 / right 14; axis lines `#999`; bar labels 12px `#6b7280` under each bar.
- **Caption (bold 13px green `#008300`, bottom center):** "smallest total wins — that is the fit".

## Slope and intercept in plain words

**Tags:** `core idea` (blue), `where it's used` (orange)

- **Slope 18** — each extra m² goes with about $18 more rent per month
- **Ten more m²** — the same fact scaled up: roughly $180 more per month
- **Intercept 400** — where the line starts at size 0; there is no 0 m² flat, so don't over-read it
- **Prediction** — a 60 m² flat: 400 + 18 × 60 = $1,480 expected rent
- **Everyday use** — pricing a new listing, spotting a bargain, sanity-checking an asking rent

*Example:* A listing at 65 m² asks $1,750; the line expects $1,570 — it's about $180 above the trend, worth asking why.

**Key point:** Slope = price per extra square meter; intercept = the line's starting level, often not meaningful on its own.

### Visualization (canvas `c3`, 720×300)

Fitted line extended to the y-axis showing intercept and a slope triangle.

- **Title (bold 15px, `#1a5276`):** "Reading the line: slope $18/m², intercept $400".
- **Axes:** x from 0 to 85 (ticks 0, 20, 40, 60, 80; label "size (m²)"), y from 0 to 2100 (labels $400, $800, $1200, $1600, $2000); padding top 42 / bottom 46 / left 62 / right 22; axis `#999`, tick labels 12px `#6b7280`.
- **Line:** green `#008300`; dashed segment (6/4, width 2) from (0, 400) to (30, 940) — the extrapolated part; solid width 3 from (30, 940) to (80, 1840).
- **Points:** the 15 listings in faint blue `rgba(42,120,214,0.45)`, radius 3.5.
- **Intercept marker:** violet `#4a3aa7` filled circle radius 6 at (0, 400) with bold 12px label "intercept $400 — the starting level, not a real flat" just above-right.
- **Slope triangle:** orange `#d95926` right-angle path from (50, 1300) to (60, 1300) to (60, 1480), width 2; labels bold 12px orange "+10 m²" below the horizontal leg and "+$180" right of the vertical leg.
- **Annotation:** bold 13px orange "each extra m² ≈ +$18/mo" near (18, 1780).

## Regeneration instructions

- **Template/layout:** tutorial topic page (see `tutorials/CLAUDE.md`; skeleton copied from `most-powerful-signals/07-social-graph-connections.html`). h1 + `.subtitle`, then three `.card-section` blocks each with an `<h2>` (bottom border `2px solid #2980b9`) and a `table.layout`. Sections 1 and 3 use two columns: `.text-col` 50% / `.viz-col` 50%. Section 2 uses three columns: `.text-col3` 38% / two `.viz-col3` at 31% each (canvases c2a 420×300 and c2b 400×300, cells centered).
- **Left column structure per section:** `.tags` row of colored pill spans (`.tag.blue` bg rgba(26,82,118,0.12) text `#1a5276`; `.tag.green` bg rgba(39,174,96,0.15) text `#27ae60`; `.tag.red` bg rgba(231,76,60,0.12) text `#e74c3c`; `.tag.orange` bg rgba(230,126,34,0.15) text `#e67e22`; 0.72rem, weight 600, radius 10px), then a `<ul>` of one-line bullets each opening with `<b>` in `#1a5276`, an italic `.example` paragraph (`#555`, 0.9rem), and a `.key-point` callout (bg `#f8f9fa`, left border `3px solid #e74c3c`, 0.9rem).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with bottom border `2px solid #2980b9`; `.subtitle` `#666` 0.95rem; table cells padding 12px, no borders; canvases `width:100%` with border `1px solid #e0e0e0`, radius 4px. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange (used in tag pills and key-point border).
- In regenerated HTML, any card links use `.html` extensions (this page has no links).
