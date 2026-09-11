# Reading the Slope

**Page type:** detail page (tutorial page: `.card-section` blocks, each h2 + two-column `table.layout` — text left with tag pills / bullets / example / key-point, canvas right)
**HTML title tag:** Reading the Slope

**Subtitle:** "+$18 rent per m²" claims an association between listings — not that adding a square meter to your flat raises its rent by $18

## What "+$18 per m²" actually says

**Tags:** `core idea` (blue), `worked example` (green)

- **The claim** — listings that differ by 1 m² differ, on average, by about $18 in rent
- **Comparing, not changing** — the slope compares different flats; nobody's wall moved
- **Per unit** — "per m²" is part of the number; 18 alone means nothing
- **Scaled up** — a 10 m² difference goes with about a $180 difference, same fact
- **Along the line** — from 50 m² ($1,300 expected) to 60 m² ($1,480 expected)

*Example:* Comparing the 45 m² and 55 m² listings: the line expects $1,210 vs $1,390 — a $180 gap for 10 m².

**Key point:** A slope is a rate of difference between observed cases — read it as "per extra unit, we see about..." not "adding a unit will cause..."

### Visualization (canvas `c1`, 720×300)

Two side-by-side panels (split by a dashed `#bdc3c7` vertical divider at x=368) showing the slope at two zoom levels.

- **Overall title (bold 15px, `#1a5276`, top center):** "The slope read at two zoom levels".
- **Left panel — whole range:** x 25–85 (ticks 30, 50, 70; label "size (m²)"), y 800–2000 (labels $1,000 and $1,800); axis `#999`, labels 12px `#6b7280`. Fitted line 400 + 18 × size in green `#008300`, width 3, from x=27 to x=83. Orange `#d95926` slope triangle from (50, 1300) to (60, 1300) to (60, 1480), width 2, with bold 12px orange labels "+10 m²" below and "+$180" right. Bold 12px ink label "whole range: 30–80 m²" near (28, 1900).
- **Right panel — zoom:** x from 49.5 to 56.5 (ticks 50, 52, 54, 56; label "size (m²)"), y 1280–1430. Fitted line in green width 3 from x=49.7 to x=56.3. Orange stair steps (width 2) for each 1 m² increment from 50 to 56, each step +1 m² across then +$18 up. Bold 12px orange label "each step: +1 m² → +$18" near (50.2, 1408); bold 12px ink label "zoomed: 50–56 m²" near (52.6, 1310).

## Association, not a lever you can pull

**Tags:** `common mistake` (red), `where it's used` (orange)

- **The trap** — reading the slope as a recipe: "extend my flat 20 m², collect $360 more"
- **Bundled causes** — bigger flats also tend to sit in newer buildings and nicer areas
- **One number, many drivers** — the $18 is size plus everything that travels with size
- **Same size, different rent** — two 60 m² flats can differ by hundreds across districts
- **To claim cause** — you need an experiment or careful controls, not a fitted line

*Example:* Illustrative: a 60 m² flat downtown at $1,700 vs the same size in an outer district at $1,250 — size identical, rents $450 apart.

**Common confusion:** The slope measures how rent differs with size across listings — it does not promise what happens if you change a flat's size.

### Visualization (canvas `c2`, 720×300)

Two side-by-side panels (dashed `#bdc3c7` divider at x=390): a confounder arrow diagram and a two-bar same-size comparison.

- **Overall title (bold 15px, `#1a5276`):** "The $18 rides along with everything size travels with".
- **Left panel — arrows diagram:** three boxes (fill `#f8f9fa`, 2px colored border, bold 12px colored labels): "size (m²)" (blue `#2a78d6`, at 40,170, 110×42), "rent" (green `#008300`, at 240,170, 110×42), and a two-line box "neighborhood," / "building age, floor" (violet `#4a3aa7`, at 115,58, 170×48). Solid blue arrow from size to rent; dashed violet arrows from the confounder box down to both size and rent (arrowheads filled triangles). Labels: bold 12px blue "measured: +$18/m²" below the size→rent arrow; 12px violet "unmeasured, moves both" above the confounder box; bold 12px magenta `#d55181` "the slope bundles all of these paths" at the bottom.
- **Right panel — bars:** two bars (width 92px, baseline y=235, scale max $2,000 over 150px): "60 m² downtown" $1,700 in aqua `#199e70` and "60 m² outer district" $1,250 in yellow `#c98500`; bold 12px `#2c3e50` dollar labels above bars, 12px `#6b7280` labels below. Headline bold 13px magenta "same size, $450 apart"; 11px `#6b7280` caption "illustrative rents".

## Rescale the units and the number changes

**Tags:** `rule of thumb` (blue), `worked example` (green)

- **Same line, three numbers** — $18 per m² = $1.67 per ft² = $180 per 10 m²
- **Why** — 1 m² is 10.76 ft², so the per-ft² slope is 18 ÷ 10.76 ≈ 1.67
- **Big ≠ strong** — a slope of 180 is not a "stronger" effect than 1.67; only the units moved
- **Reading papers** — never judge a coefficient's importance by its size before checking units
- **Say the units** — a slope quoted without units is a number without a meaning

*Example:* Measure rent in cents and the slope becomes 1,800 per m² — a hundred times "bigger", identical fact.

**Key point:** Changing y's units multiplies the slope by that factor; changing x's units divides it — the relationship in the world hasn't changed at all.

### Visualization (canvas `c3`, 720×300)

Three mini-panels with geometrically identical lines but different slope numbers.

- **Title (bold 15px, `#1a5276`):** "One relationship, three slope numbers — only the units changed".
- **Panels (each 196px wide, plot area y 52–232, identical line geometry corner-to-corner, colored line width 3):**
  1. at x=30, blue `#2a78d6`: "slope = $18" / "per m²" / x-label "size (m²): 30 … 80"
  2. at x=262, aqua `#199e70`: "slope = $1.67" / "per ft²" / x-label "size (ft²): 323 … 861"
  3. at x=494, violet `#4a3aa7`: "slope = $180" / "per 10 m²" / x-label "size (10 m²): 3 … 8"
  Slope value bold 15px in panel color, unit bold 12px, x-label 12px `#6b7280`; axes `#999`.
- **Annotation (bold 13px magenta `#d55181`, centered above panels):** "the drawn line never moves — never judge strength by the number alone".

## Regeneration instructions

- **Template/layout:** tutorial topic page (see `tutorials/CLAUDE.md`; skeleton copied from `most-powerful-signals/07-social-graph-connections.html`). h1 + `.subtitle`, then three `.card-section` blocks each with an `<h2>` (bottom border `2px solid #2980b9`) and a `table.layout` with two columns: `.text-col` 50% / `.viz-col` 50%. All three canvases are 720×300; c1 and c2 draw two sub-panels within one canvas separated by a dashed `#bdc3c7` divider, c3 draws three sub-panels.
- **Left column structure per section:** `.tags` row of colored pill spans (`.tag.blue` bg rgba(26,82,118,0.12) text `#1a5276`; `.tag.green` bg rgba(39,174,96,0.15) text `#27ae60`; `.tag.red` bg rgba(231,76,60,0.12) text `#e74c3c`; `.tag.orange` bg rgba(230,126,34,0.15) text `#e67e22`; 0.72rem, weight 600, radius 10px), then a `<ul>` of one-line bullets each opening with `<b>` in `#1a5276`, an italic `.example` paragraph (`#555`, 0.9rem), and a `.key-point` callout (bg `#f8f9fa`, left border `3px solid #e74c3c`, 0.9rem). The second section's callout opens with "Common confusion:" instead of "Key point:".
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with bottom border `2px solid #2980b9`; `.subtitle` `#666` 0.95rem; table cells padding 12px, no borders; canvases `width:100%` with border `1px solid #e0e0e0`, radius 4px. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange (used in tag pills and key-point border).
- In regenerated HTML, any card links use `.html` extensions (this page has no links).
