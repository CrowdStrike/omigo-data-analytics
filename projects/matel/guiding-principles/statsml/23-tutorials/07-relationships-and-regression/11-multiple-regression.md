# Multiple Regression

**Page type:** detail page (tutorial: card-sections, each a two-column layout table — text left 50%, canvas right 50%; one section uses a 3-column 38/31/31 layout)
**HTML title tag:** Multiple Regression

**Subtitle:** One formula shares the credit for an outcome among several inputs at once — each dial read "holding the others fixed"

## Three Dials Set the Rent

**Tags:** `core idea` (blue), `running example` (green)

- **One outcome** — the monthly rent of an apartment, predicted from three facts about it
- **Three inputs** — size in m², distance to the city center in km, and the floor
- **One formula** — rent = 400 + 10×size − 50×distance + 20×floor (illustrative)
- **Each coefficient is a dial** — +$10 per extra m², −$50 per extra km, +$20 per floor
- **Multiple regression** — the method that finds these dials from data on many apartments

*Example:* A 50 m² flat, 4 km out, on floor 2: 400 + 500 − 200 + 40 = $740 predicted rent.

**Key point:** One equation splits the rent into pieces, one piece per input — that is all multiple regression is.

### Visualization (canvas `c1`, 720×300)

Waterfall chart building one prediction step by step.

- **Title (bold 15px, ink `#1a5276`, top center):** "Building one prediction: 400 + 500 − 200 + 40 = $740"
- **Bars (5 waterfall steps, width 88px, evenly spaced):**
  1. label "base", from 0 to 400, blue `#2a78d6`
  2. label "+10 × 50 m²", from 400 to 900, green `#008300`
  3. label "−50 × 4 km", from 900 to 700, orange `#d95926`
  4. label "+20 × floor 2", from 700 to 740, violet `#4a3aa7`
  5. label "predicted rent", from 0 to 740, ink `#1a5276`
- **Value labels (bold 12px above each bar):** "$400", "+500", "-200", "+40", "$740" (first and last show absolute value; middle bars show signed delta)
- **Connectors:** dashed gray (`#6b7280`, dash 4/3) horizontal segments linking each step's end level to the next bar (between steps 1-2, 2-3, 3-4)
- **Axes:** y from $0 to $1000, gridlines (`#e5e9ef`) and muted labels ("$0", "$250", "$500", "$750", "$1000") every $250; padding: left 60, right 25, top 45, bottom 55
- **Caption (bold 13px magenta `#d55181`, bottom center):** "each dial adds or subtracts on top of the others"

## Pricing Three Apartments by Hand

**Tags:** `worked example` (green), `do it by hand` (blue)

- **Apartment A** — 50 m², 4 km, floor 2: 400 + 500 − 200 + 40 = $740
- **Apartment B** — same size, same floor, 3 km: 400 + 500 − 150 + 40 = $790
- **Only distance differs** — so the $50 gap IS the distance coefficient
- **Apartment C** — 80 m², 4 km, floor 5: 400 + 800 − 200 + 100 = $1,100
- **"Holding others fixed"** — change one input, freeze the rest, read the gap

*Example:* Move the same flat 1 km closer and the model adds exactly $50 — that is what −50 means.

**Key point:** A coefficient answers one question: how does the prediction move when this input moves and the others stay put?

This section uses the 3-column layout (text 38%, two canvases 31% each).

### Visualization (canvas `c2a`, 420×340)

Bar chart of three apartments' predicted rents.

- **Title (bold 15px, ink, top center):** "Predicted rent, three apartments"
- **Bars (width 80px):**
  - "A", sublabel "50 m² · 4 km · fl 2", $740, blue `#2a78d6`
  - "B", sublabel "50 m² · 3 km · fl 2", $790, aqua `#199e70`
  - "C", sublabel "80 m² · 4 km · fl 5", $1100, violet `#4a3aa7`
- **Value labels:** bold 13px "$740" / "$790" / "$1100" above bars; name bold 12px below baseline, sublabel muted 12px under it
- **Axes:** y $0 to $1200, gridlines and muted "$" labels every $300; padding: left 55, top 45, bottom 70
- **Caption (bold 12px green `#008300`, bottom center):** "every bar is arithmetic you can redo"

### Visualization (canvas `c2b`, 420×340)

Two-bar comparison showing only distance changed.

- **Title (bold 15px, ink, top center):** "Change one input only: 4 km → 3 km"
- **Bars (width 100px):**
  - "A: 4 km", $740, blue `#2a78d6`, sublabel "same size, same floor"
  - "B: 3 km", $790, aqua `#199e70`, sublabel "same size, same floor"
- **Value labels:** bold 13px "$740" / "$790" above bars
- **Axes:** y $0 to $900, gridlines and muted "$" labels every $300; padding: left 55, top 45, bottom 70
- **Gap bracket:** magenta (`#d55181`, width 2) square bracket to the right of bar B spanning the $740–$790 levels, with rotated bold 12px magenta label "$50 gap" alongside
- **Caption (bold 13px magenta, bottom center):** "$50 gap = the −50/km dial itself"

## Why the Size Dial Changed When Distance Joined

**Tags:** `core idea` (blue), `common mistake` (orange)

- **Size alone** — regress rent on size only and the dial reads +$7 per m², not +$10
- **Hidden pattern** — in this town the bigger apartments also sit farther out
- **Mixed message** — the size-only dial blends the true +10 with a distance penalty
- **Add distance** — the model separates the two, and size rises to its clean +10
- **General rule** — dials shift when a new variable takes over credit they absorbed

*Example:* Same data, two models: size is "worth" $7/m² alone but $10/m² once distance is in.

**Key point:** A coefficient is only defined relative to the other variables in the model — change the list, change the number.

### Visualization (canvas `c3`, 720×300)

Two-panel figure separated by a vertical dashed divider (`#bdc3c7`, dash 4/3, at x=370).

- **Overall title (bold 15px, ink, top center):** "Why +7 became +10 when distance joined the model"
- **Left panel — scatter of size vs distance (plot area x=60, y=55, 280×180):**
  - Points (blue `#2a78d6`, radius 5): sizes `[35, 40, 45, 50, 55, 60, 65, 70, 80, 90]` m² vs distances `[1.5, 1.9, 2.0, 2.5, 2.6, 3.1, 3.2, 3.7, 4.1, 4.8]` km (positively related)
  - x-axis label "size (m²)" (x scale 30–95 mapped over panel width); rotated y-axis label "distance (km)" (y scale 0–5.5 km); L-shaped gray axes `#999`
  - In-panel annotation (bold 12px orange `#d95926`, top): "bigger flats sit farther out"
  - Sub-caption (muted 12px, bottom): "the two inputs are tangled"
- **Right panel — size coefficient under two models (bars, baseline y=235, height scale max 12 over 155px, bar width 90px):**
  - "size only": +$7/m², yellow `#c98500`
  - "size + distance": +$10/m², green `#008300`
  - Value labels bold 13px "+$7/m²" / "+$10/m²" above bars; model labels 12px below baseline
  - Panel title (bold 12px ink): "the size dial, two models"
  - Annotation (bold 12px orange, bottom): "size-only dial absorbed the distance penalty"

## What "Holding the Others Fixed" Does Not Promise

**Tags:** `common mistake` (orange), `rule of thumb` (blue)

- **A comparison, not a plan** — the model compares similar flats in the data
- **Parallel worlds** — at 2 km and at 8 km, rent rises with size at the same +10 slope
- **Constant gap** — the two lines stay $300 apart: 6 km × $50, at every size
- **Not proof of cause** — the dials describe the market's pattern, not what forces rent
- **Impossible combos** — the formula happily prices a 300 m² flat on floor 90

*Example:* Adding a fake floor number to a listing will not add $20 — the dial describes comparisons.

**Key point:** Coefficients describe differences between existing apartments, not the effect of physically changing one.

### Visualization (canvas `c4`, 720×300)

Two parallel rent-vs-size lines at different distances.

- **Title (bold 15px, ink, top center):** "Same +10 slope at every distance — the gap is the distance dial"
- **Axes:** x = size (m²) from 30 to 100, tick labels every 10; y = rent from $200 to $1400, gridlines and muted labels every $400; padding: top 50, bottom 48, left 65, right 160; L-shaped gray axes `#999`; x-axis title "size (m²)" bottom center
- **Lines (width 3):**
  - Green `#008300`: flats at 2 km, rent = 340 + 10×size, drawn from size 30 to 100
  - Violet `#4a3aa7`: flats at 8 km, rent = 40 + 10×size, drawn from size 30 to 100
- **Gap annotation at size 65:** vertical magenta (`#d55181`, width 2) double-arrow between the lines at $990 and $690 (triangle arrowheads at both ends), labeled to its right in bold 13px magenta: "$300 gap = 6 km × $50"
- **Legend (top right, 14×4px color swatches, 12px text):** green swatch "flats at 2 km", violet swatch "flats at 8 km"

## Regeneration instructions

- **Template/layout:** tutorials topic-page skeleton. `<h1>` (no index number) with 2px bottom border `#2980b9`, `.subtitle` paragraph, then four `.card-section` blocks each with an `<h2>` (1.3rem, `#1a5276`, 2px bottom border `#2980b9`) and a `table.layout` (one `<tr>`: `.text-col` 50% / `.viz-col` 50%). Section 2 uses `table.layout.layout3` with `.text-col3` 38% and two `.viz-col3` cells at 31% each holding canvases `c2a`/`c2b`.
- **Left column structure:** `.tags` row of pill spans first (`.tag.blue` rgba(26,82,118,0.12)/#1a5276, `.tag.green` rgba(39,174,96,0.15)/#27ae60, `.tag.red` rgba(231,76,60,0.12)/#e74c3c, `.tag.orange` rgba(230,126,34,0.15)/#e67e22; 0.72rem, 600 weight, 2px 10px padding, radius 10px), then a `<ul>` of one-line bullets each opening with `<b>` (bold terms colored `#1a5276`), an italic `.example` paragraph (`#555`, 0.9rem), and a `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem) beginning with `<strong>Key point:</strong>`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; canvases `width:100%`, 1px solid `#e0e0e0` border, 4px radius; ul 0.92rem.
- **Chart palette (JS object P):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** declare intrinsic `width`/`height` attributes per chart; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), and calls `ctx.scale` so drawing stays in logical coordinates, and fills a white background.
- No nav bar, no back/home links, no cross-page links. In regenerated HTML, any card links use `.html` extensions.
