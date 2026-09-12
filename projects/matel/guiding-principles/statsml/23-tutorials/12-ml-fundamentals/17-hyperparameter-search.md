# Hyperparameter Search

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Hyperparameter Search

**Subtitle:** A hyperparameter is a knob you set before training — with a fixed budget of trials, random placement usually beats a neat grid, because only a few knobs really matter

## Nine Batches of Cookies, Two Knobs

**Tags:** `core idea` (blue), `hyperparameter` (green), `search budget` (orange)

- **The recipe** — a baker tunes two settings before baking: oven temperature and bake time
- **Hyperparameter** — a knob chosen before the process runs; the baking itself never adjusts it
- **The budget** — the baker can afford 9 test batches; the question is where to place them
- **Grid search** — pick 3 temperatures × 3 times and bake every combination: exactly 9 batches
- **Random search** — draw 9 (temperature, time) pairs at random from the same two ranges

*Example (italic):* With temperatures 160/180/200°C and times 8/12/16 minutes, grid search bakes all 9 pairs, from (160°C, 8 min) to (200°C, 16 min).

**Key point:** A hyperparameter is a setting fixed before training starts. Search means spending a limited trial budget on the most informative knob combinations.

### Visualization (canvas `c1`, 720×300)

Dual-panel scatter: the same 9-batch budget placed as a 3×3 grid (left) vs 9 fixed random draws (right) on a temperature-time plane, split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "One Budget of 9 Batches: Grid Placement vs Random Placement".
- **Axes (both panels):** x = temperature 150–210°C, y = bake time 6–18 min; 12px `#444` axis labels "temp °C" below and "min" beside; light `#e5e9ef` 1px frame.
- **Left panel (grid):** origin x=60, width 260, baseline y=240, chart height 180; blue `#2a78d6` 6px dots at every combination of temps `[160, 180, 200]` and times `[8, 12, 16]` (9 dots); dashed `#e5e9ef` guide lines through the 3 temp columns and 3 time rows; caption 12px `#444` "grid: 3 temps × 3 times".
- **Right panel (random):** origin x=405, width 260, same scales; green `#008300` 6px dots at fixed pairs `[(162,9.5), (166,11.2), (169,14.8), (175,8.6), (181,12.9), (186,15.7), (191,10.3), (196,13.4), (199,8.9)]`; caption "random: 9 draws (fixed, illustrative)".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Tasting the Results

**Tags:** `worked example` (blue), `grid vs random` (green)

- **Scoring** — every batch gets a taste score out of 10 from the same taster
- **Grid results** — the 9 grid batches score 5.4, 5.6, 5.3, 8.7, 8.9, 8.6, 7.7, 7.9, 7.6
- **Random results** — the 9 random batches score 5.8, 6.5, 7.0, 8.0, 9.0, 9.8, 9.3, 8.5, 8.0
- **Grid best** — 8.9, from the batch baked at 180°C for 12 minutes
- **Random best** — 9.8, from the 186°C batch — right next to the sweet spot near 187°C

*Example (italic):* Same budget of 9 batches, same oven, same taster — random's winner beats grid's winner 9.8 to 8.9.

**Key point:** Grid never bakes anything between 180 and 200°C, so it cannot land near 187°C. Random can, because its 9 batches use 9 different temperatures.

### Visualization (canvas `c2`, 720×300)

Dual-panel bar chart: taste scores of the 9 grid batches (left) vs the 9 random batches (right), best bar highlighted in each, split by a dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Taste Scores from the Same 9-Batch Budget".
- **Left panel (grid):** scores `[5.4, 5.6, 5.3, 8.7, 8.9, 8.6, 7.7, 7.9, 7.6]`; origin x=55, width 280, baseline y=240, chart height 175, y scale 0–10; bars fill `rgba(42,120,214,0.45)`; the 5th bar (8.9) gets a 2px `#1a5276` outline and bold 12px ink label "best 8.9" above; group labels 11px `#444` under bar triplets: "160°C", "180°C", "200°C"; caption 12px `#444` "grid: 3 batches at each of 3 temps".
- **Right panel (random):** scores `[5.8, 6.5, 7.0, 8.0, 9.0, 9.8, 9.3, 8.5, 8.0]` for temps 162→199 in order; origin x=400, width 280, same baseline/height/scale; bars fill `rgba(0,131,0,0.4)`; the 6th bar (9.8) gets a 2px `#008300` outline and bold 12px green label "best 9.8" above; 11px temp labels `[162, 166, 169, 175, 181, 186, 191, 196, 199]` under the bars; dashed magenta `#d55181` horizontal line at score 8.9 across the panel labeled bold 12px "grid's best"; caption "random: 9 different temps".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Why Random Usually Wins

**Tags:** `why random wins` (blue), `low effective dimension` (orange), `rule of thumb` (green)

- **The secret** — taste turns out to depend almost entirely on temperature; time barely matters
- **Wasted repeats** — grid's 9 batches use only 3 distinct temperatures, each re-tested 3 times
- **Full coverage** — random's 9 batches use 9 distinct temperatures spread across the range
- **The projection** — squash every batch onto the temperature line: grid leaves 3 marks, random 9
- **General rule** — with many knobs, usually only a few matter, and nobody knows which in advance

*Example (italic):* Projected onto the temperature line, only one of grid's 3 marks is anywhere near 187°C, while random's 186°C mark lands inside the sweet spot.

**Key point:** Grid spends most of its budget re-testing values of the knobs that do not matter. Random never repeats a value on any knob, so the knobs that do matter get full coverage.

### Visualization (canvas `c3`, 720×300)

Dual-panel projection diagram: each panel shows the 9 batch points with dashed drop lines down to a temperature axis, where grid collapses to 3 tick marks and random spreads to 9, split by a dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Only Temperature Matters: Project Every Batch onto the Temperature Line".
- **Layout (both panels):** scatter area from y=55 to y=175 (time dimension, unlabeled); horizontal temperature line at y=210, 2px `#999`, mapping 150–210°C; end labels "150" and "210" 12px `#444`; yellow band `rgba(201,133,0,0.18)` from 184 to 190°C spanning y=55 to y=218, labeled bold 12px `#c98500` "sweet spot ~187°C" above the band.
- **Left panel (grid):** line from x=60, width 260; blue `#2a78d6` 5px dots at the 9 grid points (temps `[160, 180, 200]` × times `[8, 12, 16]`, time mapped into y=55–175); dashed `#bdc3c7` drop lines to the axis; 3 bold blue tick marks (3px, 12px tall) at 160, 180, 200; magenta `#d55181` bold 13px annotation "9 batches → only 3 marks"; note the band contains no blue mark.
- **Right panel (random):** line from x=405, width 260; green `#008300` 5px dots at the 9 random pairs from c1; dashed drop lines; 9 bold green tick marks at `[162, 166, 169, 175, 181, 186, 191, 196, 199]`; green bold 13px annotation "9 batches → 9 marks"; the 186 mark falls inside the yellow band.
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## The Exponential Wall

**Tags:** `common mistake` (red), `where it's used` (blue), `scaling` (orange)

- **Real models** — a boosted-tree model easily has 5+ knobs: depth, tree count, learning rate, more
- **Grid explodes** — 3 values per knob means 3^k trials: 3, 9, 27, 81, 243 as knobs go 1 to 5
- **Random scales** — random search runs at whatever budget you set: 20 trials for 2 knobs or 10
- **Anytime** — you can stop random search early and keep the best so far; a half grid has holes
- **Log ranges** — knobs like learning rate are drawn log-uniformly, e.g. between 10^-4 and 10^-1

*Example (italic):* Adding a sixth knob multiplies a 3-per-knob grid from 243 to 729 trials — random search stays at whatever 20 trials you budgeted.

**Common mistake:** Making grid search affordable by using 2-3 coarse values per knob. A coarse grid straddles the sweet spot exactly where precision matters — like never baking between 180 and 200°C.

### Visualization (canvas `c4`, 720×300)

Bar chart of grid-search cost (3 values per knob) as the number of knobs grows from 1 to 5, with a flat dashed line showing a fixed random-search budget of 20 trials.

- **Title (bold 15px, `#1a5276`, top center):** "Cost of a 3-Value-per-Knob Grid as Knobs Are Added".
- **Data:** knob counts `[1, 2, 3, 4, 5]`, grid trials `[3, 9, 27, 81, 243]`.
- **Bars:** origin x=70, width 560, baseline y=240, chart height 180, y scale 0–260; fill `rgba(217,89,38,0.5)` with 1px `#d95926` stroke; bold 12px `#d95926` value labels "3", "9", "27", "81", "243" above each bar; x labels 12px `#444` "1 knob", "2 knobs", "3 knobs", "4 knobs", "5 knobs".
- **Random budget line:** green `#008300` dashed (dash 6/4) horizontal line at the 20-trial level across the plot; bold 13px green label above its left end "random search: fixed budget of 20 trials".
- **Annotation:** magenta `#d55181` bold 13px, two lines near the 243 bar: "each new knob" / "multiplies grid cost ×3".
- **Caption (12px `#444`, bottom left):** "grid = 3^k trials; random = any budget you choose".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- All "random" search points are fixed illustrative arrays hardcoded above — no `Math.random()` anywhere.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
