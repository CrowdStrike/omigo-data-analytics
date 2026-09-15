# Running a City

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Running a City

**Subtitle:** Fire stations, traffic lights, and bus routes — city planning as forecasts turned into placements, timings, and lines on a map

## Where the Fire Stations Go

**Tags:** `in practice` (blue), `covering` (green)

- **The question** — place the fewest stations so every block gets an engine within 5 minutes
- **The map** — a 4×4 grid of 16 blocks; a 5-minute drive reaches about 3 blocks along the streets
- **Well placed** — two stations spaced across mid-town reach all 16 blocks between them
- **Badly placed** — the same two stations crowded near downtown leave 3 far-corner blocks exposed
- **The fewest** — no single block reaches every corner in 5 minutes, so two is the true minimum

*Example (italic):* Both placements buy the same two fire engines; only one of them leaves the southeast corner waiting six-plus minutes.

**Key point:** Coverage is a placement question, not a spending question: the same station count can protect the whole map or leave corners exposed.

### Visualization (canvas `c1`, 720×300)

Two side-by-side 4×4 block grids — well-placed stations covering everything on the left, downtown-crowded stations leaving three corner blocks exposed on the right.

- **Title (bold 15px, `#1a5276`, top center):** "Two Stations, Two Placements — 5-Minute Coverage (illustrative)".
- **Caption (12px `#6b7280`, centered at y=44):** "a 5-minute drive ≈ 3 blocks along the street grid".
- **Grids:** cell size 44; left grid origin (75, 70), right grid origin (445, 70); 4 columns × 4 rows; cell borders 1px `#b8c4cf`.
- **Panel headers (bold 13px, centered over each grid at y=62):** "well placed" green `#008300` at x=163; "badly placed" orange `#d95926` at x=533.
- **Coverage model:** a block is covered if its Manhattan distance to a station is ≤ 3 blocks.
- **Cell fills:** covered `rgba(0,131,0,0.12)`; uncovered `rgba(217,89,38,0.2)` plus a bold 13px orange `#d95926` "✕" centered in the cell.
- **Stations:** filled blue `#2a78d6` circle r=10 at the cell center with bold 12px white "S".
- **Left stations (col,row):** (1,1) and (2,2) — every block covered. **Right stations:** (0,1) and (1,0) — uncovered blocks (3,2), (2,3), (3,3).
- **Panel verdicts (bold 12px, centered under each grid at y=264):** left green "16 / 16 blocks within 5 min"; right orange "13 / 16 — three corners exposed".
- **Annotation (bold 13px magenta `#d55181`, centered at y=288):** "same two stations — placement alone decides who waits".

## Timing the Traffic Lights — the Green Wave

**Tags:** `in practice` (blue), `timing` (orange)

- **The goal** — a driver at the speed limit should meet green after green down the whole avenue
- **The data** — road sensors count cars in each direction; the heavier flow earns the wave
- **The spacing** — the four lights sit 330 m apart, and at 40 km/h a car covers 330 m in 30 seconds
- **The offset** — start each light's green 30 s after the previous one and the car never stops
- **The cycle** — every light still runs 30 s green, 30 s red; only the start times are shifted

*Example (italic):* A car leaving light 1 at second 5 reaches lights 2, 3, and 4 at seconds 35, 65, and 95 — green every single time.

**Key point:** A green wave costs nothing to build: the lights are unchanged, only their offsets shift — and each offset is just distance ÷ speed.

### Visualization (canvas `c2`, 720×300)

A distance-vs-time diagram: four horizontal light rows with alternating red/green bands, and one car's diagonal line threading every green.

- **Title (bold 15px, `#1a5276`, top center):** "The Green Wave — One Car Threads Four Greens (illustrative)".
- **Scales:** time t ∈ 0–120 s mapped to x = 70 + 5·t (x 70–670); distance d ∈ 0–990 m mapped to y = 250 − d·(180/990), so lights at 0 / 330 / 660 / 990 m sit at y = 250 / 190 / 130 / 70.
- **Light rows:** each light is a band 10px tall (y±5) built from 30-s segments; green `rgba(0,131,0,0.55)`, red `rgba(231,76,60,0.55)` (red = genuine stop signal). Cycle 60 s. Green intervals — Light 1 (offset 0): 0–30, 60–90; Light 2 (offset 30): 30–60, 90–120; Light 3 (offset 60): 0–30, 60–90; Light 4 (offset 90): 30–60, 90–120.
- **Row labels (bold 12px `#1a5276`, right-aligned at x=62):** "Light 1 (0 m)", "Light 2 (330 m)", "Light 3 (660 m)", "Light 4 (990 m)" at each band's y+4.
- **Legend (top right, y=30):** 24×10 swatches — green at x=540 labeled "green", red at x=612 labeled "red"; labels 11px `#6b7280` at swatch right edge + 6.
- **Time axis:** 1px `#999` line at y=262 from x=70 to x=670; ticks every 30 s labeled "0s" … "120s" 12px `#6b7280` centered at y=278.
- **Car line:** 2.5px blue `#2a78d6` from (95, 250) to (545, 70) — leaves light 1 at t=5, passes each light 30 s later; blue filled dots r=4 at both ends; label bold 12px blue "car at 40 km/h" rotated along or beside the line near (350, 150).
- **Annotation (bold 13px green `#008300`, centered at y=296):** "each light turns green 30 s after the one before — the car never stops".

## Drawing the Bus Routes

**Tags:** `in practice` (blue), `routing` (orange)

- **The trade-off** — a straight route is fast but strands side streets; a winding one crawls
- **The data** — tap-on counts say who boards where: every stop gets a daily-boardings number
- **Route A** — straight down Main Avenue, 20 minutes end to end, but it skips Hillside entirely
- **Route B** — winds through Hillside and Mill Lane, reaching everyone in 29 minutes
- **The verdict** — Hillside's 600 daily boardings justify a bend; Mill Lane's 40 do not

*Example (italic):* Bending for Hillside costs 4 minutes and gains 600 riders; bending again for Mill Lane costs 5 more minutes for just 40.

**Key point:** Where a bus line bends is a data decision: each detour trades minutes for riders, and the boarding counts price that trade.

### Visualization (canvas `c3`, 720×300)

A small street map with two candidate routes — direct along the avenue vs winding through side streets — and boardings-per-stop dots sized by demand.

- **Title (bold 15px, `#1a5276`, top center):** "Two Candidate Routes — Ridership Decides the Bend (illustrative)".
- **Street grid (1px `#e5e9ef`):** horizontal lines at y = 110, 160, 205, 250 from x=80 to x=670; vertical lines at x = 150, 240, 330, 420, 510, 600 from y=85 to y=260.
- **Route A (direct, 3px blue `#2a78d6`):** straight line at y=105 from x=90 to x=660.
- **Route B (winding, 3px orange `#d95926`):** polyline (90,115) → (195,115) → (195,205) → (330,205) → (330,115) → (455,115) → (455,230) → (565,230) → (565,115) → (660,115).
- **Stops:** filled circles with 1.5px white stroke, radius 4 + boardings/60 (capped at 14): West Terminal (90,110) 520, Market St (260,110) 340, Center Sq (430,110) 410, East Station (660,110) 560 — all ink `#1a5276`; Hillside Loop (262,205) 600 green `#008300`; Mill Lane (510,230) 40 mute `#6b7280`.
- **Stop labels:** avenue stops 11px `#6b7280` above the line (y=88): "Terminal 520", "Market 340", "Center 410", "Station 560" (Station right-aligned at x=668, Terminal left-aligned at x=82); side stops bold 12px in their dot color — "Hillside — 600/day" at (262, 232) green, "Mill Lane — 40/day" at (510, 254) mute.
- **Route legend (left-aligned at x=82):** bold 12px blue "Route A — direct, 20 min" at y=44; bold 12px orange "Route B — winding, 29 min" at y=62.
- **Annotation (bold 13px magenta `#d55181`, centered at y=288):** "the 600-rider dot earns its bend — the 40-rider dot doesn't".

## More Isn't Better — Placement Beats Quantity

**Tags:** `common mistake` (red)

- **The instinct** — "response is slow, so build a third station" feels obvious and is often wrong
- **The numbers** — a 3rd station downtown covers 15 of 16 blocks; moving one east covers all 16
- **Placement first** — a station, light, or route helps only where the data says the demand is
- **Nothing is final** — new housing, shifting traffic, and ridership drift change the answers
- **The loop** — covering, timing, and routing are all recomputed as the data underneath moves

*Example (italic):* The cheaper plan wins twice: two well-placed stations out-cover three crowded ones and save a whole station's budget.

**Common mistake:** Believing more infrastructure is always better. Placement beats quantity — a city runs on covering, timing, and routing decisions, all recomputed as its data shifts.

### Visualization (canvas `c4`, 720×300)

Bar chart of blocks covered on the fire-station map: today's bad placement, adding a third station downtown, and simply moving one station east.

- **Title (bold 15px, `#1a5276`, top center):** "Fixing the Coverage Gap — Add vs Move (illustrative)".
- **Scale:** baseline y=232, plot height 160, value scale 0–16 blocks (10 px per block); axis lines 1px `#999` from (70, 72) down to (70, 232) and across to (690, 232).
- **Y ticks:** 0 / 4 / 8 / 12 / 16 labeled 12px `#6b7280` right-aligned at x=62; light gridlines 1px `#e5e9ef` across the plot at each tick (skip 0).
- **Bars (width 120, x = 120 / 300 / 480):**
  - "today" = 13 blocks, fill `rgba(42,120,214,0.55)`, value label bold 13px blue `#2a78d6` "13 / 16"
  - "add a 3rd station" = 15 blocks, fill `rgba(217,89,38,0.5)`, value label bold 13px orange `#d95926` "15 / 16"
  - "move one station" = 16 blocks, fill `rgba(0,131,0,0.45)`, value label bold 13px green `#008300` "16 / 16"
- **Bar captions:** two 12px `#444` lines below the baseline (y+18 and y+33): "today" / "2 stations, badly placed"; "add a 3rd station" / "kept downtown (3 stations)"; "move one station" / "east, where data says (2)".
- **Annotation (bold 13px green `#008300`, centered at y=290):** "moving one station beats buying a third — placement over quantity".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
