# Conway's Game of Life

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Conway's Game of Life

**Subtitle:** Four tiny rules about lit windows and their neighbors, applied to the whole grid at once — and out of them come blinking shapes, gliding spaceships, and even a working computer

## Four Rules for a Wall of Windows

**Tags:** `core idea` (blue), `four rules` (green), `neighbors` (orange)

- **The building** — picture an apartment wall of windows at night; each window is either lit or dark
- **Neighbors** — every window has 8 neighbors: left, right, above, below, and the four diagonals
- **Lonely** — a lit window with 0 or 1 lit neighbors goes dark; too isolated to stay awake
- **Happy** — a lit window with exactly 2 or 3 lit neighbors stays lit for another night
- **Crowded** — a lit window with 4 or more lit neighbors goes dark; the block got too noisy
- **Birth** — a dark window with exactly 3 lit neighbors lights up; three friends wake a fourth

*Example (italic):* A lit window with lit neighbors directly above and below counts 2, so it stays lit — while a dark window touching exactly 3 lit ones switches on.

**Key point:** That is the whole game — count each window's 8 neighbors, apply one of four rules, repeat night after night. Nothing else is ever added.

### Visualization (canvas `c1`, 720×300)

Four mini 3×3 window grids side by side, one per rule, each showing the center window's lit neighbors, its neighbor count, and its fate.

- **Title (bold 15px, `#1a5276`, top center):** "The Four Rules: Count the 8 Neighbors, Then Decide".
- **Panels:** four 3×3 grids of 28px cells (84×84 each), grid tops at y=70, grid left edges at x = `[75, 245, 415, 585]`; cell borders 1px `#e5e9ef`; lit cells filled blue `#2a78d6`, dark cells white.
- **Panel 1 "lonely":** lit cells (col,row) = `[[1,1],[0,0]]`; bold 13px white "1" on the center cell; below the grid, bold 12px `#e74c3c` label "1 neighbor → goes dark".
- **Panel 2 "happy":** lit cells `[[1,1],[1,0],[1,2]]`; bold 13px white "2" on the center; bold 12px `#008300` label "2 or 3 → stays lit".
- **Panel 3 "crowded":** lit cells `[[1,1],[0,0],[2,0],[0,2],[2,2]]`; bold 13px white "4" on the center; bold 12px `#e74c3c` label "4+ → goes dark".
- **Panel 4 "birth":** lit cells `[[0,1],[1,0],[2,1]]`, center `[1,1]` dark with a 2px dashed `#008300` outline; bold 13px `#008300` "3" on the center; bold 12px `#008300` label "dark + exactly 3 → lights up".
- **Panel names:** bold 13px `#1a5276` above each grid at y=60: "lonely", "happy", "crowded", "birth".
- **Annotation (bold 12px violet `#4a3aa7`, centered near y=270):** "only the 8 touching windows matter — nothing else on the wall".
- **Caption (12px `#444`, bottom right):** "illustrative — an apartment wall obeying Life's four rules".

## Checking the Blinker by Hand

**Tags:** `worked example` (blue), `by hand` (green)

- **The setup** — three lit windows stacked in a column on an otherwise dark 5×5 wall
- **Middle cell** — it touches 2 lit neighbors (above and below), so rule two keeps it lit
- **End cells** — the top and bottom lit windows each touch only 1 lit neighbor, so both go dark
- **Side cells** — the dark windows left and right of the middle each touch 3 lit ones: both light up
- **The flip** — the column of 3 becomes a row of 3; one more night and it flips back to a column
- **Period 2** — this shape is called the blinker, and it repeats every 2 nights forever

*Example (italic):* Count for the dark window right of the middle: it touches all 3 lit windows in the column — exactly 3, so it switches on.

**Key point:** Column of 3 → row of 3 → column of 3. Every step is pure neighbor-counting, and all 25 windows can be verified by hand in a minute.

### Visualization (canvas `c2`, 720×300)

Three 5×5 grids left to right showing the blinker on night 0, night 1, and night 2, with the deciding neighbor counts written on night 0's cells.

- **Title (bold 15px, `#1a5276`, top center):** "The Blinker: Column → Row → Column, Every Count Checkable".
- **Grids:** three 5×5 grids of 32px cells (160×160 each), tops at y=75, left edges at x = `[70, 290, 510]`; borders 1px `#e5e9ef`; lit cells blue `#2a78d6`.
- **Night labels (bold 13px `#1a5276`, centered above each grid at y=65):** "night 0", "night 1", "night 2".
- **Lit cells (col,row):** night 0 = `[[2,1],[2,2],[2,3]]`; night 1 = `[[1,2],[2,2],[3,2]]`; night 2 = `[[2,1],[2,2],[2,3]]`.
- **Counts on night 0:** bold 13px white "1" on cells (2,1) and (2,3), bold 13px white "2" on cell (2,2); bold 13px orange `#d95926` "3" on the dark cells (1,2) and (3,2).
- **Arrows:** 3px `#6b7280` arrows with arrowheads between grid 1→2 (x 235–285) and grid 2→3 (x 455–505) at y=155.
- **Annotation (bold 13px green `#008300`, centered near y=270):** "flips between the two shapes forever — period 2".
- **Caption (12px `#444`, bottom right):** "exact — these are Life's true steps, no numbers invented".

## From Blinking Lights to a Working Computer

**Tags:** `where it's used` (blue), `emergence` (green), `universal computation` (orange)

- **The glider** — a 5-window shape that rebuilds itself one step down-and-right every 4 nights
- **A signal** — a moving shape is a message: glider arriving = 1, no glider = 0
- **Glider guns** — larger patterns fire a steady stream of gliders, like a clock emitting pulses
- **Logic gates** — colliding glider streams can erase each other; that is enough for AND, OR, NOT
- **Turing complete** — with gates and streams you can wire up anything a real computer can compute
- **Emergence** — none of the four rules mentions motion, signals, or logic; all of it just appears

*Example (italic):* Start the 5-window glider near a corner, come back 4 nights later, and the identical shape sits exactly one window down and one to the right.

**Key point:** Four neighbor-counting rules are enough to build a full computer — complex behavior does not require complex rules.

### Visualization (canvas `c3`, 720×300)

Three 6×6 grids showing the glider on nights 0, 4, and 8, drifting one cell down-and-right per panel, with a dashed diagonal drift arrow across the panels.

- **Title (bold 15px, `#1a5276`, top center):** "The Glider: the Same 5 Windows, One Step Diagonally Every 4 Nights".
- **Grids:** three 6×6 grids of 26px cells (156×156 each), tops at y=70, left edges at x = `[70, 290, 510]`; borders 1px `#e5e9ef`.
- **Night labels (bold 13px `#1a5276`, centered above each grid at y=60):** "night 0", "night 4", "night 8".
- **Glider cells (col,row):** night 0 = `[[1,0],[2,1],[0,2],[1,2],[2,2]]` in blue `#2a78d6`; night 4 = `[[2,1],[3,2],[1,3],[2,3],[3,3]]` in green `#008300`; night 8 = `[[3,2],[4,3],[2,4],[3,4],[4,4]]` in orange `#d95926`.
- **Origin marker:** on the night-4 and night-8 grids, draw the night-0 glider cells as 1.5px dashed `#6b7280` outlines so the drift is visible.
- **Drift arrow:** 2px dashed (dash 6/4) `#6b7280` arrow inside the night-8 grid from the outlined start to the solid shape, 11px `#6b7280` label "1 down, 1 right per 4 nights".
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=272):** "a moving shape is a signal — and signals are enough to build logic gates".
- **Caption (12px `#444`, bottom right):** "exact glider steps; the computer built from them is a proven construction".

## Everyone Changes at Midnight, Together

**Tags:** `common mistake` (red), `simultaneous update` (orange)

- **The rule** — every window counts its neighbors first, then all windows switch at the same instant
- **The mistake** — updating one window at a time, letting early changes leak into later counts
- **Blinker test** — done correctly, the 3-window column flips to a row and lives forever
- **Gone wrong** — updated cell by cell, top to bottom, the same column goes completely dark in one pass
- **In code** — always write the new grid into a second copy; never edit the grid you are reading

*Example (italic):* Sweeping top to bottom, the top window dies first, robbing the middle of a neighbor; the middle dies next, then the bottom — the wall is blank by morning.

**Common mistake:** Updating the grid in place. Life's rules read the old night and write the new one; mixing the two turns a living pattern into an empty grid.

### Visualization (canvas `c4`, 720×300)

Two before/after pairs of 5×5 grids: the left pair updates all windows at once and the blinker flips; the right pair updates one cell at a time and the blinker vanishes.

- **Title (bold 15px, `#1a5276`, top center):** "Same Column, Same Rules — Only the Timing Differs".
- **Grids:** four 5×5 grids of 26px cells (130×130 each), tops at y=75, left edges at x = `[50, 215, 415, 580]`; borders 1px `#e5e9ef`; lit cells blue `#2a78d6`.
- **Pair labels (bold 13px, centered above each pair at y=62):** left pair in `#008300`: "correct — all at once"; right pair in `#e74c3c`: "wrong — one cell at a time".
- **Lit cells (col,row):** grid 1 = `[[2,1],[2,2],[2,3]]`; grid 2 = `[[1,2],[2,2],[3,2]]`; grid 3 = `[[2,1],[2,2],[2,3]]`; grid 4 = `[]` (all dark).
- **Arrows:** 3px `#6b7280` arrows with arrowheads between each pair (x 185–210 and x 550–575) at y=140.
- **Outcome labels (bold 12px, centered under each result grid at y=225):** green `#008300` "row of 3 — alive" under grid 2; red `#e74c3c` "all dark — pattern destroyed" under grid 4.
- **Annotation (bold 13px red `#e74c3c`, centered near y=272):** "same four rules, wrong timing — the pattern dies in one pass".
- **Caption (12px `#444`, bottom right):** "exact — verify the top-to-bottom sweep by hand".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. A shared `drawGrid(x0, y0, cols, rows, cell, litCells)` helper keeps the four charts consistent. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** every grid state is one of the hardcoded (col,row) lists above (no randomness); blinker and glider steps are Life's exact evolutions, and the sequential-sweep die-off in c4 is the exact result of a top-to-bottom in-place row-major pass.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
