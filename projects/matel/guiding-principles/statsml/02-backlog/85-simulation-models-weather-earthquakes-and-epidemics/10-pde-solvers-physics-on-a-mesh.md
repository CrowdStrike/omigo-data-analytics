# PDE Solvers — Physics on a Mesh

**Page type:** detail page (four titled sections, each a two-column row: text left 50%, canvas right 50%)
**HTML title tag:** PDE Solvers — Physics on a Mesh

**Subtitle:** Continuous physics — heat, air, steel — chopped onto a mesh of cells that only talk to their neighbors: how the machine works, why it explodes, and what it cannot see.

**Intro callout (blue-left-border box):** Weather forecasts, car crash tests, and wing design all run on one idea: the laws of physics only say how each point responds to its immediate surroundings, so chop space into cells, let each cell trade with its neighbors, and step time forward in small ticks. This page builds that machine by hand on a six-cell metal rod, then shows the two ways it bites: a tick that is too big does not blur the answer — it detonates it — and any detail smaller than a cell was never simulated at all.

## 1. Chop the world into cells

The rules behind weather, crashes, and airflow tie how things change in time to how they vary across space (partial differential equations), and for real shapes those rules have no formula answer — so space becomes a mesh of cells and time becomes small steps.

- **The rule is local:** Heat, pressure, and stress at a point respond only to conditions right next door.
- **No formula answer:** A real shape like a car body has no exact pencil-and-paper solution.
- **Cells replace space:** Space becomes a mesh of small boxes, each storing one number per quantity.
- **Steps replace time:** Time advances in small ticks, and every cell updates once per tick.
- **Neighbors only:** At each tick, a cell looks only at its immediate neighbors to decide its new value.
- **Influence hops:** A distant disturbance reaches a cell only by hopping across cells, one hop per tick.
- **One recipe, many physics:** The same chop-and-step recipe (finite differences) handles heat, waves, and flow.

Key point: A mesh solver never sees the smooth world — it sees one number per cell, and everything it predicts is built from neighbor-to-neighbor bookkeeping.

### Visualization (canvas `c1`, 720×360)

A smooth decaying heat profile drawn as a continuous curve, with its staircase version on an 8-cell mesh underneath — the solver stores only the staircase.

- **Title (bold 14px `#1a5276`, centered, y=22):** "One smooth reality, one staircase copy — what the solver actually sees"
- **Baseline axis:** 1.5px `#999` horizontal line at y=300 from x=60 to x=660; 10px `#999` centered "position along the rod →" at (360, 320).
- **Staircase cells (drawn first, under the curve):** 8 rectangles of width 72.5 spanning x=70 to x=650, each from its top y down to the baseline y=300; fill `rgba(26,82,118,0.12)`, stroke 1px `#1a5276`. Cell tops y: 114, 172, 212, 240, 258, 271, 280, 286 (heat values are illustrative literals from a decaying profile).
- **Smooth curve:** 2.5px `#27ae60` polyline through (70, 75), (128, 133), (186, 177), (244, 209), (302, 232), (360, 250), (418, 263), (476, 272), (534, 280), (592, 285), (650, 289).
- **Labels:** bold 10px `#27ae60` left-aligned "smooth reality" at (135, 95); bold 10px `#1a5276` centered "one number per cell" at (470, 245) with a thin 1px `#1a5276` connector from (470, 250) down to (470, 268).
- **Caption (12px `#999`, centered, y = h−14):** "Illustrative heat profile — the solver keeps one number per cell; the curve between cells is gone"

## 2. Heat on a rod, by hand

Take a metal rod chopped into 6 cells, put all the heat in cell 1 at 96 degrees, and apply one rule per tick: every interior cell moves halfway toward the average of its two neighbors, and each insulated end cell moves a quarter of the way toward its single neighbor.

- **Start (tick 0):** The cells read 96, 0, 0, 0, 0, 0 — all the heat sits in cell 1.
- **The update rule:** Each tick, a cell moves halfway toward the average of its two neighbors.
- **Insulated ends:** An end cell moves a quarter of the way toward its single neighbor.
- **Tick 1:** Cell 1 drops to 72 and cell 2 rises to 24 — the heat has hopped one cell.
- **Tick 2:** The rod reads 60, 30, 6, 0, 0, 0 — the warm front hops again.
- **Tick 3:** The rod reads 52.5, 31.5, 10.5, 1.5, 0, 0 — the profile keeps smoothing out.
- **Heat is conserved:** Every row sums to exactly 96 — the rule moves heat, it never creates it.

Key point: Three ticks of grade-school arithmetic already show heat diffusing down a rod — a supercomputer weather run is this same neighbor averaging, repeated over billions of cells and thousands of ticks.

### Visualization (canvas `c2`, 720×360)

Grouped bar chart: six cell groups, four bars per group (ticks 0–3), showing the heat spreading right and evening out; bar heights are the exact hand-computed values.

- **Title (bold 14px `#1a5276`, centered, y=22):** "The six-cell rod, ticks 0–3 — heat evening out cell by cell"
- **Baseline axis:** 1.5px `#999` horizontal line at y=300 from x=60 to x=660.
- **Data (temperature per cell at ticks 0/1/2/3; bar height = value × 2.2 px):** cell 1: 96, 72, 60, 52.5; cell 2: 0, 24, 30, 31.5; cell 3: 0, 0, 6, 10.5; cell 4: 0, 0, 0, 1.5; cells 5 and 6: all 0.
- **Bars:** group i (cell i, i = 0..5) starts at x = 80 + i×95; four bars of width 20 at offsets 0, 21, 42, 63; bars rise from the y=300 baseline; zero values draw nothing. Fills by tick: tick 0 `#1a5276`, tick 1 `#2980b9`, tick 2 `#27ae60`, tick 3 `#e67e22`.
- **Value labels:** 9px `#666` centered above each nonzero bar at y = 300 − height − 4 — cell 1: "96", "72", "60", "52.5"; cell 2: "24", "30", "31.5"; cell 3: "6", "10.5"; cell 4: "1.5".
- **Empty groups:** 9px `#999` centered "all 0" at the group center, y=292, for cells 5 and 6.
- **Group labels:** bold 11px `#555` centered "cell 1" … "cell 6" at each group center (x = 80 + i×95 + 41.5), y=320.
- **Legend (top right, swatches 12×12 at x=560, rows y=48/64/80/96, 10px `#666` labels at x=578):** `#1a5276` "tick 0", `#2980b9` "tick 1", `#27ae60` "tick 2", `#e67e22` "tick 3".
- **Caption (12px `#999`, centered, y = h−14):** "Halfway-to-the-neighbor-average rule; every tick the six cells still sum to 96"

## 3. The speed limit of the timestep

If the tick is too big for the cell size, each cell leaps past its neighbors' average instead of settling toward it, the overshoots compound, and the run explodes — there is a hard speed limit tying the tick to the cell size (the CFL condition).

- **Overshoot, not blur:** With a tick three times too big, cell 1 leaps from 96 down to 24 in one step.
- **Past the target:** The rule should stop at the neighbor average, but the oversized tick jumps far beyond it.
- **Wiggles feed themselves:** Every overshoot creates a bigger imbalance for the next tick to overshoot.
- **Explosion, not noise:** Within eight ticks the end cell swings to −147° and then +304° — physically meaningless.
- **A hard threshold:** A tick even slightly over the limit still explodes, just a few ticks later.
- **Cells set the tick:** Halving the cell size forces a smaller safe tick too, so resolution is paid twice.
- **3D is brutal:** Halving cells in three dimensions means 8x the cells before the smaller tick is even counted.

Key point: The stability limit is a cliff, not a slope — solvers compute the largest safe tick from the cell size and the physics, because guessing even slightly high turns the entire run into garbage.

### Visualization (canvas `c3`, 720×380)

Cell-1 temperature over ticks for the same rod under two tick sizes: the safe tick relaxes smoothly, the oversized tick oscillates and blows past a red dashed line marking the hottest temperature the rod ever really had. Both series are the exact simulated values.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Same rod, two tick sizes — one relaxes, one explodes"
- **Mapping:** tick t maps to x = 90 + 65t (ticks 0–8); temperature v maps to y = 210 − 0.45v.
- **Zero line:** dashed (4/3) 1px `#bbb` horizontal line at y=210 from x=70 to x=650; 10px `#999` right-aligned "0°" at (66, 214).
- **Tick labels:** 10px `#999` centered "0"–"8" under each point at y=318; 10px `#999` centered "ticks →" at (360, 340).
- **Stability limit line:** dashed (5/4) 2px `#e74c3c` horizontal line at y=167 (value 96) from x=70 to x=650; 10px `#e74c3c` left-aligned "96° — hottest the rod ever really was" at (395, 160).
- **Stable curve (safe tick):** 2.5px `#27ae60` polyline with filled radius-3 dots through values 96, 72, 60, 52.5, 47.2, 43.3, 40.2, 37.7, 35.6 → points (90, 167), (155, 178), (220, 183), (285, 186), (350, 189), (415, 190), (480, 192), (545, 193), (610, 194).
- **Exploding curve (tick 3x too big):** 2.5px `#e74c3c` polyline with filled radius-3 dots through values 96, 24, 60, 1.5, 71.2, −39.6, 127.2, −146.8, 303.9 → points (90, 167), (155, 199), (220, 183), (285, 209), (350, 178), (415, 228), (480, 153), (545, 276), (610, 73).
- **Legend (top left, 18px line swatches at x=80–98, rows y=48/64, 10px `#666` labels at x=104):** 2.5px `#27ae60` line "safe tick — moves halfway to the neighbor average"; 2.5px `#e74c3c` line "tick 3x too big — overshoots every step".
- **Annotation:** bold 11px `#e74c3c` centered at (480, 100): "−147°, then +304° — numbers from nowhere"; thin 1px `#e74c3c` connector from (555, 106) to (600, 80).
- **Caption (12px `#999`, centered, y = h−14):** "Exact simulated values — past the stability limit the run detonates, it does not degrade"

## 4. Meshes in the wild

The same cells-and-ticks recipe runs the world's biggest simulations, and the practical skill is knowing what a given mesh can and cannot resolve.

- **Weather:** Global forecast models carve the atmosphere into boxes roughly 10 km on a side.
- **Below the boxes:** Individual clouds are smaller than a box, so they are hand-modeled, not simulated.
- **Crash tests:** A car body panel becomes a mesh of tiny triangles (finite elements) that bend and tear.
- **Airflow:** Air over a wing becomes cells trading pressure and flow with neighbors (finite volumes).
- **The artifact rule:** Any feature about the size of one or two cells is grid noise, not physics.
- **Common mistake:** Zooming into a simulation and trusting a swirl that is three cells wide.
- **Refinement test:** Rerun on a finer mesh — real features stay put, mesh artifacts move or vanish.

Key point: Trust nothing within a few cells of the mesh resolution — if a feature survives a mesh-refinement rerun it is physics, and if it changes it was the grid talking.

### Visualization (canvas `c4`, 720×380)

Two side-by-side panels showing the same true temperature bump on a coarse 7-cell mesh and a fine 28-cell mesh: the coarse mesh clips the peak and smears the edges, the fine mesh tracks it.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Same bump, two meshes — the coarse grid clips and smears it"
- **Baselines:** 1.5px `#999` horizontal lines at y=300 — left panel from x=60 to x=340, right panel from x=390 to x=670.
- **Panel labels:** bold 11px `#555` centered "coarse mesh — 7 cells" at (200, 332) and "fine mesh — 28 cells" at (530, 332).
- **True bump, left panel:** 2px `#555` polyline through (60, 300), (100, 300), (140, 292), (160, 259), (180, 192), (200, 150), (220, 192), (240, 259), (260, 292), (300, 300), (340, 300).
- **True bump, right panel:** same shape centered at x=530 — 2px `#555` polyline through (390, 300), (430, 300), (470, 292), (490, 259), (510, 192), (530, 150), (550, 192), (570, 259), (590, 292), (630, 300), (670, 300).
- **Coarse cells:** 7 rectangles of width 40 from x=60, heights 0, 2, 48, 124, 48, 2, 0 rising from y=300 (zero heights draw nothing); fill `rgba(231,76,60,0.18)`, stroke 1px `#e74c3c`.
- **Fine cells:** 28 rectangles of width 10 from x=390, heights 0, 0, 0, 0, 0, 1, 2, 5, 13, 29, 55, 90, 125, 147, 147, 125, 90, 55, 29, 13, 5, 2, 1, 0, 0, 0, 0, 0 rising from y=300 (zero heights draw nothing); fill `rgba(26,82,118,0.35)`, stroke 1px `#1a5276`.
- **Left annotation:** bold 11px `#e74c3c` centered two lines — "true peak 150, coarse mesh says 124" at (200, 112) and "cell-sized features are fiction" at (200, 128); thin 1px `#e74c3c` connector from (200, 134) to (200, 146).
- **Right annotation:** bold 10px `#27ae60` centered "fine mesh tracks it" at (530, 120); thin 1px `#27ae60` connector from (530, 126) to (530, 148).
- **Caption (12px `#999`, centered, y = h−14):** "Illustrative bump — anything near the size of a cell is an artifact of the mesh, not the physics"

## Regeneration instructions

- **Template:** backlog multi-section detail page. After h1, `.subtitle`, and `.intro` callout, one `.lang-section` per numbered section: `h2` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px padding-bottom), then a `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) with lead paragraph, bullets, `.key-point`; right `<td class="viz-col">` (50%) with the canvas. Section h2 headings carry the "1.–4." numbers shown above. The h1 carries no index number.
- **Bullet style:** each bullet is a bold label + one short complete sentence that fits on one line at normal page width; split dense content into more bullets rather than longer ones. Bullet `<strong>` labels are colored `#1a5276` (via `li strong { color: #1a5276; }`).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`; `.subtitle` `#666` 0.95rem; `.intro` — background `#f0f4f8`, `border-left: 3px solid #2980b9`, padding 8px 12px, 0.9rem, 32px margin-bottom; `.key-point` — background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem; `ul` 0.92rem; canvases `width: 100%`, `border: 1px solid #e0e0e0`, radius 4px; `.lang-section` 40px margin-bottom.
- **Canvases:** intrinsic width 720, heights 360/360/380/380 as specified; shared `setupCanvas(id, h)` helper scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Canvas fonts use `-apple-system, sans-serif`. Every canvas gets a bold 14px `#1a5276` centered title at y=22 and a 12px `#999` centered caption at y = h−14.
- **Chart data:** all values are hardcoded literal arrays, never random. The rod values in sections 2 and 3 come from the exact update rule (interior cell moves halfway toward the neighbor average; end cell moves a quarter toward its single neighbor; unstable variant moves 1.5x toward the neighbor average, 0.75x at the ends); the c1 and c4 profiles are illustrative literals and their captions say so.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, accent `#2980b9`; grays `#555`/`#666`/`#999`/`#bbb`/`#ccc`; translucent fills `rgba(26,82,118,0.12)`, `rgba(26,82,118,0.35)`, `rgba(231,76,60,0.18)`.
- No nav bar, no back/home links, no cross-references to other pages, no item counts in text.
