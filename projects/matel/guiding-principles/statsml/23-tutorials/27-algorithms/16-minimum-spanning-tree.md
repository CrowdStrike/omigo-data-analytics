# Minimum Spanning Tree

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Minimum Spanning Tree

**Subtitle:** To connect every house with the least total cable, always dig the cheapest trench that links something new — Kruskal and Prim both do this, and this greedy move is provably never wrong

## Wiring Six Houses with the Least Cable

**Tags:** `core idea` (blue), `spanning tree` (green), `no loops` (orange)

- **Six houses** — a new lane has houses A–F, and every house needs to reach the shared internet line
- **Nine trenches** — the digger quotes 9 possible house-to-house trenches, each with a cost in $100s
- **Connected is enough** — a house is online if any chain of cables reaches it; no direct line needed
- **Loops waste money** — a cable that closes a circle adds cost but connects nothing new
- **The tree** — 6 houses need exactly 5 cables: connected everywhere, no loops, cheapest total 20
- **The name** — that cheapest loop-free set touching every house is the minimum spanning tree

*Example (italic):* Picking trenches D–E (2), A–C (3), A–B (4), E–F (5) and B–D (6) links all six houses for 20 — $2,000 — and no other choice of cables does it cheaper.

**Key point:** A minimum spanning tree is the cheapest set of connections that reaches every node with no loops — for n nodes that is always exactly n−1 links.

### Visualization (canvas `c1`, 720×300)

Single-panel network map: six labeled house nodes with all nine candidate trenches drawn and priced, the five chosen MST cables in bold green, the four skipped ones dashed gray.

- **Title (bold 15px, `#1a5276`, top center):** "Six Houses, Nine Possible Trenches — the Cheapest 5 Connect Everyone".
- **Nodes:** filled circles radius 14, fill `#1a5276`, bold 13px white letter centered — A(120, 120), B(300, 75), C(250, 225), D(470, 105), E(440, 240), F(620, 185).
- **MST edges (green `#008300`, 4px solid):** D–E, A–C, A–B, E–F, B–D.
- **Skipped edges (mute `#6b7280`, 2px dashed 5/4):** B–C, C–D, C–E, D–F.
- **Cost labels:** bold 12px at each edge midpoint, nudged 10px off the line, on a white background pad — green `#008300` on chosen edges ("2", "3", "4", "5", "6"), `#6b7280` on skipped ones ("5", "7", "8", "9"); edge costs: A–B 4, A–C 3, B–C 5, B–D 6, C–D 7, C–E 8, D–E 2, D–F 9, E–F 5.
- **Annotation (bold 13px green `#008300`, near x=520, y=45):** two lines: "5 cables, total 20" / "every house reached, zero loops".
- **Caption (12px `#444`, bottom right):** "trench costs in $100s, illustrative".

## Kruskal and Prim: Two Greedy Routes to the Same 20

**Tags:** `worked example` (blue), `kruskal` (green), `prim` (orange)

- **Kruskal's rule** — sort all trenches by price, take each unless it closes a loop, stop at 5
- **Sorted list** — 2, 3, 4, 5, 5, 6, 7, 8, 9: take D–E, A–C, A–B; skip B–C (loops A–B–C); take E–F, B–D
- **Done** — 5 cables in, the remaining 7, 8, 9 would all close loops: total 2+3+4+5+6 = 20
- **Prim's rule** — start at any house and repeatedly buy the cheapest trench to a not-yet-wired house
- **Prim from A** — A–C (3), A–B (4), B–D (6), D–E (2), E–F (5): different order, same 5 cables, same 20
- **Redo it by hand** — nine prices, a few loop checks; both methods land on the identical tree

*Example (italic):* Kruskal grabs the $200 trench D–E first even though D and E sit far from house A; Prim, growing from A, only buys it fourth — yet both bills total exactly 20.

**Key point:** Kruskal sorts cables and skips loop-makers; Prim grows one connected patch outward — on this lane both pick the same five trenches costing 20.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart of Kruskal's pass: the nine trenches in sorted price order, each bar marked take or skip, with a running total printed after each accepted cable.

- **Title (bold 15px, `#1a5276`, top center):** "Kruskal's Pass: Cheapest First, Skip Anything That Loops".
- **Layout:** row labels 12px `#444` right-aligned at x=150; bars start x=160, scale 40px per cost unit, bar height 16, rows at y = 62, 86, 110, 134, 158, 182, 206, 230, 254; x axis unmarked (bar length shows cost).
- **Rows (top to bottom, edge / cost / status):** D–E 2 take, A–C 3 take, A–B 4 take, B–C 5 skip (loops A–B–C), E–F 5 take, B–D 6 take, C–D 7 skip, C–E 8 skip, D–F 9 skip.
- **Bar style:** taken bars fill `rgba(0,131,0,0.35)` with 2px `#008300` border; skipped bars fill `rgba(107,114,128,0.18)` with 2px dashed `#6b7280` border.
- **End-of-bar labels (12px):** taken rows show bold green running total "total 2", "total 5", "total 9", "total 14", "total 20"; skipped rows show `#6b7280` "skip — loop".
- **Annotation (bold 13px green `#008300`, near x=480, y=200):** two lines: "5 taken = houses − 1" / "greedy total: 20".
- **Caption (12px `#444`, bottom right):** "costs in $100s, illustrative".

## Why the Cheap-First Trick Is Safe — and Where It Shows Up

**Tags:** `where it's used` (blue), `greedy wins` (green), `cut property` (orange)

- **Greedy is usually risky** — grabbing the best-looking piece first often paints you into a corner
- **Here it never does** — split the houses into any two groups; the cheapest cable across the split is always safe
- **Why** — any full wiring must cross that split somewhere; swapping its crossing for the cheapest one never costs more
- **Provably optimal** — that cut argument covers every greedy pick, so Kruskal and Prim are exact, not approximate
- **Real networks** — power grids, water lines, and fiber rollouts are priced as spanning-tree problems
- **Data science too** — single-link clustering is an MST with its longest cables cut

*Example (italic):* Split the lane into {A, B, C} and {D, E, F}: the crossings cost 6, 7 and 8, so the $600 cable B–D is guaranteed to belong in the cheapest wiring — no lookahead needed.

**Key point:** MST is the rare problem where greedy is provably perfect — the cheapest edge across any split of the nodes is always part of a cheapest tree.

### Visualization (canvas `c3`, 720×300)

Single-panel cut diagram: the six-house map with a dashed vertical split between {A, B, C} and {D, E, F}, the three crossing cables priced, and the cheapest crossing highlighted as the guaranteed pick.

- **Title (bold 15px, `#1a5276`, top center):** "The Cut Property: the Cheapest Cable Across Any Split Is Always Safe".
- **Nodes:** same style as c1 (radius 14, fill `#1a5276`, bold 13px white letters) — A(110, 120), B(270, 75), C(230, 225), D(490, 105), E(460, 240), F(640, 185).
- **Group shading:** rounded rect `rgba(42,120,214,0.08)` behind A, B, C (x 60–330, y 45–270); rounded rect `rgba(217,89,38,0.08)` behind D, E, F (x 420–690, y 45–270); 12px labels "left group" (blue `#2a78d6`) and "right group" (orange `#d95926`) at each rect's top-left.
- **Split line:** vertical dashed (dash 6/4) 2px `#6b7280` line at x=375 from y=40 to y=285.
- **Crossing cables:** B–D bold 4px green `#008300` with bold 13px green label "6 — cheapest crossing"; C–D and C–E 2px dashed `#6b7280` with 12px `#6b7280` labels "7" and "8".
- **Within-group cables (2px solid `#e5e9ef`, 11px `#9aa3ad` cost labels):** A–B 4, A–C 3, B–C 5 on the left; D–E 2, D–F 9, E–F 5 on the right.
- **Annotation (bold 13px green `#008300`, near x=380, y=290, centered):** "any wiring must cross — crossing at 6 can never lose".
- **Caption (12px `#444`, bottom right):** "one example split; the rule holds for every split — illustrative".

## A Cheapest Tree Is Not a Shortest-Path Map

**Tags:** `common mistake` (red), `mst vs shortest path` (orange)

- **The trap** — assuming the MST also gives each house its shortest route to every other house
- **Check A to E** — inside the tree the only route is A–B–D–E, costing 4+6+2 = 12
- **A cheaper trip exists** — the direct route A–C–E costs 3+8 = 11, but cable C–E was never built
- **Different goals** — MST minimizes one total dig bill; shortest path minimizes each single journey
- **Different tools** — trenching budgets want Kruskal or Prim; travel times want Dijkstra's algorithm

*Example (italic):* The lane's total dig bill is minimal at 20, yet a packet from A to E pays 12 inside the tree when an 11 route existed — the tree happily sacrifices one trip to shrink the total.

**Common mistake:** Reading an MST as a routing map. It minimizes the sum of built links, not any single journey — paths inside the tree can be longer than the true shortest paths.

### Visualization (canvas `c4`, 720×300)

Single-panel route comparison on the six-house map: the MST in faint green, the forced tree route A→E overdrawn in bold green, and the cheaper unbuilt route A→C→E in bold dashed orange.

- **Title (bold 15px, `#1a5276`, top center):** "A to E: the Tree Route Costs 12, the True Shortest Path Costs 11".
- **Nodes:** same style and coordinates as c1 — A(120, 120), B(300, 75), C(250, 225), D(470, 105), E(440, 240), F(620, 185); A and E drawn with an extra 3px `#1a5276` ring to mark endpoints.
- **MST base:** all five MST edges (D–E, A–C, A–B, E–F, B–D) 3px `rgba(0,131,0,0.25)`; skipped edges omitted except C–E.
- **Tree route A→E:** edges A–B, B–D, D–E overdrawn 5px solid green `#008300` with small arrowheads toward E; bold 12px green step labels "4", "6", "2" at midpoints; bold 13px green label near D (x≈470, y≈60): "tree route: 4+6+2 = 12".
- **Shortest route A→E:** edges A–C and C–E drawn 4px dashed (dash 7/5) orange `#d95926` with arrowheads toward E; bold 12px orange step labels "3", "8"; bold 13px orange label near x=250, y=280: "shortest path: 3+8 = 11 — cable never built".
- **Annotation (bold 13px magenta `#d55181`, near x=545, y=290, right-aligned):** "cheapest total ≠ shortest trips".
- **Caption (12px `#444`, bottom right):** "costs in $100s, illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** one hardcoded graph shared by all four canvases — nodes A–F at the pixel coordinates above; edge list with costs `[A–B 4, A–C 3, B–C 5, B–D 6, C–D 7, C–E 8, D–E 2, D–F 9, E–F 5]`; MST = {D–E, A–C, A–B, E–F, B–D}, total 20; no randomness anywhere. Kruskal running totals 2, 5, 9, 14, 20; Prim-from-A order A–C, A–B, B–D, D–E, E–F; A→E tree route 12 vs shortest path 11.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
