# A* Search

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** A* Search

**Subtitle:** A* is Dijkstra's shortest-path search with a compass — every corner is scored by distance ridden so far plus an honest guess of the distance left, so the search leans toward the goal instead of rippling everywhere

## A Courier With a Compass

**Tags:** `core idea` (blue), `g + h score` (green), `guided search` (orange)

- **The errand** — a bike courier at the depot must reach a café across town by the shortest route
- **Dijkstra's way** — try corners in order of distance ridden so far, rippling out in every direction
- **The waste** — the ripple grows behind and sideways too, exploring streets far from the café
- **The compass** — A* adds a hunch: straight-line distance to the café, so goal-ward corners jump the queue
- **The score** — every corner gets f = blocks ridden so far (g) plus straight-line blocks left (h)
- **Same guarantee** — as long as the guess never overestimates, A* still finds the truly shortest route

*Example (italic):* Two corners both sit 3 blocks of riding from the depot; the one whose straight line points at the café gets tried first.

**Key point:** A* is Dijkstra plus a compass: order corners by g + h instead of g alone, and the search stops wasting effort on streets that lead away from the goal.

### Visualization (canvas `c1`, 720×300)

Single-panel schematic map: the depot on the left, the café on the right, Dijkstra's explored region as concentric rings around the depot, and A*'s explored region as a lens-shaped beam pulled toward the café.

- **Title (bold 15px, `#1a5276`, top center):** "Dijkstra Ripples in Every Direction — A* Leans Toward the Café".
- **Depot:** blue `#2a78d6` filled 8px dot at (150, 170), bold 13px blue label "Depot" below at (150, 192).
- **Café:** green `#008300` filled 8px dot at (600, 150), bold 13px green label "Café" above at (600, 132).
- **Dijkstra rings:** three concentric dashed (dash 5/4) `#6b7280` 2px circles centered on the depot, radii 55, 100, 145; one 12px `#6b7280` label near (150, 55): "Dijkstra: equal rings of effort".
- **A* beam:** lens from depot to café — two quadratic curves depot→café with control points (375, 95) (top) and (375, 240) (bottom); fill `rgba(42,120,214,0.15)`, outline blue `#2a78d6` 2px; dashed blue center arrow from (165, 168) to (585, 152) with a small filled arrowhead at the café end.
- **Annotation (bold 13px orange `#d95926`, centered near (375, 78)):** "the compass h pulls the search toward the goal".
- **Caption (12px `#444`, bottom right):** "illustrative — shape of the explored streets, not a real map".

## Five Corners, One Café

**Tags:** `worked example` (blue), `hand-checkable` (green)

- **The map** — five corners (Depot, Market, Park, Bridge, Café) joined by six roads with block counts
- **The guesses** — straight-line blocks to the café: Depot 5, Market 4, Park 3, Bridge 2, Café 0
- **First moves** — from the Depot: Market f = 2+4 = 6 and Park f = 3+3 = 6 — a tie, both stay open
- **The break** — Park reaches the Bridge at g = 3+1 = 4, f = 4+2 = 6; via Market it would be g = 6, f = 8
- **The finish** — Bridge to Café: g = 4+2 = 6, f = 6+0 = 6; nothing open scores below 6, so stop
- **The route** — Depot → Park → Bridge → Café, 6 blocks; the 7-block Market–Café road never wins

*Example (italic):* The courier never rides Market→Café — that single road costs 7 blocks, more than the entire best route.

**Key point:** Score every corner f = g + h and always expand the lowest f: the 6-block route falls out without ever pricing the bad detours to the end.

### Visualization (canvas `c2`, 720×300)

Node-and-edge map of the five corners with block costs on every road, the g+h=f score at every node, and the winning route highlighted in green.

- **Title (bold 15px, `#1a5276`, top center):** "Five Corners, Scored f = g + h — Best Route Is 6 Blocks".
- **Node centers (px):** Depot (110, 190), Market (300, 95), Park (300, 245), Bridge (480, 190), Café (635, 105).
- **Edges:** 2px `#6b7280` straight lines Depot–Market, Market–Bridge; Market–Café drawn as a shallow curve bowed upward (control point (470, 55)); best-route edges Depot–Park, Park–Bridge, Bridge–Café in green `#008300` 4px.
- **Edge cost labels (12px `#444` on small white pills at each midpoint):** Depot–Market "2", Depot–Park "3", Market–Bridge "4", Park–Bridge "1", Bridge–Café "2", Market–Café "7".
- **Nodes:** 15px-radius circles, white fill, 2px `#1a5276` outline; Depot fill blue `#2a78d6`, Café fill green `#008300`; bold 12px `#1a5276` name labels beside each node.
- **Score labels (11px `#6b7280`, one per node, just outside the circle):** Depot "0+5=5", Market "2+4=6", Park "3+3=6", Bridge "4+2=6", Café "6+0=6".
- **Annotation (bold 12px green `#008300`, near (520, 265)):** "lowest f wins every step: 6-block route".
- **Caption (12px `#444`, bottom left):** "block counts illustrative".

## Where the Compass Pays Off

**Tags:** `where it's used` (blue), `speed` (green), `rule of thumb` (orange)

- **GPS routing** — turn-by-turn directions on a country-sized road network would crawl without a compass
- **Game characters** — every unit in a strategy game reruns pathfinding many times per second
- **Puzzle solvers** — sliding puzzles and planners use A* with "tiles out of place" style guesses
- **The savings** — on one 30×20 street grid: Dijkstra checks 480 corners, A* only 140, same 6.0 km route
- **The greedy trap** — following h alone checks just 90 corners but returns a 7.4 km route, not the best
- **Free lunch rule** — a sharper honest heuristic means fewer corners checked at zero loss of correctness

*Example (italic):* A ride-hailing app matching a thousand drivers to pickups reruns shortest-path constantly — A* is what makes that affordable.

**Key point:** A* keeps Dijkstra's shortest-route guarantee while cutting most of its work — 480 corners down to 140 here — which is why it powers maps and games.

### Visualization (canvas `c3`, 720×300)

Single-panel bar chart: corners checked by Dijkstra, A*, and greedy best-first on the same street grid, with the route length each one returns written under its bar.

- **Title (bold 15px, `#1a5276`, top center):** "One 30×20 Street Grid: Corners Checked and Route Found".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 185; y = corners checked 0 to 500, light `#e5e9ef` gridlines at 100, 200, 300, 400 with 12px `#444` tick labels; x axis unlabeled.
- **Bars (width 120, centered):** Dijkstra at x=190, blue `#2a78d6`, value 480; A* at x=390, green `#008300`, value 140; greedy best-first at x=570, orange `#d95926`, value 90.
- **Value labels (bold 13px, bar color, above each bar):** "480", "140", "90".
- **Bar names (12px `#444`, below baseline):** "Dijkstra", "A*", "greedy h-only"; second line under each (12px): "route 6.0 km" (`#444`), "route 6.0 km" (`#444`), "route 7.4 km" in red `#e74c3c`.
- **Annotation (bold 13px violet `#4a3aa7`, near (400, 85)):** two lines: "A*: 3× fewer corners than Dijkstra," / "same best route".
- **Caption (12px `#444`, bottom right):** "illustrative counts from one simulated grid".

## The Heuristic Must Never Overpromise

**Tags:** `common mistake` (red), `admissibility` (orange)

- **The rule** — a heuristic is admissible when it never overestimates the true remaining distance
- **Why straight lines work** — no road is shorter than the straight line, so it can only underpromise
- **Honest guesses** — Depot 5 (true 6), Market 4 (true 6), Park 3 (true 3), Bridge 2 (true 2): all safe
- **One inflated guess** — score the Park at 7 instead of 3 and its f becomes 3+7 = 10, so A* shelves it
- **The damage** — the search finishes through the Market at 8 blocks, never seeing the 6-block route
- **Zero is legal** — h = 0 everywhere is admissible too; A* then simply degrades into plain Dijkstra

*Example (italic):* One corner scored too pessimistically is all it takes — Park guessed at 7 quietly costs the courier 2 extra blocks.

**Common mistake:** Using a "close enough" heuristic that sometimes overestimates. Overestimates don't just slow A* down — they silently break the shortest-route guarantee.

### Visualization (canvas `c4`, 720×300)

Dot-on-bar chart for the four non-goal corners: a muted bar shows the true blocks left to the café, a green diamond marks the honest guess sitting at or below the bar top, and one red X marks the inflated Park guess poking above it.

- **Title (bold 15px, `#1a5276`, top center):** "Never Overpromise: Guesses Must Stay At or Below the True Distance".
- **Axes:** origin x=70, baseline y=245, plot height 180; y = blocks 0 to 8, light `#e5e9ef` gridlines at 2, 4, 6, 8 with 12px `#444` tick labels.
- **Groups (12px `#444` labels below baseline) at x = 170, 310, 450, 590:** "Depot", "Market", "Park", "Bridge".
- **True-distance bars:** fill `rgba(26,82,118,0.35)`, width 46, heights for values `[6, 6, 3, 2]`.
- **Honest guesses:** green `#008300` filled 8px diamonds at heights `[5, 4, 3, 2]`, 11px green value label left of each diamond.
- **Inflated guess (Park only):** red `#e74c3c` bold 9px X marker at height 7 above the Park bar; bold 12px red label beside it: "7 > 3 — breaks A*".
- **Legend (12px, top left, one line):** muted swatch "true blocks left", green diamond "honest guess", red X "inflated guess".
- **Annotation (bold 12px red `#e74c3c`, near (560, 80)):** two lines: "Park scored 7: A* returns the" / "8-block route, not 6".
- **Caption (12px `#444`, bottom right):** "illustrative — same five-corner map".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. Red only for the genuine error states (the greedy 7.4 km route, the inflated Park guess).
- **Data:** all node positions, edge costs, f-scores, bar values, and marker heights are the hardcoded literals above (no `Math.random()`); the corner counts (480/140/90), route lengths (6.0/7.4 km), and block distances are invented and must keep their "illustrative" captions; the worked example's numbers in the text (2, 3, 4, 1, 2, 7 block roads; h = 5, 4, 3, 2, 0; route cost 6; broken cost 8) must match the charts exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
