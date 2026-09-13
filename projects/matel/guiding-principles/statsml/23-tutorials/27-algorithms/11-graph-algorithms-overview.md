# Graph Algorithms Overview

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Graph Algorithms Overview

**Subtitle:** A graph is just dots joined by lines — and BFS, DFS, shortest path, and PageRank are one family of recipes for walking that drawing, each answering a different everyday question

## A Pizza Shop's Map of Town

**Tags:** `core idea` (blue), `nodes & edges` (green), `one family` (orange)

- **The shop** — a pizza shop sketches its town: six landmarks joined by roads, each road tagged in minutes
- **A graph** — that sketch is a graph: dots (nodes) for places, lines (edges) for the roads between them
- **The map** — Shop, Library, School, Park, Market, Gym, connected by nine roads of 1 to 8 minutes
- **One family** — BFS, DFS, shortest path, and PageRank are all careful ways of walking this one map
- **Four questions** — how far is everything? did I visit every corner? fastest route? busiest corner?

*Example (italic):* "Can we deliver to the Gym, and how fast?" is a graph question the shop answers by walking its own sketch — no formulas, just roads.

**Key point:** A graph is dots plus connecting lines; graph algorithms are recipes for walking it, and each recipe answers one plain question about the map.

### Visualization (canvas `c1`, 720×300)

Single-panel node-and-edge map: the six landmarks as labeled circles, the nine roads as lines with their minute labels, introducing the running example.

- **Title (bold 15px, `#1a5276`, top center):** "The Town as a Graph: 6 landmarks, 9 roads, minutes on each road".
- **Nodes (16px-radius circles, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` stroke, bold 12px `#1a5276` name label centered below each):** Shop (90, 165), Library (250, 95), School (250, 225), Park (430, 85), Market (440, 225), Gym (610, 160).
- **Edges (2px `#6b7280` lines between node centers, trimmed at circle edges):** Shop–Library, Shop–School, Library–School, Library–Park, Library–Market, School–Market, Park–Market, Park–Gym, Market–Gym.
- **Edge weights (bold 12px `#d95926`, on a small white rounded chip at each edge midpoint):** Shop–Library "2", Shop–School "5", Library–School "1", Library–Park "4", Library–Market "8", School–Market "7", Park–Market "3", Park–Gym "6", Market–Gym "2".
- **Shop highlight:** Shop node fill `rgba(0,131,0,0.20)` with 2px `#008300` stroke — it is home base for every walk on this page.
- **Annotation (bold 13px violet `#4a3aa7`, near x=480, y=45):** "same map — four different questions".
- **Caption (12px `#444`, bottom right):** "illustrative — road minutes are invented for the example".

## Counting Roads in Rings

**Tags:** `worked example` (blue), `BFS` (green), `DFS` (orange)

- **BFS in rings** — visit everything 1 road from the Shop, then 2 roads, then 3, like a spreading ripple
- **The layers** — {Shop}, then {Library, School}, then {Park, Market}, then {Gym} at 3 roads out
- **DFS dives** — follow one road as deep as it goes before backtracking: Shop → Library → Park → Gym → Market → School
- **Same six stops** — both recipes visit every landmark exactly once; only the visiting order differs
- **Roads, not minutes** — both ignore the minute labels completely; they count roads, nothing else

*Example (italic):* Redo it by hand: the Shop's neighbors Library and School are ring 1; their unvisited neighbors Park and Market are ring 2; Gym is ring 3.

**Key point:** BFS hands you the fewest-roads distance to everything (Gym = 3 roads); DFS guarantees every corner gets visited, one deep dive at a time.

### Visualization (canvas `c2`, 720×300)

Layered tree diagram: the six landmarks arranged in four columns by BFS ring, with discovery arrows between rings, showing the ripple order at a glance.

- **Title (bold 15px, `#1a5276`, top center):** "BFS from the Shop: the Map Sorted into Rings".
- **Column headers (bold 12px `#6b7280`, y=55, centered over columns at x = 100, 270, 450, 620):** "0 roads", "1 road", "2 roads", "3 roads".
- **Nodes (16px-radius circles, bold 12px name label centered below, colored by ring):** Shop (100, 160) blue `#2a78d6`; Library (270, 105) and School (270, 215) green `#008300`; Park (450, 105) and Market (450, 215) orange `#d95926`; Gym (620, 160) magenta `#d55181`. Fill each at 0.15 alpha of its stroke color.
- **Discovery arrows (2px `#6b7280` with small arrowheads):** Shop→Library, Shop→School, Library→Park, Library→Market, Market→Gym.
- **Ring separators:** vertical dashed `#e5e9ef` (dash 4/3) lines at x = 185, 360, 535 from y=70 to y=270.
- **Annotation (bold 12px magenta `#d55181`, two lines near x=560, y=235):** "Gym found 3 roads out —" / "without reading a single minute".
- **Caption (12px `#444`, bottom right):** "arrows show which visit discovered which landmark".

## The Fastest Route and the Busiest Corner

**Tags:** `where it's used` (blue), `shortest path` (green), `PageRank` (orange)

- **Minutes matter** — shortest path (Dijkstra) keeps a best-time-so-far note per landmark and updates as it walks
- **The notes** — from the Shop: Library 2, School 3, Park 6, Market 9, Gym 11 minutes at best
- **The route** — fastest Shop → Gym is Shop → Library → Park → Market → Gym: 2+4+3+2 = 11 minutes
- **The stroller** — PageRank imagines someone wandering forever, picking a random road at every corner
- **Busiest corners** — the stroller stands at Library and Market 22% of the time each; roads pile up there
- **Everywhere** — map apps run shortest path; web search, friend suggestions, and fraud rings run PageRank

*Example (italic):* Google's original trick was exactly the stroller — pages a random web-surfer keeps landing on get ranked higher.

**Key point:** Same map, two more questions: weigh the lines to find the fastest route; wander them at random to find which node matters most.

### Visualization (canvas `c3`, 720×300)

The town map redrawn once more: the fastest Shop→Gym route highlighted in green, and each landmark's circle sized by how often the random stroller visits it.

- **Title (bold 15px, `#1a5276`, top center):** "Fastest Route (green) and Where the Random Stroller Ends Up (circle size)".
- **Nodes (same centers as c1):** Shop (90, 165), Library (250, 95), School (250, 225), Park (430, 85), Market (440, 225), Gym (610, 160); radius by stroller share — 11% → 12px, 17% → 15px, 22% → 19px; shares: Shop 11%, Library 22%, School 17%, Park 17%, Market 22%, Gym 11%. Fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` stroke; Library and Market instead use orange `#d95926` stroke and `rgba(217,89,38,0.15)` fill.
- **Node labels:** bold 12px `#1a5276` name below each circle, bold 12px `#d95926` share ("11%", "22%", "17%", "17%", "22%", "11%") above each circle.
- **Edges:** all nine roads from c1 as 2px `#e5e9ef` lines with 11px `#6b7280` minute labels; then the route edges Shop–Library, Library–Park, Park–Market, Market–Gym redrawn as 4px `#008300` lines with their minute labels ("2", "4", "3", "2") in bold 12px `#008300`.
- **Route total:** bold 13px `#008300` label near x=340, y=280: "fastest Shop → Gym: 2+4+3+2 = 11 min".
- **Annotation (bold 12px orange `#d95926`, two lines near x=520, y=50):** "the stroller piles up" / "where roads pile up".
- **Caption (12px `#444`, bottom right):** "illustrative — stroller shares from road counts per corner".

## Fewest Roads Is Not Fastest

**Tags:** `common mistake` (red), `hops vs weights` (orange)

- **Two winners** — BFS's fewest-roads route to the Gym (3 roads) is not the fastest route (4 roads)
- **Route A** — Shop → Library → Park → Gym: only 3 roads, but 2+4+6 = 12 minutes on the clock
- **Route B** — Shop → Library → Park → Market → Gym: 4 roads, yet 2+4+3+2 = 11 minutes
- **The rule** — BFS answers "fewest hops"; only a weighted shortest path answers "least total cost"
- **When they agree** — if every road took the same time, BFS and Dijkstra would pick the same route

*Example (italic):* Subway riders know this one — the route with the fewest stops loses to a one-extra-stop route whenever one of its legs is slow.

**Common mistake:** Treating BFS hop counts as travel times. The moment edges carry different weights, "fewest edges" and "lowest total cost" become different questions with different answers.

### Visualization (canvas `c4`, 720×300)

Two horizontal stacked bars on a shared minutes axis: each route drawn road by road, so the 3-road route visibly ends past the 4-road route.

- **Title (bold 15px, `#1a5276`, top center):** "Fewest Roads (12 min) vs Fastest Route (11 min) — Shop to Gym".
- **Axis:** horizontal 2px `#999` line at y=250 from x=120 to x=640 (width 520), minutes 0 to 13 at 40px per minute; 12px `#444` tick labels "0", "2", "4", "6", "8", "10", "12" every 2 minutes; 12px `#444` axis label "minutes" below center.
- **Bar rows (22px tall, segments as rounded joined rectangles, left-aligned 12px `#444` labels at x=20):**
  - Row 1 (y=110), "Route A — 3 roads (BFS pick)": segments 2, 4, 6 minutes, fills alternating `rgba(42,120,214,0.45)` / `rgba(25,158,112,0.45)`, 11px `#2c3e50` in-segment labels "Shop→Library 2", "Library→Park 4", "Park→Gym 6"; bold 13px red `#e74c3c` total "12 min" just right of the bar end.
  - Row 2 (y=190), "Route B — 4 roads (Dijkstra pick)": segments 2, 4, 3, 2 minutes, same alternating fills, 11px in-segment labels "Shop→Library 2", "Library→Park 4", "Park→Market 3", "Market→Gym 2"; bold 13px green `#008300` total "11 min" just right of the bar end.
- **Guide line:** vertical dashed `#008300` (dash 4/3) line at minute 11 from y=80 to the axis.
- **Annotation (bold 13px `#008300`, near x=470, y=70):** "one extra road, one minute saved".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Node/edge maps (c1, c3) share one hardcoded coordinate table and one edge list; write a small `drawNode`/`drawEdge` helper rather than repeating code. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all node coordinates, edge lists, minute weights, BFS rings, route segments, and stroller shares are the hardcoded literals above (no randomness); stroller shares are each corner's road count over 18 total road-ends, rounded to whole percents, and labeled illustrative.
- **Consistency check:** the text's numbers must match the charts — layers {Shop} / {Library, School} / {Park, Market} / {Gym}; best times 2, 3, 6, 9, 11; Route A 2+4+6 = 12; Route B 2+4+3+2 = 11; shares 11/22/17/17/22/11.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
