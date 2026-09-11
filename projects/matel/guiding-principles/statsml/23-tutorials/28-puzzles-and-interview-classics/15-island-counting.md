# Island Counting

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Island Counting

**Subtitle:** To count islands on a grid map, walk the squares one by one and, each time you step onto unpainted land, paint that whole island before moving on — the painting move is called flood fill

## A Ranger, a Lake, and Graph Paper

**Tags:** `core idea` (blue), `flood fill` (green), `grid map` (orange)

- **The photo** — a park ranger lays graph paper over an aerial photo of a lake: 5 rows × 6 columns, 30 squares
- **Two kinds of square** — each square is marked land (1) or water (0); this lake has 9 land squares
- **The question** — "how many islands?" means how many separate clumps of land, not how many land squares
- **Touching counts** — two land squares belong to the same island when they share an edge, side by side
- **The answer here** — the 9 land squares clump into exactly 3 islands: sizes 3, 2, and 4 squares

*Example (italic):* Squares (row 0, col 0), (row 0, col 1) and (row 1, col 0) all touch by an edge, so they are one island of 3 — not three islands.

**Key point:** An island is a clump of land squares connected edge-to-edge; counting islands means counting clumps, and flood fill is the tool that finds each clump.

### Visualization (canvas `c1`, 720×300)

Single-panel grid map: the ranger's 5×6 lake grid with water squares in pale blue and each of the three islands filled in its own color, sizes labeled.

- **Title (bold 15px, `#1a5276`, top center):** "The Ranger's Map: 30 Squares, 9 Land, 3 Islands".
- **Grid:** origin x=120, y=55, cell 40×40, 6 columns × 5 rows (grid spans x=120–360, y=55–255); 1px `#e5e9ef` cell borders; 11px `#6b7280` column labels "c0"–"c5" above and row labels "r0"–"r4" left of the grid.
- **Grid data (hardcoded, 1=land, 0=water):** `[[1,1,0,0,0,1],[1,0,0,0,0,1],[0,0,1,1,0,0],[0,0,1,1,0,0],[0,0,0,0,0,0]]`.
- **Fills:** water `rgba(42,120,214,0.15)`; island A cells (0,0),(0,1),(1,0) green `rgba(0,131,0,0.45)`; island B cells (0,5),(1,5) orange `rgba(217,89,38,0.50)`; island C cells (2,2),(2,3),(3,2),(3,3) violet `rgba(74,58,167,0.40)`.
- **Island labels (bold 13px, on or beside each clump):** green "A — 3 squares" near (1,1) area, orange "B — 2 squares" right of column 5, violet "C — 4 squares" below the center block.
- **Annotation (bold 13px `#1a5276`, right side near x=470, y=140):** two lines: "count clumps, not squares:" / "9 land squares, 3 islands".
- **Caption (12px `#444`, bottom right):** "illustrative lake map — land/water grid invented for the example".

## Painting the Map, Square by Square

**Tags:** `worked example` (blue), `scan and paint` (green)

- **The walk** — scan the 30 squares left to right, top to bottom, like reading a page of text
- **First land** — square 1 (r0,c0) is unpainted land: shout "island 1!" and start painting
- **Flood fill** — paint the square, then its land neighbors, then theirs, until the clump is done: 3 painted
- **Keep walking** — squares 2–5 are painted or water; square 6 (r0,c5) is fresh land: "island 2!", paint 2
- **Last clump** — square 15 (r2,c2) starts island 3; flood fill paints its 4 squares in order 1→2→3→4
- **Done** — 30 squares walked, 9 painted, the shout counter reads 3 — that is the answer

*Example (italic):* Island 3's fill visits (2,2) first, spreads right to (2,3), down to (3,3), left to (3,2) — four steps, then nothing new touches, so it stops.

**Key point:** Count = number of times the walk steps onto unpainted land; painting each clump immediately is what stops one island being counted twice.

### Visualization (canvas `c2`, 720×300)

Grid snapshot mid-run plus a step ledger: the same 5×6 grid with islands 1 and 2 already painted, island 3's cells numbered in fill order, and a short ledger of the three "new island" moments on the right.

- **Title (bold 15px, `#1a5276`, top center):** "One Walk, Three Shouts: the Scan in Action".
- **Grid:** origin x=90, y=55, cell 40×40, same 6×5 layout and grid data as `c1`; water `rgba(42,120,214,0.15)`, 1px `#e5e9ef` borders; 11px `#6b7280` row/col labels.
- **Painted islands:** island 1 cells (0,0),(0,1),(1,0) solid green `rgba(0,131,0,0.45)`; island 2 cells (0,5),(1,5) solid orange `rgba(217,89,38,0.50)`.
- **Island 3 fill order:** cells (2,2),(2,3),(3,3),(3,2) filled violet `rgba(74,58,167,0.40)` with bold 13px white-on-violet order numbers "1","2","3","4" centered in the cells, in that cell order; 2px violet `#4a3aa7` arrows cell-center to cell-center 1→2→3→4.
- **Scan pointer:** small bold 12px `#d95926` "scan" label with a 2px orange arrow pointing at cell (2,2) from the left margin.
- **Ledger (right side, x=430, rows at y=105/145/185, 12px `#2c3e50`):** "square 1 (r0,c0): new land → island 1, paint 3" / "square 6 (r0,c5): new land → island 2, paint 2" / "square 15 (r2,c2): new land → island 3, paint 4"; below at y=225 bold 13px `#008300`: "total: 3 islands, 9 squares painted".
- **Annotation (bold 12px `#4a3aa7`, under the grid near y=285):** "flood fill = paint neighbors until the clump runs out".
- **Caption (12px `#444`, bottom right):** "illustrative — snapshot just as island 3's fill begins".

## The Map Is Secretly a Graph

**Tags:** `where it's used` (blue), `matrix as graph` (green), `connected components` (orange)

- **The reframe** — call every land square a dot, and draw a line between dots that share an edge
- **New name, same thing** — each island becomes a cluster of linked dots; graph people say "connected component"
- **Why it helps** — every trick for graphs (BFS, DFS, union-find) now works on the ranger's photo unchanged
- **Same puzzle everywhere** — blobs in a photo, linked friend groups, infected machines on a network
- **Interview move** — saying "this matrix is a graph; islands are its components" is half the answer

*Example (italic):* The 9 land squares become 9 dots with 7 connecting lines, and the 3 islands reappear as 3 separate clusters — nothing about the lake changed, only the drawing.

**Key point:** A grid is just a graph wearing a costume — once you see squares as nodes and shared edges as links, island counting becomes the standard connected-components problem.

### Visualization (canvas `c3`, 720×300)

Side-by-side before/after: the small grid map on the left, and on the right the same land squares redrawn as dots and lines, with the three components circled by dashed hulls.

- **Title (bold 15px, `#1a5276`, top center):** "Same Lake, Two Drawings: Grid → Graph".
- **Left panel (grid):** origin x=60, y=70, cell 32×32, 6×5, same grid data as `c1`; water `rgba(42,120,214,0.15)`, land `rgba(26,82,118,0.35)`, 1px `#e5e9ef` borders; 12px `#6b7280` label "the map" centered under it at y=255.
- **Center arrow:** bold 3px `#1a5276` arrow from x=280 to x=330 at y=160, 12px `#1a5276` label "same data" above it.
- **Right panel (graph):** nodes at the 9 land-cell positions, laid out on the same grid geometry with origin x=380, y=70, spacing 42; each node a filled 9px-radius circle — component A nodes (0,0),(0,1),(1,0) green `#008300`, component B nodes (0,5),(1,5) orange `#d95926`, component C nodes (2,2),(2,3),(3,2),(3,3) violet `#4a3aa7`; 3px `#6b7280` edges between edge-adjacent land nodes (6 edges total: (0,0)–(0,1), (0,0)–(1,0), (0,5)–(1,5), (2,2)–(2,3), (2,2)–(3,2), (2,3)–(3,3), (3,2)–(3,3) — draw all 7 adjacent pairs); dashed (dash 5/4) `#6b7280` rounded hull around each component; 12px `#6b7280` label "the graph" centered under it at y=255.
- **Component labels (bold 12px, next to each hull):** green "component A", orange "component B", violet "component C".
- **Annotation (bold 13px `#008300`, top right near x=560, y=55):** "3 islands = 3 components".
- **Caption (12px `#444`, bottom right):** "illustrative — nodes are land squares, links are shared edges".

## Neighbors: Do Corners Count?

**Tags:** `common mistake` (red), `4-way vs 8-way` (orange)

- **The choice** — "touching" can mean sharing an edge only (4 neighbors) or edges and corners (8 neighbors)
- **It changes the answer** — the same small map below has 2 islands edge-only but 1 island with corners
- **Diagonal bridge** — two clumps meeting only corner-to-corner merge under 8-way and stay apart under 4-way
- **Nobody is wrong** — maps and interviews usually mean 4-way; image tools often default to 8-way; ask first
- **The other classic slip** — forgetting to paint visited squares, so the fill loops forever or recounts a clump

*Example (italic):* On the small map, the clump ending at (1,1) and the clump starting at (2,2) touch only at a corner — 4-way sees 2 islands, 8-way sees 1.

**Common mistake:** Assuming everyone shares your definition of "neighbor". State 4-way or 8-way out loud before counting — the same grid honestly gives 2 or 1.

### Visualization (canvas `c4`, 720×300)

Two mini-grids side by side with identical data: left counted with 4-way neighbors (2 islands, two colors), right with 8-way neighbors (1 island, one color), the diagonal contact point marked on both.

- **Title (bold 15px, `#1a5276`, top center):** "One Grid, Two Rules: 4-Way Says 2, 8-Way Says 1".
- **Shared grid data (hardcoded, 1=land, 0=water):** `[[1,1,0,0],[0,1,0,0],[0,0,1,1],[0,0,1,0]]` — 6 land squares, two clumps touching only at the (1,1)/(2,2) corner.
- **Left panel:** origin x=110, y=75, cell 40×40, 4×4 (spans x=110–270, y=75–235); water `rgba(42,120,214,0.15)`, 1px `#e5e9ef` borders; clump (0,0),(0,1),(1,1) green `rgba(0,131,0,0.45)`, clump (2,2),(2,3),(3,2) violet `rgba(74,58,167,0.40)`; bold 13px `#1a5276` label centered below at y=251: "4-way: 2 islands".
- **Right panel:** origin x=440, y=75, same cell size and data; all 6 land cells aqua `rgba(25,158,112,0.45)`; bold 13px `#1a5276` label centered below at y=251: "8-way: 1 island".
- **Corner markers:** on both panels a 3px dashed `#e74c3c` (dash 4/3) short diagonal segment across the shared corner of cells (1,1) and (2,2); on the left a red 12px label "corners don't touch", on the right a red 12px label "corners connect", both centered below the panel count labels at y=269.
- **Annotation (bold 13px `#d95926`, centered between the panels near x=355, y=160, two lines):** "same 6 land squares —" / "the neighbor rule decides".
- **Caption (12px `#444`, bottom right):** "illustrative — say the rule before you count".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** both grids are the hardcoded 0/1 arrays above (no randomness); every count in the text (30 squares, 9 land, islands of 3/2/4, 2-vs-1 on the small grid) must match what the drawn grids show; all fills, edges, and fill-order numbers derive from those arrays by 4-way adjacency except the `c4` right panel, which uses 8-way.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
