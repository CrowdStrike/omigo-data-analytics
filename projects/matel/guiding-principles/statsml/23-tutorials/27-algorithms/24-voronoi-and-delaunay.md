# Voronoi & Delaunay

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Voronoi & Delaunay

**Subtitle:** Color every spot on a map by whichever point is closest and the map splits into territories — that is a Voronoi diagram, and connecting the points whose territories touch gives its twin, the Delaunay triangulation

## Every House Orders From Its Nearest Pizza Shop

**Tags:** `core idea` (blue), `nearest neighbor` (green), `territories` (orange)

- **The town** — three pizza shops sit in one town, and every house orders from whichever shop is closest
- **The carve-up** — color each spot of the map by its nearest shop and the town splits into three territories
- **Voronoi cell** — shop A's territory is every point that is closer to A than to any other shop
- **Straight walls** — every border between two territories is a straight line sitting halfway between the shops
- **Triple point** — the one spot where all three territories meet is equally far from all three shops

*Example (italic):* A house standing exactly on the wall between shop A and shop B could flip a coin — both shops are the same distance away.

**Key point:** A Voronoi diagram splits space into closest-to-me regions, one region per point — nothing more than "who is nearest?" drawn as a map.

### Visualization (canvas `c1`, 720×300)

Single-panel map: the town as a bordered rectangle, three shop dots, the three Voronoi territories as tinted polygons meeting at one triple point, and four houses colored by which shop serves them.

- **Title (bold 15px, `#1a5276`, top center):** "One Town, Three Pizza Shops: Closest-Shop Territories".
- **Map frame:** 1px `#e5e9ef` rectangle from (60, 50) to (660, 250); no axes or ticks (it is a map, not a chart).
- **Shops (squares 12px with bold 12px labels beside them):** A blue `#2a78d6` at (240, 160), B orange `#d95926` at (430, 120), C green `#008300` at (390, 215); labels "shop A", "shop B", "shop C".
- **Voronoi walls:** 2px `#6b7280` lines from the triple point (334, 135) to (316, 50) [A|B wall], to (293, 250) [A|C wall], and to (607, 250) [B|C wall].
- **Territory fills (drawn under everything):** A polygon `[(60,50), (316,50), (334,135), (293,250), (60,250)]` filled `rgba(42,120,214,0.15)`; B polygon `[(316,50), (660,50), (660,250), (607,250), (334,135)]` filled `rgba(217,89,38,0.12)`; C polygon `[(334,135), (607,250), (293,250)]` filled `rgba(0,131,0,0.12)`.
- **Triple point:** 6px `#1a5276` dot at (334, 135) with 11px `#1a5276` label above: "same distance to all 3 shops".
- **Houses (6px dots colored by nearest shop):** blue at (150, 120), orange at (540, 100), green at (400, 200) and (360, 170).
- **Annotation (bold 13px, `#1a5276`, near x=90, y=230):** "every wall is halfway between two shops".
- **Caption (12px `#444`, bottom right):** "illustrative town — shop and house positions invented".

## Two Shops on a Grid: Finding the Wall by Hand

**Tags:** `worked example` (blue), `perpendicular bisector` (green)

- **Setup** — put shop A at block (2, 2) and shop B at block (8, 2) on the town's street grid
- **House 1 at (4, 4)** — distance to A is √(2²+2²) = 2.8 blocks, to B is √(4²+2²) = 4.5 blocks: A delivers
- **House 2 at (6, 1)** — distance to A is √(4²+1²) = 4.1 blocks, to B is √(2²+1²) = 2.2 blocks: B delivers
- **The wall** — points equally far from both shops form the vertical line x = 5, exactly halfway between 2 and 8
- **The name** — that halfway line is the perpendicular bisector of the segment joining A and B; every Voronoi wall is one

*Example (italic):* Check any point on x = 5, say (5, 3): distance to A is √(3²+1²) = 3.2 blocks and to B is √(3²+1²) = 3.2 blocks — a perfect tie.

**Key point:** You can rebuild a Voronoi diagram with nothing but the distance formula — compute two distances per house, and the walls appear where the distances tie.

### Visualization (canvas `c2`, 720×300)

Single-panel grid map: two shops on a block grid, the vertical bisector wall at x = 5, and the two worked-example houses with their distance lines and labels.

- **Title (bold 15px, `#1a5276`, top center):** "Two Shops: the Wall Sits Exactly Halfway".
- **Axes:** origin x=60, baseline y=250; x = blocks 0 to 10 (60 px per block, so block b is at pixel 60 + 60b), y = blocks 0 to 6 (33 px per block, block b at pixel 250 − 33b); 12px `#444` tick labels "0"–"10" on x and "0"–"6" on y; light `#e5e9ef` gridlines at every block.
- **Shops (squares 12px, bold 12px labels):** A blue `#2a78d6` at block (2, 2) = pixel (180, 184), label "shop A (2,2)"; B orange `#d95926` at block (8, 2) = pixel (540, 184), label "shop B (8,2)".
- **The wall:** vertical dashed `#1a5276` (dash 6/4) 2px line at x = 5 blocks (pixel 360) from y=50 to the baseline.
- **House 1:** 7px blue dot at block (4, 4) = pixel (300, 118); solid blue 2px line to A with bold 12px blue label "2.8"; dashed (dash 4/3) `#6b7280` 1.5px line to B with 12px `#6b7280` label "4.5".
- **House 2:** 7px orange dot at block (6, 1) = pixel (420, 217); solid orange 2px line to B with bold 12px orange label "2.2"; dashed `#6b7280` 1.5px line to A with 12px `#6b7280` label "4.1".
- **Annotation (bold 13px, `#1a5276`, near x=370, y=70):** "the wall: x = 5 — ties on both sides".
- **Caption (12px `#444`, bottom right):** "distances in blocks, straight-line".

## Where a Data Scientist Meets These Maps

**Tags:** `where it's used` (blue), `1-NN` (green), `k-means` (orange)

- **1-NN classifier** — its decision regions ARE the Voronoi cells of the training points; predict = find your cell
- **k-means step** — assigning every point to its nearest centroid is drawing the Voronoi map of the centroids
- **Coverage planning** — nearest hospital, cell tower, or warehouse territories are Voronoi cells on real maps
- **Filling gaps** — nearest-neighbor interpolation copies each unmeasured spot's value from its nearest sensor
- **Meshes** — Delaunay triangulations build the well-shaped triangle meshes behind terrain models and simulations

*Example (italic):* A 1-NN spam filter with 10,000 labeled emails is secretly a 10,000-cell Voronoi diagram — a new email is labeled by whichever cell it lands in.

**Key point:** Every "assign to the nearest" step in ML — 1-NN prediction, the k-means E-step, nearest-sensor fill — is a Voronoi diagram whether you drew it or not.

### Visualization (canvas `c3`, 720×300)

Single-panel scatter: four labeled training fruits in a two-feature space, the vertical 1-NN decision boundary between the classes, and one new unlabeled fruit being classified by its nearest neighbor.

- **Title (bold 15px, `#1a5276`, top center):** "1-NN Classifier: Decision Regions Are Voronoi Cells".
- **Frame:** 1px `#e5e9ef` rectangle from (60, 50) to (660, 250); 12px `#6b7280` axis captions "weight →" centered below the frame and "length ↑" beside the left edge; no numeric ticks (features on arbitrary scales).
- **Training points (8px dots, 12px labels):** apples blue `#2a78d6` at (200, 100) and (240, 220), each labeled "apple"; lemons orange `#d95926` at (500, 100) and (460, 220), each labeled "lemon". The two classes mirror each other about x=350, so the exact 1-NN boundary is vertical.
- **Region tints:** left of the boundary filled `rgba(42,120,214,0.10)`, right filled `rgba(217,89,38,0.08)`.
- **Decision boundary:** vertical dashed `#1a5276` (dash 6/4) 2px line at x=350 from y=50 to y=250; 12px `#1a5276` label along it: "1-NN boundary = Voronoi wall".
- **New point:** 8px violet `#4a3aa7` dot at (320, 160) labeled "new fruit" (bold 12px violet); violet 2px arrow from it to the apple at (240, 220), its nearest labeled point.
- **Annotation (bold 12px violet `#4a3aa7`, near x=100, y=75):** two lines: "nearest labeled fruit is an apple" / "→ call it an apple".
- **Caption (12px `#444`, bottom right):** "illustrative — feature values invented".

## Voronoi and Delaunay Are One Map Drawn Two Ways

**Tags:** `common mistake` (red), `duality` (orange)

- **The twin** — draw a line between two shops whenever their territories share a wall: that is the Delaunay triangulation
- **Duality** — Voronoi cells and Delaunay triangles carry the same information; each drawing rebuilds the other
- **Empty circle** — the circle through a Delaunay triangle's three shops contains no other shop inside it
- **Swapped corners** — each Voronoi triple point is the center of one such circle; the shops are the triangle's corners
- **The mistake** — assuming a shop's Delaunay neighbors are simply its closest few shops; touching territories decide, not raw distance

*Example (italic):* A far-away shop can still be a Delaunay neighbor if the two territories share a wall, while a nearer shop hidden behind another one may not be.

**Common mistake:** Treating Delaunay edges as "the k nearest points". Delaunay connects territory neighbors — your single nearest point is always among them, but the rest are decided by adjacency, not by a distance cutoff.

### Visualization (canvas `c4`, 720×300)

Single-panel overlay on the same three shops as `c1`: the Voronoi walls dashed, the Delaunay triangle solid, and the empty circumcircle centered on the triple point.

- **Title (bold 15px, `#1a5276`, top center):** "Same Three Shops, Two Drawings: Voronoi (dashed) + Delaunay (solid)".
- **Map frame:** 1px `#e5e9ef` rectangle from (60, 50) to (660, 250), same coordinates as `c1`.
- **Shops:** same positions as `c1` — A blue `#2a78d6` (240, 160), B orange `#d95926` (430, 120), C green `#008300` (390, 215); 12px squares, bold 12px labels "A", "B", "C".
- **Voronoi walls (dashed `#6b7280`, dash 5/4, 1.5px):** from the triple point (334, 135) to (316, 50), to (293, 250), and to (607, 250); 11px `#6b7280` label "Voronoi walls" near (300, 240).
- **Delaunay triangle:** solid green `#008300` 2.5px lines A–B, B–C, C–A; 12px green label "Delaunay edges" near the midpoint of A–B; note each solid edge crosses exactly one dashed wall.
- **Circumcircle:** dashed orange `#d95926` (dash 4/3) 1.5px circle centered at (334, 135) with radius 97, passing through all three shops; 6px `#1a5276` dot at the center.
- **Annotation (bold 12px orange `#d95926`, near x=480, y=70):** two lines: "empty circle: no 4th shop inside —" / "its center is the Voronoi triple point".
- **Caption (12px `#444`, bottom right):** "illustrative — same shop positions as the first map".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all shop, house, wall, polygon, and circle coordinates are the hardcoded pixel values above (no randomness). The `c1`/`c4` geometry is internally consistent: (334, 135) is equidistant (≈97 px) from the three shops, so the walls, triple point, and circumcircle all agree. The `c2` distances in the text (2.8, 4.5, 4.1, 2.2, 3.2 blocks) are true Euclidean distances for the stated block coordinates and must match the chart labels.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
