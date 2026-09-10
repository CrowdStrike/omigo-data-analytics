# Vectors: Arrows and Lists

**Page type:** detail page (tutorial page: h1 + subtitle, 4 `.card-section` blocks each with h2 and a two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Vectors: Arrows and Lists

**Subtitle:** A vector is just a list of numbers you can also draw as an arrow — the basic unit of every dataset row

## One House, Two Numbers, One Arrow

**Tags:** `core idea` (blue), `running example` (green), `feature vectors` (blue)

- **The listing** — house A is 120 sqm and costs $240k: two numbers describe it
- **The list** — write them in a fixed order: [120, 240] — that list is a vector
- **The arrow** — plot size across, price up: the same list is an arrow to (120, 240)
- **Same thing twice** — list for arithmetic, arrow for geometry: pick whichever helps
- **Every row** — house B [150, 280] and house C [60, 150] are arrows on the same map

*Example:* A 3-row listings table with columns size and price is exactly 3 arrows drawn on one plane.

**Key point:** A vector is a list of numbers in a fixed order. Because each number can be a direction on a map, the list is also a point — and the arrow from zero to that point.

### Visualization (canvas `c1`, 720×300)

Split panel: mini data table on the left, the same rows as arrows on a size-price plane on the right.

- **Title (bold 15px, `#1a5276`, top center):** "Each Table Row Is an Arrow on the Size-Price Map".
- **Divider:** vertical dashed `#bdc3c7` line at x=288.
- **Left: mini table** at (30, 66), rows 225×30 with 8px gaps, each outlined 2px in its house color over rgba(0,0,0,0.03) fill. Column headers (bold 12px `#444`): "listing", "sqm", "$k", "vector". Rows:
  - house A — 120 — 240 — "[120, 240]" (blue `#2a78d6`)
  - house B — 150 — 280 — "[150, 280]" (green `#008300`)
  - house C — 60 — 150 — "[60, 150]" (orange `#d95926`)
  - Below (bold 12px violet `#4a3aa7`, centered): "fixed order: [size, price]"; then 12px `#444`: "the list IS the row — nothing lost," / "nothing added".
- **Right: plane** — origin at (350, 258), 300×195 plot; x = size 0–200 (ticks 0, 100, 200), y = price 0–400 ($k, ticks 200, 400); axis labels "size (sqm)" and rotated "price ($k)"; gridlines `#e5e9ef` every 50 sqm / 100 $k, gray `#999` axes.
- **Arrows:** from origin to each house's point, 2.5px lines in the house color with filled arrowheads and 4px endpoint dots; labels bold 12px, e.g. "A (120, 240)", "B (150, 280)", "C (60, 150)".
- **Annotation (bold 13px, magenta `#d55181`, top-left of plot):** "3 rows = 3 arrows".

## Subtract, Add, Measure: House B Minus House A

**Tags:** `worked example` (green), `geometry` (blue)

- **Subtract entry-wise** — B − A = [150 − 120, 280 − 240] = [30, 40]: the upgrade gap
- **Gap as arrow** — [30, 40] is the arrow that walks you from house A to house B
- **Length by Pythagoras** — √(30² + 40²) = √2500 = 50: how far apart A and B sit
- **Add entry-wise** — a renovation adds [20, 60]: A + [20, 60] = [140, 300], a new point
- **Scale** — 0.5 × [120, 240] = [60, 120]: halving a vector halves every entry

*Example:* The 30-40-50 triangle from school is exactly the gap between house A and house B on the plot.

**Key point:** All vector arithmetic is entry-by-entry, and the arrow picture makes it visible — subtraction is the gap, addition is a walk, length is the ruler distance.

### Visualization (canvas `c2`, 720×300)

Difference arrow B − A with the 30-40-50 right triangle, plus the hand-checkable arithmetic on the right.

- **Title (bold 15px, `#1a5276`, top center):** "B − A = [30, 40], and Its Length Is Exactly 50".
- **Zoomed plane:** origin at (90, 252), 380×190 plot; x = size 100–160 (gridlines every 10, labels every 20), y = price 220–320 (gridlines every 20, labels every 40); axis labels "size (sqm)" and rotated "price ($k)".
- **Points:** A (120, 240) blue `#2a78d6` 6px dot labeled "A (120, 240)"; B (150, 280) green `#008300` 6px dot labeled "B (150, 280)".
- **Right-triangle legs:** dashed mute `#6b7280` 1.5px lines A→(right)→B; leg labels bold 12px mute: "+30 sqm" (horizontal), "+$40k" (vertical).
- **Hypotenuse:** violet `#4a3aa7` 3px arrow from A to B with arrowhead, labeled bold 13px "gap [30, 40]".
- **Right column of arithmetic (x=505):**
  - bold 13px violet: "B − A entry-wise:", then 13px `#333`: "[150−120, 280−240]", "= [30, 40]"
  - bold 13px magenta `#d55181`: "length = √(30² + 40²)", "= √2500 = 50"
  - bold 13px green: "renovate A by [20, 60]:", then 13px `#333`: "[120+20, 240+60]", "= [140, 300]"
  - bold 12px orange `#d95926`: "every operation: entry by entry"

## Why Models See Your Rows as Arrows

**Tags:** `where it's used` (blue), `coordinates` (blue)

- **Feature vector** — every row of every dataset is a vector; models never see anything else
- **Similar = nearby** — houses with close size and price land as neighboring points
- **Nearest neighbor** — "find houses like this one" is literally "find the closest arrows"
- **More columns, same idea** — 5 columns means 5D arrows: undrawable, same arithmetic
- **Everything downstream** — distances, clusters, and predictions all start from this picture

*Example:* In the chart, the closest listing to the 115 sqm / $230k query house is house A at distance ~11 (illustrative data).

**Key point:** "Which rows are similar?" becomes "which points are close?" the moment rows are read as vectors — that one translation powers neighbors, clustering, and search.

### Visualization (canvas `c3`, 720×300)

Scatter of listings with a query house and the nearest neighbor circled.

- **Title (bold 15px, `#1a5276`, top center):** "\"Find Similar Houses\" = \"Find the Closest Points\" (illustrative)".
- **Plane:** origin at (90, 252), 500×190 plot; x = size 0–200 (labels every 50), y = price 0–400 (labels every 100); axis labels "size (sqm)" and rotated "price ($k)"; gridlines `#e5e9ef`, gray axes.
- **Listing points (rgba(42,120,214,0.6), 5px dots), hardcoded [size, price]:** `[60,150], [75,170], [90,160], [95,210], [110,200], [120,240], [130,260], [150,280], [160,310], [175,330], [55,120], [140,250], [170,290], [85,190], [105,235]`.
- **Query house:** magenta `#d55181` 7px dot at (115, 230), labeled bold 12px "query (115, 230)".
- **Nearest neighbor:** dashed green `#008300` 16px circle around (120, 240) with a green connector line from the query; bold 13px green annotation: "nearest: (120, 240), distance √(5²+10²) ≈ 11".
- **Annotation (bold 13px violet, upper left):** "5 columns? same idea," / "just a 5D arrow".

## The Trap: the Arrow Depends on Your Units

**Tags:** `common mistake` (red), `watch out` (orange)

- **Change the units** — write price in dollars and house A becomes [120, 240000]
- **Distance breaks** — A vs B gap is [30, 40000]: price contributes 40000², size only 30²
- **One feature rules** — over 99.99% of the distance is price; size might as well not exist
- **In $k instead** — gap [30, 40] gives 900 vs 1600: both features get a real say
- **Fix first** — rescale features to comparable ranges before any distance is computed

*Example:* The same two houses are "almost identical" or "far apart" depending only on whether price is in dollars or thousands.

**Common mistake:** Trusting distances between raw feature vectors. The numbers in the list carry units, so the loudest-unit feature silently decides everything — scale before you measure.

### Visualization (canvas `c4`, 720×300)

Two stacked 100% bars showing each feature's share of the squared A-vs-B distance under two unit choices.

- **Title (bold 15px, `#1a5276`, top center):** "Who Decides the A-vs-B Distance? Depends Only on Units".
- **Bars:** two horizontal 100% bars 420×44 at x=200 (y=78 and y=178), size share in aqua `#199e70` (minimum 2px sliver), price share in orange `#d95926`, `#999` 1px outline. Left-of-bar labels: bold 13px `#1a5276` name + 12px `#666` sub-label.
  1. "price in dollars", sub "gap [30, 40000]" — size share 0.0000006. Annotations: bold 12px orange inside bar: "price: >99.99% of the distance"; bold 12px aqua below: "size: invisible sliver".
  2. "price in $k", sub "gap [30, 40]" — size share 0.36. White bold 12px labels inside bar: "size 36%", "price 64%".
- **Takeaway (bold 13px magenta, centered, y=262):** "same houses, same gap — units alone flip size from mute to a 36% vote".
- **Caption (12px `#444`, centered, y=284):** "share of squared distance: size² = 900 vs price² = 1600 ($k) or 1,600,000,000 (dollars)".

## Regeneration instructions

- **Template:** tutorials topic-page layout (social-graph reference skeleton). `<h1>` (no index number), `.subtitle`, then 4 `.card-section` blocks; each has an `<h2>` with bottom border `2px solid #2980b9` and a `table.layout` with `.text-col` (50%) and `.viz-col` (50%) cells, 12px padding.
- **Text column structure:** `.tags` pill row first, then a `<ul>` of 5 one-line bullets each opening with `<b>bold term</b>` (bold terms colored `#1a5276`), one italic `.example` paragraph, one `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem) whose `<strong>` lead is "Key point:" or "Common mistake:".
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors: blue = bg rgba(26,82,118,0.12)/text `#1a5276`; green = bg rgba(39,174,96,0.15)/text `#27ae60`; red = bg rgba(231,76,60,0.12)/text `#e74c3c`; orange = bg rgba(230,126,34,0.15)/text `#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `2px solid #2980b9` bottom border; subtitle `#666` 0.95rem; h2 1.3rem `#1a5276`; canvases `width:100%`, border `1px solid #e0e0e0`, radius 4px. No nav bar, no back/home links.
- **Canvas:** each 720×300 logical, scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; a shared `arrowHead(ctx,x1,y1,x2,y2,size)` helper draws filled triangular arrowheads. All data hardcoded (no `Math.random()`). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
