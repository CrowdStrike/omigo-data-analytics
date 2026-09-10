# Matrices as Transformations

**Page type:** detail page (tutorial page: h1 + subtitle, 4 `.card-section` blocks each with h2 and a two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Matrices as Transformations

**Subtitle:** A matrix is a machine that stretches, rotates, or flips every point in space at once

## One Edit Button Moves Every Pixel: the Stretch

**Tags:** `core idea` (blue), `running example` (green), `scaling` (blue)

- **The photo** — a house drawing made of 5 corner points, like (2, 0) and (1, 3)
- **The edit** — "make it twice as wide" must move every point by the same recipe
- **The recipe** — new x = 2×x, new y = y — written as the 2×2 grid [[2, 0], [0, 1]]
- **Apply it** — (2, 0) → (4, 0), (1, 3) → (2, 3): the whole house widens at once
- **That's a matrix** — a small grid of numbers that IS the edit, not a description of it

*Example:* Dragging a photo's side handle in any editor multiplies every pixel position by exactly this kind of matrix.

**Key point:** A matrix is one rule applied to every point in space simultaneously — stretch, rotate, flip, shear. The grid of numbers stores the rule; multiplying applies it.

### Visualization (canvas `c1`, 720×300)

House outline before and after the [[2,0],[0,1]] stretch, with the matrix shown as a box.

- **Title (bold 15px, `#1a5276`, top center):** "[[2, 0], [0, 1]] Applied to All 5 Corners at Once".
- **Plane:** origin at (70, 250), 420×185 plot; x 0..5 (integer ticks), y 0..3.5 (ticks 1–3); gridlines `#e5e9ef`, gray `#999` axes, 12px `#444` tick labels.
- **House polygon (5 corners):** `[[0,0], [2,0], [2,2], [1,3], [0,2]]` — drawn closed with 3.5px corner dots.
  - Before: blue `#2a78d6` 2px outline, fill rgba(42,120,214,0.10); label bold 12px blue "before" near (0.8, 2.6).
  - After (every x doubled): orange `#d95926` 2px outline, fill rgba(217,89,38,0.08); labels bold 12px orange: "after: every x doubled" and "(1,3) → (2,3)".
- **Matrix box (right, at 545, 80, 130×70):** fill rgba(74,58,167,0.08), 2px violet `#4a3aa7` border; bold 16px violet rows "2   0" / "0   1"; below (bold 12px violet): "the edit, stored" / "as 4 numbers"; then 12px `#444`: "new x = 2x + 0y" / "new y = 0x + 1y".

## Track One Corner by Hand: Stretch, Then a Rotation

**Tags:** `worked example` (green), `rotation` (blue)

- **The recipe** — matrix × point: each output slot is (row) × (point), multiplied and added
- **Stretch (3, 2)** — [[2, 0], [0, 1]]: x′ = 2×3 + 0×2 = 6, y′ = 0×3 + 1×2 = 2
- **Result** — (3, 2) → (6, 2): x doubled, y untouched, as the button promised
- **Rotate 90°** — [[0, −1], [1, 0]]: x′ = 0×3 − 1×2 = −2, y′ = 1×3 + 0×2 = 3
- **Result** — (3, 2) → (−2, 3): same distance from center, turned a quarter circle

*Example:* Check the rotation with a ruler: both (3, 2) and (−2, 3) sit √13 ≈ 3.6 from the origin.

**Key point:** Matrix-times-point is two multiply-and-adds — row 1 makes the new x, row 2 makes the new y. Anyone can verify a transformation with one tracked point.

### Visualization (canvas `c2`, 720×300)

Two panels: the same point through two different machines — stretch (3,2)→(6,2) and rotation (3,2)→(−2,3).

- **Title (bold 15px, `#1a5276`, top center):** "The Same Point Through Two Different Machines".
- **Divider:** vertical dashed `#bdc3c7` line at x=360.
- **Left panel — "stretch [[2,0],[0,1]]"** (bold 13px `#1a5276` title): plane x 0..7, y 0..4 at (55, 235), 270×150, `#e5e9ef` gridlines. Blue `#2a78d6` 2.5px arrow from origin to (3, 2) labeled "(3, 2)"; orange `#d95926` arrow to (6, 2) labeled "(6, 2)". Below the plot: bold 12px orange "x′ = 2×3+0×2 = 6"; 12px `#444` "y′ = 0×3+1×2 = 2".
- **Right panel — "rotate 90° [[0,−1],[1,0]]"** (bold 13px title): plane x −3..4, y 0..4 at (395, 235), 290×150, y-axis drawn at x=0. Blue arrow to (3, 2) labeled "(3, 2)"; green `#008300` arrow to (−2, 3) labeled "(−2, 3)"; dashed mute `#6b7280` arc between the two arrows showing the quarter turn. Below: bold 12px green "x′ = 0×3−1×2 = −2"; 12px `#444` "y′ = 1×3+0×2 = 3".
- **Bottom takeaway (bold 13px magenta `#d55181`, centered, y=296):** "row 1 builds the new x, row 2 builds the new y".

## Where the Edit Buttons Show Up in Data Work

**Tags:** `where it's used` (blue), `linear maps` (blue)

- **Standardizing** — dividing each feature by its spread is a stretch matrix on the data cloud
- **PCA** — finds the rotation that turns a tilted data cloud until its axes line up
- **Neural layers** — each layer is a matrix: it rotates and stretches inputs before a squash
- **Embeddings** — "project 300 dims to 2 for plotting" is a wide, flattening matrix
- **Without the idea** — PCA output reads as mystery numbers instead of "the photo, rotated"

*Example:* The illustrative cloud below is 14 points; PCA's rotation lays its long diagonal flat along one axis.

**Key point:** Preprocessing and model layers are chains of these edits applied to data points instead of pixels — same machinery, different photo.

### Visualization (canvas `c3`, 720×300)

PCA as rotation — a tilted point cloud vs the same cloud rotated flat (illustrative).

- **Title (bold 15px, `#1a5276`, top center):** "PCA Is a Rotation Matrix: the Tilted Cloud Laid Flat (illustrative)".
- **Divider:** vertical dashed `#bdc3c7` line at x=360.
- **Hardcoded tilted cloud (14 points, roughly along y = x):** `[-2.0,-1.7], [-1.6,-1.9], [-1.2,-0.8], [-0.9,-1.2], [-0.5,-0.2], [-0.2,-0.6], [0.1,0.3], [0.4,0.1], [0.8,1.1], [1.1,0.7], [1.4,1.6], [1.7,1.3], [2.0,2.2], [2.3,1.9]`.
- **Rotated cloud:** each point mapped by the −45° rotation `[c·x + c·y, −c·x + c·y]` with c = √½.
- **Panels:** each 260px wide with crosshair axes (gray `#999`) centered at y=150, scale 38px per unit, 4.5px dots.
  - Left (x=55), blue `#2a78d6` dots, panel title "raw features: tilted, correlated"; note (bold 12px blue, y=272): "the two columns move together".
  - Right (x=400), aqua `#199e70` dots, panel title "after the PCA rotation"; note (bold 12px aqua): "axis 1 holds nearly all the spread".
- **Between panels:** violet `#4a3aa7` 2.5px arrow from (330, 150) to (388, 150) with arrowhead; bold 12px violet labels "rotate" above and "−45°" below.

## How to Read a Matrix: Look at Its Columns

**Tags:** `common mistake` (red), `rule of thumb` (blue)

- **The stare trap** — reading entries one by one says nothing about what the edit does
- **Column rule** — column 1 is where the step right (1, 0) lands; column 2, the step up (0, 1)
- **Rotation decoded** — [[0, −1], [1, 0]]: (1, 0) → (0, 1) and (0, 1) → (−1, 0): both turned 90°
- **Stretch decoded** — [[2, 0], [0, 1]]: (1, 0) → (2, 0) doubled, (0, 1) stays: widen only
- **Two arrows suffice** — every other point just follows the two unit arrows proportionally

*Example:* Spot a flip instantly: [[−1, 0], [0, 1]] sends the step right to (−1, 0) — mirrored across the vertical axis.

**Common mistake:** Treating a matrix as an opaque block of numbers. Read its columns as "where do the two unit arrows go?" and the geometry is visible at a glance.

### Visualization (canvas `c4`, 720×300)

Read-the-columns diagram: the rotation matrix with colored columns on the left, unit arrows before/after on the right.

- **Title (bold 15px, `#1a5276`, top center):** "The Columns Are Where the Unit Arrows Land".
- **Matrix (left, at 90, 90, 110×88, 2px `#1a5276` border):** column 1 background rgba(0,131,0,0.12), column 2 background rgba(217,89,38,0.12); bold 20px entries — green `#008300` column 1: "0" / "1"; orange `#d95926` column 2: "−1" / "0". Above (bold 12px `#1a5276`): "the 90° rotation matrix". Below, bold 12px: green "column 1:" / "(1,0) lands" / "at (0,1)"; orange "column 2:" / "(0,1) lands" / "at (−1,0)".
- **Plane (right, centered at 470, 165, 85px per unit):** gridlines `#e5e9ef`, gray crosshair axes spanning x −1.4..1.4 and y −0.5..1.15.
  - Before arrows: dashed mute `#6b7280` 2px lines to (1, 0) and (0, 1), labeled 12px "(1,0) before" and "(0,1) before".
  - After arrows: solid 3px with arrowheads — green to (0, ~0.96) labeled bold 12px "col 1 → (0,1)"; orange to (~−0.96, 0) labeled "col 2 → (−1,0)".
- **Bottom takeaway (bold 13px magenta `#d55181`, y=285):** "two arrows tell the whole story: everything turned 90°".

## Regeneration instructions

- **Template:** tutorials topic-page layout (social-graph reference skeleton). `<h1>` (no index number), `.subtitle`, then 4 `.card-section` blocks; each has an `<h2>` with bottom border `2px solid #2980b9` and a `table.layout` with `.text-col` (50%) and `.viz-col` (50%) cells, 12px padding.
- **Text column structure:** `.tags` pill row first, then a `<ul>` of 5 one-line bullets each opening with `<b>bold term</b>` (bold terms colored `#1a5276`), one italic `.example` paragraph, one `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem) whose `<strong>` lead is "Key point:" or "Common mistake:".
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors: blue = bg rgba(26,82,118,0.12)/text `#1a5276`; green = bg rgba(39,174,96,0.15)/text `#27ae60`; red = bg rgba(231,76,60,0.12)/text `#e74c3c`; orange = bg rgba(230,126,34,0.15)/text `#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `2px solid #2980b9` bottom border; subtitle `#666` 0.95rem; h2 1.3rem `#1a5276`; canvases `width:100%`, border `1px solid #e0e0e0`, radius 4px. No nav bar, no back/home links.
- **Canvas:** each 720×300 logical, scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; a shared `arrowHead(ctx,x1,y1,x2,y2,size)` helper draws filled triangular arrowheads. Shared script-scope data: `house = [[0,0],[2,0],[2,2],[1,3],[0,2]]`. All data hardcoded (no `Math.random()`); invented data labeled "illustrative". Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
