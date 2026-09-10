# K-Nearest Neighbors

**Page type:** detail page (two-column obj-table layout: text left 42%, canvas right 58%, one row per section)
**HTML title tag:** K-Nearest Neighbors - ML Assumptions

**Subtitle:** Classifies by local vote — but "local" requires meaningful distance, proper scaling, and enough density to cover the space.

## What It Does

- Classifies new points by majority vote of their K nearest neighbors in feature space. No training phase — stores all data and computes distances at prediction time (lazy learning). Makes no assumptions about the underlying data distribution.
- **Best For:** Recommendation systems, anomaly detection, missing value imputation, prototype-based classification
- **Data:** Numeric features (distance-based). Low-to-moderate dimensions. Feature scaling mandatory. Dense coverage of feature space needed.

### Visualization (canvas `c0`, 720×300)

Scatter with a query point, its K=5 neighborhood circle, and a majority-vote tally.

- **Top caption (gray `#555`, center):** "Query point with K=5 neighborhood".
- **Background points (radius 5):** Class A blue `rgba(41,128,185,0.6)` at (60,60),(100,45),(80,100),(130,75),(50,130),(110,150),(70,180),(140,115),(90,210),(55,240); Class B green `rgba(39,174,96,0.6)` at (480,50),(520,80),(490,130),(560,100),(510,170),(580,140),(450,200),(540,210),(600,60),(630,120),(570,240),(500,250).
- **Query point:** filled `#1a5276` circle (radius 11) at (300,145) with white bold "?" centered inside.
- **Neighborhood:** dashed circle `rgba(26,82,118,0.6)` (radius 70, dash 5/4) around the query.
- **K=5 neighbors (radius 7, with matching stroke ring width 2, connected to query by thin `#ccc` lines):** green class-1 at (265,125),(330,170),(270,175); blue class-0 at (250,145),(340,120).
- **Vote tally (bottom right):** bold "K=5 Vote:" in `#444`, "Green: 3" in `#27ae60`, "Blue: 2" in `#2980b9`; green arrow pointing to a green filled rectangle (80×24) containing white bold "Predict: B".
- **Class labels (bottom):** "Class A" in `#2980b9` under the left cluster, "Class B" in `#27ae60` under the right cluster.

## Feature Scaling Required

- KNN uses **Euclidean distance** (or similar) to find neighbors. Features with larger numeric ranges contribute proportionally more to distance. A feature ranging [0, 200000] completely drowns out one ranging [0, 1] — the small-range feature is effectively invisible to the neighbor search.
- **Breaks:** Income (0–200K) and normalized_score (0–1). Distance is 99.99% determined by income. Two patients with identical scores but $1000 income difference are "far apart"; patients with wildly different scores but same income are "neighbors".
- **Verify:** All features on same scale before fitting
- **Fix:** StandardScaler or MinMaxScaler; always scale in KNN pipeline

### Visualization (canvas `c1`, 720×300)

Side-by-side panels: unscaled neighbor search (income-dominated) vs scaled search where both features count.

- **Left panel (title bold red `#e74c3c`):** "Unscaled". L-shaped gray `#bbb` axes. Query: filled `#1a5276` circle (radius 8) with white "?" at ~40% width, 45% height. Chosen neighbors (radius 5, connected by gray `#888` lines) at offsets (+5,−60) green, (−8,+50) red, (+3,−30) green from the query — vertically spread because income dominates. True neighbor: green point at offset (+50,+5), circled by a dashed red ring (radius 10, dash 3/3) — with red labels to its right: "True neighbor", "(ignored — far on", " income axis)". Axis labels (gray `#555`): x "score (0-1)", rotated y "income (0-200K)".
- **Vertical divider:** light gray `#ddd` line at center.
- **Right panel (title bold green `#27ae60`):** "Scaled [0, 1]". Same-style axes and query point. Correct neighbors at offsets (+30,−20) green, (+25,+15) green, (−20,+25) red, connected by green `#27ae60` lines; dashed green k=3 circle (radius 40, dash 4/3) around the query. Bottom caption in green: "Both features count".

## Curse of Dimensionality

- In high dimensions, all points become **approximately equidistant**. The ratio of nearest-to-farthest distance approaches 1.0. When everything is equally far away, "nearest neighbor" is determined by noise dimensions, not signal. KNN degenerates to random voting.
- **Breaks:** 30 features, 500 samples. Nearest neighbor distance = 4.82, farthest = 5.01. Ratio = 0.96. The "nearest" neighbor is barely closer than the farthest point — selection is essentially random.
- **Verify:** Plot nearest/farthest distance ratio; if > 0.8, dimensionality is too high
- **Fix:** Feature selection, PCA, or switch to tree-based methods unaffected by dimensionality

### Visualization (canvas `c2`, 720×300)

Side-by-side comparison: 2D star plot with distinct near/far distances vs 50D sorted-distance bar chart where all bars are nearly equal.

- **Left panel (title bold `#1a5276`):** "2 dimensions". Query: `#1a5276` circle (radius 7) with white "?" at panel center. Near neighbors: 3 green `rgba(39,174,96,0.8)` points at offsets (20,−15),(15,22),(−18,12) connected by solid green `#27ae60` lines (width 1.5). Far points: 3 red `rgba(231,76,60,0.6)` points at offsets (90,−50),(−80,60),(70,70) connected by dashed red `#e74c3c` lines (dash 3/3, width 1). Bottom caption in green: "nearest/farthest = 0.22".
- **Vertical divider:** light gray `#ddd` line at center.
- **Right panel (title bold red `#e74c3c`):** "50 dimensions". Bar chart on `#f8f8f8` background: 20 sorted bars with heights from `d = 4.5 + i×0.035 + (i%3)×0.01`, scaled as `(d−4.2)/1.2` of the bar area — all nearly the same height. First bar green `rgba(39,174,96,0.7)` (nearest), last bar red `rgba(231,76,60,0.7)` (farthest), rest blue `rgba(41,128,185,0.5)`. Caption below bars (gray `#555`): "All pairwise distances (sorted)"; under it, left-aligned green "min: 4.52" and right-aligned red "max: 5.18". Bottom caption bold red: "nearest/farthest = 0.87".

## Dense Coverage of Feature Space

- KNN requires training data to **densely cover the prediction space**. With d dimensions, you need roughly n ∝ k^d samples to have k neighbors per cell. In sparse regions, the "nearest" neighbors may be far away and from completely different contexts — their label is irrelevant to the query point.
- **Breaks:** 5 features, each with 10 meaningful bins = 100,000 cells. With n=1000, average occupancy = 0.01 per cell. Most predictions come from distant, irrelevant neighbors.
- **Verify:** k-neighbor distances relative to feature spread; large distances = sparse region
- **Fix:** Get more data, reduce dimensions, or use local models (LOESS) that extrapolate

### Visualization (canvas `c3`, 720×300)

10×10 grid showing sparse occupancy: 30 points, 70 empty cells, query landing in an empty cell.

- **Title (bold `#1a5276`, top center):** "Feature space coverage: 2D grid (10×10 = 100 cells, n=30 points)".
- **Grid:** 10 columns × 10 rows of cells with light gray `#e0e0e0` borders (width 0.5).
- **Occupied cells (blue dot `rgba(41,128,185,0.6)`, radius 4, at cell centers), 30 cells (col,row):** [2,3],[4,5],[1,7],[6,2],[8,8],[3,1],[5,6],[7,4],[0,9],[9,0],[2,4],[4,4],[5,5],[3,3],[6,6],[1,1],[8,3],[7,7],[4,2],[6,8],[2,2],[5,3],[3,6],[8,5],[1,4],[7,1],[9,5],[0,3],[6,4],[4,7].
- **Empty cells:** tinted faint red `rgba(231,76,60,0.06)`.
- **Query cell (col 9, row 3):** stronger red tint `rgba(231,76,60,0.15)` with red `#e74c3c` border (width 2); red dot (radius 6) with white bold "?" at its center.
- **Bottom caption (gray `#555`, center):** "70 cells empty — query lands in desert, neighbors are far and irrelevant".

## No Irrelevant Features (All Contribute to Distance)

- Every feature contributes **equally to the distance metric**. If 3 features are informative and 20 are noise, the noise dimensions dominate — the nearest neighbor in the full 23-D space may be far from the query point in the 3 meaningful dimensions. Noise makes true neighbors unreachable.
- **Breaks:** 3 relevant + 47 noise features. True nearest neighbor (in 3D signal space) is ranked 85th in the full 50D space because 47 noise dimensions add random distance. The voted label is from irrelevant points.
- **Verify:** Compare KNN accuracy with all features vs. selected features — large gap = noise dilution
- **Fix:** Feature selection, weighted distance (give higher weight to informative features), LMNN

### Visualization (canvas `c4`, 720×300)

Two ranked lists comparing nearest-neighbor order in signal space vs full noisy space.

- **Title (bold `#1a5276`, top center):** "Nearest neighbor ranking: signal space vs. full space".
- **Left column:** header bold green `#27ae60` "3D Signal Space", subheader gray "(true distance)". Ranked rows (32px tall, `#f8f8f8` background): "1. Patient A (d=0.8)", "2. Patient B (d=1.2)", "3. Patient C (d=1.5)", "4. Patient D (d=2.1)", "5. Patient E (d=3.4)". Top 3 rows tinted `rgba(39,174,96,0.12)` with green `#27ae60` borders.
- **Right column:** header bold red `#e74c3c` "50D Full Space", subheader gray "(noise-dominated)". Ranked rows: "1. Patient F (d=6.1)", "2. Patient G (d=6.2)", "3. Patient H (d=6.3)", "4. Patient D (d=6.4)", "5. Patient A (d=6.5)". The Patient A row is tinted `rgba(231,76,60,0.12)` with a red `#e74c3c` border.
- **Arrow:** red `#e74c3c` arrow (width 2) from left rank-1 row to right rank-5 row, with bold red centered labels between the columns: "Rank 1 → 5" and "(noise pushed it away)".
- **Bottom caption (gray `#555`, center):** "47 noise dimensions add random distance, burying the true nearest neighbor".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (42%) holds `.obj-title` + `.obj-desc` paragraph + `.obj-detail` lines, right `<td>` (58%, centered) holds the canvas. Even rows have background `#fafcfe`.
- **Detail-line labels:** "Breaks:" uses `<span class="bad">` (red `#e74c3c`, weight 600); "Verify:" uses `<span class="tag tag-check">` (background `#eafaf1`, text `#1e8449`); "Fix:" uses `<span class="tag tag-fix">` (background `#fef9e7`, text `#b7950b`). `.tag` is inline-block, 0.75em, padding 2px 8px, radius 4px.
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px solid `#2980b9` bottom border; subtitle `#666` 0.95rem; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600 `#1a5276`; `.obj-desc` 0.9em `#333`; `.obj-detail` 0.85em `#555`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** intrinsic `width`/`height` attributes as given per chart (all 720×300); scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper; `canvas { display:block; margin:0 auto; }`.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#555`/`#333`/`#444`.
