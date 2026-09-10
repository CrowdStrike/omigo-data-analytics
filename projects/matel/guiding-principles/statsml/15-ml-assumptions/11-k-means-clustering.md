# K-Means Clustering

**Page type:** detail page (two-column obj-table layout: text left 42%, canvas right 58%, one row per section)
**HTML title tag:** K-Means Clustering - ML Assumptions

**Subtitle:** Partitions data into K spherical clusters — assumes equal-size, equal-variance blobs

## What It Does

- Iteratively assigns points to their nearest centroid, then recomputes centroids as cluster means. Converges when assignments stabilize. Partitions space into K Voronoi regions.
- **Best For:** Customer segmentation and market analysis, image compression and document grouping, feature discretization and data summarization
- **Data:** Numeric features, requires scaling, moderate dimensions. Unsupervised — no labels needed.

### Visualization (canvas `c0`, 720×300)

Three point clusters with star centroids and dashed Voronoi boundary lines.

- **Clusters (22 points each, radius 4, deterministic ring layout: angle `(i/22)·2π + ci·1.2`, radius `18 + (i%5)·10 + sin(i·2.1+ci)·8`):**
  - Cluster 0 at (20% w, 55% h), points `rgba(26,82,118,0.7)`, star `#1a5276`;
  - Cluster 1 at (50% w, 30% h), points `rgba(39,174,96,0.8)`, star `#27ae60`;
  - Cluster 2 at (75% w, 65% h), points `rgba(230,126,34,0.8)`, star `#e67e22`.
- **Centroids:** 10-point stars (outer radius 12, inner 45%) at each cluster center.
- **Voronoi boundaries:** dashed `rgba(26,82,118,0.6)` lines (dash 5/4, width 1.5) — the perpendicular bisectors between each pair of centroids (0–1 extended ×1.2, 1–2 ×1.0, 0–2 ×0.8).
- **Legend (top right, text `#444`):** small blue star "= centroid"; dashed line sample "= Voronoi boundary".

## Assumes Spherical Clusters

- K-means minimizes Euclidean distance to centroids, which partitions space into Voronoi cells — convex polygons. This only produces correct assignments when clusters are roughly round. Elongated, crescent, or ring-shaped structures get sliced incorrectly.
- **Breaks:** Two concentric rings → K-means draws a straight line through both, splitting each ring in half. Crescent moons → boundary cuts through the middle of each.
- **Verify:** Visualize cluster assignments in 2D. If boundaries cut through visually obvious groups, the shape assumption is violated.
- **Fix:** DBSCAN (density-based, any shape), spectral clustering (graph connectivity), or Gaussian Mixture Models with full covariance.

### Visualization (canvas `c1`, 720×340)

Two-panel comparison: concentric-ring data vs the K=2 K-means result splitting both rings.

- **Titles (bold 14px `#1a5276`):** "True Structure: Rings" (left, centered at x=175) and "K-Means Result (K=2)" (right, centered at x=540).
- **Left (centered at (180,175)):** inner ring — 40 green `rgba(39,174,96,0.8)` points (radius 3.5) at angle `(i/40)·2π`, radius `40 + sin(i·3.1)·6`; outer ring — 50 red `rgba(231,76,60,0.8)` points at angle `(i/50)·2π`, radius `100 + sin(i·2.7)·8`.
- **Center:** gray `#555` arrow (310→360 at y=175).
- **Right (centered at (540,175)):** same ring geometry but recolored by K-means assignment — points with `cos(angle) < 0` blue `rgba(26,82,118,0.7)`, others orange `rgba(230,126,34,0.8)` (left/right split). Vertical dashed red `#e74c3c` boundary line (dash 6/4, width 2.5) at x=540 from y=50 to y=300.
- **Captions:** bold red centered "Both rings split in half!" below the right panel; blue `#1a5276` bottom line: "Voronoi cells cannot wrap around curves".

## Assumes Equal Cluster Sizes

- K-means is biased toward producing clusters with roughly equal membership. A rare subgroup (n=20) next to a large cluster (n=5000) gets absorbed entirely — the centroid of the big cluster pulls all boundary points toward it.
- **Breaks:** Fraud detection: 50 fraud cases near 10,000 normal cases. K-means assigns fraudsters to the normal cluster because it balances membership.
- **Verify:** Compare cluster sizes — if one cluster has 10× more points, K-means may have stolen from the small one.
- **Fix:** GMM (handles unequal mixing proportions), DBSCAN (size-agnostic), or mini-batch K-means with adjusted weights.

### Visualization (canvas `c2`, 720×340)

Large cluster absorbing a small one, with the misplaced K-means boundary vs the actual boundary.

- **Title (bold 14px `#1a5276`, top center):** "Large vs Small Cluster — K-Means Absorbs the Minority".
- **Large cluster (center (250,180)):** filled disc `rgba(26,82,118,0.55)` radius 110, plus 80 blue `rgba(26,82,118,0.65)` dots (radius 2.5) in a deterministic spiral (angle `(i/80)·2π + i·0.3`, radius `15 + (i%11)·9`). Label below in `#1a5276`: "n = 5000".
- **Small cluster (center (480,130)):** 12 red `rgba(231,76,60,0.8)` dots (radius 3.5) at angle `(i/12)·2π`, radius `8 + (i%3)·6`. Label below in `#e74c3c`: "n = 20".
- **K-means boundary:** vertical dashed orange `#e67e22` line (dash 5/3, width 2) at x=380, labeled bold orange "K-Means boundary" underneath.
- **Absorption arrow:** red `#e74c3c` arrow from (440,170) toward (395,185) with red labels "Small cluster absorbed" / "into large cluster".
- **Actual boundary:** dashed green `#27ae60` circle (radius 28, dash 3/3) around the small cluster, labeled green "← actual boundary".
- **Bottom message (`#1a5276`, center):** "K-means equalizes membership — rare groups get swallowed".

## Must Specify K in Advance

- K-means requires you to declare how many clusters exist before seeing the data. There's no principled way to know K — the elbow method and silhouette scores are heuristics that often disagree or give ambiguous answers with no clear "elbow."
- **Breaks:** K=3 on data with 5 real clusters → merges two pairs. K=5 on data with 3 clusters → splits real clusters into fragments.
- **Verify:** Run elbow + silhouette + gap statistic. If they all disagree, the "right K" may not exist for K-means.
- **Fix:** DBSCAN (auto-detects K), X-means (splits until BIC stops improving), or GMM with BIC model selection.

### Visualization (canvas `c3`, 720×340)

Elbow-method line chart with a smooth curve and an ambiguous highlighted zone.

- **Title (bold 14px `#1a5276`, top center):** "The Elbow Method — Often No Clear Answer".
- **Axes:** dark `#333` L-shaped axes (width 1.5); labels `#333`: x "K (number of clusters)" with tick labels 1–10, rotated y "Inertia (within-cluster SS)".
- **Data:** K = [1,2,3,4,5,6,7,8,9,10], normalized inertia = [1.0, 0.72, 0.55, 0.44, 0.37, 0.32, 0.28, 0.25, 0.23, 0.21]; line `#1a5276` width 3 with filled dots (radius 5).
- **Ambiguous zone:** orange band `rgba(230,126,34,0.3)` spanning K=3 to K=5 (±15px); bold orange labels above the plot: "K=3?", "K=4?", "K=5?"; orange centered text inside: "← no clear elbow →".
- **Bottom message (`#1a5276`, center):** "Smooth curves give no definitive K — heuristic disagreement is common".

## Sensitive to Initialization

- K-means converges to a local optimum — different random starting centroids produce different final clusters. Run it 10 times, get 10 different answers. K-means++ helps by spacing initial centroids but still doesn't guarantee the global optimum.
- **Breaks:** Two centroids initialized in the same cluster → one real cluster is split in two and another is merged with its neighbor. Result depends on the random seed.
- **Verify:** Run K-means 20+ times with different seeds. If results vary significantly, the solution is unstable.
- **Fix:** K-means++ initialization, run N restarts and pick lowest inertia, or use global methods (spectral clustering, hierarchical).

### Visualization (canvas `c4`, 720×340)

Four small panels showing the same data clustered differently under four random seeds.

- **Title (bold 14px `#1a5276`, top center):** "Same Data, 4 Random Seeds → 4 Different Clusterings".
- **Data:** 24 points from 3 natural clusters centered at fractional positions (0.25,0.3), (0.7,0.25), (0.5,0.75); 8 points each with deterministic jitter `(sin(i·2.3+ci)·0.08, cos(i·1.7+ci)·0.08)`.
- **Panels:** four 150×130 boxes (`#ddd` borders, 20px gap) rendering the same points with per-seed color sets drawn from `rgba(39,174,96,0.8)` green, `rgba(231,76,60,0.8)` red, `rgba(26,82,118,0.7)` blue, `rgba(230,126,34,0.8)` orange, and per-seed assignments: seed 1 correct (8/8/8 by true cluster); seed 2 some swap (a few cluster-2/3 points exchanged); seed 3 boundary steal (two points assigned across); seed 4 bad split (clusters 2 and 3 scrambled).
- **Panel labels (bold, below each):** "Seed 1 ✓" (`#27ae60`), "Seed 2 ~" (`#e67e22`), "Seed 3 ~" (`#e67e22`), "Seed 4 ✗" (`#e74c3c`); with `#444` inertia captions: "Inertia: 2,340", "Inertia: 2,580", "Inertia: 2,890", "Inertia: 3,410".
- **Bottom summary (`#333`, center):** "Only Seed 1 found the global optimum — others are local minima"; then in `#1a5276`: "Fix: K-means++ init + 20 restarts → pick lowest inertia".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (42%) holds `.obj-title` + `.obj-desc` paragraph + `.obj-detail` lines, right `<td>` (58%, centered) holds the canvas. Even rows have background `#fafcfe`.
- **Detail-line labels:** "Breaks:" uses `<span class="bad">` (red `#e74c3c`, weight 600); "Verify:" uses `<span class="tag tag-check">` (background `#eafaf1`, text `#1e8449`); "Fix:" uses `<span class="tag tag-fix">` (background `#fef9e7`, text `#b7950b`). `.tag` here is weight 600, padding 1px 6px, radius 3px, 0.82em.
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 unstyled browser default (this page's style block has no h1 rule — no colored border under the title); subtitle `#666` 0.95rem; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600 `#1a5276`; `.obj-desc` 0.9em `#333`; `.obj-detail` 0.85em `#444`, margin 4px 0. No nav bar, no back/home links.
- **Canvas:** intrinsic `width`/`height` attributes as given per chart (c0 is 720×300, c1–c4 are 720×340); scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper; `canvas { display:block; margin:0 auto; }`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#555`/`#444`/`#333`.
