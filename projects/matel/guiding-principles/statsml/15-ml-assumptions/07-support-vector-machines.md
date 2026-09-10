# Support Vector Machines

**Page type:** detail page (two-column obj-table layout: text left 42%, canvas right 58%, one row per section)
**HTML title tag:** Support Vector Machines - ML Assumptions

**Subtitle:** Finds the maximum-margin hyperplane — but the margin is only meaningful when features are scaled and distance is meaningful.

## What It Does

- Finds the hyperplane that maximizes the margin (gap) between classes. Uses support vectors — the closest points to the boundary — to define the decision surface. The kernel trick maps data into a higher-dimensional space where a linear separator becomes a non-linear boundary in the original space — without ever computing the transformation explicitly.
- **Best For:** Text classification, image recognition, bioinformatics, small-to-medium datasets with clear margin separation
- **Data:** Binary classification (extended via one-vs-rest). Numeric features, moderate dimensions. Feature scaling mandatory.
- **Kernels:** Linear (hyperplane), RBF/Gaussian (smooth curves, most flexible), Polynomial (degree-d boundaries), Sigmoid (neural-network-like). RBF is the default — γ controls how local/global the boundary is.

### Visualization (canvas `c0`, 720×300)

Conceptual scatter: two classes separated by a curved (RBF) decision boundary with margin band and highlighted support vectors.

- **Title (bold 14px `#1a5276`, top center):** "SVM: Maximum Margin with Non-Linear (RBF) Boundary".
- **Boundary:** S-curve through the plot center, `boundaryX(y) = w/2 + sin((y−h/2)×0.012)×80 + cos((y−40)×0.008)×40`, drawn from y=35 to y=h−40 as a solid `#1a5276` curve, width 3.
- **Margin band:** shaded region ±45px around the boundary in `rgba(26,82,118,0.1)`; margin edges drawn as dashed `#1a5276` curves (dash 5/4, width 1.5).
- **Class A points (blue `rgba(41,128,185,0.75)`, radius 5):** [80,55],[55,85],[110,110],[70,145],[95,175],[45,200],[120,225],[65,255],[130,70],[100,195],[50,135],[140,155],[85,240],[110,50],[60,220].
- **Class B points (red `rgba(231,76,60,0.75)`, radius 5):** [550,50],[580,85],[520,120],[600,155],[540,185],[620,210],[560,240],[590,265],[510,65],[640,130],[570,170],[505,200],[630,90],[545,105],[615,245].
- **Support vectors:** 3 blue points at margin left edge (y=80, 155, 230 with x offsets +5, −2, +3 from boundary−margin) and 3 red points at margin right edge (y=100, 185, 250 with offsets −4, +2, −3 from boundary+margin); each drawn as a normal data point plus an orange highlight ring `#f39c12`, radius 10, width 2.5.
- **Labels:** "Decision Boundary" (bold `#1a5276`) with a short pointer line to the curve near the top; orange (`#e67e22`) double-headed arrow across the margin at y=140 labeled bold "Margin" above it; "Support Vectors" (bold `#f39c12`) with second line "(define the boundary)" to the left of the first support vector.
- **Legend (bottom):** blue dot "Class A", red dot "Class B", orange ring "= Support Vector" (text `#333`).

## Feature Scaling Required

- SVM is **distance-based** — it maximizes the margin between classes in feature space. Features with larger numeric ranges dominate the distance calculation entirely. An unscaled feature with range [0, 200000] makes a feature with range [0, 1] invisible to the optimizer — the margin is determined solely by the large-range feature.
- **Breaks:** Income (0–200K) and age (18–90) unscaled. The SVM boundary is a horizontal line at income=90K. Age has literally zero influence despite being highly predictive.
- **Verify:** Check feature ranges before training, confirm standardization applied
- **Fix:** StandardScaler (z-score) or MinMaxScaler before SVM; always part of pipeline

### Visualization (canvas `c1`, 720×300)

Side-by-side scatter panels: unscaled data with wrong horizontal boundary vs scaled data with correct diagonal boundary.

- **Data (age, income, class):** class 0 — (25,45000),(30,55000),(35,70000),(28,60000),(40,50000),(33,48000),(22,35000),(38,65000),(45,42000),(27,52000),(32,40000),(36,58000); class 1 — (50,120000),(55,150000),(48,130000),(60,160000),(52,140000),(58,155000),(28,135000),(30,145000),(25,125000),(62,110000),(56,145000),(45,100000). Class 0 red `rgba(231,76,60,0.8)`, class 1 green `rgba(39,174,96,0.85)`, dot radius 4.
- **Left panel ("Unscaled", bold `#1a5276`):** L-shaped gray axes `#bbb`; x positions compressed into a ~40px band (age squished: x = pad+5+(age−18)/50×40), y = income mapped over full height (income/180000). Wrong boundary: horizontal red `#e74c3c` line (width 2.5) at income=95000. Axis labels (gray `#555`): x "age (invisible)", rotated y "income (dominates)".
- **Vertical divider:** light gray `#ddd` line at center.
- **Right panel ("Scaled [0, 1]", bold green `#27ae60`):** same points spread over the full panel width (age mapped across halfW−40). Correct boundary: green `#27ae60` diagonal line (width 2.5) from lower-left to upper-right. Axis labels: x "age", rotated y "income".

## Appropriate Kernel Choice

- The kernel defines what **shape of boundary** the SVM can find. A linear kernel can only find hyperplanes; an RBF kernel finds smooth curved boundaries. Choosing the wrong kernel means the model literally cannot represent the true decision boundary — it's constrained to the wrong family of separators regardless of how much data you have.
- **Breaks:** Circular boundary (disease if BMI between 22–28). Linear SVM draws a straight line through the circle, achieving ~50% accuracy. The true boundary is unreachable with that kernel.
- **Verify:** Cross-validate across kernels (linear, RBF, poly), visualize 2D projections
- **Fix:** Start with RBF (most flexible), tune gamma; use linear only if p >> n

### Visualization (canvas `c2`, 720×300)

Side-by-side panels: same ring-plus-center data classified by linear vs RBF kernel.

- **Data (both panels, centered at panel center, cy = h/2+5):** outer ring (Class B, red `rgba(231,76,60,0.8)`, radius 4) at 16 angles [0, 0.4, 0.8, 1.2, 1.6, 2.0, 2.4, 2.8, 3.2, 3.6, 4.0, 4.4, 4.8, 5.2, 5.6, 6.0] rad with radii [45, 50, 43, 52, 47, 48, 44, 51, 46, 49, 53, 42, 50, 47, 44, 51]; inner cluster (Class A, green `rgba(39,174,96,0.8)`) at offsets [0,0],[8,−5],[−6,7],[4,10],[−9,−3],[12,2],[−4,−8],[7,−9],[−10,5],[3,6].
- **Left panel (title bold red `#e74c3c`):** "Linear Kernel (wrong)". Linear boundary: red `#e74c3c` line (width 2.5) cutting diagonally through the data (from center−70,+30 to center+70,−30). Caption below in red: "~50% accuracy".
- **Vertical divider:** light gray `#ddd` line at center.
- **Right panel (title bold green `#27ae60`):** "RBF Kernel (correct)". Circular boundary: green `#27ae60` circle (radius 30, width 2.5) around the inner cluster. Caption below in green: "~95% accuracy".

## Outlier Sensitivity (Support Vector Distortion)

- The decision boundary is defined by **support vectors** — the closest points to the margin. A single outlier that lands near the boundary becomes a support vector and pulls the entire hyperplane toward it. Unlike tree methods that ignore outliers in most leaves, SVM gives disproportionate influence to boundary-adjacent points.
- **Breaks:** One mislabeled point positioned between the classes becomes a support vector. The margin shrinks to accommodate it, tilting the boundary and misclassifying dozens of correctly-labeled nearby points.
- **Verify:** Inspect support vectors — too many means boundary is fragile; outliers among SVs = problem
- **Fix:** Soften the margin by **decreasing C** (large C ≈ hard margin that bends to fit outliers), remove outliers, or use RBF with tuned gamma to create local boundaries

### Visualization (canvas `c3`, 720×300)

Scatter with two clusters, a highlighted mislabeled outlier, and true vs distorted boundaries.

- **Axes:** L-shaped gray `#bbb` axes; labels gray `#555`: x "Feature 1" (bottom center), rotated y "Feature 2". Data coordinates in a 0–100 space mapped to the plot area (pad 55).
- **Class A (blue `rgba(41,128,185,0.75)`, radius 4), 20 points:** [22,35],[18,45],[25,55],[30,40],[20,60],[28,32],[15,50],[32,48],[24,65],[19,38],[26,42],[21,58],[29,30],[17,52],[23,44],[27,62],[16,40],[31,55],[20,35],[25,50].
- **Class B (green `rgba(39,174,96,0.75)`, radius 4), 20 points:** [62,35],[68,45],[65,55],[70,40],[60,60],[72,32],[75,50],[63,48],[67,65],[74,38],[66,42],[61,58],[69,30],[73,52],[64,44],[71,62],[76,40],[63,55],[70,35],[68,50].
- **Outlier:** blue point `rgba(41,128,185,0.9)` (radius 5) at (65,75) deep in Class B territory, circled by a red `#e74c3c` ring (radius 11, width 2.5).
- **Boundaries:** true boundary — vertical dashed green `#27ae60` line (dash 5/4, width 2) at x=46 from y=10 to y=90; distorted boundary — solid red `#e74c3c` line (width 2.5) from (56,10) to (52,90), tilted toward the outlier.
- **Outlier labels (red, right of outlier):** bold "Outlier", then "(mislabeled blue point)" and "pulls boundary →".
- **Legend (top):** dashed green line sample + "True boundary" (green); solid red line sample + "Distorted by outlier" (red).

## Curse of Dimensionality (Distance Meaningless)

- In very high dimensions, all points become **approximately equidistant** from each other. The "maximum margin" concept relies on some points being close to the boundary — if all distances converge, the margin is arbitrary. SVM's power comes from geometric separation; when geometry collapses in high-D, so does SVM.
- **Breaks:** 10,000 sparse text features. Pairwise distances between all documents converge to the same value. The "maximum margin" separates random noise — not meaningful patterns.
- **Verify:** Plot pairwise distance distribution — if concentrated in narrow band, dimensionality is too high
- **Fix:** Dimensionality reduction first (PCA, feature selection), linear SVM (works better in high-D than RBF)

### Visualization (canvas `c4`, 720×300)

Two overlaid distance-distribution density curves: broad low-D vs narrow high-D spike.

- **Axes:** L-shaped gray `#bbb` axes; x "Pairwise Distance" (0–10, gray `#555` tick labels at 0, 2, 4, 6, 8, 10), rotated y "Frequency"; pad 55.
- **Low-D curve (green):** Gaussian-shaped density `y = 0.5·exp(−(x−4)²/6)` over x 0–10; stroke `#27ae60` width 2, fill `rgba(39,174,96,0.2)` — broad hump centered at 4.
- **High-D curve (red):** narrow spike `y = 0.9·exp(−(x−6.2)²/0.4)` over x 4–8; stroke `#e74c3c` width 2, fill `rgba(231,76,60,0.2)` — tall concentrated peak at 6.2.
- **Legend (top left):** green "Low-D (p=5): distances vary → margin meaningful"; red "High-D (p=10K): all ≈ same → margin arbitrary".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (42%) holds `.obj-title` + `.obj-desc` paragraph + `.obj-detail` lines, right `<td>` (58%, centered) holds the canvas. Even rows have background `#fafcfe`.
- **Detail-line labels:** "Breaks:" uses `<span class="bad">` (red `#e74c3c`, weight 600); "Verify:" uses `<span class="tag tag-check">` (background `#eafaf1`, text `#1e8449`); "Fix:" uses `<span class="tag tag-fix">` (background `#fef9e7`, text `#b7950b`). `.tag` is inline-block, 0.75em, padding 2px 8px, radius 4px.
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px solid `#2980b9` bottom border; subtitle `#666` 0.95rem; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600 `#1a5276`; `.obj-desc` 0.9em `#333`; `.obj-detail` 0.85em `#555`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** intrinsic `width`/`height` attributes as given per chart (all 720×300); scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper; `canvas { display:block; margin:0 auto; }`.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, support-vector orange `#f39c12`, gray text `#555`/`#333`.
