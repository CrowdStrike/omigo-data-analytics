# PCA (Principal Component Analysis)

**Page type:** detail page (two-column obj-table layout: text left 42%, canvas right 58%, one row per section)
**HTML title tag:** PCA - ML Assumptions

**Subtitle:** Finds directions of maximum variance — assumes linear structure and meaningful variance

## What It Does

- Finds orthogonal directions (principal components) that capture maximum variance in the data. Projects high-dimensional data onto fewer dimensions while preserving as much information as possible.
- **Best For:** Dimensionality reduction before ML modeling, visualization of high-dimensional data in 2D/3D, noise removal and data compression
- **Data:** Continuous numeric features, high-dimensional datasets, requires feature scaling.

### Visualization (canvas `c0`, 720×300)

Two-panel diagram: parallel-coordinates plot of 6 features reduced via PCA arrow to a 2D PC scatter.

- **Left panel (title bold `#1a5276`):** "Original: 6 Features". Six vertical gray `#ccc` axes labeled "Age", "Income", "BMI", "BP", "Chol", "Gluc" (labels `#333`). 8 patients drawn as polylines (width 1.5) with normalized values:
  - [0.7,0.3,0.5,0.6,0.4,0.8], [0.8,0.4,0.6,0.7,0.5,0.7], [0.6,0.2,0.4,0.5,0.3,0.9] in greens `rgba(39,174,96,0.7)`/`0.6`/`0.7`;
  - [0.3,0.8,0.7,0.3,0.8,0.2], [0.2,0.9,0.8,0.4,0.9,0.3], [0.4,0.7,0.6,0.2,0.7,0.4] in reds `rgba(231,76,60,0.7)`/`0.6`/`0.7`;
  - [0.5,0.5,0.5,0.5,0.6,0.5] in orange `rgba(230,126,34,0.6)`; [0.9,0.1,0.3,0.8,0.2,0.6] in blue `rgba(26,82,118,0.6)`.
- **Center:** blue `#1a5276` arrow between panels labeled bold "PCA".
- **Right panel (title bold `#1a5276`):** "Reduced: 2 PCs (85% var)". Axes labeled "PC1 (62%)" (x) and rotated "PC2 (23%)" (y). Projected points (radius 6, same colors as their lines) at fractional coordinates: (0.25,0.65),(0.30,0.70),(0.20,0.75) green cluster; (0.75,0.30),(0.82,0.25),(0.70,0.35) red cluster; (0.50,0.50) orange; (0.15,0.55) blue. Dashed cluster ellipses (dash 4/3): green `rgba(39,174,96,0.6)` around the green cluster, red `rgba(231,76,60,0.6)` around the red cluster.
- **Bottom caption (`#333`, center):** "6D → 2D: clusters visible, 85% information preserved".

## Assumes Linear Relationships

- PCA finds linear combinations of features. Non-linear structure (curves, spirals, concentric rings) is invisible — variance along a curve gets split across multiple principal components, destroying the true manifold.
- **Breaks:** Two spirals or concentric classes project onto a line through the middle, mixing classes completely. Curved manifolds get shattered into noise.
- **Verify:** Plot data in top 2 PCs — if classes still overlap but are visually separable in original space, linearity assumption fails.
- **Fix:** Kernel PCA, t-SNE/UMAP for visualization, autoencoders for non-linear reduction.

### Visualization (canvas `c1`, 720×340)

Two-spiral dataset projected by PCA onto a 1D strip where classes intermix.

- **Titles (bold 14px `#1a5276`):** "Original: Two Spirals (Separable)" (left, centered at x=170) and "PCA Projection (Mixed)" (right, centered at x=540).
- **Left:** two interleaved spirals centered at (160,165), 50 points each: angle `t = (i/50)·3π`, radius `r = 15 + 12t`, plotted at `(cx + cos(t)·r·0.35, cy + sin(t)·r·0.35)` for Class A (green `rgba(39,174,96,0.85)`) and phase-shifted by π for Class B (red `rgba(231,76,60,0.85)`); dot radius 3.5.
- **Center:** gray `#555` arrow (320→370 at y=165) labeled "PCA".
- **Right:** horizontal 1D projection strip (gray `#bbb` line from x=410, width 250, y=165). Each spiral point projected to `0.5 + cos(t)·r·0.35/200` along the strip (Class A slightly above the line, Class B slightly below, jittered by `(i%5)·3.5`); green and red points fully intermixed. Labels: bold red centered "Classes completely overlapping"; gray "PC1 axis (max variance direction)".
- **Legend:** green dot "Class A", red dot "Class B" (text `#333`).
- **Bottom message (`#1a5276`, center):** "Non-linear separation is invisible to PCA — use Kernel PCA or UMAP".

## Variance ≠ Importance

- PCA ranks directions by variance. But high variance often means noise (batch effects, measurement error), while the real signal may live in low-variance directions that get discarded when you keep only the top-k PCs.
- **Breaks:** Gene expression: batch effects (technical noise) have 10× the variance of cancer signal genes. PCA puts batch noise in PC1–PC3 and discards the cancer signal in PC47.
- **Verify:** Check if top PCs correlate with the target variable. If PC1–3 have zero predictive power, the signal is buried.
- **Fix:** Supervised PCA (use target to select features first), partial least squares, or factor analysis with rotation.

### Visualization (canvas `c2`, 720×340)

Paired horizontal bar chart: variance ranking vs predictive power for 5 features.

- **Title (bold 14px `#1a5276`, top center):** "Variance Ranking vs Predictive Power".
- **Column headers (bold gray `#555`):** "Variance (PCA rank)" (left) and "Predictive Power" (right).
- **Rows (bar height 36, max bar width 220; noise rows red `rgba(231,76,60,0.55)` fill with `#e74c3c` stroke and red name labels, signal rows green `rgba(39,174,96,0.55)` fill with `#27ae60` stroke and green name labels; percentage value printed after each bar in `#333`):**

| Feature | Variance | Predictive power | Type |
|---|---|---|---|
| Batch Effect | 92% | 2% | noise |
| Lab Instrument | 78% | 5% | noise |
| Day of Week | 65% | 1% | noise |
| Gene BRCA1 | 8% | 85% | signal |
| Gene TP53 | 5% | 72% | signal |

- **Annotations:** dashed gray `#999` cut-line (dash 4/3) below the third row; red text "← PCA keeps (noise)" beside the top rows, green text "← PCA discards (signal)" beside the bottom rows.
- **Bottom message (`#1a5276`, center):** "High variance ≠ high signal — PCA can discard the most important features".

## Requires Feature Scaling

- PCA maximizes variance in raw units. A feature measured in millions (income) dominates one measured in single digits (age) purely due to scale — not information content. Without standardization, PCA just finds the largest-scale feature.
- **Breaks:** Income (0–1M) becomes PC1 at ~100% explained variance. Age (0–100), BMI (15–45), and blood pressure (60–180) are all noise in PC2+.
- **Verify:** Check feature variances before PCA — if they span orders of magnitude, scaling is mandatory.
- **Fix:** StandardScaler (z-score) before PCA. Use correlation matrix PCA instead of covariance matrix PCA.

### Visualization (canvas `c3`, 720×340)

Side-by-side scatter panels (300×250, `#ddd` borders): unscaled elongated cloud vs scaled circular cloud with PC arrows.

- **Titles (bold 14px `#1a5276`):** "Without Scaling" (left) and "With Scaling (z-score)" (right).
- **Left panel:** axis labels (`#444`): x "Income ($0–$1M)", rotated y "Age (0–100)". 60 blue `rgba(26,82,118,0.6)` points (radius 3) forming a horizontally elongated band: `px` sweeps across the panel with `sin(i·0.7)·8` jitter, `py` near vertical center with `sin(i·1.3)·10` jitter. PC1 arrow: horizontal red `#e74c3c` line (width 2.5) through the middle, labeled bold red "PC1 = Income only (99.9%)".
- **Right panel:** axis labels: x "Feature 1 (standardized)", rotated y "Feature 2 (standardized)". 60 blue points in a roughly circular cloud: angle `(i/60)·2π`, radius `30 + (i%7)·12`, scaled ×0.85 horizontally and ×0.7 vertically. PC1 arrow: green `#27ae60` diagonal (width 2.5) from lower-left to upper-right, labeled bold green "PC1 captures both (62%)". PC2 arrow: dashed blue `#1a5276` perpendicular diagonal (dash 5/4, width 1.5), labeled "PC2 (38%)".
- **Bottom message (`#1a5276`, center):** "Without scaling: largest-unit feature = PC1 regardless of information".

## Assumes Continuous Features

- Binary and one-hot encoded features create artificial directions in PC space. A single categorical with 50 categories contributes 50 binary dimensions that can dominate the top PCs entirely, even if the categorical is uninformative.
- **Breaks:** One-hot encoding of zip code (1000 categories) → PC1–PC5 are all "zip code directions," drowning out meaningful continuous features.
- **Verify:** Examine loadings of top PCs — if dominated by indicator variables, categoricals are distorting.
- **Fix:** MCA (Multiple Correspondence Analysis) for categoricals, FAMD for mixed data, or entity embeddings before PCA.

### Visualization (canvas `c4`, 720×340)

Stacked horizontal bars showing the share of each PC consumed by categorical vs continuous features.

- **Title (bold 14px `#1a5276`, top center):** "PC Space Consumed by Feature Type".
- **Legend:** red swatch `rgba(231,76,60,0.7)` "One-hot categoricals (zip: 1000 cols)" (text `#e74c3c`); green swatch `rgba(39,174,96,0.75)` "Continuous (5 cols)" (text `#27ae60`).
- **Bars (width 440, height 38, gap 12; categorical portion red `rgba(231,76,60,0.55)` with `#e74c3c` stroke, continuous portion green `rgba(39,174,96,0.55)` with `#27ae60` stroke; in-bar percentage labels in `#c0392b`/`#1e8449` when segment > 30%):**

| PC | Categorical | Continuous | Variance explained (right annotation) |
|---|---|---|---|
| PC1 | 85% | 15% | 34% var |
| PC2 | 78% | 22% | 22% var |
| PC3 | 72% | 28% | 15% var |
| PC4 | 45% | 55% | 8% var |
| PC5 | 20% | 80% | 5% var |

- **Bottom message (`#1a5276`, center):** "1000 dummy columns dominate 5 real features — use MCA for categoricals".

## Interpretability Collapses

- Each PC is a weighted combination of all original features: PC1 = 0.3×age + 0.7×income − 0.2×cholesterol + ... This has no domain meaning. You cannot explain to a doctor what "move along PC2" means clinically.
- **Breaks:** Regulatory contexts requiring feature-level explanations. Model debugging — if PC3 drives a prediction, you cannot identify which input feature to fix.
- **Verify:** Check if stakeholders need feature-level explanations. If yes, PCA's opacity is a blocker.
- **Fix:** Sparse PCA (forces near-zero loadings), varimax rotation, or skip PCA and use L1 feature selection instead.

### Visualization (canvas `c5`, 720×340)

Heatmap of PC loadings (7 features × 3 PCs) with "???" interpretation row.

- **Title (bold 14px `#1a5276`, top center):** "PC Loadings Matrix — What Does Each PC \"Mean\"?".
- **Rows (features, labels `#333`):** Age, Income, BMI, BP, Cholest., Glucose, Smoking. **Columns (bold `#1a5276` headers):** PC1, PC2, PC3. Cells 90×32.
- **Loadings matrix (positive cells blue `rgba(26,82,118, |v|·0.6)`, negative cells red `rgba(231,76,60, |v|·0.6)`; value printed in each cell with sign, white text when |v|>0.4 else `#333`):**

| Feature | PC1 | PC2 | PC3 |
|---|---|---|---|
| Age | +0.35 | −0.42 | +0.15 |
| Income | +0.72 | +0.18 | −0.31 |
| BMI | −0.28 | +0.55 | +0.44 |
| BP | +0.41 | +0.38 | −0.52 |
| Cholest. | −0.15 | −0.62 | +0.28 |
| Glucose | +0.52 | +0.22 | +0.67 |
| Smoking | −0.38 | +0.45 | −0.18 |

- **Below matrix:** bold orange `#e67e22` "???" under each PC column; orange centered line: "PC1 = \"mostly income + glucose − smoking\"  →  No clinical meaning".
- **Bottom message (`#1a5276`, center):** "Mixed loadings destroy interpretability — use Sparse PCA or feature selection".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (42%) holds `.obj-title` + `.obj-desc` paragraph + `.obj-detail` lines, right `<td>` (58%, centered) holds the canvas. Even rows have background `#fafcfe`.
- **Detail-line labels:** "Breaks:" uses `<span class="bad">` (red `#e74c3c`, weight 600); "Verify:" uses `<span class="tag tag-check">` (background `#eafaf1`, text `#1e8449`); "Fix:" uses `<span class="tag tag-fix">` (background `#fef9e7`, text `#b7950b`). `.tag` here is weight 600, padding 1px 6px, radius 3px, 0.82em.
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 unstyled browser default (this page's style block has no h1 rule — no colored border under the title); subtitle `#666` 0.95rem; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600 `#1a5276`; `.obj-desc` 0.9em `#333`; `.obj-detail` 0.85em `#444`, margin 4px 0. No nav bar, no back/home links.
- **Canvas:** intrinsic `width`/`height` attributes as given per chart (c0 is 720×300, c1–c5 are 720×340); scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper; `canvas { display:block; margin:0 auto; }`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#555`/`#444`/`#333`.
