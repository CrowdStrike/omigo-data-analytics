# Linear Discriminant Analysis (LDA)

**Page type:** detail page (two-column obj-table layout: text left 42%, canvas right 58%, one row per assumption)
**HTML title tag:** Linear Discriminant Analysis (LDA) - ML Assumptions

**Subtitle:** Finds the projection that maximizes between-class separation — but only works when classes are Gaussian ellipsoids with identical shape.

## Section 0: What It Does

Finds a linear combination of features that maximally separates classes by maximizing between-class variance relative to within-class variance. Projects high-dimensional data onto a discriminant axis where classes are most distinguishable.

- **Best For:** Face recognition, document classification, biomarker discovery in clinical data
- **Data:** Multi-class classification, continuous features, assumes Gaussian distribution per class with shared covariance structure.

### Visualization (canvas `c0`, 720×300)

Conceptual diagram: two class clouds in 2D with a discriminant axis, plus a right panel showing the classes projected onto LD1.

- **Left scatter:** 25 points per class. Class A cloud centered near (150, 100), filled `rgba(39,174,96,0.7)` (green), radius 5; Class B cloud centered near (350, 180), filled `rgba(41,128,185,0.7)` (blue), radius 5. Positions generated deterministically with cos/sin jitter (±45x, ±35y roughly).
- **Discriminant axis:** solid `#1a5276` line width 2.5 from (80, 60) to (430, 230) with a small arrowhead at the end; bold 13px label "LD1 (discriminant axis)" in `#1a5276` at (432, 233), left-aligned.
- **Class labels (bold 13px, centered):** "Class A" in `#27ae60` at (150, 25); "Class B" in `#2980b9` at (350, 25).
- **Right panel:** vertical gray (`#888`) axis line at x=540 from y=25 to h-20. Class A projected density drawn as horizontal green bars (`rgba(39,174,96,0.75)`, 2.5px tall, every 3px) for y 40–130, Gaussian centered at y=85 (variance divisor 350, max width 70). Class B projected density in `rgba(41,128,185,0.6)` for y 150–260, Gaussian centered at y=205 (divisor 400, max width 70).
- **Decision boundary:** dashed red (`#e74c3c`, dash 5/4, width 2) horizontal segment at y=140 from x=532 to x=620; bold red 13px label "decision boundary" at (543, 155).
- **Captions (13px `#555`):** "Projected onto LD1" centered at (580, h-5); "max(between-class var / within-class var)" centered at (w/2 - 50, h-5).

## Section 1: Multivariate Normality (Per Class)

Each feature must follow a **normal distribution within each class separately** — not overall. LDA models each class as a multivariate Gaussian and places the decision boundary where the two densities cross. If the true distribution is bimodal, skewed, or heavy-tailed within a class, the fitted Gaussian misrepresents where that class actually lives.

- **Breaks:** Age is bimodal in the positive class (young risk + elderly risk). LDA fits one Gaussian centered at mean=52, placing the boundary where neither subgroup lives — missing both systematically.
- **Verify:** Shapiro-Wilk per feature per class, Q-Q plots, Mardia's test for multivariate normality
- **Fix:** QDA, mixture discriminant analysis, or non-parametric classifiers

### Visualization (canvas `c1`, 720×300)

Density curves over Age showing a bimodal truth vs LDA's single-Gaussian fit.

- **Axes:** L-shaped gray (`#bbb`) axes, left padding 50; x-axis "Age" (centered bottom), rotated y-label "Density"; x maps ages 20–80 across the plot; x ticks at 25, 35, 45, 55, 65, 75 in `#555` 13px.
- **Positive class (bimodal truth):** solid green `#27ae60` curve width 2.5, sum of two Gaussians: 0.6·exp(−(x−32)²/50) + 0.5·exp(−(x−68)²/60), with fill `rgba(39,174,96,0.15)` under the curve.
- **LDA assumed density:** dashed red `#e74c3c` (dash 6/4, width 2.5) single Gaussian 0.45·exp(−(x−52)²/200).
- **Negative class:** solid blue `#2980b9` width 2 Gaussian 0.5·exp(−(x−50)²/120).
- **LDA boundary:** vertical dashed gray `#555` line (dash 3/3, width 2) at x=52, bold label "LDA boundary" above it.
- **Missed-zone annotations (bold red 13px):** "← missed" near the x=32 peak; "missed →" near the x=68 peak.
- **Legend (top right, 13px):** green line "Positive class (bimodal truth)"; dashed red line "LDA assumes (single Gaussian)"; blue line "Negative class".

## Section 2: Equal Covariance Matrices (Homoscedasticity)

LDA assumes both classes share the **same variance-covariance structure** — same spread, same orientation, same shape. It pools both covariance matrices into one and uses that to compute the linear boundary. If one class is compact and the other is diffuse, the pooled estimate is wrong for both — the boundary tilts toward the compact class, systematically misclassifying it.

- **Breaks:** Class A is a tight cluster (σ=2), Class B is a spread cloud (σ=12). The pooled σ≈8 makes the boundary too close to A — A gets classified as B at a much higher rate.
- **Verify:** Box's M test, visual comparison of per-class scatter plots
- **Fix:** QDA (fits separate covariance per class), regularized discriminant analysis

### Visualization (canvas `c2`, 720×300)

Two scatter clusters of very different spread with the true boundary vs the tilted pooled LDA boundary.

- **Class A (tight):** center (0.3w, 0.5h); ellipse 40×35 with fill `rgba(39,174,96,0.15)` and stroke `#27ae60` width 1.5; 25 green points (`rgba(39,174,96,0.7)`, radius 3) within radii ~12–40. Bold green label "Class A (σ=2)" below center.
- **Class B (diffuse):** center (0.65w, 0.48h); ellipse 120×90 rotated 0.3 rad, fill `rgba(41,128,185,0.1)`, stroke `#2980b9` width 1.5; 30 blue points (`rgba(41,128,185,0.5)`, radius 3) within radii ~25–115. Bold blue label "Class B (σ=12)" below center.
- **True boundary:** solid green `#27ae60` vertical line width 2 at the midpoint between centers + 20px; 13px label "True boundary" above.
- **LDA pooled boundary:** dashed red `#e74c3c` vertical line (dash 6/4, width 2.5) at Class A center + 55px (much closer to A); 13px label "LDA boundary (pooled)" above.
- **Misclassification zone:** rectangle between the two boundaries filled `rgba(231,76,60,0.08)`, with bold red two-line label "A misclassified" / "as B here" centered in it.

## Section 3: No Multicollinearity (Non-Singular Covariance)

LDA requires **inverting the pooled covariance matrix**. If features are linearly dependent or nearly so (e.g., total = feature_a + feature_b), the matrix becomes singular or ill-conditioned. The inversion either fails outright or produces numerically unstable results — tiny perturbations in data cause wild changes in the discriminant function.

- **Breaks:** Including both "income" and "income_thousands" (perfect linear dependency) → matrix determinant = 0, computation fails. Near-collinearity (r=0.98) → condition number explodes, discriminant coefficients are meaningless.
- **Verify:** Condition number of covariance matrix, VIF, determinant near zero
- **Fix:** Remove redundant features, PCA before LDA, regularized LDA (shrinkage)

### Visualization (canvas `c3`, 720×300)

Side-by-side comparison of a healthy covariance ellipse vs a degenerate (nearly collinear) one, split by a light gray vertical divider at 0.48w.

- **Left panel — "Well-conditioned (κ = 3)"** (bold `#1a5276` title, centered at 0.25w): ellipse 80×55 rotated 0.4 rad centered at (0.25w, 0.5h), fill `rgba(26,82,118,0.1)`, stroke `#1a5276`; 30 points `rgba(26,82,118,0.6)` radius 3 filling the ellipse; solid green `#27ae60` discriminant line width 2.5 through the center (slope down-right to up-left, ±50x/±25y); green caption "Stable discriminant" below.
- **Right panel — "Ill-conditioned (κ = 8400)"** (bold `#e74c3c` title, centered at 0.72w): extremely elongated ellipse 130×8 rotated 0.6 rad centered at (0.72w, 0.5h), fill `rgba(231,76,60,0.08)`, stroke `#e74c3c`; 25 nearly collinear points `rgba(231,76,60,0.65)` along the 0.6-rad axis with ±5px jitter; four short dashed (4/3) candidate discriminant lines at angles −0.3, 0.5, 1.2, −0.8 rad in colors `#e74c3c`, `#e67e22`, `#8e44ad`, `#2980b9`; red caption "Direction flips per sample" below.
- **Bottom annotation (13px `#555`, centered):** "Features nearly collinear → covariance matrix nearly singular → inversion unstable".

## Section 4: Sufficient Sample Size (n > p per class)

Each class needs **more observations than features** to estimate the covariance matrix reliably. With p features, you're estimating p(p+1)/2 unique covariance parameters. If n ≤ p in any class, the sample covariance matrix is rank-deficient — you can't invert it, and even if regularized, the discriminant directions are estimated from noise.

- **Breaks:** 20 features with only 15 samples in the minority class. Covariance matrix has rank ≤ 14. The discriminant function fits noise perfectly — zero training error, random test performance.
- **Verify:** n_min_class > p (strict), ideally n > 5p–10p per class for stability
- **Fix:** Reduce dimensionality first (PCA), regularized/shrinkage LDA, naive Bayes (diagonal covariance)

### Visualization (canvas `c4`, 720×300)

Line chart of training vs test accuracy as a function of samples per class, with an n=p danger boundary.

- **Axes:** L-shaped gray (`#bbb`) axes, padding 55; x-axis "Samples per class (n)" mapping n 5–100; y-axis rotated label "Classification Accuracy" mapping 0.3–0.95; x ticks at 10, 20, 30, 50, 70, 100; y ticks at 0.4, 0.5, 0.6, 0.7, 0.8, 0.9.
- **n=p reference:** vertical dashed red line (`#e74c3c`, dash 4/3, width 1.5) at n=20 with bold red labels "p = 20" above and "(n = p boundary)" below.
- **Zones:** danger zone left of n=20 filled `rgba(231,76,60,0.06)`; safe zone right of it filled `rgba(39,174,96,0.04)`.
- **Training accuracy (red `#e74c3c`, width 2.5):** points (n, acc) = (8, 0.95), (12, 0.93), (15, 0.91), (20, 0.88), (30, 0.84), (50, 0.81), (70, 0.79), (100, 0.78).
- **Test accuracy (blue `#2980b9`, width 2.5):** points (8, 0.38), (12, 0.44), (15, 0.52), (20, 0.60), (30, 0.70), (50, 0.76), (70, 0.78), (100, 0.78).
- **Annotations:** bold orange (`#e67e22`) "← massive overfit gap" near n=10 at accuracy ~0.66; green (`#27ae60`) "converges when n >> p" near n=70 at accuracy ~0.82.
- **Legend (top right):** red line "Training accuracy"; blue line "Test accuracy". Below it in `#555` 13px, three lines: "n < p: rank-deficient Σ" / "→ cannot invert" / "→ perfect train, random test".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width `border-collapse` table, one `<tr>` per section; left `<td>` (42%) holds `.obj-title` + `.obj-desc` paragraph + `.obj-detail` lines, right `<td>` (58%, centered) holds the canvas. Even rows have background `#fafcfe`.
- **Detail-line markup:** "Breaks:" uses `<span class="bad">` (red `#e74c3c`, weight 600); "Verify:" uses `<span class="tag tag-check">` (pill: background `#eafaf1`, color `#1e8449`); "Fix:" uses `<span class="tag tag-fix">` (pill: background `#fef9e7`, color `#b7950b`). Tag pills: inline-block, 0.75em, padding 2px 8px, radius 4px. Also defined: `.good` `#27ae60`, `.warn` `#e67e22`, `.tag-break` (background `#fdeaea`, color `#c0392b`).
- **Page style:** global reset (`* { margin:0; padding:0; box-sizing:border-box }`); body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px solid `#2980b9` bottom border and 8px bottom padding; `.subtitle` `#666` 0.95rem, 32px bottom margin; table cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; `.obj-title` 1.05em weight 600 `#1a5276`; `.obj-desc` 0.9em `#333`; `.obj-detail` 0.85em `#555`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** each canvas declares intrinsic `width="720" height="300"`; `canvas { display:block; margin:0 auto; }`; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper; all charts drawn with vanilla canvas 2D in IIFEs.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, gray text `#555`/`#333`.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
