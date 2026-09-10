# EM / Gaussian Mixture Models

**Page type:** detail page (two-column obj-table layout: text left 42%, canvas right 58%, one row per assumption)
**HTML title tag:** EM / Gaussian Mixture Models - ML Assumptions

**Subtitle:** Iteratively estimates latent cluster assignments and Gaussian parameters — soft probabilistic clustering

## Section 1: What It Does

Alternates between E-step (compute soft assignments: probability each point belongs to each Gaussian) and M-step (update Gaussian parameters given assignments). Converges to maximum likelihood estimate of mixture parameters.

- **Best For:** Speaker diarization and signal separation, anomaly detection and density estimation, discovering latent subgroups with soft boundaries
- **Data:** Continuous features, overlapping subpopulations, soft cluster boundaries. Unsupervised.

### Visualization (canvas `c0`, 720×300)

Two overlapping 1-D Gaussian density curves with data points colored by soft assignment.

- **Title (bold 14px `#1a5276`, top center):** "GMM: Soft Clustering with Overlapping Gaussians".
- **Axes:** horizontal gray `#bbb` baseline at 55px above the bottom, labeled "Feature value" (`#333`, bottom center). Value range 0–400 mapped across the plot width.
- **Component 1 (blue):** tall/tight Gaussian, height 0.85 of plot, μ=150 (label says μ=150), σ=25; filled `rgba(26,82,118,0.3)`, stroked `#1a5276` width 2.5. Labels above the peak: bold "Component 1" and "μ=150, σ=25, π=0.6" in `#1a5276`.
- **Component 2 (red):** wide/short Gaussian, height 0.45 of plot, μ=280, σ=55; filled `rgba(231,76,60,0.25)`, stroked `#e74c3c` width 2.5. Labels above the peak: bold "Component 2" and "μ=280, σ=55, π=0.4" in `#e74c3c`.
- **Data points:** 25 points at x-values `[90,105,115,125,130,138,145,150,155,160,168,175,185,195,210,230,245,260,275,285,295,310,325,340,355]`, drawn as 4px dots 14px below the baseline; each colored by interpolating between blue rgb(26,82,118) and red rgb(231,76,60) according to P(component 1) computed from the two Gaussian densities — purple in the overlap.
- **Annotation:** orange `#e67e22` text "← overlap: soft assignment →" centered at value ≈ 210, partway up the plot.
- **Caption (bottom center, `#333`):** "Points colored by P(component₁) — purple in overlap = uncertain membership".

## Section 2: Assumes Gaussian Components

Each cluster is modeled as a multivariate Gaussian. Non-Gaussian subpopulations (uniform blobs, skewed groups, ring-shaped clusters) are poorly fit — EM wastes multiple Gaussians trying to approximate one non-Gaussian shape, "discovering" fake subpopulations.

- **Breaks:** Fitting GMM to income data (right-skewed + spike at zero). EM uses 3–4 Gaussians to approximate the skewed tail, reporting fake subpopulations that are distributional artifacts.
- **Verify:** Plot each fitted component against the data it "owns." If a component captures a tail rather than a mode, it's compensating for wrong shape.
- **Fix:** Kernel density estimation, mixture of skew-normals, or DBSCAN for arbitrary shapes.

### Visualization (canvas `c1`, 720×340)

Right-skewed histogram overlaid with the true skewed curve and three fitted Gaussians.

- **Title (bold 14px `#1a5276`, top center):** "Skewed Data → GMM \"Discovers\" Fake Subpopulations".
- **Histogram:** 20 bins with counts `[3, 15, 28, 35, 30, 22, 15, 10, 7, 5, 4, 3, 2, 2, 1, 1, 1, 0, 0, 0]`, max scale 37, bars filled `rgba(26,82,118,0.55)`, drawn from x=60 across width−120, baseline 70px above bottom.
- **True distribution:** dashed green `#27ae60` curve (dash 5/3, width 2.5), gamma-like shape v = (4t)^1.5 × e^(−4t) × 90.
- **GMM fit:** three Gaussians stroked width 2 — red `#e74c3c` (center 0.15, width 0.06, height 32), orange `#e67e22` (center 0.32, width 0.08, height 25), purple `#8e44ad` (center 0.55, width 0.12, height 12).
- **Component labels (top, 13px, centered at each Gaussian's center):** "\"low\"" in red, "\"mid\"" in orange, "\"high\"" in purple.
- **Legend (bottom left):** green "── True: 1 skewed population"; red "── GMM: 3 \"subgroups\" (2 are fake)".
- **X-axis label:** "Income ($)" in `#444`, centered under the baseline.

## Section 3: Correct Number of Components (K)

Like K-means, GMM must be told how many components exist. Too few → merges distinct groups. Too many → splits coherent groups into fragments. BIC/AIC help but often give ambiguous answers with plateaus rather than clear minima.

- **Breaks:** K=2 on true bimodal data works perfectly. K=4 on the same data "discovers" two extra groups in the overlap zone — statistically plausible but biologically meaningless.
- **Verify:** Plot BIC vs K. If the curve flattens (no clear minimum), multiple K values are equally defensible.
- **Fix:** Dirichlet Process GMM (auto-selects K), or model comparison with held-out log-likelihood.

### Visualization (canvas `c2`, 720×340)

Two-panel comparison: K=2 (correct) vs K=4 (overfitting) on the same bimodal histogram.

- **Title (bold 14px `#1a5276`, top center):** "K=2 (Correct) vs K=4 (Overfitting)".
- **Shared histogram data:** 17 bins `[2, 5, 12, 22, 30, 22, 10, 4, 3, 4, 10, 22, 30, 22, 12, 5, 2]`, max scale 32, bars filled `rgba(26,82,118,0.25)`; left panel starts at x=40 and ends before mid-width, right panel starts at mid-width+20. Baseline 80px above bottom.
- **Left panel:** bold green heading "K = 2"; green `#27ae60` envelope curve (width 2.5) of two Gaussians (height 30, centers at bins 4 and 12, width 1.6, plotted as max of the two); green caption below baseline "✓ Two real groups".
- **Right panel:** bold red heading "K = 4"; four narrow Gaussians (height 22, width 2) at bin centers `[3, 5.5, 10.5, 13]` with widths `[1.1, 1.0, 1.0, 1.1]`, colored `#e74c3c`, `#e67e22`, `#8e44ad`, `#2980b9`; red caption below baseline "✗ 2 fake groups in overlap".
- **Divider:** thin vertical `#ddd` line at mid-width.
- **Caption (bottom center, `#1a5276`):** "Over-specifying K creates phantom clusters in overlap zones".

## Section 4: Local Optima & Degenerate Solutions

EM finds local maxima of the likelihood, not global. Different random starts produce different solutions. Worse: a component can collapse onto a single point (variance → 0, likelihood → ∞), creating a degenerate singularity that breaks the entire fit.

- **Breaks:** One component collapses to a single outlier point — variance shrinks to zero, log-likelihood shoots to infinity. EM "converges" to a mathematically valid but useless solution.
- **Verify:** Check component variances after fitting — any near-zero variance means collapse. Run 20+ restarts and compare final log-likelihoods.
- **Fix:** Regularize covariance (add λI), set minimum variance floor, or use K-means initialization (K-means++→ EM).

### Visualization (canvas `c3`, 720×340)

Two-panel comparison of EM convergence: good initialization vs degenerate collapse.

- **Title (bold 14px `#1a5276`, top center):** "EM Convergence: Good Init vs Degenerate Collapse".
- **Left panel (x=50, ~280px wide):** bold green heading "Good Init (K-means++)". Two well-separated green `#27ae60` ellipse outlines (width 2): one at (140, 160) radii 55×35 rotated −0.3, one at (260, 180) radii 50×40 rotated 0.2. 20 green `rgba(39,174,96,0.7)` dots ring the first ellipse and 20 blue `rgba(26,82,118,0.7)` dots ring the second (3px). Green caption below: "LL = −234.5".
- **Right panel (x≈390):** bold red heading "Bad Init → Collapse". One giant orange `#e67e22` ellipse outline centered in the panel (radii 110×80) containing 38 orange `rgba(230,126,34,0.65)` dots in a spiral pattern — a single component covering everything. A collapsed component drawn as a 7px filled red `#e74c3c` dot with a small red ring at upper left, annotated in red: "σ² → 0" and "(singularity!)". Red caption below: "LL = +∞ (degenerate)".
- **Caption (bottom center, `#1a5276`):** "Singularities make LL meaningless — add covariance regularization (λI)".

## Section 5: Sufficient Data per Component

Each Gaussian component needs enough points to estimate mean (d params) + covariance (d(d+1)/2 params). With 10 features, that's 65 parameters per component. K=5 means 325 parameters — if you have 500 points, you're overfitting the covariance structure.

- **Breaks:** 20 features × K=3 = 690 parameters. With n=200, covariance matrices are singular — EM crashes or produces garbage estimates.
- **Verify:** Compute params per component: d + d(d+1)/2. Multiply by K. If total > n/10, you're in danger.
- **Fix:** Diagonal covariance (d params each), tied covariance (shared across components), or reduce dimensionality first with PCA.

### Visualization (canvas `c4`, 720×340)

Canvas-drawn table of parameter-count scenarios with status badges.

- **Title (bold 14px `#1a5276`, top center):** "Parameters per Component vs Available Data".
- **Columns (bold `#1a5276` headers over a horizontal rule):** d, K, Params, n, n / params, Status.
- **Rows (d / K / Params / n / ratio / status):**
  - 3 / 2 / 24 / 200 / 8.3 / "✓ OK" (green `#27ae60`)
  - 5 / 3 / 78 / 300 / 3.8 / "✓ OK" (green)
  - 10 / 3 / 225 / 400 / 1.8 / "⚠ Tight" (orange `#e67e22`, row tinted `rgba(230,126,34,0.08)`)
  - 10 / 5 / 375 / 500 / 1.3 / "✗ Singular" (red `#e74c3c`, row tinted `rgba(231,76,60,0.08)`)
  - 20 / 3 / 693 / 200 / 0.3 / "✗ Singular" (red, row tinted red)
  - Ratio values are colored red/orange/dark by status; row height 38px.
- **Formula lines (centered):** gray `#555` "Params per component = d + d(d+1)/2 = mean + covariance entries"; bold red "Rule: need n/params > 10 for stable estimates".
- **Caption (bottom center, `#1a5276`):** "High-d + many components → use diagonal or tied covariance constraint".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (42%) holds `.obj-title` + `.obj-desc` paragraph + `.obj-detail` lines, right `<td>` (58%, centered) holds the canvas. Even rows have background `#fafcfe`.
- **Detail-line markup:** "Breaks:" uses `<span class="bad">` (red `#e74c3c`, weight 600); "Verify:" uses `<span class="tag tag-check">` (background `#eafaf1`, color `#1e8449`); "Fix:" uses `<span class="tag tag-fix">` (background `#fef9e7`, color `#b7950b`). Tags are 0.82em, weight 600, padding 1px 6px, radius 3px. `.warn` class is orange `#e67e22`.
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; `.subtitle` `#666` 0.95rem with 32px bottom margin; table cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; `.obj-title` 1.05em weight 600 `#1a5276`; `.obj-desc` 0.9em `#333`; `.obj-detail` 0.85em `#444`. No nav bar, no back/home links.
- **Canvas:** intrinsic `width`/`height` attributes per chart (c0 720×300, c1–c4 720×340); a shared `setupCanvas(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, secondary blue `#2980b9`, gray text `#444`/`#666`.
