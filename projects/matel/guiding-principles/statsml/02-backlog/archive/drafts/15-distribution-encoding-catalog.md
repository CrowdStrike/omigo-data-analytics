# Distribution Encoding Catalog

**Page type:** detail page (TOC box + two-column obj-table layout: text left 45%, canvas right 55%, one table per encoding; plus a 3-column monospace example grid near the end)
**HTML title tag:** Distribution Encoding Catalog

**Subtitle:** All the ways to capture a numerical feature's distribution characteristics for downstream use — each is a different lens on the same data.

## Table of Contents

1. Summary Statistics (#summary)
2. Density Encodings (#density)
3. Multi-Resolution Histograms (#multireso)
4. Visual / Image Encodings (CNN Input) (#visual)
5. Structural Encodings (#structural)
6. Pairwise / 2D Encodings (Feature × Target) (#pairwise)
7. Robustness Encodings (Sampling & Bagging) (#robustness)
8. Transform-Based Encodings (#transform)
9. Comparison Encodings (Distance Measures) (#comparison)
10. Clustering-Based Encodings (#clustering)

## 1. Summary Statistics (Lossy Compression)

**Fixed-Length Numeric Summaries**

- **Moments:** mean, variance, skewness, kurtosis — 4 numbers capturing center, spread, asymmetry, peakedness
- **Quantile vector:** [Q1, Q5, Q10, Q25, Q50, Q75, Q90, Q95, Q99] — robust to outliers, captures tail behavior
- **Range descriptors:** min, max, IQR, range, MAD (median absolute deviation)
- **Counts:** n, n_missing, n_unique, n_zeros

**When useful:** Quick screening, feature comparison, stationarity checks. Fast to compute, easy to compare across features.

**Limitation:** Lossy — two very different distributions can have identical moments (e.g., bimodal symmetric = same mean/variance as normal).

### Visualization (canvas `c1`, 720×240)

Two mini histograms with identical moments but different shapes.

- **Title (bold `#1a5276`, 17px, top left):** "Same Mean & Variance — Totally Different Distributions"
- **Left panel (x=40, y=45, 300×130):** normal bins `[1,3,7,14,22,30,35,30,22,14,7,3,1]`, fill `rgba(41,128,185,0.4)`, label "Normal: μ=50, σ=10"
- **Right panel (x=390, y=45, 300×130):** bimodal bins `[12,20,28,18,5,2,1,2,5,18,28,20,12]`, fill `rgba(142,68,173,0.4)`, label "Bimodal: μ=50, σ=10 (same!)"
- **Caption (bottom center, red `#e74c3c`):** "Moments are identical → lossy encoding misses the structure"

## 2. Density Encodings (Shape Representation)

**Equal-Width Histogram**

- Fixed bin boundaries, count per bin — the classic
- Simple and interpretable but density-blind (empty bins in sparse regions)
- Length depends on bin count choice

**Example:** Income in 20 equal-width bins from $0-$200k → bin 1 has 4000 people, bins 10-20 have 50 total.

### Visualization (canvas `c2a`, 720×280)

Single histogram illustrating equal-width binning failure.

- **Title (bold `#1a5276`, top left):** "Equal-Width: most bins empty, first bin overloaded"
- **Bins:** `[80, 45, 20, 10, 5, 3, 2, 1, 1, 0, 0, 0, 1, 0, 0]`, scale max 82; margins left 40 / right 20 / top 25 / bottom 25.
- **Bar colors:** bins with value > 40 in `rgba(231,76,60,0.4)` (overloaded), others `rgba(41,128,185,0.35)`; zero bins not drawn.

**Equal-Count (Percentile-Width) Histogram**

- 20 buckets each with 5% of data — width encodes density
- Narrow bucket = data is packed (high density). Wide bucket = spread thin.
- **Properties:** Fixed-length (always 20 numbers), scale-free, n-independent
- Two features with same shape produce same width-vector regardless of n or scale

**Example:** Income — first bucket (bottom 5%) spans $0-$8k (narrow=packed). Last bucket (top 5%) spans $120k-$500k (wide=sparse).

**Signal:** Width ratio between densest and sparsest bucket directly measures concentration.

### Visualization (canvas `c2b`, 720×280)

Horizontal strip of 10 variable-width segments, each holding 5% of the data.

- **Title (bold `#1a5276`, top left):** "Equal-Count (5% each): Width = Density Signal"
- **Segment widths (thousands of dollars):** `[8, 10, 12, 15, 18, 22, 30, 45, 70, 120]`, drawn proportionally across the plot width; margins left 40 / right 20 / top 35 / bottom 40.
- **Segment colors:** width < 20 → `rgba(39,174,96,0.4)` (green, dense); width < 50 → `rgba(41,128,185,0.3)` (blue); else `rgba(231,76,60,0.3)` (red, sparse); 0.5px gray `#999` outlines; "5%" centered in segments wider than 25px.
- **Footer labels:** left in green `#27ae60`: "narrow = dense"; right in red `#e74c3c`: "wide = sparse".

**KDE (Continuous Density) & CDF**

- **KDE:** Smooth continuous density curve. Bandwidth-dependent but no binning artifacts.
- **Empirical CDF:** Cumulative probability — monotone, complete, no bin choice needed.
- **Area under z-normalized curve:** Normalize to N(0,1), compute area in regions — captures how much mass is in each standard-deviation band.
- **Multi-bandwidth KDE:** Like multi-resolution histograms — same data at different smoothing levels. Narrow bandwidth reveals fine structure, wide bandwidth shows envelope. Trust what persists.

**When useful:** KDE for visual assessment and CNN input. CDF for distribution comparisons (KS test operates on CDFs). Z-normalized area for cross-feature comparison of tail heaviness.

**Bandwidth parameter:** Small h → overfits (every point a bump). Large h → oversmooths (washes out real peaks). Optimal h from cross-validation encodes structural complexity.

### Visualization (canvas `c2c`, 720×280)

Two curves side by side.

- **Title (bold `#1a5276`, top left):** "KDE (density) and CDF (cumulative) — complementary views"
- **Left curve (left ~45% of plot):** Gaussian bell curve in `#2980b9` 2.5px, labeled below in blue: "KDE (smooth density)"
- **Right curve (right ~45% of plot):** logistic sigmoid CDF in `#27ae60` 2.5px, labeled below in green: "CDF (cumulative)"

### Visualization (canvas `c2d`, 720×220)

Multi-bandwidth KDE overlay: three smoothing levels of the same bimodal data over faint gray histogram bars.

- **Title (bold `#1a5276`, top left):** "Same Data — 3 Bandwidths (like multi-resolution for density)"
- **Underlying bins (20):** `[2, 5, 10, 20, 35, 40, 30, 18, 10, 6, 4, 5, 8, 15, 28, 35, 30, 18, 8, 3]`, drawn as background bars `rgba(150,150,150,0.15)`.
- **Curves (Gaussian KDE of the bins, evaluated at 3× resolution):**
  - h=0.8, red `#e74c3c`, 2px, dash 2/2 — legend "h=0.8 (narrow): sees noise bumps"
  - h=2.0, blue `#2980b9`, 3px solid — legend "h=2.0 (medium): bimodal visible"
  - h=5.0, green `#27ae60`, 2px, dash 6/3 — legend "h=5.0 (wide): smooths to unimodal"
- **Legend:** top right, line swatch + colored label per bandwidth. Light `#ddd` baseline axis.
- **Caption (bottom center, `#555`):** "Bimodality persists at h=0.8 and h=2.0 but vanishes at h=5.0 → real but moderate"

## 3. Multi-Resolution Histograms

**Shape is Sensitive to Bucket Count — Run Multiple**

- Same data at 10 bins, 20 bins, 50 bins reveals different structure
- Too few bins: misses bimodality. Too many: noise becomes "peaks."
- **Multi-resolution approach:** Run at 4-6 scales (Sturges, √n, n/50, n/100). Trust what persists.
- **Persistence score:** A peak visible at 4/6 resolutions = real. Visible at 1/6 = artifact.

**Key insight:** No single bin count is "correct." The true shape is what survives across all resolutions. Features that appear at one scale only are noise created by that specific binning.

**See also:** [Doc 13: Bin Sizing Strategy](12-bin-sizing-strategy.md) (original href: `13-bin-sizing-strategy.html`)

### Visualization (canvas `c3`, 720×240)

Three mini histograms of the same data at increasing resolution.

- **Title (bold `#1a5276`, top left):** "Same Data at 3 Resolutions — Trust What Persists"
- **Panel 1 (x=30, y=45, 200×130), 10 bins:** `[10, 20, 35, 30, 18, 8, 12, 25, 30, 15]`, label "10 bins: \"unimodal?\""
- **Panel 2 (x=260, y=45, 200×130), 20 bins:** `[5,8,12,18,20,15,10,8,5,4,3,4,6,10,15,18,16,12,8,4]`, label "20 bins: bimodal!"
- **Panel 3 (x=490, y=45, 200×130), 40 bins:** computed `exp(-0.5*((i-12)/5)^2)*20 + exp(-0.5*((i-30)/4)^2)*15 + sin(i*1.3)*2` for i=0..39, label "40 bins: confirmed + noisy"
- **Bars:** `rgba(26,82,118,0.4)`; labels gray `#666`.
- **Caption (bottom center, bold green `#27ae60`):** "Bimodality persists at 20 & 40 bins → real structure"

## 4. Visual / Image Encodings (CNN Input)

**Render Distribution as Image → CNN Classifies Shape**

- **Histogram image (64×64):** Pixel intensity = bar height. What our CNN uses. Low-pass filters noise naturally.
- **CDF image:** Monotone curve as image — encodes different features (inflection points = modes).
- **Multi-resolution stack (3-channel):** Same data at 3 bin widths → R/G/B channels. CNN sees scale-dependent features.
- **Density heatmap with SE band:** Histogram + smoothed overlay + confidence band — richer signal per pixel.

**Why images work:** CNNs are noise-tolerant (MaxPool erases 1-pixel dips), learn from examples (no manual thresholds), and output soft probabilities (multi-candidate).

### Visualization (canvas `c4`, 720×280)

Row of four rounded orange boxes (150×65, 15px gaps, centered) naming the image encodings.

- **Title (bold `#1a5276`, top left):** "Image Encodings for CNN Classification"
- **Boxes (fill `#f39c12` at 0.12 alpha, 2px `#f39c12` stroke, bold orange two-line labels, gray desc below):**
  1. "Histogram 64×64" — "bar heights as pixels"
  2. "CDF image" — "monotone curve"
  3. "3-channel stack" — "3 resolutions as RGB"
  4. "Density + SE band" — "uncertainty overlay"
- **Caption (bottom center, `#555`):** "All feed same CNN architecture → shape class probabilities"

### Visualization (canvas `c4b`, 720×280)

Layered right-skew histogram with two smoothed overlays and SE band, plus legend.

- **Title (bold `#1a5276`, top left):** "Histogram + KDE Density + SE Confidence Band (what CNN sees)"
- **Bins (25):** `[3, 8, 18, 32, 48, 55, 50, 40, 28, 18, 12, 8, 6, 4, 3, 2, 2, 1, 1, 1, 0, 0, 0, 0, 0]`, bars `rgba(26,82,118,0.35)`; margins left 50 / right 30 / top 45 / bottom 35.
- **Overlays:** Gaussian-smoothed line σ=1.5 in `#e67e22` 2.5px with 95% SE band `rgba(230,126,34,0.25)` (effN=150); wider-bandwidth KDE σ=3.0 in `#c0392b` 2px dashed 6/3. Gray `#999` baseline.
- **Legend (top right):** blue swatch "Histogram bars"; orange line "Smoothed density + SE"; dashed dark-red line "KDE (wider bandwidth)"; orange swatch "95% SE confidence band".
- **Caption (bottom center, `#555`):** "CNN sees all three layers as a single rich image — bars + density + uncertainty"

## 5. Structural Encodings (Topological)

**Skeleton of the Distribution — Peaks, Valleys, Gaps**

- **Peak/valley decomposition:** Number of peaks, positions, heights, valley depths — the structural skeleton
- **Gap vector:** Locations and widths of empty regions (where no data exists)
- **Point mass inventory:** Which values have disproportionate frequency and how much (spike at 0, cap at 99999)
- **Cluster membership vector:** Which cluster each value belongs to after gap/valley splitting

**When useful:** Routing decisions (bimodal → split, spike → isolate). These are what determine pipeline path — not the raw density.

**Example:** Capital Gain: 1 spike (95% at zero), 1 gap, 1 continuous cluster, 1 cap spike. Four structural elements → four pipeline actions.

### Visualization (canvas `c5`, 720×240)

Annotated histogram showing structural elements.

- **Title (bold `#1a5276`, top left):** "Structural Skeleton: Peaks, Valleys, Gaps, Spikes"
- **Bins (20):** `[60, 2, 4, 10, 25, 40, 30, 15, 8, 5, 4, 6, 12, 25, 35, 28, 15, 5, 2, 1]`, scale max 62; margins left 40 / right 30 / top 45 / bottom 40.
- **Bar colors:** bin 0 (spike) `rgba(142,68,173,0.5)`; bins with value < 3 `rgba(231,76,60,0.2)`; others `rgba(41,128,185,0.4)`.
- **Annotations (bold 17px, centered):** "SPIKE" in purple `#8e44ad` above bin 0; "gap" in red `#e74c3c` mid-height near bin 1-2; "peak 1" in blue `#2980b9` above bin ~5; "valley" in green `#27ae60` at ~60% height near bin 10; "peak 2" in blue above bin ~14.
- **Caption (bottom center, `#555`):** "1 spike + 1 gap + 2 peaks + 1 valley = structural fingerprint"

## 6. Pairwise / 2D Encodings (Feature × Target)

**How the Feature Relates to the Target Class**

- **Scatter plot (feature vs target or feature vs another feature):** Visual 2D relationship — captures non-linear, threshold, and interaction patterns
- **Conditional density:** P(feature | pos) vs P(feature | neg) — two overlaid histograms showing where classes differ
- **Enrichment profile:** Enrichment ratio at each bucket — directly encodes where class signal lives
- **Separation curve:** Sorted by feature value, cumulative pos rate — like ROC but for a single feature

**Key insight:** Everything above (sections 1-5) encodes the feature ALONE. This section encodes feature × target. A feature with zero univariate signal can have strong 2D signal when combined with another feature.

**Example:** Age alone: mild signal. Age × Hours-per-week scatter: clear separation cluster (>50hrs + age 35-55 = almost all >50K).

### Visualization (canvas `c6`, 720×280)

Scatter plot with two classes (seeded PRNG mulberry32(77)).

- **Title (bold `#1a5276`, top left):** "2D Encoding: Feature × Target (scatter reveals non-linear patterns)"
- **Points:** 100 red dots `rgba(231,76,60,0.3)` (r=3) uniformly scattered over the plot; 60 green dots `rgba(39,174,96,0.5)` (r=3.5) clustered in x∈[0.4,0.75], y∈[0.1,0.5] of the plot area. Margins left 60 / right 30 / top 50 / bottom 40.
- **Axis labels (gray `#666`):** x: "Feature A (age)" bottom center; y: "Feature B (hours)" rotated left.
- **Legend (top right):** green dot "pos (>50K)"; red dot "neg (≤50K)".
- **Annotation (green `#27ae60`, centered above plot):** "2D cluster visible that neither feature shows alone"

## 7. Robustness Encodings (Sampling & Bagging)

**How Stable Is the Distribution Under Resampling?**

- **Bootstrap mean distribution:** Sample 30% repeatedly (100 times), compute mean each time → distribution of means. Width tells you how unstable the mean is.
- **Outlier sensitivity:** If subsamples have dramatically different means, outliers are dominating. The variance of subsample means IS the robustness signal.
- **Bagged statistics:** Average of means from many subsamples = robust mean. Like bagging in RF — reduce variance of the estimate.
- **Trimmed/Winsorized mean:** Drop top/bottom 5% then compute mean — how much does it change? Large change = heavy outlier influence.
- **Range of subsample quantiles:** Q75 from 100 subsamples — is it stable (±2%) or volatile (±20%)?

**When useful:** Detecting whether summary statistics are trustworthy. A mean that jumps ±30% across subsamples is not a reliable feature for any downstream model.

**Example:** Hospital charges — mean=$18k but subsample means range from $12k to $45k. The mean is dominated by rare $200k outliers. Bagged trimmed mean = $14k (stable, reliable).

### Visualization (canvas `c7`, 720×280)

Two dot-strip plots of 30 bootstrap subsample means each (seeded mulberry32(55)), blue dots `rgba(41,128,185,0.5)` r=4 jittered around a horizontal gray `#ddd` axis line.

- **Title (bold `#1a5276`, top left):** "Bootstrap 30% Subsamples: How Stable Is the Mean?"
- **Left strip (x=40, y=50, 300×130):** means `50 ± 2` (uniform jitter ±2), value range [35, 65]; label "Stable: means cluster ±2 (trustworthy)"
- **Right strip (x=390, y=50, 300×130):** means `50 ± 17.5`, value range [20, 80]; label "Unstable: means scatter ±18 (outlier-dominated)"
- **Caption (bottom center, red `#e74c3c`):** "Wide scatter → mean is unreliable → use robust alternative (bagged trimmed mean)"

## 8. Transform-Based Encodings

**Transform the Data, Then Encode the Transform Parameters**

- **Rank transform:** Replace values with ranks (0-1) — always produces uniform distribution. The mapping itself IS the encoding.
- **Box-Cox optimal λ:** Find λ that normalizes the data — λ encodes skewness (λ=0 → log needed, λ=1 → already normal, λ=0.5 → sqrt)
- **Z-normalization residuals:** After standardizing to N(0,1), where does data deviate? The residual pattern IS the non-normality signature.
- **Fourier coefficients:** Frequency decomposition of histogram — periodic patterns (seasonal data, rounding artifacts)

**Key insight:** The transform parameter (which λ? which bandwidth? which rank mapping?) is itself information about the distribution's character.

### Visualization (canvas `c8`, 720×240)

Four stacked rounded blue rows (three-column text layout).

- **Title (bold `#1a5276`, top left):** "Transform Parameters Encode Distribution Character"
- **Rows (fill `#2980b9` at 0.08 alpha, 1px `#2980b9` rounded stroke; name bold blue at x=60, result `#333` at x=250, insight `#666` at x=480):**
  1. "Rank transform" / "→ always uniform" / "mapping itself = encoding"
  2. "Box-Cox λ=0" / "→ log needed" / "highly right-skewed"
  3. "Box-Cox λ=1" / "→ already normal" / "no transform needed"
  4. "Box-Cox λ=0.5" / "→ sqrt" / "moderately skewed"

**Log Transform — A Different Lens on the Same Data**

Log compresses the right tail and expands the left. This changes SOME statistics but preserves OTHERS:

- **Changes:**
  - Mean (dramatically — log compresses outliers)
  - Variance / std (compressed scale)
  - Skewness (right-skew → symmetric if data was log-normal)
  - Kurtosis (heavy tails get tamed)
  - KS test vs normal (may pass after log when it failed on raw)
  - t-test result (different means, different variance → different p-value)
  - Shape classification (right_skew → bell after log)
  - Histogram shape (completely different visual)
  - Bucket boundaries (linear → multiplicative spacing)
- **Preserves:**
  - Rank ordering (monotone transform → all ranks stay same)
  - Mann-Whitney U result (rank-based → identical conclusion)
  - Two-sample KS "are these different?" (order preserved → same yes/no)
  - Median ordering between groups (if median_A > median_B in raw, same in log)
  - Percentile membership (same points in top 5% before and after)
  - Correlation sign (positive stays positive)

**When to apply:** Right-skewed data where the tail dominates mean-based statistics. If log(values) looks bell-shaped, parametric tests become valid on the transformed data.

**Trap:** Log changes the QUESTION. "Mean income differs" ≠ "mean log-income differs." The latter is about geometric mean, not arithmetic mean. Interpretation shifts.

### Visualization (canvas `c8b`, 720×300)

Before/after histogram pair with arrow, plus a six-column changes/preserves strip below.

- **Title (bold `#1a5276`, top left):** "Log Transform: Same Data, Different Statistical World"
- **Left histogram:** raw right-skew bins `[5, 35, 55, 45, 30, 18, 12, 8, 5, 4, 3, 2, 2, 1, 1, 1, 0, 0, 0, 1]`, bars `rgba(231,76,60,0.4)`, heading in bold red `#e74c3c`: "Raw (right-skewed)"
- **Right histogram:** log-transformed bins `[1, 3, 6, 12, 20, 30, 38, 42, 40, 35, 28, 20, 14, 8, 5, 3, 2, 1, 0, 0]`, bars `rgba(39,174,96,0.4)`, heading bold green `#27ae60`: "After log() → bell-shaped"
- **Arrow:** black `#333` horizontal arrow between panels labeled bold "log()".
- **Stats strip (six items at 110px spacing):** Mean "✓ changes" (red), Skewness "✓ changes" (red), Ranks "✓ same" (green), Mann-Whitney "✓ same" (green), KS (vs normal) "✓ changes" (red), Shape class "✓ changes" (red).

## 9. Comparison Encodings (Distance Measures)

**How Far Is This Distribution from a Reference?**

- **KS statistic:** Max gap between empirical CDF and reference CDF — single number summarizing departure
- **Earth mover's distance (Wasserstein):** Minimum "work" to transform one distribution into another — captures magnitude of difference
- **KL / JS divergence:** Information-theoretic distance from reference — asymmetric (KL) or symmetric (JS)
- **QQ-plot residuals:** Deviation from theoretical quantiles — the residual vector encodes exactly where and how the data departs from the reference

**When useful:** Drift detection (current vs baseline), distribution matching (which family fits best?), stationarity testing (window T₁ vs T₂).

### Visualization (canvas `c9`, 720×240)

Two CDF curves with the KS distance marked.

- **Title (bold `#1a5276`, top left):** "Distance from Reference = Distribution Fingerprint"
- **Reference CDF:** gray `#999` dashed (4/3) sigmoid centered mid-plot, 2px.
- **Empirical CDF:** blue `#2980b9` solid sigmoid shifted left/wider, 2.5px.
- **KS marker:** vertical red `#e74c3c` 2px segment at ~40% width connecting the two curves, labeled bold red "KS distance".
- **Legend (top right):** gray "reference (normal)"; blue "empirical (skewed)". Margins left 60 / right 30 / top 50 / bottom 30.

## 10. Clustering-Based Encodings

**Decompose the Distribution into Components**

- **GMM (Gaussian Mixture Model):** Decompose into K components — each with mean, variance, weight. The K + parameters = encoding.
- **DBSCAN density clusters:** Number of clusters found, their sizes, noise fraction — encodes structural complexity
- **Hierarchical density (HDBSCAN):** Tree of density levels — encodes at what density threshold clusters merge/split
- **K-means on 1D sorted data:** Optimal split points that minimize within-cluster variance — natural bucket boundaries

**When useful:** When you suspect the distribution is a mixture of subpopulations. The cluster count and sizes themselves become features.

**Example:** Hemoglobin GMM → 2 components (μ₁=10, μ₂=14, w=[0.35, 0.65]) → bimodal confirmed, split at valley between components.

### Visualization (canvas `c10`, 720×240)

Bimodal curve decomposed into two dashed Gaussian components.

- **Title (bold `#1a5276`, top left):** "GMM Decomposition: Distribution = Sum of Components"
- **Combined curve:** solid `#333` 2px: `0.6*exp(-0.5*((i-30)/10)^2) + 0.4*exp(-0.5*((i-70)/8)^2)` over i=0..99.
- **Component 1:** blue `#2980b9` 1.5px dashed (4/3): the first Gaussian term. Label below at 30% width: "Component 1 (60%, μ=30)".
- **Component 2:** green `#27ae60` 1.5px dashed: the second term. Label below at 70% width: "Component 2 (40%, μ=70)".
- **Header annotation (bold `#333`, centered above):** "K=2, weights, means, variances = full encoding". Margins left 40 / right 30 / top 45 / bottom 30.

## Complete Example: "income" — All Encodings

3-column grid of monospace cards (background `#f8f9fa`, border `1px solid #e0e0e0`, radius 6px, 0.82em 'SF Mono'; bold headings `#1a5276`; card 10 uses background `#fef9e7`, border `#f39c12`, heading `#e67e22`):

**1. Summary Stats** — mean: 52,400 | median: 38,200 / std: 41,000 | skew: 2.8 | kurt: 12.1 / Q5=12k Q25=24k Q50=38k Q75=62k Q95=135k / IQR: 38,000 | MAD: 18,500

**2. Percentile-Width** — 20 buckets × 5% each / widths: [3.8k, 4.2k, ... 155k, 347k] / density ratio (min/max): 91× / *narrow=packed, wide=sparse*

**3. Multi-Resolution** — tested: [10, 15, 20, 31, 50] bins / shape at all: right_skew / persistence: 1.0 (all agree) / bimodal persistence: 0.0

**4. CNN Shape** — top-1: right_skew (91%) / top-2: heavy_tail (5%) / 3 renderings agree: ✓ / confidence: HIGH

**5. Structure** — peaks: 1 (at 15th percentile) / valleys: 0 | gaps: 0 / point masses: none / tail_weight (max/median): 12.6×

**6. Pairwise (×Target)** — [8k-24k]: enrichment 0.3× STRONG_NEG / [24k-62k]: 0.9× NO_SIGNAL / [62k-135k]: 2.8× STRONG_POS / [135k+]: 4.5× STRONG_POS

**7. Robustness** — 30% subsample means: $38k–$72k / mean stable? NO (16% volatility) / trimmed mean: $44,200 / outlier influence: HIGH (18% gap)

**8. Log Transform** — skew: 2.8 → 0.15 (symmetric!) / KS vs normal: p=0.34 (passes!) / shape: right_skew → bell / Box-Cox λ=0.08 (≈log)

**9. Distance from Normal** — KS raw: 0.22, p=1e-45 (fails) / KS log: 0.03, p=0.34 (passes!) / JS divergence: 0.28 / QQ residual: curved right tail

**10. KDE (Full Spec)** — kernel: gaussian / bandwidth: 4200 (Silverman) / boundary: reflect at min=8200 / eval_points: 128 (uniform grid) / density_values: [0.0, 0.0001, ...] / peak_density: 0.000024 at $32k / **bandwidth_alternatives:** scott: 5100 | cv_optimal: 3800 / **multi-bw persistence:** h=2k,4k,6k,8k → all right_skew / **encodes:** structural complexity

## Callout (philosophy box)

**The meta-insight:** No single encoding captures everything. Summary stats lose shape. Histograms lose scale-freeness. Images lose precision. 2D encodings miss univariate structure. The pipeline uses MULTIPLE encodings at different stages — CNN images for shape classification, percentile-widths for density profiling, enrichment profiles for test selection, bootstrap resampling for robustness validation. Each lens reveals something the others miss.

## Regeneration instructions

- **Layout:** TOC-reference detail page: h1, `.subtitle`, a `.toc` box (bold "Table of Contents" + ordered anchor list to the ten section ids), then one h2 per encoding, each followed by one or more `.obj-table` blocks (left `<td>` 45% with `.obj-title` + bullets/paragraphs, right `<td>` 55% centered with canvas(es) — the KDE/CDF row holds two stacked canvases `c2c`/`c2d`, the CNN row holds `c4`/`c4b`). After section 10: an h2 "Complete Example" with an inline-styled 3-column grid of monospace cards, then a `.philosophy` callout.
- **Page CSS:** body system sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px bottom border `#2980b9`; `.subtitle` `#666` 1.05em; `.obj-table` cells border `1px solid #e0e0e0`, padding 20px 24px, even rows `#fafcfe`; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` `#1a5276`; `.toc` background `#f8fafb` border `#e0e0e0` radius 4px, links `#2980b9`; `.philosophy` background `#f0f4f8`, left border `4px solid #2980b9`, 0.9em. No nav bar, no back/home links.
- **Canvas:** intrinsic `width`/`height` attributes per chart; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. All chart text 17px -apple-system. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, secondary blue `#2980b9`, purple `#8e44ad`, gold `#f39c12`, dark red `#c0392b`, bar fill `rgba(26,82,118,0.35-0.4)`.
- In regenerated HTML, the in-text "See also" link and any card links use `.html` extensions (original href: `13-bin-sizing-strategy.html`).
