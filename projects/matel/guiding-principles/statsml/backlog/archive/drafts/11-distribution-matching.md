# Distribution Matching

**Page type:** detail page (TOC box, then two-column obj-table layout: text left 45%, canvas right 55%, one obj-table per section)
**HTML title tag:** Distribution Matching

**Subtitle:** Identifying which known distribution families fit a feature's data — multiple matches with confidence levels.

## Table of Contents

**Table of Contents** (ordered list of in-page anchor links):

1. Core Principle: Match Many, Commit to None (#principle)
2. The Basic Distribution Families (#families)
3. Fitting & Scoring (#fitting)
4. Visual Examples (#examples)
5. Non-Standard Shapes (#nonstandard)
6. Connection to Precondition Checking (#connection)

Shared canvas notes for this page: sampled data is generated with a seeded `mulberry32` PRNG (seed given per chart) and Box-Muller normals. `drawHist` renders a 25-bin histogram (bars in the given color at alpha 0.5, default margins left 30 / right 10 / top 10 / bottom 15, gray `#999` baseline), overlaid with a Gaussian-smoothed density line in `#1a5276` width 2 (sigma 1.2 bins, kernel radius 3σ) and a 95% SE band filled `rgba(230,126,34,0.18)` (SE = 1.96·smoothed/√effN, effN = clamp(n, 30, 200)).

## 1. Core Principle: Match Many, Commit to None

### Keep All Good Fits — Don't Pick One Winner

- Test each feature against known families. Report ALL matches with confidence scores.
- A feature might be "80% normal, 60% exponential" — both interpretations lead to valid tests
- Different matches suggest different test strategies — each becomes a candidate
- The right answer might be NONE of the standard families (use non-parametric)

**Why?** Forcing a single choice is artificial. Multiple plausible fits give richer downstream options for separation testing.

### Visualization (canvas `c1`, 720×280)

Split panel: histogram on the left, horizontal confidence bars on the right.

- **Title (bold 17px `#1a5276`, top left):** "One Feature → Multiple Candidate Fits".
- **Histogram:** 300 samples of exp(1 + 0.6·z), z ~ N(0,1), seed 42; drawn via `drawHist` in a 360×200 region (margins left 40, top 40), bar color `rgba(26,82,118,0.4)`.
- **Confidence bars** (starting x=400, y=50, 40px row pitch; 200px empty track with `#ddd` outline, filled bar = conf × 200 at alpha 0.6, name at left in 17px `#333`, bold value at right — colored if conf > 0.5 else `#999`), headed "Match Confidence:" in bold 17px `#1a5276`:
  - Right-skewed 0.78 `#e67e22`
  - Normal 0.15 `#3498db`
  - Exponential 0.12 `#e74c3c`
  - Uniform 0.00 `#9b59b6`
- **Caption (bottom center, 17px `#555`):** "Keep all matches above threshold — don't force one winner".

## 2. The Basic Distribution Families

### Normal (Gaussian) — Symmetric Bell

- Symmetric, mean = median = mode
- ~68% within 1σ, ~95% within 2σ
- The reference for parametric tests (t-test, ANOVA)
- **Reject if:** |skewness| > 2 or kurtosis > 9 or bimodal

**Real examples:** Height (same gender), blood pressure in healthy pop, IQ, measurement errors.

### Visualization (canvas `c-normal`, 720×300)

`drawHist` of 300 samples z ~ N(0,1) (Box-Muller, seed 100), bar color `#3498db`. Title (bold 17px `#1a5276`, top left): "Normal — symmetric bell, mean=median=mode".

### Exponential — Rapid Decay from Zero

- Highest density at minimum, monotonically decreasing
- Mean ≈ standard deviation (for pure exponential)
- Memoryless: P(X > s+t | X > s) = P(X > t)
- **Reject if:** any value < 0, or mode far from min, or |skewness - 2| > 1.5

**Real examples:** Time between events, wait times, days since last login, radioactive decay.

### Visualization (canvas `c-exp`, 720×300)

`drawHist` of 300 samples −ln(u), u ~ U(0,1) (seed 200), bar color `#e74c3c`. Title: "Exponential — decay from zero, memoryless".

### Uniform — Flat (No Peak)

- Roughly equal density everywhere between bounds
- No peak structure, kurtosis ~ 1.8
- Mean-based tests are meaningless (no concentration)
- **Reject if:** clear peaks exist, kurtosis > 2.5, or multimodal

**Real examples:** Hash values, random IDs, uniformly sampled timestamps, shuffled indices.

### Visualization (canvas `c-uniform`, 720×300)

`drawHist` of 300 samples u·10 (seed 300), bar color `#9b59b6`. Title: "Uniform — flat, no peak structure".

### Mixture of 2 Normals — Bimodal or Heavy-Shouldered

- Two overlapping subpopulations with different means/spreads
- Can appear bimodal (well-separated) or as a single wide peak with shoulders
- Decompose via EM algorithm; test each component separately
- **Reject if:** clearly unimodal (Hartigan's dip test p > 0.3)

**Real examples:** Hemoglobin (male+female mixed), blood pressure (medicated vs not), bimodal income.

### Visualization (canvas `c-mixture`, 720×300)

`drawHist` of 300 samples (seed 400): with prob 0.45 draw N(−2, 0.8), else N(2.5, 1.0); bar color `#16a085`. Title: "Mixture of 2 Normals — bimodal, two subpopulations".

## 3. Fitting & Scoring

### How to Compare Candidate Fits

- **Step 1 — Feasibility filter:** Check basic properties (all positive? symmetric? bounded?) to instantly reject impossible families. Cost: O(1).
- **Step 2 — Fit parameters:** For each surviving family, fit via MLE (maximum likelihood). Get the "best version" of each family.
- **Step 3 — Measure fit quality:** KS distance (max gap to empirical CDF), Anderson-Darling (emphasizes tails), histogram chi-squared.
- **Step 4 — Rank with AIC:** Penalize complex models (more parameters). AIC weight = probability of being best model among candidates.
- **Step 5 — Combined confidence:** Blend absolute fit (KS score) with relative ranking (AIC weight). If KS > 0.20, reject regardless of ranking.

**Total cost:** ~O(n log n) regardless of how many families tested. The sort for CDF comparison dominates.

### Visualization (canvas `c-fitting`, 720×280)

Horizontal five-step pipeline of rounded boxes (120×55, radius 6, 15px gaps, centered; fill = step color at alpha 0.15, 2px colored border, two-line bold 17px label inside, 17px `#666` sub-label below, gray `#999` arrow connectors). Title (bold 17px `#1a5276`, top left): "Fitting Pipeline".

| Step label | Sub-label | Color |
|---|---|---|
| Feasibility Filter | reject impossible | `#7f8c8d` |
| MLE Fit | best params | `#f39c12` |
| Goodness of Fit | KS, AD, χ² | `#2980b9` |
| AIC Ranking | penalize complexity | `#8e44ad` |
| Combined Confidence | 0-1 score | `#27ae60` |

## 4. Visual Examples

### Example A: Response Times (Clear Right-Skew)

- API latency, n=400. Peak early, long right tail.
- Best match: right-skewed (conf 0.82). Exponential is distant second (0.12).
- **Action:** Use non-parametric tests on raw values, or test on log(values) if normality holds after transform.

### Visualization (canvas `c-ex1`, 720×300)

`drawHist` of 400 samples exp(3.5 + 0.7·z) (seed 800), bar color `#e67e22`. Title: "Response Times — right-skewed (conf 0.82)".

### Example B: Test Scores (Near-Normal)

- Standardized exam, n=350. Symmetric bell.
- Best match: Normal (conf 0.88). Nothing else close.
- **Action:** t-test directly applicable. No transformation needed.

### Visualization (canvas `c-ex2`, 720×300)

`drawHist` of 350 samples 72 + 12·z (seed 801), bar color `#3498db`. Title: "Test Scores — normal (conf 0.88), t-test valid directly".

### Example C: Bimodal Blood Pressure

- Two subpopulations (medicated vs not), n=350.
- Best match: Mixture of 2 Normals (conf 0.74). Normal alone fails (0.18).
- **Action:** DO NOT apply t-test on combined data. Decompose into components, test each separately.

### Visualization (canvas `c-ex3`, 720×300)

`drawHist` of 350 samples (seed 804): with prob 0.55 draw N(120, 8), else N(145, 10); bar color `#16a085`. Title: "Blood Pressure — mixture (conf 0.74), decompose first".

## 5. Non-Standard Shapes

### When Nothing Matches — Use Non-Parametric

- **Zero-inflated:** Spike at 0 + continuous tail. Split into binary (has/hasn't) + fit the non-zero part.
- **Truncated/censored:** Pile-up at a boundary (sensor cap, salary limit). Fit truncated model or treat boundary as separate bucket.
- **Discrete spikes on continuous:** Round numbers over-represented. Separate spikes from smooth part.
- **Multi-component (3+):** Complex shape from heterogeneous populations. Use non-parametric tests.

**Fallback:** If all families score confidence < 0.3, use Mann-Whitney / permutation test. Range-based analysis still works perfectly regardless of distribution shape.

### Visualization (canvas `c-nonstandard`, 720×280)

Two side-by-side mini histograms (seed 900, shared PRNG stream). Title (bold 17px `#1a5276`, top left): "Non-Standard Shapes That Don't Fit Any Family".

- **Left (340×180 region, margins left 40 / top 50):** zero-inflated — 150 zeros + 100 samples exp(2 + 0.8·z); bar color `#e67e22`; centered 17px orange label above: "Zero-inflated".
- **Right (340×180 region, margins left 390 / top 50):** truncated — 200 samples N(50, 15) capped at 80; bar color `#3498db`; centered 17px blue label above: "Truncated/Censored".
- **Caption (bottom center, 17px `#555`):** "When nothing matches → non-parametric tests + range-based analysis".

## 6. Connection to Precondition Checking

### Distribution Match → Which Tests to Apply

- **Normal match:** t-test directly on raw values
- **Right-skewed match:** t-test on log(values) or Mann-Whitney on raw
- **Exponential match:** Mann-Whitney or compare rates
- **Uniform match:** Range-based analysis only (mean comparison is meaningless)
- **Mixture match:** Decompose, test components separately
- **No match:** Non-parametric fallback (Mann-Whitney, permutation)

**Multiple matches = multiple test candidates.** Each produces separation evidence. Weight by confidence during aggregation. The non-parametric fallback is always available regardless of match quality.

### Visualization (canvas `c-connection`, 720×280)

Routing list of six full-width rounded rows (30px tall, 37px pitch from y=45, x 40 to w−40; fill = row color at alpha 0.1, 1.5px colored rounded border, bold 17px colored match name at left, 17px `#333` "→  test" text at x=230). Title (bold 17px `#1a5276`, top left): "Distribution Match → Test Selection".

| Match | Test | Color |
|---|---|---|
| Normal | t-test on raw | `#3498db` |
| Right-skewed | Mann-Whitney or log-transform | `#e67e22` |
| Exponential | Mann-Whitney / rate compare | `#e74c3c` |
| Uniform | Range-based only | `#9b59b6` |
| Mixture | Decompose → test components | `#16a085` |
| No match | Non-parametric fallback | `#7f8c8d` |

## Callout (philosophy box)

**Key insight:** Distribution matching adds information but doesn't gate progress. Even "nothing matches" is useful — it steers toward non-parametric tests. The exact parametric family matters less than knowing whether the data needs transformation before testing.

## Regeneration instructions

- **Layout:** h1, subtitle, `.toc` box (background `#f8fafb`, border `1px solid #e0e0e0`, padding 20px 30px, radius 4px, `<ol>` of `#2980b9` anchor links), then six h2 sections (each with an `id` anchor). Each h2 is followed by one or more single-row `.obj-table`s: left `<td>` (45%) holds `.obj-title` + bullets/paragraphs, right `<td>` (55%, centered) holds the canvas. Section 2 has four obj-tables (one per family); section 4 has three (one per example); the rest have one each. Ends with the `.philosophy` callout.
- **obj-table:** full width, border-collapse; td border `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; even rows `#fafcfe`. `.obj-title` 1.05em, weight 600, `#1a5276`; `ul` 0.9em `#333`; `strong` `#1a5276`.
- **Page style:** body -apple-system/system sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px solid `#2980b9` bottom border; subtitle `#666` 1.05em. `.philosophy`: background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em. No nav bar, no back/home links.
- **Canvases:** intrinsic `width`/`height` attributes as given (all 720 wide); scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Data via `mulberry32(seed)` + Box-Muller; histograms via the shared `drawHist` (25 bins, alpha-0.5 bars, `#1a5276` smoothed density line, `rgba(230,126,34,0.18)` SE band, `#999` baseline). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; extras `#3498db`, `#9b59b6`, `#16a085`, `#7f8c8d`, `#f39c12`, `#2980b9`, `#8e44ad`, gray text `#555`/`#333`/`#666`.
