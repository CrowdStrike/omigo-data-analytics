# Shape Detection

**Page type:** detail page (TOC box + numbered h2 sections, each a two-column obj-table row: text left 45%, canvas right 55%)
**HTML title tag:** Shape Detection

**Subtitle:** Within each cluster determine the shape of the distribution. Shape tells you what statistical tools are valid.

## Table of Contents

1. Why Shape Matters (#why)
2. Unimodal Shapes (#unimodal)
3. Multimodal Shapes (#multimodal)
4. Special Shapes (#special)
5. Shape Evolution with Sample Size (#evolution)
6. Shape → Valid Tests (#tests)

## 1. Why Shape Matters

**Shape Determines Which Tests Are Valid**

- A t-test on bimodal data is meaningless (mean falls in the empty valley)
- A t-test on heavily skewed data is misleading (mean distorted by tail)
- Non-parametric tests (Mann-Whitney) work regardless — but are less powerful
- Knowing the shape lets you pick the MOST POWERFUL valid test

**CNN-based classification:** We use a trained CNN on 64×64 histogram images (see doc 14). Outputs soft probabilities across 11 shape classes — no manual thresholds.

**Shape evolves with data:** At n=50 a distribution may look normal. At n=500 a second peak appears. Re-evaluate when data doubles.

### Visualization (canvas `c1`, 720×240)

Three mini-histograms side by side comparing t-test validity across shapes. Each mini-chart is 200px wide × 120px tall, origins at x = 30/265/500, y = 45, with a Gaussian-smoothed density line overlay (`#1a5276`, width 2, band `rgba(26,82,118,0.12)`, sigma 1.2, effective n 100).

- **Title (bold 17px `#1a5276`, left):** "Same Test, Different Shapes → Different Validity".
- **Chart 1 — bins** `[2,5,12,25,42,55,42,25,12,5,2]`, bars `rgba(39,174,96,0.4)`, label "Bell → t-test ✓".
- **Chart 2 — bins** `[5,35,30,15,8,4,3,2,1,1,0]`, bars `rgba(231,76,60,0.4)`, label "Skewed → t-test ✗".
- **Chart 3 — bins** `[3,12,25,15,4,2,4,15,25,12,3]`, bars `rgba(231,76,60,0.4)`, label "Bimodal → t-test ✗✗".
- Labels: 17px `#333`, centered below each mini-chart.

## 2. Unimodal Shapes

### Normal (Bell) — Symmetric Hill

- One peak, equal spread both sides
- Valid: t-test, Welch's, ANOVA, Cohen's d
- Min n: 30+ per class

**Examples:** Height (same gender), blood pressure, IQ, measurement errors, resting heart rate.

### Visualization (canvas `s-normal`, 720×260)

Standard shape histogram (shared `drawShape` renderer — see Regeneration instructions).

- **Bins:** `[2, 5, 12, 25, 42, 65, 82, 95, 100, 95, 82, 65, 42, 25, 12, 5, 2]`, bars `rgba(39,174,96,0.4)`.
- **Bottom-center label (bold 17px `#1a5276`):** "Normal: symmetric bell".

### Right-Skew — Peak Left, Long Tail Right

- Most common non-normal shape in real data
- Mean >> Median (pulled by tail)
- Valid: Mann-Whitney, or log-transform then t-test
- Invalid: t-test on raw values

**Examples:** Income, hospital charges, file sizes, CRP levels, stock prices, city populations.

### Visualization (canvas `s-rskew`, 720×260)

- **Bins:** `[8, 60, 100, 90, 65, 42, 28, 18, 12, 8, 6, 4, 3, 2, 2, 1, 1]`, bars `rgba(231,76,60,0.4)`.
- **Label:** "Right-Skew: peak left, long tail right".

### Left-Skew — Peak Right, Long Tail Left

- Less common. Often from ceiling effects or bounded scales.
- Valid: Mann-Whitney, median comparison

**Examples:** Exam scores (ceiling), satisfaction ratings, oxygen saturation, time remaining in subscription.

### Visualization (canvas `s-lskew`, 720×260)

- **Bins:** `[1, 1, 2, 3, 4, 6, 8, 12, 18, 28, 42, 65, 90, 100, 60, 8]`, bars `rgba(41,128,185,0.4)`.
- **Label:** "Left-Skew: peak right, long tail left".

### Exponential — Rapid Decay from Zero

- Highest density at minimum, monotonically decreasing
- Mean ≈ standard deviation
- Valid: Mann-Whitney, rate comparison
- Invalid: t-test, variance-based tests

**Examples:** Time between events, wait times, days since login, radioactive decay.

### Visualization (canvas `s-exp`, 720×260)

- **Bins:** `[100, 72, 52, 37, 27, 20, 14, 10, 7, 5, 4, 3, 2, 1, 1, 0]`, bars `rgba(230,126,34,0.4)`.
- **Label:** "Exponential: decay from zero".

### Sharp Spike — Extreme Concentration at One Value

- 60-80%+ of data at a single value, thin tails
- The signal is in who DEVIATES from the spike
- Valid: Fisher exact (spike vs deviation), chi-square
- Invalid: continuous distribution tests

**Examples:** Body temperature (98.6°F), resting heart rate, blood pH, zero-inflated features.

### Visualization (canvas `s-spike`, 720×260)

- **Bins:** `[3, 2, 2, 3, 5, 100, 5, 3, 2, 2, 1, 1, 0, 0, 0]`, bars `rgba(142,68,173,0.4)`.
- **Label:** "Spike: extreme concentration at one value".

## 3. Multimodal Shapes

### Bimodal (Twin Peaks) — Deep Valley

- Two distinct hills with near-zero valley between them
- Usually two sub-populations mixed together
- Valid: Chi-square on peak membership, per-peak class check
- Invalid: t-test (mean falls in empty valley)

**Examples:** Hemoglobin (male+female), testosterone, fasting glucose (diabetic+healthy).

### Visualization (canvas `s-bimodal`, 720×260)

- **Bins:** `[3, 12, 30, 42, 30, 12, 5, 4, 12, 30, 42, 30, 12, 3]`, bars `rgba(41,128,185,0.4)`.
- **Label:** "Bimodal: two peaks with valley".

### Multimodal (3+ Peaks)

- Three or more modes, each a sub-population
- Treat each peak's region independently
- Check class composition per peak — some may be pure

**Examples:** Lab values (child/adult/elderly ranges), medication dosages, shift-based measurements.

### Visualization (canvas `s-multi`, 720×260)

- **Bins:** `[5, 18, 25, 12, 4, 3, 5, 20, 22, 8, 3, 4, 15, 25, 18, 5]`, bars `rgba(142,68,173,0.4)`.
- **Label:** "Multimodal: 3+ peaks".

## 4. Special Shapes

### Uniform / Flat — No Peak

- Roughly equal density everywhere
- Mean comparison is meaningless (no concentration)
- Valid: KS test, bucket chi-square, quantile comparison

**Examples:** Random IDs, hash values, uniformly sampled timestamps.

### Visualization (canvas `s-uniform`, 720×260)

- **Bins:** `[12, 11, 13, 12, 14, 11, 13, 12, 11, 14, 12, 13, 11, 12, 13]`, bars `rgba(39,174,96,0.4)`.
- **Label:** "Uniform: flat, no peak".

### U-Shaped (Bathtub) — High at Both Edges

- Data avoids the center, concentrates at extremes
- Compare which class dominates which end

**Examples:** Proportions near 0 or 1, opinion polarization, bathtub failure curves.

### Visualization (canvas `s-ushaped`, 720×260)

- **Bins:** `[30, 22, 12, 5, 3, 2, 2, 3, 5, 12, 22, 30]`, bars `rgba(230,126,34,0.4)`.
- **Label:** "U-Shaped: high at edges, low middle".

### Truncated / Censored — Hard Cutoff

- Distribution artificially cut off — spike at boundary from capping
- Boundary spike is its own category ("at limit")
- Invalid: fitting standard distributions to the full range

**Examples:** Sensor saturation, credit score bounds (300-850), salary caps, pain scales at max.

### Visualization (canvas `s-truncated`, 720×260)

- **Bins:** `[2, 5, 12, 25, 40, 55, 50, 35, 20, 10, 5, 3, 2, 2, 45]` (final bin is the boundary spike), bars `rgba(231,76,60,0.4)`.
- **Label:** "Truncated: cutoff with spike at boundary".

## 5. Shape Evolution with Sample Size

**Shape Assessment Changes as Data Grows**

- **n < 30:** Can't classify shape. Too noisy.
- **n = 30-100:** Can tell unimodal vs not. Gross skewness direction. NOT subtle bimodality.
- **n = 100-500:** Clear bimodality, skewness degree, approximate family.
- **n = 500-2000:** Subtle peaks, tail behavior, precise characterization.
- **n > 2000:** Fine structure, small secondary modes.

**Re-evaluate when:** Data doubles in size, new data extends the range, previous assessment was "tentative" (n < 100), or classification accuracy drops.

### Visualization (canvas `s-evolution`, 720×240)

Three mini-histograms (200×120px each at x = 30/260/490, y = 50) showing the same distribution at growing n, each with a density-line overlay (`#1a5276`, band `rgba(26,82,118,0.12)`, sigma 1.2, effective n 80).

- **Title (bold 17px `#1a5276`, left):** "Same Distribution: n=30 (noisy) → n=200 (emerging) → n=2000 (clear bimodal)".
- **n=30 bins:** `[3, 5, 8, 6, 4, 7, 5, 3, 6, 8, 5, 4]` (noisy), bars `rgba(231,76,60,0.4)`, label "n=30: \"normal?\"".
- **n=200 bins:** `[4, 10, 22, 30, 18, 8, 5, 7, 15, 25, 28, 12]` (structure emerging), bars `rgba(230,126,34,0.4)`, label "n=200: second bump?".
- **n=2000 bins:** `[3, 12, 30, 42, 28, 10, 4, 3, 10, 28, 42, 30, 12, 3]` (clear bimodal), bars `rgba(39,174,96,0.4)`, label "n=2000: bimodal!".
- Labels: 17px `#333`, centered below each mini-chart.

## 6. Shape → Valid Tests

**Quick Reference**

- **Normal:** t-test, Welch's, Cohen's d, ANOVA
- **Right/Left-Skew:** Mann-Whitney, log-transform + t-test
- **Exponential:** Mann-Whitney, rate comparison
- **Spike:** Fisher exact, chi-square (spike vs deviation)
- **Bimodal:** Chi-square on peak membership, per-peak test
- **Multimodal:** Per-peak class composition
- **Uniform:** KS test, bucket chi-square
- **U-shaped:** Compare class at each end
- **Truncated:** Separate at-limit from continuous, test each

**Universal fallback:** Mann-Whitney U. Works for any shape, needs 20+ per group. Only weakness: can't tell you WHERE separation occurs.

### Visualization (canvas `s-tests`, 720×280)

Reference table of 7 colored rounded rows mapping shape to valid test.

- **Title (bold 17px `#1a5276`, left):** "Shape → Valid Test Selection".
- **Rows (40px left/right margins, 27px tall, 32px pitch starting y=45; fill at 8% alpha of row color, 1px stroke of row color, radius 3; bold colored shape label at x=55, `#333` test text starting at x=250 prefixed "→  "):**
  1. "Normal" → "t-test, Welch's, ANOVA" — `#27ae60`
  2. "Right/Left-Skew" → "Mann-Whitney or log + t-test" — `#e67e22`
  3. "Exponential" → "Mann-Whitney, rate comparison" — `#e67e22`
  4. "Spike" → "Fisher exact, chi-square" — `#8e44ad`
  5. "Bimodal/Multi" → "Per-peak test after split" — `#2980b9`
  6. "Uniform / U-shaped" → "KS, bucket analysis" — `#7f8c8d`
  7. "ANY shape (fallback)" → "Mann-Whitney U (always valid)" — `#1a5276`

## Callout (philosophy box)

**When in doubt: Mann-Whitney U.** It works for any continuous distribution shape, requires no normality assumption, and only needs 20+ samples per group. Its only weakness: it tests general "stochastic dominance" — use bucket analysis to find WHERE separation occurs.

## Regeneration instructions

- **Layout:** single long page. h1, `.subtitle`, a `.toc` box (background `#f8fafb`, border `1px solid #e0e0e0`, padding 20px 30px, radius 4px, bold "Table of Contents" heading + ordered anchor list `#why`, `#unimodal`, `#multimodal`, `#special`, `#evolution`, `#tests`), then numbered h2 sections. Each shape/content block is a one-row `.obj-table`: left `<td>` (45%) holds `.obj-title` + bullets + Examples paragraph, right `<td>` (55%, centered) holds the canvas. Page ends with a `.philosophy` callout.
- **Shared shape renderer (`drawShape`):** margins left 30, right 20, top 10, bottom 25; bars normalized to bin max, 1px gaps, zero bins skipped; thin `#ddd` baseline; bold 17px `#1a5276` centered label 5px above the bottom edge; overlaid Gaussian-smoothed density line (sigma 1.3, kernel radius 3σ, winsorized at 2× bar height) in `#1a5276` width 2 with a 95% SE band `rgba(26,82,118,0.13)` computed as ±1.96·smoothed/√n with effective n 150 (clamped 30-200).
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with `border-bottom: 2px solid #2980b9`; subtitle `#666` 1.05em; `strong` in `#1a5276`; table cell borders `1px solid #e0e0e0`, padding 20px 24px, even rows `#fafcfe`; `.obj-title` 1.05em, weight 600, `#1a5276`; `.philosophy` background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em. No nav bar, no back/home links.
- **Canvas:** intrinsic `width`/`height` attributes per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart text uses 17px -apple-system.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, gray `#7f8c8d`, text grays `#666`/`#333`.
- In regenerated HTML, any card/page links use `.html` extensions.
