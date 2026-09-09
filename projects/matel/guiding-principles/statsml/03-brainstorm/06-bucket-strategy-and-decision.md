# Bucket Strategy & Decision

**Page type:** detail page (TOC box + numbered h2 sections, each a two-column obj-table row: text left 45%, canvas right 55%)
**HTML title tag:** Bucket Strategy & Decision

**Subtitle:** Form meaningful buckets from profiling results, compute class composition per bucket, make the feature selection decision.

## Table of Contents

1. Why Fixed Bins Fail (#why-adaptive)
2. Adaptive Bucketing Rules (#rules)
3. Bucket Verdicts (#verdicts)
4. Real-World Examples (#examples)
5. Feature Selection Decision (#selection)
6. Ranking Features (#ranking)
7. Cascade Classification (#cascade)

## 1. Why Fixed Bins Fail

**Equal-Width Bins Waste Information**

- Skewed data concentrates 80%+ in one bin, leaving others nearly empty
- Over-crowded bins mix very different values together (can't see signal)
- Under-populated bins have too few samples for reliable statistics
- Solution: adaptive bins guided by data structure from prior profiling steps

**Example:** Income [$0-$500k] in 10 equal bins → Bin 1 has 4200 people, Bins 5-10 have 35 people total. Useless.

### Visualization (canvas `c1`, 720×280)

Histogram of 10 equal-width bins with one dominant bin.

- **Title (bold 17px `#1a5276`, left):** "Fixed Equal-Width Bins — Most Bins Useless".
- **Bins:** `[4200, 600, 120, 45, 15, 8, 5, 3, 2, 2]`, scale max 4200; margins: left 50, right 20, top 35, bottom 40.
- **Bar colors:** bin 0 `rgba(231, 76, 60, 0.5)`, others `rgba(26, 82, 118, 0.35)`; each bar labeled "n=N" in 17px `#333` above.
- **Annotation (17px `#e74c3c`, right-aligned):** "84% crammed into bin 1".
- **X-axis:** thin `#999` baseline; endpoint labels "$0" and "$500k" in 17px `#666`.

## 2. Adaptive Bucketing Rules

**Let Data Structure Guide Boundaries**

- **Natural breaks first:** Gaps → each cluster becomes its own bucket. Valleys → split at valley.
- **Isolate point masses:** Values with disproportionate frequency get their own bucket (e.g., 1678 zeros in "2nd Floor SF")
- **Subdivide dense regions:** Within a cluster with n > 200, split by quantiles. Each sub-bucket needs n ≥ 30.
- **Merge sparse regions:** Adjacent bins with n < 30 get merged until sample requirement is met.
- **Respect shape:** Never split through a peak. Never merge across a valley.

**Example (chart):** "2nd Flr SF" → RULE 4 isolates the zero spike, gap is detected, then the continuous portion is subdivided by quantiles into 4 buckets with ~300+ samples each.

### Visualization (canvas `c2`, 720×280)

Segmented bar diagram showing adaptive rules applied to "2nd Floor SF".

- **Title (bold 17px `#1a5276`, left):** "Adaptive Rules in Action: \"2nd Floor SF\" (Ames Housing)".
- **Margins:** left 50, right 20, top 38, bottom 50. Segments span the plot width proportionally; bar heights scale n/1700 (minimum 20px), 1px `#999` outline.
- **Segments (label / width fraction / n / fill / top rule label):**
  1. "Point mass =0" — 0.18 — n=1678 — `#8e44ad` — rule label "RULE 4: Isolate" (purple)
  2. "GAP" — 0.05 — n=0 — area filled `#fce4ec` full height with centered red `#e74c3c` label "GAP"
  3. "300-500" — 0.19 — n=350 — `rgba(26,82,118,0.35)` — rule label "RULE 2: Subdivide" (blue `#2980b9`)
  4. "500-700" — 0.19 — n=380 — `rgba(26,82,118,0.35)` — "dense region"
  5. "700-900" — 0.19 — n=320 — `rgba(26,82,118,0.35)` — "by quantiles"
  6. "900+" — 0.20 — n=202 — `rgba(26,82,118,0.35)` — "(n≥30 each)"
- Each non-gap segment: "n=N" in 17px `#333` above the bar, range label in `#666` below.
- **Bracket:** 1.5px `#2980b9` horizontal line over the four continuous segments; thin `#999` baseline.

## 3. Bucket Verdicts

**Each Bucket Gets a Verdict Based on Class Composition**

For each bucket, compute: pos_ratio, enrichment vs base rate, Wilson confidence interval.

- **STRONG_POS:** enrichment > 2x AND CI lower bound > 0.5 — classify as pos from this bucket
- **STRONG_NEG:** enrichment < 0.5x AND CI upper bound < base rate — classify as neg
- **MILD_POS / MILD_NEG:** significant but not definitive (enrichment 1.5-2x or 0.5-0.7x)
- **NO_SIGNAL:** matches base rate — this range tells you nothing

**Example:** Fasting glucose [180-320]: enrichment 3.43x, CI [0.64-0.74] → STRONG_POS

### Visualization (canvas `c3`, 720×280)

Enrichment bar chart with verdict labels.

- **Title (bold 17px `#1a5276`, left):** "Fasting Glucose — Bucket Verdicts (base rate = 20%)".
- **Margins:** left 60, right 30, top 40, bottom 55; y scale 0–4.0x; bars at 60% alpha; bold "N.Nx" value above each bar, bold colored verdict below, gray "[range]  n=N" beneath that.
- **Buckets:**
  1. [65-90] n=1200 — 0.20x — STRONG_NEG — `#2980b9`
  2. [90-110] n=1800 — 0.50x — MILD_NEG — `#85c1e9`
  3. [110-130] n=1000 — 1.10x — NO_SIGNAL — `#bbb`
  4. [130-180] n=650 — 2.40x — STRONG_POS — `#27ae60`
  5. [180-320] n=350 — 3.43x — STRONG_POS — `#1e8449`
- **Base rate line:** horizontal dashed red (`#e74c3c`, 1.5px, dash 5/3) at 1.0x, right-aligned red 17px label "base rate (1.0x)"; thin `#999` baseline.

## 4. Real-World Examples

### Example A: Income (Extreme Right-Skew)

- **Problem:** 80% of data in [$0-$50k], long tail to $500k+. Equal-width bins: first bin mixes minimum wage with middle class
- **Strategy:** Quantile bins in dense region + isolate high-income tail
- **Buckets:** [$0-18k] n=1000, [$18-35k] n=1000, [$35-55k] n=1000, [$55-85k] n=700, [$85k+] n=300
- **Effect:** [$85k+] is STRONG_POS (enrichment 3.2x for >$50k class). Without adaptive bins, this signal is diluted across multiple underpopulated fixed bins.

**Value:** Found that income > $85k is a near-certain positive indicator — impossible to see with fixed bins.

### Visualization (canvas `cex1`, 720×280)

Enrichment bar chart, 5 quantile buckets (60% alpha bars, bold "N.Nx" above, gray range and "n=N" labels below; y scale 0–4.0x; dashed red base-rate line at 1.0x; margins left 55, right 20, top 38, bottom 50).

- **Title:** "Income — Quantile Bins Reveal High-Income Signal".
- **Buckets:** "$0-18k" n=1000 0.3x `#2980b9`; "$18-35k" n=1000 0.5x `#85c1e9`; "$35-55k" n=1000 1.0x `#bbb`; "$55-85k" n=700 1.9x `#27ae60`; "$85k+" n=300 3.2x `#1e8449`.

### Example B: Hours per Week (Spike at Mode)

- **Problem:** 50% of values are exactly 40 (full-time standard). Continuous tails on both sides. Not zero-inflated — spike at the mode.
- **Strategy:** Isolate point mass (40) + bucket the tails separately
- **Buckets:** [1-34] part-time n=1200, [35-39] near-FT n=800, [=40] spike n=16000, [41-50] mild OT n=8000, [51+] heavy OT n=4000
- **Effect:** [51+] is STRONG_POS (enrichment 2.8x for >$50k). The spike at 40 is NO_SIGNAL (matches base rate). Part-time [1-34] is STRONG_NEG.

**Value:** Treating 40 as its own bucket reveals that the signal is entirely in the DEVIATORS — people who work unusually few or many hours.

### Visualization (canvas `cex2`, 720×280)

Enrichment bar chart with verdict labels (same style; y scale 0–3.5x; dashed red base-rate line).

- **Title:** "Hours/Week — Spike at 40 Is NO_SIGNAL, Deviators Have Signal".
- **Buckets:** "1-34" n=1200 0.4x STRONG_NEG `#2980b9`; "35-39" n=800 0.8x NO_SIGNAL `#bbb`; "=40" n=16000 1.0x NO_SIGNAL `#bbb`; "41-50" n=8000 1.3x MILD_POS `#85c1e9`; "51+" n=4000 2.8x STRONG_POS `#1e8449`.

### Example C: Age (Monotonic Trend)

- **Problem:** No single age bucket is overwhelmingly one class, but there's a clear trend — older = more likely positive.
- **Strategy:** Equal-count bins across the range (roughly uniform distribution)
- **Buckets:** [17-25] n=800, [26-35] n=900, [36-45] n=1000, [46-55] n=900, [56-90] n=700
- **Effect:** Monotonic enrichment: 0.3x → 0.6x → 1.1x → 1.8x → 2.5x. No single bucket is overwhelmingly strong, but the TREND is unmistakable.

**Value:** The feature passes selection via the "monotonic trend" criterion even though no single bucket exceeds 3x enrichment. Endpoints [17-25] (STRONG_NEG) and [56-90] (STRONG_POS) classify 35% of data.

### Visualization (canvas `cex3`, 720×280)

Enrichment bars (40% alpha) with an orange trend line (`#e67e22`, 3px) through bar centers; y scale 0–3.0x; dashed red base-rate line.

- **Title:** "Age — Monotonic Trend (No Single Strong Bucket, But Clear Pattern)".
- **Buckets:** "17-25" n=800 0.3x; "26-35" n=900 0.6x; "36-45" n=1000 1.1x; "46-55" n=900 1.8x; "56-90" n=700 2.5x. Bar color by enrichment: <0.7 → `#2980b9`; 0.7–1.3 → `#bbb`; >1.3 → `#27ae60`.

### Example D: Capital Gain (Gap Split + Zero-Inflated)

- **Problem:** 95% of values are exactly 0. Remaining 5% scattered from $100 to $99,999 with a second spike at the cap.
- **Strategy:** Isolate zero (point mass) + gap split at empty bins + isolate cap spike
- **Buckets:** [=0] n=30913, [$100-$7k] n=800, [$7k-$15k] n=400, [$15k-$40k] n=289, [=99999] n=159
- **Effect:** [=0] is MILD_NEG (enrichment 0.7x). [=99999] is extreme STRONG_POS (enrichment 4.8x — nearly all are >$50k income). [$15k-$40k] is STRONG_POS (2.9x).

**Value:** The capped value at 99999 is a nearly perfect classifier for a small subset. Without isolating point masses and gap-splitting, this tiny group would be invisible in a fixed bin with the zeros.

### Visualization (canvas `cex4`, 720×280)

Enrichment bar chart (same style; y scale 0–5.5x; dashed red base-rate line).

- **Title:** "Capital Gain — Zero-Inflated + Gap + Cap Spike".
- **Buckets:** "=0" n=30913 0.7x `#85c1e9`; "$100-7k" n=800 1.4x `#f4d03f`; "$7k-15k" n=400 2.1x `#27ae60`; "$15k-40k" n=289 2.9x `#1e8449`; "=99999" n=159 4.8x `#145a32`.

### Example E: Education (Ordinal Discrete)

- **Problem:** Integer values 1-16 representing education years. Not truly continuous, but ordered. Some values rare (1-3).
- **Strategy:** Start with each integer as a bucket, merge adjacent rare values
- **Buckets:** [1-8] (no HS) n=500, [9] (HS dropout) n=1200, [10] (HS grad) n=10500, [11-12] (some college) n=7300, [13] (bachelors) n=5400, [14-16] (grad) n=5100
- **Effect:** Sharp step function: [1-9] is STRONG_NEG (0.2x-0.4x), [13+] is STRONG_POS (2.5x-4.0x). The signal is not gradual — it jumps at degree boundaries.

**Value:** Merging rare low-education values into [1-8] gives enough n to confirm they're all negative. Keeping [10] separate reveals it's the transition point (1.0x, NO_SIGNAL). The step function pattern suggests degree completion matters more than years.

### Visualization (canvas `cex5`, 720×280)

Enrichment bar chart, 6 buckets (same style; y scale 0–4.5x; dashed red base-rate line).

- **Title:** "Education — Step Function at Degree Boundaries".
- **Buckets:** "1-8" n=500 0.2x `#2980b9`; "9" n=1200 0.4x `#2980b9`; "10 (HS)" n=10500 1.0x `#bbb`; "11-12" n=7300 1.4x `#f4d03f`; "13 (BA)" n=5400 2.5x `#27ae60`; "14-16" n=5100 4.0x `#1e8449`.

## 5. Feature Selection Decision

**USE a Feature If It Has Definitive Signal**

- **USE if:** at least one bucket is STRONG (n ≥ 50, CI excludes base rate)
- **USE if:** multiple adjacent buckets show monotonic enrichment trend
- **USE if:** cluster membership alone separates classes (each cluster n ≥ 30)

**REJECT if:** no bucket exceeds 1.5x enrichment, no CI excludes base rate, no trend, KS test fails (p > 0.05).

**Gray zone:** Features with mild signal (1.3-1.8x) are marked "WEAK_CANDIDATE" — useful in combination but not alone.

### Visualization (canvas `c4`, 720×280)

Five colored rounded criteria rows (fill at 10% alpha, 1.5px stroke, radius 4, 34px tall, 42px pitch starting y=50; bold colored icon at x=80, 17px `#333` label at x=110).

- **Title (bold 17px `#1a5276`, centered):** "Feature Selection Criteria".
- Rows:
  1. "✓" "USE: STRONG bucket exists" — `#27ae60`
  2. "✓" "USE: Monotonic trend" — `#27ae60`
  3. "✓" "USE: Cluster separation" — `#27ae60`
  4. "~" "WEAK: Mild signal (1.3-1.8x)" — `#e67e22`
  5. "✗" "REJECT: No enrichment > 1.5x" — `#e74c3c`

## 6. Ranking Features

**Best Features = High Accuracy on a Meaningful Slice**

- **Rank 1:** Best single-bucket accuracy (highest purity with sufficient n)
- **Rank 2:** Classifiable coverage (what % of data can this feature classify?)
- **Rank 3:** Maximum enrichment (how extreme is the signal?)
- **Rank 4:** CI tightness (narrow = precise = trustworthy)

**Key insight:** A feature that classifies 20% of data with 98% accuracy is more valuable than one that classifies 80% with 60% accuracy. Coverage gaps get filled by combining multiple features.

### Visualization (canvas `c5`, 720×280)

Ranked list with numbered circles and weight bars.

- **Title (bold 17px `#1a5276`, centered):** "Feature Ranking — Which Features Matter Most?".
- **Rows (50px pitch starting y=50):** each has a filled `#2980b9` circle (14px radius) with white bold rank number, a bold `#1a5276` label, a gray `#666` description, and a horizontal weight bar at x=430 (width = w·250px, fill `rgba(41, 128, 185, 0.25)`, 1px `#2980b9` stroke, 24px tall):
  1. "Single-bucket accuracy" — "highest purity with n≥50" — bar weight 0.95
  2. "Classifiable coverage" — "% of data this feature can classify" — bar weight 0.80
  3. "Max enrichment" — "how extreme the signal is" — bar weight 0.65
  4. "CI tightness" — "narrow = precise = trustworthy" — bar weight 0.50

## 7. Cascade Classification

**Check Features in Order Until Classified**

- Features ranked by signal strength. Check strongest first.
- If value falls in a STRONG bucket → classify immediately. Done.
- If value in NO_SIGNAL zone → move to next feature.
- If 3+ features show mild enrichment → combined evidence → classify.
- If nothing fires strongly → report "unclassifiable" (honest uncertainty).

**Every classification is backed by:** a specific feature, a specific range, a measured accuracy with CI, and a sample size. Fully explainable. No black box.

### Visualization (canvas `c6`, 720×280)

Left-to-right flow diagram of the cascade with rounded boxes and arrows (all at y=55, 55px tall, radius 6).

- **Title (bold 17px `#1a5276`, left):** "Cascade: Check Features in Order".
- **Boxes:**
  1. (30, 120px wide) fill `#f4ecf7`, stroke `#8e44ad`: "New point:" / "glucose=95" / "age=52"
  2. (195, 140px wide) fill `#ebf5fb`, stroke `#2980b9`: "Feature 1: glucose" / "bucket [90-110]" / "NO_SIGNAL"
  3. (380, 140px wide) fill `#eafaf1`, stroke `#27ae60`: "Feature 2: age" / "bucket [51-70]" / "STRONG_POS"
  4. (565, 130px wide) fill `#d4efdf`, stroke `#27ae60`, bold `#1e8449` text: "CLASSIFY:" / "Positive" / "conf = 0.72"
- **Arrows:** gray `#666` box 1→2; orange `#f39c12` box 2→3 with "skip" label below; green `#27ae60` box 3→4.
- **Evidence trace (17px `#555`, left, two lines below the flow):** "Evidence: age ∈ [51-70] → enrichment 2.1x → CI excludes base rate → classify" and "Every prediction = feature + range + accuracy + CI + sample size. Fully auditable."

## Callout (philosophy box)

**Bridge to ML:** This is where the stats layer feeds into ML. Each feature used in classification has proven significant separation with adequate sample size. No feature enters the model without earning its place through statistical evidence.

## Regeneration instructions

- **Layout:** single long page. h1, `.subtitle`, a `.toc` box (background `#f8fafb`, border `1px solid #e0e0e0`, padding 20px 30px, radius 4px, bold "Table of Contents" heading + ordered anchor list `#why-adaptive`, `#rules`, `#verdicts`, `#examples`, `#selection`, `#ranking`, `#cascade`), then numbered h2 sections. Each content block (including each of the five Real-World Examples A–E under section 4) is a one-row `.obj-table`: left `<td>` (45%) holds `.obj-title` + bullets/paragraphs, right `<td>` (55%, centered) holds the canvas. Page ends with a `.philosophy` callout.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with `border-bottom: 2px solid #2980b9`; subtitle `#666` 1.05em; `strong` in `#1a5276`; table cell borders `1px solid #e0e0e0`, padding 20px 24px, even rows `#fafcfe`; `.obj-title` 1.05em, weight 600, `#1a5276`; `.philosophy` background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em. No nav bar, no back/home links.
- **Canvas:** all canvases 720×280; intrinsic `width`/`height` attributes, scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; shared `box()` (rounded rect + centered multi-line text) and `arrow()` (line + triangle head) helpers for diagram canvases. Chart text uses 17px -apple-system. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, light blue `#85c1e9`, green `#27ae60` (dark `#1e8449`, darkest `#145a32`), red `#e74c3c`, orange `#e67e22`/`#f39c12`, yellow `#f4d03f`, purple `#8e44ad`, neutral `#bbb`, text grays `#666`/`#333`/`#555`.
- In regenerated HTML, any card/page links use `.html` extensions.
