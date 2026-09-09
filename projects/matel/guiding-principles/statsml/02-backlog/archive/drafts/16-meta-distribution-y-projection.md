# Meta-Distribution: Y-Axis Projection

**Page type:** detail page (TOC box + two-column obj-table layout: text left 45%, canvas right 55%, one table per section)
**HTML title tag:** Meta-Distribution: Y-Axis Projection

**Subtitle:** Take a histogram. Make a histogram of the bin heights. What does that second-order distribution tell you?

## Table of Contents

1. The Idea (#idea)
2. Two Projection Methods (#two-methods)
3. What It Encodes (#what-it-encodes)
4. Useful Metrics from Y-Projection (#metrics)
5. Example: Income (Right-Skewed → Spiky Meta) (#ex-income)
6. Example: Age (Near-Uniform → Flat Meta) (#ex-age)
7. Example: Capital Gain (Extreme Spike → Long-Tail Meta) (#ex-capital)
8. Example: Hours/Week (Spike-at-Mode → Bimodal Meta) (#ex-hours)
9. Shape → Meta-Shape Mapping (#summary)
10. What Meta Catches That Others Miss (#unique)

## 1. The Idea

**Histogram of Histogram Heights = Meta-Distribution**

- Take a feature, bin it into 20-30 bins → get bin heights [h₁, h₂, ... h₂₀]
- Now treat those heights as data points. What's THEIR distribution?
- A histogram of those heights = the meta-distribution (Y-axis projection)
- This tells you how "concentrated" vs "spread" the density is

**Intuition:** If most bins are near-zero and one bin is massive → meta has a long tail → original is spiky. If all bins are similar height → meta is a spike → original is uniform.

**It's cheap:** Just compute stats on 20-30 numbers (the bin heights). No new data needed — derived entirely from the histogram you already have.

### Visualization (canvas `c1`, 720×280)

Original histogram → arrow → meta-distribution.

- **Title (bold `#1a5276`, 17px, top left):** "Original Histogram → Y-Projection → Meta-Distribution"
- **Left panel (x=30, y=45, 250×150):** bell-ish bins `[5, 20, 45, 60, 50, 35, 22, 14, 8, 5, 3, 2, 1, 1, 0]`, bars `rgba(41,128,185,0.4)`, label "Original (bell-ish)"
- **Arrow:** black `#333` horizontal arrow at mid-height with gray two-line label "histogram" / "of heights"
- **Right panel (x=350, y=45, 250×150):** 10-bin histogram of the left panel's heights, bars `rgba(230,126,34,0.5)`, label "Meta (right-skewed)"
- **Caption (bottom center, `#555`):** "Most bins are short (tails) → meta skews right. Few bins are tall (peak) → meta tail."

## 2. Two Projection Methods

**Method A: Frontier Points (Bar Top Heights)**

- Take the height of each bar as a data point. 20 bars → 20 values.
- Empty bars contribute height = 0 (important signal — means a gap exists)
- Then make a histogram of those 20 height values
- **Detects:** Concentration inequality, spike severity, gaps (zeros in the meta)

**Key property:** Zero-valued bins show up explicitly. A bimodal original with a deep valley produces zeros in the frontier → meta has a cluster at zero = "there are empty regions."

### Visualization (canvas `c_methodA`, 720×240)

Histogram with red dots marking each bar-top (the frontier points).

- **Title (bold `#1a5276`, top left):** "Method A: Take Bar Top Heights as Data Points"
- **Bins (20):** `[5, 20, 45, 60, 50, 35, 22, 14, 8, 0, 0, 3, 15, 30, 25, 12, 5, 2, 0, 0]`, scale max 62; margins left 40 / right 30 / top 45 / bottom 50.
- **Bars:** non-zero `rgba(41,128,185,0.35)`; zero bins drawn as 3px stubs in `rgba(231,76,60,0.15)`.
- **Frontier dots:** red `#e74c3c` filled circles (r=4) at each bar top center.
- **Caption (bottom center, red):** "● = height values: [5, 20, 45, 60, ... 0, 0] → histogram THESE → meta"
- **Annotation (green `#27ae60`, top right):** "zeros = gaps detected!"

**Method B: Horizontal Slices (Occupancy Profile)**

- Draw 10 horizontal lines at evenly-spaced density levels (10%, 20%, ... 100% of max)
- At each level, count how many bars reach that height or above
- Result: a monotonically DECREASING sequence [20, 18, 14, 10, 7, 4, 3, 2, 1, 1]
- Lower levels always have more bars (all non-empty bars pass level 1)
- **Detects:** How quickly density drops off — the "steepness of concentration"

**Key property:** Always non-zero at lower levels (unlike frontier). The SHAPE of the occupancy curve encodes concentration:

- **Uniform original → flat occupancy:** [20, 20, 20, 20, ...] (all bars at same height)
- **Bell → gentle decline:** [20, 18, 14, 10, 6, 4, 2, 1, 1, 1]
- **Spike → steep cliff:** [20, 2, 1, 1, 1, 1, 1, 1, 1, 1] (only spike bar reaches level 2+)

### Visualization (canvas `c_methodB`, 720×240)

Histogram with dashed orange slice lines plus an occupancy bar profile at right.

- **Title (bold `#1a5276`, top left):** "Method B: Horizontal Slices — Count Bars Reaching Each Level"
- **Left histogram:** bins `[5, 20, 45, 60, 50, 35, 22, 14, 8, 5, 3, 2, 1, 1, 0]`, scale max 62, bars `rgba(41,128,185,0.3)`; margins left 40 / right 350 / top 45 / bottom 30.
- **Slice lines:** 5 dashed (3/3) horizontal lines `rgba(230,126,34,0.5)` at 20/40/60/80/100% of max; each labeled at right in orange `#e67e22` with "N bars" (count of bars at or above that level).
- **Right panel (230px wide):** bold blue heading "Occupancy Profile"; 10 occupancy bars `rgba(230,126,34,0.4)` computed at levels 10%..100%; gray footer labels "low", "level →", "high".

**Comparison: Different Signals from Each**

- **Frontier (A) is better at:** Detecting gaps (zeros), counting spikes, distinguishing "one tall bar" from "three medium bars" (different meta shapes)
- **Occupancy (B) is better at:** Measuring RATE of drop-off, distinguishing "sharp peak + empty" from "broad plateau + thin tails" (same max height but different occupancy curves)
- **Combined:** Frontier Gini + Occupancy slope = two complementary numbers. High Gini + steep slope = isolated spike. High Gini + gentle slope = heavy tail (many bins at moderate height, one very high).

**Example:** Bell and right-skew may have similar max/mean ratios. But occupancy curves differ: bell declines symmetrically (both tails lose bars equally), right-skew declines asymmetrically (right tail loses bars faster).

### Visualization (canvas `c_compare`, 720×280)

Spike vs plateau histograms side by side with method readouts below.

- **Title (bold `#1a5276`, top left):** "Same Data — Different Signal from Each Method"
- **Left histogram (purple `rgba(142,68,173,0.4)`, heading "Spike: one tall bar"):** `[5, 3, 2, 80, 2, 3, 2, 1, 1, 1]`, scale max 82.
- **Right histogram (green `rgba(39,174,96,0.4)`, heading "Plateau: several medium bars"):** `[10, 35, 50, 60, 65, 60, 55, 40, 20, 5]`.
- **Readout rows (y≈185):** "Frontier (A):" — purple "Gini=0.88 (extreme inequality)" vs green "Gini=0.35 (moderate spread)"; "Occupancy (B):" — purple "Cliff: [10, 1, 1, 1, 1, ...]" vs green "Gentle: [10, 9, 8, 6, 4, 2, 1, ...]".
- **Takeaway (bottom center, bold `#1a5276`):** "Frontier: HOW UNEQUAL are heights. Occupancy: HOW FAST does density drop off."

## 3. What It Encodes

**The Meta-Distribution Encodes Concentration**

- **Uniform original → spike meta:** All bins same height → meta is concentrated at one value
- **Bell original → right-skewed meta:** Many low tail bins, few high peak bins → meta skews right
- **Spike original → extreme long-tail meta:** One bin dominates → meta has an outlier
- **Bimodal original → bimodal meta:** Bin heights cluster at two levels (peak heights vs valley)

**Key insight:** The tail weight of the meta-distribution directly measures spikiness/concentration of the original. A feature where the meta has kurtosis > 10 is extremely concentrated — probably needs point-mass isolation.

### Visualization (canvas `c2`, 720×280)

Three original→meta pairs (100×80 mini histograms with orange arrow between, at x=30/270/510, y=45).

- **Title (bold `#1a5276`, top left):** "Shape → Meta-Shape Correspondence"
- **Pair 1:** uniform `[20,20,20,20,20,20,20,20,20,20]` → meta; label "Uniform→Spike meta", metric "Gini≈0"
- **Pair 2:** bell `[2,8,20,40,55,40,20,8,2,1]` → meta; label "Bell→Right-skew meta", metric "Gini≈0.5"
- **Pair 3:** spike `[90,5,2,1,1,0,0,0,0,1]` → meta; label "Spike→Long-tail meta", metric "Gini≈0.95"
- **Style:** originals `rgba(41,128,185,0.4)`, metas `rgba(230,126,34,0.5)` (10-bin histogram of heights); labels `#333`, metric values bold orange `#e67e22`.
- **Caption (bottom center, `#555`):** "Meta tail length = original spikiness. Gini is the single-number summary."

## 3. Useful Metrics from Y-Projection

**Cheap Second-Order Statistics**

- **Gini coefficient of bin heights:** 0 = all bins equal (uniform). 1 = all mass in one bin (spike). Measures relative inequality.
- **Variance / Std of heights:** How much bin counts deviate from the mean count. Measures absolute magnitude of imbalance. Low std = uniform. High std = some bins dominate.
- **CV of heights:** std / mean. Scale-free — combines variance with sample size normalization. CV < 0.3 = uniform. CV > 1.5 = concentrated. CV > 3 = extreme spike.
- **Max/mean ratio:** Tallest bin ÷ average bin. Uniform → ≈1. Spike → 10-50×. Bell → 2-4×.
- **Entropy of heights:** -Σ(pᵢ log pᵢ) where pᵢ = hᵢ/Σh. High = uniform. Low = concentrated.
- **Fraction of bins above mean:** Uniform → 50%. Right-skew → 30-40%. Spike → 5-10%.
- **Number of zero bins:** Directly counts gaps. Zero in a 20-bin histogram = 5% of range is empty.

**Gini vs Variance — when they diverge:** Heights [100, 200, 300, 400, 500, 600] have HIGH variance but MODERATE Gini (evenly spaced). Heights [1, 1, 1, 1, 1, 100] have HIGH variance AND high Gini. Gini = relative inequality. Variance = absolute scale of imbalance. Use both.

**All O(k) where k = number of bins.** No iteration over raw data needed.

### Visualization (canvas `c3`, 720×280)

Metric comparison table drawn on canvas (alternating row backgrounds `#f8f9fa`/white).

- **Title (bold `#1a5276`, top left):** "Meta-Metrics: Gini, Variance, CV, Max/Mean, Entropy"
- **Column headers (bold `#1a5276`):** Metric / Uniform / Bell / Right-Skew / Spike (at x=60/200/320/440/580).
- **Rows (metric in `#333`; values colored — Uniform green `#27ae60`, Bell blue `#2980b9`, Right-Skew orange `#e67e22`, Spike red `#e74c3c`):**
  - Gini: 0.0 / 0.45 / 0.72 / 0.95
  - Std: 12 / 165 / 430 / 7200
  - CV: 0.05 / 0.65 / 1.45 / 4.7
  - Max/Mean: 1.0× / 2.5× / 7.6× / 19×
  - Entropy: 4.3 / 3.2 / 2.4 / 1.1
  - Zero bins: 0 / 0 / 1 / 14
- **Caption (bottom center, `#555`):** "Gini=relative inequality. Std/CV=absolute scale. Both needed. Zero bins=structural gaps."

## 4. Example: Income (Right-Skewed)

**Income → Right-Skewed Meta**

**Original:** Right-skewed. Peak near $30k, long tail to $500k.

**Bin heights (20 bins):** [1850, 1200, 680, 420, 280, 180, 120, 80, 55, 40, 30, 22, 15, 10, 7, 5, 3, 2, 1, 0]

**Meta-distribution of heights:** Most heights are small (0-100 range), a few are large (1000+). Meta is right-skewed.

- Gini: 0.72 (high inequality)
- Max/mean: 1850/245 = 7.6×
- Entropy: 2.4 (moderate)
- CV: 1.45
- Bins above mean: 5/20 = 25%

**What this tells you:** Density is concentrated in a small region — 75% of bins are below average height. The feature has most of its information packed into the left portion.

### Visualization (canvas `c4`, 720×250)

Three-panel triple (shared `drawTriple` layout: Original histogram | Frontier Meta | Occupancy Profile, separated by `#ddd` vertical lines; original in the listed color, frontier meta `rgba(230,126,34,0.5)` with "Gini=…" label, occupancy `rgba(39,174,96,0.5)` with shape label; log-scaled original and log-spaced occupancy levels when max/min > threshold).

- **Title:** "Income: right-skewed → right-skewed meta → gentle occupancy decline"
- **Bins:** the 20 heights above; original color `rgba(41,128,185,0.4)`; Gini=0.72; occupancy label "gentle slope".

## 5. Example: Age (Near-Uniform)

**Age → Nearly Flat Meta**

**Original:** Roughly uniform from 17-90 with mild working-age bulge.

**Bin heights (20 bins):** [180, 220, 280, 310, 340, 350, 360, 350, 330, 310, 290, 270, 250, 230, 200, 180, 150, 120, 90, 60]

**Meta-distribution of heights:** Heights range from 60 to 360 — moderate spread. Meta looks like a mild left-skew (most bins are mid-height).

- Gini: 0.22 (low inequality)
- Max/mean: 360/245 = 1.5×
- Entropy: 4.1 (high — near-uniform)
- CV: 0.35
- Bins above mean: 10/20 = 50%

**What this tells you:** Density is spread evenly — no dominant region. Standard binning works fine. No spike isolation needed.

### Visualization (canvas `c5`, 720×250)

Triple panel: "Age: near-uniform → flat meta → very flat occupancy"; bins above; original color `rgba(39,174,96,0.4)`; Gini=0.22; occupancy label "flat (all bars similar)".

## 6. Example: Capital Gain (Extreme Spike)

**Capital Gain → Extreme Long-Tail Meta**

**Original:** 95% at zero, scattered non-zero values, spike at cap (99999).

**Bin heights (20 bins):** [30913, 878, 157, 360, 38, 49, 5, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 159]

**Meta-distribution of heights:** 14 bins at zero/near-zero, then a few scattered, then one extreme outlier (30913). Meta has extreme long tail.

- Gini: 0.95 (extreme inequality)
- Max/mean: 30913/1628 = 19×
- Entropy: 1.1 (very low — concentrated)
- CV: 4.7
- Bins above mean: 2/20 = 10%

**What this tells you:** Gini > 0.9 is a screaming signal for point-mass isolation. The meta long-tail says "one bin dominates everything" — standard analysis on the full range is meaningless without separating the spike first.

### Visualization (canvas `c6`, 720×250)

Triple panel: "Capital Gain: extreme spike → long-tail meta → cliff occupancy"; bins above; original color `rgba(142,68,173,0.4)` (log scale, "(log scale)" note shown); Gini=0.95; occupancy label "cliff (drops to 1 at level 2)".

## 7. Example: Hours/Week (Spike at Mode)

**Hours/Week → Bimodal Meta**

**Original:** Spike at 40, continuous tails on both sides.

**Bin heights (20 bins):** [205, 531, 645, 1547, 1015, 1302, 1635, 16100, 2442, 677, 3036, 841, 1519, 277, 365, 83, 182, 20, 34, 105]

**Meta-distribution of heights:** Most heights cluster 100-2000, but one outlier at 16100. Meta is bimodal: {low cluster of normal bins} + {one spike bin}.

- Gini: 0.62 (moderate-high)
- Max/mean: 16100/1628 = 9.9×
- Entropy: 2.8
- CV: 2.1
- Bins above mean: 5/20 = 25%

**What this tells you:** Max/mean close to 10× but not as extreme as capital gain (19×). The spike is dominant but there's real continuous structure around it — not just empty bins. Suggests: isolate spike, profile remainder (unlike capital gain where remainder is too sparse).

### Visualization (canvas `c7`, 720×250)

Triple panel: "Hours/Week (Adult): spike-at-mode → bimodal meta → step occupancy"; bins above; original color `rgba(230,126,34,0.4)` (log scale); Gini=0.62; occupancy label "step (drops then plateau)".

**Example E: SalePrice (Ames Housing) — Bell-ish with Tail**

**Original:** Roughly bell-shaped with a right tail (expensive homes). Most prices cluster $100k-$250k.

**Bin heights:** [11, 135, 451, 882, 565, 343, 210, 119, 88, 46, 35, 16, 11, 3, 6, 3, 4, 0, 0, 2]

- Gini: 0.58 (moderate — one dominant bin but others have mass)
- Occupancy: gentle decline (many bins are moderate height)

**What meta tells you:** Not extreme enough for spike isolation (Gini < 0.6), but adaptive bins will help since the peak bin has 5× the average.

### Visualization (canvas `c_sale`, 720×250)

Triple panel: "SalePrice (Ames): bell + tail → moderate meta → gentle occupancy"; bins above; original color `rgba(41,128,185,0.4)`; Gini=0.58; occupancy label "gentle decline".

**Example F: Education-Num (Adult Census) — Discrete with Spikes**

**Original:** Integer values 1-16. Not smooth — specific values (9, 10, 13) have disproportionate counts.

**Bin heights:** [50, 160, 330, 640, 1200, 7300, 10500, 5400, 1700, 1100, 730, 580, 430, 200, 150, 80]

- Gini: 0.55 (moderate — dominated by bins 6-7 but others non-trivial)
- Occupancy: two-step (big cluster of low bars + 2 tall bars)

**What meta tells you:** The occupancy two-step reveals that this isn't smooth — there are "popular" education levels (HS grad, some college) creating discrete spikes within the continuous range.

### Visualization (canvas `c_edu`, 720×250)

Triple panel: "Education-Num (Adult): discrete spikes → two-step meta → stepped occupancy"; 16 bins above; original color `rgba(142,68,173,0.4)` (log scale); Gini=0.55; occupancy label "two-step (popular levels)".

**Example G: Lot Frontage (Ames) — Sparse with Zeros**

**Original:** Many missing/zero values (no frontage), then a bell-shaped cluster for actual frontage values.

**Bin heights:** [490, 0, 12, 45, 120, 280, 420, 380, 250, 140, 70, 35, 18, 8, 4, 2, 1, 0, 0, 0]

- Gini: 0.68 (high — the zero-spike bin dominates + many empty bins)
- Occupancy: steep initial drop then plateau at 1

**What meta tells you:** The combination of Gini=0.68 + zeros in frontier = structural feature (point mass + gaps + continuous). Needs: isolate zero-bin, skip gaps, profile remainder. More complex than just "right-skewed."

### Visualization (canvas `c_lot`, 720×250)

Triple panel: "Lot Frontage (Ames): zeros + bell → high Gini meta → steep occupancy"; bins above; original color `rgba(231,76,60,0.4)` (log scale); Gini=0.68; occupancy label "steep (spike + zeros)".

**Example H: Garage Area (Ames) — Zero-Inflated Bell**

**Original:** ~5% at exactly 0 (no garage), rest forms a clean bell around 400-500 sqft.

**Bin heights:** [150, 20, 45, 90, 180, 320, 480, 520, 450, 310, 180, 90, 45, 20, 10, 5, 3, 2, 1, 0]

- Gini: 0.42 (moderate — one somewhat-tall bin but overall spread is even)
- Occupancy: smooth decline (looks like a proper bell's occupancy)

**What meta tells you:** Gini is LOWER than capital gain (0.42 vs 0.95) even though both have a zero spike — because the continuous part here is well-populated. The meta says "mild concentration" which means: yes, isolate the zero but the remainder is well-behaved.

### Visualization (canvas `c_garage`, 720×250)

Triple panel: "Garage Area (Ames): zero-inflated bell → mild meta → smooth occupancy"; bins above; original color `rgba(39,174,96,0.4)`; Gini=0.42; occupancy label "smooth decline".

## 8. Shape → Meta-Shape Mapping

**Quick Reference: What Meta Tells You**

- **Meta is a spike (Gini<0.2):** Original is uniform → standard binning works, no special treatment
- **Meta is mild right-skew (Gini 0.3-0.6):** Original is bell or mild skew → normal pipeline
- **Meta is heavy right-skew (Gini 0.6-0.85):** Original has significant concentration → adaptive bins needed, possible tail isolation
- **Meta has extreme tail (Gini>0.85):** Original has point mass domination → MUST isolate spike before any analysis
- **Meta is bimodal:** Original has a spike-at-mode + continuous structure → spike isolation + profile remainder

**Actionable thresholds:**

- Max/mean > 15× → point mass isolation mandatory
- Max/mean 5-15× → investigate, likely needs adaptive treatment
- Max/mean < 3× → standard pipeline is fine

### Visualization (canvas `c8`, 720×280)

Five stacked rounded action rows (colored 0.1-alpha fill + 1.5px colored stroke; meta label bold in row color at x=60, action in `#333` after "→" at x=310).

- **Title (bold `#1a5276`, top left):** "Meta-Distribution → Pipeline Action"
- Rows:
  1. "Spike (Gini < 0.2)" → "Standard pipeline — uniform density" — green `#27ae60`
  2. "Mild skew (Gini 0.3-0.6)" → "Normal pipeline, maybe adaptive bins" — blue `#2980b9`
  3. "Heavy skew (Gini 0.6-0.85)" → "Adaptive bins mandatory, tail investigation" — orange `#e67e22`
  4. "Extreme tail (Gini > 0.85)" → "MUST isolate point mass before any analysis" — red `#e74c3c`
  5. "Bimodal meta" → "Spike-at-mode: isolate spike + profile remainder" — purple `#8e44ad`

## Callout (philosophy box)

**The meta-distribution is a cheap, second-order signal.** It answers: "how concentrated is the density itself?" — a question that summary statistics (mean, skewness) don't directly address. High Gini/long-tail meta = the feature needs structural treatment (spike isolation, adaptive bins) before any statistical test makes sense. It's a 1-line computation that can route the entire pipeline.

## 9. What Meta Catches That Others Miss

**Hard for Histogram / CNN / Algos — Easy for Meta**

- **Spike buried in a dense region:** A histogram shows a tall bar, but if surrounding bars are also tall, the CNN may classify it as "bell." Meta detects it: one bin height is 3× the next highest → max/mean ratio jumps. The CNN sees shape; meta sees concentration inequality.
- **Uniform-with-one-hole vs uniform:** A histogram with 19 equal bars and 1 empty bar looks "nearly uniform" to CNN. Meta immediately shows it: one zero in an otherwise constant vector → the min/max ratio and zero-bin count are anomalous.
- **Degree of spikiness (quantitative):** CNN says "spike" for both 60% concentration and 99% concentration — same class label. Meta gives you the NUMBER: Gini 0.7 vs 0.98. This determines whether you need point-mass isolation (0.98) or just adaptive bins (0.7).
- **Multiple small spikes vs one big spike:** A feature with 3 values each at 15% (and rest spread) gets CNN label "multimodal" or "spike." Meta sees it differently: 3 bins at similar high values → low variance among the top heights → "distributed concentration" (not single-point-mass).
- **Flat tail vs dying tail:** A histogram where the last 8 bins are [5,5,4,5,4,5,5,4] (flat tail) vs [8,6,5,3,2,1,0,0] (dying tail) — same count roughly, but meta sees that flat-tail has LOW variance in the tail heights. This signals a different population in the tail (cluster) vs genuine decay.
- **Bin-count sensitivity detector:** Run meta at 20 bins and 40 bins. If Gini changes dramatically → the feature is resolution-sensitive (structure depends on binning). If Gini is stable → robust feature that doesn't need multi-resolution treatment.

### Visualization (canvas `c9`, 720×340)

Three hard-case examples with CNN vs Meta readouts.

- **Title (bold `#1a5276`, top left):** "Hard for CNN — Easy for Meta"
- **Case 1 (x=30, 200×100):** bins `[30, 35, 32, 28, 95, 30, 28, 25, 20, 15]` in `rgba(41,128,185,0.4)`; below in red: "CNN: \"bell\" (shape is bell-like)"; in green: "Meta: max/mean=2.8× (hidden spike!)"
- **Case 2 (two 90×100 histograms at x=270/380, purple `rgba(142,68,173,0.4)`):** `[60, 8, 7, 6, 5, 4, 3, 3, 2, 2]` and `[99, 0, 0, 0, 0, 0, 0, 0, 0, 1]`; purple caption "CNN: both = \"spike\""; green: "Meta: Gini 0.7 vs 0.98" / "(different treatment!)"
- **Case 3 (two 90×100 histograms at x=520/620, orange `rgba(230,126,34,0.4)`):** flat tail `[40, 35, 25, 15, 5, 5, 5, 5, 5, 5]` and dying tail `[40, 35, 25, 15, 8, 5, 3, 1, 0, 0]`; orange caption "CNN: both = \"right_skew\""; green: "Meta: flat tail has low" / "variance (=hidden cluster!)"
- **Caption (bottom center, `#555`):** "Meta detects DEGREE and ANOMALIES that categorical shape labels miss"

**Comparison: What Each Method Detects Well**

- **Histogram:** Visual shape, peaks, valleys. Misses: quantitative concentration degree.
- **CNN:** Shape class (bell, spike, bimodal). Misses: severity within a class, subtle structural anomalies.
- **Summary stats:** Mean, skew, kurtosis. Misses: multi-modality, structural holes.
- **KS test:** Departure from reference. Misses: WHERE and HOW much concentration.
- **Meta-distribution:** Concentration inequality, spike severity, structural anomalies (holes, flat tails, distributed spikes). Misses: spatial information (which bins are tall — left or right?).

**Meta's weakness:** It throws away positional information. It knows "one bin dominates" but not WHERE that bin is. That's why it complements (not replaces) the histogram/CNN — they know where, meta knows how much.

### Visualization (canvas `c10`, 720×280)

Four stacked rounded method rows (0.1-alpha fill + 2px stroke in method color; bold method name at x=50; "✓ detects" in `#333` and "✗ misses" in `#999` at x=150).

- **Title (bold `#1a5276`, top left):** "Method Strengths: Each Catches Different Things"
- Rows:
  1. Histogram — detects "Visual shape, peaks, valleys"; misses "Concentration degree" — blue `#2980b9`
  2. CNN — detects "Shape class (11 labels)"; misses "Severity within class" — gold `#f39c12`
  3. Stats — detects "Mean, skew, kurtosis"; misses "Multi-modality, holes" — purple `#8e44ad`
  4. Meta — detects "Concentration inequality, spike severity"; misses "Position (where)" — green `#27ae60`
- **Takeaway (bottom center, bold `#1a5276`):** "Meta complements (not replaces): knows HOW MUCH, others know WHERE"

## Regeneration instructions

- **Layout:** TOC-reference detail page: h1, `.subtitle`, `.toc` box (bold "Table of Contents" + ordered anchor list to ids `#idea`, `#two-methods`, `#what-it-encodes`, `#metrics`, `#ex-income`, `#ex-age`, `#ex-capital`, `#ex-hours`, `#summary`, `#unique`), then h2 sections each with one or more `.obj-table` blocks (left `<td>` 45% with `.obj-title` + bullets/paragraphs, right `<td>` 55% centered canvas; even rows `#fafcfe`). Note: the source page numbers both "What It Encodes" and "Useful Metrics" sections as "3." and examples E-H are extra obj-tables inside the Hours/Week section without their own h2. The `.philosophy` callout sits between sections 8 and 9.
- **Triple-panel helper:** examples use a shared `drawTriple(id, bins, title, color, gini, maxMean, occLabel)` — three equal panels separated by `#ddd` lines: (1) original histogram in the given color, log-scaled when max > 5000 with a gray "(log scale)" note; (2) frontier meta = 10-bin histogram of the heights, `rgba(230,126,34,0.5)`, labeled "Frontier Meta" + "Gini=…"; (3) occupancy profile bars `rgba(39,174,96,0.5)` over 10 levels (log-spaced levels when max/min > 100), labeled "Occupancy Profile" + shape note.
- **Page CSS:** body system sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px bottom border `#2980b9`; `.subtitle` `#666` 1.05em; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` `#1a5276`; `.toc` background `#f8fafb` border `#e0e0e0` radius 4px, links `#2980b9`; `.philosophy` background `#f0f4f8`, left border `4px solid #2980b9`, 0.9em. No nav bar, no back/home links.
- **Canvas:** intrinsic `width`/`height` per chart; shared `setup(id)` helper scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). All chart text 17px -apple-system.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, secondary blue `#2980b9`, purple `#8e44ad`, gold `#f39c12`, meta-bar fill `rgba(230,126,34,0.5)`, occupancy fill `rgba(39,174,96,0.5)`.
- In regenerated HTML, any card/anchor links use `.html` extensions.
