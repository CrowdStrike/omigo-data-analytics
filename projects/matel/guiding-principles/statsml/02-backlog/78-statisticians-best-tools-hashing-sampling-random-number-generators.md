# Statisticians' Best Tools — Hashing, Sampling, Random Number Generators

**Page type:** detail page (backlog-style two-column layout: text left 50%, canvas right 50%, one `.lang-section` per topic; h1 carries a BACKLOG status pill)
**HTML title tag:** Statisticians' Best Tools — Hashing, Sampling, Random Number Generators

**Subtitle:** Hashing, sampling, and random number generators do most of the work in large-scale statistics — and each fails in a way that looks like a valid result.

**Intro callout:** Three primitives carry almost every large-scale analysis: hashing turns an identity into a deterministic bucket without storing a table, sampling turns an intractable population into a tractable estimate, and a generator turns a seed into the randomness that simulation and randomization need. All three are cheap, which is why they are reached for before anyone checks the precondition each one quietly assumes.

## 1. Hashing as Deterministic Assignment

A hash is a lookup table you never have to store: the same identifier always lands in the same bucket.

- **No state to keep** — `bucket = hash(user_id + salt) mod 100` reproduces the same arm on every host.
- **Stateless is the point** — no assignment table to replicate, migrate, or keep consistent under retries.
- **Avalanche matters** — sequential ids need a mixed hash; a weak one leaves id structure in the bucket.
- **Uniform, not equal** — 10,000 users over 100 buckets gives 100 expected, SD √(10000·0.01·0.99) = 9.95.
- **Observed spread agrees** — the plotted buckets range 79 to 119 with SD 9.84, right at that 9.95.
- **A 50/50 split lands at 49.68/50.32** — buckets 0–49 hold 4,968 users, buckets 50–99 hold 5,032.
- **Never a cryptographic claim** — a bucketing hash is fast and non-secret, not a commitment or a MAC.

**Key point:** Bucket imbalance of a few percent is the expected binomial spread, not a broken hash.

### Visualization (canvas `c1`, 720×340)

Histogram of 10,000 users hashed into 100 buckets, with the theoretical ±1 SD band drawn over it.

- **Title (bold 16px, `#1a5276`, top center):** "10,000 Users, 100 Hash Buckets".
- **Subtitle (12px `#7f8c8d`, centered under title):** "Illustrative Example — FNV-1a with murmur3 finalizer, salt fixed".
- **Data (literal array, bucket 0 → 99, counts summing to 10,000):**
  `[95,96,112,111,93,98,81,89,103,108,103,88,102,107,109,96,97,115,102,103,98,93,93,104,104,104,92,113,113,100,101,92,107,107,82,108,96,83,112,119,93,86,93,88,98,86,112,102,97,84,90,94,92,118,97,116,119,103,100,95,80,110,107,83,98,92,97,100,106,108,86,97,92,113,97,104,103,104,95,103,95,99,113,107,118,103,107,99,111,97,98,119,79,115,86,98,92,109,107,81]`
- **Plot area:** x=64, y=76, width = canvas−110, height = canvas−146; L-shaped axes `#95a5a6` (1.4px).
- **Scales:** y from 0 to 130, tick labels every 26 (12px `#5a6875`, right-aligned); x is bucket index, labelled 0, 25, 50, 75, 99 (12px `#4a5866`) with axis label "Hash bucket" (13px, centered below).
- **Bars:** 100 slots, each bar `slot·0.8` wide, fill `rgba(26,82,118,0.35)`, no stroke.
- **Expected line:** solid green `#27ae60` 2px horizontal line at y=100, label "expected 100" (12px `#27ae60`, right of the plot's right edge minus 4px, above the line).
- **±1 SD band:** two dashed (dash 4/4, 1.4px) `#27ae60` horizontal lines at 100 ± SD, where SD = √(10000·0.01·0.99) computed in JS from n=10000 and p=0.01 — never hardcoded.
- **Computed footer (12px `#2c3e50`, centered below the x-axis label):** built at render time from the plotted array — "min <min> · max <max> · observed SD <sd, 2dp> · predicted SD <√(np(1−p)), 2dp>". With the array above this renders "min 79 · max 119 · observed SD 9.84 · predicted SD 9.95".

## 2. One Salt, Correlated Arms

Re-using a single hash across sequential experiments makes the second experiment a copy of the first.

- **The bug is invisible** — each experiment alone shows a clean, well-balanced 50/50 split.
- **Same input, same output** — `hash(user_id) mod 100` is a function, so experiment 2 repeats experiment 1.
- **Cross-tab collapses** — every user is A/A or B/B; the off-diagonal cells hold exactly 0 users.
- **φ = 1 exactly** — perfect arm correlation, so experiment 2 measures the carry-over, not its own treatment.
- **Per-experiment salt fixes it** — `hash(user_id + experiment_id)` re-randomizes each assignment.
- **With distinct salts** — the four cells hold 2,490 / 2,497 / 2,554 / 2,459 of 10,000 users.
- **That residue is noise** — φ = −0.0102, so χ² = 10000·φ² = 1.04 on 1 df, p ≈ 0.31.

**Key point:** The salt is what makes the assignment independent; without it the hash is deterministic *across* experiments too.

### Visualization (canvas `c2`, 720×340)

Two 2×2 cross-tabs of experiment-1 arm versus experiment-2 arm, shared salt beside per-experiment salt.

- **Title (bold 16px, `#1a5276`, top center):** "Same Salt Twice vs a Salt per Experiment".
- **Subtitle (12px `#7f8c8d`, centered under title):** "Illustrative Example — 10,000 simulated users".
- **Data (cells ordered A/A, A/B, B/A, B/B; each grid totals 10,000):**
  - shared salt: `[4968, 0, 0, 5032]`
  - per-experiment salt: `[2490, 2497, 2554, 2459]`
- **Layout:** two square grids, each side 132px, tops at y=104; left grid at x=96, right grid at x = canvas−96−132.
- **Cell fill:** blue `rgba(26,82,118,α)` with α = 0.10 + 0.70·(count / 5032) using the max across both grids, so the two panels share one intensity scale; cell border `#95a5a6` 1px.
- **Cell label (bold 13px, `#1a5276` on light cells, `#fff` where α > 0.55, centered):** the count, then the percent of 10,000 on a second line (11px) — both computed from the arrays.
- **Axis labels (12px `#4a5866`):** "exp 1: A / B" down the left of each grid (rotated −90°, centered on the grid's left edge minus 12px); "exp 2: A / B" under each grid.
- **Panel captions (bold 13px, centered above each grid at y=94):** left "one shared salt" in `#e74c3c`; right "salt per experiment" in `#27ae60`.
- **Computed footer per panel (12px, centered under the "exp 2" label):** φ computed at render time from that panel's four cells as (n11·n22 − n12·n21)/√(row1·row2·col1·col2), printed to 4dp with a typographic minus. Renders "φ = 1.0000" (in `#e74c3c`) on the left, "φ = −0.0102" (in `#27ae60`) on the right.

## 3. Sketches: Kilobytes for a Bounded Error

Hash-based sketches answer "how many distinct?" and "how often?" in fixed memory, giving up exactness.

- **HyperLogLog** — hashes each key, tracks the longest leading-zero run per register, then averages.
- **Its error is a formula** — relative standard error ≈ 1.04/√m for m registers, independent of cardinality.
- **m = 16,384 registers** — 1.04/√16384 = 0.813% error in 16384·5/8 = 10,240 bytes = 10.0 KB.
- **m = 65,536 registers** — error halves to 0.406%, memory quadruples to 40,960 bytes = 40.0 KB.
- **The exact alternative** — a billion 8-byte ids is 8 GB, about 195,000× the 40 KB sketch.
- **Count-min sketch** — d hash rows of w counters; per-key overcount only, never an undercount.
- **Sizing it** — ε = 0.001, δ = 0.01 needs w = ⌈e/ε⌉ = 2,719 and d = ⌈ln(1/δ)⌉ = 5, so 53.1 KB.
- **Where it bites** — collisions inflate rare keys, so a sketch is safe for heavy hitters, wrong for the tail.

**Key point:** A sketch's error is a design parameter, so state it beside the estimate rather than reporting the estimate alone.

### Visualization (canvas `c3`, 720×340)

Log-x scatter with a fitted curve: HyperLogLog standard error versus register count, on a log memory axis.

- **Title (bold 16px, `#1a5276`, top center):** "HyperLogLog: Error Halves as Memory Quadruples".
- **Data (m, computed in JS — do not hardcode error or bytes):** m ∈ `[256, 1024, 4096, 16384, 65536, 262144]`; for each, error = 1.04/√m and bytes = m·5/8.
- **Plot area:** x=76, y=72, width = canvas−140, height = canvas−140; L-shaped axes `#95a5a6` (1.4px).
- **X scale (log₁₀ bytes):** from 128 B to 256 KB, ticks at 160 B, 640 B, 2.5 KB, 10 KB, 40 KB, 160 KB — each label computed from the corresponding m (12px `#4a5866`, centered); axis label "Sketch size" (13px, centered below).
- **Y scale (log₁₀ error %):** from 0.15% to 8%, tick labels 0.2, 0.5, 1, 2, 5 (12px `#5a6875`, right-aligned, "%" suffix); axis label "Relative standard error" rotated −90° at x=22.
- **Curve:** the six points joined by straight segments, stroke `#1a5276` 2.5px; filled circles radius 5 `#1a5276`.
- **Highlighted point:** the m = 16384 point drawn as a radius-7 open circle, stroke `#e67e22` 2.5px, with label "10.0 KB → 0.81%" (13px `#e67e22`, left-aligned 10px right of the point) — both numbers formatted from the computed bytes and error.
- **Slope annotation (12px `#27ae60`, along the curve near the m = 4096 point, below the line):** "slope −1/2 on log–log" — verified in JS by checking |log(e₂/e₁)/log(b₂/b₁) + 0.5| < 0.01 across consecutive points before the label is drawn.

## 4. Reservoir Sampling on an Unbounded Stream

Reservoir sampling draws k items uniformly from a stream whose length is unknown until it ends.

- **Algorithm R** — keep the first k; for item i > k, admit it with probability k/i, evicting a uniform slot.
- **One pass, k slots** — memory is the sample size, not the stream length, so length can stay unknown.
- **The invariant** — after i items, every item so far is in the reservoir with probability exactly k/i.
- **At the end** — with k = 10 and n = 200 every position has inclusion probability 10/200 = 5%.
- **The lazy alternative** — "first 10 rows" gives positions 1–10 probability 100% and the rest 0%.
- **Why that is fatal** — streams are time-ordered, so the truncated sample is a sample of one hour.
- **Simulated check** — 4,000 seeded replications give mean inclusion 5.000% across the 200 positions.
- **Spread is Monte Carlo noise** — SE = √(0.05·0.95/4000) = 0.345pp, and 12 of 200 fall outside ±1.96 SE.

**Key point:** Under-coverage from truncating a stream is a bias no sample-size increase can remove.

### Visualization (canvas `c4`, 720×340)

Per-position inclusion rate from a seeded reservoir simulation, against the flat 5% target and the truncation baseline.

- **Title (bold 16px, `#1a5276`, top center):** "Inclusion Probability by Stream Position".
- **Subtitle (12px `#7f8c8d`, centered under title):** "Illustrative Example — Algorithm R, k = 10, n = 200, 4,000 seeded replications".
- **Simulation (in JS, seeded — never `Math.random()`):** one generator `var rnd = lcg(20260908)` for the whole chart; for each of 4,000 trials, fill the reservoir with positions 0–9, then for i = 10…199 admit i when `rnd() < 10/(i+1)`, evicting slot `Math.floor(rnd()*10)`; tally per-position inclusion counts and divide by 4,000.
- **Plot area:** x=70, y=82, width = canvas−124, height = canvas−152; L-shaped axes `#95a5a6` (1.4px).
- **Scales:** y from 0% to 8%, ticks every 2% (12px `#5a6875`, right-aligned); x = stream position 1…200, labels 1, 50, 100, 150, 200; axis label "Position in stream" (13px, centered below).
- **Reservoir series:** the 200 simulated rates as a thin polyline, stroke `#1a5276` 1.4px.
- **Target line:** solid green `#27ae60` 2px at y = k/n computed as 10/200, label "uniform target k/n = 5.0%" (12px `#27ae60`, left-aligned just above the line at x = plotX+8).
- **Monte Carlo band:** dashed `#27ae60` (dash 4/4, 1.2px) at 5% ± 1.96·√(0.05·0.95/4000), both bounds computed in JS.
- **Truncation baseline:** red `#e74c3c` 2.5px step — 8% (clipped at the top of the scale, drawn at the axis maximum) for positions 1–10, then 0% for 11–200; label "take first 10: 100% then 0%" (12px `#e74c3c`, left-aligned at x = plotX+8, y just below the top of the plot).
- **Computed footer (12px `#2c3e50`, centered under the axis label):** from the simulated array — "mean <3dp>% · min <2dp>% · max <2dp>% · outside ±1.96 SE: <count>/200". With seed 20260908 this renders "mean 5.000% · min 4.00% · max 6.13% · outside ±1.96 SE: 12/200".

## 5. Stratified Beats Simple When the Tail Carries the Mean

Simple random sampling is unbiased but can be uselessly noisy when a tiny stratum dominates the total.

- **Illustrative population** — 1% enterprise accounts at mean 1,000 (SD 200), 99% small at mean 10 (SD 5).
- **Population mean** — 0.01·1000 + 0.99·10 = 19.9 per account.
- **Population SD** — √(E[X²] − μ²) = 100.64, driven almost entirely by the 1% stratum.
- **That 1% is half the total** — 0.01·1000 / 19.9 = 50.25% of all revenue sits in the enterprise stratum.
- **Simple random, n = 10,000** — SE = 100.64/√10000 = 1.006, a 5.06% coefficient of variation.
- **Proportional stratified, n = 10,000** — SE = √((0.01·200² + 0.99·5²)/10000) = 0.206, CV 1.04%.
- **4.88× tighter for free** — matching that SE with simple random sampling needs n ≈ 238,000.
- **Small samples miss it entirely** — at n = 200, P(zero enterprise accounts) = 0.99²⁰⁰ = 13.4%.

**Key point:** Stratifying removes between-stratum variance from the estimator, so the gain grows with how unequal the strata are.

### Visualization (canvas `c5`, 720×340)

Grouped bars comparing the standard error of the mean under simple random and proportional stratified sampling across sample sizes.

- **Title (bold 16px, `#1a5276`, top center):** "Standard Error: Simple Random vs Proportional Stratified".
- **Subtitle (12px `#7f8c8d`, centered under title):** "Illustrative Example — 1% of accounts at mean 1,000 (SD 200), 99% at mean 10 (SD 5)".
- **Computation (in JS, from the stated parameters — no hardcoded SEs):** μ = 0.01·1000 + 0.99·10; σ² = 0.01·(200²+1000²) + 0.99·(5²+10²) − μ²; for each n, seSRS = √(σ²/n) and seStrat = √((0.01·200² + 0.99·5²)/n).
- **Sample sizes (x categories):** `[1000, 5000, 10000, 50000]`, labelled "n = 1,000" … "n = 50,000".
- **Plot area:** x=70, y=84, width = canvas−124, height = canvas−156; scale max 3.4; L-shaped axes `#95a5a6` (1.4px).
- **Scales:** y tick labels every 0.85 formatted to 2dp (12px `#5a6875`, right-aligned); axis label "Standard error of the mean" rotated −90° at x=20; x axis label "Sample size" (13px `#4a5866`, centered below the n labels).
- **Bars:** 4 slots; two bars per slot each 0.34·slot wide — left (simple random) fill `rgba(231,76,60,0.50)` stroke `#e74c3c` 1.4px; right (stratified) fill `rgba(39,174,96,0.50)` stroke `#27ae60` 1.4px.
- **Bar value labels (11px, centered above each bar, matching the bar's stroke colour):** the computed SE to 3dp.
- **Legend (top-left inside plot, 13px `#2c3e50`):** red swatch + "simple random", green swatch + "proportional stratified".
- **Ratio annotation (13px `#1a5276`, centered above the n = 10,000 slot):** "<ratio, 2dp>× tighter", computed as seSRS/seStrat at that n — renders "4.88× tighter" and is constant across n, since both SEs scale as 1/√n.

## 6. Sampling a Join Is Not the Join of the Samples

Independently sampling two tables at rate p leaves only p² of the matching pairs, and the survivors are not a random subset.

- **The rate collapses** — two tables at 10% each retain 0.10² = 1% of joined rows, a 10× undercount.
- **The bias is worse than the loss** — survival is not uniform across the entities being joined.
- **Illustrative population** — 800 customers with 2 orders each, 200 with 20 orders each; 5,600 orders total.
- **True mean** — 5,600 / 1,000 = 5.6 orders per customer.
- **Sample orders at 10%** — a light customer appears with probability 1 − 0.9² = 19.0%.
- **A heavy customer appears** — with probability 1 − 0.9²⁰ = 87.8%, so heavy accounts nearly always survive.
- **The join skews** — heavy customers are 20% of the population but 53.6% of the 327.7 expected survivors.
- **The estimate breaks** — 560 sampled orders / 327.7 customers ÷ 0.1 = 17.1, a 3.05× overstatement.
- **The fix** — sample on the join key: hash the customer id, keep every row for the chosen customers.

**Key point:** Sample the join key, not the rows, so each retained entity keeps its complete fan-out.

### Visualization (canvas `c6`, 720×340)

Grouped bars: true population composition beside the composition surviving a naive per-table 10% sample.

- **Title (bold 16px, `#1a5276`, top center):** "Who Survives a Naive 10% Sample of Both Tables".
- **Subtitle (12px `#7f8c8d`, centered under title):** "Illustrative Example — 800 customers × 2 orders, 200 customers × 20 orders".
- **Computation (in JS from those parameters):** pLight = 1 − 0.9², pHigh = 1 − 0.9²⁰; expected survivors 800·pLight = 152.0 and 200·pHigh = 175.7; shares of the 327.7 total.
- **Categories (3 slots):** "light customers (2 orders)", "heavy customers (20 orders)", "mean orders / customer".
- **Series:** blue = truth, orange = what the naive sample implies. For slots 1–2 the bars are percent shares (80.0 / 20.0 true; 46.4 / 53.6 sampled); for slot 3 the bars are the mean orders per customer (5.6 true; 17.09 naive, computed as 560/327.7/0.1).
- **Dual scale:** slots 1–2 use a 0–100 percent scale; slot 3 uses its own 0–20 scale, drawn with a light `#e0e0e0` separator line at the slot boundary and its own right-hand axis (ticks 0, 5, 10, 15, 20, 12px `#5a6875`, left-aligned outside the plot's right edge).
- **Plot area:** x=70, y=90, width = canvas−140, height = canvas−162; L-shaped axes `#95a5a6` (1.4px); left ticks 0, 25, 50, 75, 100 with "%" suffix.
- **Bars:** two per slot, each 0.30·slot wide — left fill `rgba(26,82,118,0.35)` stroke `#1a5276` 1.4px; right fill `rgba(230,126,34,0.50)` stroke `#e67e22` 1.4px. Value labels (11px, above each bar, matching stroke colour) formatted to 1dp for shares and 2dp for the means.
- **Legend (top-left inside plot, 13px `#2c3e50`):** blue swatch + "true population", orange swatch + "naive 10% × 10% sample".
- **Annotation (13px `#e74c3c`, centered above the third slot's orange bar):** "<ratio, 2dp>× overstated", computed as naiveMean/trueMean — renders "3.05× overstated".

## 7. Seeding: When Parallel Workers Share One Stream

A fixed seed makes a simulation reproducible; the same fixed seed on every worker makes it a single replicate.

- **Reproducibility needs a seed** — an unseeded run cannot be re-derived, audited, or bisected.
- **The failure** — each worker calls the same fixed seed value, so all W workers draw an identical stream.
- **Nothing looks wrong** — the run completes, the histogram looks fine, the estimate has the right sign.
- **Effective sample size** — W identical replicates carry the information of 1, not of W.
- **SE is understated by √W** — with 8 workers the reported interval is 2.83× narrower than the truth.
- **Coverage collapses** — a nominal 95% interval covers 83.4% at W = 2, 51.2% at W = 8, 37.6% at W = 16.
- **Coverage formula** — 2Φ(1.96/√W) − 1; the true error is √W times the reported SE.
- **The fix** — derive per-worker seeds from one master seed, or use a generator with a stream/counter parameter.

**Key point:** Reproducibility requires one recorded master seed, not one identical seed per worker.

### Visualization (canvas `c7`, 720×340)

Bar chart of realised coverage of a nominal 95% interval as the number of workers sharing a seed grows.

- **Title (bold 16px, `#1a5276`, top center):** "Coverage of a Nominal 95% Interval When Workers Share a Seed".
- **Computation (in JS):** for W ∈ `[1, 2, 4, 8, 16, 32]`, coverage = 2Φ(1.96/√W) − 1, with Φ from an Abramowitz–Stegun erf approximation included inline. Nothing hardcoded.
- **Plot area:** x=70, y=76, width = canvas−124, height = canvas−148; scale 0–100%; L-shaped axes `#95a5a6` (1.4px).
- **Scales:** y ticks every 20% (12px `#5a6875`, right-aligned, "%" suffix); x labels "1", "2", "4", "8", "16", "32" (13px `#4a5866`) with axis label "Workers sharing one seed" (13px, centered below).
- **Bars:** 6 slots, each bar 0.5·slot wide; fill `rgba(26,82,118,0.35)` when coverage ≥ 0.90, `rgba(230,126,34,0.50)` for 0.60–0.90, `rgba(231,76,60,0.50)` below 0.60; stroke the matching solid colour 1.4px.
- **Bar value labels (12px, centered above each bar, matching stroke colour):** the computed coverage to 1dp with "%".
- **Nominal line:** dashed green `#27ae60` (dash 5/4, 2px) at 95%, label "nominal 95%" (12px `#27ae60`, right-aligned at the plot's right edge, just above the line).
- **Annotation (12px `#e74c3c`, left-aligned inside the plot near the W = 16 bar):** "reported SE is √W too small" — the √W factor for the labelled bar printed from the computed value.

## 8. Modulo Bias and the Rejection Fix

Reducing a random integer to a range with `mod` splits the range into slightly-likelier and slightly-less-likely values.

- **The mechanism** — 256 equally likely bytes over 100 buckets leaves 256 = 2·100 + 56 unevenly split.
- **Two tiers** — buckets 0–55 have 3 preimages (1.1719% each), buckets 56–99 have 2 (0.7813% each).
- **The ratio is 1.5** — a bucket-0 assignment is 50% likelier than a bucket-99 one, from arithmetic alone.
- **A 50/50 split is not 50/50** — buckets 0–49 collect 58.59% of users, buckets 50–99 get 41.41%.
- **Wide sources hide it** — a 32-bit value mod 100 has relative excess 1/42,949,672 ≈ 2.3×10⁻⁸, negligible.
- **Narrow sources do not** — the bias scales as range/source-size, so it bites when the range is large.
- **The rejection fix** — discard draws ≥ 200 (the largest multiple of 100 ≤ 256), then take mod 100.
- **Its cost is small** — acceptance is 200/256 = 78.125%, so 256/200 = 1.28 draws per usable value.

**Key point:** Modulo is only unbiased when the source size is an exact multiple of the range; otherwise reject the overhang.

### Visualization (canvas `c8`, 720×340)

Bar chart of per-bucket probability under naive modulo, showing the two-tier step, with the uniform target overlaid.

- **Title (bold 16px, `#1a5276`, top center):** "Modulo Bias: 256 Byte Values into 100 Buckets".
- **Computation (in JS):** for each bucket b in 0…99, count preimages as `Math.floor(256/100) + (b < 256 % 100 ? 1 : 0)`, then probability = count/256. Every printed figure derives from this array.
- **Plot area:** x=76, y=80, width = canvas−130, height = canvas−152; L-shaped axes `#95a5a6` (1.4px).
- **Scales:** y from 0 to 1.4 (percent), ticks every 0.35 formatted 2dp with "%" (12px `#5a6875`, right-aligned); x labels 0, 25, 50, 55, 75, 99 (11px `#4a5866`) with axis label "Bucket (value mod 100)" (13px, centered below).
- **Bars:** 100 slots, each bar 0.85·slot wide; fill `rgba(231,76,60,0.50)` stroke `#e74c3c` 0.8px for the 3-preimage buckets, fill `rgba(26,82,118,0.35)` stroke `#1a5276` 0.8px for the 2-preimage ones.
- **Uniform target:** dashed green `#27ae60` (dash 4/4, 2px) horizontal line at 1/100 = 1.00%, label "uniform 1.00%" (12px `#27ae60`, right-aligned at the plot's right edge, above the line).
- **Step annotation (12px, above the bars):** red "3 preimages: <p, 4dp>%" over bucket ~25; blue "2 preimages: <p, 4dp>%" over bucket ~78 — both read from the computed array, rendering 1.1719% and 0.7813%.
- **Computed footer (12px `#2c3e50`, centered under the axis label):** "buckets 0–49: <sum, 2dp>% of draws · buckets 50–99: <sum, 2dp>% · rejection sampling accepts <200/256, 3dp>%", summing the computed array — renders "buckets 0–49: 58.59% of draws · buckets 50–99: 41.41% · rejection sampling accepts 78.125%".

## Regeneration instructions

- **Layout:** backlog detail page. `h1` (2rem `#1a5276`, bottom border `2px solid #2980b9`) with inline `.status` pill "BACKLOG" (background `#fef9e7`, border `1px solid #f39c12`, text `#b7950b`, 4px radius, 0.8rem); `.subtitle` (`#666`, 0.95rem); `.intro` callout (background `#f0f4f8`, left border `3px solid #2980b9`, 8px 12px padding, 0.9rem). One `.lang-section` per numbered h2 (1.3rem `#1a5276`, bottom border `2px solid #2980b9`); inside each, `table.layout` with `td.text-col` 50% and `td.viz-col` 50%, both `vertical-align: top`, 12px padding. No index number in the `h1` or `<title>`.
- **Text blocks:** intro `<p>`, `<ul>` bullets (0.92rem) with `<strong>` lead-ins, `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, 0.9rem).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. Canvases `width: 100%`, `height: auto`, `1px solid #e0e0e0` border, 4px radius.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange; bar fill `rgba(26,82,118,0.35)`; gray labels `#5a6875`/`#4a5866`/`#7f8c8d`, axes `#95a5a6`.
- **Canvas:** intrinsic 720×340. A shared `setup(id)` helper stores the intrinsic size in `dataset`, caps display width via `style.maxWidth`, sizes the backing store to rendered width × `window.devicePixelRatio`, and `ctx.scale`s back to logical coordinates. Chart functions are pushed into a `__charts` array, invoked once, and re-invoked on a 150ms-debounced `resize`.
- **Randomness:** only chart `c4` generates data, using the inline seeded generator `function lcg(seed){var s=seed;return function(){s=(s*16807)%2147483647;return s/2147483647;};}` with seed 20260908. `Math.random()` must not appear anywhere on the page. Every statistic printed beside generated or derived data is computed at render time from the plotted values.
- **No cross-reference, back, or home links of any kind.**
