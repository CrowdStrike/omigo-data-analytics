# CNN Shape Classification — Results (Multi-Rendering Pipeline)

**Page type:** other (results dashboard: metrics banner, 3-per-row chart grids of procedurally generated silhouettes, 2-per-row confusion pair cards, full-width noise-artifact cards with softmax bar tables, takeaway list; all canvases created and rendered by JS at load time)
**HTML title tag:** CNN Shape Classification — Results

**Intro line (gray `#666`):** 11 shape classes. 330K training images (3 renderings × 10K per class). Same CNN classifies each rendering; disagreement logic detects artifacts.

## Metrics banner

Four `.metric-box` tiles (big number + small label):

| Number | Label |
|--------|-------|
| 95.8% | Combined Top-1 |
| 99.6% | Combined Top-2 |
| 94.6% | Agreement Rate |
| 95.7% | Best Val Acc |

**Footnote (0.8em `#666`):** Per-rendering: hist_sturges 94.5%, hist_√n 95.8%, KDE 95.9%. Multi-rendering combined Top-2 reaches 99.6%. All 3 renderings agree 94.6% of the time.

## Per-Class Accuracy

Each cell shows a representative example of the class and its validation accuracy.

Grid of 11 `.chart-cell` tiles (3 per row). Each holds a silhouette canvas (25-bin smoothed histogram of seeded synthetic data, size 220, drawn in the class color), the class name label, and the accuracy number colored green (≥97%), orange (90-97%), or red (<90%).

| Class | Accuracy | Color class | Synthetic data generator (seed = 1000 + i·53, n) |
|-------|----------|-------------|--------------------------------------------------|
| bell | 94.2% | mid | Normal(50, 12), n=800 |
| right_skew | 89.4% | low | sum of 3 exponentials × 5 (Gamma-3), n=800 |
| left_skew | 84.2% | low | 100 − lognormal(μ=2, σ=0.7), n=800 |
| bimodal | 97.0% | high | Normal(30,7) ×400 + Normal(65,7) ×400 |
| multimodal | 96.6% | mid | Normal(20,4) ×270 + Normal(50,4) ×260 + Normal(80,4) ×270 |
| uniform | 99.8% | high | Uniform(0,100), n=800 |
| descending | 98.8% | high | Exponential ×10, n=800 |
| ascending | 97.0% | high | 100 + log(U)·15 (mirrored exponential), n=800 |
| u_shaped | 99.2% | high | Beta-like b1/(b1+b2)·80 with b=U^0.4, n=800 |
| spike | 98.8% | high | 50 + Normal(0,0.3) ×750 + Uniform(0,100) ×50 |
| heavy_tail | 98.4% | high | Student-t(3) × 5, n=800 |

**Per-class silhouette colors:** bell `#2980b9`, right_skew `#e67e22`, left_skew `#8e44ad`, bimodal `#16a085`, multimodal `#27ae60`, uniform `#3498db`, descending `#e74c3c`, ascending `#c0392b`, u_shaped `#9b59b6`, spike `#7f8c8d`, heavy_tail `#d35400`.

## Realistic Data — Correct

Real-world-like distributions correctly classified. The number below is the model's confidence.

Grid of 9 `.chart-cell success` tiles: silhouette canvas (size 220, class color), class label, green confidence number, gray description.

| Class | Confidence | Description | Synthetic data generator (seed = 3000 + i·67) |
|-------|------------|-------------|-----------------------------------------------|
| bell | 97% | Employee ages | round(Normal(35,8)) clamped to [18,65], n=800 |
| right_skew | 97% | Monthly income | exp(10.5 + 0.8·Normal(0,1)), n=2000 |
| bimodal | 98% | Commute: walk+drive | Normal(10,3) ×300 + Normal(35,8) ×500 |
| multimodal | 97% | Shift starts: 7/15/23 | Normal(7,0.5) ×300 + Normal(15,0.5) ×350 + Normal(23,0.5) ×250 |
| uniform | 98% | Day-of-month txns | floor(U·30)+1, n=1200 |
| descending | 84% | Days since login | Exponential ×12, n=800 |
| heavy_tail | 97% | Stock returns (t-3) | Student-t(3) × 2.5, n=1000 |
| spike | 96% | Price=9.99 (99%) | 9.99 + Normal(0,0.01) ×800 + Uniform(1,21) ×100 |
| u_shaped | 97% | Temp extremes | Beta-like b1/(b1+b2)·50 − 10 with b=U^0.4, n=700 |

## Boundary Cases — Struggles

Misclassified or low-confidence. Red = wrong prediction, orange = correct but low confidence.

Grid of 5 tiles. Misses show label "true → predicted" and red "N% conf"; correct-but-low show label and orange "N%".

| True class | Predicted | Confidence | Description | Synthetic data generator (seed = 6000 + i·83) |
|------------|-----------|------------|-------------|-----------------------------------------------|
| left_skew | ascending (miss) | 4% conf | Ratings 1-5 (ceiling) | 5.5 − exp(0.3 + 0.6·Normal(0,1)), clamped [0.5, 5], n=800 |
| left_skew | ascending (miss) | 1% conf | Delivery days (max 30) | min(30, 30 + log(U)·4), n=600 |
| bimodal | multimodal (miss) | 0% conf | 250g vs 500g (narrow) | Normal(250,5) ×400 + Normal(500,8) ×350 |
| bell | bell | 72% | Normal n=100 (noisy) | Normal(35,8), n=100 |
| right_skew | right_skew | 70% | Gamma(5) mild skew | sum of 5 exponentials × 4, n=500 |

## Confused Pairs — Side by Side

Why these get confused. At 64x64 pixels, these silhouettes are genuinely similar.

Four `.pair-card` boxes (2 per row): centered h4 title, two silhouette canvases (size 200, class colors, seeds 8000 + i·99 for side A, 9000 + i·77 for side B) separated by a gray "vs", then an orange-left-bordered reason box.

| Pair title | Side A generator | Side B generator | Reason |
|------------|------------------|------------------|--------|
| left_skew vs ascending | 100 − lognormal(2, 0.7), n=800 | 80 + log(U)·10, n=800 | Ceiling effect: data piling at a boundary looks like ascending. Does the curve drop off after the peak (skew) or hit the edge at maximum (ascending)? |
| right_skew vs descending | Gamma-3 × 5, n=800 | Exponential ×10, n=800 | Peak AT the left edge (descending) vs peak NEAR the edge (right_skew). At 64px, 1-2 pixels of difference. |
| right_skew vs bell | Gamma-6 × 3, n=800 | Normal(50,12), n=800 | Mild skew (Gamma shape>5) looks almost symmetric. A human would also debate: "slightly skewed" or "basically normal." |
| bimodal vs multimodal | Normal(25,3) ×400 + Normal(75,3) ×400 | Normal(15,3) ×270 + Normal(45,3) ×260 + Normal(75,3) ×270 | Narrow well-separated peaks look like "multiple spikes" not "two hills." Peak WIDTH relative to separation determines perception. |

## Noise Artifacts — Multi-Rendering Classification

The CNN classifies the same data rendered 3 ways: histogram at two bin sizes + KDE density. When all 3 agree, confidence is high. When they disagree, it signals an artifact — the disagreement pattern reveals the corruption type.

Five full-width `.noise-card` boxes. Each has: centered h4 title; green "Clean: <label>" header over a 3-panel row (raw histogram at Sturges bins, raw histogram at √n bins, KDE density — clean data drawn green `#27ae60`, KDE line `#8e44ad`); red "Corrupted: <label>" header over the same 3-panel row for corrupted data (histograms drawn red `#e74c3c`); then a 3-column softmax bar table ("Hist (Sturges)" / "Hist (√n)" / "KDE density", corrupted data), an orange **Verdict** box, and a blue-bordered insight box. Bin counts: Sturges = max(8, ceil(log2(n)+1)); √n = max(10, ceil(√n)). Panel canvases size 250; clean data seed 11000 + i·111, corrupted seed 12000 + i·131. Softmax bar colors: ≥50% red `#c62828`, 10-49% orange `#e65100`, <10% blue `#1a5276`; zero-valued classes omitted.

### Case 1: Missing data gap (looks multimodal)

- **Clean:** bell — Normal(45,12), n=1000. **Corrupted:** bell + gap — same but values in (40,50) deleted.
- **Softmax Hist (Sturges):** multimodal 98, bimodal 1, spike 1
- **Softmax Hist (√n):** multimodal 49, bimodal 30, u_shaped 10, heavy_tail 5, bell 3, right_skew 1, left_skew 1, spike 1
- **Softmax KDE:** multimodal 98, bell 1, spike 1
- **Verdict:** ALL AGREE on multimodal (49-98%). All 3 renderings see the gap as real structure. CNN cannot distinguish from true multimodal.
- **Insight:** When data has values deleted in a range, the gap creates genuine visual structure in all renderings. KDE smoothing doesn't recover the bell because the gap is wide enough to survive it.

### Case 2: Rounding to nearest 5 (comb)

- **Clean:** continuous — Normal(60,15), n=800. **Corrupted:** rounded to 5s — round(x/5)·5.
- **Softmax Hist (Sturges):** bell 61, multimodal 15, spike 10, right_skew 6, bimodal 4, heavy_tail 2, left_skew 1, u_shaped 1
- **Softmax Hist (√n):** bell 42, multimodal 25, spike 14, bimodal 8, right_skew 5, heavy_tail 3, left_skew 1, uniform 1, u_shaped 1
- **Softmax KDE:** bell 92, right_skew 3, left_skew 2, heavy_tail 1, multimodal 1, u_shaped 1
- **Verdict:** AGREE on bell (42-92%). KDE strongly recovers the bell envelope. Model correctly identifies underlying shape despite rounding.
- **Insight:** KDE smooths over the comb teeth (92% bell). Histograms are confused by the spikes but still lead with bell (42-61%). When all 3 agree on top-1 but KDE is much more confident → discrete/rounded data.

### Case 3: Small sample n=150 (noisy)

- **Clean:** n=1000 — Normal(50,10). **Corrupted:** n=150 — same distribution, only 150 samples.
- **Softmax Hist (Sturges):** bell 92, right_skew 3, multimodal 2, uniform 1, heavy_tail 1, u_shaped 1
- **Softmax Hist (√n):** bell 94, right_skew 2, multimodal 1, uniform 1, heavy_tail 1, u_shaped 1
- **Softmax KDE:** bell 93, right_skew 3, left_skew 1, heavy_tail 1, multimodal 1, u_shaped 1
- **Verdict:** ALL AGREE: bell 92-94% across all renderings. Model is robust to small-N noise at n=150.
- **Insight:** Even with n=150, all 3 renderings correctly identify bell with high confidence (92-94%). The training jitter made the model robust to sampling noise.

### Case 4: True bimodal vs gap artifact

- **Clean:** true bimodal — 50/50 mixture Normal(35,8) / Normal(55,8), n=500. **Corrupted:** bell + deleted middle — Normal(45,12), n=700, values in (40,50) deleted.
- **Softmax Hist (Sturges):** bimodal 89, multimodal 5, u_shaped 3, heavy_tail 1, bell 1, spike 1
- **Softmax Hist (√n):** bimodal 86, multimodal 6, u_shaped 4, heavy_tail 2, bell 1, spike 1
- **Softmax KDE:** bimodal 84, multimodal 6, u_shaped 4, bell 3, heavy_tail 2, spike 1
- **Verdict:** ALL AGREE: bimodal 84-89% across all renderings. Indistinguishable from true bimodal. CNN cannot help here.
- **Insight:** When all 3 renderings produce nearly identical softmax outputs AND the shape matches a clean archetype → CNN alone cannot detect the artifact. Requires metadata check: was data filtered/censored?

### Case 5: Outlier cluster at 95

- **Clean:** clean bell — Normal(50,10), n=800. **Corrupted:** bell + 5% outliers — Normal(50,10) ×760 + Normal(95,2) ×40.
- **Softmax Hist (Sturges):** heavy_tail 42, right_skew 25, bell 18, bimodal 8, multimodal 4, left_skew 1, uniform 1, spike 1
- **Softmax Hist (√n):** right_skew 92, heavy_tail 4, bell 2, bimodal 1, spike 1
- **Softmax KDE:** heavy_tail 90, right_skew 5, bell 3, bimodal 1, spike 1
- **Verdict:** DISAGREE: Sturges=heavy_tail(42%), √n=right_skew(92%), KDE=heavy_tail(90%). Disagreement signals artifact.
- **Insight:** Coarse bins (Sturges) are uncertain because the cluster is a small bump. Fine bins (√n) see clear rightward mass and call right_skew. KDE smooths it into heavy_tail. Disagreement between bin sizes → check for outlier clusters.

## Practical Guidance

**Green note box:**

**All 3 agree with >90% confidence:** almost always correct — trust the verdict.
**All 3 agree, 60-90%:** likely correct, report top-2 candidates.
**KDE disagrees with histograms:** artifact detected — report the disagreement pattern.
**All 3 below 60%:** genuinely ambiguous — combine CNN + algorithmic methods.

**Takeaway list (blue-left-bordered items):**

- **97.8% combined top-2 on realistic data** — correct shape almost always in top 2 candidates when using multi-rendering
- **+3.5% over single rendering:** gains come from artifact cases where KDE rescues histograms (rounding, small n, outlier clusters)
- **Disagreement = information:** when renderings disagree, the pattern identifies the artifact type (rounding, gap, noise, outliers)
- **Failures at genuine boundaries:** left_skew vs ascending (ceiling), right_skew vs descending (peak near vs at edge) — these persist across all renderings
- **Unfixable case:** true bimodal vs gap artifact produces identical outputs across all 3 renderings — requires domain knowledge
- **810K params × 3 inferences, sub-ms total** — no GPU needed. Same model, different inputs.

## Regeneration instructions

- **Layout:** single page, all content built by JS on DOMContentLoaded. Order: h1, gray intro paragraph, `.metrics-banner` (grid `repeat(4, 1fr)`, gap 12px; 2 columns below 600px), footnote paragraph, then h2 sections: Per-Class Accuracy (`#classGrid`), Realistic Data — Correct (`#successGrid`), Boundary Cases — Struggles (`#struggleGrid`) — all `.chart-grid` `repeat(3, 1fr)` gap 20px (2 cols below 900px, 1 below 600px); Confused Pairs (`#confusionGrid`, `.pair-grid` `repeat(2, 1fr)`, 1 col below 900px); Noise Artifacts (`#noiseGrid`, `.pair-grid` overridden to 1 column); Practical Guidance (`.note` + `.takeaway-list`). Each section h2 is preceded by a one-line `.section-desc` paragraph (text captured above). No nav bar, no back/home links.
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto), background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a3a4a` with `border-bottom: 2px solid #1a5276`, margin 50px 0 16px; h3 1.1em `#1a5276`; p `#3a3a3a`, max-width 900px.
- **Tile styles:** `.metric-box` — background `#fafcfe`, border `1px solid #e0e0e0`, `#1a5276`, radius 10px, padding 18px 12px, centered; `.number` 2.2em weight 600, `.label` 0.75em opacity 0.7. `.chart-cell` — background `#fafcfe`, radius 10px, border `1px solid #e0e0e0`, centered, canvas full-width; `.cell-label` 0.95em weight 500 `#1a5276`; `.cell-acc` 1.6em weight 600, `.high` `#2e7d32`, `.mid` `#e65100`, `.low` `#c62828`; `.cell-desc` 0.85em `#888`. `.pair-card` — same surface, padding 20px; `.pair-vs` 1.4em `#ccc`; `.pair-reason` 0.88em `#555`, background `#f8f9fa`, radius 6px, left border `3px solid #e65100`. `.noise-card` — same surface; `.multi-view` 3-column grid gap 8px, panel canvases bordered `#e8e8e8` radius 4px, `.mv-label` 0.7em `#888`; `.multi-view-header` 0.75em weight 500 (clean header inline `#2e7d32`, corrupted inline `#c62828`); `.noise-insight` 0.78em `#555` background `#f8f9fa`, left border `3px solid #1a5276` (verdict variant: border `#e65100`, background `#fff8e1`, bold "Verdict:" prefix). Softmax rows: 0.72em, monospace label width 75px right-aligned, bar track `#f0f0f0` height 14px radius 3px, bar width = max(2, value)%, monospace pct width 38px. `.note` — background `#e8f5e9`, left border `4px solid #2e7d32`, padding 14px 18px, radius 0 8px 8px 0, 0.9em, max-width 800px; `.warn` (defined, unused) — `#fff3e0` / `#e65100`. `.takeaway-list` — no bullets; items background `#f8f9fa`, radius 6px, left border `4px solid #1a5276`, padding 10px 15px, 0.9em, strong `#1a5276`.
- **Data generation:** deterministic `mulberry32(seed)` PRNG plus Box-Muller `genNormal`; per-tile seeds as listed in the tables above. Data is clipped to the 1st-99th percentile before rendering (skip clipping if fewer than 20 values remain).
- **Renderers (all use `window.devicePixelRatio` scaling — backing store = rendered width × dpr, CSS width 100%, `ctx.scale` back to logical coordinates; background `#fafcfe`; thin `#bbb` baseline):**
  - `renderSilhouette(size, color)`: height = 0.4·size, 25 bins, histogram bars at 0.5 alpha in the class color, Gaussian-smoothed line (σ=1.2, width 1.5) and orange SE band `rgba(230, 126, 34, 0.14)` using ±1.96·smoothed/√effN with effN = clip(n, 30, 200).
  - `renderRawHistogram(numBins, size, color)`: height = 0.5·size, bars 0.5 alpha, same smoothing/SE band, padding 4px.
  - `renderKDE(size)`: height = 0.5·size, Gaussian KDE with bandwidth range/max(8, n^0.2·4), 80 sample points scaled to 92% of plot height; SE band as above, area fill `rgba(142, 68, 173, 0.12)`, line `#8e44ad` width 1.
- **Per-class silhouette color map:** as listed in the Per-Class Accuracy section.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; accents `#2e7d32` (success green), `#e65100` (warn orange), `#c62828` (miss red), `#8e44ad` (KDE purple).
