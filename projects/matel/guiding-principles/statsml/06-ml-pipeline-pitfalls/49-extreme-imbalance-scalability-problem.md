# Pitfall: Extreme Imbalance Scalability Problem

**Page type:** detail page (three card-sections, each a two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** Extreme Imbalance Scalability Problem

**Subtitle:** Rare positives + statistical requirements = need massive negative data, creating scalability crisis.

## The Problem

**Tags:** `the trap` (red), `rare positives` (blue)

- **Rare positives** — at 0.01% prevalence, enough positives implies billions of negatives
- **Sampling trade-off** — downsampling to 1:10 or 1:100 discards negative-class diversity
- **Distribution shift** — the model overfits sampled negatives, then serving looks different
- **Statistical power** — ~30 positives per cell across 10,000 cells means ~3B total examples
- **Scalability limits** — billions of rows exceed memory, and cross-validation takes weeks
- **Class weights** — the optimal minority weight is data-dependent and shifts with the ratio

*Example:* A fraud model at a 0.02% rate needs 50K fraud examples, so 250M total records of which 249.95M are negatives; after 1:50 downsampling, the production false-positive rate runs 3x the test rate.

**Impact:** The model either starves for positives or drowns in negatives — naive downsampling swaps the scalability problem for a distribution-shift problem.

### Visualization (canvas `c1`, 720×300)

Diagram: a large data rectangle with a tiny positive sliver, plus two requirement/overhead boxes.

All figures in this chart derive at render time from three inputs — `nBuckets = 20`, `posPerBucket = 30`, `prevalence = 0.0001` — so the arithmetic closes: `posNeeded = 20 × 30 = 600`, `totalNeeded = 600 / 0.0001 = 6,000,000`, `negNeeded = 6,000,000 − 600 = 5,999,400`, `negPct = 99.99%`. No figure is hardcoded beside the diagram.

- **Title (bold 14px `#1a5276`, top center):** "Extreme Imbalance: 99.99% Overhead for 0.01% Signal" — both percentages computed.
- **Total-data rectangle:** at (70, 50), 580×100, fill `rgba(26,82,118,0.15)`, stroke `#1a5276` width 2. Centered labels inside: bold 13px "6M Total Records", 11px "(99.99% negatives)".
- **Positive sliver:** solid `#e74c3c` rectangle 6px wide at the right edge of the total rectangle, full 100px height.
- **Arrow:** red (`#e74c3c`, width 1.5) line from below the rectangle up to the sliver; right-aligned labels bold 12px "600 positives" and 11px "(0.01%)".
- **Statistical requirements box:** stroke `#e67e22` width 2 at (70, 180), 280×70. Text in `#e67e22`: bold 12px "Statistical Requirements:", then 11px lines "• 20 buckets for profiling", "• 30 positives per bucket", "= 600 positives minimum".
- **Compute overhead box:** stroke `#e74c3c` width 2 at (370, 180), 280×70. Text in `#e74c3c`: bold 12px "Compute Overhead:", then 11px lines "• 6M records to process", "• 5,999,400 are negatives", "• 99.99% wasted compute".
- **Bottom annotation (bold 12px `#e74c3c`, centered):** "Downsample negatives → lose diversity. Keep all → scalability crisis."

## Why It Happens

**Tags:** `root cause` (orange), `rare events` (blue)

- **Colliding demands** — rare-event math meets methods that assume both classes are common
- **Rare-event math** — each analysis bucket needs enough positives, so total data explodes
- **Method assumptions** — standard profiling tests expect reasonable counts in every class
- **Split fragility** — stratified splits still yield near-empty minority cells in some folds
- **Misleading metrics** — predicting all-negative already scores 99.9% at extreme ratios

*Example:* At a 0.05% fraud rate, a 100K training set holds only 50 fraud cases — just 5 per bucket across 10 profiling buckets.

**Root Cause:** Statistical tests assume sufficient samples in each class, and at 1000:1 or worse ratios per-class statistics have enormous variance.

### Visualization (canvas `c2`, 720×300)

Diagram: class-distribution bars on the left, 10 tiny profiling buckets with honest 95% error bars on the right.

The bucket array is the single source of truth: `[8, 3, 7, 4, 6, 5, 2, 9, 3, 3]` sums to 50 positives, so the minority label (50), the majority label (100,000 − 50 = 99,950), the mean (50 ÷ 10 = 5.0 per bucket), and every error bar are all computed from it at render time.

- **Title (bold 14px `#1a5276`, top center):** "Class Distribution: Minority Vanishes in Buckets".
- **Majority bar:** at x=60, width 80, height 180 above baseline y=240; fill `rgba(26,82,118,0.35)`, stroke `#1a5276` width 2; labels bold 11px `#1a5276`: computed "99,950" above, "Majority" below.
- **Minority bar:** at x=180, width 80, height 4; fill `rgba(231,76,60,0.5)`, stroke `#e74c3c` width 2; labels bold 11px `#e74c3c`: computed "50" above, "Minority" below.
- **Dashed arrow:** orange `#e67e22` width 1.5, dash 4/3, from beside the minority bar rightward to x=340 then up to y=55, connecting to the bucket panel.
- **Bucket panel header (bold 12px `#e67e22`, centered at 530,48):** "Split into 10 Profiling Buckets" — count taken from the array length.
- **10 bucket bars:** starting at x=360, width 32, gap 6, baseline y=220; sample counts `[8, 3, 7, 4, 6, 5, 2, 9, 3, 3]`, bar height = count × 6px; fill `rgba(231,76,60,0.4)`, stroke `#e74c3c` width 1; 10px count label above each bar.
- **Error bars:** orange `#e67e22` width 1.5, vertical whisker with 5px caps centered on each bucket bar. Height is **computed, not generated**: the 95% Poisson interval for a count of n is ±1.96·√n samples, drawn in the same 6px-per-sample units as the bar. This ranges from ±1.96·√2 ≈ 2.8 samples (33px) at n=2 to ±1.96·√9 = 5.9 samples (71px) at n=9 — genuinely enormous relative to the bars, and now for a stated statistical reason rather than by decoration.
- **Error-bar label (bold 11px `#e67e22`, centered at 530,245):** "95% intervals up to ±139% — no statistical power". The 139% is computed as the largest relative half-interval across the plotted buckets: 1.96·√2 / 2 = 1.386.
- **Bottom annotation (bold 12px `#e74c3c`, centered):** "50 positives ÷ 10 buckets = 5 per bucket, meaningless comparisons" — all three numbers computed from the array.

## The Correct Approach

**Tags:** `the fix` (green), `calibration` (blue)

- **Downsample with correction** — sampling stays practical only if its distortions are undone
- **Prior correction** — recalibrate predicted probabilities for the known sampling rate
- **Hard negatives** — keep boundary-adjacent negatives via importance weighting or mining
- **Wider buckets** — merge cells until each meets a minimum minority count, e.g. 20
- **Rank tests, with limits** — Mann-Whitney drops distribution assumptions, not power needs
- **Two-tier profiling** — profile the majority finely, pool the minority into coarser buckets

*Example:* With 50 total positives and a 20-per-bucket minimum, adaptive bucketing yields at most 2 minority buckets instead of 10 near-empty ones.

**Fix:** Downsample only with prior correction and retained hard negatives, and widen buckets until each meets a minimum minority-count threshold.

### Visualization (canvas `c3`, 720×300)

Diagram: downsampled class bars on the left, two wide green buckets with computed error bars on the right, split by a dashed divider.

The bucket array `[26, 24]` is the source of truth and sums to the same 50 positives as chart `c2`. The majority label is `50 × 10 = 500`, matching the stated 10:1 downsampling ratio, and both error bars use the same 95% Poisson rule as `c2`.

- **Title (bold 14px `#1a5276`, top center):** "Correct: Downsample + Calibrate, Wider Buckets for Minority".
- **Left header (bold 12px `#1a5276`, centered at 130,48):** "Downsample to 10:1" — ratio from the `downsampleRatio` input.
- **Downsampled majority bar:** at x=60, width 60, height 100 above baseline y=200; fill `rgba(26,82,118,0.35)`, stroke `#1a5276` width 2; 11px labels computed "500" above, "Majority" and "(downsampled)" below.
- **Minority bar:** at x=150, width 60, height 10; fill `rgba(39,174,96,0.4)`, stroke `#27ae60` width 2; 11px `#27ae60` labels computed "50" above, "Minority" below.
- **Green check:** bold 16px `#27ae60` "✓" at (130, 250), with 11px lines "Manageable ratio" and "+ calibration correction".
- **Divider:** vertical dashed gray `#ccc` line at x=270, dash 4/4.
- **Right header (bold 12px `#1a5276`, centered at 500,48):** "Wider Buckets (2 instead of 10)" — count from the array length.
- **Two wide buckets:** width 100, gap 20, starting x=385, baseline y=190; counts `[26, 24]`, height = count × 4px; fill `rgba(39,174,96,0.3)`, stroke `#27ae60` width 2; bold 12px `#27ae60` labels "n=26", "n=24" above; 11px `#555` labels "Bucket 1", "Bucket 2" below; bold 14px green "✓" below each label.
- **Error bars:** small green whiskers with 6px caps, height **computed** as the 95% Poisson interval ±1.96·√n in the chart's 4px-per-sample units — ±10.0 samples (80px) at n=26 and ±9.6 samples (77px) at n=24. Visibly tighter *relative to the bar* than in `c2`, which is the actual point.
- **Threshold line:** dashed orange `#e67e22` width 1.5, dash 5/3, horizontal at the 20-samples height (y = 190 − 20×4 = 110) across both buckets, labeled 11px "min 20 samples" to the right. Both buckets clear it.
- **Bottom annotation (bold 12px `#27ae60`, centered):** "Pooling 50 positives into 2 buckets tightens intervals to ±40% — usable, still not precise". The 40% is computed as the largest relative half-interval: 1.96·√24 / 24 = 0.400. The earlier wording ("Statistically valid: adequate samples per bucket for reliable comparisons") overclaimed — a ±40% interval is an improvement on ±139%, not precision.

## Regeneration instructions

- **Layout:** three `.card-section` blocks (The Problem / Why It Happens / The Correct Approach), each an h2 with blue bottom border followed by a `table.layout` with one row: left `td.text-col` (45%) holding `.tags` pills, a `ul` of labeled bullets, an italic `.example` paragraph, and a `.key-point` callout; right `td.viz-col` (55%) holding one 720×300 canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border. `ul` 0.92rem; `li b` in `#1a5276`.
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors: blue `rgba(26,82,118,0.12)`/`#1a5276`; green `rgba(39,174,96,0.15)`/`#27ae60`; red `rgba(231,76,60,0.12)`/`#e74c3c`; orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `strong` in `#1a5276`. `.example` italic `#555` 0.9rem.
- **Canvas:** intrinsic 720×300, CSS `width: 100%`, border `1px solid #e0e0e0`, radius 4px; scaled via `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) through a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
