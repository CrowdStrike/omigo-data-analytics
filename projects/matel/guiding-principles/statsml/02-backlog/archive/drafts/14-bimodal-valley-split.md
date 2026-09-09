# Bimodal Valley Split — Real Twin Peaks vs Noise

**Page type:** detail page (single-column doc with h2 sections, algo-step box, decision tables, and 3-column chart grids of generated histogram cells with verdict labels)
**HTML title tag:** Bimodal Valley Split — Real Twin Peaks vs Noise

**Intro paragraph:** When a histogram shows two peaks with data between them (a valley, not a gap), is it genuinely bimodal or just random variation in a single distribution? Splitting incorrectly creates two fake half-distributions. Not splitting means you can't apply t-test to what might be two real normals.

## The Problem: Valley ≠ Bimodal

Many things create two peaks that aren't real bimodal: sampling noise at low n, heavy tails, skewed distributions with a bump, plateaus with slight dips. Only split when confident.

### Visualization (chart grid `problemGrid`, 3 cells, each canvas 280×112)

Each cell: 25-bin histogram (bars = cell color at 0.5 alpha), Gaussian-smoothed (σ=1.2) density line `#1a5276` 1.5px, 95% SE band `rgba(230,126,34,0.18)` with effN clamped 30-200, thin gray `#999` baseline; optional dashed red `#c62828` split line (2.5px, dash 6/4). Below each canvas: title, sub, and a bold verdict line (green `#2e7d32` if it contains "SPLIT" without "NO", red `#c62828` for "NO SPLIT"). Data via mulberry32 PRNG + Box-Muller normals.

1. **"Real bimodal: two populations"** — sub: "Heights (children + adults)"; verdict: "SPLIT — two real normals". Data (seed 100): 400 Normal(30,5) + 400 Normal(60,6); split line at 45; color `#16a085`.
2. **"Noise: bell at n=80"** — sub: "Random dip looks like two peaks"; verdict: "NO SPLIT — sampling noise". Data (seed 201): 80 Normal(50,12); color `#e74c3c`.
3. **"Heavy-tail: NOT bimodal"** — sub: "Fat tails create bumps that look like peaks"; verdict: "NO SPLIT — single distribution". Data (seed 302): 600 Student-t-like `50 + z/sqrt(chi2(3)/3)*10`; color `#8e44ad`.

## Real Bimodal: Two Populations Sharing a Feature

These have a genuine physical reason for two peaks — different subgroups generating data from different processes.

### Visualization (chart grid `realGrid`, 6 cells, each canvas 280×112)

1. **"Height: women + men"** — sub: "Two normals, μ=165cm vs 178cm"; verdict: "Valley drop: 45% from both peaks". Data (seed 110): 300 Normal(165,6) + 350 Normal(178,5); split at 172; color `#2980b9`.
2. **"Commute: bike vs car"** — sub: "Bike ~20min, car ~38min"; verdict: "Valley drop: 40% / 35%". Data (seed 220): 300 Normal(20,3) + 350 Normal(38,4); split at 29; color `#e67e22`.
3. **"GPA: arts vs engineering"** — sub: "Different grading cultures"; verdict: "Valley drop: 55% from both peaks". Data (seed 330): 350 Normal(3.2,0.4) + 300 Normal(4.6,0.3); split at 3.9; color `#27ae60`.
4. **"Sleep onset: early birds + night owls"** — sub: "Two chronotype groups"; verdict: "Valley drop: 42% / 35%". Data (seed 440): 300 Normal(6,1.5) + 350 Normal(14,2); split at 10; color `#8e44ad`.
5. **"Systolic BP: normal + hypertensive"** — sub: "Two clinical groups, ~30 units apart"; verdict: "Valley drop: 42% / 35%". Data (seed 550): 400 Normal(118,6) + 250 Normal(148,7); split at 133; color `#e74c3c`.
6. **"Birth weight: preterm + term"** — sub: "Different gestational ages"; verdict: "Valley drop: 48% / 40%". Data (seed 660): 300 Normal(2500,300) + 350 Normal(3800,400); split at 3100; color `#16a085`.

## Fake Bimodal: Noise That Looks Like Two Peaks

These look bimodal but are actually one distribution with artifacts. Splitting them would create meaningless segments.

### Visualization (chart grid `fakeGrid`, 6 cells, each canvas 280×112)

1. **"Normal, n=60: random two-peak illusion"** — sub: "Same seed, same distribution — just noisy"; verdict: "NO SPLIT — n too low, histogram unstable". Data (seed 111): 60 Normal(50,10); color `#3498db`.
2. **"Gamma(4): skewed with a shoulder"** — sub: "Peak + flat region looks bimodal"; verdict: "NO SPLIT — valley only 12% drop from shoulder". Data (seed 222): 500 gamma-like `(sum of 4 exponentials)*3`; color `#e67e22`.
3. **"Uniform + edge accumulation"** — sub: "Slight pile-up at left boundary"; verdict: "NO SPLIT — right \"peak\" is just flat uniform". Data (seed 333): 500 Uniform(20,80) + 50 Normal(22,2); color `#9b59b6`.
4. **"Student-t (df=2): tail bumps"** — sub: "Heavy tails create secondary humps"; verdict: "NO SPLIT — bumps at <20% of main peak". Data (seed 444): 800 `50 + z/sqrt(chi2(2)/2)*8`; color `#e74c3c`.
5. **"Main peak + tiny group (n=30)"** — sub: "Second peak exists but too small"; verdict: "NO SPLIT — right side n=30, marginal". Data (seed 555): 500 Normal(50,8) + 30 Normal(75,3); color `#16a085`.
6. **"Rounding to nearest 5: fake peaks"** — sub: "Regular spikes are rounding, not bimodal"; verdict: "NO SPLIT — peaks are equispaced (rounding flag)". Data (seed 666): 600 `round(Normal(50,8)/5)*5`; color `#7f8c8d`.

## The Valley Split Algorithm

**Algo-steps box (ordered list):**

1. **Find the two highest peaks** in the histogram (local maxima)
2. **Find the valley** — lowest bin between the two peaks
3. **Valley depth test:** valley must drop ≥ 25% from left peak AND ≥ 25% from right peak
4. **Sample size test:** n on each side of valley ≥ 30 (enough for downstream t-test)
5. **If both pass → SPLIT** at the valley minimum
6. **Classify each side** independently (expect bell-shaped if truly two normals)

**Note callout (green):** **Why 25% from BOTH peaks?** Requiring the drop from both sides prevents splitting when you have one real peak + a minor shoulder. A shoulder barely rises above the valley, so it fails the depth test on that side.

### Valley depth criterion visualized

Same scenario, different valley depths. Watch the verdict change.

### Visualization (chart grid `depthGrid`, 3 cells, each canvas 280×112)

1. **"Deep valley: 60% drop from both"** — sub: "Well-separated peaks, valley near zero"; verdict: "SPLIT". Data (seed 1100): 400 Normal(30,4) + 400 Normal(55,4); split at 42; color `#27ae60`.
2. **"Moderate valley: 30% drop from both"** — sub: "Overlapping but distinct — borderline"; verdict: "SPLIT (just passes 25% threshold)". Data (seed 1200): 400 Normal(35,7) + 400 Normal(55,7); split at 45; color `#e67e22`.
3. **"Shallow valley: 15% drop"** — sub: "Peaks too close relative to spread"; verdict: "NO SPLIT — below 25% threshold". Data (seed 1300): 400 Normal(40,9) + 400 Normal(55,9); no split line; color `#e74c3c`.

## Sample Size Effect

The same true bimodal distribution looks different at different n. At low n, the histogram is noisy and peaks may not be stable. The minimum-n-per-side guard prevents splitting on noise.

### Visualization (chart grid `sampleGrid`, 3 cells, each canvas 280×112)

Same bimodal mixture (half Normal(35,5), half Normal(55,5)) at three sizes (seeds 2000 + i*77):

1. **"n=40: peaks barely visible"** — sub: "Too noisy — min_n guard prevents split"; verdict: "NO SPLIT — n per side < 30 threshold". No split line; color `#e74c3c`.
2. **"n=150: peaks emerging"** — sub: "~75 per side, histogram stabilizing"; verdict: "SPLIT — 25% drop met, n=75 per side". Split at 45; color `#e67e22`.
3. **"n=600: clear bimodal"** — sub: "~300 per side, unambiguous"; verdict: "SPLIT — 40% drop, n=300 per side". Split at 45; color `#27ae60`.

## Adaptive Threshold

Optionally: tighten the threshold at low n where histograms are noisier.

| n per side | Valley drop threshold | Reason |
|---|---|---|
| 30-100 | 30% | Noisy histogram, need strong signal |
| 100-200 | 25% | Moderate stability |
| 200+ | 20% | Stable histogram, subtle dips are meaningful |

## Relationship to Gap Splitting (Doc 16)

Two splitting mechanisms run in sequence:

|  | Gap Split (Doc 16) | Valley Split (Doc 17) |
|---|---|---|
| **Signal** | Empty bins (zero density) | Low-but-nonzero valley |
| **Criterion** | Consecutive empty ≥ x% | Valley drop ≥ 25% from both peaks |
| **Order** | Check first | Check only if no gap found |
| **Split point** | Proportional to σ_L / (σ_L + σ_R) | Valley minimum (lowest bin) |
| **Confidence** | High — physical separation | Moderate — statistical judgment |

### Visualization (chart grid `comparisonGrid`, 3 cells, each canvas 280×112)

1. **"Gap split (Doc 16): empty bins between"** — sub: "No data in 28-60 range → gap criterion triggers first"; verdict: "GAP SPLIT at σ-proportional point". Data (seed 3100): 350 Normal(20,3) + 350 Normal(70,5); split at 38; color `#2980b9`.
2. **"Valley split (Doc 17): data in between"** — sub: "Valley has data but drops 35% from both peaks"; verdict: "VALLEY SPLIT at minimum bin". Data (seed 3200): 350 Normal(35,6) + 350 Normal(58,6); split at 46; color `#16a085`.
3. **"Neither triggers: too much overlap"** — sub: "No empty bins, valley <20% drop"; verdict: "NO SPLIT — classify as-is (broad bell or bimodal)". Data (seed 3300): 350 Normal(40,10) + 350 Normal(55,10); no split line; color `#7f8c8d`.

## After Split: What Each Side Should Look Like

If the split is correct (real bimodal from two normals), each side should classify as bell-shaped. If a side comes out looking skewed or uniform, the split may have been wrong — or the components aren't normal.

### Visualization (chart grid `afterGrid`, 3 cells, each canvas 280×112)

One dataset (seed 4000): 400 Normal(32,5) + 350 Normal(58,6). No verdict lines on these cells.

1. **"Before split: bimodal"** — sub: "Two peaks, valley at ~44". Full data, split line at 44, color `#8e44ad`.
2. **"Left segment: bell (μ=32, σ=5)"** — sub: "Passes normality → ready for t-test". Data v < 44, color `#2e7d32`.
3. **"Right segment: bell (μ=58, σ=6)"** — sub: "Passes normality → ready for t-test". Data v ≥ 44, color `#2980b9`.

## Edge Cases

### Visualization (chart grid `edgeGrid`, 3 cells, each canvas 280×112)

1. **"Asymmetric: big peak + small peak"** — sub: "Left peak 4x taller. Valley drops 25% from small peak?"; verdict: "CHECK — small peak must still show 25% drop". Data (seed 5100): 600 Normal(40,6) + 150 Normal(68,4); no split line; color `#e67e22`.
2. **"Three peaks: split first valley only"** — sub: "Recursive split (doc 16 style) handles the rest"; verdict: "SPLIT at first valley, then recurse". Data (seed 5200): 250 Normal(20,3) + 250 Normal(45,4) + 250 Normal(70,3); split at 33; color `#8e44ad`.
3. **"Exponential + normal: not two normals"** — sub: "Valley exists but left side won't be bell after split"; verdict: "SPLIT valid — but left classifies as descending, not bell". Data (seed 5300): 350 exponential `-log(u)*4` + 300 Normal(35,5); split at 18; color `#16a085`.

## Regeneration instructions

- **Layout:** single-column long doc. h1 + intro paragraph, then h2 sections in document order containing paragraphs, one `.algo-steps` white rounded box (ordered list), one `.note` green callout, two `.decision-table` HTML tables, and `.chart-grid` divs (cells appended by JS via a `makeCell(grid, canvas, title, sub, verdict, cls)` helper). `.chart-grid` is CSS grid `repeat(3, 1fr)`, 20px gap (2 columns below 900px, 1 below 600px). `.chart-cell`: background `#fafcfe`, radius 10px, border `1px solid #e0e0e0`, centered; canvas `width:100%`; `.cell-title` 0.85em weight 600 `#1a5276`; `.cell-sub` 0.75em `#888`; `.cell-verdict` 0.8em weight 700 — `.split` `#2e7d32`, `.nosplit` `#c62828` (class chosen by whether verdict text contains "SPLIT" not preceded by "NO").
- **Chart renderer:** shared `renderChart(canvas, data, opts)` — 25-bin histogram; bars = cell color at 0.5 alpha; Gaussian-smoothed (σ=1.2, kernel radius 3σ) density line `#1a5276` 1.5px; 95% SE band `rgba(230,126,34,0.18)` with effN clamped to [30, 200]; optional orange valley highlight `rgba(255,152,0,0.12)`; optional dashed red `#c62828` split line (2.5px, dash 6/4); gray `#999` baseline. Canvas intrinsic 280×112 (h = 0.4×w), scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates), CSS width 100%. Deterministic mulberry32 PRNG with the seeds listed per cell, Box-Muller `genNormal`.
- **Page CSS:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a3a4a` with 2px bottom border `#1a5276`; h3 1.1em `#1a5276`; p `#3a3a3a` max-width 900px; `.algo-steps` white, border `#e0e0e0`, radius 10px, max-width 800px, li strong `#1a5276`; `.note` background `#e8f5e9` left border `4px solid #2e7d32`; `.decision-table` 0.85em, th background `#1a5276` white text, borders `#ddd`, even rows `#f8f9fa`. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, plus `#2980b9`, `#16a085`, `#8e44ad`, `#2e7d32`, `#c62828`, `#7f8c8d`, `#3498db`, `#9b59b6`.
- In regenerated HTML, any card/anchor links use `.html` extensions.
