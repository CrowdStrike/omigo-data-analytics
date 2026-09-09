# Range Splitting — Mixture of Arbitrary Distributions

**Page type:** detail page (single-column doc with h2 sections, prose paragraphs, decision tables, algo-step boxes, and 3-column chart grids of generated histogram cells)
**HTML title tag:** Range Splitting — Mixture of Arbitrary Distributions

**Intro paragraph:** Real-world features are often mixtures of different populations, each with its own distribution shape. Standard GMM assumes all components are Gaussian. We don't. Split the range at significant gaps, classify each segment independently.

## The Problem: One Feature, Multiple Populations

A single feature column often contains data from fundamentally different processes. Treating it as one distribution loses information.

### Visualization (chart grid `problemGrid`, 3 cells, each canvas 400×160)

Each cell: smoothed histogram (18 bins) of synthetic mixture data with orange SE band and colored density line over `rgba(color,0.35)` bars. Data generated with seeded PRNG (mulberry32, seeds 100 + i*37) and Box-Muller normals.

1. **"Income: exponential tips + normal salary"** — sub: "Descending (tips $1-30) + Bell (salary $50-90K)". Data: 400 points exponential `-log(u)*5 + 1` plus 500 points Normal(70, 12). Line color `#2980b9`.
2. **"Response time: spike + right-skew"** — sub: "Cache hits spike at 3ms, DB queries skewed 80-500ms". Data: 500 points at `3 + Normal(0, 0.3)` plus 200 points gamma-like `80 + (sum of 2 exponentials)*50`. Line color `#e67e22`.
3. **"Delivery days: uniform + descending"** — sub: "Standard shipping (uniform 5-12 days) + Express (descending 1-3 days)". Data: 300 points exponential `-log(u)*0.8 + 0.5` plus 400 points Uniform(5, 12). Line color `#8e44ad`.

## Step 1: Detect Candidate Gaps

Bin the data and find consecutive runs of empty bins. A gap is a candidate if the run length meets: **max(min_buckets, x% of total_buckets)**. This prevents splitting on single empty bins from small samples while catching genuine separations.

### Real gaps vs sampling noise

These examples show the same underlying distribution at different sample sizes. Small n creates false gaps; large n fills them in.

### Visualization (chart grid `gapNoiseGrid`, 3 cells, each canvas 400×160)

Same right-skew exponential `-log(u)*8 + 5` at three sample sizes (seeds 300 + i*71):

1. **"Right-skew, n=60"** — sub: "Gaps in tail are natural at low n". n=60, line color `#e74c3c`.
2. **"Right-skew, n=300"** — sub: "Tail fills in, gaps shrink". n=300, line color `#e67e22`.
3. **"Right-skew, n=2000"** — sub: "Smooth — no spurious gaps". n=2000, line color `#27ae60`.

### Genuine gaps in real data

These have real physical reasons for the gap — different populations with no overlap.

### Visualization (chart grid `realGapGrid`, 3 cells, each canvas 400×160)

Each cell has an orange gap-highlight band `rgba(255,152,0,0.12)` between gapStart and gapEnd (seeds 400 + i*53):

1. **"API latency: spike + heavy-tail"** — sub: "Fast path spike at 2ms, slow path heavy-tail 50-500ms". Data: 600 points `2 + |Normal(0,0.4)|` plus 200 points Student-t-like `150 + z/sqrt(chi2(2)/2)*60`. Gap 5→50. Line color `#16a085`.
2. **"Purchases: descending + uniform"** — sub: "Small purchases (exponential $1-15) + subscription tiers ($40-$80)". Data: 500 points `-log(u)*4 + 1` plus 200 points Uniform(40, 80). Gap 18→38. Line color `#e67e22`.
3. **"Sensor: uniform + ascending (accumulator)"** — sub: "Normal operation (uniform 20-60°C) + thermal runaway (ascending 80-120°C)". Data: 400 points Uniform(20, 60) plus 200 points `80 + u^0.5 * 40`. Gap 62→78. Line color `#8e44ad`.

## Step 2: Validate with Variance

For each candidate gap, check both sides: given the fitted distribution's variance, is there a reasonable probability of seeing data in the gap region? If the distribution's density at the gap is negligible, the gap is real.

| Left expects data in gap? | Right expects data in gap? | Action |
|---|---|---|
| No | No | Split — neither side owns the gap |
| No | Yes | Split left off — gap is right's sparse tail |
| Yes | No | Split right off — gap is left's sparse tail |
| Yes | Yes | No split — gap is natural sparse zone of one distribution |

### Variance check in action

### Visualization (chart grid `varianceGrid`, 3 cells, each canvas 400×160)

Gap highlight bands; split cells also get a dashed red `#c62828` vertical split line (2.5px, dash 6/4). Seeds 500 + i*61:

1. **"Spike + uniform → SPLIT"** — sub: "Spike σ=0.5 can't reach the uniform region at all". Data: 500 points `10 + Normal(0,0.5)` plus 300 points Uniform(30, 80). Gap 12→28, split line at 12. Line color `#27ae60`.
2. **"Heavy-tail + bell → NO SPLIT"** — sub: "Heavy-tail's spread easily reaches the gap. Sparse zone is natural.". Data: 500 points Student-t-like `30 + z/sqrt(chi2(2)/2)*15` plus 300 points Normal(75, 6). Gap 58→62, no split line. Line color `#e74c3c`.
3. **"Descending + ascending → SPLIT"** — sub: "Exponential decay ends, power ramp-up starts. Clear separation.". Data: 400 points `-log(u)*5` plus 350 points `60 + u^0.4 * 40`. Gap 28→58, split line at 38. Line color `#2980b9`.

## Step 3: Place the Split Point

The split point is placed proportional to each side's variance spread within the gap:

**Algo box (code):** `split = gap_left_edge + gap_width × (σ_L / (σ_L + σ_R))`

The side with more spread claims more of the gap — it's more likely to produce a future point in that region. No dead zones: every possible value belongs to one component.

### Split point examples

Same gap, different variance ratios → different split points.

### Visualization (chart grid `splitPointGrid`, 3 cells, each canvas 400×160)

Each with dashed red split line. Seeds 600 + i*43:

1. **"Exponential (σ=8) + spike (σ=1) → split near spike"** — sub: "Exponential claims most of gap — higher spread". Data: 400 points `-log(u)*8` plus 400 points `60 + Normal(0,1)`. Split at 53. Line color `#16a085`.
2. **"Uniform (σ=14) + bell (σ=5) → split favors uniform"** — sub: "Uniform has wider spread, takes more of gap". Data: 400 points Uniform(0, 50) plus 400 points Normal(80, 5). Split at 62. Line color `#e67e22`.
3. **"Bell (σ=6) + right-skew (σ=15) → split favors skew"** — sub: "Skewed side has more reach, claims more territory". Data: 400 points Normal(20, 6) plus 400 points gamma-like `55 + (sum of 2 exponentials)*15`. Split at 43. Line color `#8e44ad`.

## Step 4: Recursive Split (One More Level)

After the initial split, each segment can split once more — max depth of 2. This yields up to 4 components. If a segment still looks multimodal after its one allowed recursion, it's labeled *multimodal* and left as-is.

### Recursive split example

### Visualization (chart grid `recursiveGrid`, 6 cells, each canvas 400×160)

One dataset (seed 700): 300 points spike `5 + Normal(0,0.8)` + 250 points Uniform(25, 55) + 250 points right-skew `70 + (-log(u)*10)`.

1. **"Original: spike + uniform + right_skew"** — sub: "Level 0 — find first gap at ~10, split". Full data, split line at 12, color `#1a5276`.
2. **"Left: spike (σ=0.8)"** — sub: "Level 1 — single peak, done". Data filtered v < 12, color `#27ae60`.
3. **"Right: uniform + right_skew → split again"** — sub: "Level 1 — gap at ~58, recurse". Data v ≥ 12, split line at 60, color `#e67e22`.
4. **"Segment A: spike"** — sub: "Classified: spike". Left data again, color `#27ae60`.
5. **"Segment B: uniform"** — sub: "Classified: uniform". Right data with v < 60, color `#2980b9`.
6. **"Segment C: right_skew"** — sub: "Classified: right_skew". Right data with v ≥ 60, color `#8e44ad`.

## Step 5: Classify Each Segment

Each resulting segment is independently classified into one of the 11 shapes (bell, right_skew, left_skew, uniform, etc.). The output describes the feature as a structured mixture.

### Full pipeline examples

### Visualization (chart grid `pipelineGrid`, 6 cells, each canvas 400×160)

Each with dashed red split line. Seeds 800 + i*47:

1. **"Wait time → [descending, bell]"** — sub: "Quick exits (exp decay 0-8min) + Appointments (normal ~25min)". Data: 400 points `min(8, -log(u)*1.5)` plus 350 points Normal(25, 4). Split at 11. Color `#2980b9`.
2. **"Session length → [spike, uniform]"** — sub: "Bounces (spike at 2s) + Engaged users (uniform 30-180s)". Data: 350 points `2 + |Normal(0,0.8)|` plus 300 points Uniform(30, 180). Split at 15. Color `#16a085`.
3. **"Error count → [spike, right_skew]"** — sub: "Healthy nodes (spike at 0) + Degraded nodes (skewed 8-60)". Data: 500 points `|Normal(0,0.4)|` plus 200 points `8 + (sum of 2 exponentials)*8`. Split at 4. Color `#e74c3c`.
4. **"CPU usage → [uniform, ascending]"** — sub: "Idle variation (uniform 5-40%) + Load saturation (ascending 60-100%)". Data: 350 points Uniform(5, 40) plus 250 points `60 + u^0.5*40`. Split at 48. Color `#8e44ad`.
5. **"Purchase amount → [descending, bell]"** — sub: "Micro-txns (exp $0.50-$5) + Regular orders (normal ~$45)". Data: 400 points `0.5 + (-log(u)*1.5)` plus 300 points Normal(45, 8). Split at 20. Color `#e67e22`.
6. **"Disk IO → [bell, heavy_tail]"** — sub: "Normal ops (bell ~10ms) + Contention spikes (heavy-tail 50-300ms)". Data: 500 points `max(1, Normal(10, 3))` plus 150 points `80 + |z/sqrt(chi2(2)/2)|*40`. Split at 35. Color `#27ae60`.

## Challenges and Edge Cases

### Truncation at split points

Cutting a distribution creates an artificial edge. A bell split at its tail looks like ascending/descending. The classifier must account for hard boundaries at split points — different from natural distribution edges.

### Visualization (chart grid `truncationGrid`, 3 cells, each canvas 400×160)

One exponential dataset (seed 900): 1000 points `-log(u)*15`.

1. **"Original: descending (exponential)"** — sub: "Before any split". Full data, color `#1a5276`.
2. **"Left of cut (0-20): still descending"** — sub: "Correct shape preserved at cut". Data v < 20, color `#27ae60`.
3. **"Right of cut (20+): looks like right_skew?"** — sub: "Truncation artifact — still same exponential tail". Data v ≥ 20, color `#e74c3c`.

### Overlapping tails vs real gaps

Two distributions with overlapping tails create a valley, not a gap. The valley has data in it — sparse but present. The empty-bin criterion prevents splitting here.

### Visualization (chart grid `overlapGrid`, 3 cells, each canvas 400×160)

Seeds 1000 + i*59:

1. **"Exponential tail overlaps bell → NO SPLIT"** — sub: "Right-skew tail extends into bell region. Valley has data.". Data: 400 points gamma-like `(sum of 3 exponentials)*4` plus 300 points Normal(30, 5). No split line. Color `#e67e22`.
2. **"U-shaped + descending overlap → NO SPLIT"** — sub: "U-shape's right edge runs into descending start. Continuous density.". Data: 300 points beta-like U-shape `b1/(b1+b2)*40` with `b=u^0.4` plus 250 points `35 + (-log(u)*10)`. No split line. Color `#8e44ad`.
3. **"Spike + uniform with clear gap → SPLIT"** — sub: "Spike ends at 3, uniform starts at 15. Nothing between.". Data: 400 points `1 + |Normal(0,0.5)|` plus 300 points Uniform(15, 75). Split at 5. Color `#16a085`.

## Algorithm Summary

**Algo-steps box (ordered list):**

1. **Bin** the data (15-20 bins based on range)
2. **Find gaps** — consecutive empty runs ≥ max(min_buckets, x% of total)
3. **Validate** — for each gap, classify both sides, compute expected density in gap from each side's fitted distribution
4. **Split** — place split at expected density threshold point, proportional to σ_L / (σ_L + σ_R)
5. **Recurse** — each segment may split once more (max depth 2, max 4 segments)
6. **Classify** — label each final segment's shape independently

**Note callout (green):** **Output format:** feature "income" → [right_skew (0-45K, σ=12K, n=820), bell (55K-130K, σ=18K, n=340)]
Each segment: shape, range, variance, sample count. No dead zones — every possible value belongs to a component.

## Regeneration instructions

- **Layout:** single-column long doc. h1 + intro paragraph, then h2 sections in document order; sections contain paragraphs, `.decision-table` HTML tables, `.algo-steps` white rounded boxes (code line or ordered list), `.note` green callout, and `.chart-grid` divs (empty in markup; cells appended by JS). `.chart-grid` is CSS grid `repeat(3, 1fr)`, 20px gap (2 columns below 900px, 1 below 600px). Each `.chart-cell`: background `#fafcfe`, radius 10px, border `1px solid #e0e0e0`, centered, canvas at `width:100%`, `.cell-title` 0.85em weight 600 `#1a5276`, `.cell-sub` 0.75em `#888`.
- **Chart renderer:** shared `renderChart(canvas, data, opts)` — 18-bin histogram of the generated data; bars filled with the cell color at 0.35 alpha; Gaussian-smoothed (σ=1.2 bins) density line in the cell color (1.5px), winsorized at 2× bar height; 95% SE band `rgba(230,126,34,0.18)` around the smoothed line using effN = max(30, n); optional gap highlight `rgba(255,152,0,0.12)` and dashed red `#c62828` split line(s) (2.5px, dash 6/4); thin gray `#999` baseline axis. Canvas intrinsic 400×160 scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates), CSS width 100%. Data via deterministic mulberry32 PRNG with the per-grid seeds listed above and Box-Muller `genNormal(rng, mu, sigma)`.
- **Page CSS:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a3a4a` with 2px bottom border `#1a5276`; h3 1.1em `#1a5276`; p `#3a3a3a` max-width 900px; `.algo-steps` white background, border `#e0e0e0`, radius 10px, max-width 800px; `.note` background `#e8f5e9` left border `4px solid #2e7d32`; `.warn` background `#fff3e0` left border `4px solid #e65100`; `.decision-table` 0.85em, th background `#1a5276` white text, borders `#ddd`, even rows `#f8f9fa`. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, plus secondary `#2980b9`, teal `#16a085`, purple `#8e44ad`, split-line red `#c62828`.
- In regenerated HTML, any card/anchor links use `.html` extensions.
