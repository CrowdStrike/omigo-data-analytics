# 08b. Architecture Examples

**Page type:** other (single-page long doc: six `.example` boxes, each with h2, canvas histogram, `.pipeline-path` callout, 2×2 grid of `.step-box` cards, and a `.result` or `.skip` callout; then a summary h2 + summary canvas)
**HTML title tag:** 08b. Architecture Examples — Real Features Through the Pipeline

**Subtitle:** How real features from Ames Housing and Adult Census flow through the pipeline — each taking a different path based on its shape

## Example 1: SalePrice (Ames Housing) — Right-Skew Path

### Visualization (canvas `ex1-hist`, 960×200)

Histogram (20 bins) with orange SE band and smoothed center line (shared `drawHist` renderer, see Regeneration instructions).

- **Data (bin counts):** `[11, 135, 451, 882, 565, 343, 210, 119, 88, 46, 35, 16, 11, 3, 6, 3, 4, 0, 0, 2]`
- **X labels (left/center/right):** `$13k`, `$384k`, `$755k`
- **Title (bold 12px `#1a5276`, top left):** "SalePrice — right-skew, no gaps, no valleys"

**Pipeline path callout:** `Shape Classification` → right_skew (87%) → `Gap Split?` NO (no empty bins) → `Valley Split?` NO (no bimodal valley) → `Model Fitting` (right_skew, gamma candidates) → `Range Testing`

**Step 1: Shape = right_skew**
- CNN sees: peak early, long right tail. Top-2: right_skew (87%), heavy_tail (8%).
- Routing: bell/skew path — direct to model fitting, no splitting needed.

**Step 2-3: Splitting = SKIPPED**
- No empty-bin gaps in the histogram. No valley between two peaks. The distribution is unimodal.
- Feature stays as one segment.

**Step 4: Model Fitting**
- Candidate A: Right-skew (μ=11.9, σ=0.42) — fit score 0.91
- Candidate B: Gamma (α=5.1, β=35k) — fit score 0.84
- Candidate C: Weibull — fit score 0.78

**Step 5: Range Testing**
- Right-skew model: range [300k+] has purity 0.82 for high-quality homes
- Gamma model: range [50k-100k] has purity 0.91 for lower-income neighborhoods
- Both kept as extended features.

**Result (green callout):** 1 feature → 1 segment → 2-3 candidate models → 3-4 extended binary features

## Example 2: 2nd Flr SF (Ames Housing) — Zero-Inflated / Gap Split Path

### Visualization (canvas `ex2-hist`, 960×200)

Histogram (20 bins) with SE band, plus a gap highlight.

- **Data (bin counts):** `[1678, 8, 19, 50, 99, 172, 210, 199, 210, 73, 70, 52, 46, 22, 6, 7, 1, 5, 2, 1]`
- **X labels:** `0`, `1000`, `2065`
- **Title:** "2nd Flr SF — spike at 0, gap, then continuous"
- **Gap highlight:** bin slot 1 (one bin width starting at x = 10 + 1·(940/20), from y=15 to y=180) filled `rgba(231,76,60,0.15)` and outlined `#e74c3c` width 1.5 dashed [4,2]; label "gap" in 10px `#e74c3c` centered above at slot midpoint.

**Pipeline path callout:** `Shape Classification` → spike (72%) → `Gap Split?` YES — empty bins between 0 and ~300 → Segment 1: spike at 0 (n=1678) | Segment 2: right-skew (n=1252) → `Model Fitting` on Segment 2 only

**Step 1: Shape = spike**
- CNN sees: massive spike at 0 (57% of data), then a separate continuous distribution. Top-2: spike (72%), bimodal (15%).
- Routing: spike path — separate zero-mass, fit remainder.

**Step 2: Gap Split = YES**
- Empty bins between bin 0 (value=0) and bin ~3 (value≈300). Run length = 1-2 bins depending on resolution.
- But more importantly: structural zero. Values ARE zero or ARE a continuous floor area. Binary split at 0.

**Step 3: Valley Split = N/A**
- Segment 1 is a point mass (all zeros). No valley to detect.
- Segment 2 (non-zero values) is checked: unimodal right-skew. No valley.

**Step 4-5: Model Fitting & Ranges**
- Segment 1 → binary feature: "has_2nd_floor" (yes/no)
- Segment 2 → Right-skew fit on non-zero values. Ranges: [300-600] typical, [1200+] large homes.
- The binary feature alone may be highly predictive.

**Result (green callout):** 1 feature → 2 segments → binary indicator + continuous model → 3 extended features (has_2nd_floor, 2ndflr_typical, 2ndflr_large)

## Example 3: Capital_Gain (Adult Census) — Extreme Zero-Inflation + Outlier Spike

### Visualization (canvas `ex3-hist`, 960×200)

Histogram (20 bins) with SE band, plus a wide gap highlight.

- **Data (bin counts):** `[30913, 878, 157, 360, 38, 49, 5, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 159]`
- **X labels:** `0`, `50k`, `99999`
- **Title:** "Capital_Gain — 95% zero, scattered mid, spike at cap"
- **Gap highlight:** bins 7–18 (12 bin widths starting at slot 7, y=15 to y=180) filled `rgba(231,76,60,0.1)`; label "empty gap (12 bins)" in 10px `#e74c3c` centered at slot 13.

**Pipeline path callout:** `Shape Classification` → spike (91%) → `Gap Split?` YES — multiple gaps → Segment 1: zero (n=30913) | Segment 2: mid-range (n=1489) | Segment 3: cap at 99999 (n=159) → `Model Fitting` on Segment 2 | Segment 3 is a point mass

**Step 1: Shape = spike (extreme)**
- 95% of data at zero. Remaining 5% scattered with an additional spike at max value. CNN: spike (91%).
- Routing: spike path with recursive gap detection.

**Step 2: Gap Split = YES (two gaps)**
- Gap 1: between 0 and first non-zero values (~100). Clear structural zero.
- Gap 2: between mid-range cluster (tops at ~40k) and outlier spike at 99999. Many empty bins in between.
- Result: 3 segments.

**Segment Analysis**
- Seg 1 (n=30913): All zeros — binary feature "has_capital_gain"
- Seg 2 (n=1489): Right-skewed continuous. Genuine capital gains ($100-$40k)
- Seg 3 (n=159): Point mass at 99999 — likely a coding cap/censored value

**Model Fitting**
- Seg 1 → binary indicator
- Seg 2 → Right-skew fit (most gains are small, few are large)
- Seg 3 → binary indicator "capital_gain_capped" — very strong signal (likely all >50K income)
- Extended: has_gain, gain_small, gain_large, gain_capped

**Result (green callout):** 1 feature → 3 segments → 4 extended binary features. The "capped at 99999" group is likely 100% >50K income — extremely high purity.

## Example 4: Year Built (Ames Housing) — Left-Skew / Possible Bimodal

### Visualization (canvas `ex4-hist`, 960×200)

Histogram (20 bins) with SE band, plus a valley highlight.

- **Data (bin counts):** `[2, 9, 9, 6, 37, 53, 117, 122, 60, 102, 70, 186, 270, 258, 218, 218, 76, 200, 352, 565]`
- **X labels:** `1872`, `1941`, `2010`
- **Title:** "Year Built — ascending / possible bimodal"
- **Valley highlight:** bins 8–10 (3 bin widths starting at slot 8, y=15 to y=180) filled `rgba(142,68,173,0.12)`; label "valley?" in 10px `#8e44ad` centered at slot 9.5.

**Pipeline path callout:** `Shape Classification` → left_skew (45%) / bimodal (30%) → `Gap Split?` NO (no empty bins) → `Valley Split?` MAYBE — check valley between 1970s dip and 2000s peak → If valley ≥ 25% from both peaks → split into "older stock" vs "newer construction"

**Step 1: Shape = ambiguous**
- CNN top-2: left_skew (45%), bimodal (30%). Ascending overall trend with a possible dip in 1980s-1990s.
- When top-1 confidence is low, try BOTH paths and compare.

**Step 3: Valley Split — boundary case**
- Peak 1: ~1960 (older stock), Peak 2: ~2005 (newer build)
- Valley: ~1980-1990 region. Drop from Peak 1: moderate. Drop from Peak 2: moderate.
- This is a judgment call — depends on exact bin counts whether 25% threshold is met.

**If NO split (left-skew path)**
- Treat as single continuous feature. Model: no standard parametric fit (it's a historical accumulation).
- Non-parametric ranges: [<1960] old, [1960-2000] mid, [2000+] new

**If YES split (bimodal path)**
- Segment 1: houses built before ~1985 (older stock, n≈1500)
- Segment 2: houses built after ~1985 (newer construction, n≈1430)
- Each segment tested for significance against price target independently.

**Multi-candidate approach (yellow callout):** When CNN confidence is split between two shape classes, run BOTH pipelines and keep whichever produces more significant ranges. This is the "multiple interpretations" philosophy in action.

## Example 5: Hours_per_week (Adult Census) — Spike-Dominated

### Visualization (canvas `ex5-hist`, 960×200)

Histogram (20 bins) with SE band, plus a spike marker.

- **Data (bin counts):** `[205, 531, 645, 1547, 1015, 1302, 1635, 16100, 2442, 677, 3036, 841, 1519, 277, 365, 83, 182, 20, 34, 105]`
- **X labels:** `1`, `50`, `99`
- **Title:** "Hours/week — massive spike at 40, continuous tails"
- **Spike marker:** vertical dashed line (`#e67e22`, width 2, dash [4,2]) at slot 7.5 from y=15 to y=180; label "40hr spike" in bold 10px `#e67e22` centered above the line.

**Pipeline path callout:** `Shape Classification` → spike (78%) → `Gap Split?` NO (no empty bins — values exist everywhere from 1 to 99) → `Valley Split?` MAYBE — valleys on either side of the 40hr spike → `Model Fitting`: non-standard (point-mass + continuous mixture)

**Step 1: Shape = spike**
- ~50% of values at exactly 40 (full-time standard). Continuous tails on both sides. CNN: spike (78%).
- This is NOT zero-inflated — the spike is at the MODE, not at a boundary.

**Step 2-3: Splitting = COMPLEX**
- No empty gaps (all hours 1-99 have some data). Valley exists on both sides of 40 but it's the spike creating artificial "valleys."
- Valley criterion: the spike IS the peak. Drop from spike to neighbors is large, but this is structural, not bimodal.

**Pipeline Challenge**
- This feature doesn't fit the standard split → fit → range pipeline cleanly.
- Options: (a) Treat 40 as a point mass + fit remainder, (b) discretize into bins (part-time, full-time, overtime), (c) non-parametric model only.

**Practical Resolution**
- Create structural bins: <35 (part-time), 35-45 (full-time), >45 (overtime)
- Each bin tested for target separation independently.
- Extended features: is_parttime, is_fulltime, is_overtime

**Lesson (yellow callout):** Not every feature fits neatly into "detect shape → split → parametric model." Spike-at-mode features need special handling — the pipeline must recognize when structural binning is more appropriate than distributional splitting.

## Example 6: Lot Area (Ames Housing) — Extreme Right-Skew with Outliers

### Visualization (canvas `ex6-hist`, 960×200)

Histogram (20 bins) with SE band, plus a wide outlier-territory highlight.

- **Data (bin counts):** `[2301, 570, 30, 11, 10, 3, 1, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 1]`
- **X labels:** `1.3k`, `108k`, `215k`
- **Title:** "Lot Area — extreme right-skew, outliers beyond 50k"
- **Highlight:** bins 7–19 (13 bin widths starting at slot 7, y=15 to y=180) filled `rgba(231,76,60,0.1)`; label "mostly empty — outlier territory" in 10px `#e74c3c` centered at slot 13.

**Pipeline path callout:** `Shape Classification` → exponential (55%) / heavy_tail (35%) → `Gap Split?` YES — empty bins above ~50k (long tail) → Segment 1: main mass 1300-25000 (n≈2870) | Segment 2: outliers 50k-215k (n≈60) → `Model Fitting` on Segment 1 | Segment 2 too small for reliable modeling

**Step 1: Shape = exponential/heavy_tail**
- 98% of data below 25k, but max is 215k. The histogram is dominated by 2 bins. CNN unsure between exponential decay and heavy-tail.

**Step 2: Gap Split = YES**
- Multiple empty bins between ~50k and the scattered extreme outliers. Gap is genuine — these are estate-sized parcels vs normal residential lots.
- Split point: ~50k (well beyond 3σ of main distribution).

**Segment 1: Main distribution**
- n≈2870, range [1300-50000]. Still right-skewed but manageable.
- After log transform: approximately normal! → right_skew model fits well.
- Ranges: [1300-5000] small lots, [15000-50000] large lots.

**Segment 2: Outlier parcels**
- n≈60, range [50000-215000]. Too few for reliable parametric model.
- Treatment: binary indicator "is_large_parcel" — useful if these correlate with specific neighborhoods or prices.

**Result (green callout):** Gap split isolates outliers → main mass gets right_skew fit → transformation detection (Phase 2) would find log(Lot_Area) is bell-shaped → parametric tests apply on the transform.

## Pipeline Path Summary

### Visualization (canvas `summary-canvas`, 960×350)

Canvas-drawn table on `#f8f9fa` background.

- **Title (bold 14px `#1a5276`, centered at y=25):** "Pipeline Paths by Feature Shape"
- **Column headers (bold 11px `#1a5276`, at y=55, x positions 50/170/300/400/480/550/750):** Feature | Shape Class | Gap Split | Valley | Segs | Model Candidates | Ext Feats. Header underline: `#2980b9` line from x=40 to x=850 at y=62.
- **Rows (11px, starting y=82, 42px row pitch, zebra stripes `#f0f4f8` / `#fff` on 810×38 row rectangles):**

| Feature | Shape Class | Gap Split | Valley | Segs | Model Candidates | Ext Feats |
|---------|-------------|-----------|--------|------|------------------|-----------|
| SalePrice | right_skew | — | — | 1 | right_skew, gamma | 3-4 |
| 2nd Flr SF | spike | YES (at 0) | — | 2 | binary + right_skew | 3 |
| Capital_Gain | spike (extreme) | YES (×2) | — | 3 | binary + log-norm + binary | 4 |
| Year Built | left_skew/bimodal | — | MAYBE | 1-2 | non-parametric / per-segment | 2-4 |
| Hours/week | spike (at mode) | — | — | 3 (structural) | structural bins | 3 |
| Lot Area | exponential | YES | — | 2 | right_skew + binary | 3 |

- **Cell colors:** Feature bold `#333`; Gap Split `#aaa` when "—" else `#e74c3c`; Valley `#aaa` when "—", `#e67e22` when "MAYBE", else `#8e44ad`; Ext Feats bold `#1a5276`; others `#333`.
- **Bottom note (italic 11px `#666`, centered, y=330 and y=346):** "Each raw feature takes a different path through the pipeline based on its detected shape." / "The pipeline is NOT one-size-fits-all — it adapts per feature."

## Regeneration instructions

- **Layout:** single-column page: `h1`, `.subtitle`, then six `.example` boxes (background `#fafafa`, border `1px solid #e8e8e8`, radius 8px, padding 20px, margin 30px 0) each containing: h2, full-width canvas, `.pipeline-path` callout, `.grid` (2 columns `1fr 1fr`, gap 20px, 1 column below 700px) of four `.step-box` cards, and a `.result` or `.skip` callout. After the examples: h2 "Pipeline Path Summary" + summary canvas.
- **Callout styles:** `.pipeline-path` — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, radius 0 6px 6px 0; inline `code` chips background `#e8f0f8`, padding 2px 6px, radius 3px, 0.9em, `#1a5276`; `.arrow` spans `#2980b9` bold. `.step-box` — white, border `1px solid #ddd`, radius 6px, padding 12px; h4 0.9em `#2980b9`; p 0.85em. `.result` — background `#d4efdf`, border `1px solid #27ae60`, radius 6px, padding 10px 14px, 0.88em. `.skip` — background `#fef9e7`, border `1px solid #f4d03f`, same box metrics.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with `border-bottom: 2px solid #2980b9`, padding-bottom 8px, margin 50px 0 15px; h3 1.1em `#6c3483`; p 0.92em `#333`; subtitle `#666` 1.05em. Canvas CSS: `width: 100%; height: auto`, background `#f8f9fa`, border `1px solid #e0e0e0`, radius 8px. No nav bar, no back/home links.
- **Canvas setup:** the six example canvases declare HTML attributes `width="960" height="300"` (summary `height="350"`) but the JS `setup(id, w, h)` helper overrides backing-store size to 960×200 for examples and 960×350 for the summary, multiplied by `window.devicePixelRatio`, with `ctx.scale` back to logical coordinates and CSS width 100%.
- **Shared histogram renderer (`drawHist`):** margins left/right 10, top 15, bottom 20; bars normalized to max bin, fill `rgba(26, 82, 118, 0.35)`, 1px inset per side; thin `#ccc` baseline; three x labels in 11px `#666` (left at margin+30, center, right at width−margin−30); bold 12px `#1a5276` title at top left. Overlaid SE band: Gaussian-weighted smoothing of the bins (σ=1.2, radius 3σ); smoothed values winsorized at 2× the original bar height (for zero bins, capped at 2× the nearest nonzero neighbor); effective N = sum of bins clamped to [30, 200] for visual clarity; band = smoothed ± 1.96·smoothed/√effN drawn as a closed polygon filled `rgba(230, 126, 34, 0.25)`; smoothed center line stroked `#1a5276` width 2. Each canvas is first flood-filled `#f8f9fa`.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, bar fill `rgba(26,82,118,0.35)`.
