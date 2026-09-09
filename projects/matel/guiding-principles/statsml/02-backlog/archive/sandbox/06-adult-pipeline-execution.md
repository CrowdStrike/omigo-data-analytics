# 19b. Adult Census — Pipeline Execution

**Page type:** detail page (single-column sequence of `.example` boxes, each: h2 title, full-width canvas histogram, meta pill row, summary callout, result/skip callout)
**HTML title tag:** 19b. Adult Census — Pipeline Execution (Numerical Features)

**Subtitle:** 6 numerical features from the Adult Census dataset (n=32,561) flowing through the full shape classification pipeline. v3-full CNN results only.

All six canvases share one `drawHist` renderer: 20-bin histogram (bars `rgba(26,82,118,0.35)`, drawn only for non-zero bins, 2px gutter) with a Gaussian-smoothed density curve (sigma=1.2 over bin indices; bins capped at 4× the median bin count for smoothing, then spike bins above the cap restored), curve stroked `#1a5276` width 2, plus a confidence band filled `rgba(230,126,34,0.25)` (±1.96·smoothed/√effN, floor 3% of max, ceiling 120% of max). Margins: left/right 10, top 15, bottom 20. Thin `#ccc` baseline. Three x-axis labels (`#666` 11px, centered) near left edge, center, right edge. Title top-left in bold 12px `#1a5276`. Canvas background `#f8f9fa`. HTML canvas attributes are `width="960" height="300"`, but the JS `setup()` renders each at 960×200 logical pixels.

Each `.example` has a `.meta` row of monospace pill spans (background `#e8f0f8`; the Shape pill is green: background `#d4efdf`, border `1px solid #27ae60`, bold), then a `.summary-text` callout (background `#f0f4f8`, left border `4px solid #2980b9`), then a green `.result` or yellow `.skip` callout.

## 1. Age

Meta pills: Type: **numerical** | Shape: right_skew (100%) | Gap: NO | Valley: NO

Summary: Right-skewed continuous ages (17–90) with a working-age bulge peaking around 25–40 and monotone decline toward 90. No gaps or valleys detected — single unimodal segment.

**Result:** 1 segment → 4 range-based extended features (young, mid, senior, elderly) (green `.result` callout)

### Visualization (canvas `f1-hist`, 960×200)

- **Bins:** `[2410,3160,2461,3429,3465,2583,3198,2965,1828,2139,1558,1033,996,599,269,227,120,54,20,47]`
- **X labels:** "17", "53", "90"
- **Title:** "Age (n=32,561) — right-skew, working-age bulge"
- No overlay annotation.

## 2. fnlwgt (Census Weight)

Meta pills: Type: **numerical** | Shape: right_skew (99%) | Gap: YES (sparse tail >500k) | Valley: NO

Summary: Strongly right-skewed with most mass in 100k–250k range and a sparse extreme tail beyond 500k. Gap detection splits into main segment (n=32,400) and extreme weights (n=160). Main segment fits right_skew well.

**Domain note:** fnlwgt is a sampling weight, not a personal attribute. Predictive use requires domain justification. (yellow `.skip` callout)

### Visualization (canvas `f2-hist`, 960×200)

- **Bins:** `[4483,8634,10945,4382,2510,988,341,136,64,38,15,5,5,5,2,3,1,1,1,2]`
- **X labels:** "12k", "748k", "1.48M"
- **Title:** "fnlwgt (n=32,561) — right-skew, sparse extreme tail"
- **Annotation:** faint red overlay `rgba(231,76,60,0.08)` covering bins 6-19 (14 bins wide, y 15-180), labeled "sparse tail region (n<200 total)" in red `#e74c3c` 10px centered above at bin 13.

## 3. Education_Num

Meta pills: Type: **ordinal** | Shape: spike (92%) | Gap: discrete artifact | Valley: structural

Summary: 16 discrete ordinal levels with prominent peaks at 9 (HS-grad), 10 (Some-college), and 13 (Bachelors). Zero-count bins are artifacts of histogram binning on integer data, not real gaps. Valleys between peaks are structural properties of the education system, not separate populations. Strong monotone relationship with target validates ordinal treatment.

**Result:** Ordinal dual-path → natural groupings (no_hs, hs_grad, some_college, bachelors, graduate) → 5 extended features (green `.result` callout)

### Visualization (canvas `f3-hist`, 960×200)

- **Bins:** `[51,168,333,0,646,514,933,0,1175,433,10501,0,7291,1382,1067,0,5355,1723,576,413]`
- **X labels:** "1", "8.5", "16"
- **Title:** "Education_Num (n=32,561) — multimodal ordinal, peaks at 9,10,13"
- No overlay annotation.

## 4. Capital_Gain

Meta pills: Type: **numerical** | Shape: zero_inflated (100%) | Gap: YES (multiple) | Valley: N/A

Summary: Extreme zero-inflation (95% zeros) with a second spike at the cap value of 99,999. Gap detection identifies 3 segments: zeros (n=30,913), mid-range gains (n=1,489), and capped values (n=159). The capped group is nearly 100% high-income — an extremely strong signal.

**Result:** 3 segments via gap splits → 4 extended features (has_gain, gain_small, gain_large, gain_capped) (green `.result` callout)

### Visualization (canvas `f4-hist`, 960×200)

- **Bins:** `[30913,878,157,360,38,49,5,0,2,0,0,0,0,0,0,0,0,0,0,159]`
- **X labels:** "0", "50k", "99999"
- **Title:** "Capital_Gain (n=32,561) — 95% zero, cap spike at 99999"
- **Annotation:** translucent red overlay `rgba(231,76,60,0.1)` covering bins 7-18 (12 bins wide, y 15-180), labeled "12 empty bins" in red `#e74c3c` 10px centered above at bin 13.

## 5. Capital_Loss

Meta pills: Type: **numerical** | Shape: zero_inflated (100%) | Gap: YES | Valley: possible in non-zero segment

Summary: Similar zero-inflation pattern (95.4% zeros) but no cap spike. Gap between zero and first non-zero values creates a clean structural split into zeros (n=31,047) and actual losses (n=1,514). The non-zero segment peaks around $1,500–$2,000 with possible bimodality, though sample size is modest.

**Result:** 2 segments via gap split → 4 extended features (has_loss, loss_small, loss_typical, loss_large) (green `.result` callout)

### Visualization (canvas `f5-hist`, 960×200)

- **Bins:** `[31047,6,15,2,8,13,105,356,475,304,119,88,12,2,0,0,2,4,0,3]`
- **X labels:** "0", "2178", "4356"
- **Title:** "Capital_Loss (n=32,561) — 95% zero, peak around $1700-$2000"
- **Annotation:** translucent red overlay `rgba(231,76,60,0.12)` covering bin 1 (one bin wide, y 15-180), labeled "gap" in red `#e74c3c` 10px centered above at bin 1.5.

## 6. Hours_per_week

Meta pills: Type: **numerical** | Shape: spike (100%) | Gap: NO | Valley: structural (spike-created)

Summary: Massive spike at exactly 40 hours (~50% of data) with continuous tails on both sides. All bins 1–99 are populated so no gaps exist. Valleys flanking the spike are artifacts of the spike itself, not separate populations. No standard parametric model fits; structural binning (part-time/full-time/overtime) is more appropriate than distributional splitting.

**Pipeline note:** Spike-at-mode features require domain-informed structural binning rather than the standard shape-split-model path. (yellow `.skip` callout)

### Visualization (canvas `f6-hist`, 960×200)

- **Bins:** `[205,531,645,1547,1015,1302,1635,16100,2442,677,3036,841,1519,277,365,83,182,20,34,105]`
- **X labels:** "1", "50", "99"
- **Title:** "Hours/week (n=32,561) — massive spike at 40, continuous tails"
- **Annotation:** vertical dashed orange line (`#e67e22`, width 2, dash 4/2) at bin 7.5 from y=15 to y=180, labeled "40hr spike" in bold orange `#e67e22` 10px centered above the line.

## Regeneration instructions

- **Layout:** single-column page: h1 + `.subtitle`, then six `.example` boxes (background `#fafafa`, border `1px solid #e8e8e8`, radius 8px, padding 20px, margin 30px 0). Each box: `<h2>` "N. Feature" (numbered inline, not matching separate files), full-width `<canvas>`, `.meta` flex row of pill spans, `.summary-text` blue-left-border callout, then a `.result` or `.skip` callout.
- **Page CSS:** body system sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6. h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with bottom border `2px solid #2980b9` (margin-top 0 inside `.example`); subtitle `#666` 1.05em; p 0.92em `#333`. `.meta` flex gap 20px, 0.88em, spans monospace with background `#e8f0f8`, radius 4px; `.meta span.shape` green (background `#d4efdf`, border `1px solid #27ae60`, bold). `.summary-text` background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em, rounded right corners. `.result` green callout (background `#d4efdf`, border `1px solid #27ae60`, radius 6px, padding 10px 14px, 0.88em); `.skip` yellow callout (background `#fef9e7`, border `1px solid #f4d03f`).
- **Canvas:** `<canvas width="960" height="300">` attributes in HTML; shared `setup(id, w, h)` helper renders at 960×200 logical with `window.devicePixelRatio` scaling (backing store = rendered width × dpr, `ctx.scale` back to logical coordinates, CSS `width:100%; height:auto`); canvas element styled with background `#f8f9fa`, border `1px solid #e0e0e0`, radius 8px. Shared `drawHist(ctx, bins, w, h, opts)` renderer as described at top of this spec.
- **Palette:** #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange (band fill `rgba(230,126,34,0.25)` and 40hr-spike marker), bar fill `rgba(26,82,118,0.35)`, secondary blue `#2980b9`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
