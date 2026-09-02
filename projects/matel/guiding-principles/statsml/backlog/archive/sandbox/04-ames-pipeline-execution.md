# 18b. Ames Housing — Pipeline Execution

**Page type:** detail page (single-column sequence of `.example` boxes, each: h2 title, full-width canvas histogram, type/shape lines, summary paragraph, result/skip callout)
**HTML title tag:** 18b. Ames Housing — Pipeline Execution

**Subtitle:** How 7 numerical features from the Ames Housing dataset flow through the shape classification pipeline.

All seven canvases share one `drawHist` renderer: 20-bin histogram (bars `rgba(26,82,118,0.35)`, drawn only for non-zero bins, 2px gutter) with a Gaussian-smoothed density curve (sigma=1.2 over bin indices; bins are capped at 4× the median bin count for smoothing, then spike bins above the cap are restored), curve stroked `#1a5276` width 2, plus a confidence band filled `rgba(230,126,34,0.25)` (±1.96·smoothed/√effN, floor 3% of max, ceiling 120% of max). Margins: left/right 10, top 15, bottom 20. Thin `#ccc` baseline. Three x-axis labels (`#666` 11px, centered): near left edge, center, near right edge. Title top-left in bold 12px `#1a5276`. Canvas background `#f8f9fa`. HTML canvas attributes are `width="960" height="300"`, but the JS `setup()` renders each at 960×200 logical pixels.

## 1. SalePrice — Right-Skew Numerical (Target)

Type: `numerical`

Shape: right_skew (97%)

Strong right-skew with peak around $130k-$180k and long tail to $755k. No gap detected (all bins populated in the main body). No valley detected (single clear peak with monotone decline). Right-skew model fits well; produces 3 range-based extended features.

**Result:** 1 segment → right_skew model → 3 extended features (green `.result` callout)

### Visualization (canvas `f1-hist`, 960×200)

- **Bins:** `[11,135,451,882,565,343,210,119,88,46,35,16,11,3,6,3,4,0,0,2]`
- **X labels:** "$12.8k", "$384k", "$755k"
- **Title:** "SalePrice (n=2930)"

## 2. Lot Area — Extreme Right-Skew with Outliers

Type: `numerical`

Shape: heavy_tail (96%)

98% of data below 25k sq ft with extreme tail to 215k. Gap detected: empty bins above ~50k separate the main mass from scattered outlier parcels. Main segment (n~2870) fits right_skew; outlier segment (n~60) becomes a binary indicator.

**Result:** Gap split → 2 segments → right_skew on main + binary outlier flag → 4 extended features (green `.result` callout)

### Visualization (canvas `f2-hist`, 960×200)

- **Bins:** `[2301,570,30,11,10,3,1,0,0,0,1,0,0,0,1,1,0,0,0,1]`
- **X labels:** "1.3k", "108k", "215k"
- **Title:** "Lot Area (n=2930)"
- **Annotation:** translucent red overlay `rgba(231,76,60,0.1)` covering bins 7-19 (from x = 10+7·binW, width 13·binW, y 15-180), labeled "gap region" in red `#e74c3c` 10px centered above at bin 13.

## 3. Year Built — Left-Skew / Possible Bimodal

Type: `numerical`

Shape: ascending (100%)

Ascending distribution with recent builds dominating. No gap (all year ranges populated). Valley borderline: moderate dip in 1930s-1940s between peaks at ~1960s and 2005+. Multi-candidate approach runs both split and no-split paths; whichever produces stronger target associations wins.

**Multi-candidate:** Both pipeline paths execute. The one producing more significant range-target associations wins. (yellow `.skip` callout)

### Visualization (canvas `f3-hist`, 960×200)

- **Bins:** `[2,9,9,6,37,53,117,122,60,102,70,186,270,258,218,218,76,200,352,565]`
- **X labels:** "1872", "1941", "2010"
- **Title:** "Year Built (n=2930)"
- **Annotation:** translucent purple overlay `rgba(142,68,173,0.12)` covering bins 8-10 (3 bins wide, y 15-180), labeled "possible valley" in purple `#8e44ad` 10px centered above at bin 9.5.

## 4. Overall Qual — Ordinal (1–10 Scale)

Type: `ordinal` (dual numerical + categorical)

Shape: spike (90%)

10 discrete integer values peaking at 5 (n=825). Alternating zero bins in histogram are artifacts of integer values, not real gaps. Processed on both paths: numerical path detects discrete structure; categorical path tests each quality level against target and finds strong monotone relationship.

**Result:** Dual-path → ordinal groupings (low/mid/high/premium) + linear score → 5 extended features (green `.result` callout)

### Visualization (canvas `f4-hist`, 960×200)

- **Bins:** `[4,0,13,0,40,0,226,0,825,0,0,732,0,602,0,350,0,107,0,31]`
- **X labels:** "1", "5.5", "10"
- **Title:** "Overall Qual (n=2930)"
- No overlay annotation.

## 5. 2nd Flr SF — Zero-Inflated / Gap Split

Type: `numerical`

Shape: zero_inflated (100%)

57% of homes have zero (no 2nd floor), creating a massive spike at 0 with a structural gap before the continuous distribution begins at ~200 SF. Gap split separates zeros (n=1678) from non-zero values (n=1252). Non-zero segment is unimodal right-skew, fits right_skew.

**Result:** Gap split at 0 → binary indicator + 3 ranges on non-zero segment → 4 extended features (green `.result` callout)

### Visualization (canvas `f5-hist`, 960×200)

- **Bins:** `[1678,8,19,50,99,172,210,199,210,73,70,52,46,22,6,7,1,5,2,1]`
- **X labels:** "0", "1032", "2065"
- **Title:** "2nd Flr SF (n=2930)"
- **Annotation:** bin 1 (one bin wide, y 15-180) highlighted with red fill `rgba(231,76,60,0.15)` and dashed red border (`#e74c3c`, width 1.5, dash 4/2), labeled "gap" in red `#e74c3c` 10px centered above at bin 1.5.

## 6. Garage Area — Bell-Shaped with Zeros

Type: `numerical`

Shape: bell (94%)

Peak around 400-550 SF, roughly symmetric. Only 5.4% zeros (n=157), below the 20% threshold for triggering a point-mass split. No valley detected. Excluding zeros, normal distribution fits well. Produces binary garage indicator plus 3 size-based ranges.

**Result:** Borderline zero-inflation (5.4%) → binary split + bell-shaped main body → normal model → 4 extended features (green `.result` callout)

### Visualization (canvas `f6-hist`, 960×200)

- **Bins:** `[157,1,86,354,271,413,514,477,203,137,111,115,50,14,9,7,4,1,3,2]`
- **X labels:** "0", "744", "1488"
- **Title:** "Garage Area (n=2929)"
- No overlay annotation.

## 7. Pool Area — Extreme Zero-Inflation (Sparse)

Type: `numerical` (effectively binary)

Shape: zero_inflated (100%)

99.6% zeros (2917 of 2930 homes have no pool). Gap confirmed: completely empty bins between 0 and ~120 SF. Only 13 non-zero observations, far below minimum n=30 for shape classification or model fitting. Pipeline correctly gates further analysis.

**Sample sufficiency:** With only 13 non-zero observations, no parametric modeling is justified. Only output: has_pool binary indicator. (yellow `.skip` callout)

### Visualization (canvas `f7-hist`, 960×200)

- **Bins:** `[2917,0,0,1,0,1,0,0,0,1,0,1,3,1,2,0,1,0,1,1]`
- **X labels:** "0", "400", "800"
- **Title:** "Pool Area (n=2930)"
- **Annotation:** faint red overlay `rgba(231,76,60,0.08)` covering bins 1-19 (19 bins wide, y 15-180), labeled "n=13 total in this region" in red `#e74c3c` 10px centered above at bin 10.

## Regeneration instructions

- **Layout:** single-column page: h1 + `.subtitle`, then seven `.example` boxes (background `#fafafa`, border `1px solid #e8e8e8`, radius 8px, padding 20px, margin 30px 0). Each box: `<h2>` "N. Feature — Descriptor" (numbered inline, not matching separate files), full-width `<canvas>`, `.type-line` ("Type: `code`"), `.shape-line` ("Shape: …"), `.summary-text` paragraph, then a `.result` or `.skip` callout.
- **Page CSS:** body system sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6. h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with bottom border `2px solid #2980b9` (margin-top 0 inside `.example`); subtitle `#666` 1.05em; p 0.92em `#333`. `.type-line` 0.88em `#555` with `code` in green pill (background `#d4efdf`, border `1px solid #27ae60`, bold `#1a5276`). `.shape-line` 0.92em bold `#1a5276`. `.result` green callout (background `#d4efdf`, border `1px solid #27ae60`, radius 6px, padding 10px 14px, 0.88em); `.skip` yellow callout (background `#fef9e7`, border `1px solid #f4d03f`).
- **Canvas:** `<canvas width="960" height="300">` attributes in HTML; a shared `setup(id, w, h)` helper re-sizes to 960×200 logical with `window.devicePixelRatio` scaling (backing store = rendered width × dpr, `ctx.scale` back to logical coordinates, CSS `width:100%; height:auto`); canvas element styled with background `#f8f9fa`, border `1px solid #e0e0e0`, radius 8px. Shared `drawHist(ctx, bins, w, h, opts)` renderer as described at top of this spec.
- **Palette:** #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange (band fill `rgba(230,126,34,0.25)`), bar fill `rgba(26,82,118,0.35)`, purple `#8e44ad`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
