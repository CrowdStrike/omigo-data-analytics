# Ames Housing Dataset

**Page type:** other (single-page long dataset-exploration doc: stat boxes, data snippet table, type-card grid, CSS bar charts, JS-built tables, canvas summary charts, chart-cell gallery)
**HTML title tag:** Ames Housing Dataset — Summary & Exploration

**Subtitle:** Dataset exploration — feature types, distributions, and observations for pipeline validation

## Stat boxes (top of page)

Row of `.stat-box` elements (label over bold value):

| Label | Value |
|-------|-------|
| Rows | 2,930 |
| Columns | 82 |
| Numeric | 39 |
| Categorical | 43 |
| Target | SalePrice |

## Data Snippet

First 5 rows, selected columns (showing mix of numeric and categorical features):

| Order | Neighborhood | Bldg Type | Year Built | Gr Liv Area | Overall Qual | Garage Area | Total Bsmt SF | SalePrice |
|-------|--------------|-----------|------------|-------------|--------------|-------------|---------------|-----------|
| 1 | NAmes | 1Fam | 1960 | 1656 | 6 | 528 | 1080 | $215,000 |
| 2 | NAmes | 1Fam | 1961 | 896 | 5 | 730 | 882 | $105,000 |
| 3 | NAmes | 1Fam | 1958 | 1329 | 6 | 312 | 1329 | $172,000 |
| 4 | NAmes | 1Fam | 1968 | 2110 | 7 | 522 | 2110 | $244,000 |
| 5 | Gilbert | 1Fam | 1997 | 1629 | 5 | 482 | 928 | $189,900 |

## Feature Categories

The 82 columns fall into distinct groups relevant to our pipeline:

Four `.type-card` boxes in a 2-column grid, each an h4 title plus bullet list:

**Continuous (true numeric)**
- SalePrice — right-skewed, target variable
- Lot Area — extreme right-skew, possible outliers
- Gr Liv Area — right-skewed, area above grade
- Total Bsmt SF — right-skewed with zero-spike
- 1st Flr SF, Garage Area, Lot Frontage
- Mas Vnr Area, Wood Deck SF, Open Porch SF

**Zero-inflated (spike at 0 + spread)**
- 2nd Flr SF — 57% are zero (no 2nd floor)
- Mas Vnr Area — 65% are zero
- Wood Deck SF — 56% are zero
- Open Porch SF — mostly small or zero
- BsmtFin SF 1 — 44% are zero
- Pool Area, 3Ssn Porch, Screen Porch

**Discrete / Ordinal**
- Overall Qual — 1 to 10 scale
- Overall Cond — 1 to 9 scale
- Full Bath, Half Bath — counts (0-4)
- Bedroom AbvGr — count (0-8)
- Fireplaces, Garage Cars — counts
- Mo Sold, Yr Sold — time variables

**Categorical (nominal & ordinal)**
- Neighborhood — 28 levels
- MS Zoning — 7 levels (dominated by RL)
- House Style — 8 levels
- Foundation — 6 levels
- Exter Qual, Kitchen Qual — ordinal (Po-Ex)
- Fence, Pool QC, Alley — high missingness

## Missing Data

Features with significant missing values (NA means "not applicable" for most — no pool, no alley, no basement, etc.):

CSS horizontal bar chart (`.cat-section` of `.cat-chart` rows; bar width = percentage, gradient `#1a5276` → `#2980b9`):

| Feature | Bar width | Count label |
|---------|-----------|-------------|
| Pool QC | 99.6% | 2917 / 2930 (99.6%) |
| Misc Feature | 96.4% | 2824 / 2930 (96.4%) |
| Alley | 93.2% | 2732 / 2930 (93.2%) |
| Fence | 80.5% | 2358 / 2930 (80.5%) |
| Fireplace Qu | 48.5% | 1422 / 2930 (48.5%) |
| Lot Frontage | 16.7% | 490 / 2930 (16.7%) |
| Garage (all) | 5.4% | 159 / 2930 (5.4%) |
| Bsmt (all) | 2.7% | 80 / 2930 (2.7%) |

Note callout (`.note`, yellow `#fef9e7` background, left border `#d4ac0d`):

**Note:** In Ames Housing, "NA" typically means the feature doesn't exist (no garage, no pool) rather than missing data. This is a semantic encoding — the absence IS the information. For our pipeline, these features are better modeled as zero-inflated or binary (has/doesn't have) + conditional distribution.

## Numeric Feature Distributions

Density plots with histogram overlay for 15 key numeric features. These show the shape diversity relevant to our classification pipeline (docs 11-15).

### Visualization (dynamic canvas gallery in `#numericGrid`, each canvas 500×200)

A 3-column grid of `.chart-cell` boxes. Each cell: `.chart-label` (feature name, bold), a canvas density-plot, and a `.chart-stats` line "n=… | mean=… | med=… | std=… | range=[min, max]" (numbers formatted: ≥1M as "X.XM", ≥1000 as "Xk", else integer).

Chart type per cell: 20-bin histogram (bars `rgba(26,82,118,0.3)`, width plotW/20 minus 1px) with a Gaussian-smoothed density curve overlay (sigma=1.2 over bin indices), curve stroked `#1f6feb` width 2 with fill `rgba(31,111,235,0.2)` under it, drawn with quadratic curves through bin midpoints. Background `#f8f9fa`; margins left/right 10, top 15, bottom 25; thin gray `#ccc` baseline; x-axis labels in `#666` 17px at min, midpoint, and max values (formatted as above), centered near left edge, center, right edge.

Feature order and exact data (bins = 20 raw counts; stats):

| Feature | bins | min | max | n | mean | median | std |
|---------|------|-----|-----|---|------|--------|-----|
| SalePrice | [11,135,451,882,565,343,210,119,88,46,35,16,11,3,6,3,4,0,0,2] | 12789 | 755000 | 2930 | 180796 | 160000 | 79887 |
| Gr Liv Area | [10,179,553,616,646,436,215,126,88,35,11,6,3,1,0,2,1,1,0,1] | 334 | 5642 | 2930 | 1500 | 1442 | 506 |
| Lot Area | [2301,570,30,11,10,3,1,0,0,0,1,0,0,0,1,1,0,0,0,1] | 1300 | 215245 | 2930 | 10148 | 9437 | 7880 |
| Year Built | [2,9,9,6,37,53,117,122,60,102,70,186,270,258,218,218,76,200,352,565] | 1872 | 2010 | 2930 | 1971 | 1973 | 30 |
| Total Bsmt SF | [97,205,908,853,467,279,85,17,11,1,4,0,0,0,0,0,1,0,0,1] | 0 | 6110 | 2929 | 1052 | 990 | 441 |
| 1st Flr SF | [74,396,866,654,431,315,114,47,16,10,2,1,1,0,1,0,0,0,1,1] | 334 | 5095 | 2930 | 1160 | 1084 | 392 |
| 2nd Flr SF | [1678,8,19,50,99,172,210,199,210,73,70,52,46,22,6,7,1,5,2,1] | 0 | 2065 | 2930 | 336 | 0 | 428 |
| Garage Area | [157,1,86,354,271,413,514,477,203,137,111,115,50,14,9,7,4,1,3,2] | 0 | 1488 | 2929 | 473 | 480 | 215 |
| Lot Frontage | [164,273,617,687,436,149,63,28,10,5,3,2,1,0,0,0,0,0,0,2] | 21 | 313 | 2440 | 69 | 68 | 23 |
| Overall Qual | [4,0,13,0,40,0,226,0,825,0,0,732,0,602,0,350,0,107,0,31] | 1 | 10 | 2930 | 6.1 | 6 | 1.4 |
| Mas Vnr Area | [1902,259,263,183,92,73,46,26,21,16,4,6,3,5,3,2,1,1,0,1] | 0 | 1600 | 2907 | 102 | 0 | 179 |
| Wood Deck SF | [1631,355,504,210,114,54,31,14,5,7,2,0,2,0,0,0,0,0,0,1] | 0 | 1424 | 2930 | 94 | 0 | 126 |
| Open Porch SF | [1686,558,289,165,91,54,42,20,9,5,3,2,0,2,2,1,0,0,0,1] | 0 | 742 | 2930 | 48 | 27 | 68 |
| BsmtFin SF 1 | [1290,566,533,300,159,57,14,6,2,0,0,0,0,0,1,0,0,0,0,1] | 0 | 5644 | 2929 | 443 | 370 | 456 |
| Bsmt Unf SF | [429,349,363,330,262,254,246,167,118,97,64,67,59,57,31,19,9,4,3,1] | 0 | 2336 | 2929 | 559 | 466 | 440 |

## Categorical Feature Distributions

Three `.cat-section` boxes in the 3-column grid, each with an h3 heading and CSS bar rows (bar width = max(2, count/maxCount×200) px; label, bar, count).

**Neighborhood (28 levels)** — only the top 15 are rendered:
NAmes 443, CollgCr 267, OldTown 239, Edwards 194, Somerst 182, NridgHt 166, Gilbert 165, Sawyer 151, NWAmes 131, SawyerW 125, Mitchel 114, BrkSide 108, Crawfor 103, IDOTRR 93, Timber 72. (Full data array also includes: NoRidge 71, StoneBr 51, SWISU 48, ClearCr 44, MeadowV 37, BrDale 30, Blmngtn 28, Veenker 24, NPkVill 23, Blueste 10, Greens 8, GrnHill 2, Landmrk 1.)

**House Style:** 1Story 1481, 2Story 873, 1.5Fin 314, SLvl 128, SFoyer 83, 2.5Unf 24, 1.5Unf 19, 2.5Fin 8.

**Building Type:** 1Fam 2425, TwnhsE 233, Duplex 109, Twnhs 101, 2fmCon 62.

## Feature Type Classification (First Gate)

Before shape classification, each column is scored independently as numerical, binary, or categorical (doc 08). Scores are independent 0-100% — a feature can be 75% numerical AND 45% categorical simultaneously (ordinal). This determines which downstream path each feature takes.

JS-built table (`#typeTable`, font-size 0.82em) with columns Feature / n / Unique / Num% / Bin% / Cat% / Dominant / Notes. Row background by dominant type: numerical `#f0fff0`, binary `#f0f8ff`, tied `#fffbf0`, categorical white. Score cells monospace: ≥70 green `#2e7d32`, ≥40 orange `#e65100`, else `#bbb`; bold when ≥50. Dominant column colored: numerical `#2e7d32`, binary `#1a5276`, categorical `#8e44ad`, tied `#e65100`. Notes italic `#666`.

| Feature | n | Unique | Num% | Bin% | Cat% | Dominant | Notes |
|---------|---|--------|------|------|------|----------|-------|
| Order | 2930 | 2930 | 85 | 0 | 0 | numerical | ID-like — skip |
| PID | 2930 | 2930 | 85 | 0 | 0 | numerical | ID-like — skip |
| MS SubClass | 2930 | 16 | 45 | 0 | 45 | tied | numeric code for house type |
| MS Zoning | 2930 | 7 | 0 | 0 | 90 | categorical | |
| Lot Frontage | 2440 | 128 | 65 | 0 | 0 | numerical | |
| Lot Area | 2930 | 1960 | 85 | 0 | 0 | numerical | |
| Street | 2930 | 2 | 0 | 70 | 90 | categorical | Pave/Grvl |
| Alley | 198 | 2 | 0 | 95 | 90 | binary | |
| Lot Shape | 2930 | 4 | 0 | 0 | 90 | categorical | |
| Land Contour | 2930 | 4 | 0 | 0 | 90 | categorical | |
| Neighborhood | 2930 | 28 | 0 | 0 | 90 | categorical | |
| Bldg Type | 2930 | 5 | 0 | 0 | 90 | categorical | |
| House Style | 2930 | 8 | 0 | 0 | 90 | categorical | |
| Overall Qual | 2930 | 10 | 40 | 0 | 60 | categorical | ordinal 1-10 |
| Overall Cond | 2930 | 9 | 40 | 0 | 60 | categorical | ordinal 1-9 |
| Year Built | 2930 | 118 | 65 | 0 | 0 | numerical | |
| Year Remod/Add | 2930 | 61 | 65 | 0 | 0 | numerical | |
| Mas Vnr Area | 2907 | 445 | 75 | 0 | 0 | numerical | |
| BsmtFin SF 1 | 2929 | 995 | 75 | 0 | 0 | numerical | |
| BsmtFin SF 2 | 2929 | 274 | 65 | 0 | 0 | numerical | |
| Bsmt Unf SF | 2929 | 1137 | 75 | 0 | 0 | numerical | |
| Total Bsmt SF | 2929 | 1058 | 75 | 0 | 0 | numerical | |
| Central Air | 2930 | 2 | 0 | 95 | 90 | binary | Y/N |
| 1st Flr SF | 2930 | 1083 | 75 | 0 | 0 | numerical | |
| 2nd Flr SF | 2930 | 635 | 75 | 0 | 0 | numerical | |
| Gr Liv Area | 2930 | 1292 | 75 | 0 | 0 | numerical | |
| Bsmt Full Bath | 2928 | 4 | 40 | 0 | 60 | categorical | count 0-3 |
| Full Bath | 2930 | 5 | 40 | 0 | 60 | categorical | count 0-4 |
| Half Bath | 2930 | 3 | 40 | 0 | 60 | categorical | count 0-2 |
| Bedroom AbvGr | 2930 | 8 | 40 | 0 | 60 | categorical | count 0-8 |
| Kitchen AbvGr | 2930 | 4 | 40 | 0 | 60 | categorical | count 1-3 |
| TotRms AbvGrd | 2930 | 14 | 45 | 0 | 45 | tied | count 2-15 |
| Fireplaces | 2930 | 5 | 40 | 0 | 60 | categorical | count 0-4 |
| Garage Yr Blt | 2771 | 103 | 65 | 0 | 0 | numerical | |
| Garage Cars | 2929 | 6 | 40 | 0 | 60 | categorical | count 0-5 |
| Garage Area | 2929 | 603 | 75 | 0 | 0 | numerical | |
| Wood Deck SF | 2930 | 380 | 75 | 0 | 0 | numerical | |
| Open Porch SF | 2930 | 252 | 65 | 0 | 0 | numerical | |
| Enclosed Porch | 2930 | 183 | 65 | 0 | 0 | numerical | |
| Screen Porch | 2930 | 121 | 65 | 0 | 0 | numerical | |
| Pool Area | 2930 | 14 | 45 | 0 | 45 | tied | mostly zero |
| Mo Sold | 2930 | 12 | 45 | 0 | 45 | tied | month 1-12 |
| Yr Sold | 2930 | 5 | 40 | 0 | 60 | categorical | 2006-2010 |
| SalePrice | 2930 | 1032 | 75 | 0 | 0 | numerical | target |

### Visualization (canvas `typeSummaryChart`, 600×140)

Horizontal bar chart of dominant-type counts computed from the table above (Numerical 19, Categorical 17, Binary 2, Tied (ordinal) 6 — computed at render time from the data).

- **Background:** `#f8f9fa`, canvas styled `width:100%; height:auto`, rounded 8px.
- **Bars:** start at x=140, max width 350, height 20, one per row at y = 20 + i×30; width proportional to count/total (total 44). Colors: Numerical `#2e7d32`, Categorical `#8e44ad`, Binary `#1a5276`, Tied (ordinal) `#e65100`.
- **Labels:** type name right-aligned in `#2a2a2a` 17px left of bar; right of bar in `#555`: "N features (P%)" with P = round(count/total×100).
- **Title (bottom center, bold 17px `#2a2a2a`):** "Feature Type Distribution (44 selected columns shown)".

## CNN Shape Classification (Multi-Rendering Pipeline)

Each numeric feature classified by the trained multi-rendering CNN (docs 14-15). Same data rendered 3 ways — histogram at Sturges bins, histogram at √n bins, and KDE density — then classified independently. Disagreement between renderings flags artifacts.

JS-built table (`#cnnTable`, font-size 0.82em) with columns Feature / n / Unique / Hist Sturges / Hist √n / KDE / Verdict / Agree / Artifact Signal. Classification cells monospace as "shape (confidence%)". Rows with disagreement get background `#fff8f0`. Verdict and Agree colored `#2e7d32` when agree=YES, `#e65100` when NO; artifact column italic `#888`.

| Feature | n | Unique | Hist Sturges | Hist √n | KDE | Verdict | Agree | Artifact Signal |
|---------|---|--------|--------------|---------|-----|---------|-------|-----------------|
| MS SubClass | 2930 | 16 | multimodal (67%) | multimodal (39%) | multimodal (73%) | multimodal | YES | |
| Lot Frontage | 2440 | 128 | heavy_tail (91%) | heavy_tail (87%) | heavy_tail (97%) | heavy_tail | YES | |
| Lot Area | 2930 | 1960 | heavy_tail (43%) | heavy_tail (74%) | heavy_tail (84%) | heavy_tail | YES | |
| Overall Qual | 2930 | 10 | multimodal (79%) | right_skew (26%) | heavy_tail (97%) | multimodal | NO | kde_disagrees |
| Overall Cond | 2930 | 9 | heavy_tail (45%) | heavy_tail (46%) | heavy_tail (59%) | heavy_tail | YES | |
| Year Built | 2930 | 118 | ascending (90%) | multimodal (83%) | multimodal (91%) | multimodal | NO | bin_sensitivity |
| Year Remod/Add | 2930 | 61 | u_shaped (79%) | u_shaped (41%) | multimodal (98%) | u_shaped | NO | kde_disagrees |
| Mas Vnr Area | 2907 | 445 | descending (82%) | spike (31%) | descending (92%) | descending | NO | bin_sensitivity |
| BsmtFin SF 1 | 2929 | 995 | descending (96%) | u_shaped (32%) | descending (94%) | descending | NO | bin_sensitivity |
| BsmtFin SF 2 | 2929 | 274 | heavy_tail (54%) | spike (44%) | spike (37%) | spike | NO | bin_sensitivity |
| Bsmt Unf SF | 2929 | 1137 | descending (80%) | descending (96%) | descending (91%) | descending | YES | |
| Total Bsmt SF | 2929 | 1058 | heavy_tail (96%) | heavy_tail (96%) | heavy_tail (96%) | heavy_tail | YES | |
| 1st Flr SF | 2930 | 1083 | right_skew (97%) | right_skew (67%) | heavy_tail (86%) | right_skew | NO | kde_disagrees |
| 2nd Flr SF | 2930 | 635 | descending (52%) | spike (35%) | heavy_tail (55%) | descending | NO | kde_disagrees |
| Low Qual Fin SF | 2930 | 36 | heavy_tail (54%) | spike (44%) | spike (43%) | spike | NO | bin_sensitivity |
| Gr Liv Area | 2930 | 1292 | right_skew (88%) | heavy_tail (60%) | heavy_tail (85%) | heavy_tail | NO | bin_sensitivity |
| Bedroom AbvGr | 2930 | 8 | multimodal (71%) | spike (29%) | multimodal (58%) | multimodal | NO | bin_sensitivity |
| TotRms AbvGrd | 2930 | 14 | heavy_tail (89%) | right_skew (46%) | heavy_tail (94%) | heavy_tail | NO | bin_sensitivity |
| Garage Yr Blt | 2771 | 103 | ascending (97%) | ascending (61%) | ascending (48%) | ascending | YES | |
| Garage Area | 2929 | 603 | heavy_tail (75%) | heavy_tail (71%) | heavy_tail (97%) | heavy_tail | YES | |
| Wood Deck SF | 2930 | 380 | descending (90%) | spike (26%) | descending (91%) | descending | NO | bin_sensitivity |
| Open Porch SF | 2930 | 252 | descending (87%) | descending (57%) | descending (96%) | descending | YES | |
| Enclosed Porch | 2930 | 183 | spike (31%) | spike (44%) | descending (27%) | spike | NO | kde_disagrees |
| 3Ssn Porch | 2930 | 31 | heavy_tail (54%) | spike (44%) | spike (44%) | spike | NO | bin_sensitivity |
| Screen Porch | 2930 | 121 | heavy_tail (54%) | spike (44%) | spike (43%) | spike | NO | bin_sensitivity |
| Pool Area | 2930 | 14 | heavy_tail (54%) | spike (44%) | spike (44%) | spike | NO | bin_sensitivity |
| Misc Val | 2930 | 38 | heavy_tail (54%) | spike (44%) | spike (43%) | spike | NO | bin_sensitivity |
| Mo Sold | 2930 | 12 | multimodal (38%) | multimodal (52%) | multimodal (83%) | multimodal | YES | |
| SalePrice | 2930 | 1032 | heavy_tail (57%) | heavy_tail (70%) | heavy_tail (91%) | heavy_tail | YES | |

Legend line below the table (0.82em, `#555`): green `#2e7d32` swatch = "All 3 agree"; orange `#e65100` swatch = "Disagreement (artifact detected)"; italic: "bin_sensitivity = histogram bin count changes classification | kde_disagrees = KDE sees different shape than both histograms".

### Visualization (canvas `cnnShapeChart`, 700×220)

Horizontal bar chart of verdict counts, sorted descending (computed from the table: heavy_tail 8, spike 6, descending 6, multimodal 4, right_skew 1, ascending 1, u_shaped 1).

- **Background:** `#f8f9fa`; margins left 120, right 20, top 20, bottom 30; bar height min(22, rowHeight−4); bar width proportional to count/maxCount over the plot width.
- **Bar colors by shape:** heavy_tail `#1a5276`, spike `#8e44ad`, descending `#27ae60`, multimodal `#e74c3c`, right_skew `#d35400`, ascending `#2980b9`, u_shaped `#16a085`, bell `#f39c12`, left_skew `#c0392b`, bimodal `#7d3c98`, uniform `#2c3e50` (fallback `#555`).
- **Labels:** shape name right-aligned `#2a2a2a` 17px left of bar; "N features" in `#555` right of bar.
- **Title (bottom center, bold 17px):** "Shape Classification Distribution (29 numeric features)".

## Generative CNN — Independent Class Scores

Unlike the softmax model (which forces scores to sum to 100%), the generative model gives each class an **independent 0-100% score**. A feature can be 95% right_skew AND 34% bell simultaneously — acknowledging that real distributions sit between archetypes.

JS-built table (`#genTable`, font-size 0.82em) with columns Feature / Score 1 / Score 2 / Score 3 / Rendering Agreement. Score cells monospace "shape P%" with a 3px mini progress bar under the text (track `#ddd`; fill width = P%; fill color ≥80 `#2e7d32`, ≥50 `#1a5276`, else `#999`; text color ≥80 `#2e7d32`, ≥50 `#1a5276`, else `#888`; bold when ≥60). Agreement column: "Stable" (green `#2e7d32`) when agree, else "Varies" (orange `#e65100`); agreeing rows get background `#f0fff0`.

| Feature | Score 1 | Score 2 | Score 3 | Rendering Agreement |
|---------|---------|---------|---------|---------------------|
| MS SubClass | multimodal 67% | right_skew 48% | heavy_tail 31% | Varies |
| Lot Frontage | heavy_tail 97% | bell 44% | right_skew 31% | Stable |
| Lot Area | right_skew 83% | heavy_tail 61% | descending 21% | Varies |
| Overall Qual | heavy_tail 42% | multimodal 36% | bell 32% | Varies |
| Overall Cond | spike 52% | heavy_tail 49% | right_skew 45% | Varies |
| Year Built | multimodal 59% | bimodal 32% | u_shaped 31% | Varies |
| Year Remod/Add | u_shaped 63% | multimodal 47% | bimodal 31% | Varies |
| Mas Vnr Area | descending 77% | right_skew 55% | heavy_tail 41% | Varies |
| BsmtFin SF 1 | descending 72% | u_shaped 47% | right_skew 39% | Varies |
| BsmtFin SF 2 | descending 84% | heavy_tail 73% | right_skew 55% | Varies |
| Bsmt Unf SF | right_skew 76% | descending 60% | heavy_tail 23% | Varies |
| Total Bsmt SF | heavy_tail 94% | bell 45% | right_skew 31% | Stable |
| 1st Flr SF | right_skew 97% | bell 35% | heavy_tail 31% | Stable |
| 2nd Flr SF | right_skew 34% | heavy_tail 34% | u_shaped 31% | Varies |
| Low Qual Fin SF | heavy_tail 78% | descending 76% | spike 63% | Stable |
| Gr Liv Area | right_skew 99% | bell 34% | heavy_tail 30% | Stable |
| Bedroom AbvGr | multimodal 69% | spike 31% | bell 30% | Varies |
| TotRms AbvGrd | right_skew 72% | heavy_tail 37% | multimodal 33% | Varies |
| Garage Yr Blt | ascending 54% | multimodal 42% | left_skew 22% | Varies |
| Garage Area | heavy_tail 72% | bell 53% | right_skew 31% | Varies |
| Wood Deck SF | descending 67% | heavy_tail 49% | right_skew 48% | Varies |
| Open Porch SF | descending 93% | right_skew 59% | heavy_tail 29% | Stable |
| Enclosed Porch | descending 69% | heavy_tail 61% | right_skew 55% | Varies |
| 3Ssn Porch | heavy_tail 78% | descending 76% | spike 63% | Stable |
| Screen Porch | descending 80% | heavy_tail 74% | spike 53% | Varies |
| Pool Area | descending 55% | heavy_tail 55% | spike 44% | Varies |
| Misc Val | descending 80% | heavy_tail 77% | spike 59% | Stable |
| Mo Sold | bell 53% | multimodal 40% | heavy_tail 32% | Varies |
| SalePrice | right_skew 95% | bell 34% | heavy_tail 30% | Stable |

Note callout:

**Interpretation:** Multiple high scores = distribution sits between archetypes. For example, SalePrice is "95% right_skew, 34% bell, 30% heavy_tail" — it's primarily right-skewed but has enough bell-like symmetry and tail weight to partially match those archetypes too. This is more informative than the softmax model's single verdict.

## Observations for Pipeline Validation

### Shape Diversity

This dataset is excellent for testing shape classification because it contains:

- **Right-skewed:** SalePrice, Lot Area, Gr Liv Area, 1st Flr SF — classic right-tail features
- **Zero-inflated / bimodal:** 2nd Flr SF (giant spike at 0 + separate distribution), Mas Vnr Area, Wood Deck SF
- **Approximately bell:** Lot Frontage, Garage Area (after removing zeros)
- **Left-skewed / ascending:** Year Built (more recent homes dominate)
- **Uniform-ish:** Bsmt Unf SF shows a relatively flat spread
- **Discrete bounded:** Overall Qual (1-10), bath counts — integer distributions
- **Extreme outliers:** Lot Area (215k max vs 10k median), SalePrice (755k max vs 160k median)

### Gap Splitting Candidates (Doc 16)

Features where empty-bin gaps may indicate genuinely separated populations:

- **Garage Area:** Spike at 0 (no garage) then gap before the main distribution starts ~200 SF
- **2nd Flr SF:** Massive zero-spike, gap, then separate distribution — classic structural bimodality
- **Lot Area:** Main mass under 20k, then isolated extreme parcels at 50k-215k

### Valley Splitting Candidates (Doc 17)

Features where the distribution has a valley (non-zero dip between two modes):

- **Year Built:** Possible bimodality between older stock (1950s-60s) and newer construction (1990s-2010)
- **Overall Qual:** Bimodal-ish — peaks at 5 and 7
- **SalePrice:** Slight shoulder suggesting mixture of price tiers

### Pipeline Challenges

Note callout:

**Challenge 1 — Zero inflation:** Features like 2nd Flr SF, Wood Deck SF have a structural zero (absence of feature). The pipeline must decide: split at zero vs. treat zeros as part of the distribution. Gap splitting (doc 16) should detect the empty bins between 0 and the first non-zero values.

**Challenge 2 — Extreme skew + outliers:** Lot Area has 98% of data below 25k but max is 215k. With 20 bins, most data gets compressed into 2-3 bins. The pipeline needs to handle dynamic binning or transformation detection (Phase 2).

**Challenge 3 — Ordinal discrete:** Overall Qual is numeric (1-10) but discrete. The histogram looks "gapped" between integers. The pipeline must distinguish structural gaps (discrete values) from genuine population separation.

## Regeneration instructions

- **Layout:** single long page in document order: h1 + `.subtitle`, inline `.stat-box` row, h2 sections separated by comment dividers. Sections use: scrollable data-snippet table, 2-column `.feature-types` grid of `.type-card` boxes, `.cat-section` CSS bar charts, `.note` callouts, a 3-column `.grid` of `.chart-cell` canvas density plots, JS-built tables (`#typeTable`, `#cnnTable`, `#genTable`) populated from data arrays, and two summary canvases. Responsive grid: 2 columns below 900px, 1 below 600px.
- **Page CSS:** body system sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6. h1 2em `#1a5276`; h2 1.5em `#1a5276` with bottom border `1px solid #ddd`; h3 1.2em `#6c3483`; subtitle `#555` 1.1em. Tables `display:block` scrollable, 0.85em, cell borders `1px solid #ddd`, header background `#f0f4f8` `#1a5276` sticky, zebra rows `#f8f9fa`/white. `.stat-box` gray `#f8f9fa` rounded box, label 0.85em `#555`, value 1.4em bold `#1a5276`. `.note` background `#fef9e7`, left border `3px solid #d4ac0d`, rounded right corners. `code` background `#f0f4f8`, `#1a5276`. `.cat-bar` 20px tall, gradient `linear-gradient(90deg, #1a5276, #2980b9)`, radius 3px. `.chart-cell` background `#f8f9fa`, border `#e0e0e0`, radius 8px.
- **Palette:** #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange; plus this page's density-curve blue `#1f6feb`, greens `#2e7d32`, orange `#e65100`, purple `#8e44ad`.
- **Canvases:** all use `window.devicePixelRatio` scaling (backing store = rendered width × dpr, `ctx.scale` back to logical coordinates, CSS size fixed); density gallery canvases 500×200 logical, summary charts 600×140 and 700×220 with `style="width:100%; height:auto"`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
