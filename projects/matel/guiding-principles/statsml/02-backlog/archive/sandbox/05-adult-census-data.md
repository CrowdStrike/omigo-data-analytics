# Adult Census Dataset

**Page type:** other (single-page long dataset-exploration doc: stat boxes, target split bar, data snippet table, type-card grid, CSS bar charts, JS-built tables, canvas summary charts, chart-cell gallery)
**HTML title tag:** Adult Census Dataset — Summary & Exploration

**Subtitle:** Dataset exploration — income prediction (>50K vs <=50K) from demographic features

## Stat boxes (top of page)

Row of `.stat-box` elements (label over bold value):

| Label | Value |
|-------|-------|
| Train Rows | 32,561 |
| Test Rows | 16,282 |
| Columns | 15 |
| Numeric | 6 |
| Categorical | 9 |
| Target | Income >50K |

## Target Distribution

Binary classification: predict whether income exceeds $50K/year. Imbalanced — 76% earn <=50K.

`.target-bar` — a single 30px-tall horizontal stacked bar (rounded 6px, white bold labels centered in each segment):

| Segment | Width | Background | Label |
|---------|-------|------------|-------|
| <=50K | 75.9% | `#2980b9` | <=50K: 24,720 (75.9%) |
| >50K | 24.1% | `#e74c3c` | >50K: 7,841 (24.1%) |

## Data Snippet

First 5 rows showing the mix of demographic, employment, and financial features:

| Age | Workclass | Education | Edu Num | Marital Status | Occupation | Sex | Capital Gain | Hours/wk | Country | Target |
|-----|-----------|-----------|---------|----------------|------------|-----|--------------|----------|---------|--------|
| 39 | State-gov | Bachelors | 13 | Never-married | Adm-clerical | Male | 2174 | 40 | United-States | <=50K |
| 50 | Self-emp-not-inc | Bachelors | 13 | Married-civ-spouse | Exec-managerial | Male | 0 | 13 | United-States | <=50K |
| 38 | Private | HS-grad | 9 | Divorced | Handlers-cleaners | Male | 0 | 40 | United-States | <=50K |
| 53 | Private | 11th | 7 | Married-civ-spouse | Handlers-cleaners | Male | 0 | 40 | United-States | <=50K |
| 28 | Private | Bachelors | 13 | Married-civ-spouse | Prof-specialty | Female | 0 | 40 | United-States | <=50K |

## Feature Categories

Four `.type-card` boxes in a 2-column grid, each an h4 title plus bullet list:

**Continuous Numeric**
- Age — 17 to 90, roughly uniform with right decay
- fnlwgt — census sampling weight, right-skewed
- Hours_per_week — massive spike at 40 (full-time)

**Zero-inflated Numeric**
- Capital_Gain — 95% zeros, then scattered values + spike at 99999
- Capital_Loss — 95% zeros, then a separate small distribution

**Discrete / Ordinal**
- Education_Num — ordinal encoding of education (1-16)

**Categorical**
- Workclass — 8 levels, dominated by Private (70%)
- Education — 16 levels, HS-grad most common
- Marital_Status — 7 levels
- Occupation — 14 levels, fairly balanced top-5
- Relationship — 6 levels
- Race — 5 levels, dominated by White (85%)
- Sex — binary (Male 67%, Female 33%)
- Country — 41 levels, dominated by US (91%)
- Target — binary (<=50K, >50K)

## Missing Data

Missing values are encoded as "?" in this dataset. Only 3 features have missing values:

CSS bar chart (`.cat-section` of `.cat-chart` rows; here bar widths are fixed pixel values, gradient `#1a5276` → `#2980b9`):

| Feature | Bar width | Count label |
|---------|-----------|-------------|
| Occupation | 28px | 1,843 / 32,561 (5.7%) |
| Workclass | 28px | 1,836 / 32,561 (5.6%) |
| Country | 9px | 583 / 32,561 (1.8%) |

Note callout:

**Note:** Workclass and Occupation are missing together — they correspond to the same individuals (likely "Never-worked" or people outside the labor force). This is structural missingness, not random.

## Numeric Feature Distributions

Density plots with histogram overlay for all 6 numeric features. These demonstrate very different distribution shapes — from near-uniform (Age) to extreme zero-inflation (Capital_Gain).

### Visualization (dynamic canvas gallery in `#numericGrid`, each canvas 500×200)

3-column grid of `.chart-cell` boxes. Each cell: `.chart-label` (feature name, bold), a canvas, and a `.chart-stats` line "n=… | mean=… | med=… | std=… | [min, max]" (n with locale commas; numbers formatted ≥1M as "X.XM", ≥1000 as "Xk", else integer).

Chart type per cell: 20-bin histogram (bars `rgba(26,82,118,0.3)`, width plotW/20 minus 1px) with Gaussian-smoothed density curve overlay (sigma=1.2 over bin indices), stroked `#1a5276` width 2 with fill `rgba(31,111,235,0.15)` under it, drawn with quadratic curves through bin midpoints. Background `#f8f9fa`; margins left/right 10, top 15, bottom 25; thin `#ccc` baseline; x-axis labels `#666` 17px at min, midpoint, max.

Feature order and exact data:

| Feature | bins | min | max | n | mean | median | std |
|---------|------|-----|-----|---|------|--------|-----|
| Age | [2410,3160,2461,3429,3465,2583,3198,2965,1828,2139,1558,1033,996,599,269,227,120,54,20,47] | 17 | 90 | 32561 | 38.6 | 37 | 13.6 |
| fnlwgt | [4483,8634,10945,4382,2510,988,341,136,64,38,15,5,5,5,2,3,1,1,1,2] | 12285 | 1484705 | 32561 | 189778 | 178356 | 105550 |
| Education_Num | [51,168,333,0,646,514,933,0,1175,433,10501,0,7291,1382,1067,0,5355,1723,576,413] | 1 | 16 | 32561 | 10.1 | 10 | 2.6 |
| Capital_Gain | [30913,878,157,360,38,49,5,0,2,0,0,0,0,0,0,0,0,0,0,159] | 0 | 99999 | 32561 | 1078 | 0 | 7385 |
| Capital_Loss | [31047,6,15,2,8,13,105,356,475,304,119,88,12,2,0,0,2,4,0,3] | 0 | 4356 | 32561 | 87 | 0 | 403 |
| Hours_per_week | [205,531,645,1547,1015,1302,1635,16100,2442,677,3036,841,1519,277,365,83,182,20,34,105] | 1 | 99 | 32561 | 40.4 | 40 | 12.3 |

## Categorical Feature Distributions

Six `.cat-section` boxes in two 3-column grid rows, each with an h3 heading and CSS bar rows (bar width = max(2, count/maxCount×200) px; label min-width 120px; counts locale-formatted).

**Education (16 levels):** HS-grad 10,501; Some-college 7,291; Bachelors 5,355; Masters 1,723; Assoc-voc 1,382; 11th 1,175; Assoc-acdm 1,067; 10th 933; 7th-8th 646; Prof-school 576; 9th 514; 12th 433; Doctorate 413; 5th-6th 333; 1st-4th 168; Preschool 51.

**Occupation (14 levels):** Prof-specialty 4,140; Craft-repair 4,099; Exec-managerial 4,066; Adm-clerical 3,770; Sales 3,650; Other-service 3,295; Machine-op-inspct 2,002; Transport-moving 1,597; Handlers-cleaners 1,370; Farming-fishing 994; Tech-support 928; Protective-serv 649; Priv-house-serv 149; Armed-Forces 9.

**Marital Status** (data key "Martial_Status"): Married-civ-spouse 14,976; Never-married 10,683; Divorced 4,443; Separated 1,025; Widowed 993; Married-spouse-absent 418; Married-AF-spouse 23.

**Workclass:** Private 22,696; Self-emp-not-inc 2,541; Local-gov 2,093; State-gov 1,298; Self-emp-inc 1,116; Federal-gov 960; Without-pay 14; Never-worked 7.

**Relationship:** Husband 13,193; Not-in-family 8,305; Own-child 5,068; Unmarried 3,446; Wife 1,568; Other-relative 981.

**Race:** White 27,816; Black 3,124; Asian-Pac-Islander 1,039; Amer-Indian-Eskimo 311; Other 271.

## Feature Type Classification (First Gate)

Before shape classification, each column is scored independently as numerical, binary, or categorical (doc 08). Scores are independent 0-100% — a feature can be 75% numerical AND 45% categorical simultaneously (ordinal). This determines which downstream path each feature takes.

JS-built table (`#typeTable`, 0.82em), columns Feature / n / Unique / Num% / Bin% / Cat% / Dominant / Notes. Row background by dominant: numerical `#f0fff0`, binary `#f0f8ff`, tied `#fffbf0`, categorical white. Score cells monospace: ≥70 `#2e7d32`, ≥40 `#e65100`, else `#bbb`; bold ≥50. Dominant colors: numerical `#2e7d32`, binary `#1a5276`, categorical `#8e44ad`, tied `#e65100`. Notes italic `#666`.

| Feature | n | Unique | Num% | Bin% | Cat% | Dominant | Notes |
|---------|---|--------|------|------|------|----------|-------|
| Age | 32561 | 73 | 55 | 0 | 0 | numerical | integer, working age |
| Workclass | 30725 | 8 | 0 | 0 | 90 | categorical | Private, Gov, etc |
| fnlwgt | 32561 | 21648 | 85 | 0 | 0 | numerical | high cardinality |
| Education | 32561 | 16 | 0 | 0 | 90 | categorical | HS-grad, Bachelors, etc |
| Education_Num | 32561 | 16 | 45 | 0 | 45 | tied | ordinal encoding |
| Martial_Status | 32561 | 7 | 0 | 0 | 90 | categorical | |
| Occupation | 30718 | 14 | 0 | 0 | 90 | categorical | |
| Relationship | 32561 | 6 | 0 | 0 | 90 | categorical | |
| Race | 32561 | 5 | 0 | 0 | 90 | categorical | |
| Sex | 32561 | 2 | 0 | 95 | 90 | binary | Male/Female |
| Capital_Gain | 32561 | 119 | 55 | 0 | 0 | numerical | zero-inflated |
| Capital_Loss | 32561 | 92 | 55 | 0 | 0 | numerical | zero-inflated |
| Hours_per_week | 32561 | 94 | 55 | 0 | 0 | numerical | spike at 40 |
| Country | 31978 | 41 | 0 | 0 | 75 | categorical | 91% United-States |
| Target | 32561 | 2 | 0 | 95 | 90 | binary | <=50K / >50K |

### Visualization (canvas `typeSummaryChart`, 600×140)

Horizontal bar chart of dominant-type counts computed from the table above (Numerical 5, Categorical 7, Binary 2, Tied (ordinal) 1 — computed at render time).

- **Background:** `#f8f9fa`, canvas styled `width:100%; height:auto`, rounded 8px.
- **Bars:** start x=140, max width 350, height 20, one per row at y = 20 + i×30; width = count/total (total 15) × 350. Colors: Numerical `#2e7d32`, Categorical `#8e44ad`, Binary `#1a5276`, Tied (ordinal) `#e65100`.
- **Labels:** type name right-aligned `#2a2a2a` 17px left of bar; "N features (P%)" in `#555` right of bar.
- **Title (bottom center, bold 17px `#2a2a2a`):** "Feature Type Distribution (15 columns)".

Note callout below the chart:

**Key observation:** Adult Census has a clean separation — features are either clearly numerical (Age, fnlwgt, etc.) or clearly categorical (Workclass, Education, etc.). The only ambiguous feature is `Education_Num` (num:45, cat:45) which is a numeric encoding of the categorical Education column. The binary features (Sex, Target) are correctly identified with 95% confidence.

## CNN Shape Classification (Multi-Rendering Pipeline)

Each numeric feature classified by the trained multi-rendering CNN (docs 14-15). Same data rendered 3 ways — histogram at Sturges bins, histogram at √n bins, and KDE density — then classified independently. Disagreement between renderings flags artifacts.

JS-built table (`#cnnTable`, 0.82em), columns Feature / n / Unique / Hist Sturges / Hist √n / KDE / Verdict / Agree / Artifact Signal. Classification cells monospace "shape (confidence%)". Disagreeing rows background `#fff8f0`; Verdict/Agree colored `#2e7d32` (YES) or `#e65100` (NO); artifact italic `#888`.

| Feature | n | Unique | Hist Sturges | Hist √n | KDE | Verdict | Agree | Artifact Signal |
|---------|---|--------|--------------|---------|-----|---------|-------|-----------------|
| Age | 32561 | 73 | right_skew (96%) | right_skew (96%) | right_skew (96%) | right_skew | YES | |
| fnlwgt | 32561 | 21648 | right_skew (97%) | right_skew (96%) | right_skew (97%) | right_skew | YES | |
| Education_Num | 32561 | 16 | heavy_tail (46%) | spike (91%) | heavy_tail (69%) | heavy_tail | NO | bin_sensitivity |
| Capital_Gain | 32561 | 119 | spike (33%) | spike (44%) | spike (41%) | spike | YES | |
| Capital_Loss | 32561 | 92 | heavy_tail (54%) | spike (44%) | spike (40%) | spike | NO | bin_sensitivity |
| Hours_per_week | 32561 | 94 | spike (91%) | spike (95%) | spike (90%) | spike | YES | |

Legend line below the table (0.82em, `#555`): green `#2e7d32` swatch = "All 3 agree"; orange `#e65100` swatch = "Disagreement (artifact detected)"; italic: "bin_sensitivity = histogram bin count changes classification | kde_disagrees = KDE sees different shape than both histograms".

### Visualization (canvas `cnnShapeChart`, 700×160)

Horizontal bar chart of verdict counts, sorted descending (computed from the table: spike 3, right_skew 2, heavy_tail 1).

- **Background:** `#f8f9fa`; margins left 120, right 20, top 20, bottom 30; bar height min(26, rowHeight−6); width proportional to count/maxCount.
- **Bar colors by shape:** heavy_tail `#1a5276`, spike `#8e44ad`, descending `#27ae60`, multimodal `#e74c3c`, right_skew `#d35400`, ascending `#2980b9`, u_shaped `#16a085`, bell `#f39c12`, left_skew `#c0392b`, bimodal `#7d3c98`, uniform `#2c3e50` (fallback `#555`).
- **Labels:** shape name right-aligned `#2a2a2a` 17px left of bar; "N features" `#555` right of bar.
- **Title (bottom center, bold 17px):** "Shape Classification Distribution (6 numeric features)".

Note callout below the chart:

**Key findings:** 4/6 features agree across all renderings (67%). The two disagreements both involve **zero-inflation** — Capital_Loss and Education_Num have structural patterns (massive zero-spike or discrete gaps) that change appearance with bin count. This is exactly what the multi-rendering pipeline is designed to detect: the same underlying data looks like different shapes depending on bin resolution, signaling an artifact rather than a clean archetype.

## Generative CNN — Independent Class Scores

The generative model gives each class an **independent 0-100% score** (sigmoid, not softmax). Multiple classes can score high simultaneously, revealing how real distributions sit between archetypes.

JS-built table (`#genTable`, 0.82em), columns Feature / Score 1 / Score 2 / Score 3 / Rendering Agreement. Score cells monospace "shape P%" with 3px mini progress bar (track `#ddd`; fill width P%; fill ≥80 `#2e7d32`, ≥50 `#1a5276`, else `#999`; text ≥80 `#2e7d32`, ≥50 `#1a5276`, else `#888`; bold ≥60). Agreement: "Stable" green `#2e7d32` / "Varies" orange `#e65100`; agreeing rows background `#f0fff0`.

| Feature | Score 1 | Score 2 | Score 3 | Rendering Agreement |
|---------|---------|---------|---------|---------------------|
| Age | right_skew 98% | bell 36% | heavy_tail 31% | Stable |
| fnlwgt | right_skew 94% | bell 35% | heavy_tail 31% | Stable |
| Education_Num | multimodal 63% | spike 39% | bell 28% | Varies |
| Capital_Gain | descending 80% | heavy_tail 77% | spike 59% | Stable |
| Capital_Loss | descending 81% | heavy_tail 76% | spike 58% | Varies |
| Hours_per_week | spike 97% | heavy_tail 54% | bell 27% | Stable |

Note callout:

**Key insight — spike vs descending vs heavy_tail:** Capital_Gain scores descending 80%, heavy_tail 77%, spike 59% — all three are valid descriptions of zero-inflated data. The generative model doesn't force a single answer; it says "this is mostly descending AND heavy-tailed AND spike-like" which is exactly right for a feature that's 95% zeros with scattered outliers. This is far more honest than the softmax model's single "spike" verdict.

## Observations for Pipeline Validation

### Shape Diversity

Despite having only 6 numeric features, the shapes are highly varied:

- **Near-uniform with tail decay:** Age — working population bulge from 20-50, then smooth decline
- **Right-skewed:** fnlwgt — classic right-tail, most values clustered low
- **Extreme zero-inflation + outlier spike:** Capital_Gain — 95% at 0, scattered mid-range, spike at 99999 (likely a cap/code)
- **Zero-inflation + secondary distribution:** Capital_Loss — 95% zero, separate small cluster around 1500-2200
- **Single-spike dominant:** Hours_per_week — massive peak at 40, with shoulders (part-time left, overtime right)
- **Multi-modal discrete:** Education_Num — distinct peaks at 9 (HS-grad), 10 (Some-college), 13 (Bachelors)

### Gap Splitting Candidates (Doc 16)

- **Capital_Gain:** Clear gap between zero cluster and non-zero values. Another gap before the 99999 spike. Multi-level gap split candidate.
- **Capital_Loss:** Gap between zero mass and the secondary cluster (~1500-2200)

### Valley Splitting Candidates (Doc 17)

- **Hours_per_week:** Valley between part-time (<35) and full-time (40) populations? The spike at 40 creates an asymmetric shape that might look bimodal in certain binnings.
- **Education_Num:** Multiple peaks (9, 10, 13) — not bimodal but multi-modal. Tests whether pipeline correctly identifies >2 modes.

### Pipeline Challenges

Note callout:

**Challenge 1 — Extreme zero-inflation:** Capital_Gain and Capital_Loss are >95% zeros. These are essentially binary (has/doesn't have) plus a conditional distribution for those who have it. The pipeline must detect this structural pattern — neither a standard distribution nor a simple gap split.

**Challenge 2 — Spike distributions:** Hours_per_week has ~50% of values at exactly 40. This creates a spike-shape that no standard distribution family matches. The pipeline must handle point-mass + continuous mixture.

**Challenge 3 — Ordinal encoding:** Education_Num is a numeric encoding of a categorical variable. It's technically numeric but the gaps between integers are meaningless — gap splitting should NOT trigger on these structural gaps.

**Challenge 4 — Dominated categoricals:** Country (91% US), Race (85% White), Workclass (70% Private) — heavily imbalanced categoricals where minority classes may not have enough samples for reliable separation testing.

### Comparison with Ames Housing

| Aspect | Ames Housing | Adult Census |
|--------|--------------|--------------|
| Numeric features | 39 (many continuous) | 6 (mostly problematic) |
| Sample size | 2,930 | 32,561 |
| Target | Continuous (SalePrice) | Binary (income >50K) |
| Main shapes | Right-skew, zero-inflated | Zero-inflated, spike, discrete |
| Pipeline focus | Shape classification, splits | Structural patterns, categoricals |

## Regeneration instructions

- **Layout:** single long page in document order: h1 + `.subtitle`, inline `.stat-box` row, h2 sections separated by comment dividers. Sections use: `.target-bar` stacked split bar, scrollable data-snippet table, 2-column `.feature-types` grid of `.type-card` boxes, `.cat-section` CSS bar charts (two 3-column grid rows for categoricals), `.note` callouts, 3-column `.grid` of `.chart-cell` canvas density plots, JS-built tables (`#typeTable`, `#cnnTable`, `#genTable`) populated from data arrays, two summary canvases, and a normal (non-scrolling, `display:table; width:auto`) comparison table. Responsive grid: 2 columns below 900px, 1 below 600px.
- **Page CSS:** body system sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6. h1 2em `#1a5276`; h2 1.5em `#1a5276` with bottom border `1px solid #ddd`; h3 1.2em `#6c3483`; subtitle `#666` 1.1em. Tables `display:block` scrollable, 0.85em, cell borders `1px solid #ddd`, header `#f0f4f8`/`#1a5276` sticky, zebra rows `#f8f9fa`/white. `.stat-box` gray rounded box, label 0.85em `#666`, value 1.4em bold `#1a5276`. `.note` background `#fef9e7`, left border `3px solid #d4ac0d`. `code` background `#f0f4f8`, `#1a5276`. `.cat-bar` 20px, gradient `linear-gradient(90deg, #1a5276, #2980b9)`. `.target-bar` 30px flex bar, white 0.85em bold segment labels. `.chart-cell` background `#f8f9fa`, border `#e0e0e0`, radius 8px.
- **Palette:** #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange; plus greens `#2e7d32`, orange `#e65100`, purple `#8e44ad`, secondary blue `#2980b9`, density fill `rgba(31,111,235,0.15)`, bar fill `rgba(26,82,118,0.3)`.
- **Canvases:** all use `window.devicePixelRatio` scaling (backing store = rendered width × dpr, `ctx.scale` back to logical coordinates, CSS size fixed); density gallery canvases 500×200 logical, summary charts 600×140 and 700×160 with `style="width:100%; height:auto"`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
