# Marginal vs Joint Distributions — The Data Cost of Dimensions

**Page type:** detail page (backlog-style two-column layout: text left 50%, canvas right 50%, one `.lang-section` per topic; h1 carries a BACKLOG status pill)
**HTML title tag:** Marginal vs Joint Distributions — The Data Cost of Dimensions

**Subtitle:** One column is cheap to study. Every column you add multiplies the number of groups you have to fill, so the same dataset splits into cells that each need their own sample.

**Intro callout:** Correlating height against age needs one histogram and a few hundred rows. Add gender, region, education, smoking status, and income band, and the question is no longer "how does height vary with age" but "how does it vary within each of 2,560 combinations." The row count does not grow to match — it gets divided. Some cells stay solid, most turn to noise, and hundreds hold no rows at all. All figures below are an **Illustrative Example** built on a 10,000-row survey.

## 1. One Column Is Cheap

Studying height against a single column is the easy case, and it sets the baseline everything else is measured against.

- **The running example** — a 10,000-row survey records height plus six descriptive columns.
- **The first question** — how does height vary with age band, ignoring every other column.
- **Eight age bands** — 10,000 rows split eight ways leaves about 1,250 rows per band.
- **That is a marginal** — height summed over everyone else, collapsing the other five columns.
- **Precision is comfortable** — at n=1,250 and sd=8cm, the 95% interval is ±0.44cm.
- **A 2cm age trend is obvious** — the effect is more than four interval widths wide.
- **Why it feels easy** — a marginal spends the whole dataset on one question.

**Key point:** A marginal is cheap precisely because it throws information away — every row counts toward exactly one of eight buckets.

### Visualization (canvas `c1`, 720×320)

Grouped bar chart: mean height by age band, with 95% error bars, on a single-column split.

- **Title (bold 16px, `#1a5276`, top center):** "One Column: Mean Height by Age Band (n≈1,250 each)".
- **Data:** eight age bands `20-29, 30-39, 40-49, 50-59, 60-69, 70-79, 80-89, 90+` with mean heights `[171.4, 171.0, 170.6, 170.2, 169.4, 168.6, 167.6, 166.4]` cm; each n=1250.
- **Error bars:** computed at render time as `1.96 * 8 / Math.sqrt(1250)` = ±0.44cm — drawn as a vertical whisker with 6px caps in `#1a5276`, never hardcoded.
- **Bars:** fill `rgba(26,82,118,0.35)`, stroke `#1a5276` width 1, bar width 52, 8px gap. Padding: top 58, bottom 62, left 62, right 30.
- **Y-axis:** 165 to 173 cm with gridlines `#e5e9ef` and labels at 166/168/170/172 (12px `#666`); rotated label "mean height (cm)" 13px `#2c3e50`.
- **X-axis labels:** age bands, 11px `#2c3e50`, rotated -35°.
- **Annotation (bold 13px `#27ae60`, top right):** "error bars ±0.44cm — the 5cm trend is unmistakable".
- **Caption (12px `#444`, bottom):** "Illustrative Example — sd assumed 8cm".

## 2. Cells Multiply, Rows Do Not

The count of combinations is a product of level counts, so it grows multiplicatively while the dataset stays the same size.

- **The arithmetic** — cells = product of the levels: 8 × 2 × 4 × 4 × 2 × 5 = 2,560.
- **Adding gender doubles it** — 8 cells become 16, and average rows per cell halve to 625.
- **Adding region ×4** — 64 cells, about 156 rows each; still workable for a mean.
- **Adding education ×4** — 256 cells, about 39 rows each; intervals widen to ±2.51cm.
- **Adding smoker ×2** — 512 cells, about 20 rows each, ±3.51cm — as wide as the effect itself.
- **Adding income ×5** — 2,560 cells, about 4 rows each; a cell mean means almost nothing.
- **The asymmetry** — each column costs a *multiple* of the data, and you only ever added one column.

**Key point:** Feature count grows additively and cell count grows multiplicatively, so the rows available per question fall geometrically as you describe your population more finely.

### Visualization (canvas `c2`, 720×340)

Two-series chart on a log y-axis: cells (rising) against average rows per cell (falling), crossing over as columns are added.

- **Title (bold 16px, `#1a5276`, top center):** "Adding Columns: Cells Rise, Rows per Cell Fall (log scale)".
- **X-axis categories (bold 12px `#2c3e50`, rotated -30°):** `age (8)`, `+gender (2)`, `+region (4)`, `+education (4)`, `+smoker (2)`, `+income (5)`.
- **Cells series (orange `#d95926`, filled circles r=5 joined by a 2px line):** `[8, 16, 64, 256, 512, 2560]`, value labels above each point (bold 12px orange).
- **Rows-per-cell series (blue `#2a78d6`, filled squares 8px joined by a 2px line):** computed at render time as `10000 / cells` → `[1250, 625, 156.3, 39.1, 19.5, 3.9]`, value labels below each point (bold 12px blue) rounded to one decimal below 100.
- **Y-axis:** log10 scale from 1 to 4000, gridlines `#e5e9ef`, labels "1", "10", "100", "1,000" (12px `#666`).
- **Crossover marker:** at the `+region` → `+education` span, a vertical dashed `#bdc3c7` line where the two series cross, labeled bold 12px `#d55181` "the lines cross — more cells than rows to fill them".
- **Legend (top left, 12px):** orange circle "number of cells", blue square "average rows per cell".
- **Caption (12px `#444`, bottom):** "fixed dataset of 10,000 rows — Illustrative Example".

## 3. Each Cell Carries Its Own Distribution

A cell is not a smaller copy of the whole. Its mean, spread, and even its shape can differ, which is the entire reason the split was worth doing.

- **Different centers** — men 30-39 average about 178cm; women 60-69 about 158cm.
- **Different spreads** — one cell may run sd 6cm and another sd 9cm, so equal n buys unequal precision.
- **Different shapes** — a cell mixing two unpooled subgroups can be bimodal, not bell-shaped.
- **The marginal is their weighted average** — 0.25 × (178 + 176 + 165 + 163) = 170.5cm.
- **A worked check** — male 178/176 averages 177, female 165/163 averages 164, and (177+164)/2 = 170.5.
- **Why the split matters** — no single number describes a population whose cells genuinely differ.
- **Why it is expensive** — every distinct distribution needs its own sample to estimate, not a share of one.

**Key point:** If cells shared one distribution you would not need to split at all — the split is worth doing exactly when it is also expensive.

### Visualization (canvas `c3`, 720×340)

Four overlaid normal curves for four gender × region cells, with the marginal curve drawn behind them.

- **Title (bold 16px, `#1a5276`, top center):** "Four Cells, Four Distributions — and the Marginal Behind Them".
- **Data (hardcoded, sd in cm):** male-urban mean 178 sd 7, male-rural mean 176 sd 7, female-urban mean 165 sd 6, female-rural mean 163 sd 6; each cell n=2,500.
- **Marginal curve (gray `#95a5a6`, 2px, filled `rgba(149,165,166,0.18)`):** computed at render time as the equal-weight mixture of the four cell curves — visibly bimodal, not a bell.
- **Cell curves:** male-urban `#1a5276` solid 2px; male-rural `#2980b9` dashed (5/3) 2px; female-urban `#e74c3c` solid 2px; female-rural `#e67e22` dashed (5/3) 2px. Each drawn over x=145..200cm.
- **X-axis:** 145 to 200 cm, ticks every 10cm, labels 12px `#2c3e50`; caption "height (cm)" 13px.
- **Marginal mean marker:** vertical dashed `#d55181` line at the render-time weighted mean 170.5cm, labeled bold 13px `#d55181` above: "marginal mean 170.5cm — nobody's cell mean".
- **Legend (right side, 12px, one line per curve with its mean):** "male · urban — 178", "male · rural — 176", "female · urban — 165", "female · rural — 163", "marginal (mixture)".
- **Caption (12px `#444`, bottom):** "Illustrative Example — equal cell sizes, so the marginal is a plain average".

## 4. Precision Collapses Unevenly

Real marginals are not uniform, so the split does not divide the data evenly. A few cells stay well populated while the tail empties out.

- **Uneven marginals** — regions run 40/30/20/10 and income bands 30/25/20/15/10 percent.
- **The largest cell** — the product of every common level: about 27 rows out of the 10,000.
- **The smallest cell** — the product of every rare level: an expected 0.125 rows.
- **A 216-fold span** — so cell precision varies about 14.7-fold across one table.
- **Most cells are unusable** — 76% of the 2,560 cells expect fewer than 5 rows.
- **Hundreds are empty** — at these rates about 546 cells expect no rows at all.
- **The trap** — one table mixes trustworthy estimates and pure noise with no visual difference between them.

**Key point:** Sparsity is not spread evenly, so a per-cell report silently interleaves solid estimates with numbers drawn from two or three rows.

### Visualization (canvas `c4`, 720×340)

Histogram of expected rows per cell across all 2,560 cells, with the usable region shaded off from the noise region.

- **Title (bold 16px, `#1a5276`, top center):** "Expected Rows per Cell, All 2,560 Cells".
- **Data:** cell counts computed at render time as the outer product of the marginals `age [1/8 ×8]`, `gender [0.5, 0.5]`, `region [0.40, 0.30, 0.20, 0.10]`, `education [0.45, 0.30, 0.15, 0.10]`, `smoker [0.80, 0.20]`, `income [0.30, 0.25, 0.20, 0.15, 0.10]`, each multiplied by 10,000. Every printed statistic is derived from this array, never hardcoded.
- **Bins (bold-labeled, computed counts):** `0–0.5` → 224, `0.5–1` → 384, `1–2` → 560, `2–5` → 784, `5–10` → 368, `10–20` → 192, `20–30` → 48. Bin totals must sum to 2,560 — verify at render time.
- **Bars:** bins below 5 rows filled `rgba(231,76,60,0.35)` stroke `#e74c3c`; bins at 5 rows and above filled `rgba(39,174,96,0.35)` stroke `#27ae60`. Padding: top 58, bottom 66, left 66, right 34.
- **Axes:** L-shaped `#2c3e50`; y-axis "number of cells" rotated 13px with gridlines `#e5e9ef`; x labels are the bin ranges (11px `#2c3e50`).
- **Divider:** vertical dashed `#e74c3c` line at the 5-row boundary, labeled bold 12px `#e74c3c` "below 5 rows: 1,952 cells (76%)" — percentage computed at render time.
- **Annotation (bold 13px `#1a5276`, top right, two lines):** "largest cell: 27 rows" / "smallest cell: 0.125 rows — a 216× span".
- **Caption (12px `#444`, bottom):** "Illustrative Example — expected counts from the assumed marginals".

## 5. Thousands of Cells Means Thousands of Tests

Once the table has thousands of cells, scanning it for interesting ones becomes a multiple-testing problem, and the noisiest cells are the ones that look most interesting.

- **The scan** — testing each cell's mean against the overall mean is 2,560 hypothesis tests.
- **The false-positive count** — at α=0.05, about 128 cells clear the bar with no real effect.
- **Small cells produce extremes** — a 3-row cell has a ±9.1cm interval, so it wanders far by luck.
- **Selection makes it worse** — reporting only the most extreme cells guarantees the noisiest ones.
- **The correction is brutal** — Bonferroni at 2,560 tests needs |z| > 4.27, not 1.96.
- **Small cells cannot clear it** — a 4-row cell would need a 17cm gap to reach that threshold.
- **The honest reading** — a deep table can only ever confirm effects in the cells that stayed large.

**Key point:** The number of cells sets the multiple-testing burden, so splitting deeper buys both less data per test and a stricter threshold for each one.

### Visualization (canvas `c5`, 720×340)

Two-panel chart: expected false positives by table depth, next to the Bonferroni threshold each depth demands.

- **Title (bold 16px, `#1a5276`, top center):** "Deeper Tables: More False Positives, Higher Bar".
- **Left panel (x=60..380), bars, expected false positives at α=0.05:** cells `[8, 16, 64, 256, 512, 2560]`; heights computed at render time as `0.05 × cells` → `[0.4, 0.8, 3.2, 12.8, 25.6, 128]` on a log10 y-axis from 0.1 to 200. Fill `rgba(231,76,60,0.35)` stroke `#e74c3c`; value labels bold 11px `#e74c3c` above bars. Panel heading bold 13px: "cells that look real but are not".
- **Right panel (x=430..690), line, Bonferroni critical |z|:** computed at render time from `α = 0.05 / cells` via a two-sided normal quantile → `[2.73, 2.96, 3.36, 3.73, 3.90, 4.27]`; drawn as a 2px `#4a3aa7` line with r=4 dots, value labels bold 11px violet. Y-axis 2.0 to 4.6, gridlines `#e5e9ef`. A horizontal dashed `#95a5a6` line at 1.96 labeled 11px "uncorrected 1.96". Panel heading bold 13px: "threshold each cell must clear".
- **Shared x labels (11px `#2c3e50`, rotated -30°, both panels):** `8`, `16`, `64`, `256`, `512`, `2560` under a 12px `#444` caption "number of cells".
- **Annotation (bold 12px `#d55181`, bottom center):** "at 2,560 cells: 128 spurious hits, and a 4-row cell needs a 17cm gap to be believed".
- **Caption (12px `#444`, bottom right):** "Illustrative Example — sd 8cm, two-sided".

## 6. What Actually Works Instead

The response is not to collect exponentially more data. It is to stop treating every cell as an independent estimation problem.

- **The brute-force price** — detecting a 2cm gap at 80% power needs 251 rows per cell.
- **That is 643,000 rows** — 64× the dataset, for a table you probably do not need at full depth.
- **Additive main effects first** — model each column's contribution separately, add interactions only where evidence supports one.
- **Partial pooling** — a hierarchical model shrinks small-cell estimates toward the marginal, in proportion to how thin they are.
- **Collapse levels** — merging income into three bands instead of five cuts the table from 2,560 cells to 1,536.
- **Set a minimum-n floor** — suppress or flag any cell below a pre-declared row count instead of printing it.
- **Pre-declare the depth** — fix which columns you will condition on before looking, so the table is not chosen by its extremes.
- **Report the interval, not the point** — a wide interval communicates thinness that a bare cell mean hides.

**Key point:** The exponential cost is a property of estimating every cell independently — pooling, collapsing, and additive structure buy back most of the resolution at a fraction of the data.

### Visualization (canvas `c6`, 720×340)

Comparison chart: cell-mean estimates against partially-pooled estimates for a set of cells of very different sizes.

- **Title (bold 16px, `#1a5276`, top center):** "Raw Cell Means vs Partially Pooled, by Cell Size".
- **Data (hardcoded, seven cells spanning the size range):** row counts `[3, 4, 8, 19, 39, 156, 625]`; raw cell means `[182.4, 159.1, 176.8, 164.2, 173.5, 168.9, 170.9]` cm; grand mean 170.5cm.
- **Pooled series computed at render time** — shrink each raw mean toward 170.5 with weight `n / (n + 40)`, giving `[171.3, 169.5, 171.6, 168.5, 172.0, 169.2, 170.9]` rounded to one decimal; the shrinkage constant 40 is stated in the caption as an assumption, not a recommendation.
- **Layout:** one horizontal row per cell (7 rows, y from 78 spaced 34px), x-axis 155 to 190 cm at the bottom, gridlines `#e5e9ef`.
- **Raw marks:** open circles r=6, 2px `#e74c3c` outline. **Pooled marks:** filled diamonds 9px `#27ae60`. A thin `#bdc3c7` arrow from each raw mark to its pooled mark.
- **Grand-mean line:** vertical 2px `#1a5276` line at 170.5, labeled bold 12px "grand mean 170.5".
- **Row labels (left, 11px `#2c3e50`):** "n = 3", "n = 4", "n = 8", "n = 19", "n = 39", "n = 156", "n = 625".
- **Annotation (bold 12px `#27ae60`, right side, two lines):** "thin cells get pulled to the grand mean;" / "the n=625 cell barely moves".
- **Caption (12px `#444`, bottom):** "Illustrative Example — shrinkage weight n/(n+40), constant chosen for illustration".

## Regeneration instructions

- **Template/layout:** backlog detail page (kusto-style two-column). Structure: `<h1>` with a `<span class="status">BACKLOG</span>` pill, `.subtitle` paragraph, `.intro` callout, then one `.lang-section` per numbered section, each containing an `<h2>` and a `table.layout` with a single `<tr>`: left `<td class="text-col">` (50%) holds the lead sentence, bullets, and `.key-point`; right `<td class="viz-col">` (50%) holds the canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px solid `#2980b9` bottom border; h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border. `.subtitle` `#666` 0.95rem. `.intro` background `#f0f4f8`, left border 3px solid `#2980b9`, padding 8px 12px, 0.9rem. `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem. `ul` 0.92rem with 4px li spacing. `.status` pill background `#fef9e7`, border 1px `#f39c12`, color `#b7950b`, 2px 10px padding, radius 4px, 0.8rem. Canvas `width: 100%`, `height: auto`, border `1px solid #e0e0e0`, radius 4px. No nav bar, no back/home links.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange; chart accents `#2a78d6` blue, `#d95926` orange, `#d55181` magenta, `#4a3aa7` violet, `#95a5a6` gray, `#e5e9ef` gridlines, bar fill `rgba(26,82,118,0.35)`.
- **Canvas:** intrinsic 720×320 (c1) and 720×340 (c2–c6); scale via `window.devicePixelRatio` in a shared `setupCanvas(id)` helper that reads the element's declared width/height, sets the backing store to rendered width × dpr, caps display at the logical width via `style.maxWidth`, and calls `ctx.scale` back to logical coordinates. Draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Data integrity:** no `Math.random()` anywhere — every series is either a hardcoded literal array or computed at render time from the stated marginals. The cell-count array in c4, the false-positive counts and Bonferroni thresholds in c5, the mixture curve and weighted mean in c3, the error bars in c1, the rows-per-cell series in c2, and the pooled means in c6 are all **derived in JS at draw time**, so every printed figure follows from the plotted data. Bin totals in c4 sum to 2,560.
- **Normal quantile helper:** c5 needs a two-sided normal inverse CDF for the Bonferroni thresholds — implement it as a small rational approximation (Acklam or equivalent) rather than a lookup table, so the values follow from the cell counts.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
