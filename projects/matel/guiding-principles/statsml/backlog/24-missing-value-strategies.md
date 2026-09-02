# Missing Value Strategies by Scenario

**Page type:** detail page (backlog kusto-style 2-col layout: text left 45%, canvas right 55%, one `.card-section` per numbered section)
**HTML title tag:** Missing Value Strategies by Scenario

**Status badge:** TO DISCUSS

**Subtitle:** How the pipeline should handle missing values depends on *why* they are missing, *what kind* of feature it is, and *what downstream task* is intended — three questions that must be answered before any fill is chosen.

## 1. Why It Is Missing — Mechanism and Feature Type

The mechanism decides whether any fill is *legitimate*; the feature type only decides *how* to fill once that is settled.

- **MCAR** — missingness independent of observed and unobserved values. Dropping or simple imputation is unbiased; the only cost is sample size.
- **MAR** — missingness explained by observed columns. Needs conditional imputation (regression, KNN, model-based) that uses those columns.
- **MNAR** — missingness depends on the unobserved value itself. Imputation biases estimates, and no fill recovers what was never recorded.

**Feature type sets the fill mechanics:**

- **Continuous** — mean / median / KNN / regression, chosen by distribution shape (skew or outliers → median).
- **Categorical** — mode, an explicit "Unknown" level, or model-based prediction.
- **Binary** — ask first whether the absence itself is the informative bit (missingness-as-feature).
- **Ordinal** — interpolation between neighbouring levels vs treating "missing" as its own level.

**Key point (red-accent callout):** **Testability limit:** MCAR vs MAR is checkable — regress the missingness indicator on the observed columns and see if it is predictable. MAR vs MNAR is *not* identifiable from the data alone; it needs external knowledge or an explicit assumption. Any "MNAR detector" is really a domain rule.

**Open question (orange-accent callout):** **Open:** Which heuristics run automatically to classify the mechanism, and how is the untestable MAR/MNAR boundary declared? Should the imputation strategy be bound to the type detected by the feature-type classifier, or stay overridable per column?

### Visualization (canvas `c1`, 720×380)

Three side-by-side panels illustrating MCAR / MAR / MNAR with mini age/income tables.

- **Title (bold 14px, `#1a5276`, centered, y=25):** "Missingness Mechanism: What Drives the Gap".
- **Three panels** each (w−60)/3 wide starting x=30, outlined rect (44 to 344 tall) stroked in the panel color:
  1. **MCAR** (green `#27ae60`) — driver caption: "Rows dropped by a flaky sensor"; income column `[41k, ?, 52k, ?, 60k, 58k]`; note: "no column explains NA"; test line (green): "detectable from data"; verdict box: "Drop rows or simple" / "fill — no bias".
  2. **MAR** (orange `#e67e22`) — driver: "Older respondents skip income"; income `[41k, 38k, 52k, ?, ?, ?]` with age rows 3–5 outlined orange; note: "age predicts NA"; test line (green): "detectable from data"; verdict: "Condition on the" / "observed columns".
  3. **MNAR** (red `#e74c3c`) — driver: "Top earners refuse to answer"; income `[?, 36k, ?, 48k, ?, 39k]` with gray "(high)" ghost text beside each NA; note: "the hidden value predicts NA"; test line (red): "NOT detectable from data"; verdict: "Any fill injects bias —" / "needs domain input".
- **Mini table per panel:** two columns headed "age" / "income" (10px `#888`), 6 rows of 44×21 cells; age values `[24, 31, 45, 62, 71, 68]` in `rgba(26,82,118,0.12)` cells; missing income cells filled `rgba(231,76,60,0.18)`, stroked `#e74c3c`, text "NA" in `#e74c3c`.
- **Verdict boxes:** color-tinted rect (panel color at ~8% alpha) with panel-color border, bold 10px panel-color two-line text.
- **Caption (11px `#888`, bottom center):** "Classify the mechanism first — it decides whether any imputation is legitimate".

## 2. Patterns, and What the Downstream Task Tolerates

The geometry of missingness across rows and columns constrains the strategy as much as the mechanism does.

- **Sparse random** — scattered cells, low share; simple imputation is usually adequate.
- **Block** — whole columns or rows absent for a subpopulation; filling the block invents a group that was never measured.
- **Monotone** — dropout: later variables progressively more missing, typical of panel and longitudinal data.
- **Structured** — missing because *not applicable*, e.g. `spouse_income` for single people. Domain semantics, not a quality defect.

**The intended task changes the acceptable answer:**

- **Descriptive stats** — complete-case analysis is defensible under MCAR, misleading otherwise.
- **Hypothesis testing** — missingness drains power and can shift p-values in either direction.
- **ML prediction** — tree-based learners route NA natively; linear and distance-based models cannot.
- **Causal inference** — MNAR gaps manufacture associations that do not exist.

**Key point (red-accent callout):** **The silent cost:** an imputed cell carries no information, yet every downstream estimator treats it as measured. Standard errors shrink, intervals narrow, and significance is overstated — single imputation understates uncertainty by construction.

**Open question (orange-accent callout):** **Open:** Structured missingness must be separated from broken data before anything else runs. Can applicability rules be inferred from co-occurrence with other columns, or does this require a declared schema? And should the recommended strategy differ by the planned test or model?

### Visualization (canvas `c2`, 720×380)

Four missingness-pattern matrices (8 columns × 13 rows of 12px cells each).

- **Title (bold 14px, `#1a5276`, centered, y=25):** "Missingness Patterns Across Rows and Columns".
- **Legend (top center):** swatch `rgba(26,82,118,0.35)` = "observed cell"; swatch `rgba(231,76,60,0.35)` = "missing cell".
- **Four panels**, each (w−40)/4 wide from x=20, panel name bold 11px in panel color, "columns →" hint above each grid:
  1. **Sparse random** (`#27ae60`) — missing where `(r·7+c·13) % 19 == 0` or `(r·5+c·3) % 23 == 0` (scattered); caption "scattered cells, nothing" / "aligns across rows"; verdict "Simple fill adequate".
  2. **Block** (`#e74c3c`) — missing block rows 5–9 × cols 4–7; caption "a subpopulation was" / "never measured"; verdict "Fill invents a group".
  3. **Monotone** (`#e67e22`) — missing where `r >= rows − max(0, c−1)·1.6` (staircase toward bottom-right); caption "dropout — later columns" / "progressively worse"; verdict "Sequential methods".
  4. **Structured** (`#1a5276`) — missing where `c >= 6` and `r % 3 != 0` (right columns mostly missing); caption "field not applicable to" / "part of the population"; verdict "Semantics, not a defect".
- **Caption (11px `#888`, bottom center):** "Same missing share, four different problems — the pattern picks the strategy".

## 3. Drop vs Impute — What Each Strategy Costs

Every option trades injected bias against destroyed signal. None is free.

- **Drop rows** — no distortion of retained values, but discards observed cells too and biases the sample unless MCAR.
- **Drop column** — removes the feature and, with it, any signal carried by its missingness.
- **Mean fill** — preserves the mean, shrinks the variance, plants an artificial mode at the mean.
- **Median fill** — robust under skew, same variance collapse.
- **Model-based / KNN** — respects conditional structure under MAR; risks target leakage and overconfident fills.
- **Indicator flag + fill** — keeps the missingness signal at the cost of one extra column per feature.

**Key point (red-accent callout):** **No threshold rule:** "drop if under 5%" is a magic number that ignores the two things that matter — mechanism and pattern. One percent MNAR in a dominant driver is worse than thirty percent MCAR in a weak feature. Whether the remaining N supports the analysis is a power question, answered from effect size and test, not from a percentage.

**Open question (orange-accent callout):** **Open:** Should the pipeline ever refuse to proceed — very high missingness, or a domain rule flagging MNAR — or always proceed and attach a provenance warning to every affected estimate?

### Visualization (canvas `c3`, 720×300)

Grouped bar chart: bias injected vs signal lost per strategy.

- **Title (bold 14px, `#1a5276`, centered, y=25):** "Strategy Cost: Bias Injected vs Signal Lost".
- **Data (six two-line-labelled groups, values 0–100):**
  | Strategy | Bias | Loss |
  |---|---|---|
  | Drop rows | 60 | 55 |
  | Drop column | 25 | 90 |
  | Mean fill | 55 | 45 |
  | Median fill | 40 | 45 |
  | Model / KNN fill | 30 | 20 |
  | Flag + fill | 30 | 10 |
- **Axes:** plot area x=70, width w−110, y=58, height 170; gridlines every 25 with labels 0/25/50/75/100; rotated y-axis label "Relative cost" (`#666`).
- **Bars:** per group, left bar fill `rgba(231,76,60,0.5)` (bias) with red `#e74c3c` value label, right bar fill `rgba(26,82,118,0.5)` (loss) with blue `#1a5276` value label; bar width 28% of group slot.
- **Legend (top right of plot):** "Bias injected" (red swatch), "Signal lost" (blue swatch).
- **Caption (11px `#888`, bottom center):** "Illustrative ordering — the real ranking depends on mechanism, pattern and missing share".

## 4. What Single Imputation Does to the Distribution

Filling with a constant does not merely restore rows — it reshapes the column.

- **Variance collapses** — with a share *f* missing, the standard deviation shrinks by roughly √(1−*f*) because every filled row sits exactly at the centre.
- **An artificial spike** appears at the fill value and can become the mode, corrupting shape detection.
- **Correlations dilute** toward zero, since the filled cells vary with nothing.
- **Uncertainty is lost** — standard errors are computed as if the filled cells were observed.

**Multiple imputation** draws several plausible values per gap, fits the analysis on each, then pools with a variance inflation term for the imputation uncertainty. It matters when the quantity of interest is a variance, a standard error, or a p-value. It matters far less when the goal is a point prediction from a tree ensemble, which can absorb the fill artefact through splits.

**Key point (red-accent callout):** **Shape detection interaction:** the pipeline's own histogram/shape profiling must run on observed values only, or on flagged-imputed data — otherwise a fill spike is read as genuine multimodality.

*Example: a feature with a fifth of values mean-filled shows a sharp central peak no real process produced — the gap/valley detector reports bimodality that is pure artefact.*

### Visualization (canvas `c4`, 720×300)

Two-panel histogram comparison: observed distribution vs after mean fill.

- **Title (bold 14px, `#1a5276`, centered, y=25):** "Mean Fill: Variance Collapse and an Artificial Spike".
- **Shared data:** observed bins `[2, 4, 8, 14, 21, 24, 21, 14, 8, 4, 2]` (11 bins, 22px wide, baseline y=232, shared scale max = 24 + 30 = 54 over 150px height).
- **Left panel (x₀=30):** title "Observed values only"; bars fill `rgba(26,82,118,0.35)`; sub-caption bold 11px green `#27ae60`: "sd = 1.00, true shape"; axis hint "feature value →" (`#999`).
- **Right panel (x₀=390):** title "After mean fill (a fifth missing)"; same blue bars plus a red spike of 30 stacked on the centre bin (index 5), fill `rgba(231,76,60,0.55)` stroked `#e74c3c`, annotated in red 10px: "all filled rows" / "land here"; sub-caption bold 11px red `#e74c3c`: "sd = 0.89, false mode".
- **Divider:** vertical `#e0e0e0` line at w/2.
- **Caption (11px `#888`, bottom center):** "sd shrinks by √(1 − missing share); correlations dilute toward zero".

## 5. Missingness Is Itself a Signal

Absence often tracks the outcome more strongly than the value would have. Filling erases exactly that.

- **Add the flag** — `is_missing_<col>` when absence is plausibly informative, then fill the original however you like; the flag preserves what the fill destroys.
- **Domain convention decides meaning** — a missing lab result usually means "not ordered", which is not the same as "normal".
- **Structured absence encodes group membership** — no spouse income means unmarried; there the flag *is* the real feature.
- **Under MCAR the flag is noise** — it has no association with the target by definition and only adds dimensionality.

**Key point (red-accent callout):** **Reuse, don't re-derive:** this is the same object as value-existence mapping (02). Per-column existence is already computed there; the missing-value strategy should consume that map rather than recompute presence patterns independently.

**Open question (orange-accent callout):** **Open:** Should flags be generated for every column above some missing share, or only where the flag shows association with the target on training folds — and how is that check kept free of leakage?

### Visualization (canvas `c5`, 720×300)

Grouped bar chart: outcome rate when a field is present vs absent, per field.

- **Title (bold 14px, `#1a5276`, centered, y=25):** "Absence as a Predictor: Outcome Rate by Presence".
- **Data (outcome rate %, scale max 25%):**
  | Field | Present | Absent | Verdict label |
  |---|---|---|---|
  | income | 7% | 22% | flag it |
  | phone_verified | 5% | 16% | flag it |
  | lab_result | 4% | 12% | flag it |
  | employer_ref | 6% | 19% | flag it |
  | signup_channel | 9% | 9% | MCAR: no flag |
- **Axes:** plot area x=80, width w−120, y=58, height 170; gridlines every 5% labeled 0%–25%; rotated y-axis label "Outcome rate" (`#666`).
- **Bars:** left bar per group fill `rgba(26,82,118,0.5)` (present) with `#1a5276` % label; right bar fill `rgba(231,76,60,0.5)` (absent) with `#e74c3c` % label; field name below in `#2c3e50`; verdict below that in bold 9px — orange `#e67e22` for "flag it", gray `#999` for "MCAR: no flag".
- **Legend:** "value present" (blue swatch), "value missing" (red swatch).
- **Caption (11px `#888`, bottom center):** "Illustrative rates — where the gap is real, an is_missing flag keeps what the fill erases".

## Regeneration instructions

- **Layout:** backlog detail page. Body → h1 → `.status` badge ("TO DISCUSS") → `.subtitle` → one `.card-section` per numbered section, each an `<h2>` plus a `table.layout` with one `<tr>`: left `td.text-col` (45%) for paragraphs/bullets/`.key-point`/`.questions`/`.example`, right `td.viz-col` (55%) for the canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`. `.subtitle` `#666` 0.95rem. `.status` inline-block pill: background `#e8f0f8`, color `#1a5276`, padding 3px 10px, radius 12px, 0.8rem bold. h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border. `.key-point` background `#f8f9fa`, `border-left: 3px solid #e74c3c`. `.questions` background `#fdf6ef`, `border-left: 3px solid #e67e22`. `.example` italic `#555` 0.9rem. `strong` in `#1a5276`; `code` background `#e8f0f8`, color `#1a5276`, radius 3px. Canvas: `width: 100%`, border `1px solid #e0e0e0`, radius 4px. No nav bar, no back/home links, no index number in h1.
- **Palette:** primary blue `#1a5276`, accent blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`.
- **Canvas:** intrinsic width/height attributes as given per chart (c1/c2 are 720×380, c3/c4/c5 720×300); a `setup(id)` helper (and inline equivalents for the 380-tall canvases) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- Regenerated HTML has no card links (detail page); any links elsewhere use `.html` extensions.
