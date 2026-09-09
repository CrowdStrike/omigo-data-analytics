# Feature Data Model

**Page type:** detail page (TOC box, then one h2 + two-column obj-table row per section: text left 45%, canvas right 55%, closing philosophy callout)
**HTML title tag:** Feature Data Model — Statistical ML

**Subtitle:** Why we need a rich data model — and what it captures at each stage of the pipeline.

## Table of Contents

**Table of Contents** (a `.toc` box with an ordered list of in-page anchor links)

1. The Feature Pipeline (#pipeline)
2. Feature Profile (Core Object) (#profile)
3. Candidate Models (#candidates)
4. Expanded Features (#expanded)
5. Conditional Distributions (#conditional)
6. Test Registry & Selection (#tests)
7. Interactions & Correlations (#interactions)
8. Classification = Reasoning Trace (#classification)
9. Complete Feature Lifecycle (#lifecycle)

## The Feature Pipeline

**1. Raw → Profiled → Modeled → Validated → Expanded**

Every feature passes through 5 stages. Each stage enriches the data model with new information:

- **Raw:** Just numbers in a column. No knowledge.
- **Profiled:** Type (continuous/categorical/discrete), shape (bell/skewed/bimodal), ranges, gaps, missing patterns.
- **Modeled:** Multiple candidate fits (right_skew, mixture, KDE). Each with fit quality and preconditions met.
- **Validated:** Significant ranges identified. Tests applied with verified preconditions. p-values, purity, enrichment.
- **Expanded:** Each significant range becomes a binary feature. Fully traceable to source model and test.

The data model is the single source of truth. No ML model runs without consulting it.

### Visualization (canvas `c1`, 720×280)

Horizontal flowchart of five solid colored rounded boxes (110×60, 22px gaps, centered vertically, radius 8, white text: bold 17px label + 17px sub-label) joined by gray arrows (`#999`, width 3):

- "Raw / numbers" `#bdc3c7` → "Profiled / type, shape" `#3498db` → "Modeled / candidates" `#8e44ad` → "Validated / p-values" `#e67e22` → "Expanded / binary features" `#27ae60`.
- **Caption (bottom center, `#555`, 17px):** "Each stage enriches the data model — nothing is lost".

## Feature Profile

**2. The Core Object — Everything Known About One Feature**

- **Identity:** name, source path (e.g., `patient.labs.cholesterol`), type
- **Raw stats:** n, missing count, min/max, mean/median, std, skewness, kurtosis, percentiles
- **Value map:** populated regions (dense core, sparse tails), gaps, point masses, outlier islands
- **Shape:** global shape (right_skewed), regional shapes (normal in [180-270], exponential in [270-603])
- **Candidate models:** list of fits that passed quality threshold
- **Conditional profiles:** how shape changes when stratified by another feature
- **Expanded features:** the derived binary features produced from significant ranges

**Example:** Cholesterol — continuous, n=12505, range [89-603], right-skewed globally, but approximately normal in [180-270]. Mixture model (2 components, fit 0.89) captures the bimodal structure.

### Visualization (canvas `c2`, 720×280)

Profile-card mockup: a bordered panel styled like a record.

- **Panel:** fill `#f8fafb`, stroke `#1a5276` (width 3), inset 30px/15px; header bar filled `#1a5276` (32px tall) with white bold 17px monospace title "cholesterol_mg_dl".
- **Fields (monospace 17px `#333`, left-aligned at x=50, 30px line spacing from y=65):**
  - "type: continuous | n: 12505 | missing: 342"
  - "range: [89, 603] | skew: 0.82 | kurt: 1.34"
  - "global_shape: right_skewed"
  - "regions: [89-180] sparse, [180-270] dense, [270-603] tail"
  - "candidates: mixture_2(0.89)"
  - "significant: [270-603] pos p<0.00001, [240-320] pos p<1e-8"

## Candidate Models

**3. Multiple Fits Per Feature — Keep All Good Candidates**

Each candidate model captures:

- **Model type:** e.g., mixture(2 components, means=[198, 268])
- **Fit quality:** KS statistic, AIC/BIC, composite fit score (0-1)
- **Valid range:** where this model applies
- **Preconditions met:** what was verified before fitting (positive values, no zeros, sufficient n)
- **Significant ranges:** within this model, which ranges show class separation? Each with: purity, dominant class, enrichment ratio, test used, p-value, preconditions verified

**Key:** Different models find different ranges. Right-skew finds [270-603] via Mann-Whitney. Mixture finds [240-320] via t-test. Both are valid — different lenses on same data.

### Visualization (canvas `c3`, 720×280)

Histogram with two overlaid model curves.

- **Histogram (20 bars, data `[2,5,10,18,28,35,38,32,22,14,8,5,4,6,10,8,5,3,2,1]`, scale max 40, fill `rgba(26,82,118,0.2)`, plot from x=50, bar width (w-100)/20, baseline h-50, height range h-100).**
- **Curves (width 3, through bar centers):**
  - Right-skew in `#e74c3c`: `[4,10,20,32,36,34,28,20,14,10,7,5,3,2,2,1,1,1,0,0]`.
  - Mixture in `#27ae60`: `[3,6,12,20,28,34,37,30,20,12,7,4,4,7,11,9,5,3,2,1]`.
- **Legend (17px, left-aligned at x=w-240):** "Right-skew (fit=0.84)" in `#e74c3c` (y=30), "Mixture-2 (fit=0.89)" in `#27ae60` (y=52).
- **Caption (bottom center, `#555`, 17px):** "Different models find different significant ranges".

## Expanded Features

**4. One Raw Feature → Many Binary Features**

Each significant range from each candidate model becomes an expanded feature:

- `cholesterol__right_skew__270_603__pos` — fires when value in [270,603], predicts pos, purity 0.76, Mann-Whitney p<0.00001
- `cholesterol__mixture2__240_320__pos` — fires when value in [240,320], predicts pos, purity 0.70, t-test p<0.00000001
- `cholesterol__mixture2__89_160__neg` — fires when value in [89,160], predicts neg, purity 0.92, proportion z-test p<0.0000001

Overlapping ranges from different models are BOTH kept. If a value hits both, that's stronger evidence (two independent models agree).

**One raw feature → 5 expanded features** (3 unconditional + 2 conditional on gender).

### Visualization (canvas `c4`, 720×280)

Stacked feature-row list.

- **Title (bold 17px `#1a5276`, top center):** "1 raw feature → 5 expanded binary features".
- **Rows (full-width minus 80px, 36px tall, 42px spacing from y=50; fill = row color at alpha 0.12, stroke = row color width 1.5; feature name in monospace 17px `#333` left-aligned; purity + test bold 17px in row color right-aligned):**
  - `chol__lognorm__270_603__pos` — "76% | MW" — `#27ae60`.
  - `chol__mix2__240_320__pos` — "70% | t-test" — `#27ae60`.
  - `chol__mix2__89_160__neg` — "92% | z-test" — `#e74c3c`.
  - `chol__norm__280_603__pos|M` — "83% | t-test" — `#2980b9`.
  - `chol__lognorm__260_603__pos|F` — "79% | MW" — `#8e44ad`.

## Conditional Distributions

**5. Feature Behavior Changes by Subpopulation**

Cholesterol looks different for men vs women. Weight looks different by age group. The data model captures this:

- **Trigger:** KS test between groups shows significantly different distributions
- **Per stratum:** own shape, own candidate models, own significant ranges, own valid tests
- **Males:** cholesterol is normal in [180-270] → t-test valid. Purity at [280-603] = 0.83
- **Females:** cholesterol is still skewed → Mann-Whitney needed. Purity at [260-603] = 0.79

Without stratification: forced to use Mann-Whitney globally (loses power). With stratification: use t-test where valid, Mann-Whitney where needed. Result: stronger signal, correctly validated.

### Visualization (canvas `c5`, 720×280)

Side-by-side male/female histograms divided by a light vertical rule (`#ddd`).

- **Left panel — Males:** 13 bars, data `[1,3,8,16,28,38,42,38,28,16,8,3,1]`, scale max 44, fill `rgba(41,128,185,0.4)`; title (bold 17px `#2980b9`, top center): "Males: normal → t-test valid"; note below (17px `#555`): "purity 0.83 at [280-603]".
- **Right panel — Females:** data `[1,2,5,12,22,30,28,20,14,10,8,6,4]`, fill `rgba(142,68,173,0.4)`; title (bold 17px `#8e44ad`): "Females: skewed → Mann-Whitney"; note: "purity 0.79 at [260-603]".

## Test Registry & Selection

**6. Every Test Has Preconditions — Selection Is Automatic**

The test registry encodes preconditions explicitly:

- **t-test:** needs normality (Shapiro p>0.05) + equal variance (Levene p>0.05) + n≥30
- **Welch's t:** needs normality + n≥30 (relaxes equal variance)
- **Mann-Whitney:** needs ordinal/continuous + n≥20 per group
- **Chi-squared:** needs categorical + all expected cells ≥5
- **Fisher's exact:** categorical + small n (fallback when chi-sq fails)
- **Proportion z:** needs np≥10 and n(1-p)≥10
- **Permutation:** universal fallback — only needs n≥15

**Selection logic:** categorical → chi-sq/Fisher. Continuous + normal → t-test/Welch. Continuous + not normal → Mann-Whitney. Nothing else works → permutation test.

### Visualization (canvas `c6`, 720×280)

Decision-arrow diagram: five rows, each a left condition box with an arrow to a right colored test box.

- **Rows (46px spacing from y=20):** left box at x=40, 250×36, fill `#f0f4f8`, stroke `#ddd`, 17px `#1a5276` centered label; right box at x=440, 220×36, filled with row color, white bold 17px label; gray connecting arrow (`#aaa`).
  - "Categorical?" → "Chi-sq / Fisher", `#8e44ad`.
  - "Normal + equal var?" → "t-test", `#27ae60`.
  - "Normal + unequal var?" → "Welch's t", `#2980b9`.
  - "Not normal?" → "Mann-Whitney", `#e67e22`.
  - "Nothing else works?" → "Permutation", `#e74c3c`.

## Interactions & Correlations

**7. Features Don't Exist in Isolation**

- **Correlations:** cholesterol ↔ LDL (r=0.89) → redundant signal, keep the stronger one
- **Categorical splits:** gender splits cholesterol into two clean subpopulations (KS p=0.00001). Signal improves +0.15 within strata.
- **Pair interactions:** cholesterol + BP together (joint signal=0.89) is much stronger than either alone (0.67 and 0.58). Both high = very strong pos evidence.
- **Feature groups:** lipid panel (cholesterol, HDL, LDL, triglycerides) are mathematically related. Don't treat as independent evidence.

### Visualization (canvas `c7`, 720×280)

Three labeled interaction-type boxes.

- **Title (bold 17px `#1a5276`, top center):** "Feature Interactions".
- **Boxes (200×150 at y=45, 20px gaps, centered as group; fill = color at alpha 0.1, stroke = color width 3; bold 17px colored heading, 17px `#333` two-line detail, bold 17px colored action at the bottom):**
  - "Redundant" — "chol ↔ LDL" / "r=0.89" — action "Keep stronger only" — `#e74c3c`.
  - "Conditional" — "chol | gender" / "KS p<0.00001" — action "Stratify" — `#2980b9`.
  - "Joint Signal" — "chol + BP" / "0.67+0.58→0.89" — action "Combine" — `#27ae60`.
- **Caption (bottom center, `#555`, 17px):** "Don't count the same signal twice. Do combine complementary signals."

## Classification = Reasoning Trace

**8. Not Just a Label — Full Evidence Chain**

For a patient with cholesterol=285, male, age=58:

- Hits `cholesterol__right_skew__270_603__pos` (purity 0.76, MW p=0.00001)
- Hits `cholesterol__mixture2__240_320__pos` (purity 0.70, t-test p=1e-8)
- Hits `cholesterol__normal__280_603__pos__IF_gender_M` (purity 0.83, t-test p=0.00003)
- Hits `bp__mixture__140_plus__pos` (purity 0.72, MW p=0.0002)

**Result:** 4 features × 6 expanded hits × all agreeing pos → **predict pos, confidence 0.91**

Every piece of evidence is traceable to: source model → range → test → verified preconditions. Fully auditable.

### Visualization (canvas `c8`, 720×280)

Monospace reasoning-trace text block (17px SF Mono, left-aligned at x=30, 38px spacing from y=35):

- "Input: cholesterol=285, male, age=58, BP=148" — bold, `#1a5276`.
- "→ chol__lognorm__270_603__pos: purity=0.76, MW p=0.00001" — `#27ae60`.
- "→ chol__mix2__240_320__pos: purity=0.70, t-test p=1e-8" — `#27ae60`.
- "→ chol__norm__280_603__pos|M: purity=0.83, t-test p=0.00003" — `#27ae60`.
- "→ bp__mix__140+__pos: purity=0.72, MW p=0.0002" — `#27ae60`.
- "Verdict: POSITIVE (confidence 0.91) — 4 features, 6 hits, all agree" — bold, `#1a5276`.

**Caption (bottom center, `#555`, 17px):** "Every prediction = auditable evidence chain".

## Complete Feature Lifecycle

**9. End-to-End: Cholesterol from Raw to Classification**

1. **Ingest:** nested JSON → extract `patient.labs.cholesterol`
2. **Type:** unique_ratio=0.068 → continuous. Range [89-603].
3. **Shape:** globally right-skewed. Regional: normal in [180-270], exponential decay in [270-603].
4. **Models:** right_skew (0.84) ✓, mixture-2 (0.89) ✓, normal (0.62) ✗
5. **Ranges:** [270-603] pos via MW; [240-320] pos via t-test; [89-160] neg via z-test
6. **Conditional:** stratify by gender → males normal → t-test valid; females skewed → MW
7. **Expand:** 1 raw feature → 5 validated binary features
8. **Classify:** patient hits 3 expanded features → all pos → confidence 0.91

### Visualization (canvas `c9`, 720×280)

Eight-step numbered circle timeline.

- **Steps (72px slots, 12px gaps, centered as group; numbered circle radius 18 at mid-height, white bold 17px number; bold 17px `#1a5276` label below; 17px `#555` detail below that; thin `#ccc` connector lines between circles):**
  - 1 "Ingest" / "JSON extract", 2 "Type" / "continuous", 3 "Shape" / "right_skew" — circles `#3498db`.
  - 4 "Models" / "2 candidates", 5 "Ranges" / "3 significant", 6 "Stratify" / "by gender" — circles `#8e44ad`.
  - 7 "Expand" / "5 features", 8 "Classify" / "conf=0.91" — circles `#27ae60`.
- **Caption (bold 17px `#1a5276`, bottom center):** "Cholesterol: from nested JSON → auditable classification evidence".

## Callout (philosophy box)

**The Principle:** The data model is not documentation — it's the operating system. Every decision (which test, which range, which model) is answered by consulting this structure. Nothing is assumed. Everything is verified, recorded, and traceable.

## Regeneration instructions

- **Layout:** single page: h1, `.subtitle`, a `.toc` box (background `#f8fafb`, border `1px solid #e0e0e0`, padding 20px 30px, radius 4px, bold "Table of Contents" heading + `<ol>` of anchor links in `#2980b9`), then one h2 (with `id` anchor) per section, each followed by its own single-row `.obj-table`: left `<td>` (45%) holds `.obj-title` + bullets/paragraphs (section 9 uses an inline-styled `<ol>` instead of `<ul>`), right `<td>` (55%, centered) holds the canvas. Ends with a `.philosophy` callout.
- **Page CSS:** body system sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 1.05em; `code` background `#e8f0f8`, `#1a5276`, radius 3px; table cell borders `1px solid #e0e0e0`, padding 20px 24px, even rows `#fafcfe`; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` `#1a5276`; `.philosophy` background `#f0f4f8`, left border `4px solid #2980b9`, padding 16px 20px, 1em. No nav bar, no back/home links.
- **Canvases:** all 720×280 intrinsic; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper.
- **Palette:** primary blue `#1a5276`, accent blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, light blue `#3498db`, gray `#bdc3c7`, bar fill `rgba(26,82,118,0.35)` (this page uses 0.2 alpha for the section-3 histogram).
