# STATSML — Statistical and ML Capabilities

**Page type:** other (single-page long reference doc: TOC box, 13 numbered h2 sections each with a two-column obj-table row — text left 45%, canvas right 55% — plus two full-width comparison tables)
**HTML title tag:** STATSML — Statistical and ML Capabilities

**Subtitle:** Statistical Analysis + ML with Verified Assumptions on Every Feature

## Callout (philosophy box)

**Core Thesis:** Don't assume. Verify preconditions. Match the test to the data shape. Only trust results from valid tests.

## Table of Contents

**Table of Contents** (ordered list of in-page anchor links)

1. Goals (#goals)
2. Single Feature Significance (#single-feature)
3. Multi-Model Candidates (#multi-model)
4. Why Not Decision Trees (#why-not-trees)
5. Right Test for Right Data (#right-test)
6. All ML Models Violate Assumptions (#assumptions)
7. Verify Then Apply (#verify-apply)
8. Data Model (#data-model)
9. Split Inheritance (#split-inheritance)
10. Multi-Candidate Precondition Verification (#multi-candidate)
11. CNN Shape Classifier (#cnn-shape)
12. Semantic Metadata (#semantic-metadata)
13. Discussion Backlog (#discussion-backlog)

## 1. Goals

**Obj-title:** Statistical Analysis + ML with Statistical Foundation

**Goal 1 — Statistical Analysis:** Use statistics to analyze any unknown data by gathering statistics, computing metrics that are helpful to do statistical tests.

**Goal 2 — ML with Statistical Foundation:** Build ML algorithms that work only with attributes that satisfy statistical properties and have significant differences.

- Profile every feature before modeling
- Verify preconditions before applying any test
- Only use features with statistically validated signal

### Visualization (canvas `c1`, 720×280)

Raw-features-to-validated-features transformation diagram.

- **Headers (bold 16px `#222`):** "Raw Features" centered at x=170, "Validated Features" at x=550.
- **Left: three raw histogram rows** (bar width 28, row height 65, starting y=38, max scale 48, each boxed in 0.5px `#ccc` with a red `#e74c3c` bold "?" at row center):
  - Row 1 data `[8,25,12,5,30,2,18,35,10,6]`, fill `rgba(150,150,150,0.5)`.
  - Row 2 data `[3,3,45,3,3,3,3,3,3,3]`, fill `rgba(180,150,130,0.5)`.
  - Row 3 data `[15,20,12,8,22,28,5,10,18,25]`, fill `rgba(130,150,180,0.5)`.
- **Middle:** blue `#2980b9` 3px arrow from (320, h/2) to (395, h/2); `#1a5276` labels stacked at x=355: "Profile" / "Verify" / "Test".
- **Right: three validated histogram rows** (bar width 26, starting x=420, boxed in 1.5px stroke of the result color, label to the right of each box in the result color):
  - `[2,5,12,25,38,42,38,25,12,5]`, fill `rgba(39,174,96,0.35)`, green `#27ae60`, label "bell, t-test p<0.001".
  - `[3,3,45,3,3,3,3,3,3,3]`, fill `rgba(231,76,60,0.2)`, red `#e74c3c`, label "spike — excluded (n<30)".
  - `[35,28,20,14,10,7,5,3,2,1]`, fill `rgba(39,174,96,0.35)`, green `#27ae60`, label "right_skew, MW p<0.01".

## 2. Single Feature Significance

**Obj-title:** Binary Classification via Feature-Level Separation

**Setup:** Data with 100s of numeric columns and a binary target column (pos/neg).

**Goal:** Identify columns where positive class values are significantly different from negative class values.

**Example:** High cholesterol correlating with positive heart disease — one feature alone provides strong classification signal.

- A single feature's distribution difference can be enough signal
- Find ranges where one class dominates
- Validate with the right statistical test

### Visualization (canvas `c2`, 720×280)

Paired histogram: positive vs negative class over 16 bins with highlighted dominance zones.

- **Title (bold 16px `#1a5276`, top center):** "Cholesterol — Pos vs Neg Class Separation".
- **Data (16 bins, max scale 38):**
  - Positive: `[1,2,4,7,12,18,28,35,30,20,12,5,2,1,0,0]`, bars fill `rgba(39,174,96,0.5)` (left half of each bin).
  - Negative: `[0,0,1,2,3,5,8,12,18,25,32,28,18,10,5,2]`, bars fill `rgba(231,76,60,0.5)` (right half of each bin).
- **Highlight zones:** bins 0–7 background `rgba(39,174,96,0.08)`; bins 9–16 background `rgba(231,76,60,0.08)`.
- **Legend (top right):** green square "Positive", red square "Negative".
- **Zone labels (bold 15px, bottom):** green "pos dominates" under the left zone, red "neg dominates" under the right zone.

## 3. Multi-Model Candidates

**Obj-title:** Multiple Candidate Models Per Feature

For each feature, don't commit to a single model. Try multiple reasonable fits and keep all good candidates as parallel lines of reasoning:

- **Multiple models valid simultaneously** — each captures a different "lens"
- **Range-based significance** — signal may exist only in certain ranges
- **Carry through classification** — all good candidates vote at decision time
- **Feature expansion** — each significant range becomes a new derived feature
- **Architecture:** Model Registry, Range Registry, Feature Expansion, Evidence Aggregation

Multiple agreeing models = stronger evidence. Disagreeing models = lower confidence.

### Visualization (canvas `c3`, 720×280)

Histogram with three overlaid model-fit curves.

- **Histogram data (20 bins, max 42):** `[2,5,10,18,28,35,38,32,22,14,8,5,4,6,10,8,5,3,2,1]`, bars fill `rgba(26,82,118,0.2)`.
- **Model curves (2.5px lines connecting bin centers):**
  - "Right-skew", red `#e74c3c`: `[4,10,20,30,36,38,34,26,18,12,7,4,3,2,2,1,1,1,0,0]`.
  - "Mixture (2-comp)", green `#27ae60`: `[3,6,12,20,28,34,37,30,20,12,7,4,4,7,11,9,5,3,2,1]`.
  - "KDE (non-param)", purple `#8e44ad`: `[2,5,10,18,27,34,37,32,22,14,8,5,4,6,10,8,5,3,2,1]`.
- **Legend (top right):** colored line swatch + name per model.
- **Caption (bottom center, `#222`):** "Keep all candidates that fit well — different lenses on same data".

## 4. Why Not Decision Trees

**Obj-title:** Four Fundamental Flaws

1. **No statistical validation of splits:** Info gain picks "best" split but never asks if it's significant given n
2. **Top nodes good, lower nodes fragmented:** First 1-2 splits find real signal; deeper nodes split on noise
3. **No assumption checking:** Same greedy split applied regardless of shape, distribution, or sample size
4. **Overfitting by design:** Without guardrails, trees always find splits that look good on training — even in pure noise

A split on 12 data points is treated the same as a split on 12,000.

### Visualization (canvas `c4`, 720×280)

Split-panel comparison: fragmented decision tree (left) vs validated ranges (right), separated by a dashed vertical divider.

- **Left panel (title bold 16px `#1a5276`: "Decision Tree"):** tree diagram centered at x=w/4.
  - Root box "n=5000" (80×28, fill `#f0f4f8`, stroke `#1a5276`).
  - Level 1: "n=2400" (fill `#d4efdf`, stroke/text `#27ae60`) and "n=2600" (fill `#fef9e7`, stroke/text `#e67e22`).
  - Level 2 (four fragmented leaves, fill `#fdedec`, stroke/text `#e74c3c`): "n=180", "n=12", "n=8", "n=3".
  - Bold 14px red caption: "Noise! No validation!".
- **Right panel (title bold 16px `#1a5276`: "This Approach"):** three range cards (280×42) centered at x=3w/4:
  - "[180-240]  n=1200" — "t-test p<0.001" (fill `#d4efdf`, stroke `#27ae60`).
  - "[240-320]  n=2100" — "MW p<0.00001" (fill `#d4efdf`, stroke `#27ae60`).
  - "[320-400]  n=28" — "n too low — skip" (fill `#fdedec`, stroke `#e74c3c`).
- **Divider:** dashed `#ddd` vertical line at w/2.
- **Caption (bottom center, `#222`):** "Fragmented leaves vs statistically validated ranges".

## 5. Right Test for Right Data

**Obj-title:** The 6-Step Flow

1. Find a candidate range (data mining / exploration)
2. Check: does this range have enough data? (sample sufficiency)
3. Check: what shape is the data in this range? (shape detection)
4. Pick the RIGHT statistical test for that shape (test selection)
5. Run the test: is the pos/neg difference significant? (validation)
6. Only THEN use this range as a feature

A range that looks great on info gain but fails step 2, 3, 4, or 5 gets rejected.

### Visualization (canvas `c5`, 720×280)

Horizontal 6-step flow diagram with rejection arrows.

- **Steps (rounded 90×50 boxes, white bold text, 20px gap, centered row):**
  1. "1. Find\nRange" — `#2980b9`
  2. "2. Enough\nData?" — `#e67e22`
  3. "3. What\nShape?" — `#8e44ad`
  4. "4. Pick\nTest" — `#1a5276`
  5. "5. Run\nTest" — `#27ae60`
  6. "6. Use as\nFeature" — `#27ae60`
- **Arrows:** gray `#555` 2px arrows between consecutive boxes.
- **Rejection arrows:** red `#e74c3c` downward arrows from steps 2, 3, 4, 5; centered red label below: "REJECT (fails precondition)".
- **Caption (bottom center, `#222`):** "Each step is a gate — only ranges that pass all 6 steps become features".

### Comparison table (full-width, comparison-table style)

| Aspect | Decision Trees | This Approach |
|--------|----------------|---------------|
| Split criterion | Info gain (no statistical test) | Statistical significance test |
| Sample check | None (will split on n=5) | Minimum n required per range |
| Distribution check | None | Must identify shape first |
| Test selection | N/A | Matched to actual data shape |
| Deep splits | Fragmented, noisy | Only if statistically validated |
| Model per feature | One greedy path | Multiple candidate models kept |
| Confidence | None reported | Confidence from statistical test |
| Explainability | Path through tree | "Range [x-y], test Z, p<0.01, purity 0.93" |

## 6. All ML Models Violate Assumptions

**Obj-title:** The Universal Problem

**Current practice:** Pick a model, apply to all features, hope it's "robust enough." Tune hyperparameters until validation score looks good.

**The problem:** You have no idea which features actually satisfy the model's requirements and which are just noise being overfitted.

A logistic regression on a bimodal feature is nonsense — but it'll still produce a coefficient.

### Visualization (canvas `c6`, 720×280)

Bar chart: count of routinely violated assumptions per model.

- **Title (bold 16px `#1a5276`):** "Assumptions Routinely Violated (per model)".
- **Data (bar width 60, gap 18, y-axis 0–6 with gridlines):** LogReg 4, SVM 3, DTree 3, LinReg 5, NB 2, K-Means 3, PCA 3, NN 3.
- **Bars:** vertical gradient from `#e74c3c` (top) to `rgba(231,76,60,0.4)` (bottom), 1px `#c0392b` stroke; white bold count inside top of each bar; `#1a5276` model name below each bar.
- **Caption (bottom center, `#222`):** "Every model has assumptions. Nobody checks them."

### Comparison table (full-width, comparison-table style)

| Model | Assumes | Reality |
|-------|---------|---------|
| Logistic Regression | Features ~normal, linear with log-odds | Skewed, multimodal, spikes |
| SVM | Features scaled [0,1], margin geometry meaningful | Different scales, outliers destroy margin |
| Decision Trees | Splits meaningful at any depth | Noisy leaves, no significance check |
| Linear Regression | Linearity, normal residuals, homoscedasticity | Non-linear, heavy-tailed errors |
| Naive Bayes | Features independent | Features correlated everywhere |
| K-Means | Spherical clusters, equal variance | Elongated, unequal, overlapping |
| PCA | Linear relationships, variance = importance | Non-linear structure, noise dominates |
| Neural Networks | Sufficient data, smooth manifold | Small data, discontinuities, imbalance |

## 7. Verify Then Apply

**Obj-title:** Inverted Workflow

**Standard workflow:**

1. Pick model
2. Feed ALL features in
3. Hope model handles violations
4. Tune until numbers look good
5. No idea why it works or when it breaks

**This approach:**

1. Profile each feature: type, shape, ranges, sample size
2. For each range: which preconditions hold?
3. Apply ONLY the test whose preconditions are satisfied
4. Normal range + enough n + significant separation = use with full confidence

Every piece of evidence is statistically justified.

### Visualization (canvas `c7`, 720×280)

Two horizontal flow paths separated by a dashed divider labeled "vs".

- **Top path (heading bold 16px `#1a5276`: "Standard Workflow"):** five boxes (95×32, fill `rgba(231,76,60,0.15)`, stroke/text `#e74c3c`, red arrows between): "Pick Model" → "Feed All" → "Hope" → "Tune" → "???".
- **Divider:** dashed `#ddd` horizontal line at mid-height with gray "vs" label.
- **Bottom path (heading bold 16px `#1a5276`: "This Approach"):** five rounded boxes (100×32, white bold text, green arrows between): "Profile" (`#2980b9`) → "Check Shape" (`#8e44ad`) → "Match Test" (`#1a5276`) → "Validate" (`#27ae60`) → "Use" (`#27ae60`); green `#27ae60` checkmark (✓) under every step after the first.
- **Caption (bottom center, `#222`):** "Every step produces verifiable evidence".

## 8. Data Model

**Obj-title:** Feature Ontology

The system needs a rich data model capturing:

- **Distribution Catalog:** normal, right_skew, exponential, mixtures, KDE, spike+continuous
- **Test Registry:** each test with preconditions explicitly encoded
- **Precondition Chain:** checks that must pass before a test is valid
- **Conditional Distributions:** feature X stratified by feature Y
- **Hierarchical Features:** parent-child, groups, derived features
- **Correlation Map:** redundancy, interactions, category-driven splits
- **Feature Ontology:** full description = raw_name + type + shape + ranges + conditionals + models + significance

### Visualization (canvas `c8`, 720×280)

Feature-ontology card mockup.

- **Card:** full-canvas card (inset 40,20) with `#f8fafb` fill, 2px `#1a5276` stroke, and a `#1a5276` header band with white bold 16px monospace text "Feature: body_weight_kg".
- **Fields (monospace key/value rows, bold 12px `#1a5276` keys, 14px `#333` values, 28px pitch):**
  - type: continuous
  - global_shape: right_skewed
  - range: [2.1, 280.0]
  - conditional_on: gender: M=normal(82,15), F=normal(65,12)
  - candidate_models: mixture_2 (fit=0.91), right_skew (fit=0.78)
  - significant_ranges: [95-120] pos p=0.002 | [45-60|F] neg p=0.001
  - interactions: blood_pressure (joint_sep=0.85)
- **Mini histogram (top right of card, 12 bars × 12px, height 80, fill `rgba(26,82,118,0.35)`):** data `[15,22,18,12,8,6,5,4,3,2,2,1]`, labeled "right_skew" below.

## 9. Split Inheritance

**Obj-title:** Inheriting Parent Knowledge

After splitting, don't re-profile from scratch. Start from what the parent tells us:

- **Predict:** Parent model predicts what child distribution should look like
- **Check:** Does actual child data match prediction?
- **Match = inherit:** Keep parent's models, ranges, shape, test selection
- **Mismatch = investigate:** Split revealed new conditional structure

**Why it matters:** Efficiency (skip expensive re-profiling), context preservation (know WHY child looks this way), discovery (mismatches reveal hidden structure).

### Visualization (canvas `c9`, 720×280)

Parent histogram splitting into two child histograms.

- **Parent (top center, boxed 1.5px `#1a5276`, title bold 16px):** "Parent: normal(82, 15), n=5000"; data `[2,5,12,22,35,42,38,25,14,6,3]` (bar width 22, max 45), fill `rgba(26,82,118,0.35)`.
- **Split arrows:** blue `#2980b9` 2px lines to each child, labeled "split on age < 40" at center.
- **Child A (bottom left, boxed `#27ae60`):** data `[1,4,10,20,32,38,34,22,12,5,2]`, fill `rgba(39,174,96,0.35)`; bold 15px green caption "Child A: matches prediction" and "INHERIT parent model".
- **Child B (bottom right, boxed `#e74c3c`):** bimodal data `[8,18,25,15,5,3,5,15,25,18,8]`, fill `rgba(231,76,60,0.35)`; bold 15px red caption "Child B: MISMATCH!" and "RE-PROFILE (new structure)".
- **Caption (bottom center, `#222`):** "Mismatch = discovery: the split revealed conditional structure".

## 10. Multi-Candidate Precondition Verification

**Obj-title:** Beyond Single Thresholds

**Problem:** A threshold of 0.3 means 0.29 passes and 0.31 fails — arbitrary boundary. Formal tests (Shapiro-Wilk) are too sensitive.

**Solution:** Run precondition checks with multiple parameter combinations:

- Strict: tight symmetry, low tail tolerance
- Moderate: reasonable thresholds across all checks
- Lenient + CLT: looser shape, relies on large n
- Shape-specific: focuses on the detected non-normality type

**Validation:** Re-run on different samples. Only interpretations stable across samples are trusted.

A t-test backed by 3/4 stable interpretations = high trust. Only the most lenient survived = low trust.

### Visualization (canvas `c10`, 720×280)

Histogram with four vertical threshold lines representing different precondition interpretations.

- **Title (bold 16px `#1a5276`):** "Multiple Precondition Interpretations".
- **Histogram data (16 bins, max 45):** `[3,6,12,20,30,38,42,38,30,20,12,6,3,2,1,1]`, fill `rgba(26,82,118,0.25)`.
- **Threshold lines (vertical, 2px, positioned as fraction of chart width, labeled with ✓ stable / ✗ unstable):**
  - Strict at 0.20, red `#e74c3c`, solid — ✗.
  - Moderate at 0.30, orange `#e67e22`, dash [6,3] — ✓.
  - Lenient at 0.35, green `#27ae60`, dash [3,2] — ✓.
  - Shape-specific at 0.55, purple `#8e44ad`, dash [8,4] — ✓.
- **Bottom labels:** bold 15px green "3/4 stable across samples = HIGH TRUST"; red "Strict failed on resample — too tight for this data".

## 11. CNN Shape Classifier

**Obj-title:** CNN: 11 Shape Classes

**Architecture:** Shared 1D convolutional backbone with dual heads:

- Discriminative: softmax over classes
- Generative: independent sigmoid per class (multi-label)

**Results:**

- Discriminative: 93.1% top-1, 99.3% top-2
- Generative: 92.6% top-1, 99.4% top-2
- FP rates: multimodal 0%, u_shaped 0.1%
- Agreement between models: 97.7%
- Calibration: 90-100% conf band = 95.2% actual

**Key decisions:** Independent per-class scores (sum > 100%), top-2 reporting, variance/SE bands.

### Visualization (canvas `c11`, 720×280)

Bar chart: per-class top-1 accuracy for 11 shape classes with an average line.

- **Title (bold 16px `#1a5276`):** "CNN: Per-Class Top-1 Accuracy".
- **Data (bar width 46, gap 12, y-axis 85–100% with 5% gridlines, rotated −45° class labels):** bell 96.2, right_skew 94.8, left_skew 93.1, uniform 95.5, bimodal 91.8, multimodal 89.4, u_shaped 92.0, spike 97.3, heavy_tail 88.5, plateau 90.2, j_shaped 94.0.
- **Bar colors:** ≥93.1% fill `rgba(39,174,96,0.6)` stroke `#27ae60`; below fill `rgba(230,126,34,0.6)` stroke `#e67e22`. Value labels above each bar in `#1a5276`.
- **Average line:** dashed red `#e74c3c` horizontal line at 93.1 labeled "93.1% avg".

## 12. Semantic Metadata

**Obj-title:** Three Layers + Payoff Matrix

**Layer 0 — Fact Sheet:** Mechanical properties (col_name, type_scores, shape_scores, range). Zero interpretation.

**Layer 1 — Assessment:** LLM-generated hypotheses tagged with epistemic source:

- `fact` (trust=1.0): directly from data
- `derived` (trust=0.7): traceable inference
- `guess` (trust=0.3): hypothesis to test

**Layer 2 — Lineage:** Tracks transformations. Assessments get promoted or demoted as pipeline progresses.

**Payoff:** Final strength = statistical signal x semantic relevance.

### Visualization (canvas `c12`, 720×280)

2×2 payoff quadrant.

- **Title (bold 16px `#1a5276`):** "Payoff Matrix: Statistical Signal x Semantic Relevance".
- **Axes:** 2px `#1a5276` cross centered slightly below middle; x-axis label "Statistical Signal →", rotated y-axis label "Semantic Relevance →"; corner scale labels "Weak"/"Strong" (x) and "Low"/"High" (y).
- **Quadrants (260×100 each, tinted background, bold 17px label + sub-caption):**
  - Top-right: REPORT — "Strong stat + Strong semantic", green `#27ae60`, bg `rgba(39,174,96,0.1)`.
  - Top-left: INVESTIGATE — "Weak stat + Strong semantic", orange `#e67e22`, bg `rgba(230,126,34,0.1)`.
  - Bottom-right: SUSPICIOUS — "Strong stat + No semantic", red `#e74c3c`, bg `rgba(231,76,60,0.1)`.
  - Bottom-left: IGNORE — "Weak stat + No semantic", gray `#555`, bg `rgba(150,150,150,0.08)`.

## 13. Discussion Backlog

**Obj-title:** Selected Future Directions

- **1-level conditioning:** Split data by categorical variable to reveal hidden subpopulations (e.g., weight by gender = two clean bells from one noisy mountain)
- **Type-aware bucket boundaries:** int_num must split at integer boundaries; flt_num can use fractional
- **Archetype library:** Pre-built assessment templates for common column types (age, income, identifier, date)
- **Confidence decay with depth:** Assessments grow stale after multiple transformations — decay model needed

### Visualization (canvas `c13`, 720×280)

Bimodal histogram splitting by gender into two clean bells.

- **Title (bold 16px `#1a5276`):** "1-Level Conditioning: Reveal Hidden Subpopulations".
- **Left: combined bimodal histogram** (bar width 18, max 34, centered at x=140), data `[5,12,22,30,25,10,4,3,4,10,25,30,22,12,5]`, fill `rgba(26,82,118,0.35)`; labels below: "Weight (all)" / "bimodal".
- **Middle:** blue `#2980b9` 2.5px arrow from (220, h/2) to (305, h/2), labeled "split by" / "gender".
- **Right (stacked at x=480, bar width 14):**
  - Top bell "Male: bell(82, 15)": data `[2,5,12,25,38,42,38,25,12,5,2]`, fill `rgba(41,128,185,0.4)`, blue `#2980b9` label.
  - Bottom bell "Female: bell(65, 12)": data `[2,6,14,28,40,44,40,28,14,6,2]`, fill `rgba(231,76,60,0.4)`, red `#e74c3c` label.
- **Checkmarks:** bold 17px green `#27ae60` "✓ t-test valid" beside each bell.

## Regeneration instructions

- **Template/layout:** TOC-reference style long doc (see `docs/statsml/ui-templates/03-toc-reference`). Order: h1, `.subtitle`, `.philosophy` callout, `.toc` box with an ordered list of in-page anchor links, then 13 `h2` sections (each with `id` anchor and numbered title). Each section holds one `.obj-table` (full-width table, one row: first `td` 45% text with `.obj-title` heading, second `td` 55% centered canvas). Sections 5 and 6 are each followed by a full-width `.comparison-table`.
- **Page CSS:** body -apple-system/BlinkMacSystemFont/'Segoe UI' sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6. h1 1.8em `#1a5276`. h2 1.4em `#1a5276` with 2px `#2980b9` bottom border, margins 40px 0 15px. `.subtitle` `#666` 1.05em. `strong` `#1a5276`. `code` background `#e8f0f8`, `#1a5276`, 2px 6px padding, 3px radius. `.philosophy`: background `#f0f4f8`, left border `4px solid #2980b9`, padding 16px 20px. `.toc`: background `#f8fafb`, `1px solid #e0e0e0`, padding 20px 30px, 4px radius; links `#2980b9`, underline on hover. `.obj-table` td: `1px solid #e0e0e0` border, 20px 24px padding, middle-aligned; even rows `#fafcfe`; `.obj-title` 1.05em weight 600 `#1a5276`. `.comparison-table` 0.88em: th background `#1a5276` white 10px 14px; td `1px solid #e0e0e0` 8px 14px; even rows `#f8fafb`, hover `#eef4fa`.
- **Canvas:** all 13 canvases intrinsic 720×280, `display: block; margin: 0 auto`; scaled by `window.devicePixelRatio` via a shared `setupCanvas(id)` helper that multiplies the backing store, and calls `ctx.scale` so drawing stays in logical coordinates.
- **Palette:** `#1a5276` primary blue, `#2980b9` secondary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange, `#8e44ad` purple, bar fill `rgba(26,82,118,0.35)`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
