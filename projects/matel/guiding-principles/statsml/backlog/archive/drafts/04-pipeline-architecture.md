# Architecture: Pipeline Overview

**Page type:** detail page (TOC box, then one h2 + two-column obj-table row per section: text left 45%, canvas right 55%, closing philosophy callout)
**HTML title tag:** Architecture: Pipeline Overview

**Subtitle:** One raw feature → type classification → shape classification → splitting → model fitting → range testing → evidence aggregation

## Table of Contents

**Table of Contents** (a `.toc` box with an ordered list of in-page anchor links)

1. High-Level Pipeline Flow (#pipeline)
2. Feature Type Classification (#typegate)
3. Shape Classification (CNN) (#shape)
4. Gap Splitting (#gapsplit)
5. Valley Splitting (#valleysplit)
6. Multiple Candidate Models (#models)
7. Range-Based Significance (#ranges)
8. Feature Expansion (#expansion)
9. Classification: Evidence Aggregation (#classification)

## 1. High-Level Pipeline Flow

**Raw Features → Classification Evidence**

- **Step 1:** Classify feature type (numerical / binary / categorical)
- **Step 2:** Classify shape via CNN (11 classes)
- **Step 3:** Split if needed (gaps → valleys)
- **Step 4:** Fit multiple candidate models per segment
- **Step 5:** Find significant ranges within each model
- **Step 6:** Expand into binary features
- **Step 7:** Aggregate evidence at classification time

Each step enriches the data model. Nothing is assumed — everything verified.

### Visualization (canvas `c1`, 720×280)

Horizontal flowchart of seven solid colored rounded boxes (85×40, 10px gaps, centered, radius 6, white bold 17px labels) joined by gray arrows (`#999`):

- "Type Gate" `#7f8c8d` → "Shape CNN" `#f39c12` → "Gap Split" `#e74c3c` → "Valley Split" `#8e44ad` → "Model Fit" `#2980b9` → "Range Test" `#27ae60` → "Expand" `#e67e22`.
- **Above the chain (bold 17px `#1a5276`, centered):** "Raw Feature" with a "↓" below it.
- **Below the chain:** "↓" then "Expanded Binary Features → Evidence → Classification".

## 2. Feature Type Classification

**Multi-Score Gate — Independent Type Scores**

- Each column scored independently as numerical (0-100%), binary (0-100%), categorical (0-100%)
- Scores are NOT mutually exclusive — "Overall Qual (1-10)" can be 75% numerical AND 45% categorical
- Routing: high numerical → shape CNN → splitting → models. High binary → proportion test. High categorical → chi-squared.
- Ambiguous features (ordinal) run BOTH paths — never force a single wrong route

**Example:** Zip codes parse as numbers but are 100% categorical. Multi-score catches this.

### Visualization (canvas `c2`, 720×280)

Score-bar routing diagram.

- **Input box:** "Raw Column" (100×50 at x=30 y=100, fill `#f0f4f8`, stroke `#7f8c8d`, 17px label) with gray arrow to the score bars.
- **Score bars (at x=180, 200×24, 40px row spacing from y=70; background `#f8f9fa`, stroke `#ddd`, filled portion = value×2 px):**
  - "numerical: 85%" fill `#27ae60`.
  - "binary: 10%" fill `#1abc9c`.
  - "categorical: 15%" fill `#8e44ad`.
  - Labels right of each bar at x=390 in `#333` 17px.
- **Route:** green arrow (`#27ae60`) at the numerical row pointing right to bold green text "→ Shape CNN" with gray note "(highest score routes)" below.
- **Caption (bottom center, `#888`, 17px):** "Scores are independent — one feature can be 75% num AND 45% cat".

## 3. Shape Classification (CNN)

**CNN on Histogram Silhouettes → 11 Classes**

- 64×64 grayscale histogram image → CNN → soft probabilities across all 11 shape classes
- Classes: bell, right_skew, left_skew, bimodal, multimodal, heavy_tail, spike, uniform, u_shaped, ascending, descending
- Multi-candidate: top-2 shapes give alternative interpretations when confidence is low
- Shape determines pipeline routing — bell goes direct to testing, bimodal gets split first

**Why CNN over statistical tests?** Tests give binary yes/no. CNN gives shape identity with confidence.

### Visualization (canvas `c3`, 720×280)

Histogram → CNN → probabilities flow.

- **Mini histogram (x=40, y=50, 150×120 area, background `#f8f9fa`):** 13 bars, data `[3,8,15,28,42,55,60,52,38,25,14,6,2]`, scale max 60, fill `rgba(26,82,118,0.4)`; label below (17px `#333`, centered): "64×64 image".
- **CNN box:** orange arrow (`#f39c12`) to a box at x=250 (110×70, fill `#fef9e7`, stroke `#f39c12`) with bold 17px three-line label "CNN" / "11-class" / "softmax"; then another orange arrow right.
- **Top probabilities (monospace 17px, left-aligned at x=430, 30px spacing from y=55):** "bell  82%" (bold `#1a5276`), "right_skew  9%" (`#999`), "bimodal  4%" (`#999`).
- **Caption (bottom center, `#555`, 17px):** "Shape → Pipeline Route: bell/skew=direct, bimodal=split first, spike=separate".

## 4. Gap Splitting

**Empty Bins → Separated Populations**

- Contiguous empty histogram bins indicate true gaps between sub-populations
- Variance validation: if 3σ of either side can fill the gap, it's not real — skip
- Split point placed proportional to left/right variance
- Recurse once per side, maximum 4 segments total

**Example:** "2nd Flr SF" — spike at 0, gap, then continuous distribution. Gap split → binary (has_2nd_floor) + continuous segment.

### Visualization (canvas `c4`, 720×280)

Histogram with a highlighted gap and a split line.

- **Histogram (20 bins, plot area x=80 y=40, 550×140, scale max 45, fill `rgba(26,82,118,0.35)`, zero-height bins skipped):** `[5,12,25,38,45,42,30,15,4,0,0,0,0,3,10,22,35,28,12,4]`.
- **Gap highlight:** bins 9-12 shaded `rgba(231,76,60,0.12)` with dashed red (`#e74c3c`, width 2, dash [5,3]) rectangle outline; bold red 17px label "GAP" above it.
- **Split line:** vertical dashed green (`#27ae60`, width 2, dash [6,3]) line at bin 10.5, labeled "split" below the plot in 17px green.
- **Segment labels (bold 17px, bottom):** "Segment 1" in `#2980b9` under bins ~4, "Segment 2" in `#8e44ad` under bins ~16.

## 5. Valley Splitting

**Density Dip Between Two Peaks → Split**

- Valley = lowest bin between two peaks
- Must drop ≥25% from BOTH peaks (not just one — a shoulder doesn't count)
- Adaptive threshold: 30% for n<100, 25% for n=100-200, 20% for n>200
- Minimum 30 samples on each side after split

**Purpose:** A t-test on bimodal data is meaningless. After splitting, each component can be tested independently.

### Visualization (canvas `c5`, 720×280)

Bimodal histogram with valley bins highlighted and peak/valley markers.

- **Histogram (20 bins, plot area x=80 y=40, 550×140, scale max 42):** `[2,6,15,30,42,38,25,15,10,8,7,9,14,22,35,40,32,18,8,3]`; bins 8-11 (valley) filled `rgba(142,68,173,0.4)`, all others `rgba(26,82,118,0.35)`.
- **Peak markers:** dashed blue (`#2980b9`, width 1.5, dash [3,2]) vertical lines at bins 4.5 and 15.5, labeled "Peak 1" and "Peak 2" above in 17px blue.
- **Valley marker:** dashed purple (`#8e44ad`, width 2, dash [5,3]) vertical line at bin 10.
- **Caption (bold 17px `#8e44ad`, bottom center):** "Valley (split if ≥25% drop from both peaks)".

## 6. Multiple Candidate Models

**Keep All Good Fits — Don't Force One Winner**

- Same data can be described by multiple mathematical models
- Each model is a different lens — highlights different ranges and patterns
- Keep any model above a quality threshold; discard only if redundant
- Different models may find different significant ranges → richer signal

**Key:** The exact model family matters less than what significant ranges it finds.

### Visualization (canvas `c6`, 720×280)

Fan-out diagram: feature → three model boxes → significant ranges.

- **Feature box:** "Feature" (100×50 at x=30 y=100, fill `#e8f8f5`, stroke `#1abc9c`, bold 17px) with three gray arrows fanning right.
- **Model boxes (150×36 at x=195, 17px labels):** "Model A (fit 0.91)" fill `#fef9e7` stroke `#f39c12` (y=55); "Model B (fit 0.87)" fill `#fdedec` stroke `#e74c3c` (y=107); "Model C (fit 0.82)" fill `#f4ecf7` stroke `#8e44ad` (y=159).
- **Range outputs (green `#27ae60` 17px text at x=415, with gray arrows from the model boxes):** "→ Range [60-90] sig", "→ Range [18-25] sig" (both from Model A), "→ Range [65-80] sig" (Model B), "→ Range [70-75] sig" (Model C).
- **Caption (bottom center, `#555`, 17px):** "Different models find different significant ranges — keep all of them".

## 7. Range-Based Significance

**Local Signal > Global Signal**

- A feature can show no global significance (same mean for pos and neg) but within a specific range, one class dominates
- Each model identifies ranges where pos/neg ratio differs significantly from base rate
- Ranges carry: purity score, dominant class, p-value, sample count
- Only ranges with sufficient n AND statistical significance are kept

**Example:** Age has no global signal, but [70-85] is 92% positive — range-based testing finds it.

### Visualization (canvas `c7`, 720×280)

Number line (0 to 100) with two highlighted significant ranges.

- **Number line:** horizontal `#333` line (width 2) at y=130 from x=80 to w-80, 11 ticks, endpoint labels "0" and "100" (17px, centered).
- **Ranges (50px-tall boxes above the line):**
  - [20%-40%] of line: fill `rgba(39,174,96,0.2)`, stroke `#27ae60`, labeled above "purity 0.89 pos" in green.
  - [60%-80%] of line: fill `rgba(231,76,60,0.2)`, stroke `#e74c3c`, labeled above "purity 0.92 neg" in red.
- **Middle label:** "no signal" in `#bbb` 17px at the 50% mark.
- **Caption (bottom center, `#555`, 17px):** "Global test sees no difference. Range-based testing finds local pockets of purity."

## 8. Feature Expansion

**1 Raw Feature → Many Binary Features**

- Each significant range from each model → one binary feature ("is value in this range?")
- Overlapping ranges from different models are both kept — they're independent evidence
- Each expanded feature carries: source model, range bounds, purity, confidence, test used
- Typical: 1 raw feature → 3-5 expanded binary features

**Result:** 50 raw features → 150-250 statistically validated binary features. This is what the classifier sees.

### Visualization (canvas `c8`, 720×280)

Three-stage expansion diagram.

- **Left box:** "5 raw / features" (100×110 at x=30 y=80, fill `#e8f8f5`, stroke `#1abc9c`, bold 17px, two lines).
- **Middle box:** "Pipeline: / type → shape → / split → model → / range test" (150×70 at x=200 y=100, fill `#eaf2f8`, stroke `#2980b9`, 17px, four lines). Gray arrows connect the boxes.
- **Right box:** 250×150 at x=420 y=60, fill `#d4efdf`, stroke `#27ae60`, containing monospace 17px lines: "age_range_60_90   [0|1]", "age_range_18_25   [0|1]", "inc_range_150k+   [0|1]", "chol_range_240+   [0|1]", "bp_range_140+     [0|1]", "... (13-15 total)".
- **Caption (bold 17px `#e74c3c`, bottom center):** "5 raw → 13-15 validated binary features".

## 9. Classification: Evidence Aggregation

**Every Prediction = Auditable Evidence Chain**

- New data point evaluated against all extended features — which ranges does it hit?
- Multiple models agreeing → strong evidence. Models disagreeing → low confidence.
- Cascade: check strongest features first, classify when agreement is sufficient
- Full reasoning trace: "feature X, model A: value 72 in [65-80], purity 0.93 pos"

**Never force a classification without evidence.** If nothing fires strongly, report low confidence.

### Visualization (canvas `c9`, 720×280)

Input → evaluation → hits → verdict diagram.

- **Input box:** "New patient: / age=72 / chol=285" (120×60 at x=30 y=90, fill `#fef9e7`, stroke `#f39c12`, 17px, three lines); gray arrow to next box.
- **Evaluation box:** "Check all / extended / features" (120×60 at x=210 y=90, fill `#eaf2f8`, stroke `#2980b9`); gray arrow right.
- **Hits (green `#27ae60` 17px text at x=395, 25px spacing):** "✓ age_60_90 → pos (0.93)", "✓ chol_240+ → pos (0.70)", "✓ chol_280+ → pos (0.83)", "✗ inc_150k+ → miss".
- **Result:** green arrow down to a box "POSITIVE (conf 0.91)" (180×45, fill `#d4efdf`, stroke `#27ae60`, bold 17px text in `#1e8449`).
- **Caption (bottom center, `#555`, 17px):** "3 models agree pos, 0 disagree → classify with full reasoning trace".

## Callout (philosophy box)

**Living system:** As new data arrives, shapes become clearer, new models emerge, previously significant ranges may lose significance, and new ranges appear. The architecture supports incremental updates — no full rebuild needed.

## Regeneration instructions

- **Layout:** single page: h1, `.subtitle`, a `.toc` box (background `#f8fafb`, border `1px solid #e0e0e0`, padding 20px 30px, radius 4px, containing a bold "Table of Contents" heading and an `<ol>` of anchor links in `#2980b9`), then one h2 (with `id` anchor) per section, each followed by its own single-row `.obj-table`: left `<td>` (45%) holds `.obj-title` + bullets/paragraphs, right `<td>` (55%, centered) holds the canvas. Ends with a `.philosophy` callout.
- **Page CSS:** body system sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 1.05em; table cell borders `1px solid #e0e0e0`, padding 20px 24px, even rows `#fafcfe`; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` `#1a5276`; `.philosophy` background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em. No nav bar, no back/home links.
- **Canvases:** all 720×280 intrinsic; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; shared `box()` (rounded rect + centered multi-line text) and `arrow()` (line + filled arrowhead) helpers. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, accent blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gold `#f39c12`, purple `#8e44ad`, teal `#1abc9c`, gray `#7f8c8d`, bar fill `rgba(26,82,118,0.35)`.
