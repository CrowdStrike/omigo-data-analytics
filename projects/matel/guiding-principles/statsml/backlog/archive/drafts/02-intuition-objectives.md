# STATSML — Intuition & Key Objectives

**Page type:** detail page (intro h2 sections, then two-column obj-table layout: text left 45%, canvas right 55%, one row per objective, plus a closing philosophy callout)
**HTML title tag:** STATSML — Intuition & Key Objectives

**Subtitle:** Don't assume. Verify. Then — and only then — apply the right tool.

## The Core Problem

Every ML model has assumptions about the data it operates on. In practice, these assumptions are never checked — they're just hoped to hold.

- Logistic regression assumes normality — nobody checks
- Decision trees split on info gain — nobody asks if the split is statistically meaningful
- t-tests assume normality — people apply them to bimodal data

**We violate assumptions left and right, then wonder why models don't generalize.**

## The Core Intuition

Once you know the shape, you know which tests are valid. Once you know which tests are valid, you can trust their results. Only results you can trust should inform decisions.

## Key Objectives

## 1. Profile Before Modeling

Before any ML model touches the data, profile every feature:

- What type? (categorical, discrete, continuous)
- Where do values exist? (clusters, gaps, sparse regions)
- What shape? (don't assume normal)
- Enough data in each region?

### Visualization (canvas `c1`, 540×220)

Three small histograms side by side showing three different shapes of the same feature.

- **Panels:** each 150px wide, 30px gap, centered horizontally; bars fill `rgba(26,82,118,0.35)`, height scaled to max value within panel over (h-60), baseline at h-30, bar width = 150/11 with 1px gap.
  - Panel 1 label "bell", data `[2,5,12,22,35,42,35,22,12,5,2]`.
  - Panel 2 label "bimodal", data `[20,30,25,8,3,2,3,8,25,30,20]`.
  - Panel 3 label "spike", data `[3,3,3,3,3,45,3,3,3,3,3]`.
- **Panel labels:** centered under each panel in `#1a5276`, 20px system font.
- **Title (top center, `#222`, 20px):** "Same feature — which shape? Profile first."

## 2. Match the Test to the Data

- Normal in this range? → t-test valid
- Not normal? → Mann-Whitney
- Too few samples? → don't test
- Categorical? → chi-squared or Fisher's exact

Never apply a test whose preconditions aren't met.

### Visualization (canvas `c2`, 540×220)

Decision-arrow diagram: four rows, each a left box (data shape) with an arrow to a right colored box (test).

- **Rows (46px tall, starting y=18):** left box at x=30, 140×34, fill `#f0f4f8`, stroke `#ddd`, label centered in `#1a5276` 20px; right box at x=340, 160×34, filled with row color, white bold 17px label; connecting gray arrow (`#aaa`, width 1.5) from x=175 to x=335 with filled arrowhead.
  - "Normal" → "t-test", box color `#27ae60`.
  - "Skewed" → "Mann-Whitney", box color `#e67e22`.
  - "n < 20" → "Don't test", box color `#e74c3c`.
  - "Categorical" → "Chi-squared", box color `#8e44ad`.

## 3. Multiple Candidate Models Per Feature

One feature described by multiple models — don't force a single choice:

- Right-skew captures tail behavior
- Mixture model reveals subpopulations
- KDE gives non-parametric view

Keep all that pass quality threshold. Different lenses on the same data.

### Visualization (canvas `c3`, 540×220)

Histogram with three overlaid model curves.

- **Histogram:** 16 bars, data `[2,5,12,22,35,42,35,22,12,5,2,3,8,15,10,5]`, scale max 45, fill `rgba(26,82,118,0.18)`, plot area x from 40 with bar width (w-80)/16, baseline at h-40, height range h-70.
- **Curves (line width 2.5, points at bar centers):**
  - "Right-skew" in `#e74c3c`: `[5,15,30,40,38,28,18,10,6,4,3,2,2,1,1,1]`.
  - "Mixture" in `#27ae60`: `[3,8,15,24,35,40,34,20,10,4,3,7,14,12,7,3]`.
  - "KDE" in `#8e44ad`: `[2,5,12,22,34,41,35,22,12,5,3,4,9,14,9,5]`.
- **Legend (top right, left-aligned at x=w-130, 20px):** "Right-skew" (`#e74c3c`, y=25), "Mixture" (`#27ae60`, y=44), "KDE" (`#8e44ad`, y=63).

## 4. Find Signal in Ranges, Not Globally

A feature might show no global significance (same mean for pos and neg) but have a specific range where one class dominates with 95%+ purity.

Global metrics miss local signal. Always look at the range level.

### Visualization (canvas `c4`, 540×220)

Paired-bar overlapping distributions (pos vs neg) with a highlighted left range.

- **Data (16 bins, scale max 30, bar width (w-80)/16, plot from x=40, baseline h-35, height range h-70):**
  - pos (left half-bars, fill `rgba(26,82,118,0.4)`): `[1,3,6,10,15,20,25,28,25,18,12,7,4,2,1,1]`.
  - neg (right half-bars, fill `rgba(231,76,60,0.4)`): `[1,1,2,4,7,12,18,25,28,25,20,15,10,6,3,1]`.
- **Highlighted range:** first 5 bin-widths shaded `rgba(39,174,96,0.12)` from y=25 to baseline; bold 16px green (`#27ae60`) label above it: "95% pos here".
- **Legend (bottom left):** blue square `#1a5276` labeled "pos" and red square `#e74c3c` labeled "neg" (12×12 swatches at y=h-22).
- **Caption (bottom center, `#222`, 20px):** "Global: no difference. Local: strong signal."

## 5. Expand Features Based on Validated Ranges

Each significant range becomes a new binary feature:

- `cholesterol in [270-603]` → Mann-Whitney p < 0.00001
- `cholesterol in [240-320]` → t-test p < 0.00000001

One raw feature → many derived features, each statistically justified.

### Visualization (canvas `c5`, 540×220)

Fan-out tree: one raw feature node branching to four range boxes.

- **Root label (bold 17px `#1a5276`, top center):** "Cholesterol (raw)" with a vertical blue (`#2980b9`, width 2) stem from y=36 to y=60, then thin lines (width 1.5) fanning to each child box center.
- **Child boxes (110×70 at y=95, 12px gaps, centered as group):** labels `[180-240]`, `[240-320]`, `[270-603]`, `[320-400]`; test text (bold 12px, second line) `n.s.`, `p < 1e-8`, `p < 1e-5`, `p < 0.01`; box fills `#bbb`, `#27ae60`, `#27ae60`, `#f39c12` respectively; white 20px text for the range label.
- **Caption (bottom center, `#222`, 20px):** "Each range = independent binary feature with its own p-value".

## 6. Conditional Distributions Matter

Weight for men ≠ weight for women. A categorical feature splits continuous features into subpopulations where:

- Shape is different
- Different tests become valid
- Signal invisible globally becomes clear within a stratum

### Visualization (canvas `c6`, 540×220)

Paired histograms (male vs female) with a dashed combined-average line.

- **Title (top center, `#222`, 20px):** "Weight (all) — dashed".
- **Data (12 bins, scale max 36, bar width (w-100)/12, plot from x=50, baseline h-35, height range h-65):**
  - male (left half-bars, fill `rgba(41,128,185,0.4)`): `[1,3,5,10,18,30,35,28,15,8,4,2]`.
  - female (right half-bars, fill `rgba(231,76,60,0.4)`): `[2,6,15,28,32,25,14,7,3,1,1,0]`.
- **Dashed line:** average of male and female per bin, stroke `#aaa`, width 1.5, dash [5,4], through bar centers.
- **Legend (top right):** 14×14 swatch `rgba(41,128,185,0.9)` labeled "Male" and `rgba(231,76,60,0.9)` labeled "Female".
- **Caption (bottom center, `#222`, 20px):** "Split by category → different shapes, different tests valid".

## 7. Statistical Guardrails on Splits

Decision trees exploit range-based splits but never check if a split is statistically significant. They treat 12 data points the same as 12,000.

Our approach: only split when validated, then use parent knowledge to predict what child should look like.

### Visualization (canvas `c7`, 540×220)

Split-decision tree diagram with sample counts.

- **Root:** bold 17px `#1a5276` text "n = 5000" at top center; below it a "Split?" box (80×30 at y=32, fill `#f0f4f8`, stroke `#2980b9`) with 20px `#1a5276` label.
- **Branches:** two blue (`#2980b9`, width 2) lines from root to left child at w/4 and right child at 3w/4 (y=95).
- **Left child (good):** 120×55 box at y=100, fill `#d4efdf`, stroke `#27ae60`; bold 17px green "n = 2400" and 20px "p < 0.0001".
- **Right child (bad):** 120×55 box at y=100, fill `#fdedec`, stroke `#e74c3c`; bold 17px red "n = 8" and 20px "STOP".
- **Caption (bottom center, `#222`, 20px):** "Only split when statistically justified".

## 8. Classification = Statistical Reasoning

The system produces a reasoning trace, not a bare label:

- "Cholesterol=285 in [270-603], purity 0.76, Mann-Whitney p<0.00001"
- "Also in [240-320] from mixture, purity 0.70, t-test"
- "Two models agree → strong evidence"

### Visualization (canvas `c8`, 540×220)

Monospace reasoning-trace text block, one line per step (17px SF Mono, left-aligned at x=30, starting y=40, 42px line spacing):

- "Input: cholesterol = 285" — bold, `#1a5276`.
- "Range [270-603]: purity=0.76, Mann-Whitney p<1e-5" — `#27ae60`.
- "Range [240-320]: purity=0.70, t-test p<1e-8" — `#27ae60`.
- "2 models agree → POSITIVE (strong)" — bold, `#1a5276`.

**Caption (bottom center, `#222`, 20px):** "Every prediction = auditable evidence chain".

## 9. Inherit Knowledge Across Splits

- Parent model PREDICTS what child should look like
- Prediction matches → inherit, no re-work
- Doesn't match → split revealed new structure, investigate

Preserves context that decision trees lose.

### Visualization (canvas `c9`, 540×220)

Stacked parent/child histograms connected by an arrow.

- **Parent histogram (top):** 11 bars, data `[2,5,12,25,38,42,38,25,12,5,2]`, bar width 40, scale max 44, fill `rgba(26,82,118,0.3)`, baseline y=75 with 60px height range, centered horizontally; label above (20px `#1a5276`, centered): "Parent: bell-shaped".
- **Arrow:** vertical blue (`#2980b9`, width 2) line from y=82 to y=108 with filled arrowhead; 20px `#333` text "predicts →" beside it at y=100.
- **Child histogram (bottom):** data `[1,4,10,22,35,40,36,23,11,4,1]`, fill `rgba(39,174,96,0.35)`, baseline y=195.
- **Caption (bottom center, bold 16px `#27ae60`):** "Child: matches → inherit models".

## 10. Detect Drift in Temporal Data

Distributions shift over time. A bell-shaped feature last quarter may be bimodal now.

- Sliding-window shape classification
- Monitor score stability (confidence drops = something changed)
- Changepoint detection
- Re-validate test assumptions after drift

### Visualization (canvas `c10`, 540×220)

Three histogram panels showing a bell morphing to bimodal, with red arrows between panels.

- **Panels (140px wide, 30px gap, centered as group; scale max 42, baseline h-40, height range h-70; label centered under each in 20px `#1a5276`):**
  - "Q1 (bell)", fill `rgba(26,82,118,0.4)`, data `[2,8,20,35,40,35,20,8,2]`.
  - "Q2 (flattening)", fill `rgba(230,126,34,0.4)`, data `[3,10,22,30,28,30,22,10,3]`.
  - "Q3 (bimodal)", fill `rgba(231,76,60,0.4)`, data `[8,20,28,15,5,15,28,20,8]`.
- **Arrows:** red (`#e74c3c`, width 2.5) horizontal arrows with filled arrowheads at mid-height between panel 1→2 and 2→3.
- **Title (top center, bold 17px `#e74c3c`):** "DRIFT DETECTED".

## 11. Detect Data Type Evolution

Data types evolve as systems change:

- `90210` → `90210-1234` (ZIP+4)
- `2024-01-15` → `2024-01-15T08:30:00.123Z`
- `5551234` → `+1-555-1234`
- `99999` → `A00001` (overflow)

Monitor type distribution over time — shifts signal schema evolution or upstream changes.

### Visualization (canvas `c11`, 540×220)

Format-progression rows plus a type-distribution bar.

- **Rows (starting y=35, 48px spacing):** monospace 16px before-value in `#1a5276` at x=30, gray 20px "→" at x=180, monospace 16px after-value in `#e74c3c` at x=210, 20px `#222` label at x=450.
  - `90210` → `90210-1234`, label "ZIP → ZIP+4".
  - `2024-01-15` → `2024-01-15T08:30:00.123Z`, label "Date → ISO+ms".
  - `99999` → `A00001`, label "Int → Alphanumeric".
- **Distribution bar (below rows, x=30, width 420, height 22):** background `#f0f4f8`; 88% segment fill `rgba(26,82,118,0.5)` labeled "88% old format" in white 20px; 12% segment fill `rgba(231,76,60,0.5)` labeled "12% new" in white.

## Callout (philosophy box)

**The Philosophy in One Sentence:** Build ML that only uses what it can statistically justify — the right model, for the right data, with verified assumptions, producing explainable evidence.

## Regeneration instructions

- **Layout:** single page: h1, `.subtitle`, two intro h2 sections ("The Core Problem" with bullets, "The Core Intuition" paragraph), then an h2 "Key Objectives" followed by one `.obj-table` (full-width, border-collapse) with one `<tr>` per objective — left `<td>` (45%) holds `.obj-title` ("N. Title") + paragraphs/bullets, right `<td>` (55%, centered) holds the canvas. Ends with a `.philosophy` callout.
- **Page CSS:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 1.05em; ul 0.9em `#333`; `code` background `#e8f0f8`, `#1a5276`, radius 3px; `strong` `#1a5276`; table cells `1px solid #e0e0e0`, padding 20px 24px, even rows `#fafcfe`; `.obj-title` 1.05em weight 600 `#1a5276`; `.philosophy` background `#f0f4f8`, left border 4px solid `#2980b9`, padding 16px 20px, 1em. No nav bar, no back/home links.
- **Canvases:** all 540×220 intrinsic size; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper.
- **Palette:** primary blue `#1a5276`, accent blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, bar fill `rgba(26,82,118,0.35)`.
