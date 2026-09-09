# Temporal Dataset Scenarios

**Page type:** detail page (TOC box followed by numbered h2 sections, each an obj-table row: text left 45%, canvas right 55%; closing philosophy callout)
**HTML title tag:** Temporal Dataset Scenarios

**Subtitle:** Distributions aren't static. Data collected over time shifts, drifts, and cycles — here's how to handle it.

## Table of Contents

**Table of Contents** (boxed `.toc`, ordered list of in-page anchor links):

1. Why Temporal Matters (#why)
2. Shape Drift (#drift)
3. Seasonality (#seasonality)
4. Concept Drift (#concept)
5. Changepoints (#changepoints)
6. Stationarity (#stationarity)
7. Windowed Profiling (#windowed)

## 1. Why Temporal Matters

**A Static Profile Can Be a Lie**

- Profile a feature once → get "right-skewed, mean=45." But that mean was 35 last year and will be 55 next year.
- A bucket that was "92% pos" in Q1 may be "60% pos" in Q3 — same range, different truth.
- Models trained on historical data silently degrade as the world changes.
- Temporal awareness = knowing WHEN your profile was true and when to re-profile.

**Key question:** Is this feature's distribution stable enough that a single profile is valid? Or does time matter?

### Visualization (canvas `c1`, 720×240)

Line chart of a feature mean drifting upward over years, with a confidence band.

- **Title (bold 17px `#1a5276`, left-aligned at 40,22):** "Feature Mean Drifting: Looks Stable in Snapshot, Moving Over Time".
- **Margins:** left 60, right 30, top 45, bottom 35.
- **Series:** blue line `#2980b9`, width 3, 50 points; y rises linearly from plot mid-height by 0.4×plot-height over the span, with sinusoidal wobble `sin(i*0.3)*10`.
- **Confidence band:** filled `rgba(41,128,185,0.15)`, ±20px around the line.
- **X labels (17px `#666`, centered):** "2019", "2020", "2021", "2022", "2023" at 0%, 25%, 50%, 75%, 100% of plot width along the bottom.
- **Annotations (17px `#e74c3c`, right-aligned):** "mean=35" near lower-left of the line start; "mean=55" near upper-right of the line end.

## 2. Shape Drift

**The Distribution Shape Itself Changes Over Time**

- Feature looks normal in Year 1, becomes bimodal by Year 3 (new subpopulation entered)
- Right-skew gets heavier over time (wealth concentration, tech adoption curves)
- A spike appears or disappears (new default value introduced, old one retired)

**Example — E-commerce spending:** Pre-2020: right-skewed bell (median $45). Post-2020: bimodal — a new cluster of high-frequency small orders ($5-15 delivery apps) emerged alongside the original distribution. Shape changed from unimodal to bimodal.

**Impact:** Tests valid for the old shape (t-test on bell) become invalid for the new shape (need splitting for bimodal).

### Visualization (canvas `c2`, 720×280)

Two side-by-side mini histograms showing a unimodal-to-bimodal shift.

- **Title (bold 17px `#1a5276` at 40,22):** "E-commerce Spending: Unimodal → Bimodal Over Time".
- **Left histogram** at x=40, y=50, 300×150, fill `rgba(41,128,185,0.4)`, bins: `[2, 5, 12, 22, 35, 30, 20, 12, 6, 3, 2, 1, 0, 0, 0]`, label below (17px `#333`, centered): "Pre-2020: Right-skew (single peak at $45)".
- **Right histogram** at x=390, y=50, 300×150, fill `rgba(39,174,96,0.4)`, bins: `[8, 18, 25, 15, 8, 5, 4, 5, 10, 18, 25, 20, 12, 5, 2]`, label: "Post-2020: Bimodal ($12 delivery + $45 regular)".
- **Arrow:** bold 17px orange `#e67e22` "→" centered between them at (360, 130).
- Bars are normalized to each histogram's max; 1px gap between bars.

## 3. Seasonality

**Recurring Patterns That Make Static Profiles Misleading**

- The "mean" of a seasonal feature is a fiction — nobody experiences the average
- A feature may have high separation in winter but zero separation in summer
- Profiling across all seasons blurs the signal and underestimates peak effects

**Example — Hospital ER admissions:** Summer: right-skewed, peaks at injuries/heat. Winter: bimodal — flu cluster + normal baseline. Annual profile looks "heavy-tailed bell" — missing both the summer spike pattern and winter bimodality.

**Example — Energy consumption:** July: right-skewed (AC load). April: near-uniform (mild weather). Profiling all months together: looks multimodal — an artifact of mixing seasons.

### Visualization (canvas `c3`, 720×280)

Three side-by-side mini histograms, one per season.

- **Title (bold 17px `#1a5276` at 40,22):** "Energy Consumption: Different Shape Each Season".
- **Histogram 1** at x=30, y=50, 200×130, fill `rgba(231,76,60,0.4)`, bins `[2, 4, 8, 12, 18, 25, 35, 30, 20, 10]`, label: "Summer (AC load)".
- **Histogram 2** at x=260, y=50, 200×130, fill `rgba(39,174,96,0.4)`, bins `[8, 10, 12, 14, 13, 12, 11, 10, 9, 8]`, label: "Spring (mild)".
- **Histogram 3** at x=490, y=50, 200×130, fill `rgba(41,128,185,0.4)`, bins `[3, 5, 10, 20, 30, 15, 8, 12, 22, 28]`, label: "Winter (heating bimodal)".
- **Bottom caption (17px `#e74c3c`, centered):** "Annual profile averages these → looks multimodal (artifact!)".

## 4. Concept Drift

**The Relationship Between Features and Target Changes**

- Feature distribution may stay the same, but what predicts "pos" shifts
- A range that was 85% pos last year is now 60% pos — same values, different meaning
- New patterns emerge that didn't exist in training data

**Example — Fraud detection:** 2019: large transactions at 3am = fraud signal (enrichment 8x). 2022: same pattern = legitimate crypto trading. The feature value didn't change — the world did. Enrichment dropped from 8x to 1.2x.

**Example — Loan defaults:** Pre-recession: debt-to-income >40% = high risk. During recession: even 25% DTI defaults. The threshold that separated classes moved.

**Detection:** Monitor enrichment scores over time. If a bucket's enrichment decays steadily, flag for re-evaluation.

### Visualization (canvas `c4`, 720×280)

Decaying line chart of enrichment over years with a threshold line.

- **Title (bold 17px `#1a5276` at 40,22):** "Fraud Signal Decay: Same Feature, Different Meaning Over Time".
- **Margins:** left 60, right 40, top 50, bottom 40.
- **Data points (year, enrichment):** 2019→8.0x, 2020→6.5x, 2021→4.0x, 2022→2.0x, 2023→1.2x; y-scale 0–9 mapped to plot height.
- **Line:** red `#e74c3c`, width 3, connecting all points.
- **Dots:** radius 6; red `#e74c3c` while enrichment > 2, gray `#bbb` otherwise (2022 and 2023 gray). Value labels ("8x", "6.5x", "4x", "2x", "1.2x") bold 17px `#333` above each point; year labels 17px `#666` along bottom.
- **Threshold line:** green `#27ae60` dashed (5,3), width 1.5, at enrichment 2.0, labeled right-aligned in green: "min useful (2x)".
- **Annotation above plot (17px `#e74c3c`, left-aligned):** '"Large 3am transactions = fraud" — signal decayed as crypto trading grew'.

## 5. Changepoints

**Abrupt Regime Shifts — Before/After Are Different Worlds**

- Not gradual drift — a sudden break where everything changes at once
- Data before the changepoint and after are effectively different datasets
- Profiling across the break produces a meaningless average of two regimes

**Example — COVID (March 2020):** Air travel bookings: pre-COVID bell (mean 2M/day). Post-COVID: collapsed to spike near zero, then slowly recovered with different shape (more leisure, less business). A single profile spanning 2019-2021 is fiction.

**Example — Policy change:** Hospital billing after ICD-10 transition (Oct 2015). Code distributions, claim amounts, denial rates all shifted overnight. Pre/post must be treated as separate populations.

**Detection:** CUSUM or PELT algorithms on rolling statistics. KS test between adjacent windows.

### Visualization (canvas `c5`, 720×280)

Time series with an abrupt vertical break at mid-plot.

- **Title (bold 17px `#1a5276` at 40,22):** "Air Travel Bookings: COVID Changepoint (March 2020)".
- **Margins:** left 60, right 30, top 50, bottom 35.
- **Pre-break series (left half):** blue `#2980b9`, width 2, stable wavy line near the top of the plot (`sin(i*0.5)*15` wobble), points 0–24 of 50.
- **Break marker:** vertical dashed red `#e74c3c` line (dash 5,3, width 2) at 50% of plot width, spanning full plot height, labeled bold red "BREAK" centered above it.
- **Post-break series (right half):** green `#27ae60`, width 2, collapses to near the bottom then slowly recovers over points 25–49 (linear recovery of the drop with `sin(i*0.4)*8` wobble).
- **Bottom labels (17px, centered):** blue at 25% width: "Pre-COVID: stable bell (2M/day)"; green at 75% width: "Post-COVID: collapsed → slow recovery (different shape)".

## 6. Stationarity

**When a Feature IS Stable Enough for Static Profiling**

- Stationary = mean, variance, and shape don't change over time
- Most demographic features are approximately stationary (age distribution, height, blood type)
- Most economic/behavioral features are NOT stationary (spending, engagement, prices)
- Test: KS test between first half and second half of data. If p > 0.05, treat as stationary.

**Examples of stationary features:** Patient height, resting heart rate, hemoglobin (within healthy pop), blood type frequencies, fingerprint patterns.

**Examples of non-stationary:** Stock prices, website traffic, social media engagement, transaction amounts, diagnosis codes, technology adoption rates.

### Visualization (canvas `c6`, 720×240)

Side-by-side line panels: stationary vs non-stationary series.

- **Title (bold 17px `#1a5276` at 40,22):** "Stationary vs Non-Stationary Features".
- **Left panel** (x 40–340): green `#27ae60` line, width 2, flat around y=100 with noise `sin(i*0.8)*12`; labels below in green (17px, centered at x=190): "Height (stationary)" and "✓ Single profile valid".
- **Right panel** (x 390–690): red `#e74c3c` line, width 2, trending upward from y≈140 by 60px with volatility `sin(i*1.2)*15`; labels in red (centered at x=540): "Stock price (non-stationary)" and "✗ Must use recent window".
- **Axis labels (17px `#999`, centered at bottom):** "time →" under each panel.

## 7. Windowed Profiling

**The Solution: Profile in Time Windows, Compare Across**

- **Step 1:** Divide data into time windows (monthly, quarterly, yearly — depends on expected change rate)
- **Step 2:** Profile each window independently (shape, ranges, enrichment)
- **Step 3:** Compare profiles across windows using KS test
- **Step 4:** If stable → use full dataset. If drifting → use most recent window only. If seasonal → profile per season.

**Re-profiling triggers:**

- KS test between current window and profile-window rejects (p < 0.01)
- Enrichment of any strong bucket drops below 1.5x
- Shape classification changes (bell → bimodal)
- Data volume doubles since last profile

### Visualization (canvas `c7`, 720×280)

Four rounded-rectangle window cards (Q1–Q4) connected by arrows, each showing shape / KS / action.

- **Title (bold 17px `#1a5276` at 40,22):** "Windowed Profiling: Compare Profiles Across Time Windows".
- **Cards:** four equal-width boxes across the canvas (each `(w-80)/4` wide minus 10px, at y=45, height 180, corner radius 6), 10% alpha fill plus 2px stroke of the card color. Card contents, centered:
  - Q1 — "shape: bell", "KS: —", action "baseline" (blue `#2980b9`)
  - Q2 — "shape: bell", "KS: p=0.42", action "stable ✓" (green `#27ae60`)
  - Q3 — "shape: right_skew", "KS: p=0.003", action "DRIFT! re-profile" (red `#e74c3c`)
  - Q4 — "shape: bimodal", "KS: p<0.001", action "SHIFT! new regime" (red `#e74c3c`)
- Window label bold 17px in card color; "shape:" and "KS:" lines 17px `#333`; action bold 17px in card color.
- **Connectors:** small gray `#999` arrows between adjacent cards at mid-height.
- **Bottom caption (17px `#555`, centered):** "KS test between windows detects when re-profiling is needed".

## Closing callout (philosophy box)

**The principle:** Every profile has a shelf life. A model trained on 2019 data making predictions in 2024 is using expired ingredients. Temporal awareness means knowing when your statistical facts were true — and detecting when they stop being true.

## Regeneration instructions

- **Layout:** long-form detail page: h1, `.subtitle`, boxed `.toc` (background `#f8fafb`, border `1px solid #e0e0e0`, padding 20px 30px, radius 4px, ordered list of `#anchor` links in `#2980b9`), then numbered `h2` sections each with an `id` matching its TOC anchor. Each section is a one-row `.obj-table`: full-width, border-collapse, cells `1px solid #e0e0e0` with 20px 24px padding; first `<td>` 45% (`.obj-title` + bullets/paragraphs), last `<td>` 55% centered holding the canvas; even rows background `#fafcfe`. Page ends with a `.philosophy` callout (background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em).
- **Page style:** body `-apple-system` sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with `2px solid #2980b9` bottom border and 8px padding-bottom; `.subtitle` `#666` 1.05em; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` `#1a5276`; ul 0.9em `#333`. No nav bar, no back/home links.
- **Canvas:** intrinsic `width`/`height` attributes as given per chart; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), and calls `ctx.scale` so drawing stays in logical coordinates. Chart text uses 17px `-apple-system` (bold 17px for titles/emphasis).
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; bar fills at 0.4 alpha (`rgba(41,128,185,0.4)`, `rgba(39,174,96,0.4)`, `rgba(231,76,60,0.4)`); grays `#666`/`#555`/`#333`/`#999`/`#bbb`.
