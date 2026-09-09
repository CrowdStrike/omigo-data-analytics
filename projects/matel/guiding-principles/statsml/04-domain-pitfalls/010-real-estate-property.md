# Real Estate Data Pitfalls

**Page type:** detail page (one h2 per pitfall, each followed by a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** Real Estate Data Pitfalls

**Subtitle:** Why property valuation models fail: unique assets, hidden data, and market psychology

## Callout (philosophy box)

Real estate is one of the most treacherous domains for ML. Every property is unique, the data you see is heavily filtered by market dynamics, and macro cycles can overwhelm any feature-based model. Understanding these pitfalls is essential before trusting any automated valuation model (AVM).

## Comparables Aren't Comparable

**Obj-title:** Each Property Is Unique; Adjustments Are Subjective

The "comp" approach assumes similar properties sell for similar prices. But no two properties are identical. Appraisers apply adjustment factors for differences in square footage, lot size, condition, and view -- but these adjustments are highly subjective.

- **Problem:** Two appraisers adjusting the same comps can produce valuations $80k apart
- **Impact:** Model trained on "adjusted comps" inherits human bias
- **Example:** A $15/sqft adjustment vs $25/sqft changes a 2,400 sqft comp's value by $24,000

There is no ground truth for what adjustments should be -- only market conventions that vary by appraiser, region, and time.

### Visualization (canvas `canvas1`, 720×260)

Dot-plot comparison of two appraisers' adjusted values for the same three comps.

- **Title (bold 15px, `#1a5276`, centered at y=20):** "Same Comps, Different Adjustments = Different Valuations".
- **Subheading (17px, `#1a5276`, centered at y=48):** "Subject: 2,400 sqft, Good Condition".
- **Plot area:** x from 80 to 680, y from 70 (top) to 230 (bottom); value scale $390k–$520k. Light `#f0f0f0` horizontal gridlines with right-aligned `#666` 13px labels at $400k, $430k, $460k, $490k, $520k.
- **Data** (three x positions evenly spaced; each comp shows two dots radius 7 connected by a dashed 4/4 `#e0e0e0` vertical line; 13px `#333` comp label below axis):
  - Comp A: Appraiser A (blue `#2980b9`) 470, Appraiser B (orange `#e67e22`) 500
  - Comp B: Appraiser A 430, Appraiser B 410
  - Comp C: Appraiser A 448, Appraiser B 460
- **Average lines:** dashed 6/4 width-2 horizontal lines across the plot at each appraiser's mean — blue at 449.33, orange at 456.67.
- **Legend (top right, 13px, color swatch squares):** blue "Appraiser A: $449k"; orange "Appraiser B: $457k".
- **Bottom annotation (bold 14px, `#e74c3c`, centered):** "Gap: $80k+ on same comps".

## Survivorship Bias in Listings

**Obj-title:** Only Sold Properties Are Visible; Failures Disappear

Your training data consists of properties that successfully sold. But many listings are withdrawn, expired, or reduced until they sell at a different price. The unsold properties -- which would tell you about overpricing -- are invisible.

- **Problem:** Model only sees the "winners" -- properties that found a buyer
- **Impact:** Overestimates value because overpriced listings are censored from the data
- **Scale:** In a typical market, 20-30% of listings never sell at original terms

This is classic survivorship bias: your model learns what sold, not what the market actually looks like.

### Visualization (canvas `canvas2`, 720×260)

Horizontal funnel bar chart of listing outcomes.

- **Title (bold 15px, `#1a5276`, centered at y=22):** "What Your Model Sees vs. Reality".
- **Bars** (x from 200 to 620 = 100 count; height 32, gap 10, starting y=45; right-aligned 14px `#333` label left of bar; count text in white inside bar when wide enough, e.g. "100 properties", else `#333` after bar):
  - "Listed" — 100, fill `rgba(26,82,118,0.35)`
  - "Sold (in your data)" — 72, `#27ae60`
  - "Price Reduced & Sold" — 13, `#e67e22`
  - "Withdrawn" — 9, `#e74c3c`
  - "Expired" — 6, `#c0392b`
- **Brackets (right of bars, square bracket shapes, 12px labels):** green `#27ae60` bracket spanning the first three bars labeled "Visible" / "(85%)"; red `#e74c3c` bracket spanning the last two bars labeled "Invisible" / "(15%)".
- **Bottom note (bold 13px, `#e74c3c`, centered):** "Model trains only on sold properties = biased toward "sellable" prices".

## Spatial Autocorrelation Leakage

**Obj-title:** Neighbors in Train and Test = Memorizing Location

Properties near each other share unobserved factors: school districts, noise levels, future development plans. If your train/test split puts neighboring properties in both sets, the model memorizes location rather than learning generalizable features.

- **Problem:** Random train/test split ignores spatial structure
- **Impact:** Model appears to have 95% accuracy but fails in new neighborhoods
- **Fix:** Spatial blocking -- entire neighborhoods must be in train OR test, never both

A model that knows "123 Oak St sold for $450k" and predicts "125 Oak St = $455k" isn't learning features -- it's memorizing addresses.

### Visualization (canvas `canvas3`, 720×260)

Two 7×7 dot grids comparing random vs spatial-block train/test splits.

- **Title (bold 15px, `#1a5276`, centered at y=20):** "Random Split vs. Spatial Block Split".
- **Grid geometry:** 7×7 cells of 28px; dots radius 10; train dots `rgba(41,128,185,0.7)`, test dots `rgba(231,76,60,0.7)`.
- **Left grid** at (60,45), 13px `#333` heading "Random Split (leaks)". Pattern rows (1=train, 0=test):
  `[1,0,1,1,0,1,0]`, `[0,1,0,1,1,0,1]`, `[1,1,0,0,1,0,0]`, `[0,1,1,0,0,1,1]`, `[1,0,0,1,1,0,1]`, `[0,0,1,1,0,1,0]`, `[1,1,0,0,1,0,1]`.
  The two top-left adjacent cells (row 0, cols 0–1) are circled with dashed `#e74c3c` rings (radius 14) joined by a dashed line, with 11px `#e74c3c` label "Neighbors leak!" above.
- **Right grid** at (410,45), heading "Spatial Block Split (correct)". Columns 0–3 all train (blue), columns 4–6 all test (red); vertical dashed 6/4 `#1a5276` divider line at column boundary; 11px `#27ae60` two-line label below the divider: "No neighbors" / "across boundary".
- **Legend (bottom center, 13px):** blue dot "Train", red dot "Test".

## Hidden Renovations Masquerade as Appreciation

**Obj-title:** Price Jumps Attributed to Market When Renovation Was the Cause

A property sells for $400k in 2019, then $580k in 2022. Your model says "15% annual appreciation!" Reality: the owner spent $150k on a kitchen, bathrooms, and roof. Public records rarely capture renovation spend reliably.

- **Problem:** Permit data is incomplete; many renovations go unpermitted
- **Impact:** Appreciation models are inflated by hidden capital improvements
- **Scale:** ~40% of resales involve some renovation between purchase and sale

Without renovation data, your "appreciation model" is partially a "renovation detection failure."

### Visualization (canvas `canvas4`, 720×260)

Line chart: actual price history with renovation jump vs smooth model line.

- **Title (bold 15px, `#1a5276`, centered at y=20):** "Price History: What the Model Thinks vs. Reality".
- **Plot area:** x 80–650, y 50–210; value scale $300k–$650k with `#f0f0f0` gridlines and right-aligned 12px `#666` labels every $50k; 12px x-axis year labels.
- **Data (years 2017–2023):**
  - Actual prices (solid `#e74c3c`, width 2.5): `[340, 355, 370, 400, 420, 580, 600]`
  - Model line (dashed 6/4 `#2980b9`, width 2): `[340, 365, 390, 418, 448, 480, 515]`
- **Jump highlight:** vertical `#e67e22` arrow (width 2, arrowhead at top) at the 2021 x-position spanning from $420k up to $580k.
- **Annotations (12px):** `#2980b9` "Model: "15%/yr appreciation"" near top right; `#e74c3c` two lines next to the arrow: "Reality: $150k renovation" / "happened here (2021)".
- **Legend (below x-axis, 12px `#333`):** dashed blue line sample + "Model (smooth appreciation)"; solid red line sample + "Actual (with hidden renovation)".

## Market Cycles Dominate Features

**Obj-title:** Hot Market: Shed Sells for $500k; Cold Market: Mansion Doesn't Sell

In a hot market, any property sells above asking. In a cold market, even premium properties sit for months. Market sentiment overwhelms property features, making feature-based models unreliable across cycles.

- **Problem:** Features explain only 30-40% of variance; market cycle explains 50%+
- **Impact:** Model trained in hot market catastrophically overvalues in downturn
- **Example:** Same 3BR/2BA home: $520k in 2022 (12 offers), $410k in 2024 (sat 90 days)

If your model doesn't explicitly account for market regime, it's fitting noise from whichever cycle it was trained on.

### Visualization (canvas `canvas5`, 720×260)

Two side-by-side scenario boxes for the same house in different markets.

- **Title (bold 15px, `#1a5276`, centered at y=20):** "Same House, Different Market = Wildly Different Outcomes".
- **Left box:** 280×190 at (50,45), stroke `#e74c3c` width 2. Header (bold 14px `#e74c3c`): "HOT MARKET (2022)". Feature list (13px `#333`): "3 BR / 2 BA", "1,800 sqft", "Built 1985", "Needs new roof". Result in green `#27ae60`: bold 16px "SOLD: $520,000", then 12px "12 offers, 3 days on market" and "$45k over asking".
- **Right box:** 280×190 at (390,45), stroke `#2980b9` width 2. Header (bold 14px `#2980b9`): "COLD MARKET (2024)". Same feature list. Result in red `#e74c3c`: bold 16px "SOLD: $410,000", then 12px "0 offers for 90 days, 2 price cuts" and "$65k under original asking".
- **Connector:** dashed 4/4 `#666` horizontal line between the boxes with 12px `#666` two-line label "Same" / "house".
- **Bottom note (bold 13px, `#e74c3c`, centered):** "$110k difference (21%) driven entirely by market cycle, not features".

## Regeneration instructions

- **Layout:** standard detail page. h1 + `.subtitle` + one `.philosophy` callout, then one `<h2>` per pitfall followed by an `.obj-table` (full-width, border-collapse) with a single `<tr>`: left `<td>` (45%) holds `.obj-title`, an intro paragraph, a labeled-bullet `<ul>`, and a closing paragraph; right `<td>` (55%, centered) holds the canvas. Even rows background `#fafcfe`.
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 1.05em; paragraphs 0.95em `#333`; cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` `#1a5276`; `.philosophy` — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em. No nav bar, no back/home links.
- **Canvases:** each 720×260 intrinsic; shared `setupCanvas(id, w, h)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), and calls `ctx.scale` so drawing stays in logical coordinates. Titles bold 15px -apple-system.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`/dark red `#c0392b`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`, gray text `#333`/`#666`.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
