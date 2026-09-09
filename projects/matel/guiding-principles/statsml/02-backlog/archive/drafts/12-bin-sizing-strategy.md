# Bin Sizing Strategy

**Page type:** detail page (TOC box + two-column obj-table layout: text left 45%, canvas right 55%, one table per section)
**HTML title tag:** Bin Sizing Strategy

**Subtitle:** How bin width affects shape perception — and why we run at multiple scales.

## Table of Contents

1. The Bin Size Problem (#problem)
2. Standard Formulas (#formulas)
3. Two Complementary Strategies (#strategies)
4. Multi-Resolution Persistence (#persistence)
5. Real-World Examples (#examples)

## 1. The Bin Size Problem

**Same Data, Different Bin Widths = Different Shapes**

- **Too few bins (wide):** Smooths out real peaks and valleys. A bimodal distribution looks unimodal.
- **Too many bins (narrow):** Noise looks like peaks. A smooth normal appears multimodal.
- **The "right" number:** Depends on n, shape, and what you're detecting — which you don't know yet.

**This is not just visualization.** If shape detection runs at the wrong bin width, every downstream decision (test selection, splitting, feature evaluation) inherits the error.

### Visualization (canvas `c1`, 720×280)

Three mini bar-histograms of the same underlying bimodal data at three bin counts, side by side.

- **Title (bold `#1a5276`, 17px, top left at 40,22):** "Same Data — 5 Bins vs 15 Bins vs 50 Bins"
- **Panel 1 (x=30, y=40, 200×200), data (5 bins):** `[25, 30, 28, 22, 15]`, maxV=32, label below: "5 bins: \"unimodal\""
- **Panel 2 (x=260, y=40, 200×200), data (15 bins):** `[5, 10, 18, 25, 20, 12, 8, 6, 7, 12, 20, 22, 18, 10, 5]`, maxV=26, label: "15 bins: bimodal!"
- **Panel 3 (x=490, y=40, 200×200), data (50 bins):** computed as `exp(-0.5*((i-15)/5)^2)*20 + exp(-0.5*((i-35)/6)^2)*18 + sin(i*1.7)*3` for i=0..49, maxV=24, label: "50 bins: noisy mess"
- **Bars:** fill `rgba(26,82,118,0.4)`, 1px gap between bars; labels in gray `#666` 17px centered below each panel.

## 2. Standard Formulas

**Each Formula Makes Different Assumptions**

- **Square root:** √n bins. Simple, no assumptions. Ignores data spread.
- **Sturges:** ⌈log₂(n) + 1⌉. Assumes normality. Too few for skewed data.
- **Scott:** range / (3.49 × σ × n⁻¹/³). Assumes smooth. Oversmooths tails.
- **Freedman-Diaconis:** range / (2 × IQR × n⁻¹/³). Robust to outliers.

**Problem:** Every formula gives ONE answer. No single bin count is correct for all shapes. You can't know which formula's assumptions hold until after you've detected the shape — a chicken-and-egg problem.

### Visualization (canvas `c2`, 720×280)

Horizontal bar chart comparing bin counts from six formulas for n=1000.

- **Title (bold `#1a5276`, top left):** "Bin Count Formulas for n=1000"
- **Rows (one per formula, 36px apart starting y=48; bar starts at x=200, width scaled to max 35 bins over 400px):**
  - Sturges: 11 bins, color `#e74c3c`
  - n/100 (static): 10 bins, color `#e67e22`
  - n/50 (static): 20 bins, color `#f39c12`
  - Scott (typical): 18 bins, color `#2980b9`
  - Freedman-Diaconis: 24 bins, color `#8e44ad`
  - √n: 31 bins, color `#27ae60`
- **Bar style:** fill at 0.3 alpha of the row color, 1.5px stroke of the row color, height 26px; formula name in `#333` at x=40; bold value label "N bins" in row color right of the bar.
- **Caption (bottom center, gray `#888`):** "Range: 10-31 bins. Which is \"right\"? → Run all of them."

## 3. Two Complementary Strategies

**Adaptive (Resolution) + Static (Density)**

- **Strategy A — Adaptive:** Bin count from formulas (sqrt, Sturges, Scott, FD). Optimizes resolution — finest detail you can resolve. May produce bins with very few observations.
- **Strategy B — Static:** bins = n / min_per_bin (50 or 100). Guarantees every bin has enough data for statistical conclusions.
- **Combined:** Run shape detection at ALL scales. Trust what persists across resolutions.

**Example (n=1000):** √n=31 bins, Sturges=11, n/50=20, n/100=10. Run all four → if bimodality shows at 11, 20, and 31 bins, it's real.

### Visualization (canvas `c3`, 720×280)

Two side-by-side rounded boxes comparing the strategies, plus a bottom takeaway line.

- **Title (bold `#1a5276`, top left):** "Strategy A (Resolution) + Strategy B (Density)"
- **Left box (x=30, y=45, 320×180):** blue `#2980b9` — 0.1-alpha fill + 2px stroke. Centered lines: bold blue "A: Adaptive (Resolution)"; then in `#333`: "√n, Sturges, Scott, FD" / "Goal: finest detail" / "Trade-off: some bins may" / "have very few observations"; final blue line "Finds peaks & valleys".
- **Right box (x=380, y=45, 320×180):** green `#27ae60` — same style. Centered lines: bold green "B: Static (Density)"; then in `#333`: "n/50, n/100" / "Goal: every bin has n≥50+" / "Trade-off: may miss fine" / "structure (fewer bins)"; final green line "Enables statistics per bin".
- **Takeaway (bottom center, bold `#555`):** "Run BOTH → trust what persists"

## 4. Multi-Resolution Persistence

**Real Structure Persists Across Scales; Artifacts Don't**

- Run shape detection at 4-6 different bin counts (the "resolution ladder")
- **Persistence score** = (resolutions that detect it) / (total resolutions)
- Score 1.0: detected everywhere → definite real structure
- Score 0.67+: detected at most scales → likely real
- Score 0.33-0.66: ambiguous — marginal or artifact
- Score < 0.33: probably noise created by that specific bin width

**Example:** A second peak at 10 bins and 15 bins but not at 20 or 31 → persistence 0.33 → probably noise, not real bimodality.

### Visualization (canvas `c4`, 720×280)

Persistence matrix: three feature rows × six resolution columns of check/dot cells with score verdicts.

- **Title (bold `#1a5276`, top left):** "Persistence: Does Structure Survive Across Resolutions?"
- **Column header (gray `#666`, centered at y=55):** "bins →" then resolution labels "10", "15", "18", "20", "24", "31" at x=230 + i*55.
- **Rows (start y=75, 60px apart; feature name in `#333` at x=40; 24×30 cells):**
  - "Second peak": detections `[0,1,1,1,0,0]`, score 0.50 → "Ambiguous"
  - "Right tail": detections `[1,1,1,1,1,1]`, score 1.00 → "Real!"
  - "Small bump": detections `[0,0,1,0,0,0]`, score 0.17 → "Noise"
- **Cell style:** detected = fill `rgba(39,174,96,0.5)`, stroke `#27ae60`, bold green "✓"; not detected = fill `rgba(200,200,200,0.3)`, stroke `#ddd`, gray "·".
- **Score labels (bold, x=570):** "0.50 → Ambiguous" in `#e67e22`, "1.00 → Real!" in `#27ae60`, "0.17 → Noise" in `#e74c3c` (color rule: ≥0.67 green, 0.34-0.66 orange, else red).

## 5. Real-World Examples

**Income: Bimodality Depends on Resolution**

- At 10 bins: looks like single right-skewed hill
- At 30 bins: a dip appears around $45k — potential bimodal?
- At 50 bins: the dip is noise (different position every time)
- **Verdict:** Persistence 0.3 → not truly bimodal. Right-skewed unimodal confirmed.

### Visualization (canvas `c5a`, 720×240)

Two mini histograms of income at different resolutions with a red verdict caption.

- **Title (bold `#1a5276`, top left):** "Income: Apparent Dip Doesn't Persist → Right-Skewed Confirmed"
- **Left panel (x=40, y=50, 300×140), 10 bins:** `[35, 25, 15, 10, 6, 4, 3, 1, 1, 0]`, label "10 bins: smooth right-skew"
- **Right panel (x=390, y=50, 300×140), 30 bins:** `[15,12,10,9,8,7,6,5,5,4,3,4,3,2,3,2,2,1,1,1,1,1,0,0,1,0,0,0,0,0]`, label "30 bins: dip at $45k (noise)"
- **Bars:** `rgba(26,82,118,0.4)`; labels gray `#666` centered below panels.
- **Caption (bottom center, red `#e74c3c`):** "Persistence 0.3 → not bimodal"

**Hours/Week: Spike Persists at All Resolutions**

- At 10 bins: massive spike in bin containing 40
- At 20 bins: spike still dominates
- At 50 bins: spike clearly isolated at exactly 40
- **Verdict:** Persistence 1.0 → the spike at 40 is undeniable real structure.

### Visualization (canvas `c5b`, 720×240)

Three mini histograms of hours/week at increasing resolution; the tallest (spike) bar highlighted red in each.

- **Title (bold `#1a5276`, top left):** "Hours/Week: Spike at 40 Persists at Every Resolution"
- **Panel 1 (x=30, y=50, 200×130), 10 bins:** `[3, 5, 8, 12, 50, 15, 5, 2, 1, 0]`, label "10 bins"
- **Panel 2 (x=260, y=50, 200×130), 20 bins:** `[1,2,3,4,5,6,7,8,50,12,8,5,4,3,2,1,1,1,0,0]`, label "20 bins"
- **Panel 3 (x=490, y=50, 200×130), 50 bins:** value 50 at i=20, otherwise `exp(-0.5*((i-20)/8)^2)*8` for i=0..49, label "50 bins"
- **Bars:** spike (max) bar `rgba(231,76,60,0.5)`, others `rgba(26,82,118,0.35)`; labels gray `#666`.
- **Caption (bottom center, bold green `#27ae60`):** "Persistence 1.0 → spike is real structure"

## Callout (philosophy box)

**The CNN shape classifier uses a fixed 64-bin rendering.** But multi-resolution analysis runs separately to validate: if the CNN says "bimodal" but persistence is low, downgrade confidence. If the CNN says "bell" but persistence reveals a hidden valley, flag for review.

## Regeneration instructions

- **Layout:** TOC-reference detail page: h1, `.subtitle`, a `.toc` box (bold "Table of Contents" + ordered anchor list linking to `#problem`, `#formulas`, `#strategies`, `#persistence`, `#examples`), then one h2 per section each followed by one or more `.obj-table` blocks (full-width table; left `<td>` 45% with `.obj-title` + bullets/paragraphs, right `<td>` 55% centered holding the canvas; even rows background `#fafcfe`). Section 5 has two consecutive obj-tables. A `.philosophy` callout closes the page before the script.
- **Page CSS:** body system sans-serif, white `#ffffff` background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px bottom border `#2980b9`; `.subtitle` `#666` 1.05em; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` `#1a5276`; ul 0.9em `#333`; `.toc` background `#f8fafb`, border `1px solid #e0e0e0`, radius 4px, links `#2980b9`; `.philosophy` background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em. No nav bar, no back/home links.
- **Canvas:** intrinsic `width`/`height` attributes as given per chart; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. All chart text 17px -apple-system. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35-0.4)`, secondary blue `#2980b9`, purple `#8e44ad`, gray text `#666`/`#333`.
- In regenerated HTML, any card/anchor links use `.html` extensions.
