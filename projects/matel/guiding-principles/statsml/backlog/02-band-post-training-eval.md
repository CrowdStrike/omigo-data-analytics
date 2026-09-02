# Using Band in Post-Training Evaluation

**Page type:** detail page (backlog-style two-column layout table: text left 45%, viz right 55%, one `.lang-section` per section)
**HTML title tag:** Using Band in Post-Training Evaluation — Discussion Backlog

**Subtitle:** A principled gate for which CNN predictions to trust

## Callout (intro box)

CNN scores in the 50-60% range correspond to only 14% actual accuracy. The SE band provides a principled gate for which predictions to trust.

## Section 1: Calibration Gap

Current CNN: scores of 50-60% mean only 14% chance of being correct. Perfect calibration = diagonal. Band gating pushes low scores down to match reality.

### Visualization (canvas `c1`, 720×300)

Bar chart of actual accuracy per CNN score range, colored by calibration gap, with ideal-calibration dash markers.

- **Title (bold 16px, top center, `#1a5276`):** "CNN Calibration: Score Range vs Actual Accuracy".
- **Data:** score ranges `['50-60%', '60-70%', '70-80%', '80-90%', '90-100%']`; actual accuracy `[0.14, 0.38, 0.62, 0.82, 0.95]`; ideal calibration (range midpoints) `[0.55, 0.65, 0.75, 0.85, 0.95]`.
- **Margins:** top 30, right 30, bottom 50, left 60.
- **Axes:** L-shaped axes in `#2c3e50`, 1.5px; y labels 0%–100% in 20% steps (14px `#2c3e50`) with light gridlines `#ecf0f1` 0.5px; y-axis label "Actual"; x-axis title "CNN Score Range" (15px, centered below).
- **Bars:** one per range, width = slot − 20; color by |actual − ideal| gap: `#e74c3c` if gap > 0.25 (50-60%, 60-70% bars), `#e67e22` if gap > 0.15 (none), else `#27ae60` (70-80%, 80-90%, 90-100% bars); 0.5px `#1a5276` stroke. Value label (bold 15px white) inside the top of each bar: "14%", "38%", "62%", "82%", "95%". Range label 14px `#2c3e50` below each bar.
- **Ideal markers:** short dashed horizontal ticks (`#1a5276`, 2px, dash 4/3, 16px wide) at each bar center at the ideal-calibration level.
- **Legend (top right):** dashed blue line sample with 14px `#2c3e50` text "Ideal (perfect cal.)".

**Caption (italic, `.example`):** Calibration curve: CNN score vs actual accuracy

## Section 2: Band-Gated Confidence

- **Step 1:** CNN outputs per-class scores (e.g., bimodal=62%)
- **Step 2:** Check if critical features exceed SE band
  - Valley depth vs band width
  - Peak prominence vs band ceiling
- **Step 3:** If feature NOT above band → multiply score by `band_confidence` (0.3-0.7)
- **Result:** Low-confidence predictions suppressed; high-confidence untouched

**Key Question:** Can the confidence factor be learned from a calibration dataset? Use held-out set to fit band_confidence = f(CNN_score, band_ratio).

### Visualization (canvas `c2`, 720×300)

Calibration scatter/line plot: predicted score vs actual accuracy, before (red) vs after (green) band gating, with a perfect-calibration diagonal.

- **Title (bold 16px, top center, `#1a5276`):** "Band Gating: Before (red) vs After (green)".
- **Data (score, actual):**
  - Before (red): `(0.55, 0.14), (0.65, 0.38), (0.75, 0.62), (0.85, 0.82), (0.95, 0.95)`.
  - After (green): `(0.22, 0.14), (0.42, 0.38), (0.68, 0.62), (0.84, 0.82), (0.95, 0.95)` — low scores pushed down by band_confidence factors (0.55×0.4, 0.65×0.65, 0.75×0.9; top two untouched).
- **Margins:** top 30, right 20, bottom 50, left 60.
- **Axes:** L-shaped axes in `#2c3e50`, 1.5px; both axes labeled 0%–100% in 20% steps (13px), horizontal gridlines `#ecf0f1` 0.5px; x-axis title "Predicted Score" (15px), rotated y-axis title "Actual Accuracy".
- **Diagonal:** dashed gray line (`#bdc3c7`, 2px, dash 6/4) from bottom-left to top-right (perfect calibration).
- **Series:** each series drawn as a connected 2.5px line through its points plus 6px-radius filled dots with 1.5px white outlines — before in `#e74c3c`, after in `#27ae60`.
- **Suppression arrows:** thin dotted gray connectors (`#7f8c8d`, 1px, dash 2/2) linking the first three before-points to their after-points.
- **Legend (top right):** red dot + "Before gating", green dot + "After gating" (14px `#2c3e50`).

**Caption (italic, `.example`):** Before/after band gating — improved calibration

## Regeneration instructions

- **Layout:** backlog detail page (kusto-style 2-col). h1 (no index number) + `.subtitle` paragraph + `.intro` callout, then one `.lang-section` per section: `<h2>N. Title</h2>` followed by `<table class="layout">` with one `<tr>`: left `<td class="text-col">` (45%) holding bullets/paragraphs and optional `.key-point` div, right `<td class="viz-col">` (55%) holding the canvas plus an italic `.example` caption paragraph.
- **Page CSS:** body `system-ui, -apple-system, sans-serif`, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`; h2 1.3rem `#1a5276` with `border-bottom: 2px solid #2980b9`; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8`, left border `3px solid #2980b9`, padding 8px 12px, 0.9rem; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem; `ul` 0.92rem; canvas `width: 100%`, 1px `#e0e0e0` border, 4px radius; `code` background `#e8f0f8`, `#1a5276`. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, accent blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Canvas:** intrinsic 720×300; shared `setupCanvas(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; CSS scales the canvas to 100% of the viz cell.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
