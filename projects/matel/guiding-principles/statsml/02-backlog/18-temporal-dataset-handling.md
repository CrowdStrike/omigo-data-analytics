# Temporal Dataset Handling — Time as a Hidden Dimension

**Page type:** detail page (backlog-style: intro callout, numbered h2 sections, two-column layout table with text left ~45% and canvas right ~55%)
**HTML title tag:** Temporal Dataset Handling — Discussion Backlog

**Subtitle:** Every observation carries a time coordinate that silently invalidates static profiles

**Intro callout:** Temporal data = feature values + timestamp. Every observation carries a time coordinate that silently invalidates static profiles.

## 1. Core Challenges

- **Shape drift:** Bell-shaped today → bimodal next quarter
- **Bucketing instability:** Boundaries optimized for Q1 cut through peaks in Q3
- **Concept drift:** Same values, different meaning. 85% pos last year → 60% now
- **Seasonality masquerading as shape:** Mixing summer/winter = artificial multimodality
- **Changepoints:** Abrupt regime shifts (COVID, policy changes) make before/after incomparable
- **Stationarity assumption:** Every profile implicitly assumes stationarity

### Visualization (canvas `c2`, 720×300)

Side-by-side histogram comparison showing distribution shift between two time windows, on light gray `#f8f9fa` background.

- **Title (bold 15px, `#1a5276`, top center):** "Distribution Shift: Same Feature, Different Times"
- **Left histogram** at x=40, y=50, 260×180: label above (bold 14px `#1a5276`): "Time T1 (Bell-shaped)". Bar heights `[3, 7, 14, 22, 30, 35, 30, 22, 14, 7, 3]` scaled to max 35; bar width = 260/11 − 2, 2px gaps; fill `rgba(26,82,118,0.35)`, stroke `#1a5276` width 1. Baseline axis line in `#2c3e50`. Below axis (13px, green `#27ae60`): "Shape: BELL".
- **Middle KS-test annotation** at x≈330: dashed red arrow (`#e74c3c`, dash 5/3, width 2) pointing right from x=330 to x=390 at y=140 with solid arrowhead; bold red 14px label "KS Test" above, 13px red lines "p < 0.001" and "REJECT H0" below.
- **Right histogram** at x=410, y=50, 260×180: label above (bold 14px `#1a5276`): "Time T2 (Bimodal)". Bar heights `[8, 20, 28, 18, 5, 3, 5, 18, 28, 20, 8]` scaled to same max 35; fill `rgba(231,76,60,0.25)`, stroke `#e74c3c` width 1. Baseline axis line in `#2c3e50`. Below axis (13px, red `#e74c3c`): "Shape: BIMODAL".
- **Bottom annotation (14px `#2c3e50`, centered at y=280):** "Profile computed at T1 is INVALID at T2 — re-profiling needed"

## 2. How Time Affects Each Pipeline Step

- **Shape detection:** Per-window classification needed
- **Bucket strategy:** Quantile bins shift as distribution drifts
- **Enrichment scores:** Core signal metric decays
- **Sample sufficiency:** n=5000 spanning 5 years may be worse than n=500 from last month

### Visualization (canvas `c1`, 720×320)

Pipeline diagram: four step boxes with downward arrows to red issue boxes, over a horizontal time axis; light gray `#f8f9fa` background.

- **Title (bold 15px, `#1a5276`, top center):** "How Time Breaks Each Pipeline Step"
- **Top row — four solid blue boxes** (`#1a5276` fill, white bold 14px two-line text, 130×50 each, starting x=60 with 165px spacing, y=50): "Shape Detection", "Bucket Strategy", "Enrichment Scoring", "Sample Sufficiency". Boxes connected horizontally by light gray `#bdc3c7` lines with arrowheads.
- **Red arrows down** (`#e74c3c`, width 2, filled arrowheads) from each step box to a matching issue box at y=170.
- **Issue boxes** (fill `rgba(231,76,60,0.1)`, stroke `#e74c3c` width 1.5, red 13px two-line text): "Seasonal multimodality", "Boundaries shift", "Signal decays", "Staleness vs volume".
- **Time arrow at bottom** (y=270): blue `#1a5276` line from x=40 to x=680 with filled arrowhead; bold 15px blue "TIME" label centered below; tick markers with 13px `#555` labels above the line: "Q1 2024", "Q2 2024", "Q3 2024", "Q4 2024", "Q1 2025" (starting x=80, spaced 145px).
- **Caption (12px red `#e74c3c`, centered at y=300):** "Each step assumes stationarity — time breaks that assumption"

## 3. Approaches

Full-width section (no canvas).

- **Windowed profiling:** Profile in time windows, compare via KS test
- **Re-profiling triggers:** KS rejects, enrichment drops, shape class changes
- **Temporal metadata:** Every profile carries valid from/to + staleness score
- **Seasonal decomposition:** Detect periodicity, profile per season
- **Decay-weighted profiling:** Exponential decay on sample weights

**Key Questions** (red-bordered key-point callout):
(1) Right window size?
(2) Distinguish drift from seasonal?
(3) Re-train CNN per window?
(4) Per-feature temporal metadata?
(5) Discard vs tag pre-break data?

## Regeneration instructions

- **Layout:** backlog detail-page style. `<h1>` (2rem, `#1a5276`, 2px solid `#2980b9` bottom border), `.subtitle` (`#666`, 0.95rem), `.intro` callout (background `#f0f4f8`, left border `3px solid #2980b9`, padding 8px 12px, 0.9rem). Each section is a `.lang-section` (40px bottom margin) with an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) followed by a `table.layout` (full width, collapsed borders) with one `<tr>`: left `td.text-col` (45%) with bullets/callouts, right `td.viz-col` (55%) with the canvas. Section 3 has no table — bullets and key-point directly under the h2.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; `ul` 0.92rem with 20px left margin; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange, bar fill `rgba(26,82,118,0.35)`.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper. Note canvas ids: section 1 uses `c2`, section 2 uses `c1`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
