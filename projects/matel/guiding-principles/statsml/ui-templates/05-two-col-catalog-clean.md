# Catalog Title — Items by Category

**Page type:** other — UI template file (two-column catalog: one `.obj-table` with header row, each body row = text 50% | canvas 50%; placeholder content)
**HTML title tag:** TEMPLATE: Two-Column Catalog — Rows with Canvas Visualizations

**Template header comment (verbatim, kept in the CSS block):**

> ═══ TEMPLATE: Catalog of items, each as a 2-col row (text 50% | canvas 50%) ═══
> Use for: Bad metrics, examples, case studies, pattern catalogs
> Pattern: Title + subtitle → table rows (domain badge + title + bullets | canvas)
> Source: reference/metrics/bad-examples.html, reference/metrics/metric-testing.html

**Subtitle:** One-line description. Each row: one item with explanation and visualization.

## Document structure (in order)

1. `<h1>` — "Catalog Title — Items by Category" (ampersand written as `&mdash;` entity in source).
2. `.subtitle` paragraph.
3. Optional section heading (HTML comment: "Optional section heading (use if catalog has multiple groups)"): `<h2>1. Group Title</h2>`.
4. One `table.obj-table` with `<thead>` header row: "Description & Analysis" | "Visualization", then three body rows (placeholders).
5. Closing `.philosophy` callout (HTML comment: "Closing callout").
6. `<script>` with `setup(id)` devicePixelRatio helper, palette comment, and three example canvas drawings.

## Table row 1 (placeholder text verbatim)

- Domain badge (`.metric-domain`): Domain
- Title (`.metric-title`): 1. Item Title
- Bullets (`.metric-desc` ul):
  - **Why bad:** Core problem in one sentence
  - **What it hides:** What the metric/item conceals
  - **Real damage:** Concrete consequence
  - **Fix:** What to use instead
- Right cell: `<canvas id="canvas1">`

### Visualization (canvas `canvas1`, 720×200)

Two-line comparison chart (example placeholder). Script comment: "Canvas 1: Example — two-line comparison".

- **Title (bold 14px, `#1a5276`, at pad.left, y=18):** "Metric A vs. Metric B Over Time"
- **Padding:** top 30, bottom 35, left 60, right 140. Axes: L-shaped (left vertical + bottom horizontal), stroke `#999`, width 1.
- **Data:** `data1 = [20, 35, 45, 60, 70, 80, 85, 90]` (rising line, red `#e74c3c`, width 3); `data2 = [50, 52, 48, 45, 44, 42, 40, 38]` (declining line, green `#27ae60`, width 3). y-scale maxY = 100; x evenly spaced across chart width.
- **Legend (top right at x = W−130, 12×12 swatches, 13px text `#222`):** red swatch "Vanity metric" (y≈40–51), green swatch "Real metric" (y≈58–69).

## Table row 2 (placeholder text verbatim)

- Domain badge: Domain
- Title: 2. Item Title
- Bullets:
  - **Why bad:** Core problem
  - **What it hides:** Hidden reality
  - **Real damage:** Concrete consequence
  - **Fix:** Better approach
- Right cell: `<canvas id="canvas2">`

### Visualization (canvas `canvas2`, 720×200)

Grouped bar comparison (example placeholder). Script comment: "Canvas 2: Example — bar comparison".

- **Title (bold 14px, `#1a5276`, y=18):** "Comparison: Reported vs Actual"
- **Padding:** top 30, bottom 40, left 60, right 30. Bar group width 80, gap 40, centered horizontally.
- **Data:** labels `['Item A', 'Item B', 'Item C', 'Item D']`; `reported = [85, 90, 70, 95]` drawn as left half-bars in light red `rgba(231,76,60,0.3)`; `actual = [30, 40, 60, 20]` drawn as right half-bars in solid green `#27ae60`. Scale: value/100 of chart height.
- **Labels:** each group label in `#222` 13px below bars at y = H−15.

## Table row 3 (placeholder text verbatim)

- Domain badge: Domain
- Title: 3. Item Title
- Bullets:
  - **Why bad:** Core problem
  - **What it hides:** Hidden reality
  - **Real damage:** Concrete consequence
  - **Fix:** Better approach
- Right cell: `<canvas id="canvas3">`

### Visualization (canvas `canvas3`, 720×200)

Histogram / distribution (example placeholder). Script comment: "Canvas 3: Example — scatter / distribution".

- **Title (bold 14px, `#1a5276`, y=18):** "Distribution — Shape Reveals the Problem"
- **Padding:** top 30, bottom 35, left 60, right 30.
- **Data (bin heights):** `[5, 12, 25, 40, 35, 20, 10, 5, 3, 2, 1, 1, 0, 0, 1]`, scale maxB = 42; bars fill `rgba(26,82,118,0.35)`, bar width = chartWidth/15 minus 2px gap; zero bins skipped.
- **Annotation (bold 13px red `#e74c3c` at 50% chart width, y = pad.top+15):** "Not normal → t-test invalid"

## Closing callout (`.philosophy`, verbatim)

**The meta-point:** Summary of the overarching lesson from this catalog.

## Script palette comment (verbatim)

- Primary: #1a5276 (blue)
- Positive: #27ae60 (green)
- Negative: #e74c3c (red)
- Warning: #e67e22 (orange)
- Bar fill: rgba(26,82,118,0.35)
- Text: #222 (labels), #444 (secondary)

## Regeneration instructions

- **This is a UI template file** in `ui-templates/`, kept as a starting point for catalog pages (bad metrics, examples, case studies, pattern catalogs). All item text is placeholder.
- **Layout:** h1 + `.subtitle` + optional `<h2>` group heading + one `table.obj-table` (header row + one `<tr>` per item: left td 50% text, right td 50% canvas) + closing `.philosophy` callout.
- **Page CSS:** body `-apple-system, BlinkMacSystemFont, sans-serif`, background `#fafafa`, color `#1a1a1a`, font-size 15px, padding 20px 10px. h1 `#1a5276`, margin-bottom 4px. h2 1.3em `#1a5276`, margin 35px 0 12px, border-bottom `2px solid #2980b9`, padding-bottom 8px. `.subtitle` `#333`, 1.1em, margin-bottom 30px.
- **Table CSS:** `.obj-table` width 100%, border-collapse collapse, margin-top 20px. th background `#1a5276`, white text, padding 12px 16px, border `1px solid #2980b9`. td border `1px solid #2980b9`, padding 14px 16px, vertical-align top; even rows background `#f0f8ff`; first td 50%, last td 50%.
- **Row content CSS:** `.metric-title` weight 700, `#1a5276`, 1.1em, margin-bottom 6px. `.metric-domain` inline-block badge, background `#2980b9`, white, padding 2px 8px, radius 3px, 0.85em, margin-bottom 8px. `.metric-desc ul` margin 4px 0 0 16px, line-height 1.7, 0.95em, `#222`; li margin-bottom 2px.
- **Canvas CSS:** `canvas { display: block; width: 720px; height: 200px; margin-top: 8px; }`. Script `setup(id)` sets backing store to 720×200 × `window.devicePixelRatio`, CSS size 720×200px, `ctx.scale(dpr, dpr)`, base font `14px -apple-system, sans-serif`.
- **Callout CSS:** `.philosophy` background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, margin 20px 0, 0.95em, `#222`.
- **Palette:** #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange, bar fill rgba(26,82,118,0.35).
- **HTML comments to preserve:** the template-header block comment inside `<style>`, "Optional section heading…", "Row 1/2/3", "Closing callout", "═══ Canvas setup with devicePixelRatio ═══", "═══ Color palette ═══", and per-canvas example comments.
- No nav bar, no back/home links. In regenerated HTML any card links would use `.html` extensions (this page has none).
