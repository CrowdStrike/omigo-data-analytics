# Schema Compliance Without Data Compliance

**Page type:** detail page (two-column obj-table layout: text left 40%, canvas right 60%, one row per section)
**HTML title tag:** Schema Compliance Without Data Compliance — Common Bad Practices

**Subtitle:** Negligence — Data passes structural validation (correct types, non-null, within range) but is semantically incompatible with everything else. Can't JOIN, can't compare, can't integrate. Creates parallel data universes that require custom ETL to bridge.

## Section 1: The Practice

- New feature launches with its own event naming, timestamp format, granularity, or entity definition.
- Schema validator passes: fields are present, types match, values non-null. ✓
- But: event names don't follow existing conventions, timestamps use a different timezone, user IDs reference a different identity system, granularity is per-session while everything else is per-event.
- Data exists. Data is correct in isolation. Data cannot be combined with anything else without a custom translation layer.

### Visualization (canvas `c1`, 720×300)

Waterfall chart: 10,000 schema-valid records successively reduced to the subset that is semantically usable.

- **Title (bold 15px, `#1a5276`, top center):** "10,000 Records Pass Schema Validation. How Many Are Actually Right?".
- **Plot area:** left 70, right w-30, top 55, bottom 235; y scale 0–10,000. Horizontal gridlines `rgba(0,0,0,0.06)` at 0/2,500/5,000/7,500/10,000 with 11px `#666` right-aligned y labels ("0", "2,500", "5,000", "7,500", "10,000"); solid `#ccc` baseline at the bottom.
- **Six bars** (width 66, centered in equal slots across the plot area), each with a bold 12px value label 6px above the bar top (in the bar's color) and a two-line 11px `#666` x label below the baseline (at +15 and +28):
  1. "Pass / schema" — full column 0→10,000, blue `#1a5276`, label "10,000".
  2. "Placeholder / values" — floating drop 10,000→8,600, red `#e74c3c`, label "−1,400".
  3. "Stale / defaults" — drop 8,600→7,500, red, label "−1,100".
  4. "Wrong unit / / timezone" — drop 7,500→6,600, red, label "−900".
  5. "Wrong / granularity" — drop 6,600→5,800, red, label "−800".
  6. "Semantically / usable" — full column 0→5,800, green `#27ae60`, label "5,800 (58%)".
- **Connectors:** dashed (4/4) `#999` width 1 horizontal lines from each bar's right edge to the next bar's left edge, at the running-total level (10,000 / 8,600 / 7,500 / 6,600 / 5,800).
- **Insight annotation (bold 13px `#e74c3c`, right-aligned at chart right edge, y=46):** "42% of 'valid' records carry wrong or meaningless data".
- **Caption (italic 13px `#666`, bottom center):** "Every record here has correct types, no nulls, and in-range values. Schema validation caught none of these problems."

## Section 2: Examples

**Timestamp Mismatch** (example box)
Existing system: UTC timestamps, millisecond precision. New feature: local timezone, second precision. JOIN on timestamp? Off by hours. Aggregate by day? Depends which timezone "day" means. Both pass schema validation.

**Entity Granularity Mismatch** (example box)
Existing events: one row per user action (click, view, purchase). New feature: one row per session (aggregated). Want click-through rate per session? Can't compute — new data already collapsed the denominator. Need raw events? Don't exist.

**Naming Convention Drift** (example box)
Existing: `user_clicked_product`, `user_viewed_page`. New feature: `ProductClick`, `PageView`. Same semantics, different names. Every downstream query, dashboard, and model needs to know both conventions. Or: one gets discovered 6 months late.

**Identity System Mismatch** (example box)
Platform uses `user_id` (account-level). New mobile feature uses `device_id` (device-level). One user, three devices = three "users" in new data. Conversion metrics inflated 3×. JOIN with account-level data? Requires identity resolution layer that doesn't exist.

### Visualization (canvas `c2`, 720×340)

Heatmap-style coverage grid: validation dimensions (columns) × tables/fields (rows); green cell = a check exists, red cell = nothing checks it. The semantic columns are almost entirely red.

- **Title (bold 15px, `#1a5276`, top center):** "Validation Coverage Heatmap: What Gets Checked vs What Matters".
- **Grid geometry:** row labels end at x=140 (right-aligned, 12px monospace `#333`); grid starts at gridL=150, column width 90, gridTop=76, row height 32, 5 rows.
- **Group labels (bold 12px, centered, y=44):** "Structural — validated" in green `#27ae60` over columns 1-3; "Semantic — nobody checks" in red `#e74c3c` over columns 4-6. Dashed (4/4) `#999` width 1 vertical divider at x=gridL+3·colW from y=34 to the grid bottom.
- **Column headers (bold 11px `#333`, centered, y=66):** "Type", "Nullability", "Range", "Cross-field", "Freshness", "Semantics".
- **Rows and check matrix (1 = check exists → green cell with "✓"; 0 = unchecked → red cell with "✗"):**
  - `orders.amount` — [1, 1, 1, 0, 0, 0]
  - `users.signup_ts` — [1, 1, 0, 0, 1, 0]
  - `events.name` — [1, 1, 0, 0, 0, 0]
  - `mobile.device_id` — [1, 0, 1, 0, 0, 0]
  - `sessions.duration` — [1, 1, 1, 0, 0, 0]
- **Cells:** inset 2px within each 90×32 slot; green cells fill `rgba(39,174,96,0.15)` stroke `#27ae60`, red cells fill `rgba(231,76,60,0.12)` stroke `#e74c3c` (width 1); bold 13px "✓"/"✗" glyph centered, in the stroke color.
- **Coverage row (20px below grid):** right-aligned 12px `#666` label "coverage" in the row-label gutter; per-column bold 12px counts centered under each column — "5/5", "4/5", "3/5", "0/5", "1/5", "0/5" — green when ≥3/5, red otherwise.
- **Insight annotation (bold 13px `#e74c3c`, centered, y=292):** "The checks that would catch semantic incompatibility are exactly the ones nobody runs".
- **Caption (italic 13px `#666`, bottom center):** "Green = a check exists. Red = nothing looks. Every red cell passes validation by default."

## Section 3: Why It Persists

- **Schema validators don't check semantics:** Type = string ✓, non-null ✓, length < 255 ✓. Whether the string MEANS the same thing as another string in another table — not validated.
- **Teams work in isolation:** Each team picks conventions that make sense for THEIR feature. Nobody checks cross-team compatibility until integration time.
- **"My data works":** The new feature's dashboards look fine. The incompatibility only surfaces when someone tries to combine with historical data — months later.
- **No semantic contract enforcement:** Organizations have schema registries but not semantic registries. The types match; the meaning doesn't.

### Visualization (canvas `c3`, 720×300)

Line chart with filled area: quadratic growth of ETL bridges as incompatible systems grow.

- **Title (bold 15px, `#1a5276`, top center):** "ETL Bridge Count as Incompatible Systems Grow".
- **Plot area:** left 100, right w-60, bottom 240, top 50; light gray `#ccc` L-shaped axes width 1.
- **Data:** systems `[2, 3, 4, 5, 6, 7, 8]`, bridges = n(n-1)/2 → `[1, 3, 6, 10, 15, 21, 28]`; y scale max 28.
- **Area fill:** under the line, `rgba(231,76,60,0.12)`.
- **Line:** red `#e74c3c`, width 3, with 5px-radius red dots at each point and bold 12px `#333` value labels 12px above each dot.
- **X labels (12px `#666`, centered):** "2 systems" … "8 systems" at each point, 16px below axis.
- **Y labels (12px `#666`, right-aligned):** "0 bridges", "7 bridges", "14 bridges", "21 bridges", "28 bridges" at values 0/7/14/21/28.
- **Formula label (bold 13px `#1a5276`):** "Bridges needed = n(n-1)/2" centered at (chartR-100, chartTop+15).
- **Caption (italic 13px `#666`, bottom center):** "Quadratic growth. Each incompatible system makes ALL existing integrations harder."

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table with `border-collapse: collapse`, one `<tr>` per section; left `<td>` (40%) holds `.obj-title` + bullets or `.example-box` divs, right `<td>` (60%, centered) holds the canvas. Section 2's left cell uses four `.example-box` divs each with a `.ex-title` heading line.
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#666` 1.0em; `ul` 0.9em `#333`; `strong` `#1a5276`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `.example-box` background `#f8fafb`, border `1px solid #e0e0e0`, radius 6px, padding 12px 16px, margin 10px 0, 0.88em; `.ex-title` weight 700 `#1a5276`; `code` background `#f0f0f0`, padding 2px 5px, radius 3px, 0.85em. No nav bar, no back/home links.
- **Canvas:** intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; `canvas { display: block; margin: 0 auto; }`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, gray text `#666`/`#333`.
- Card links elsewhere pointing to this page use the `.html` extension in regenerated HTML.
