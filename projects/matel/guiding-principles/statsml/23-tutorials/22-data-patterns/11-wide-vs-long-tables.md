# Wide vs Long Tables

**Page type:** detail page (tutorial card-section layout: one h2 per section, two-column `table.layout` with 50% text / 50% viz)
**HTML title tag:** Wide vs Long Tables

**Subtitle:** The same data can sit as one column per measure or one row per measurement — each shape makes different work easy

## Twelve Temperature Readings, Two Table Shapes

**Tags:** `core idea` (blue), `running example` (green)

- **The data** — 3 temperature sensors (A, B, C) read once a day, Monday to Thursday
- **Wide shape** — 4 rows, one per day; sensors A, B, C each get their own column
- **Long shape** — 12 rows, one per reading; columns are just day, sensor, temp
- **Same 12 numbers** — nothing is added or lost; only the arrangement changes
- **Vocabulary** — wide→long is "melt" (unpivot); long→wide is "pivot"

**Example (italic):** Monday's three readings — A 21°, B 24°, C 19° — are one wide row or three long rows.

**Key point:** Wide and long are two arrangements of the same facts — shape is a choice, not a property of the data.

### Shared dataset (used across all four canvases)

Temperatures (°) by day × sensor:

| day | A | B | C |
|-----|---|---|---|
| Mon | 21 | 24 | 19 |
| Tue | 23 | 25 | 20 |
| Wed | 22 | 27 | 18 |
| Thu | 24 | 26 | 21 |

### Visualization (canvas `c1`, 720×300)

Side-by-side rendered tables: the wide table (left) and the long table (right), drawn as cell grids.

- **Title (bold 15px ink `#1a5276`, top center):** "Same 12 Readings: Wide (4 rows) vs Long (12 rows)"
- **Left, wide table (header bold 13px blue `#2a78d6` at x=178: "WIDE — one column per sensor"):** 4-column grid starting (58, 66), cells 60×26; header row [day, A, B, C] filled `rgba(42,120,214,0.15)` with ink text; day column filled `rgba(42,120,214,0.05)` bold; value cells white with blue `#2a78d6` borders showing the 12 temperatures with ° suffix. Below: 12px mute `#6b7280` "4 rows × 4 columns".
- **Right, long table (header bold 13px green `#008300` at x=520: "LONG — one row per reading"):** 3-column grid starting (430, 62), cells 60×16.5; header row [day, sensor, temp] filled `rgba(0,131,0,0.15)` with dark-green `#0a5a0a` text; 12 data rows (Mon/A/21° … Thu/C/21°) — day and sensor cells filled `rgba(0,131,0,0.04)`, temp cells white, green borders. Below: 12px mute "12 rows × 3 columns".
- **Between the tables (bold 13px orange `#d95926`, x=350):** "melt →" (y=130) and "← pivot" (y=165).
- **Caption (bold 13px orange, centered under the wide table at x=178, y=285):** "same facts, different arrangement"

## Melting One Wide Row by Hand

**Tags:** `worked example` (green), `core idea` (blue)

- **Take Tuesday's row** — (Tue, A=23, B=25, C=20): one row, three measure columns
- **Rule** — each measure column becomes its own row; the column NAME becomes a value
- **Row 1** — (Tue, sensor=A, temp=23)
- **Row 2** — (Tue, sensor=B, temp=25), **Row 3** — (Tue, sensor=C, temp=20)
- **Reverse it** — pivot groups the 3 rows by day and spreads sensor back into columns

**Example (italic):** Melting the whole wide table: 4 rows × 3 measure columns = 12 long rows, every time.

**Key point:** Melt turns column names into cell values; pivot turns cell values back into column names. The two operations undo each other.

### Visualization (canvas `c2`, 720×300)

Transformation diagram: one wide row on the left fanning out via arrows into three long rows on the right.

- **Title (bold 15px ink, top center):** "Melting Tuesday's Wide Row: 1 Row Becomes 3"
- **Wide row (left, cells 62×30 starting x=60, data row at y=120):** header row [day, A, B, C] filled `rgba(42,120,214,0.15)` ink text; data row [Tue, 23°, 25°, 20°] — "Tue" cell filled `rgba(42,120,214,0.05)` bold, value cells white, blue borders. Below it, bold 12px blue label "one wide row".
- **Three long rows (right, x=470, cells 62 wide):** each is a mini 3-column table with its own header [day, sensor, temp] (22px tall, fill `rgba(0,131,0,0.12)`, `#0a5a0a` text) above a 30px data row, green borders; the sensor cell is highlighted `rgba(217,89,38,0.10)` and bold:
  | y | values |
  |---|--------|
  | 62 | Tue, A, 23° |
  | 132 | Tue, B, 25° |
  | 202 | Tue, C, 20° |
- **Arrows:** three arrows from the right edge of the wide row (same origin point) to each long row, colored aqua `#199e70`, violet `#4a3aa7`, magenta `#d55181` respectively.
- **Captions (bottom center):** bold 13px orange `#d95926` 'the column name ("A", "B", "C") became a VALUE in the sensor column' (y=275); 12px mute "pivot runs the arrows backwards" (y=293).

## What Each Shape Makes Easy

**Tags:** `where it's used` (blue), `trade-off` (orange)

- **Long + group-by** — "average temp per sensor" is one line: group by sensor, mean of temp
- **Long + filter** — "all readings above 24°" is one condition on one column
- **Long scales** — a new sensor D is just more rows; no schema change needed
- **Wide for models** — one row per day with A, B, C side by side IS the feature matrix
- **Wide for eyes** — humans compare sensors fastest when they sit in adjacent columns

**Example (italic):** The plotting library wanted long input; the regression wanted wide — same data, one melt apart.

**Key point:** Store and clean in long ("tidy data"), pivot to wide at the last step for the model or the report.

### Visualization (canvas `c3`, 720×300)

Bar chart of average temperature per sensor plus a text side-panel comparing the two shapes.

- **Title (bold 15px ink, top center):** "One Group-By on the Long Shape: Average Temp per Sensor"
- **Axes:** y = 0° to 30° with gridlines (`#e5e9ef`) and 12px `#666` labels every 10°; axis lines `#999`; padding: top 60, bottom 58, left 65, right 260.
- **Bars (70px wide, fill at 0.5 alpha with 2px solid stroke; value bold 13px `#222` above each bar, sensor label below):**
  | sensor | average | color |
  |--------|---------|-------|
  | sensor A | 22.5° | `#2a78d6` (blue) |
  | sensor B | 25.5° | `#d95926` (orange) |
  | sensor C | 19.5° | `#199e70` (aqua) |
- **Side panel (left-aligned text at x = width−240):** bold 13px green `#008300` "LONG shape is best for:" then 12px `#444` "group-by, filters, plots," / "adding sensors (just rows)"; bold 13px blue `#2a78d6` "WIDE shape is best for:" then 12px `#444` "feature matrix for models," / "correlations, human reading"; bold 12px orange "clean long, pivot wide last".
- **Caption (12px mute, bottom center, y=290):** "means of the 12 readings: A (21,23,22,24), B (24,25,27,26), C (19,20,18,21)"

## The Confusion: Missing Data Looks Different in Each Shape

**Tags:** `common mistake` (red), `rule of thumb` (blue)

- **Sensor C dies Wednesday** — no reading exists for (Wed, C)
- **Wide shows a hole** — the Wed row keeps its C column, filled with NULL
- **Long shows nothing** — the (Wed, C) row simply is not there: 11 rows, not 12
- **The trap** — counting long rows per day silently gives Wed a count of 2, not 3
- **Rule of thumb** — in long shape, always ask "which rows SHOULD exist but don't?"

**Example (italic):** The daily average for Wednesday used 2 sensors instead of 3 — and nobody saw a NULL anywhere.

**Key point:** Wide makes missing data visible as NULL cells; long makes it invisible as absent rows — the silent one is the dangerous one.

### Visualization (canvas `c4`, 720×300)

Side-by-side rendered tables with the (Wed, C) reading missing: a NULL cell in the wide table vs an absent row in the long table.

- **Title (bold 15px ink, top center):** "Sensor C Dies on Wednesday: NULL Hole vs Missing Row"
- **Left, wide table (header bold 13px blue `#2a78d6` at x=178: "WIDE: the hole is visible"):** same grid geometry as c1's wide table; the (Wed, C) cell shows "NULL" in bold red `#e74c3c` on `rgba(231,76,60,0.15)` fill with a red border; all other cells as in c1. Below: bold 12px red "↑ you can SEE the gap".
- **Right, long table (header bold 13px green `#008300` at x=520: "LONG: the row just is not there"):** same grid geometry as c1's long table, but with only 11 data rows — the (Wed, C) row is skipped entirely. Below: bold 12px red "11 rows, not 12 — nothing marks the gap".
- **Caption (bold 13px magenta `#d55181`, bottom center, y=288):** "the absent row is the silent one — check expected row counts"

## Regeneration instructions

- **Template:** tutorials topic-page skeleton. `<h1>` (no index number) with 2px `#2980b9` bottom border, `.subtitle` gray line, then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with `td.text-col` (50%) holding tags/bullets/example/key-point and `td.viz-col` (50%) holding the canvas. No payload blocks on this page.
- **Text column structure:** `.tags` row of colored pill spans (0.72rem bold, radius 10px — blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`); 5 one-line bullets each opening with `<b>` in `#1a5276`; one italic `.example` line (`#555`, 0.9rem); one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; canvases `width:100%` with 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvases:** intrinsic 720×300; shared `setup(id)` helper scales backing store by `window.devicePixelRatio` and calls `ctx.scale` so drawing stays in logical coordinates; a shared `cell(ctx, x, y, w, h, txt, fill, stroke, bold, txtColor)` helper draws table cells; shared arrays `DAYS` (Mon–Thu), `SENSORS` (A, B, C), and `TEMPS` (the 4×3 matrix above) drive all four canvases. All data hardcoded.
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card/grid links use `.html` extensions (this page has none — no cross-page links).
