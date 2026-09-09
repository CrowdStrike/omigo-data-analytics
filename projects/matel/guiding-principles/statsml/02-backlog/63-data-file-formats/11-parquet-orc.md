# Parquet / ORC

**Page type:** detail page (single card-section, two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** Parquet / ORC

**Subtitle:** Columnar on-disk storage.

## Intro callout

**Core trade-off:** Analytical query speed through column pruning and predicate pushdown — at the cost of expensive row-level updates and write amplification.

## How It Works

Binary columnar formats that store data by column, not by row. Parquet (Hadoop ecosystem, now universal) and ORC (from Hive) solve the same problem: read only what the query touches.

- **Schema:** In the file footer, with per-column statistics
- **Layout:** Row groups (stripes) → contiguous compressed column chunks
- **Encoding:** Per column — dictionary, RLE, delta
- **Best for:** Wide tables, few-column analytical queries

**How it's read:** The reader starts at the footer: schema, row group locations, and min/max/null-count statistics per column chunk. For `SELECT avg(price) WHERE region = 'US'` on a 200-column table, it reads only the `price` and `region` chunks (~1% of the file), and the statistics let it skip entire row groups whose region range excludes 'US' entirely — predicate pushdown. Similar values stored together also compress far better than interleaved rows. Total I/O can land under 0.5% of what a CSV scan reads.

**Key point (red-left-border callout):** **Failure mode:** Treating it like a database. Updating one row means rewriting its whole row group; streaming small appends amplifies writes. It's a scan-optimized archive format, not a mutable store.

*Example: S3 + Parquet data lakes, Spark/Presto/DuckDB engines, ML feature stores, BigQuery exports.*

### Visualization (canvas `c1`, 720×300)

Grid diagram of a Parquet file: 3 row groups × 5 column chunks, showing which chunks a query actually reads, plus a footer strip.

- **Title (bold 14px system-ui, `#1a5276`, centered at y=25):** "Parquet — Read Only the Columns and Row Groups the Query Touches".
- **Query line (12px monospace `#333`, centered at y=48):** `SELECT avg(price) WHERE region = 'US'`.
- **Grid:** columns `price`, `region`, `user_id`, `ts`, `… 196 more`; needed columns are `price` and `region`. Cells 92px wide, 52px tall, 6px horizontal gap, 10px vertical gap, starting x=70, y=66. Three row groups; row group 2 is skipped by statistics.
  - Row-group labels (10px, right-aligned, `#666`): "row group 1/2/3"; second label line colored per group — green `#27ae60` "stats: region A–Z → scan" for groups 1 and 3, red `#e74c3c` "stats: region A–M → skip" for group 2.
  - Cell fills: read cells (needed column AND scanned group) `rgba(39,174,96,0.5)` with 2px `#27ae60` border and bold 11px monospace `#1a5276` label; skipped-group cells `rgba(231,76,60,0.08)`; other unread cells `rgba(200,200,200,0.25)`; unread borders `#ccc` 1px, labels `#999` 11px monospace.
- **Footer strip:** full-grid-width bar 22px tall, fill `#1a5276`, white 10px centered text: "footer: schema + row group offsets + min/max/null-count stats per column chunk — read FIRST".
- **Bottom note (gray `#666`, 11px, centered):** "green = actually read (~2 of 200 columns, 2 of 3 row groups) · everything else never leaves disk · CSV reads 100% every time".

## Regeneration instructions

- **Layout:** single `.card-section` with h2 "How It Works" (1.3rem `#1a5276`, 2px `#2980b9` bottom border), containing a `table.layout` (100% width, border-collapse) with one `<tr>`: left `td.text-col` (45%) holds paragraph, `<ul>` bullets, a `.key-point` div, and a `.example` paragraph; right `td.viz-col` (55%) holds the canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border. `.subtitle` `#666` 0.95rem. `.intro-callout` background `#f8f9fa`, left border 3px solid `#2980b9`, padding 10px 14px, 0.93rem. `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem. `.example` italic `#555` 0.9rem. `ul` 0.92rem. `code` background `#f0f4f8`, padding 2px 6px, radius 3px. Canvas `width: 100%`, border `1px solid #e0e0e0`, radius 4px. No nav bar, no back/home links.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange; translucent green/red/gray fills for read/skipped/unread cells.
- **Canvas:** intrinsic 720×300; use `window.devicePixelRatio` scaling (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper.
- In regenerated HTML, any card links use `.html` extensions.
