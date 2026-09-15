# Parquet

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Parquet

**Subtitle:** Parquet stores a table column by column instead of row by row — so a query that needs 2 of 40 columns reads 5% of the bytes, and every engine from pandas to Spark opens the same file

## Forty Columns, Stored Column by Column

**Tags:** `core idea` (blue), `columnar layout` (green), `Apache` (orange)

- **The table** — an `events` table: 100 million rows, 40 columns, saved as one 2 GB Parquet file
- **Row layout** — a CSV writes row after row, so every column's bytes are interleaved on disk
- **Column layout** — Parquet regroups values by column: all user_ids together, all dates together
- **The structure** — rows split into row groups; inside each, one column chunk per column, cut into pages
- **The origin** — built by Twitter and Cloudera in 2013, borrowing the columnar shredding of Google's Dremel paper
- **Self-describing** — the footer stores the schema, offsets, and stats; no side metadata file needed

*Example (italic):* The 2 GB events file holds ten row groups of 10 million rows; each row group holds forty column chunks, and each chunk is cut into ~1 MB pages.

**Key point:** A Parquet file is rows chopped into row groups and regrouped by column — the unit a query reads is a column chunk, not a row.

### Visualization (canvas `c1`, 720×300)

Two-band diagram of the same 10 million rows: interleaved row layout on top, one Parquet row group (column chunks with page dividers) below.

- **Title (bold 15px, `#1a5276`, top center):** "Same 10 Million Rows: Interleaved vs Regrouped by Column".
- **Top band (row layout):** 12px `#444` label "row layout (CSV)" at x=20, y=80; six row boxes starting at x=150, y=62, each 88px wide × 30px tall, 4px gaps, 1px `#999` border; each box split into four 22px vertical strips filled `rgba(42,120,214,0.30)` / `rgba(0,131,0,0.30)` / `rgba(217,89,38,0.30)` / `rgba(74,58,167,0.30)` — the four columns interleaved in every row; 11px `#6b7280` "row 1" under the first box and "… 10M rows →" right-aligned under the last box.
- **Bottom band (columnar):** 12px `#444` label "Parquet row group" at x=20, y=190; four chunk blocks at y=172, height 34, starting x=150 with 8px gaps: user_id width 170 fill `rgba(42,120,214,0.30)` border `#2a78d6`; event_date width 60 fill `rgba(0,131,0,0.30)` border `#008300`; country width 40 fill `rgba(217,89,38,0.30)` border `#d95926`; revenue width 170 fill `rgba(74,58,167,0.30)` border `#4a3aa7`; dashed 1px `#6b7280` vertical dividers inside user_id and revenue marking pages; 12px `#6b7280` "… 36 more chunks" right of revenue, right-aligned at x=716.
- **Chunk labels (11px `#444`, staggered on two rows y=222 / y=236 under each chunk center):** "user_id 5 MB", "event_date 1 MB", "country 0.4 MB", "revenue 5 MB".
- **Annotation (bold 13px green `#008300`, centered near y=270):** "a query for 2 columns touches 2 chunks and skips the rest".
- **Caption (12px `#444`, bottom right):** "chunk widths ∝ compressed size, sizes illustrative".

## The Two-Column Query That Reads 5%

**Tags:** `worked example` (blue), `column pruning` (green), `encodings` (orange)

- **The query** — `SELECT user_id, revenue` needs 2 of the 40 columns of the events table
- **The seek** — the footer lists every chunk's byte offset, so the reader jumps straight to those two
- **The math (exact)** — 2 of 40 columns at ~50 MB each: 100 MB read of 2,000 MB stored = 5%
- **Dictionary encoding** — a country chunk stores 195 distinct strings once, then small integer codes
- **Run-length encoding** — sorted dates collapse to runs: "Aug 20 × 3.1M" instead of 3.1M copies
- **The payoff** — per row group, country shrinks 40 MB → 0.4 MB and event_date → 1 MB (illustrative)

*Example (italic):* On the 2 GB events file, the two-column query reads the ten user_id chunks and ten revenue chunks — 100 MB, or 5% — before any filter is even applied.

**Key point:** Column pruning comes free with columnar layout — bytes read scale with the columns you select, not the columns the table stores.

### Visualization (canvas `c2`, 720×300)

Top: a strip of 40 thin blocks (the column chunks) with 2 highlighted as read. Bottom: two horizontal bars comparing bytes stored vs bytes read.

- **Title (bold 15px, `#1a5276`, top center):** "SELECT 2 of 40 Columns: Read 100 MB of 2,000 MB".
- **Column strip:** 40 blocks at y=70, height 36, starting x=60, each 14px wide with 1px gaps (step 15, total width 600); blocks 1 and 12 solid green `#008300` (the selected user_id and revenue), the other 38 filled `rgba(107,114,128,0.25)` with 1px `#999` border; 11px `#008300` labels "user_id" above block 1 and "revenue" above block 12; 11px `#6b7280` "40 column chunks (one row group shown)" under the strip at y=122.
- **Bar 1 (y=170):** blue fill `rgba(42,120,214,0.30)`, border `#2a78d6`, x=60, width 600, height 22; 12px `#444` label "bytes stored: 2,000 MB" at the bar's right end.
- **Bar 2 (y=210):** solid green `#008300`, x=60, width 30 (5% of 600), height 22; 12px `#008300` label "bytes read: 100 MB" just right of the bar.
- **Annotation (bold 13px violet `#4a3aa7`, near x=300, y=250):** "2 / 40 = 5% — the fraction is exact, the sizes illustrative".
- **Caption (12px `#444`, bottom right):** "equal ~50 MB per column assumed, illustrative".

## Min/Max Stats: Skipping Whole Row Groups

**Tags:** `where it's used` (blue), `predicate pushdown` (green), `every engine` (orange)

- **The stats** — the footer keeps min/max per column chunk: row group 7's event_date spans Aug 19–21
- **The filter** — `WHERE event_date = '2026-08-20'`: nine of the ten row groups' ranges exclude Aug 20
- **The skip** — the reader proves those nine can't match and never fetches them; only row group 7 is read
- **The bill** — one row group's user_id + revenue + event_date chunks: 5 + 5 + 1 = 11 MB of 2,000 MB
- **Sorting helps** — min/max prunes well only when data is sorted or clustered by the filter column
- **Every engine** — Spark, DuckDB, pandas (pyarrow), Trino, and cloud warehouses read the same file; lakehouse formats like Iceberg and Delta store Parquet underneath

*Example (italic):* DuckDB on a laptop answers the Aug-20 revenue query by reading 11 MB of the 2 GB file — about 0.55% — because nine row groups are skipped unopened.

**Key point:** Min/max statistics turn a scan into targeted reads — the engine proves a row group cannot match the filter and skips it without reading a byte of its data.

### Visualization (canvas `c3`, 720×300)

Strip of ten row-group boxes with their event_date min–max ranges; one is highlighted as read for the Aug 20 filter, nine are grayed out as skipped.

- **Title (bold 15px, `#1a5276`, top center):** "WHERE event_date = Aug 20: 9 of 10 Row Groups Skipped".
- **Row groups:** ten boxes at y=110, height 70, starting x=60, each 56px wide with 4px gaps (step 60, total 596); min–max date ranges hardcoded as `["1–3","4–6","7–9","10–12","13–15","16–18","19–21","22–24","25–27","28–30"]` (August days), drawn as 11px `#444` labels centered under each box at y=200 with axis note 11px `#6b7280` "event_date min–max (Aug)" at x=60, y=218.
- **Skipped boxes (indexes 0–5 and 7–9):** fill `rgba(107,114,128,0.18)`, 1px `#999` border, 12px `#6b7280` "skip" centered inside.
- **Read box (index 6, range "19–21"):** solid-edged 2px `#008300` border, fill `rgba(0,131,0,0.25)`, bold 12px `#008300` "read" centered inside; bold 12px `#008300` "Aug 20 ∈ [19, 21]" above it at y=95.
- **Filter marker:** bold 13px `#1a5276` "filter: event_date = Aug 20" top left at x=60, y=62.
- **Annotation (bold 13px green `#008300`, centered near y=250):** "11 MB read of 2,000 MB — stats decided before any data was touched".
- **Caption (12px `#444`, bottom right):** "data sorted by date; ranges illustrative".

## A File Format, Not a Database

**Tags:** `common mistake` (red), `immutability` (orange)

- **The confusion** — columnar files are so fast for scans that people start treating Parquet like a database
- **Point lookups** — fetching one event by id decodes pages from all 40 chunks: ~80 MB touched for one row
- **No edits** — Parquet files are immutable; changing one value means rewriting the entire file
- **Appends too** — adding rows means writing a new file; streams of tiny files drown queries in footer reads
- **The fix** — put a table format or database on top; keep Parquet as bulk columnar storage, which it is

*Example (italic):* Correcting one customer's country in the 2 GB file rewrites all 2,000 MB — a transactional database changes that one row in place.

**Common mistake:** Expecting row-store behavior from a column store. Parquet trades single-row reads, updates, and appends for cheap scans — engines that need mutability put a layer on top rather than edit the file.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart: bytes touched by four operations on the 2 GB events file — scans stay small, point lookups and edits blow up.

- **Title (bold 15px, `#1a5276`, top center):** "Bytes Touched per Operation on the 2 GB File".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right, max width 440; log-feel achieved by hardcoded pixel widths, not a real log axis.
- **Rows (top to bottom at y = 70, 120, 170, 220), each with a left-aligned 12px `#444` label at x=20:**
  - "SUM(revenue), 100M rows — 50 MB": green `#008300` bar width 120
  - "2 cols + date filter — 11 MB": green `#008300` bar width 60
  - "fetch 1 row by event_id — ~80 MB": orange `#d95926` bar width 150 with 12px orange label "more than the full-column scan"
  - "update 1 value — rewrite 2,000 MB": red `#e74c3c` bar width 440 with 12px red label "whole file"
- **Bar style:** 16px tall, solid fills, 11px `#444` MB labels at bar ends.
- **Annotation (bold 13px magenta `#d55181`, right side near y=255):** "great at scanning millions of rows, terrible at touching one".
- **Caption (12px `#444`, bottom right):** "pixel widths schematic, MB illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); file size, chunk sizes, row-group date ranges, and bytes-touched figures are invented and labeled illustrative; the 2/40 = 5% fraction and the 5 + 5 + 1 = 11 MB sum are exact arithmetic on those illustrative sizes. Text numbers must stay identical to chart numbers (2,000 MB / 100 MB / 11 MB / 10 row groups / 195 dictionary entries).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
