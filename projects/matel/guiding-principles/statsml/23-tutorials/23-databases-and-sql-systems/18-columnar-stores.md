# Columnar Stores

**Page type:** detail page (tutorial: 4 `.card-section` blocks, each an h2 + two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Columnar Stores

**Subtitle:** "Average order value over 100 million orders" only needs the amount column — so store each column together and read one column, not every row

## Section 1: A Question That Needs One Column of a Hundred Million Rows

**Tags:** `core idea` (blue), `running example` (green)

- **The setup** — an orders table: 100M rows, 10 columns, about 8 GB on disk
- **The question** — `SELECT AVG(amount) FROM orders`: it touches ONE column
- **Row storage** — rows sit whole on disk, so reading amount drags 9 other columns along
- **Column storage** — all 100M amounts sit together: read just that strip, 0.8 GB
- **10x less disk read** — 0.8 GB instead of 8 GB, before compression helps further

*Example (italic):* Disks hand back data in blocks — you can't grab one field out of a row block without reading the block.

**Key point:** Analytics questions touch few columns of many rows, so laying data out column-by-column means reading only what the question asks about.

### Visualization (canvas `c1`, 720×300)

Two horizontal strips of 10 column segments each, comparing bytes read.

- **Title (bold 15px `#1a5276`, top center):** "AVG(amount) over 100M orders: bytes the disk must hand over".
- **Column names (11px, centered in each 52×44 segment, 2px gaps, starting x=100):** id, user, amount, status, date, ship, tax, disc, src, note.
- **Row-store strip** (y=70, left label bold 13px `#333` "row store"): all 10 segments filled `rgba(217,89,38,0.55)` with orange `#d95926` strokes and dark-orange `#7a3312` labels; bold orange result to the right: "8 GB read"; gray 11px caption below: "columns interleaved inside every row — reading amount drags all 10 along".
- **Column-store strip** (y=175, left label "column store"): only the `amount` segment (index 2) hot — fill `rgba(0,131,0,0.6)`, green `#008300` 2.5px stroke, bold white label; the other 9 segments fill `rgba(42,120,214,0.12)` with gray `#b9c2cc` strokes and gray labels; bold green result: "0.8 GB read"; gray caption: "each column stored as its own strip — 9 strips never touched".
- **Bottom caption (bold 14px green, centered):** "10x less I/O by skipping the columns the question never mentions".

## Section 2: Four Orders, Byte by Byte, Both Ways

**Tags:** `worked example` (green)

- **The orders** — ids 501-504, amounts 12, 40, 12, 95, statuses shipped ×3, returned ×1
- **Row layout** — disk reads left to right: id, user, amount, status, id, user, amount, status...
- **Column layout** — same bytes regrouped: all ids, then all users, then all amounts...
- **AVG(amount), row layout** — touch all 16 cells to use 4 of them
- **AVG(amount), column layout** — jump to the amount strip, read 4 cells, done

*Example (italic):* Same 16 values in both drawings — only the order they sit on disk changes.

**Key point:** With 4 columns the saving is 4x; with the real table's 10 columns it's 10x — the win grows with table width, not table length.

### Visualization (canvas `c2`, 720×300)

Two strips of 16 cells showing the same 4 orders in row vs column order.

- **Title (bold 15px `#1a5276`, top center):** "Same 4 Orders on Disk — read order shown left to right".
- **Data:** ids `['501','502','503','504']`, users `['u17','u02','u17','u31']`, amounts `['12','40','12','95']`, statuses `['shp','shp','shp','ret']`. Field colors: id blue `#2a78d6`, user aqua `#199e70`, amount yellow `#c98500`, status violet `#4a3aa7`. Cells 38×30 (overlapping by 1px), monospace 11px values, starting x=105; amount cells "hot" — filled in their color at 0.85 alpha with bold white text and 2px stroke; all other cells fill `#f4f6f8`, gray `#666` text, 1px stroke in the field color.
- **Row layout strip** (y=66, left label bold 13px `#333` "row layout"): 16 cells in per-order sequence id,user,amount,status ×4 — so the four amounts are scattered at positions 3, 7, 11, 15. Gray 11px caption: "the 4 amounts are scattered — every cell must stream past to collect them".
- **Column layout strip** (y=165, left label "column layout"): 16 cells regrouped — all 4 ids, all 4 users, all 4 amounts, all 4 statuses. A yellow bracket (2.5px, `#c98500`) spans the 4-cell amount strip (cells 9-12) with bold yellow label above: "seek here, read 4 cells, stop". Gray caption: "all ids, then all users, then all amounts, then all statuses".
- **Bottom caption (bold 14px green `#008300`, centered):** "AVG(amount): 4 cells touched instead of 16 — same data, 4x less read".

## Section 3: Compression Loves Columns

**Tags:** `core idea` (blue), `worked example` (green)

- **Columns are same-typed** — a strip of only statuses, or only dates, repeats heavily
- **Run-length** — "shipped, shipped, shipped, shipped, returned" becomes "shipped ×4, returned ×1"
- **Dictionary** — 5 distinct statuses across 100M rows: store each as a number 0-4
- **Rows mix types** — id next to text next to a date: no pattern for the compressor
- **Compound win** — read 1 of 10 columns, then ~5x compression: ~0.16 GB, not 8 GB

*Example (italic):* The status column has 100M values but only 5 distinct ones — a compressor's dream.

**Key point:** Columnar wins twice — skip the columns you don't need, then compress the ones you do far better than mixed-type rows ever compress.

### Visualization (canvas `c3`, 720×300)

Three-part diagram: raw status strip, run-length-encoded strip, and a bytes-scanned bar funnel.

- **Title (bold 15px `#1a5276`, top center):** "Why a Column Strip Compresses So Well (status column)".
- **Raw strip** (y=62, left label bold 12px "raw", cells 74×30 starting x=95): sequence `shipped, shipped, shipped, shipped, returned, shipped, shipped, pending`; cell colors shipped aqua `#199e70`, returned magenta `#d55181`, pending yellow `#c98500` (fill at 0.35 alpha, 1.2px stroke, 11px monospace `#444` text). Gray 11px note: "... repeats like this for 100M values, only 5 distinct words".
- **Encoded strip** (y=140, left label "encoded"): four proportional-width blocks (weight×32px, 0.6 alpha fills, bold 10px monospace labels): "shipped ×4" (aqua, weight 4), "returned ×1" (magenta, 1.6), "shipped ×2" (aqua, 2), "pending ×1" (yellow, 1.6). Bold green 12px note to the right: "run-length: value + count".
- **Bytes funnel** (rows from y=210, 26px pitch; plot x=260, width 330, scale max 8 GB; bars 17px at 0.7 alpha, bold GB label after each):
  - "whole table (row store)" = 8 GB, orange `#d95926`
  - "amount column only" = 0.8 GB, blue `#2a78d6`
  - "amount column, compressed" = 0.16 GB, green `#008300` (bar clamped to minimum 5px)
- **Bottom caption (bold 13px green, centered):** "50x less than the row store scan — skip, then squeeze".

## Section 4: Why Warehouses Are Columnar — and App Databases Aren't

**Tags:** `where it's used` (blue), `common mistake` (red)

- **Warehouse queries** — averages, group-bys, trends: few columns, all the rows
- **App queries** — "show order 501": all columns, one row — row layout is perfect there
- **That's the split** — BigQuery, Redshift, Snowflake, Parquet files: columnar; the checkout database: rows
- **The classic mistake** — `SELECT *` in a warehouse reads all 10 column strips: the 10x win gone
- **Writes cost more** — inserting one order touches every column strip, so columnar loads in batches

*Example (italic):* Some warehouses bill by bytes scanned — there, `SELECT *` on the orders table costs 10x the two-column query.

**Common mistake:** Column layout only pays when you name your columns — `SELECT *` turns a columnar store back into a slow, expensive row store.

### Visualization (canvas `c4`, 720×300)

Two-panel heat-grid comparison split by a vertical dashed divider `#bdc3c7` (dash 4/3) at x=360.

- **Title (bold 15px `#1a5276`, top center):** "Two Workloads Cut the Table in Opposite Directions".
- **Grids:** each panel a 7-row × 10-column grid of 20px squares (24px pitch); cold cells `rgba(107,114,128,0.14)`.
- **Left panel** (heading bold 13px violet `#4a3aa7` centered at x=190): "app: \"show order 501\"" — grid at (45,70) with the whole 4th row (r=3) hot in `rgba(74,58,167,0.75)`. Bold violet caption: "one row, every column → row store wins".
- **Right panel** (heading bold 13px green `#008300` centered at x=545): "analytics: AVG(amount)" — grid at (400,70) with the whole 5th column (c=4) hot in `rgba(0,131,0,0.7)`. Bold green caption: "one column, every row → column store wins".
- **Bottom caption (bold 13px orange `#d95926`, centered):** "SELECT * lights up the whole grid either way — name your columns in a warehouse".

## Regeneration instructions

- **Layout:** tutorial detail page. h1 (2rem `#1a5276`, 2px solid `#2980b9` bottom border) + `.subtitle` (`#666`, 0.95rem), then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `.text-col` (50%) holding `.tags`, a `<ul>` of bullets (inline `<code>` allowed), `.example` italic line, and `.key-point` callout; `.viz-col` (50%) holding the canvas.
- **Text styles:** body system-ui sans, white background, text `#2c3e50`, padding 40px, line-height 1.6; bullets 0.92rem with bold lead terms `<b>` in `#1a5276`; `.example` italic `#555` 0.9rem; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, 0.9rem; inline `code` monospace 0.9em on `#f4f6f8` with 3px radius.
- **Tag pills:** `.tag` inline pill, 0.72rem bold, radius 10px — blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Canvas:** 720×300 intrinsic, CSS `width:100%`, 1px `#e0e0e0` border, 4px radius; scaled by `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Chart palette (JS `P` object):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links would use `.html` extensions.
